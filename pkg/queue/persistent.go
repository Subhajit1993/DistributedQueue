package queue

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*
Sequence Of Actions<
1. Create a new persistent queue instance - NewPersistentQueue
	- Initialize write-ahead log (WAL) - initWAL
	- Load the most recent snapshot - loadLatestSnapshot
	- Replay WAL entries since the last snapshot - replayWAL
	- Start background tasks - startBackgroundTasks
2. Event publisher enqueues a message - PersistentQueue.Enqueue
	- Persistent queue writes the operation to the WAL - writeWALEntry
	- Event data is enqueued in the in-memory queue - MemoryQueue.Enqueue
4. Persistent queue updates the in-memory queue - applyOperation
5. Background task creates a snapshot - createSnapshot
	- Create a snapshot with the current state - QueueSnapshot
	- Write the snapshot to a temporary file - createSnapshot
	- Rename the temporary file to the final snapshot name - createSnapshot
	- Clean up old snapshots - cleanOldSnapshots
6. Background task cleans up old WAL files - cleanOldWALFiles
*/

type OperationType int

const (
	ENQUEUE OperationType = iota // 0
	DEQUEUE                      // 1
	ACK                          // 2
	NACK                         // 3
)

const snapshotTimeFormat = "2006-01-02T15:04:05.000Z"

type QueueOperation struct {
	Type      OperationType // ENQUEUE, DEQUEUE, ACK, NACK
	Timestamp time.Time     // When the operation occurred
	MessageID string        // ID of the message involved
	Data      []byte        // Message data (for ENQUEUE)
}

type WALEntry struct {
	SequenceID uint64         // Monotonically increasing ID
	Operation  QueueOperation // The actual operation
	Checksum   uint32         // CRC32 checksum for data integrity
}

type PersistentQueue struct {
	memQueue    *MemoryQueue // Underlying in-memory queue
	walFile     *os.File     // Write-ahead log file
	walOffset   int64        // Current position in WAL
	snapshotDir string       // Directory for snapshots
	lastSeqID   uint64       // Last used sequence ID
	config      *PersistConfig
	mu          sync.RWMutex
}
type QueueSnapshot struct {
	Version    uint64              // Snapshot version number
	Timestamp  time.Time           // When the snapshot was taken
	Messages   []*Message          // Current queue contents
	Processing map[string]*Message // In-progress messages
	Stats      QueueStats          // Current statistics
	LastSeqID  uint64              // Last WAL sequence ID included
}

type PersistConfig struct {
	DataDir          string        // Directory for WAL and snapshots
	SyncWrites       bool          // Whether to fsync after each write
	SnapshotInterval time.Duration // How often to create snapshots
	MaxWALSize       int64         // Maximum size of WAL before rotation
	RetainWALFiles   int           // Number of WAL files to keep
}

func init() {
	// Register types that will be encoded/decoded by gob
	gob.Register(QueueOperation{})
	gob.Register(OperationType(0))
}

func logOperation(op *QueueOperation, prefix string) {
	log.Printf("%s Operation Details:", prefix)
	log.Printf("  Type: %d", op.Type)
	log.Printf("  Timestamp: %v (UnixNano: %d)", op.Timestamp, op.Timestamp.UnixNano())
	log.Printf("  MessageID: %s", op.MessageID)
	if op.Data != nil {
		log.Printf("  Data (hex): %s", hex.EncodeToString(op.Data))
		log.Printf("  Data (len): %d", len(op.Data))
	} else {
		log.Printf("  Data: nil")
	}
}

// NewPersistentQueue creates a new persistent queue
func NewPersistentQueue(config *PersistConfig) (*PersistentQueue, error) {

	// Validate configuration
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	// Initialize directory structure
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, err
	}

	// Create queue instance
	pq := &PersistentQueue{
		memQueue:    NewMemoryQueue(DefaultConfig()).(*MemoryQueue),
		snapshotDir: filepath.Join(config.DataDir, "snapshots"),
		config:      config,
	}

	// Initialize WAL
	if err := pq.initWAL(); err != nil {
		return nil, err
	}

	// Load the most recent snapshot if it exists
	if err := pq.loadLatestSnapshot(); err != nil {
		return nil, fmt.Errorf("failed to load snapshot: %w", err)
	}

	// Replay WAL entries since the last snapshot
	if err := pq.replayWAL(); err != nil {
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	// Start background tasks
	pq.startBackgroundTasks()

	return pq, nil
}

// writes an operation to the WAL
func (pq *PersistentQueue) writeWALEntry(op *QueueOperation) error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	log.Printf("=== Writing WAL Entry ===")
	checksum := calculateChecksum(op)

	entry := WALEntry{
		SequenceID: pq.lastSeqID + 1,
		Operation:  *op,
		Checksum:   checksum,
	}

	log.Printf("WAL Entry created:")
	log.Printf("  SequenceID: %d", entry.SequenceID)
	log.Printf("  Checksum: %d (0x%08x)", checksum, checksum)

	data, err := entry.Marshal()
	if err != nil {
		return err
	}
	log.Printf("Marshaled entry size: %d bytes", len(data))

	return pq.writeEntryToFile(&entry, data)
}

// Helper function for actual file writing
func (pq *PersistentQueue) writeEntryToFile(entry *WALEntry, data []byte) error {
	length := uint32(len(data))
	log.Printf("Writing entry length: %d", length)

	if err := binary.Write(pq.walFile, binary.BigEndian, length); err != nil {
		return err
	}

	written, err := pq.walFile.Write(data)
	if err != nil {
		return err
	}
	log.Printf("Wrote %d bytes to WAL file", written)

	if pq.config.SyncWrites {
		if err := pq.walFile.Sync(); err != nil {
			return err
		}
		log.Printf("Synced to disk")
	}

	pq.lastSeqID++
	pq.walOffset += int64(4 + len(data))

	return nil
}

// initWAL initializes the write-ahead log
func (pq *PersistentQueue) initWAL() error {
	// Open or create WAL file
	walPath := filepath.Join(pq.config.DataDir, "wal")
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	// Seek to the end of the file
	offset, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	pq.walFile = file
	pq.walOffset = offset

	return nil
}

// Enqueue implementation with persistence
func (pq *PersistentQueue) Enqueue(ctx context.Context, data []byte) (string, error) {
	// Write to WAL first
	op := &QueueOperation{
		Type:      ENQUEUE,
		Timestamp: time.Now(),
		Data:      data,
	}

	if err := pq.writeWALEntry(op); err != nil {
		return "", err
	}

	// Then update memory queue
	return pq.memQueue.Enqueue(ctx, data)
}

func (pq *PersistentQueue) Dequeue(ctx context.Context, timeout time.Duration) (*Message, error) {
	msg, err := pq.memQueue.Dequeue(ctx, timeout)
	if err != nil {
		return nil, err
	}

	// Log the dequeue operation to WAL
	op := &QueueOperation{
		Type:      DEQUEUE,
		Timestamp: time.Now(),
		MessageID: msg.Id,
	}

	if err := pq.writeWALEntry(op); err != nil {
		// If we fail to write to WAL, we should try to put the message back
		pq.memQueue.Nack(ctx, msg.Id)
		return nil, err
	}

	return msg, nil
}

// GetStatus returns status of a message
func (pq *PersistentQueue) GetStatus(ctx context.Context, messageID string) (*MessageStatus, error) {
	// Forward to memory queue as the memory queue has the current state
	status, err := pq.memQueue.GetStatus(ctx, messageID)
	if err != nil {
		return nil, err
	}

	return status, nil
}

// Stats returns statistics about the queue
func (pq *PersistentQueue) Stats(ctx context.Context) (*QueueStats, error) {
	// Forward to memory queue as it maintains the stats
	return pq.memQueue.Stats(ctx)
}

func (pq *PersistentQueue) loadLatestSnapshot() error {
	// Find the most recent snapshot file
	snapshots, err := os.ReadDir(pq.snapshotDir)
	if err != nil {
		return fmt.Errorf("failed to read snapshot directory: %w", err)
	}

	if len(snapshots) == 0 {
		// No snapshots found, starting fresh
		return nil
	}

	// Find latest snapshot by parsing timestamp from filename
	var latestSnapshot os.DirEntry
	var latestTime time.Time

	for _, snapshot := range snapshots {
		timestamp, err := time.Parse(snapshotTimeFormat, strings.TrimSuffix(snapshot.Name(), ".snap"))
		if err != nil {
			continue
		}
		if timestamp.After(latestTime) {
			latestTime = timestamp
			latestSnapshot = snapshot
		}
	}

	if latestSnapshot == nil {
		return nil
	}

	// Read and deserialize snapshot
	snapshotPath := filepath.Join(pq.snapshotDir, latestSnapshot.Name())
	file, err := os.Open(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer file.Close()

	var snapshot QueueSnapshot
	if err := gob.NewDecoder(file).Decode(&snapshot); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	// Apply snapshot to memory queue
	var snapshotMessages list.List
	for _, msg := range snapshot.Messages {
		snapshotMessages.PushBack(msg)
	}
	pq.mu.Lock()
	pq.lastSeqID = snapshot.LastSeqID
	pq.memQueue.messages = &snapshotMessages
	pq.memQueue.processing = snapshot.Processing
	pq.memQueue.stats = snapshot.Stats
	pq.mu.Unlock()
	return nil
}

func (pq *PersistentQueue) replayWAL() error {
	log.Printf("=== Starting WAL Replay ===")

	if _, err := pq.walFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek WAL: %w", err)
	}

	reader := bufio.NewReader(pq.walFile)
	entryCount := 0

	for {
		log.Printf("\nReading WAL entry #%d", entryCount+1)

		// Read entry length
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				log.Printf("Reached end of WAL file after %d entries", entryCount)
				break
			}
			return fmt.Errorf("failed to read WAL entry length: %w", err)
		}
		log.Printf("Entry length: %d bytes", length)

		// Read entry data
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return fmt.Errorf("failed to read WAL entry data: %w", err)
		}
		log.Printf("Read %d bytes of entry data", len(data))

		// Unmarshal and verify entry
		var entry WALEntry
		if err := entry.Unmarshal(data); err != nil {
			log.Printf("Failed to unmarshal entry: %v", err)
			log.Printf("Raw data (hex): %s", hex.EncodeToString(data))
			return fmt.Errorf("failed to unmarshal WAL entry: %w", err)
		}

		log.Printf("Successfully unmarshaled entry:")
		log.Printf("  SequenceID: %d", entry.SequenceID)
		log.Printf("  Stored Checksum: %d (0x%08x)", entry.Checksum, entry.Checksum)

		calculatedChecksum := calculateChecksum(&entry.Operation)
		log.Printf("  Calculated Checksum: %d (0x%08x)", calculatedChecksum, calculatedChecksum)

		if entry.Checksum != calculatedChecksum {
			log.Printf("CHECKSUM MISMATCH!")
			logOperation(&entry.Operation, "Operation causing mismatch")
			return fmt.Errorf("WAL entry checksum mismatch at sequence %d", entry.SequenceID)
		}

		if entry.SequenceID <= pq.lastSeqID { // pq.lastSeqID is loaded from snapshot
			log.Printf("Skipping already applied entry %d (last applied: %d)",
				entry.SequenceID, pq.lastSeqID)
			continue
		}

		if err := pq.applyOperation(&entry.Operation); err != nil {
			return fmt.Errorf("failed to apply WAL operation: %w", err)
		}

		pq.lastSeqID = entry.SequenceID
		entryCount++
	}

	log.Printf("=== WAL Replay Complete ===")
	log.Printf("Processed %d entries", entryCount)

	offset, err := pq.walFile.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek WAL to end: %w", err)
	}
	pq.walOffset = offset
	log.Printf("Final WAL offset: %d", offset)

	return nil
}
func (pq *PersistentQueue) startBackgroundTasks() {
	// Start snapshot manager
	go func() {
		ticker := time.NewTicker(pq.config.SnapshotInterval)
		defer ticker.Stop()

		for range ticker.C {
			if err := pq.createSnapshot(); err != nil {
				// Log error but continue operating
				log.Printf("Failed to create snapshot: %v", err)
			}
		}
	}()

	// Start WAL cleaner
	go func() {
		// Clean old WAL files every hour
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()

		for range ticker.C {
			if err := pq.cleanOldWALFiles(); err != nil {
				// Log error but continue operating
				log.Printf("Failed to clean old WAL files: %v", err)
			}
		}
	}()
}

func validateConfig(config *PersistConfig) error {
	// First, check if config is nil
	if config == nil {
		return fmt.Errorf("configuration cannot be nil")
	}

	// Validate DataDir
	if config.DataDir == "" {
		return fmt.Errorf("DataDir cannot be empty")
	}

	// Check if DataDir path is absolute
	if !filepath.IsAbs(config.DataDir) {
		return fmt.Errorf("DataDir must be an absolute path")
	}

	// Validate SnapshotInterval
	if config.SnapshotInterval <= 0 {
		return fmt.Errorf("SnapshotInterval must be greater than 0")
	}

	// Ensure reasonable snapshot interval (not too frequent)
	if config.SnapshotInterval < time.Second {
		return fmt.Errorf("SnapshotInterval cannot be less than 1 second")
	}

	// Validate MaxWALSize
	if config.MaxWALSize <= 0 {
		return fmt.Errorf("MaxWALSize must be greater than 0")
	}

	// Ensure WAL size is reasonable (at least 1MB)
	if config.MaxWALSize < 1024*1024 {
		return fmt.Errorf("MaxWALSize must be at least 1MB")
	}

	// Validate RetainWALFiles
	if config.RetainWALFiles <= 0 {
		return fmt.Errorf("RetainWALFiles must be greater than 0")
	}

	// Ensure we keep at least 2 WAL files for safety
	if config.RetainWALFiles < 2 {
		return fmt.Errorf("RetainWALFiles must be at least 2 for safety")
	}

	// Try to create/check data directory permissions
	if err := checkDirectoryPermissions(config.DataDir); err != nil {
		return fmt.Errorf("data directory permission error: %w", err)
	}

	return nil
}

// Helper function to check directory permissions
func checkDirectoryPermissions(dir string) error {
	// Try to create the directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create a test file to verify write permissions
	_ = filepath.Join(dir, ".test_write")
	f, err := os.CreateTemp(dir, ".test_write")
	if err != nil {
		return fmt.Errorf("directory is not writable: %w", err)
	}

	// Clean up test file
	f.Close()
	os.Remove(f.Name())

	return nil
}

func (e *WALEntry) Unmarshal(data []byte) error {
	// Create a buffer from the input data
	buf := bytes.NewBuffer(data)

	// First, read the fixed-size fields using binary encoding
	// This is more efficient than gob encoding for simple numeric types
	if err := binary.Read(buf, binary.BigEndian, &e.SequenceID); err != nil {
		return fmt.Errorf("failed to read sequence ID: %w", err)
	}

	if err := binary.Read(buf, binary.BigEndian, &e.Checksum); err != nil {
		return fmt.Errorf("failed to read checksum: %w", err)
	}

	// Use gob decoder for the Operation field since it contains complex types
	decoder := gob.NewDecoder(buf)
	if err := decoder.Decode(&e.Operation); err != nil {
		return fmt.Errorf("failed to decode operation: %w", err)
	}

	return nil
}

// Marshal serializes a WALEntry to a byte slice
func (e *WALEntry) Marshal() ([]byte, error) {
	// Create a buffer to hold the serialized data
	buf := new(bytes.Buffer)

	// Write fixed-size fields using binary encoding
	if err := binary.Write(buf, binary.BigEndian, e.SequenceID); err != nil {
		return nil, fmt.Errorf("failed to write sequence ID: %w", err)
	}

	if err := binary.Write(buf, binary.BigEndian, e.Checksum); err != nil {
		return nil, fmt.Errorf("failed to write checksum: %w", err)
	}

	// Use gob encoder for the Operation field
	encoder := gob.NewEncoder(buf)
	if err := encoder.Encode(e.Operation); err != nil {
		return nil, fmt.Errorf("failed to encode operation: %w", err)
	}

	return buf.Bytes(), nil
}

func (pq *PersistentQueue) createSnapshot() error {
	// Take a lock to ensure consistency while creating snapshot
	pq.mu.Lock()
	defer pq.mu.Unlock()

	// Create snapshot directory if it doesn't exist
	if err := os.MkdirAll(pq.snapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	// Create snapshot with current state
	snapshot := QueueSnapshot{
		Version:    1, // For future format changes
		Timestamp:  time.Now(),
		LastSeqID:  pq.lastSeqID,
		Processing: make(map[string]*Message),
		Stats:      pq.memQueue.stats,
	}

	// Deep Copy messages from queue to snapshot
	// We need to convert from list.List to slice for serialization
	var messages []*Message
	for e := pq.memQueue.messages.Front(); e != nil; e = e.Next() {
		if msg, ok := e.Value.(*Message); ok {
			// Create complete copy of each message
			messages = append(messages, &Message{
				Id:        msg.Id,
				Data:      append([]byte(nil), msg.Data...), // Deep copy the data
				Timestamp: msg.Timestamp,
				Attempts:  msg.Attempts,
			})
		}
	}
	snapshot.Messages = messages

	// Deep copy processing messages map
	for id, msg := range pq.memQueue.processing {
		snapshot.Processing[id] = &Message{
			Id:        msg.Id,
			Data:      append([]byte(nil), msg.Data...), // Deep copy of data
			Timestamp: msg.Timestamp,
			Attempts:  msg.Attempts,
		}
	}

	// Create a temporary file for the snapshot
	// This ensures atomic snapshot creation
	tmpFile, err := os.CreateTemp(pq.snapshotDir, ".snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary snapshot file: %w", err)
	}
	tmpName := tmpFile.Name()

	// Use a buffered writer for better performance
	bufWriter := bufio.NewWriter(tmpFile)
	encoder := gob.NewEncoder(bufWriter)

	// Write the snapshot
	if err := encoder.Encode(snapshot); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return fmt.Errorf("failed to encode snapshot: %w", err)
	}

	// Ensure all data is written to the buffer
	if err := bufWriter.Flush(); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return fmt.Errorf("failed to flush snapshot data: %w", err)
	}

	// Sync to ensure data is written to disk
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return fmt.Errorf("failed to sync snapshot file: %w", err)
	}

	// Close the file before renaming
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("failed to close snapshot file: %w", err)
	}

	// Generate final snapshot name with timestamp
	finalName := filepath.Join(pq.snapshotDir,
		time.Now().UTC().Format(snapshotTimeFormat)+".snap")

	// Atomically rename temporary file to final name
	if err := os.Rename(tmpName, finalName); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("failed to finalize snapshot: %w", err)
	}

	// Clean up old snapshots
	if err := pq.cleanOldSnapshots(); err != nil {
		// Log error but don't fail - this isn't critical
		log.Printf("Warning: failed to clean old snapshots: %v", err)
	}

	return nil
}

// Helper method to clean up old snapshots
func (pq *PersistentQueue) cleanOldSnapshots() error {
	// Keep only the 5 most recent snapshots
	const keepSnapshots = 5

	entries, err := os.ReadDir(pq.snapshotDir)
	if err != nil {
		return err
	}

	// Filter and sort snapshot files
	var snapshots []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".snap") {
			snapshots = append(snapshots, entry.Name())
		}
	}

	// Sort by timestamp (newest first)
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i] > snapshots[j]
	})

	// Remove older snapshots
	for i := keepSnapshots; i < len(snapshots); i++ {
		path := filepath.Join(pq.snapshotDir, snapshots[i])
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("failed to remove old snapshot %s: %w", path, err)
		}
	}

	return nil
}

// calculateChecksum computes a CRC32 checksum for a queue operation
func calculateChecksum(op *QueueOperation) uint32 {
	buf := new(bytes.Buffer)
	logOperation(op, "Calculating checksum for")

	// Write each field and log the buffer state after each write
	log.Printf("Building checksum buffer:")

	// Write Type
	if err := binary.Write(buf, binary.BigEndian, int32(op.Type)); err != nil {
		log.Printf("Error writing Type: %v", err)
	}
	log.Printf("After Type: %x", buf.Bytes())

	// Write Timestamp
	if err := binary.Write(buf, binary.BigEndian, op.Timestamp.UnixNano()); err != nil {
		log.Printf("Error writing Timestamp: %v", err)
	}
	log.Printf("After Timestamp: %x", buf.Bytes())

	// Write MessageID
	buf.WriteString(op.MessageID)
	log.Printf("After MessageID: %x", buf.Bytes())

	// Write Data
	if op.Data != nil {
		buf.Write(op.Data)
		log.Printf("After Data: %x", buf.Bytes())
	}

	finalBytes := buf.Bytes()
	checksum := crc32.ChecksumIEEE(finalBytes)

	log.Printf("Final buffer content (hex): %s", hex.EncodeToString(finalBytes))
	log.Printf("Calculated checksum: %d (0x%08x)", checksum, checksum)

	return checksum
}

func (pq *PersistentQueue) cleanOldWALFiles() error {

	// Take a read lock to ensure lastSeqID doesn't change during cleanup
	pq.mu.RLock()
	currentSeqID := pq.lastSeqID
	pq.mu.RUnlock()

	// First, we need to get a list of all WAL files in the directory
	entries, err := os.ReadDir(pq.config.DataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	// Create a slice to hold WAL file information
	type walFile struct {
		path     string
		sequence int       // Sequence number from filename
		modTime  time.Time // Last modification time
	}
	var walFiles []walFile

	// Identify and collect WAL files
	for _, entry := range entries {
		// Skip directories and non-WAL files
		if entry.IsDir() {
			continue
		}

		// Check if this is a WAL file by examining the name pattern
		// WAL files are named like "wal.1", "wal.2", etc.
		if !strings.HasPrefix(entry.Name(), "wal.") {
			continue
		}

		// Extract sequence number from filename
		seqStr := strings.TrimPrefix(entry.Name(), "wal.")
		seq, err := strconv.Atoi(seqStr)
		if err != nil {
			// Log malformed filename but continue processing
			log.Printf("Warning: malformed WAL filename: %s", entry.Name())
			continue
		}

		// Get file info for modification time
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("failed to get file info for %s: %w", entry.Name(), err)
		}

		walFiles = append(walFiles, walFile{
			path:     filepath.Join(pq.config.DataDir, entry.Name()),
			sequence: seq,
			modTime:  info.ModTime(),
		})
	}

	// Sort WAL files by sequence number (descending)
	sort.Slice(walFiles, func(i, j int) bool {
		return walFiles[i].sequence > walFiles[j].sequence
	})

	// Keep the current WAL file and the configured number of retained files
	numToKeep := pq.config.RetainWALFiles
	if len(walFiles) <= numToKeep {
		// Not enough files to clean up
		return nil
	}

	// Find the latest snapshot sequence ID to ensure we don't delete needed WAL files
	latestSnapshotSeq := int(currentSeqID)

	// Delete old WAL files, but ensure we keep files needed for recovery
	for i := numToKeep; i < len(walFiles); i++ {
		file := walFiles[i]

		// Check if this WAL file might be needed for recovery
		// We need to keep WAL files that contain entries after the latest snapshot
		if file.sequence > latestSnapshotSeq {
			continue
		}

		// Try to delete the file
		if err := os.Remove(file.path); err != nil {
			// Log error but continue with other files
			log.Printf("Warning: failed to delete old WAL file %s: %v", file.path, err)
			continue
		}
	}

	return nil
}

func (pq *PersistentQueue) applyOperation(op *QueueOperation) error {
	// Create a background context since this is during recovery
	//ctx := context.Background()

	switch op.Type {
	case ENQUEUE:
		// For enqueue, we directly manipulate the memory queue
		// to bypass WAL writing (since we're replaying from WAL)
		if len(op.Data) > pq.memQueue.config.MaxMessageSize {
			return fmt.Errorf("message exceeds maximum size")
		}

		msg := &Message{
			Id:        op.MessageID,
			Data:      make([]byte, len(op.Data)),
			Timestamp: op.Timestamp,
			Attempts:  0,
		}
		copy(msg.Data, op.Data)

		pq.mu.Lock()
		pq.memQueue.messages.PushBack(msg)
		pq.memQueue.stats.Size++
		pq.memQueue.stats.TotalEnqueued++
		pq.mu.Unlock()
	case DEQUEUE:
		pq.mu.Lock()
		// Find the message in the queue
		for e := pq.memQueue.messages.Front(); e != nil; e = e.Next() {
			msg := e.Value.(*Message)
			if msg.Id == op.MessageID {
				// Remove from queue and add to processing
				pq.memQueue.messages.Remove(e)
				msg.Attempts++
				pq.memQueue.processing[msg.Id] = msg
				pq.memQueue.stats.Size--
				break
			}
		}
		pq.mu.Unlock()

	case ACK:
		pq.mu.Lock()
		// Remove from processing map and update stats
		if _, exists := pq.memQueue.processing[op.MessageID]; exists {
			delete(pq.memQueue.processing, op.MessageID)
			pq.memQueue.stats.TotalDequeued++
		}
		pq.mu.Unlock()

	case NACK:
		pq.mu.Lock()
		// Find message in processing map
		if msg, exists := pq.memQueue.processing[op.MessageID]; exists {
			delete(pq.memQueue.processing, op.MessageID)

			// Check retry limit
			if msg.Attempts >= pq.memQueue.config.RetryLimit {
				// Message exceeded retry limit, don't requeue
				pq.memQueue.stats.TotalDequeued++
			} else {
				// Requeue the message
				pq.memQueue.messages.PushBack(msg)
				pq.memQueue.stats.Size++
			}
		}
		pq.mu.Unlock()

	default:
		return fmt.Errorf("unknown operation type: %v", op.Type)
	}

	return nil
}

// Close closes the queue, ensuring all data is persisted
func (pq *PersistentQueue) Close() error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	// Create final snapshot
	if err := pq.createSnapshot(); err != nil {
		return err
	}

	// Close WAL file
	if err := pq.walFile.Close(); err != nil {
		return err
	}

	// Close memory queue
	return pq.memQueue.Close()
}

func (pq *PersistentQueue) Ack(ctx context.Context, messageID string) error {
	// Write ACK operation to WAL first
	op := &QueueOperation{
		Type:      ACK,
		Timestamp: time.Now(),
		MessageID: messageID,
	}

	if err := pq.writeWALEntry(op); err != nil {
		return err
	}

	// Then update memory queue
	return pq.memQueue.Ack(ctx, messageID)
}

// Nack indicates a message processing failure
func (pq *PersistentQueue) Nack(ctx context.Context, messageID string) error {
	// Write NACK operation to WAL first
	op := &QueueOperation{
		Type:      NACK,
		Timestamp: time.Now(),
		MessageID: messageID,
	}

	if err := pq.writeWALEntry(op); err != nil {
		return err
	}

	// Then update memory queue
	return pq.memQueue.Nack(ctx, messageID)
}

// Size returns the number of messages in the queue
func (pq *PersistentQueue) Size() int {
	// Forward to memory queue since it maintains the current state
	return pq.memQueue.Size()
}

// Peek returns the next message without removing it
func (pq *PersistentQueue) Peek(ctx context.Context) (*Message, error) {
	// Forward directly to memory queue since Peek doesn't modify state
	return pq.memQueue.Peek(ctx)
}
