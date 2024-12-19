// main.go
package main

import (
	"DistributedQueue/pkg/queue"
	"DistributedQueue/pkg/transport/grpc"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	// Add command line flags
	nodeID := flag.String("node", "", "Node ID (node-1, node-2, or node-3)")
	port := flag.Int("port", 50051, "Port number (50051, 50052, or 50053)")
	peerAddrs := flag.String("peers", "", "Comma-separated list of peer addresses")
	flag.Parse()

	// Validate node ID
	if *nodeID == "" {
		log.Fatal("Please provide a node ID using -node flag")
	}

	// Create data directory specific to this node
	dataDir := fmt.Sprintf("/Users/145subhajit/GolandProjects/DistributedQueue/data/%s", *nodeID)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Create persistent queue
	persistConfig := &queue.PersistConfig{
		DataDir:          dataDir,
		SyncWrites:       true,
		SnapshotInterval: 15 * time.Second,
		MaxWALSize:       100 * 1024 * 1024, // 100MB
		RetainWALFiles:   3,
	}

	persistentQueue, err := queue.NewPersistentQueue(persistConfig)
	if err != nil {
		log.Fatalf("Failed to create persistent queue: %v", err)
	}

	// Create server config with Raft settings
	address := fmt.Sprintf("localhost:%d", *port)
	config := &grpc.ServerConfig{
		NodeID:            *nodeID,
		Address:           address,
		PeerAddresses:     parsePeerAddresses(*peerAddrs),
		ElectionTimeout:   1 * time.Second,
		HeartbeatInterval: 100 * time.Millisecond,
		Queue:             persistentQueue,
	}

	// Create and start server
	server, err := grpc.NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Handle shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		log.Printf("Starting %s on %s", *nodeID, address)
		if err := server.Start(); err != nil {
			errChan <- err
		}
	}()

	// Wait for shutdown signal or error
	select {
	case err := <-errChan:
		log.Fatalf("Server error: %v", err)
	case sig := <-sigChan:
		log.Printf("Received signal %v, shutting down...", sig)
		server.Stop()
	}
}

// Helper function to parse peer addresses
func parsePeerAddresses(peers string) []string {
	if peers == "" {
		return nil
	}
	return strings.Split(peers, ",")
}
