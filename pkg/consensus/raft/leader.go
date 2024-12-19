// node.go
package raft

import (
	pb "DistributedQueue/api/proto/pb/queue"
	"DistributedQueue/api/proto/pb/raft"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"sync"
	"time"

	"DistributedQueue/pkg/queue"
)

var (
	ErrNotLeader    = fmt.Errorf("not the leader")
	ErrQueueFull    = fmt.Errorf("queue is full")
	ErrQueueEmpty   = fmt.Errorf("queue is empty")
	ErrTermMismatch = fmt.Errorf("term mismatch")
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// Config holds node configuration
type Config struct {
	NodeID           string
	Address          string
	PeerAddresses    []string
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
}

// CommandType represents different queue operations
type CommandType int

const (
	CmdEnqueue CommandType = iota
	CmdDequeue
	CmdAck
	CmdNack
)

// Command represents an operation to be replicated
type Command struct {
	Type      CommandType
	Data      []byte
	MessageID string
	Timestamp time.Time
}

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term    int64
	Index   int64
	Command Command
}

// Node represents a Raft consensus node
type Node struct {
	raft.UnimplementedRaftServiceServer
	config *Config
	queue  queue.Queue

	// Persistent state
	currentTerm int64
	votedFor    string
	log         []LogEntry

	// Volatile state
	state       NodeState
	commitIndex int64
	lastApplied int64
	lastContact time.Time
	leaderId    string

	// Leader state
	nextIndex  map[string]int64
	matchIndex map[string]int64

	// Node management
	peers    map[string]*PeerConnection
	stateCh  chan NodeState
	stopCh   chan struct{}
	commitCh chan CommitEvent

	// Synchronization
	mu sync.RWMutex
}

func (n *Node) SetQueue(q queue.Queue) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.queue = q
}

// GetStats returns statistics about the queue and Raft node.
func (n *Node) GetStats() (map[string]interface{}, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Example statistics
	stats := map[string]interface{}{
		"size":          n.queue.Size(), // Assuming `queue.Queue` has a `Size()` method
		"totalEnqueued": len(n.log),     // Total number of log entries
		"totalDequeued": n.lastApplied,  // Number of applied logs
		"currentTerm":   n.currentTerm,
		"commitIndex":   n.commitIndex,
		"state":         n.state, // Leader, Follower, etc.
	}

	return stats, nil
}

type CommitEvent struct {
	Index  int64
	Result interface{}
	Error  error
}

type AppendEntriesRequest struct {
	Term         int64
	LeaderID     string
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	CommitIndex  int64
}

type AppendEntriesResponse struct {
	Term    int64
	Success bool
}

type RequestVoteRequest struct {
	Term         int64
	CandidateID  string
	LastLogIndex int64
	LastLogTerm  int64
}

type RequestVoteResponse struct {
	Term        int64
	VoteGranted bool
}

// NewNode creates a new Raft node
func NewNode(config *Config) *Node {
	return &Node{
		config:     config,
		state:      Follower,
		peers:      make(map[string]*PeerConnection),
		nextIndex:  make(map[string]int64),
		matchIndex: make(map[string]int64),
		stateCh:    make(chan NodeState, 1),
		stopCh:     make(chan struct{}),
		commitCh:   make(chan CommitEvent, 100),
		log:        make([]LogEntry, 0),
	}
}

// Start initializes and starts the node
func (n *Node) Start() error {
	// Initialize peer connections
	if err := n.connectToPeers(); err != nil {
		return fmt.Errorf("failed to connect to peers: %w", err)
	}

	// Start main loop
	go n.run()

	return nil
}

func (n *Node) run() {
	for {
		select {
		case <-n.stopCh:
			return
		case state := <-n.stateCh:
			log.Printf("Raft node state changed: %v", state)
		default:
			// Handle states
			fmt.Println("Current State ", n.getState())
			switch n.getState() {
			case Follower:
				n.runFollower()
			case Candidate:
				n.runCandidate()
			case Leader:
				n.runLeader()
			}
		}
	}
}

func (n *Node) runFollower() {
	timeout := n.randomTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-timer.C:
			n.startElection()
			return
		}
	}
}

func (n *Node) runCandidate() {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.config.NodeID
	currentTerm := n.currentTerm
	n.mu.Unlock()

	votes := 1                     // Vote for self
	totalNodes := len(n.peers) + 1 // +1 for self
	votesNeeded := (len(n.peers)+1)/2 + 1
	fmt.Printf("Now node is candidate, Total Nodes %d, votesNeeded %d", totalNodes, votesNeeded)

	// For single node, we already have enough votes (self-vote)
	//if totalNodes == 1 {
	//	n.becomeLeader()
	//	return
	//}

	// Prepare vote request
	lastLogIndex, lastLogTerm := n.getLastLogInfo()
	req := RequestVoteRequest{
		Term:         currentTerm,
		CandidateID:  n.config.NodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// Collect votes from peers
	var wg sync.WaitGroup
	votesCh := make(chan bool, len(n.peers))

	for _, peer := range n.peers {
		wg.Add(1)
		go func(p *PeerConnection) {
			defer wg.Done()
			resp, err := p.RequestVote(req)
			if err != nil {
				votesCh <- false
				return
			}
			fmt.Println("Response ", resp)
			if resp.Term > currentTerm {
				n.stepDown(resp.Term)
				votesCh <- false
				return
			}
			votesCh <- resp.VoteGranted
		}(peer)
	}

	// Wait for votes
	go func() {
		wg.Wait()
		close(votesCh)
	}()

	// Count votes
	for granted := range votesCh {
		if granted {
			votes++
			if votes >= votesNeeded {
				n.becomeLeader()
				return
			}
		}
	}

	// If not enough votes, return to follower state
	n.setState(Follower)
}

func (n *Node) runLeader() {
	// Initialize leader state
	n.initLeaderState()

	ticker := time.NewTicker(n.config.HeartbeatTimeout)
	defer ticker.Stop()

	// Send initial heartbeats
	n.sendHeartbeats()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.sendHeartbeats()
		}
	}
}

// ProposeCommand is the main entry point for clients to propose new commands
func (n *Node) ProposeCommand(ctx context.Context, cmdType CommandType, data []byte) (interface{}, error) {
	if !n.IsLeader() {
		return nil, ErrNotLeader
	}

	cmd := Command{
		Type:      cmdType,
		Data:      data,
		Timestamp: time.Now(),
	}

	entry := LogEntry{
		Term:    n.currentTerm,
		Command: cmd,
	}

	index := n.appendLog(entry)
	commitTimeout := time.After(10 * time.Second) // Timeout after 10 seconds

	for {
		select {
		case commit := <-n.commitCh:
			if commit.Index == index {
				// Handle committed result
				if commit.Error != nil {
					return nil, commit.Error
				}
				return commit.Result, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-commitTimeout:
			return nil, fmt.Errorf("commit timed out for log entry at index %d", index)
		}
	}
}

func (n *Node) appendLog(entry LogEntry) int64 {
	n.mu.Lock()
	defer n.mu.Unlock()

	entry.Index = int64(len(n.log))
	n.log = append(n.log, entry)

	// Trigger replication to followers
	go n.replicateLog()

	return entry.Index
}

func (n *Node) replicateLog() {
	for peerID, peer := range n.peers {
		go func(id string, p *PeerConnection) {
			n.mu.RLock()
			prevIndex := n.nextIndex[id] - 1
			entries := n.log[n.nextIndex[id]:]
			term := n.currentTerm
			n.mu.RUnlock()

			req := AppendEntriesRequest{
				Term:         term,
				LeaderID:     n.config.NodeID,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  n.getTermForIndex(prevIndex),
				Entries:      entries,
				CommitIndex:  n.commitIndex,
			}

			resp, err := p.AppendEntries(req)
			if err != nil {
				return
			}

			if resp.Success {
				n.mu.Lock()
				n.nextIndex[id] = prevIndex + int64(len(entries)) + 1
				n.matchIndex[id] = n.nextIndex[id] - 1
				n.tryAdvanceCommitIndex()
				n.mu.Unlock()
			} else if resp.Term > term {
				n.stepDown(resp.Term)
			} else {
				// Log inconsistency, decrement nextIndex and retry
				n.mu.Lock()
				if n.nextIndex[id] > 0 {
					n.nextIndex[id]--
				}
				n.mu.Unlock()
			}
		}(peerID, peer)
	}
}

func (n *Node) tryAdvanceCommitIndex() {
	// Find the highest log entry that has been replicated to a majority of nodes
	for i := n.commitIndex + 1; i < int64(len(n.log)); i++ {
		if n.log[i].Term == n.currentTerm {
			replicatedCount := 1
			for _, matchIndex := range n.matchIndex {
				if matchIndex >= i {
					replicatedCount++
				}
			}
			if replicatedCount > len(n.peers)/2 {
				n.commitIndex = i
				go n.applyCommitted()
			}
		}
	}
}

// Helper methods

func (n *Node) getTermForIndex(index int64) int64 {
	if index < 0 || index >= int64(len(n.log)) {
		return 0
	}
	return n.log[index].Term
}

func (n *Node) getLastLogInfo() (index int64, term int64) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if len(n.log) > 0 {
		lastLog := n.log[len(n.log)-1]
		return lastLog.Index, lastLog.Term
	}
	return -1, 0
}

func (n *Node) randomTimeout() time.Duration {
	min := n.config.ElectionTimeout
	max := min + min/2
	return min + time.Duration(rand.Int63n(int64(max-min)))
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = Leader
	n.leaderId = n.config.NodeID
	n.initLeaderState()
}

func (n *Node) stepDown(term int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.currentTerm = term
	n.state = Follower
	n.votedFor = ""
}

func (n *Node) setState(state NodeState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = state
}

func (n *Node) getState() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state == Leader
}

func (n *Node) initLeaderState() {
	lastLogIndex := int64(len(n.log) - 1)
	for peerID := range n.peers {
		n.nextIndex[peerID] = lastLogIndex + 1
		n.matchIndex[peerID] = 0
	}
}

// Missing methods for node.go

// startElection initiates the election process
func (n *Node) startElection() {
	n.setState(Candidate)
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.config.NodeID
	currentTerm := n.currentTerm
	n.mu.Unlock()

	// Prepare vote request
	lastLogIndex, lastLogTerm := n.getLastLogInfo()
	req := RequestVoteRequest{
		Term:         currentTerm,
		CandidateID:  n.config.NodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votes := 1 // Vote for self
	totalNodes := len(n.peers) + 1
	votesNeeded := (len(n.peers)+1)/2 + 1

	// For single node, become leader immediately
	if totalNodes == 1 {
		n.becomeLeader()
		return
	}

	// Request votes from all peers
	var wg sync.WaitGroup
	votesCh := make(chan bool, len(n.peers))

	for _, peer := range n.peers {
		wg.Add(1)
		go func(p *PeerConnection) {
			defer wg.Done()
			resp, err := p.RequestVote(req)
			if err != nil {
				votesCh <- false
				return
			}
			if resp.Term > currentTerm {
				n.stepDown(resp.Term)
				votesCh <- false
				return
			}
			votesCh <- resp.VoteGranted
		}(peer)
	}

	// Wait for votes
	go func() {
		wg.Wait()
		close(votesCh)
	}()

	// Count votes
	for granted := range votesCh {
		fmt.Println("Granted ", granted)
		if granted {
			votes++
			if votes >= votesNeeded {
				n.becomeLeader()
				return
			}
		}
	}

	// If not enough votes, return to follower state
	n.setState(Follower)
}

// sendHeartbeats sends heartbeat messages to all peers
func (n *Node) sendHeartbeats() {
	n.mu.RLock()
	term := n.currentTerm
	n.mu.RUnlock()

	for _, peer := range n.peers {
		go func(p *PeerConnection) {
			// Create empty AppendEntries as heartbeat
			req := AppendEntriesRequest{
				Term:         term,
				LeaderID:     n.config.NodeID,
				PrevLogIndex: -1,
				Entries:      nil,
				CommitIndex:  n.commitIndex,
			}

			resp, err := p.AppendEntries(req)
			if err != nil {
				return
			}

			if resp.Term > term {
				n.stepDown(resp.Term)
			}
		}(peer)
	}
}

// applyCommitted applies committed log entries to the state machine
func (n *Node) applyCommitted() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]

		result, err := n.applyCommand(&entry.Command)

		// Notify waiting clients through the commit channel
		n.commitCh <- CommitEvent{
			Index:  n.lastApplied,
			Result: result,
			Error:  err,
		}
	}
}

// applyCommand applies a single command to the state machine
func (n *Node) applyCommand(cmd *Command) (interface{}, error) {
	ctx := context.Background()

	switch cmd.Type {
	case CmdEnqueue:
		return n.queue.Enqueue(ctx, cmd.Data)
	case CmdDequeue:
		return n.queue.Dequeue(ctx, 0)
	case CmdAck:
		return nil, n.queue.Ack(ctx, cmd.MessageID)
	case CmdNack:
		return nil, n.queue.Nack(ctx, cmd.MessageID)
	default:
		return nil, fmt.Errorf("unknown command type: %v", cmd.Type)
	}
}

// handleAppendEntries processes AppendEntries RPC from leader
func (n *Node) handleAppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	// 1. Reply false if term < currentTerm
	if req.Term < n.currentTerm {
		return resp, nil
	}

	// If we get a valid AppendEntries, recognize leader
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.state = Follower
		n.votedFor = ""
	}

	n.leaderId = req.LeaderID
	n.lastContact = time.Now()

	// 2. Reply false if log doesn't contain entry at prevLogIndex with prevLogTerm
	if req.PrevLogIndex >= 0 {
		if req.PrevLogIndex >= int64(len(n.log)) {
			return resp, nil
		}
		if n.log[req.PrevLogIndex].Term != req.PrevLogTerm {
			return resp, nil
		}
	}

	// 3. If existing entries conflict with new ones, delete existing entries and all that follow
	if len(req.Entries) > 0 {
		newIndex := req.PrevLogIndex + 1
		for i, entry := range req.Entries {
			if newIndex+int64(i) < int64(len(n.log)) {
				if n.log[newIndex+int64(i)].Term != entry.Term {
					n.log = n.log[:newIndex+int64(i)]
					n.log = append(n.log, req.Entries[i:]...)
					break
				}
			} else {
				n.log = append(n.log, req.Entries[i:]...)
				break
			}
		}
	}

	// 4. Update commit index
	if req.CommitIndex > n.commitIndex {
		n.commitIndex = min(req.CommitIndex, int64(len(n.log)-1))
		go n.applyCommitted()
	}

	resp.Success = true
	return resp, nil
}

// handleRequestVote processes RequestVote RPC from candidate
func (n *Node) handleRequestVote(req *RequestVoteRequest) (*RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// Reject if term is older
	if req.Term < n.currentTerm {
		return resp, nil
	}

	// If term is newer, update term and become follower
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.state = Follower
		n.votedFor = ""
	}

	// Check if we can vote for this candidate
	lastLogIndex, lastLogTerm := n.getLastLogInfo()
	canVote := n.votedFor == "" || n.votedFor == req.CandidateID
	logIsUpToDate := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if canVote && logIsUpToDate {
		resp.VoteGranted = true
		n.votedFor = req.CandidateID
		n.lastContact = time.Now()
	}

	return resp, nil
}

// Helper function for min value
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// GetLeader returns the current leader's ID
func (n *Node) GetLeader() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderId
}

// GetCurrentTerm returns the current term
func (n *Node) GetCurrentTerm() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

// Stop gracefully shuts down the node
func (n *Node) Stop() {
	close(n.stopCh)
}

// PeerConnection represents a connection to a peer node
type PeerConnection struct {
	address string
	client  *grpc.ClientConn
	mu      sync.RWMutex
}

func NewPeerConnection(address string) (*PeerConnection, error) {
	// Create gRPC connection with default options
	conn, err := grpc.Dial(
		address,
		grpc.WithInsecure(), // For development only
		grpc.WithBlock(),    // Block until connection is ready
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer %s: %w", address, err)
	}

	return &PeerConnection{
		address: address,
		client:  conn,
	}, nil
}

func (p *PeerConnection) AppendEntries(req AppendEntriesRequest) (*AppendEntriesResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Implement the RPC call using your gRPC client
	// This is a placeholder for the actual gRPC call
	response := &AppendEntriesResponse{
		Term:    req.Term,
		Success: true,
	}

	return response, nil
}

func (n *Node) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	// Convert protobuf request to internal type
	internalReq := RequestVoteRequest{
		Term:         req.Term,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}

	// Call your existing handling logic
	resp, err := n.handleRequestVote(&internalReq)
	if err != nil {
		return nil, err
	}

	// Convert internal response to protobuf
	return &pb.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (p *PeerConnection) RequestVote(req RequestVoteRequest) (*RequestVoteResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Convert our internal RequestVoteRequest to the protobuf version
	pbReq := &raft.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}

	// Create the gRPC client
	client := raft.NewRaftServiceClient(p.client)

	// Make the actual RPC call
	pbResp, err := client.RequestVote(ctx, pbReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send RequestVote RPC: %w", err)
	}

	// Convert protobuf response back to our internal type
	response := &RequestVoteResponse{
		Term:        pbResp.Term,
		VoteGranted: pbResp.VoteGranted,
	}

	return response, nil
}
func (n *Node) connectToPeers() error {
	if len(n.config.PeerAddresses) == 0 {
		log.Printf("[%s] Starting as a single-node cluster", n.config.NodeID)
		return nil
	}

	var errs []error
	for _, peerAddr := range n.config.PeerAddresses {
		// Skip if the address is our own
		if peerAddr == n.config.Address {
			continue
		}

		// Create new peer connection
		peer, err := NewPeerConnection(peerAddr)
		if err != nil {
			log.Printf("[%s] Failed to connect to peer %s: %v", n.config.NodeID, peerAddr, err)
			errs = append(errs, err)
			continue
		}

		// Store the peer connection
		n.mu.Lock()
		n.peers[peerAddr] = peer
		n.mu.Unlock()

		log.Printf("[%s] Connected to peer: %s", n.config.NodeID, peerAddr)
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to connect to some peers: %v", errs)
	}

	log.Printf("[%s] Successfully connected to all %d peers", n.config.NodeID, len(n.peers))
	return nil
}
