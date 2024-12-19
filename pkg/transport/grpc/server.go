// server.go
package grpc

import (
	"DistributedQueue/pkg/queue"
	"context"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"sync"
	"time"

	pb "DistributedQueue/api/proto/pb/queue"
	"DistributedQueue/pkg/consensus/raft"
	"google.golang.org/grpc"
)

// Server represents our gRPC server implementation
type Server struct {
	pb.UnimplementedQueueServiceServer
	raftNode   *raft.Node // Reference to Raft consensus node
	grpcServer *grpc.Server
	mu         sync.RWMutex
	clients    map[string]time.Time
	config     *ServerConfig
	shutdownCh chan struct{}
}

// ServerConfig holds the configuration for our gRPC server
type ServerConfig struct {
	NodeID            string
	Address           string
	TLSCert           string
	Queue             queue.Queue // Add this field
	TLSKey            string
	PeerAddresses     []string
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
}

// NewServer creates a new gRPC server instance
func NewServer(config *ServerConfig) (*Server, error) {
	if config == nil {
		return nil, fmt.Errorf("server configuration cannot be nil")
	}

	// Create Raft node configuration
	raftConfig := &raft.Config{
		NodeID:           config.NodeID,
		Address:          config.Address,
		PeerAddresses:    config.PeerAddresses,
		HeartbeatTimeout: config.HeartbeatInterval,
		ElectionTimeout:  config.ElectionTimeout,
	}

	// Initialize Raft node
	raftNode := raft.NewNode(raftConfig)
	raftNode.SetQueue(config.Queue) // Add this

	server := &Server{
		raftNode:   raftNode,
		clients:    make(map[string]time.Time),
		config:     config,
		shutdownCh: make(chan struct{}),
	}

	// Initialize the gRPC server with interceptors
	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(server.unaryInterceptor),
		grpc.StreamInterceptor(server.streamInterceptor),
	}

	// Add TLS if configured
	if config.TLSCert != "" && config.TLSKey != "" {
		creds, err := loadTLSCredentials(config.TLSCert, config.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		opts = append(opts, creds)
	}

	server.grpcServer = grpc.NewServer(opts...)
	pb.RegisterQueueServiceServer(server.grpcServer, server)

	return server, nil
}

// Start begins listening for gRPC requests
func (s *Server) Start() error {
	// Start the Raft node first
	if err := s.raftNode.Start(); err != nil {
		return fmt.Errorf("failed to start raft node: %w", err)
	}

	// Start listening for gRPC requests
	lis, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	log.Printf("Starting gRPC server on %s", s.config.Address)
	return s.grpcServer.Serve(lis)
}

// Stop gracefully shuts down the server
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close the shutdown channel to signal all listeners
	close(s.shutdownCh)

	// Graceful stop of the gRPC server
	log.Println("Shutting down gRPC server...")
	s.grpcServer.GracefulStop()

	// Stop the raft node gracefully
	log.Println("Stopping Raft node...")
	s.raftNode.Stop() // Ensure the Raft Node has a proper Stop method implementation
}

// Enqueue implements the queue service Enqueue RPC
func (s *Server) Enqueue(ctx context.Context, req *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
	if !s.raftNode.IsLeader() {
		// Forward to leader if we're not the leader
		resp, err := s.forwardToLeader(ctx, "Enqueue", req)
		if err != nil {
			return nil, err
		}
		return resp.(*pb.EnqueueResponse), nil
	}

	// Validate request
	if err := validateEnqueueRequest(req); err != nil {
		return nil, err
	}

	// Use Raft node to process the request
	messageID, err := s.raftNode.ProposeCommand(ctx, raft.CmdEnqueue, req.Data)
	if err != nil {
		return nil, convertError(err)
	}

	return &pb.EnqueueResponse{
		MessageId: messageID.(string),
		Success:   true,
	}, nil
}

// Dequeue implements the queue service Dequeue RPC
func (s *Server) Dequeue(ctx context.Context, req *pb.DequeueRequest) (*pb.DequeueResponse, error) {
	if !s.raftNode.IsLeader() {
		resp, err := s.forwardToLeader(ctx, "Dequeue", req)
		if err != nil {
			return nil, err
		}
		return resp.(*pb.DequeueResponse), nil
	}

	// Validate request
	if err := validateDequeueRequest(req); err != nil {
		return nil, err
	}

	// Use Raft node to process the request
	result, err := s.raftNode.ProposeCommand(ctx, raft.CmdDequeue, nil)
	if err != nil {
		return nil, convertError(err)
	}

	msg := result.(*raft.Message)
	return &pb.DequeueResponse{
		Message: convertMessage(msg),
		Success: true,
	}, nil
}

// Helper functions

func validateEnqueueRequest(req *pb.EnqueueRequest) error {
	if len(req.Data) == 0 {
		return status.Error(codes.InvalidArgument, "message data cannot be empty")
	}
	if req.ClientId == "" {
		return status.Error(codes.InvalidArgument, "client ID is required")
	}
	return nil
}

func validateDequeueRequest(req *pb.DequeueRequest) error {
	if req.ClientId == "" {
		return status.Error(codes.InvalidArgument, "client ID is required")
	}
	return nil
}

func convertMessage(msg *raft.Message) *pb.Message {
	return &pb.Message{
		Id:        msg.Id,
		Data:      msg.Data,
		Timestamp: msg.Timestamp.UnixNano(),
		Attempts:  int32(msg.Attempts),
	}
}

func convertError(err error) error {
	switch err {
	case raft.ErrNotLeader:
		return status.Error(codes.FailedPrecondition, "not the leader")
	case raft.ErrQueueFull:
		return status.Error(codes.ResourceExhausted, "queue is full")
	case raft.ErrQueueEmpty:
		return status.Error(codes.NotFound, "queue is empty")
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// Additional methods for server.go

// forwardToLeader forwards requests to the current leader
func (s *Server) forwardToLeader(ctx context.Context, method string, req interface{}) (interface{}, error) {
	leaderAddr := s.raftNode.GetLeader()
	if leaderAddr == "" {
		return nil, status.Error(codes.Unavailable, "no leader available")
	}

	var opts []grpc.DialOption
	if s.config.TLSCert != "" && s.config.TLSKey != "" {
		creds, err := credentials.NewClientTLSFromFile(s.config.TLSCert, "")
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to load TLS credentials: %v", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	conn, err := grpc.DialContext(ctx, leaderAddr, opts...)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "failed to connect to leader: %v", err)
	}
	defer conn.Close()

	client := pb.NewQueueServiceClient(conn)

	switch method {
	case "Enqueue":
		return client.Enqueue(ctx, req.(*pb.EnqueueRequest))
	case "Dequeue":
		return client.Dequeue(ctx, req.(*pb.DequeueRequest))
	default:
		return nil, status.Error(codes.Unimplemented, "method not implemented")
	}
}

// unaryInterceptor handles logging and metrics for unary RPCs
func (s *Server) unaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	clientID := extractClientID(ctx)

	// Log request
	log.Printf("Request: method=%s client=%s", info.FullMethod, clientID)

	// Update client activity
	if clientID != "" {
		s.updateClientActivity(clientID)
	}

	// Handle the request
	resp, err := handler(ctx, req)

	// Log completion
	duration := time.Since(start)
	if err != nil {
		log.Printf("Error: method=%s client=%s duration=%v error=%v",
			info.FullMethod, clientID, duration, err)
	} else {
		log.Printf("Success: method=%s client=%s duration=%v",
			info.FullMethod, clientID, duration)
	}

	return resp, err
}

// streamInterceptor handles logging and metrics for streaming RPCs
func (s *Server) streamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	clientID := extractClientID(ss.Context())

	// Initialize wrapped server stream with method name
	wrappedStream := newWrappedServerStream(ss, info.FullMethod)

	log.Printf("Stream started: method=%s client=%s", info.FullMethod, clientID)

	if clientID != "" {
		s.updateClientActivity(clientID)
	}

	err := handler(srv, wrappedStream)

	log.Printf("Stream ended: method=%s client=%s duration=%v, error=%v",
		info.FullMethod, clientID, time.Since(start), err)

	return err
}

// wrappedServerStream wraps grpc.ServerStream for interceptor functionality

// updateClientActivity updates the last seen time for a client
func (s *Server) updateClientActivity(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[clientID] = time.Now()
}

// loadTLSCredentials loads TLS credentials from cert and key files
func loadTLSCredentials(certFile, keyFile string) (grpc.ServerOption, error) {
	creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS credentials: %w", err)
	}
	return grpc.Creds(creds), nil
}

// extractClientID extracts client ID from context metadata
func extractClientID(ctx context.Context) string {
	// Implementation depends on how client ID is passed
	// Example using metadata:
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if ids := md.Get("client-id"); len(ids) > 0 {
		return ids[0]
	}
	return ""
}

// Additional methods for queue operations

// Ack implements the queue service Ack RPC
func (s *Server) Ack(ctx context.Context, req *pb.AckRequest) (*pb.AckResponse, error) {
	if !s.raftNode.IsLeader() {
		return nil, status.Error(codes.FailedPrecondition, "not the leader")
	}

	if req.MessageId == "" {
		return nil, status.Error(codes.InvalidArgument, "message ID is required")
	}

	_, err := s.raftNode.ProposeCommand(ctx, raft.CmdAck, []byte(req.MessageId))
	if err != nil {
		return nil, convertError(err)
	}

	return &pb.AckResponse{Success: true}, nil
}

// Nack implements the queue service Nack RPC
func (s *Server) Nack(ctx context.Context, req *pb.NackRequest) (*pb.NackResponse, error) {
	if !s.raftNode.IsLeader() {
		return nil, status.Error(codes.FailedPrecondition, "not the leader")
	}

	if req.MessageId == "" {
		return nil, status.Error(codes.InvalidArgument, "message ID is required")
	}

	_, err := s.raftNode.ProposeCommand(ctx, raft.CmdNack, []byte(req.MessageId))
	if err != nil {
		return nil, convertError(err)
	}

	return &pb.NackResponse{Success: true}, nil
}

// GetQueueStats implements the queue service GetQueueStats RPC
func (s *Server) GetQueueStats(ctx context.Context, req *pb.GetQueueStatsRequest) (*pb.GetQueueStatsResponse, error) {
	stats, err := s.raftNode.GetStats()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get queue stats: %v", err)
	}

	size, ok := stats["size"].(int)
	if !ok {
		return nil, status.Errorf(codes.Internal, "unexpected type for size")
	}

	enqueued, ok := stats["totalEnqueued"].(int)
	if !ok {
		return nil, status.Errorf(codes.Internal, "unexpected type for totalEnqueued")
	}

	dequeued, ok := stats["totalDequeued"].(int)
	if !ok {
		return nil, status.Errorf(codes.Internal, "unexpected type for totalDequeued")
	}

	return &pb.GetQueueStatsResponse{
		Size:          int32(size),
		TotalEnqueued: int32(enqueued),
		TotalDequeued: int32(dequeued),
	}, nil
}

// Interceptors and other helper methods remain the same...
