package grpc

import (
	pb "DistributedQueue/api/proto/pb/queue"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// QueueClient wraps the gRPC client functionality
type QueueClient struct {
	clientID string
	conn     *grpc.ClientConn
	client   pb.QueueServiceClient
}

// NewQueueClient creates a new client instance
func NewQueueClient(address string, clientID string) (*QueueClient, error) {
	// Set up connection to server
	conn, err := grpc.Dial(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	return &QueueClient{
		clientID: clientID,
		conn:     conn,
		client:   pb.NewQueueServiceClient(conn),
	}, nil
}

// Close closes the client connection
func (c *QueueClient) Close() error {
	return c.conn.Close()
}

// Enqueue adds a message to the queue
func (c *QueueClient) Enqueue(ctx context.Context, data []byte) (string, error) {
	req := &pb.EnqueueRequest{
		ClientId: c.clientID,
		Data:     data,
	}

	resp, err := c.client.Enqueue(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to enqueue message: %v", err)
	}

	return resp.MessageId, nil
}

// Dequeue removes and returns a message from the queue
func (c *QueueClient) Dequeue(ctx context.Context, timeoutMs int64) (*pb.Message, error) {
	req := &pb.DequeueRequest{
		ClientId:  c.clientID,
		TimeoutMs: timeoutMs,
	}

	resp, err := c.client.Dequeue(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to dequeue message: %v", err)
	}

	return resp.Message, nil
}
