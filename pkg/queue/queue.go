package queue

import (
	"context"
	"errors"
	"time"
)

var (
	ErrQueueIsFull    = errors.New("queue is full")
	ErrQueueIsEmpty   = errors.New("queue is empty")
	ErrQueueIsClosed  = errors.New("queue is closed")
	ErrInvalidMessage = errors.New("invalid message")
)

type Message struct {
	Id        string
	Data      []byte
	Timestamp time.Time
	Attempts  int // Number of attempts to deliver the message
}

// MessageStatus represents the current status of a message
type MessageStatus struct {
	Id           string
	EnqueuedTime time.Time
	LastDelivery time.Time
	Attempts     int
}

// QueueStats contains statistics about the queue
type QueueStats struct {
	Size          int // Number of messages in the queue currently
	TotalEnqueued int // Total number of messages enqueued
	TotalDequeued int // Total number of messages dequeued
}

// Queue defines the interface for queue operations
type Queue interface {
	// Enqueue adds a message to the queue
	// Returns message ID and error if any
	Enqueue(ctx context.Context, data []byte) (string, error)
	// Dequeue removes and returns a message from the queue
	// If timeout > 0, waits up to timeout duration for a message to be available
	Dequeue(ctx context.Context, timeout time.Duration) (*Message, error)
	// Peek returns the next message without removing it
	Peek(ctx context.Context) (*Message, error)

	//Ack acknowledges a message as processed successfully
	Ack(ctx context.Context, messageID string) error

	//Nack acknowledges a message as processed unsuccessfully
	Nack(ctx context.Context, messageID string) error

	// GetStatus returns status of a message
	GetStatus(ctx context.Context, messageID string) (*MessageStatus, error)

	// Stats returns statistics about the queue
	Stats(ctx context.Context) (*QueueStats, error)
	// Close closes the queue
	Close() error
	Size() int
}

// Config holds queue configuration
type Config struct {
	MaxSize        int           // Maximum number of messages in queue
	MaxMessageSize int           // Maximum size of a single message in bytes
	RetryLimit     int           // Maximum number of delivery attempts
	RetryDelay     time.Duration // Delay between retry attempts
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		MaxSize:        10000,
		MaxMessageSize: 1 << 20, // 1MB - The expression 1 << 20 is a bitwise left shift operation that shifts the number 1 by 20 bits to the left, which is equivalent to (2^{20}) or 1,048,576 bytes (1MB).
		RetryLimit:     3,
		RetryDelay:     time.Second * 30,
	}
}
