package queue

import (
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

type MemoryQueue struct {
	config     *Config
	messages   *list.List          //The List type represents a doubly linked list
	processing map[string]*Message // Messages currently being processed
	stats      QueueStats
	mu         sync.RWMutex
	closed     bool
	waitChan   chan struct{} // For blocking dequeue operations
}

func generateId() string {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func (m *MemoryQueue) Enqueue(ctx context.Context, data []byte) (string, error) {
	if m.closed {
		return "", ErrQueueIsClosed
	}
	if len(data) > m.config.MaxMessageSize {
		return "", ErrInvalidMessage
	}
	if len(data) == 0 {
		return "", ErrInvalidMessage
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.messages.Len() >= m.config.MaxSize {
		return "", ErrQueueIsFull
	}
	msg := &Message{
		Id:        generateId(),
		Data:      make([]byte, len(data)),
		Timestamp: time.Now(),
		Attempts:  0,
	}
	copy(msg.Data, data)
	m.messages.PushBack(msg)
	m.stats.Size++
	m.stats.TotalEnqueued++

	// Notify any waiting dequeue operation
	/*	If the `m.waitChan` channel is full (i.e., no goroutine is currently waiting to receive from it),
		the send operation would block. The `default` case prevents this blocking.
		If the send on `m.waitChan` cannot proceed immediately,
		the `default` case is selected and executed */
	select {
	case m.waitChan <- struct{}{}:
	default:
	}

	return msg.Id, nil
}

func (m *MemoryQueue) Dequeue(ctx context.Context, timeout time.Duration) (*Message, error) {
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, ErrQueueIsClosed
		}
		// Check if there are any messages in the queue
		if elem := m.messages.Front(); elem != nil {
			msg := elem.Value.(*Message) // Get message from front
			m.messages.Remove(elem)      // Remove message from queue
			m.processing[msg.Id] = msg   // Add message to processing map
			msg.Attempts++
			m.stats.Size--
			m.stats.TotalDequeued++
			m.mu.Unlock()
			return msg, nil
		}
		// 4. Handle non-blocking case
		if timeout == 0 {
			m.mu.Unlock()
			return nil, ErrQueueIsEmpty
		}
		// 5. Handle blocking case
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
			// Timeout reached
			return nil, ErrQueueIsEmpty
		case <-m.waitChan:
			// Wake up from waiting
			continue
		}
	}
}

func (m *MemoryQueue) Peek(ctx context.Context) (*Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, ErrQueueIsClosed
	}
	if elem := m.messages.Front(); elem != nil {
		msg := elem.Value.(*Message)
		return msg, nil
	}
	return nil, ErrQueueIsEmpty
}

func (m *MemoryQueue) Ack(ctx context.Context, messageID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrQueueIsClosed
	}

	if _, ok := m.processing[messageID]; ok {
		delete(m.processing, messageID)
		m.stats.TotalDequeued++
		return nil
	}
	return ErrInvalidMessage
}

func (m *MemoryQueue) Nack(ctx context.Context, messageID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrQueueIsClosed
	}
	msg, exists := m.processing[messageID]
	if !exists {
		return ErrInvalidMessage
	}
	if msg.Attempts >= m.config.RetryLimit {
		delete(m.processing, messageID)
		return nil
	}

	delete(m.processing, messageID)
	m.messages.PushBack(msg)
	m.stats.Size++
	return nil
}

func (m *MemoryQueue) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.messages.Len()
}

func (m *MemoryQueue) GetStatus(ctx context.Context, messageID string) (*MessageStatus, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MemoryQueue) Stats(ctx context.Context) (*QueueStats, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MemoryQueue) Close() error {
	//TODO implement me
	panic("implement me")
}

func NewMemoryQueue(config *Config) Queue {
	if config == nil {
		config = DefaultConfig()
	}
	return &MemoryQueue{
		config:     config,
		messages:   list.New(),
		processing: make(map[string]*Message),
		waitChan:   make(chan struct{}),
	}
}
