// pkg/transport/grpc/stream.go

package grpc

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"log"
	"time"
)

// wrappedServerStream wraps grpc.ServerStream and implements the grpc.ServerStream interface
type wrappedServerStream struct {
	grpc.ServerStream
	startTime  time.Time
	msgCount   int
	lastAccess time.Time
	methodName string
}

func newWrappedServerStream(ss grpc.ServerStream, methodName string) grpc.ServerStream {
	return &wrappedServerStream{
		ServerStream: ss,
		startTime:    time.Now(),
		lastAccess:   time.Now(),
		methodName:   methodName,
	}
}

// RecvMsg intercepts messages being received
func (w *wrappedServerStream) RecvMsg(m interface{}) error {
	w.lastAccess = time.Now()
	w.msgCount++
	err := w.ServerStream.RecvMsg(m)
	if err != nil {
		log.Printf("Error receiving message: %v", err)
		return err
	}
	log.Printf("Stream received message #%d after %v",
		w.msgCount, time.Since(w.startTime))
	return nil
}

// SendMsg intercepts messages being sent
func (w *wrappedServerStream) SendMsg(m interface{}) error {
	w.lastAccess = time.Now()
	w.msgCount++
	err := w.ServerStream.SendMsg(m)
	if err != nil {
		log.Printf("Error sending message: %v", err)
		return err
	}
	log.Printf("Stream sent message #%d after %v",
		w.msgCount, time.Since(w.startTime))
	return nil
}

// Context returns the context for this stream
func (w *wrappedServerStream) Context() context.Context {
	return w.ServerStream.Context()
}

// SetHeader sets the header metadata
func (w *wrappedServerStream) SetHeader(md metadata.MD) error {
	return w.ServerStream.SetHeader(md)
}

// SendHeader sends the header metadata
func (w *wrappedServerStream) SendHeader(md metadata.MD) error {
	return w.ServerStream.SendHeader(md)
}

// SetTrailer sets the trailer metadata
func (w *wrappedServerStream) SetTrailer(md metadata.MD) {
	w.ServerStream.SetTrailer(md)
}

// Method returns the streaming RPC's method name
func (w *wrappedServerStream) Method() string {
	return w.methodName
}
