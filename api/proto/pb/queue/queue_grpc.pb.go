// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.29.1
// source: api/proto/queue.proto

package queue

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	QueueService_Enqueue_FullMethodName            = "/queue.QueueService/Enqueue"
	QueueService_Dequeue_FullMethodName            = "/queue.QueueService/Dequeue"
	QueueService_Ack_FullMethodName                = "/queue.QueueService/Ack"
	QueueService_Nack_FullMethodName               = "/queue.QueueService/Nack"
	QueueService_JoinCluster_FullMethodName        = "/queue.QueueService/JoinCluster"
	QueueService_LeaveCluster_FullMethodName       = "/queue.QueueService/LeaveCluster"
	QueueService_Heartbeat_FullMethodName          = "/queue.QueueService/Heartbeat"
	QueueService_ReplicateOperation_FullMethodName = "/queue.QueueService/ReplicateOperation"
	QueueService_RequestSync_FullMethodName        = "/queue.QueueService/RequestSync"
	QueueService_RequestVote_FullMethodName        = "/queue.QueueService/RequestVote"
	QueueService_GetQueueStats_FullMethodName      = "/queue.QueueService/GetQueueStats"
)

// QueueServiceClient is the client API for QueueService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// QueueService defines the RPC methods for queue operations
type QueueServiceClient interface {
	// Queue operations
	Enqueue(ctx context.Context, in *EnqueueRequest, opts ...grpc.CallOption) (*EnqueueResponse, error)
	Dequeue(ctx context.Context, in *DequeueRequest, opts ...grpc.CallOption) (*DequeueResponse, error)
	Ack(ctx context.Context, in *AckRequest, opts ...grpc.CallOption) (*AckResponse, error)
	Nack(ctx context.Context, in *NackRequest, opts ...grpc.CallOption) (*NackResponse, error)
	// Node management
	JoinCluster(ctx context.Context, in *JoinRequest, opts ...grpc.CallOption) (*JoinResponse, error)
	LeaveCluster(ctx context.Context, in *LeaveRequest, opts ...grpc.CallOption) (*LeaveResponse, error)
	Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error)
	// State replication
	ReplicateOperation(ctx context.Context, in *ReplicationRequest, opts ...grpc.CallOption) (*ReplicationResponse, error)
	RequestSync(ctx context.Context, in *SyncRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[SyncResponse], error)
	RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error)
	GetQueueStats(ctx context.Context, in *GetQueueStatsRequest, opts ...grpc.CallOption) (*GetQueueStatsResponse, error)
}

type queueServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewQueueServiceClient(cc grpc.ClientConnInterface) QueueServiceClient {
	return &queueServiceClient{cc}
}

func (c *queueServiceClient) Enqueue(ctx context.Context, in *EnqueueRequest, opts ...grpc.CallOption) (*EnqueueResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(EnqueueResponse)
	err := c.cc.Invoke(ctx, QueueService_Enqueue_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queueServiceClient) Dequeue(ctx context.Context, in *DequeueRequest, opts ...grpc.CallOption) (*DequeueResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(DequeueResponse)
	err := c.cc.Invoke(ctx, QueueService_Dequeue_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queueServiceClient) Ack(ctx context.Context, in *AckRequest, opts ...grpc.CallOption) (*AckResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(AckResponse)
	err := c.cc.Invoke(ctx, QueueService_Ack_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queueServiceClient) Nack(ctx context.Context, in *NackRequest, opts ...grpc.CallOption) (*NackResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(NackResponse)
	err := c.cc.Invoke(ctx, QueueService_Nack_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queueServiceClient) JoinCluster(ctx context.Context, in *JoinRequest, opts ...grpc.CallOption) (*JoinResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(JoinResponse)
	err := c.cc.Invoke(ctx, QueueService_JoinCluster_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queueServiceClient) LeaveCluster(ctx context.Context, in *LeaveRequest, opts ...grpc.CallOption) (*LeaveResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(LeaveResponse)
	err := c.cc.Invoke(ctx, QueueService_LeaveCluster_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queueServiceClient) Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(HeartbeatResponse)
	err := c.cc.Invoke(ctx, QueueService_Heartbeat_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queueServiceClient) ReplicateOperation(ctx context.Context, in *ReplicationRequest, opts ...grpc.CallOption) (*ReplicationResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ReplicationResponse)
	err := c.cc.Invoke(ctx, QueueService_ReplicateOperation_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queueServiceClient) RequestSync(ctx context.Context, in *SyncRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[SyncResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &QueueService_ServiceDesc.Streams[0], QueueService_RequestSync_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[SyncRequest, SyncResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type QueueService_RequestSyncClient = grpc.ServerStreamingClient[SyncResponse]

func (c *queueServiceClient) RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(RequestVoteResponse)
	err := c.cc.Invoke(ctx, QueueService_RequestVote_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queueServiceClient) GetQueueStats(ctx context.Context, in *GetQueueStatsRequest, opts ...grpc.CallOption) (*GetQueueStatsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetQueueStatsResponse)
	err := c.cc.Invoke(ctx, QueueService_GetQueueStats_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// QueueServiceServer is the server API for QueueService service.
// All implementations must embed UnimplementedQueueServiceServer
// for forward compatibility.
//
// QueueService defines the RPC methods for queue operations
type QueueServiceServer interface {
	// Queue operations
	Enqueue(context.Context, *EnqueueRequest) (*EnqueueResponse, error)
	Dequeue(context.Context, *DequeueRequest) (*DequeueResponse, error)
	Ack(context.Context, *AckRequest) (*AckResponse, error)
	Nack(context.Context, *NackRequest) (*NackResponse, error)
	// Node management
	JoinCluster(context.Context, *JoinRequest) (*JoinResponse, error)
	LeaveCluster(context.Context, *LeaveRequest) (*LeaveResponse, error)
	Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error)
	// State replication
	ReplicateOperation(context.Context, *ReplicationRequest) (*ReplicationResponse, error)
	RequestSync(*SyncRequest, grpc.ServerStreamingServer[SyncResponse]) error
	RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error)
	GetQueueStats(context.Context, *GetQueueStatsRequest) (*GetQueueStatsResponse, error)
	mustEmbedUnimplementedQueueServiceServer()
}

// UnimplementedQueueServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedQueueServiceServer struct{}

func (UnimplementedQueueServiceServer) Enqueue(context.Context, *EnqueueRequest) (*EnqueueResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Enqueue not implemented")
}
func (UnimplementedQueueServiceServer) Dequeue(context.Context, *DequeueRequest) (*DequeueResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Dequeue not implemented")
}
func (UnimplementedQueueServiceServer) Ack(context.Context, *AckRequest) (*AckResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Ack not implemented")
}
func (UnimplementedQueueServiceServer) Nack(context.Context, *NackRequest) (*NackResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Nack not implemented")
}
func (UnimplementedQueueServiceServer) JoinCluster(context.Context, *JoinRequest) (*JoinResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method JoinCluster not implemented")
}
func (UnimplementedQueueServiceServer) LeaveCluster(context.Context, *LeaveRequest) (*LeaveResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method LeaveCluster not implemented")
}
func (UnimplementedQueueServiceServer) Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Heartbeat not implemented")
}
func (UnimplementedQueueServiceServer) ReplicateOperation(context.Context, *ReplicationRequest) (*ReplicationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplicateOperation not implemented")
}
func (UnimplementedQueueServiceServer) RequestSync(*SyncRequest, grpc.ServerStreamingServer[SyncResponse]) error {
	return status.Errorf(codes.Unimplemented, "method RequestSync not implemented")
}
func (UnimplementedQueueServiceServer) RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RequestVote not implemented")
}
func (UnimplementedQueueServiceServer) GetQueueStats(context.Context, *GetQueueStatsRequest) (*GetQueueStatsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetQueueStats not implemented")
}
func (UnimplementedQueueServiceServer) mustEmbedUnimplementedQueueServiceServer() {}
func (UnimplementedQueueServiceServer) testEmbeddedByValue()                      {}

// UnsafeQueueServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to QueueServiceServer will
// result in compilation errors.
type UnsafeQueueServiceServer interface {
	mustEmbedUnimplementedQueueServiceServer()
}

func RegisterQueueServiceServer(s grpc.ServiceRegistrar, srv QueueServiceServer) {
	// If the following call pancis, it indicates UnimplementedQueueServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&QueueService_ServiceDesc, srv)
}

func _QueueService_Enqueue_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EnqueueRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueueServiceServer).Enqueue(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: QueueService_Enqueue_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueueServiceServer).Enqueue(ctx, req.(*EnqueueRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _QueueService_Dequeue_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DequeueRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueueServiceServer).Dequeue(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: QueueService_Dequeue_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueueServiceServer).Dequeue(ctx, req.(*DequeueRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _QueueService_Ack_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AckRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueueServiceServer).Ack(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: QueueService_Ack_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueueServiceServer).Ack(ctx, req.(*AckRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _QueueService_Nack_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(NackRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueueServiceServer).Nack(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: QueueService_Nack_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueueServiceServer).Nack(ctx, req.(*NackRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _QueueService_JoinCluster_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JoinRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueueServiceServer).JoinCluster(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: QueueService_JoinCluster_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueueServiceServer).JoinCluster(ctx, req.(*JoinRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _QueueService_LeaveCluster_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaveRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueueServiceServer).LeaveCluster(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: QueueService_LeaveCluster_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueueServiceServer).LeaveCluster(ctx, req.(*LeaveRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _QueueService_Heartbeat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HeartbeatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueueServiceServer).Heartbeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: QueueService_Heartbeat_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueueServiceServer).Heartbeat(ctx, req.(*HeartbeatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _QueueService_ReplicateOperation_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReplicationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueueServiceServer).ReplicateOperation(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: QueueService_ReplicateOperation_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueueServiceServer).ReplicateOperation(ctx, req.(*ReplicationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _QueueService_RequestSync_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(SyncRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(QueueServiceServer).RequestSync(m, &grpc.GenericServerStream[SyncRequest, SyncResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type QueueService_RequestSyncServer = grpc.ServerStreamingServer[SyncResponse]

func _QueueService_RequestVote_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RequestVoteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueueServiceServer).RequestVote(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: QueueService_RequestVote_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueueServiceServer).RequestVote(ctx, req.(*RequestVoteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _QueueService_GetQueueStats_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetQueueStatsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueueServiceServer).GetQueueStats(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: QueueService_GetQueueStats_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueueServiceServer).GetQueueStats(ctx, req.(*GetQueueStatsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// QueueService_ServiceDesc is the grpc.ServiceDesc for QueueService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var QueueService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "queue.QueueService",
	HandlerType: (*QueueServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Enqueue",
			Handler:    _QueueService_Enqueue_Handler,
		},
		{
			MethodName: "Dequeue",
			Handler:    _QueueService_Dequeue_Handler,
		},
		{
			MethodName: "Ack",
			Handler:    _QueueService_Ack_Handler,
		},
		{
			MethodName: "Nack",
			Handler:    _QueueService_Nack_Handler,
		},
		{
			MethodName: "JoinCluster",
			Handler:    _QueueService_JoinCluster_Handler,
		},
		{
			MethodName: "LeaveCluster",
			Handler:    _QueueService_LeaveCluster_Handler,
		},
		{
			MethodName: "Heartbeat",
			Handler:    _QueueService_Heartbeat_Handler,
		},
		{
			MethodName: "ReplicateOperation",
			Handler:    _QueueService_ReplicateOperation_Handler,
		},
		{
			MethodName: "RequestVote",
			Handler:    _QueueService_RequestVote_Handler,
		},
		{
			MethodName: "GetQueueStats",
			Handler:    _QueueService_GetQueueStats_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "RequestSync",
			Handler:       _QueueService_RequestSync_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "api/proto/queue.proto",
}