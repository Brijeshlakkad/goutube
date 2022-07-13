// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package replication_v1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion7

// ReplicationClient is the client API for Replication service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ReplicationClient interface {
	Replicate(ctx context.Context, in *ReplicateRequest, opts ...grpc.CallOption) (Replication_ReplicateClient, error)
}

type replicationClient struct {
	cc grpc.ClientConnInterface
}

func NewReplicationClient(cc grpc.ClientConnInterface) ReplicationClient {
	return &replicationClient{cc}
}

func (c *replicationClient) Replicate(ctx context.Context, in *ReplicateRequest, opts ...grpc.CallOption) (Replication_ReplicateClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Replication_serviceDesc.Streams[0], "/replication.v1.Replication/Replicate", opts...)
	if err != nil {
		return nil, err
	}
	x := &replicationReplicateClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Replication_ReplicateClient interface {
	Recv() (*ReplicateResponse, error)
	grpc.ClientStream
}

type replicationReplicateClient struct {
	grpc.ClientStream
}

func (x *replicationReplicateClient) Recv() (*ReplicateResponse, error) {
	m := new(ReplicateResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// ReplicationServer is the server API for Replication service.
// All implementations must embed UnimplementedReplicationServer
// for forward compatibility
type ReplicationServer interface {
	Replicate(*ReplicateRequest, Replication_ReplicateServer) error
	mustEmbedUnimplementedReplicationServer()
}

// UnimplementedReplicationServer must be embedded to have forward compatible implementations.
type UnimplementedReplicationServer struct {
}

func (UnimplementedReplicationServer) Replicate(*ReplicateRequest, Replication_ReplicateServer) error {
	return status.Errorf(codes.Unimplemented, "method Replicate not implemented")
}
func (UnimplementedReplicationServer) mustEmbedUnimplementedReplicationServer() {}

// UnsafeReplicationServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ReplicationServer will
// result in compilation errors.
type UnsafeReplicationServer interface {
	mustEmbedUnimplementedReplicationServer()
}

func RegisterReplicationServer(s *grpc.Server, srv ReplicationServer) {
	s.RegisterService(&_Replication_serviceDesc, srv)
}

func _Replication_Replicate_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ReplicateRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ReplicationServer).Replicate(m, &replicationReplicateServer{stream})
}

type Replication_ReplicateServer interface {
	Send(*ReplicateResponse) error
	grpc.ServerStream
}

type replicationReplicateServer struct {
	grpc.ServerStream
}

func (x *replicationReplicateServer) Send(m *ReplicateResponse) error {
	return x.ServerStream.SendMsg(m)
}

var _Replication_serviceDesc = grpc.ServiceDesc{
	ServiceName: "replication.v1.Replication",
	HandlerType: (*ReplicationServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Replicate",
			Handler:       _Replication_Replicate_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "api/replication/v1/replication.proto",
}