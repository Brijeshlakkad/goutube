package server

import (
	"context"
	"encoding/binary"

	replication_api "github.com/Brijeshlakkad/goutube/api/replication/v1"
	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"go.opencensus.io/plugin/ocgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

var (
	enc = binary.BigEndian
)

type Config struct {
	ReplicationConfig *ReplicationConfig
	StreamingConfig   *StreamingConfig
}

func NewServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	sm, err := NewStreamingServer(config.StreamingConfig)
	if err != nil {
		return nil, err
	}
	rm, err := NewReplicationManager(config.ReplicationConfig)
	if err != nil {
		return nil, err
	}
	opts = append(opts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_auth.StreamServerInterceptor(authenticate),
			)),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_auth.UnaryServerInterceptor(authenticate),
			)),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	)
	gRPCServer := grpc.NewServer(opts...)
	streaming_api.RegisterStreamingServer(gRPCServer, sm)
	replication_api.RegisterReplicationServer(gRPCServer, rm)
	return gRPCServer, nil
}

type LociManager interface {
	GetLoci() []string
	GetPoints(string) []string
	Append(string, string, []byte) (uint64, error)
	Read(string, string, uint64) ([]byte, error)
	ReadAt(string, string, []byte, int64) (int, error)
	ClosePoint(string, string) error
	CloseAll() error
}

// Interceptor reading the subject out of the client’s cert and writing it to the RPC’s context.
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
