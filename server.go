package goutube

import (
	"context"
	"encoding/binary"

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

type ServerConfig struct {
	StreamingConfig      *StreamingConfig
	ResolverHelperConfig *ResolverHelperConfig
	Rule                 ParticipationRule
}

type ResolverHelperConfig struct {
	GetServerer   GetServerer
	GetFollowerer GetFollowerer
}

// START: Load Balancer Resolver Helper

type GetServerer interface {
	GetServers(*streaming_api.GetServersRequest) ([]*streaming_api.Server, error)
}

func NewLBResolverHelper(config *ResolverHelperConfig) (*LBResolverHelper, error) {
	return &LBResolverHelper{
		ResolverHelperConfig: config,
	}, nil
}

type LBResolverHelper struct {
	streaming_api.UnimplementedLBResolverHelperServer
	*ResolverHelperConfig
}

func (r *LBResolverHelper) GetServers(ctx context.Context, req *streaming_api.GetServersRequest) (*streaming_api.GetServersResponse, error) {
	servers, err := r.GetServerer.GetServers(req)
	if err != nil {
		return nil, err
	}
	return &streaming_api.GetServersResponse{Servers: servers}, nil
}

// END: Load Balancer Resolver Helper

// START: Follower Resolver Helper

type GetFollowerer interface {
	GetFollowers(*streaming_api.GetFollowersRequest) ([]*streaming_api.Server, error)
}

func NewFollowerResolverHelper(config *ResolverHelperConfig) (*FollowerResolverHelper, error) {
	return &FollowerResolverHelper{
		ResolverHelperConfig: config,
	}, nil
}

type FollowerResolverHelper struct {
	streaming_api.UnimplementedFollowerResolverHelperServer
	*ResolverHelperConfig
}

func (r *FollowerResolverHelper) GetFollowers(ctx context.Context, req *streaming_api.GetFollowersRequest) (*streaming_api.GetFollowersResponse, error) {
	servers, err := r.GetFollowerer.GetFollowers(req)
	if err != nil {
		return nil, err
	}
	return &streaming_api.GetFollowersResponse{Servers: servers}, nil
}

// END: Follower Resolver Helper

func NewServer(config *ServerConfig, opts ...grpc.ServerOption) (*grpc.Server, error) {
	sm, err := NewStreamingServer(config.StreamingConfig)
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
	// Register LoadBalancer Resolver Helper
	// Any member type server can get the load balancers of the ring.
	if config.ResolverHelperConfig != nil {
		lbm, err := NewLBResolverHelper(config.ResolverHelperConfig)
		if err != nil {
			return nil, err
		}
		streaming_api.RegisterLBResolverHelperServer(gRPCServer, lbm)
	}
	// Register Follower Resolver Helper
	// Only ShardMember members are implementing the GetFollowers method.
	if config.ResolverHelperConfig != nil && shouldImplementFollowerResolver(config.Rule) {
		fm, err := NewFollowerResolverHelper(config.ResolverHelperConfig)
		if err != nil {
			return nil, err
		}
		streaming_api.RegisterFollowerResolverHelperServer(gRPCServer, fm)
	}
	return gRPCServer, nil
}

// Interceptor reading the subject out of the client's cert and writing it to the RPC’s context.
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
