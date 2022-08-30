package goutube

import (
	"context"
	"errors"
	"io"
	"sync"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/Brijeshlakkad/ring"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/hashicorp/go-hclog"
	"go.opencensus.io/plugin/ocgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	ErrCannotHandleRequest = errors.New("Couldn't handle the request")
)

// loadBalancer finds the responsible server for the provided request.
// If the request relates to producing, it will redirect it to the leader of the cluster responsible for the respective object.
// For now, the request relating to consuming will get redirected to the replica of that object cluster in the round-robin fashion.
type loadBalancer struct {
	streaming_api.UnimplementedStreamingServer
	*loadbalancerConfig

	connPool     map[ServerAddress][]*grpc.ClientConn
	connPoolLock sync.Mutex

	// streamCtx is used to cancel existing connection handlers.
	streamCtx     context.Context
	streamCancel  context.CancelFunc
	streamCtxLock sync.RWMutex

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	maxPool int
	logger  hclog.Logger

	cache map[ServerAddress]*followerCache // Todo: TTL and invalidate bad addresses
}

type loadbalancerConfig struct {
	id         string
	ring       *ring.Ring
	Authorizer *authorizer
	MaxPool    int
	Logger     hclog.Logger
}

func newLoadBalancer(config *loadbalancerConfig) (*loadBalancer, error) {
	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "load-balancer-net",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	lb := &loadBalancer{
		loadbalancerConfig: config,
		connPool:           make(map[ServerAddress][]*grpc.ClientConn),
		logger:             config.Logger,
		maxPool:            config.MaxPool,
		shutdownCh:         make(chan struct{}),
		cache:              make(map[ServerAddress]*followerCache),
	}
	// Create the connection context and then start our listener.
	lb.setupStreamContext()
	return lb, nil
}

func NewLoadBalancer(config *loadbalancerConfig, opts ...grpc.ServerOption) (*grpc.Server, error) {
	lb, err := newLoadBalancer(config)
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
	streaming_api.RegisterStreamingServer(gRPCServer, lb)
	return gRPCServer, nil
}

func (lb *loadBalancer) ProduceStream(stream streaming_api.Streaming_ProduceStreamServer) error {
	if err := lb.Authorizer.Authorize(
		subject(stream.Context()),
		objectWildCard,
		produceAction,
	); err != nil {
		return err
	}

	var conn *grpc.ClientConn
	returnTheConnection := false

	req, err := stream.Recv()
	if err == io.EOF {
		// can return the connection back.
		returnTheConnection = true
	} else if err != nil {
		// cannot return the connection back to the pool.
		return err
	} else {
		// This will return the rpc address of the leader node.
		shardNodeRPCAddr, found := lb.ring.GetNode(req.Point)
		if !found {
			return ErrCannotHandleRequest
		}

		conn, err = lb.getConn(ServerAddress(shardNodeRPCAddr.(string)))
		if err != nil {
			return err
		}

		client := streaming_api.NewStreamingClient(conn)

		produceStream, err := client.ProduceStream(lb.streamCtx)
		if err != nil {
			return err
		}

		// Forward the request to designated server.
		for {
			if err = produceStream.Send(req); err != nil {
				return err
			}
			req, err = stream.Recv()
			if err == io.EOF {
				// Close the forwarding stream.
				resp, err := produceStream.CloseAndRecv()
				if err != nil {
					return err
				}
				// Send the response and close the client stream.
				if err := stream.SendAndClose(resp); err != nil {
					return err
				}
				// can return the connection back.
				returnTheConnection = true
				break
			} else if err != nil {
				// cannot return the connection back to the pool.
				return err
			}
		}
	}

	// return the connection back to the pool.
	if returnTheConnection {
		lb.returnConn(conn)
	}
	return nil
}

func (lb *loadBalancer) ConsumeStream(req *streaming_api.ConsumeRequest, stream streaming_api.Streaming_ConsumeStreamServer) error {
	if err := lb.Authorizer.Authorize(
		subject(stream.Context()),
		objectWildCard,
		consumeAction,
	); err != nil {
		return err
	}

	// This will return the rpc address of the leader node.
	shardNodeRPCAddrI, found := lb.ring.GetNode(req.Point)
	if !found {
		return ErrCannotHandleRequest
	}

	shardNodeRPCAddr := ServerAddress(shardNodeRPCAddrI.(string))

	leaderConn, err := lb.getConn(shardNodeRPCAddr)
	if err != nil {
		return err
	}

	var conn *grpc.ClientConn

	_, followerCached := lb.cache[shardNodeRPCAddr]

	if !followerCached {
		resolverHelperClient := streaming_api.NewFollowerResolverHelperClient(leaderConn)
		var resp *streaming_api.GetFollowersResponse
		resp, err = resolverHelperClient.GetFollowers(lb.streamCtx, &streaming_api.GetFollowersRequest{})
		// cache the list of followers for this leader.
		lb.cache[shardNodeRPCAddr] = NewFollowerCache(resp.Servers)
	}

	cache, _ := lb.cache[shardNodeRPCAddr]
	followerAddress, found := cache.getNextFollower()

	if err != nil || !found {
		// Try forwarding the request to the leader node of the object replication cluster.
		conn = leaderConn
	} else {
		conn, err = lb.getConn(followerAddress)
		if err != nil {
			return err
		}
	}

	client := streaming_api.NewStreamingClient(conn)

	clientStream, err := client.ConsumeStream(lb.streamCtx, req)
	if err != nil {
		return err
	}

	returnTheConnection := false

CONSUME:
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			resp, err := clientStream.Recv()
			if err == io.EOF {
				returnTheConnection = true
				// we've reached the end of the stream
				break CONSUME
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}

	// return the connection back to the pool.
	if returnTheConnection {
		lb.returnConn(conn)
	}
	return nil
}

// returnConn returns a connection back to the pool.
func (lb *loadBalancer) returnConn(conn *grpc.ClientConn) {
	lb.connPoolLock.Lock()
	defer lb.connPoolLock.Unlock()

	key := ServerAddress(conn.Target())
	conns, _ := lb.connPool[key]

	if !lb.IsShutdown() && len(conns) < lb.maxPool {
		lb.connPool[key] = append(conns, conn)
	} else {
		conn.Close()
	}
}

// setupStreamContext is used to create a new stream context. This should be
// called with the stream lock held.
func (lb *loadBalancer) setupStreamContext() {
	ctx, cancel := context.WithCancel(context.Background())
	lb.streamCtx = ctx
	lb.streamCancel = cancel
}

// getStreamContext is used retrieve the current stream context.
func (lb *loadBalancer) getStreamContext() context.Context {
	lb.streamCtxLock.RLock()
	defer lb.streamCtxLock.RUnlock()
	return lb.streamCtx
}

// getExistingConn is used to grab a pooled connection.
func (lb *loadBalancer) getPooledConn(target ServerAddress) *grpc.ClientConn {
	lb.connPoolLock.Lock()
	defer lb.connPoolLock.Unlock()

	conns, ok := lb.connPool[target]
	if !ok || len(conns) == 0 {
		return nil
	}

	var conn *grpc.ClientConn
	num := len(conns)
	conn, conns[num-1] = conns[num-1], nil
	lb.connPool[target] = conns[:num-1]
	return conn
}

// getConn is used to get a connection from the pool.
func (lb *loadBalancer) getConn(target ServerAddress) (*grpc.ClientConn, error) {
	// Check for a pooled conn
	if conn := lb.getPooledConn(target); conn != nil {
		return conn, nil
	}

	peerTLSConfig, err := SetupTLSConfig(TLSConfig{
		CertFile:      RootClientCertFile,
		KeyFile:       RootClientKeyFile,
		CAFile:        CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})

	if err != nil {
		return nil, err
	}

	tlsCreds := credentials.NewTLS(peerTLSConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}

	// Dial a new connection
	conn, err := grpc.Dial(string(target), opts...)

	return conn, nil
}

// CloseStreams closes the current streams.
func (lb *loadBalancer) CloseStreams() {
	lb.connPoolLock.Lock()
	defer lb.connPoolLock.Unlock()

	// Shutdown all the connections in the connection pool and then remove their entry.
	for k, e := range lb.connPool {
		for _, conn := range e {
			conn.Close()
		}
		delete(lb.connPool, k)
	}

	// Cancel the existing connections and create a new context. Both these
	// operations must always be done with the lock held otherwise we can create
	// connection handlers that are holding a context that will never be
	// cancelable.
	lb.streamCtxLock.Lock()
	lb.streamCancel()
	lb.setupStreamContext()
	lb.streamCtxLock.Unlock()
}

// Close is used to stop the network transport.
func (lb *loadBalancer) Close() error {
	lb.shutdownLock.Lock()
	defer lb.shutdownLock.Unlock()

	if !lb.shutdown {
		close(lb.shutdownCh)
		lb.CloseStreams()
		lb.shutdown = true
	}
	return nil
}

// IsShutdown is used to check if the transport is shutdown.
func (lb *loadBalancer) IsShutdown() bool {
	select {
	case <-lb.shutdownCh:
		return true
	default:
		return false
	}
}

// followerCache stores the addresses of the followers and keeps the track of which follower should be requested next.
type followerCache struct {
	followers         map[uint8]ServerAddress
	nextFollowerIndex uint8
	lock              sync.Mutex
}

func NewFollowerCache(followers []*streaming_api.Server) *followerCache {
	fc := &followerCache{
		followers:         make(map[uint8]ServerAddress),
		nextFollowerIndex: 0,
	}

	i := uint8(0)
	for _, follower := range followers {
		fc.followers[i] = ServerAddress(follower.RpcAddr)
		i++
	}

	return fc
}

func (f *followerCache) getNextFollower() (ServerAddress, bool) {
	if len(f.followers) == 0 {
		return "", false
	}

	curIndex := f.nextFollowerIndex

	f.lock.Lock()
	f.nextFollowerIndex = (f.nextFollowerIndex + 1) % uint8(len(f.followers))
	f.lock.Unlock()

	return f.followers[curIndex], true
}
