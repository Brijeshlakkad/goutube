package goutube

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"go.opencensus.io/plugin/ocgrpc"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/Brijeshlakkad/ring"
	"github.com/soheilhy/cmux"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestLoadBalancer_ProduceStream_ConsumeStream_GetMetaData(t *testing.T) {
	objectKey := testPointId
	loadBalancerClient, ringInstance, lociMap, teardown := setupTestLoadBalancer(t)
	defer teardown()

	// Find the responsible server for this object.
	nodeTags, found := ringInstance.GetNode(objectKey)
	require.True(t, found)

	nodeRPCAddr, ok := nodeTags[rpcAddressRingTag]
	require.True(t, ok)

	objNodeIP, err := net.ResolveTCPAddr("tcp", nodeRPCAddr)
	require.NoError(t, err)

	var objLeader *testReplicationClusterMember
	for leader, _ := range lociMap {
		leaderIP, err := net.ResolveTCPAddr("tcp", leader.DistributedLoci.config.Distributed.RPCAddress)
		require.NoError(t, err)
		if objNodeIP.String() == leaderIP.String() {
			objLeader = leader
			break
		}
	}
	require.NotNil(t, objLeader)

	stream, err := loadBalancerClient.ProduceStream(context.Background())
	require.NoError(t, err)

	expectedOffset := uint64(0)
	expectedPos := uint64(0)
	chunkSize := int(testChunkSize)
	file := setupFile(t, chunkSize, testPointLines)
	for i := 0; i < testPointLines; i++ {
		expectedOffset = expectedPos
		data := file[i*chunkSize : (i+1)*chunkSize]
		err := stream.Send(&streaming_api.ProduceRequest{Point: objectKey, Frame: data})
		require.NoError(t, err)
		expectedPos = expectedOffset + uint64(len(data))
	}
	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, 1, len(resp.Records))
	require.Equal(t, objectKey, resp.Records[0].Point)
	require.Equal(t, expectedOffset, resp.Records[0].Offset)

	time.Sleep(3 * time.Second)

	// test consume stream from load balancer streamingClient
	resStream, err := loadBalancerClient.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: objectKey})
	if err != nil {
		t.Fatalf("error while calling ConsumeStream from Load Balancer: %v", err)
	}
	i := 0
	for ; i < testPointLines; i++ {
		expectedData := file[i*chunkSize : (i+1)*chunkSize]
		resp, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		b := resp.GetFrame()
		require.Equal(t, expectedData, b)
	}
	require.Equal(t, testPointLines, i)

	// test consume stream from the responsible server
	resStream, err = objLeader.streamingClient.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: testPointId})
	if err != nil {
		t.Fatalf("error while calling ConsumeStream from Load Balancer: %v", err)
	}
	i = 0
	for ; i < testPointLines; i++ {
		expectedData := file[i*chunkSize : (i+1)*chunkSize]
		resp, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		b := resp.GetFrame()
		require.Equal(t, expectedData, b)
	}
	require.Equal(t, testPointLines, i)

	// test consume stream from the followers of the object replication cluster.
	// get the followers of the server.
	respFollowers, err := objLeader.followerResolverClient.GetFollowers(context.Background(), &streaming_api.GetFollowersRequest{})
	require.NoError(t, err)

	require.Equal(t, len(lociMap[objLeader]), len(respFollowers.Servers))

	// Check if replication happened to all the followers of the leader for its relevant object replication cluster.
	for _, followerServer := range lociMap[objLeader] {
		resStream, err = followerServer.streamingClient.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: testPointId})
		if err != nil {
			t.Fatalf("error while calling ConsumeStream from Load Balancer: %v", err)
		}
		i = 0
		for ; i < testPointLines; i++ {
			expectedData := file[i*chunkSize : (i+1)*chunkSize]
			resp, err := resStream.Recv()
			if err == io.EOF {
				// we've reached the end of the stream
				break
			}
			require.NoError(t, err)
			b := resp.GetFrame()
			require.Equal(t, expectedData, b)
		}
		require.Equal(t, testPointLines, i)
	}

	// Test GetMetadata to receive the list of workers and size of the point.
	metadataResp, err := loadBalancerClient.GetMetadata(context.Background(), &streaming_api.MetadataRequest{
		Point: testPointId,
	})
	require.NoError(t, err)
	require.Equal(t, len(respFollowers.Servers)/2, len(metadataResp.Workers))
	require.Equal(t, len(file), int(metadataResp.Size))
	for _, workerAddr := range metadataResp.Workers {
		found := false
		for _, follower := range respFollowers.Servers {
			if follower.RpcAddr == workerAddr {
				found = true
				break
			}
		}
		require.True(t, found)
	}
}

func TestLoadBalancer_FollowerCache(t *testing.T) {
	cache := make(map[string]*followerCache)
	testRPCAddr := "server-rpc-address"

	// check on empty followers list.
	cache[testRPCAddr] = NewFollowerCache([]*streaming_api.Server{}, MultiStreamPercent)
	_, found := cache[testRPCAddr].getNextFollower()
	require.False(t, found)

	var followers []*streaming_api.Server
	followerCount := 5
	ports := dynaport.Get(followerCount)
	for _, port := range ports {
		followers = append(followers, &streaming_api.Server{
			RpcAddr: fmt.Sprintf("localhost:%d", port),
		})
	}

	// check if the getNextFollower returns the expected follower address.
	cache[testRPCAddr] = NewFollowerCache(followers, MultiStreamPercent)
	followerIndex := 0
	for i := 0; i < followerCount+1; i++ {
		curFollower, found := cache[testRPCAddr].getNextFollower()
		require.True(t, found)
		require.Equal(t, followers[followerIndex].GetRpcAddr(), string(curFollower))
		followerIndex = (followerIndex + 1) % followerCount
	}
}

func TestLoadBalancer_ConsumeStream_Cache(t *testing.T) {
	var teardowns []func()

	defer func() {
		for _, teardown := range teardowns {
			teardown()
		}
	}()

	expectedCache := make(map[ServerAddress]*followerCache)
	// START: setup the ring members
	_, _, _, distributedLoci_Leader_1, teardown_Leader_1 := setupTestStreamingServer(t,
		LeaderRule,
		"distributed-locus-leader-1",
		&ring.Config{
			MemberType: ring.ShardMember,
		})
	defer teardown_Leader_1()

	leaderAddress := ServerAddress(distributedLoci_Leader_1.config.Distributed.RPCAddress)
	expectedCache[leaderAddress] = &followerCache{
		followers: make(map[uint8]ServerAddress),
	}
	followerCount_1 := 3
	for i := 0; i < followerCount_1; i++ {
		_, _, _, distributedLoci_Follower, teardown_Follower := setupTestStreamingServer(t,
			FollowerRule,
			fmt.Sprintf("distributed-locus-follower-1-%d", i),
			nil)
		teardowns = append(teardowns, teardown_Follower)

		expectedCache[leaderAddress].followers[uint8(i)] = ServerAddress(distributedLoci_Follower.config.Distributed.RPCAddress)
		err := distributedLoci_Leader_1.Join(distributedLoci_Follower.config.Distributed.StreamLayer.Addr().String(), distributedLoci_Follower.config.Distributed.Rule)
		require.NoError(t, err)
	}
	// END: setup the ring members

	// START: setup the load balancer configuration
	ports := dynaport.Get(2)

	id := "load-balancer"

	tags := map[string]string{
		rpcAddressRingTag: fmt.Sprintf("localhost:%d", ports[0]),
	}

	ringInstance, err := ring.NewRing(ring.Config{
		NodeName:         id,
		BindAddr:         fmt.Sprintf("localhost:%d", ports[1]),
		Tags:             tags,
		VirtualNodeCount: 3,
		SeedAddresses:    []string{distributedLoci_Leader_1.ring.BindAddr},
		MemberType:       ring.LoadBalancerMember,
	})
	require.NoError(t, err)

	authorizer := newAuth(
		ACLModelFile,
		ACLPolicyFile,
	)

	cfg := &loadbalancerConfig{
		id:         "load-balancer",
		ring:       ringInstance,
		Authorizer: authorizer,
		MaxPool:    5,
	}
	require.NoError(t, err)
	// END: setup the load balancer configuration

	// START: setup load balancer server
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// Configure the server’s TLS credentials.
	serverTLSConfig, err := SetupTLSConfig(TLSConfig{
		CertFile:      ServerCertFile,
		KeyFile:       ServerKeyFile,
		CAFile:        CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	lb, err := newLoadBalancer(cfg)
	require.NoError(t, err)
	opts := []grpc.ServerOption{grpc.Creds(serverCreds)}
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
	require.NoError(t, err)

	go func() {
		gRPCServer.Serve(l)
	}()
	// END: setup load balancer server

	// START: setup the streaming client
	tlsConfig, err := SetupTLSConfig(TLSConfig{
		CertFile: RootClientCertFile,
		KeyFile:  RootClientKeyFile,
		CAFile:   CAFile,
		Server:   false,
	})
	require.NoError(t, err)

	tlsCreds := credentials.NewTLS(tlsConfig)
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	conn, err := grpc.Dial(l.Addr().String(), dialOpts...)
	require.NoError(t, err)

	loadBalancerClient := streaming_api.NewStreamingClient(conn)
	// END: setup the streaming client

	objectKey := testPointId

	stream, err := loadBalancerClient.ProduceStream(context.Background())
	require.NoError(t, err)

	expectedOffset := uint64(0)
	expectedPos := uint64(0)
	chunkSize := int(testChunkSize)
	file := setupFile(t, chunkSize, testPointLines)
	for i := 0; i < testPointLines; i++ {
		expectedOffset = expectedPos
		data := file[i*chunkSize : (i+1)*chunkSize]
		err := stream.Send(&streaming_api.ProduceRequest{Point: objectKey, Frame: data})
		require.NoError(t, err)
		expectedPos = expectedOffset + uint64(len(data))
	}
	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, 1, len(resp.Records))
	require.Equal(t, objectKey, resp.Records[0].Point)
	require.Equal(t, expectedOffset, resp.Records[0].Offset)

	time.Sleep(3 * time.Second)

	// test consume stream from load balancer streamingClient
	resStream, err := loadBalancerClient.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: objectKey})
	if err != nil {
		t.Fatalf("error while calling ConsumeStream from Load Balancer: %v", err)
	}
	i := 0
	for ; i < testPointLines; i++ {
		expectedData := file[i*chunkSize : (i+1)*chunkSize]
		resp, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		b := resp.GetFrame()
		require.Equal(t, expectedData, b)
	}
	require.Equal(t, testPointLines, i)

	// validate cache
	require.Equal(t, len(expectedCache), len(lb.cache))
	for key, expectedFollowerCache := range expectedCache {
		actualFollowerCache, found := lb.cache[key]

		require.True(t, found)
		for _, followerAddress := range expectedFollowerCache.followers {
			expectedAddress, err := net.ResolveTCPAddr("tcp", string(followerAddress))
			expectedAddressStr := ServerAddress(expectedAddress.String())
			require.NoError(t, err)
			found := false
			for _, value := range actualFollowerCache.followers {
				if value == expectedAddressStr {
					found = true
					break
				}
			}
			require.True(t, found)
		}
	}
}

type testReplicationClusterMember struct {
	streamingClient        streaming_api.StreamingClient
	followerResolverClient streaming_api.FollowerResolverHelperClient
	DistributedLoci        *DistributedLoci
}

func setupTestLoadBalancer(t *testing.T) (streaming_api.StreamingClient,
	*ring.Ring,
	map[*testReplicationClusterMember][]*testReplicationClusterMember,
	func()) {
	t.Helper()

	var teardowns []func()
	lociMap := make(map[*testReplicationClusterMember][]*testReplicationClusterMember)

	// START: setup the ring members
	streamingClient_Leader_1, _, followerResolverHelperClient_Leader_1, distributedLoci_Leader_1, teardown_Leader_1 := setupTestStreamingServer(t,
		LeaderRule,
		"distributed-locus-leader-1",
		&ring.Config{
			MemberType: ring.ShardMember,
		})
	teardowns = append(teardowns, teardown_Leader_1)
	testObj_Leader_1 := &testReplicationClusterMember{
		streamingClient_Leader_1,
		followerResolverHelperClient_Leader_1,
		distributedLoci_Leader_1,
	}

	followerCount_1 := 3
	for i := 0; i < followerCount_1; i++ {
		streamingClient_Follower, _, followerResolverHelperClient_Follower, distributedLoci_Follower, teardown_Follower := setupTestStreamingServer(t,
			FollowerRule,
			fmt.Sprintf("distributed-locus-follower-1-%d", i),
			nil)
		teardowns = append(teardowns, teardown_Follower)
		lociMap[testObj_Leader_1] = append(lociMap[testObj_Leader_1], &testReplicationClusterMember{
			streamingClient_Follower,
			followerResolverHelperClient_Follower,
			distributedLoci_Follower,
		})

		err := distributedLoci_Leader_1.Join(distributedLoci_Follower.config.Distributed.StreamLayer.Addr().String(), distributedLoci_Follower.config.Distributed.Rule)
		require.NoError(t, err)
	}

	streamingClient_Leader_2, _, followerResolverHelperClient_Leader_2, distributedLoci_Leader_2, teardown_Leader_2 := setupTestStreamingServer(t,
		LeaderRule,
		"distributed-locus-leader-2",
		&ring.Config{
			MemberType:    ring.ShardMember,
			SeedAddresses: []string{distributedLoci_Leader_1.ring.BindAddr},
		})
	teardowns = append(teardowns, teardown_Leader_2)
	testObj_Leader_2 := &testReplicationClusterMember{
		streamingClient_Leader_2,
		followerResolverHelperClient_Leader_2,
		distributedLoci_Leader_2,
	}

	followerCount_2 := 4
	for i := 0; i < followerCount_2; i++ {
		streamingClient_Follower, _, followerResolverHelperClient_Follower, distributedLoci_Follower, teardown_Follower := setupTestStreamingServer(t,
			FollowerRule,
			fmt.Sprintf("distributed-locus-follower-2-%d", i),
			nil)
		teardowns = append(teardowns, teardown_Follower)
		lociMap[testObj_Leader_2] = append(lociMap[testObj_Leader_2], &testReplicationClusterMember{
			streamingClient_Follower,
			followerResolverHelperClient_Follower,
			distributedLoci_Follower,
		})

		err := distributedLoci_Leader_2.Join(distributedLoci_Follower.config.Distributed.StreamLayer.Addr().String(), distributedLoci_Follower.config.Distributed.Rule)
		require.NoError(t, err)
	}
	// END: setup the ring members

	time.Sleep(3 * time.Second)

	// START: setup the load balancer configuration
	ports := dynaport.Get(2)

	id := "load-balancer"

	tags := map[string]string{
		rpcAddressRingTag: fmt.Sprintf("localhost:%d", ports[0]),
	}

	ringInstance, err := ring.NewRing(ring.Config{
		NodeName:         id,
		BindAddr:         fmt.Sprintf("localhost:%d", ports[1]),
		Tags:             tags,
		VirtualNodeCount: 3,
		SeedAddresses:    []string{distributedLoci_Leader_1.ring.BindAddr, distributedLoci_Leader_2.ring.BindAddr},
		MemberType:       ring.LoadBalancerMember,
	})
	require.NoError(t, err)

	authorizer := newAuth(
		ACLModelFile,
		ACLPolicyFile,
	)

	cfg := &loadbalancerConfig{
		id:         "load-balancer",
		ring:       ringInstance,
		Authorizer: authorizer,
		MaxPool:    5,
	}
	require.NoError(t, err)
	// END: setup the load balancer configuration

	// START: setup load balancer server
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// Configure the server’s TLS credentials.
	serverTLSConfig, err := SetupTLSConfig(TLSConfig{
		CertFile:      ServerCertFile,
		KeyFile:       ServerKeyFile,
		CAFile:        CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	gRPCServer, err := NewLoadBalancer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		gRPCServer.Serve(l)
	}()
	// END: setup load balancer server

	// START: setup the streaming client
	tlsConfig, err := SetupTLSConfig(TLSConfig{
		CertFile: RootClientCertFile,
		KeyFile:  RootClientKeyFile,
		CAFile:   CAFile,
		Server:   false,
	})
	require.NoError(t, err)

	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	conn, err := grpc.Dial(l.Addr().String(), opts...)
	require.NoError(t, err)

	streamingClient := streaming_api.NewStreamingClient(conn)
	// END: setup the streaming client

	return streamingClient, ringInstance, lociMap, func() {
		gRPCServer.Stop()
		conn.Close()
		l.Close()
		for _, teardown := range teardowns {
			teardown()
		}
	}
}

func setupTestStreamingServer(t *testing.T,
	rule ParticipationRule,
	localId string,
	ringConfig *ring.Config,
) (streaming_api.StreamingClient,
	streaming_api.LBResolverHelperClient,
	streaming_api.FollowerResolverHelperClient,
	*DistributedLoci,
	func()) {
	t.Helper()

	ports := dynaport.Get(1)

	rpcAddr := fmt.Sprintf(
		"localhost:%d",
		ports[0],
	)
	rpcListener, err := net.Listen("tcp", rpcAddr)
	require.NoError(t, err)

	mux := cmux.New(rpcListener)

	// Configure the server’s TLS credentials.
	serverTLSConfig, err := SetupTLSConfig(TLSConfig{
		CertFile:      ServerCertFile,
		KeyFile:       ServerKeyFile,
		CAFile:        CAFile,
		ServerAddress: rpcAddr,
		Server:        true,
	})
	require.NoError(t, err)

	tlsConfig, err := SetupTLSConfig(TLSConfig{
		CertFile:      RootClientCertFile,
		KeyFile:       RootClientKeyFile,
		CAFile:        CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	serverCreds := credentials.NewTLS(serverTLSConfig)

	authorizer := newAuth(
		ACLModelFile,
		ACLPolicyFile,
	)

	// the order of the mux.Match matters.
	lociLn := mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Compare(b, []byte{byte(RingRPC)}) == 0
	})

	serverListener := mux.Match(cmux.Any())

	locusInstance, locusTeardown := setupTestDistributedLoci_LoadBalancer(t, rule, localId, ringConfig, lociLn, ports[0], serverTLSConfig, tlsConfig)

	cfg := &ServerConfig{
		StreamingConfig: &StreamingConfig{
			Locus:      locusInstance,
			Authorizer: authorizer,
		},
		ResolverHelperConfig: &ResolverHelperConfig{
			GetServerer:   locusInstance,
			GetFollowerer: locusInstance,
		},
		Rule: rule,
	}

	gRPCServer, err := NewServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		gRPCServer.Serve(serverListener)
	}()

	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	conn, err := grpc.Dial(serverListener.Addr().String(), opts...)
	require.NoError(t, err)

	streamingClient := streaming_api.NewStreamingClient(conn)
	resolverHelperClient := streaming_api.NewLBResolverHelperClient(conn)
	followerResolverHelperClient := streaming_api.NewFollowerResolverHelperClient(conn)

	go mux.Serve()

	return streamingClient, resolverHelperClient, followerResolverHelperClient, locusInstance, func() {
		gRPCServer.Stop()
		conn.Close()
		rpcListener.Close()
		lociLn.Close()
		serverListener.Close()
		locusTeardown()
	}
}

func setupTestDistributedLoci_LoadBalancer(t *testing.T,
	rule ParticipationRule,
	localId string,
	ringConfig *ring.Config,
	listener net.Listener,
	rpcPort int,
	serverTLSConfig *tls.Config,
	peerTLSConfig *tls.Config,
) (*DistributedLoci, func()) {
	t.Helper()

	dataDir, err := ioutil.TempDir("", "distributed-locus-test")
	require.NoError(t, err)

	c := Config{}
	c.Distributed.MaxChunkSize = testChunkSize
	c.Distributed.StreamLayer = NewStreamLayer(
		listener,
		serverTLSConfig,
		peerTLSConfig,
	)
	c.Distributed.RPCAddress = fmt.Sprintf("localhost:%d", rpcPort)
	c.Distributed.LocalID = localId
	c.Distributed.Rule = rule

	if ringConfig != nil {
		ports := dynaport.Get(1)
		nodeName := ringConfig.NodeName
		if nodeName == "" {
			nodeName = localId
		}
		bindAddr := ringConfig.BindAddr
		if bindAddr == "" {
			bindAddr = fmt.Sprintf("localhost:%d", ports[0])
		}
		virtualNodeCount := ringConfig.VirtualNodeCount
		if virtualNodeCount == 0 {
			virtualNodeCount = 3
		}
		seedAddresses := ringConfig.SeedAddresses
		if seedAddresses == nil {
			seedAddresses = []string{}
		}
		tags := map[string]string{
			rpcAddressRingTag: fmt.Sprintf("localhost:%d", rpcPort),
		}
		ringInstance, err := ring.NewRing(ring.Config{
			NodeName:         nodeName,
			BindAddr:         bindAddr,
			Tags:             tags,
			VirtualNodeCount: virtualNodeCount,
			SeedAddresses:    seedAddresses,
			MemberType:       ringConfig.MemberType,
		})
		require.NoError(t, err)
		c.Distributed.Ring = ringInstance
	}

	distributedLoci, err := NewDistributedLoci(dataDir, c)
	require.NoError(t, err)

	return distributedLoci, func() {
		os.RemoveAll(dataDir)
		err := distributedLoci.Shutdown()
		require.NoError(t, err)
	}
}
