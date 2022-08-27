package goutube

import (
	"bytes"
	"context"
	"fmt"
	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/Brijeshlakkad/goutube/pointcron"
	"github.com/Brijeshlakkad/ring"
	"github.com/soheilhy/cmux"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"io"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"
)

func TestLoadBalancer_ProduceStream(t *testing.T) {
	objectKey := testPointId
	loadBalancerClient, ringInstance, lociMap, teardown := setupTestLoadBalancer(t)
	defer teardown()

	// Find the responsible server for this object.
	nodeRPCAddr, found := ringInstance.GetNode(objectKey)
	require.True(t, found)

	objNodeIP, err := net.ResolveTCPAddr("tcp", nodeRPCAddr.(string))
	require.NoError(t, err)

	var objLeader streaming_api.StreamingClient
	for leader, _ := range lociMap {
		leaderIP, err := net.ResolveTCPAddr("tcp", leader.DistributedLoci.config.Distributed.RPCAddress)
		require.NoError(t, err)
		if objNodeIP.String() == leaderIP.String() {
			objLeader = leader.client
			break
		}
	}
	require.NotNil(t, objLeader)

	stream, err := loadBalancerClient.ProduceStream(context.Background())
	require.NoError(t, err)

	expectedOffset := uint64(0)
	expectedPos := uint64(0)
	for i := 1; i <= testPointLines; i++ {
		expectedOffset = expectedPos
		data := []byte(fmt.Sprintf("Line: %d", i))
		err := stream.Send(&streaming_api.ProduceRequest{Point: objectKey, Frame: data})
		require.NoError(t, err)
		expectedPos = expectedOffset + uint64(len(data)+lenWidth)
	}
	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, 1, len(resp.Records))
	require.Equal(t, objectKey, resp.Records[0].Point)
	require.Equal(t, expectedOffset, resp.Records[0].Offset)

	// test consume stream from load balancer client
	resStream, err := loadBalancerClient.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: objectKey})
	if err != nil {
		t.Fatalf("error while calling ConsumeStream from Load Balancer: %v", err)
	}
	i := 1
	for ; i <= testPointLines; i++ {
		expectedData := fmt.Sprintf("Line: %d", i)
		resp, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		b := resp.GetFrame()
		require.Equal(t, expectedData, string(b))
	}
	require.Equal(t, testPointLines+1, i)

	// test consume stream from the responsible server
	resStream, err = objLeader.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: testPointId})
	if err != nil {
		t.Fatalf("error while calling ConsumeStream from Load Balancer: %v", err)
	}
	i = 1
	for ; i <= testPointLines; i++ {
		expectedData := fmt.Sprintf("Line: %d", i)
		resp, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		b := resp.GetFrame()
		require.Equal(t, expectedData, string(b))
	}
	require.Equal(t, testPointLines+1, i)
}

func TestLoadBalancer_ConsumeStream(t *testing.T) {
	objectKey := "sample-object-key"
	loadBalancerClient, ringInstance, lociMap, teardown := setupTestLoadBalancer(t)
	defer teardown()

	nodeRPCAddr, found := ringInstance.GetNode(objectKey)
	require.True(t, found)

	objNodeIP, err := net.ResolveTCPAddr("tcp", nodeRPCAddr.(string))
	require.NoError(t, err)

	var objLeader *DistributedLoci
	for leader, _ := range lociMap {
		leaderIP, err := net.ResolveTCPAddr("tcp", leader.DistributedLoci.config.Distributed.RPCAddress)
		require.NoError(t, err)
		if objNodeIP.String() == leaderIP.String() {
			objLeader = leader.DistributedLoci

			fmt.Println(leader.DistributedLoci.locus.locusDir)
			break
		}
	}
	require.NotNil(t, objLeader)

	actualPos := uint64(0)
	for i := 1; i <= testPointLines; i++ {
		data := []byte(fmt.Sprintf("Line: %d", i))
		n, _, err := objLeader.locus.Append(testPointId, data)
		require.NoError(t, err)
		actualPos += uint64(len(data) + lenWidth)
		require.Equal(t, n, actualPos)
	}

	// test consume stream
	resStream, err := loadBalancerClient.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: testPointId})
	if err != nil {
		t.Fatalf("error while calling ConsumeStream from Load Balancer: %v", err)
	}
	i := 1
	for ; i <= testPointLines; i++ {
		expectedData := fmt.Sprintf("Line: %d", i)
		resp, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		b := resp.GetFrame()
		require.Equal(t, expectedData, string(b))
	}
	require.Equal(t, testPointLines+1, i)
}

type testReplicationClusterMember struct {
	client          streaming_api.StreamingClient
	DistributedLoci *DistributedLoci
}

func setupTestLoadBalancer(t *testing.T) (streaming_api.StreamingClient,
	*ring.Ring,
	map[*testReplicationClusterMember][]*testReplicationClusterMember,
	func()) {
	t.Helper()

	var teardowns []func()
	lociMap := make(map[*testReplicationClusterMember][]*testReplicationClusterMember)

	// START: setup the ring members
	streamingClient_Leader_1, _, distributedLoci_Leader_1, teardown_Leader_1 := setupTestStreamingServer(t,
		LeaderRule,
		"distributed-locus-leader-1",
		&ring.Config{
			MemberType: ring.ShardMember,
		})
	teardowns = append(teardowns, teardown_Leader_1)
	testObj_Leader_1 := &testReplicationClusterMember{
		streamingClient_Leader_1,
		distributedLoci_Leader_1,
	}

	followerCount_1 := 3
	for i := 0; i < followerCount_1; i++ {
		streamingClient_Follower, _, distributedLoci_Follower, teardown_Follower := setupTestStreamingServer(t,
			FollowerRule,
			fmt.Sprintf("distributed-locus-follower-1-%d", i),
			nil)
		teardowns = append(teardowns, teardown_Follower)
		lociMap[testObj_Leader_1] = append(lociMap[testObj_Leader_1], &testReplicationClusterMember{
			streamingClient_Follower,
			distributedLoci_Follower,
		})

		err := distributedLoci_Leader_1.Join(distributedLoci_Follower.config.Distributed.StreamLayer.Addr().String(), distributedLoci_Follower.config.Distributed.Rule)
		require.NoError(t, err)
	}

	streamingClient_Leader_2, _, distributedLoci_Leader_2, teardown_Leader_2 := setupTestStreamingServer(t,
		LeaderRule,
		"distributed-locus-leader-2",
		&ring.Config{
			MemberType:    ring.ShardMember,
			SeedAddresses: []string{distributedLoci_Leader_1.ring.BindAddr},
		})
	teardowns = append(teardowns, teardown_Leader_2)
	testObj_Leader_2 := &testReplicationClusterMember{
		streamingClient_Leader_2,
		distributedLoci_Leader_2,
	}

	followerCount_2 := 4
	for i := 0; i < followerCount_2; i++ {
		streamingClient_Follower, _, distributedLoci_Follower, teardown_Follower := setupTestStreamingServer(t,
			FollowerRule,
			fmt.Sprintf("distributed-locus-follower-1-%d", i),
			nil)
		teardowns = append(teardowns, teardown_Follower)
		lociMap[testObj_Leader_2] = append(lociMap[testObj_Leader_2], &testReplicationClusterMember{
			streamingClient_Follower,
			distributedLoci_Follower,
		})

		err := distributedLoci_Leader_2.Join(distributedLoci_Follower.config.Distributed.StreamLayer.Addr().String(), distributedLoci_Follower.config.Distributed.Rule)
		require.NoError(t, err)
	}
	// END: setup the ring members

	// START: setup the load balancer configuration
	ports := dynaport.Get(2)

	id := "load-balancer"

	ringInstance, err := ring.NewRing(ring.Config{
		NodeName:         fmt.Sprintf("replication-%s", id),
		BindAddr:         fmt.Sprintf("localhost:%d", ports[1]),
		RPCPort:          ports[0],
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
	streaming_api.ResolverHelperClient,
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

	serverListener := mux.Match(cmux.Any())

	// Configure the server’s TLS credentials.
	serverTLSConfig, err := SetupTLSConfig(TLSConfig{
		CertFile:      ServerCertFile,
		KeyFile:       ServerKeyFile,
		CAFile:        CAFile,
		ServerAddress: serverListener.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	authorizer := newAuth(
		ACLModelFile,
		ACLPolicyFile,
	)

	lociLn := mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Compare(b, []byte{byte(RingRPC)}) == 0
	})

	// set necessary configuration for ring.
	if ringConfig != nil {
		ringConfig.RPCPort = ports[0]
	}
	locusInstance, locusTeardown := setupTestDistributedLoci_LoadBalancer(t, rule, localId, ringConfig, lociLn)

	cfg := &ServerConfig{
		StreamingConfig: &StreamingConfig{
			Locus:      locusInstance,
			Authorizer: authorizer,
		},
		ResolverHelperConfig: &ResolverHelperConfig{
			GetServerer: locusInstance,
		},
	}

	gRPCServer, err := NewServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		gRPCServer.Serve(serverListener)
	}()

	tlsConfig, err := SetupTLSConfig(TLSConfig{
		CertFile: RootClientCertFile,
		KeyFile:  RootClientKeyFile,
		CAFile:   CAFile,
		Server:   false,
	})
	require.NoError(t, err)

	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	conn, err := grpc.Dial(serverListener.Addr().String(), opts...)
	require.NoError(t, err)

	streamingClient := streaming_api.NewStreamingClient(conn)
	resolverHelperClient := streaming_api.NewResolverHelperClient(conn)

	go mux.Serve()

	return streamingClient, resolverHelperClient, locusInstance, func() {
		mux.Close()
		gRPCServer.Stop()
		conn.Close()
		rpcListener.Close()
		lociLn.Close()
		serverListener.Close()
		locusInstance.Shutdown()
		locusTeardown()
	}
}

func setupTestDistributedLoci_LoadBalancer(t *testing.T,
	rule ParticipationRule,
	localId string,
	ringConfig *ring.Config,
	listener net.Listener,
) (*DistributedLoci, func()) {
	t.Helper()

	dataDir, err := ioutil.TempDir("", "distributed-locus-test")
	require.NoError(t, err)

	ports := dynaport.Get(1)

	c := Config{}
	c.Distributed.StreamLayer = &LocusStreamLayer{
		listener,
		nil,
		nil,
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	c.Distributed.RPCAddress = fmt.Sprintf("localhost:%s", port)
	c.Distributed.LocalID = localId
	c.Distributed.Rule = rule
	pointcronConfig := pointcron.Config{}
	pointcronConfig.CloseTimeout = 3 * time.Second
	pointcronConfig.TickTime = time.Second
	c.Point.pointScheduler = pointcron.NewPointScheduler(pointcronConfig)
	c.Point.pointScheduler.StartAsync()

	if ringConfig != nil {
		nodeName := ringConfig.NodeName
		if nodeName == "" {
			nodeName = fmt.Sprintf("replication-%s", localId)
		}
		bindAddr := ringConfig.BindAddr
		if bindAddr == "" {
			bindAddr = fmt.Sprintf("localhost:%d", ports[0])
		}
		rpcPort := ringConfig.RPCPort
		if rpcPort == 0 {
			resolvedRPC, err := net.ResolveTCPAddr("tcp", listener.Addr().String())
			require.NoError(t, err)
			rpcPort = resolvedRPC.Port
		}
		virtualNodeCount := ringConfig.VirtualNodeCount
		if virtualNodeCount == 0 {
			virtualNodeCount = 3
		}
		seedAddresses := ringConfig.SeedAddresses
		if seedAddresses == nil {
			seedAddresses = []string{}
		}
		ringInstance, err := ring.NewRing(ring.Config{
			NodeName:         nodeName,
			BindAddr:         bindAddr,
			RPCPort:          rpcPort,
			VirtualNodeCount: virtualNodeCount,
			SeedAddresses:    seedAddresses,
		})
		require.NoError(t, err)
		c.Distributed.Ring = ringInstance
	}

	distributedLoci, err := NewDistributedLoci(dataDir, c)
	require.NoError(t, err)

	return distributedLoci, func() {
		defer os.RemoveAll(dataDir)

		err := distributedLoci.Shutdown()
		require.NoError(t, err)
	}
}
