package goutube

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestAgent_Replication(t *testing.T) {
	serverTLSConfig, err := SetupTLSConfig(TLSConfig{
		CertFile:      ServerCertFile,
		KeyFile:       ServerKeyFile,
		CAFile:        CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	peerTLSConfig, err := SetupTLSConfig(TLSConfig{
		CertFile:      RootClientCertFile,
		KeyFile:       RootClientKeyFile,
		CAFile:        CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	agents := setupTestAgent(t, 2, func(i int, agents []*Agent, config *AgentConfig) {
		var startJoinAddrs []string
		var leaderJoinAddrs []string
		rule := LeaderRule
		if i != 0 {
			rule = LeaderFollowerRule
			startJoinAddrs = append(
				startJoinAddrs,
				agents[0].BindAddr,
			)
			replicationAddr, err := agents[0].ReplicationRPCAddr()
			require.NoError(t, err)

			leaderJoinAddrs = append(
				leaderJoinAddrs,
				replicationAddr,
			)
		}
		config.SeedAddresses = startJoinAddrs
		config.LeaderAddresses = leaderJoinAddrs
		config.Rule = rule
		config.ServerTLSConfig = serverTLSConfig
		config.PeerTLSConfig = peerTLSConfig
	})

	defer func() {
		for _, agentInstance := range agents {
			err := agentInstance.Shutdown()
			require.NoError(t, err)
			require.NoError(t,
				os.RemoveAll(agentInstance.DataDir),
			)
		}
	}()

	time.Sleep(3 * time.Second)

	var (
		pointId = "sample_file"
		lines   = 10
	)

	leaderClient := client(t, agents[0], peerTLSConfig)
	stream, err := leaderClient.ProduceStream(context.Background())
	require.NoError(t, err)

	for i := 0; i < lines; i++ {
		err := stream.Send(&streaming_api.ProduceRequest{Point: pointId, Frame: []byte(fmt.Sprintln(i))})
		require.NoError(t, err)
	}

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	// Wait for the servers to replicate the record before consuming with the leader client.
	time.Sleep(3 * time.Second)

	// test consume stream
	followerClient := client(t, agents[1], peerTLSConfig)
	resStream, err := followerClient.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: pointId})
	if err != nil {
		log.Fatalf("error while calling ConsumeStream RPC: %v", err)
	}
	i := 0
	for i = 0; i < lines; i++ {
		resp, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		b := resp.GetFrame()
		require.Equal(t, fmt.Sprintln(i), string(b))
	}
	require.Equal(t, lines, i)
}

func TestAgent_ParticipationRule(t *testing.T) {
	serverTLSConfig, err := SetupTLSConfig(TLSConfig{
		CertFile:      ServerCertFile,
		KeyFile:       ServerKeyFile,
		CAFile:        CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	peerTLSConfig, err := SetupTLSConfig(TLSConfig{
		CertFile:      RootClientCertFile,
		KeyFile:       RootClientKeyFile,
		CAFile:        CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	agents := setupTestAgent(t, 2, func(i int, agents []*Agent, config *AgentConfig) {
		var startJoinAddrs []string
		var leaderJoinAddrs []string
		rule := LeaderRule
		if i != 0 {
			rule = LeaderRule
			startJoinAddrs = append(
				startJoinAddrs,
				agents[0].BindAddr,
			)
			replicationAddr, err := agents[0].ReplicationRPCAddr()
			require.NoError(t, err)

			leaderJoinAddrs = append(
				leaderJoinAddrs,
				replicationAddr,
			)
		}
		config.SeedAddresses = startJoinAddrs
		config.LeaderAddresses = leaderJoinAddrs
		config.Rule = rule
		config.ServerTLSConfig = serverTLSConfig
		config.PeerTLSConfig = peerTLSConfig
	})

	defer func() {
		for _, agentInstance := range agents {
			err := agentInstance.Shutdown()
			require.NoError(t, err)
			require.NoError(t,
				os.RemoveAll(agentInstance.DataDir),
			)
		}
	}()

	time.Sleep(3 * time.Second)

	var (
		pointId = "sample_file"
		lines   = 10
	)

	leaderClient := client(t, agents[0], peerTLSConfig)
	stream, err := leaderClient.ProduceStream(context.Background())
	require.NoError(t, err)

	for i := 0; i < lines; i++ {
		err := stream.Send(&streaming_api.ProduceRequest{Point: pointId, Frame: []byte(fmt.Sprintln(i))})
		require.NoError(t, err)
	}

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	// Wait for the servers to replicate the record before consuming with the leader client.
	time.Sleep(3 * time.Second)

	// Consume stream
	// Even if the second agent has the address of the first agent as a leader, this shouldn't replicate any stream.
	anotherAgentClient := client(t, agents[1], peerTLSConfig)
	resStream, err := anotherAgentClient.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: pointId})
	if err != nil {
		log.Fatalf("error while calling ConsumeStream RPC: %v", err)
	}
	i := 0
	for i = 0; i < lines; i++ {
		_, err = resStream.Recv()
		require.Error(t, io.EOF)
		break
	}
}

func setupTestAgent(t *testing.T, count int, callBefore func(i int, agent []*Agent, config *AgentConfig)) []*Agent {
	var agents []*Agent
	for i := 0; i < count; i++ {
		ports := dynaport.Get(3)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]
		replicationPort := ports[2]

		dataDir, err := ioutil.TempDir("", "agentInstance-test-loci")
		require.NoError(t, err)

		config := AgentConfig{
			NodeName:        fmt.Sprintf("%d", i),
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			ReplicationPort: replicationPort,
			DataDir:         dataDir,
			ACLModelFile:    ACLModelFile,
			ACLPolicyFile:   ACLPolicyFile,
		}

		callBefore(i, agents, &config)

		agent, err := NewAgent(config)
		require.NoError(t, err)

		agents = append(agents, agent)
	}
	return agents
}

func client(
	t *testing.T,
	agent *Agent,
	tlsConfig *tls.Config,
) streaming_api.StreamingClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.RPCAddr()
	require.NoError(t, err)
	conn, err := grpc.Dial(rpcAddr, opts...)
	require.NoError(t, err)
	client := streaming_api.NewStreamingClient(conn)
	return client
}
