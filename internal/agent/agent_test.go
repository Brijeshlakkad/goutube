package agent_test

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
	"github.com/Brijeshlakkad/goutube/internal/agent"
	"github.com/Brijeshlakkad/goutube/internal/config"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	var agents []*agent.Agent
	for i := 0; i < 2; i++ {
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := ioutil.TempDir("", "agentInstance-test-loci")
		require.NoError(t, err)

		var startJoinAddrs []string
		bootstrap := true
		if i != 0 {
			bootstrap = false
			startJoinAddrs = append(
				startJoinAddrs,
				agents[0].Config.BindAddr,
			)
		}

		agentInstance, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("%d", i),
			SeedAddresses:   startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
			Bootstrap:       bootstrap,
		})
		require.NoError(t, err)

		agents = append(agents, agentInstance)
	}
	defer func() {
		for _, agentInstance := range agents {
			err := agentInstance.Shutdown()
			require.NoError(t, err)
			require.NoError(t,
				os.RemoveAll(agentInstance.Config.DataDir),
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

func client(
	t *testing.T,
	agent *agent.Agent,
	tlsConfig *tls.Config,
) streaming_api.StreamingClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)
	conn, err := grpc.Dial(rpcAddr, opts...)
	require.NoError(t, err)
	client := streaming_api.NewStreamingClient(conn)
	return client
}
