package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	api "github.com/Brijeshlakkad/goutube/api/v1"
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

	rpcPort := dynaport.Get(1)[0]

	agent, err := New(Config{
		RPCPort:         rpcPort,
		ACLModelFile:    config.ACLModelFile,
		ACLPolicyFile:   config.ACLPolicyFile,
		ServerTLSConfig: serverTLSConfig,
		Host:            "127.0.0.1",
	})
	require.NoError(t, err)

	defer func() {
		err := agent.Shutdown()
		require.NoError(t, err)
	}()

	time.Sleep(3 * time.Second)

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	clientS := client(t, agent, peerTLSConfig)
	require.NoError(t, err)

	stream, err := clientS.ConsumeStream(
		context.Background(),
		&api.ConsumeRequest{Url: "../../test/resources/sample_1"},
	)
	require.NoError(t, err)
	if err != nil {
		log.Fatalf("error while calling ConsumeStream RPC: %v", err)
	}
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		fmt.Println(string(res.Frame))
	}
}

func client(
	t *testing.T,
	agent *Agent,
	tlsConfig *tls.Config,
) api.StreamingClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)
	conn, err := grpc.Dial(rpcAddr, opts...)
	require.NoError(t, err)
	client := api.NewStreamingClient(conn)
	return client
}
