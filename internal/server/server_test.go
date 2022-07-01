package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"testing"

	api "github.com/Brijeshlakkad/goutube/api/v1"
	"github.com/Brijeshlakkad/goutube/internal/auth"
	"github.com/Brijeshlakkad/goutube/internal/config"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestServer(t *testing.T) {
	client, teardown := setupServer(t, nil)
	defer teardown()
	testConsumeStream(t, client)
}

func setupServer(t *testing.T, fn func()) (
	api.StreamingClient,
	func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// Configure the server’s TLS credentials.
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	authorizer := auth.New(
		config.ACLModelFile,
		config.ACLPolicyFile,
	)

	cfg := &Config{
		Authorizer: authorizer,
	}

	gRPCServer, err := NewStreamingServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		gRPCServer.Serve(l)
	}()

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.RootClientCertFile,
		KeyFile:  config.RootClientKeyFile,
		CAFile:   config.CAFile,
		Server:   false,
	})
	require.NoError(t, err)

	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	conn, err := grpc.Dial(l.Addr().String(), opts...)
	require.NoError(t, err)

	client := api.NewStreamingClient(conn)

	if fn != nil {
		fn()
	}
	return client, func() {
		gRPCServer.Stop()
		conn.Close()
		l.Close()
	}
}

func testConsumeStream(t *testing.T, client api.StreamingClient) {
	resStream, err := client.ConsumeStream(context.Background(), &api.ConsumeRequest{Url: "../../test/resources/sample_1"})
	if err != nil {
		log.Fatalf("error while calling ConsumeStream RPC: %v", err)
	}
	for {
		res, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		fmt.Println(string(res.Frame))
	}
}
