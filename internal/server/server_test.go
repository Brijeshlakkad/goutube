package server

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"testing"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/Brijeshlakkad/goutube/internal/auth"
	"github.com/Brijeshlakkad/goutube/internal/config"
	"github.com/Brijeshlakkad/goutube/internal/locus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	locusId = "goutube-client"
	pointId = "sample_file"
	lines   = 10
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client streaming_api.StreamingClient,
		config *Config,
	){
		"produce/consume stream succeeds": testProduceConsumeStream,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func()) (
	streaming_api.StreamingClient,
	*Config,
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

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	lociManager, err := locus.NewLociManager(dir, locus.Config{})
	require.NoError(t, err)

	cfg := &Config{
		LociManager: lociManager,
		Authorizer:  authorizer,
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

	client := streaming_api.NewStreamingClient(conn)

	if fn != nil {
		fn()
	}
	return client, cfg, func() {
		gRPCServer.Stop()
		conn.Close()
		l.Close()
		err := lociManager.RemoveAll()
		fmt.Println(err)
	}
}

func testProduceConsumeStream(t *testing.T, client streaming_api.StreamingClient, config *Config) {
	stream, err := client.ProduceStream(context.Background())
	require.NoError(t, err)

	for i := 0; i < lines; i++ {
		err := stream.Send(&streaming_api.ProduceRequest{Locus: locusId, Point: pointId, Frame: []byte(fmt.Sprintln(i))})
		require.NoError(t, err)
	}

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, 1, len(resp.Points))
	require.Equal(t, locusId, resp.Points[0].Locus)
	require.Equal(t, pointId, resp.Points[0].Point)

	// test consume stream
	resStream, err := client.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Locus: locusId, Point: pointId})
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
