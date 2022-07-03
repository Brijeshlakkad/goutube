package server

import (
	"bufio"
	"context"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"testing"

	api "github.com/Brijeshlakkad/goutube/api/v1"
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
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.StreamingClient,
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
	api.StreamingClient,
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

	client := api.NewStreamingClient(conn)

	if fn != nil {
		fn()
	}
	return client, cfg, func() {
		gRPCServer.Stop()
		conn.Close()
		l.Close()
	}
}

func testProduceConsumeStream(t *testing.T, client api.StreamingClient, config *Config) {
	stream, err := client.ProduceStream(context.Background())
	require.NoError(t, err)

	// File url (will be path in the future)
	file, err := os.Open("../../test/resources/sample_1")
	defer file.Close()
	require.NoError(t, err)

	fileScanner := bufio.NewScanner(file)

	fileScanner.Split(bufio.ScanLines)

	for fileScanner.Scan() {
		err := stream.Send(&api.ProduceRequest{Locus: locusId, Point: pointId, Frame: fileScanner.Bytes()})
		require.NoError(t, err)
	}

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, 1, len(resp.Points))
	require.Equal(t, locusId, resp.Points[0].Locus)
	require.Equal(t, pointId, resp.Points[0].Point)

	resStream, err := client.ConsumeStream(context.Background(), &api.ConsumeRequest{Locus: locusId, Point: pointId})
	if err != nil {
		log.Fatalf("error while calling ConsumeStream RPC: %v", err)
	}
	for {
		_, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
	}
}
