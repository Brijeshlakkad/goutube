package goutube

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"testing"
	"time"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	pointId = "sample_file"
	lines   = 10
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		streamingClient streaming_api.StreamingClient,
		config *ServerConfig,
	){
		"produce and consume stream succeeds": testProduceConsumeStream,
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
	*ServerConfig,
	func(),
) {
	t.Helper()

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

	authorizer := newAuth(
		ACLModelFile,
		ACLPolicyFile,
	)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	locusConfig := Config{}
	locusConfig.Point.CloseTimeout = 10 * time.Second

	lociLn, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	locusConfig.Distributed.StreamLayer = NewStreamLayer(
		lociLn,
		nil,
		nil,
	)
	locusConfig.Distributed.BindAdr = "localhost:0"
	locusInstance, err := NewDistributedLoci(dir, locusConfig)
	require.NoError(t, err)

	cfg := &ServerConfig{
		StreamingConfig: &StreamingConfig{
			Locus:      locusInstance,
			Authorizer: authorizer,
		},
	}

	gRPCServer, err := NewServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		gRPCServer.Serve(l)
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
	conn, err := grpc.Dial(l.Addr().String(), opts...)
	require.NoError(t, err)

	streamingClient := streaming_api.NewStreamingClient(conn)

	if fn != nil {
		fn()
	}
	return streamingClient, cfg, func() {
		gRPCServer.Stop()
		conn.Close()
		l.Close()
		locusInstance.Remove()
	}
}

func testProduceConsumeStream(
	t *testing.T,
	client streaming_api.StreamingClient,
	config *ServerConfig,
) {
	stream, err := client.ProduceStream(context.Background())
	require.NoError(t, err)

	for i := 0; i < lines; i++ {
		err := stream.Send(&streaming_api.ProduceRequest{Point: pointId, Frame: []byte(fmt.Sprintln(i))})
		require.NoError(t, err)
	}

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, 1, len(resp.Records))
	require.Equal(t, pointId, resp.Records[0].Point)
	require.Equal(t, uint64(90), resp.Records[0].Offset)

	// test consume stream
	resStream, err := client.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: pointId})
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
