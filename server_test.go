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

func TestStreamingManager_ProduceStream_And_ConsumeStream(t *testing.T) {
	client, _, _, teardown := setupTest(t, nil)
	defer teardown()

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

func setupTest(t *testing.T, fn func()) (
	streaming_api.StreamingClient,
	streaming_api.ResolverHelperClient,
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
	locusInstance, err := NewDistributedLoci(dir, locusConfig)
	require.NoError(t, err)

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
	resolverHelperClient := streaming_api.NewResolverHelperClient(conn)

	if fn != nil {
		fn()
	}
	return streamingClient, resolverHelperClient, cfg, func() {
		gRPCServer.Stop()
		conn.Close()
		l.Close()
		locusInstance.Remove()
	}
}
