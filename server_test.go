package goutube

import (
	"context"
	"io"
	"io/ioutil"
	"net"
	"testing"
	"time"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	testPointId    = "sample_file"
	testPointLines = 10
)

func TestStreamingManager_ProduceStream_And_ConsumeStream(t *testing.T) {
	chunkSize := int(testChunkSize)
	client, _, _, teardown := setupTestServer(t, nil, uint64(chunkSize))
	defer teardown()

	stream, err := client.ProduceStream(context.Background())
	require.NoError(t, err)

	file := setupFile(t, chunkSize, testPointLines)

	for i := 0; i < testPointLines; i++ {
		b := file[i*chunkSize : (i+1)*chunkSize]
		err := stream.Send(&streaming_api.ProduceRequest{Point: testPointId, Frame: b})
		require.NoError(t, err)
	}

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, 1, len(resp.Records))
	require.Equal(t, testPointId, resp.Records[0].Point)
	require.Equal(t, uint64(2304), resp.Records[0].Offset)

	// test consume stream
	resStream, err := client.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: testPointId})
	if err != nil {
		t.Fatalf("error while calling ConsumeStream RPC: %v", err)
	}
	i := 0
	for ; ; i++ {
		resp, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		b := resp.GetFrame()
		require.Equal(t, file[i*chunkSize:(i+1)*chunkSize], b)
	}
	require.Equal(t, testPointLines, i)
}

func TestStreamingManager_ProduceStream_And_ConsumeStream_WithLimit(t *testing.T) {
	chunkSize := int(testChunkSize)
	client, _, _, teardown := setupTestServer(t, nil, uint64(chunkSize))
	defer teardown()

	stream, err := client.ProduceStream(context.Background())
	require.NoError(t, err)

	file := setupFile(t, chunkSize, testPointLines)

	for i := 0; i < testPointLines; i++ {
		b := file[i*chunkSize : (i+1)*chunkSize]
		err := stream.Send(&streaming_api.ProduceRequest{Point: testPointId, Frame: b})
		require.NoError(t, err)
	}

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, 1, len(resp.Records))
	require.Equal(t, testPointId, resp.Records[0].Point)
	require.Equal(t, uint64(2304), resp.Records[0].Offset)

	// test consume stream
	resStream, err := client.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: testPointId})
	if err != nil {
		t.Fatalf("error while calling ConsumeStream RPC: %v", err)
	}
	i := 0
	for ; ; i++ {
		resp, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		b := resp.GetFrame()
		require.Equal(t, file[i*chunkSize:(i+1)*chunkSize], b)
	}
	require.Equal(t, testPointLines, i)

	offset := 2
	// test consume stream with preset offset
	resStream, err = client.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: testPointId, Offset: uint64(chunkSize * offset)})
	if err != nil {
		t.Fatalf("error while calling ConsumeStream RPC: %v", err)
	}
	i = offset
	for ; ; i++ {
		resp, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		b := resp.GetFrame()
		require.Equal(t, file[i*chunkSize:(i+1)*chunkSize], b)
	}
	require.Equal(t, testPointLines, i)

	limit := 8
	// test consume stream with preset limit
	resStream, err = client.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: testPointId, Limit: uint64(chunkSize * limit)})
	if err != nil {
		t.Fatalf("error while calling ConsumeStream RPC: %v", err)
	}
	i = 0
	for ; ; i++ {
		resp, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		b := resp.GetFrame()
		require.Equal(t, file[i*chunkSize:(i+1)*chunkSize], b)
	}
	require.Equal(t, limit, i)

	offset = 2
	limit = 7
	// test consume stream with preset offset and limit
	resStream, err = client.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: testPointId, Offset: uint64(chunkSize * offset), Limit: uint64(chunkSize * limit)})
	if err != nil {
		t.Fatalf("error while calling ConsumeStream RPC: %v", err)
	}
	i = offset
	for ; ; i++ {
		resp, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		b := resp.GetFrame()
		require.Equal(t, file[i*chunkSize:(i+1)*chunkSize], b)
	}
	require.Equal(t, limit, i)

	offset = 1
	limit = 5
	customChunkSize := 512
	expectedI := limit * chunkSize / customChunkSize
	// test consume stream with preset chunk size, offset and limit
	resStream, err = client.ConsumeStream(context.Background(), &streaming_api.ConsumeRequest{Point: testPointId, ChunkSize: uint64(customChunkSize), Offset: uint64(chunkSize * offset), Limit: uint64(chunkSize * limit)})
	if err != nil {
		t.Fatalf("error while calling ConsumeStream RPC: %v", err)
	}
	i = offset
	for ; ; i++ {
		resp, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		b := resp.GetFrame()
		require.Equal(t, file[i*customChunkSize:(i+1)*customChunkSize], b)
	}
	require.Equal(t, expectedI+1, i) // +1 because for loop will iterate one more time and will get io.EOF error
}

func setupTestServer(t *testing.T, fn func(), chunkSize uint64) (
	streaming_api.StreamingClient,
	streaming_api.LBResolverHelperClient,
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
	locusConfig.Distributed.MaxChunkSize = chunkSize

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
	resolverHelperClient := streaming_api.NewLBResolverHelperClient(conn)

	if fn != nil {
		fn()
	}
	return streamingClient, resolverHelperClient, cfg, func() {
		gRPCServer.Stop()
		conn.Close()
		l.Close()
		locusInstance.Shutdown()
	}
}

func setupFile(t *testing.T, size int, times int) []byte {
	t.Helper()
	file := make([]byte, size*times)
	for i := 0; i < size*times; i++ {
		file[i] = byte(i)
	}
	return file
}
