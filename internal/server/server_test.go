package server_test

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"testing"
	"time"

	replication_api "github.com/Brijeshlakkad/goutube/api/replication/v1"
	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/Brijeshlakkad/goutube/internal/auth"
	"github.com/Brijeshlakkad/goutube/internal/config"
	"github.com/Brijeshlakkad/goutube/internal/locus"
	. "github.com/Brijeshlakkad/goutube/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	locusId   = "goutube-client"
	pointId   = "sample_file"
	locusId_2 = "second-goutube-client"
	lines     = 10
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		streamingClient streaming_api.StreamingClient,
		replicationClient replication_api.ReplicationClient,
		config *Config,
	){
		"produce and consume stream succeeds": testProduceConsumeStream,
		"replicate stream succeeds":           testReplicateStream,
	} {
		t.Run(scenario, func(t *testing.T) {
			streamingClient, replicationClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, streamingClient, replicationClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func()) (
	streaming_api.StreamingClient,
	replication_api.ReplicationClient,
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

	locusConfig := locus.Config{}
	locusConfig.Point.CloseTimeout = 10 * time.Second
	lociManager, err := locus.NewLociManager(dir, locusConfig)
	require.NoError(t, err)

	cfg := &Config{
		StreamingConfig: &StreamingConfig{
			LociManager: lociManager,
			Authorizer:  authorizer,
		},
		ReplicationConfig: &ReplicationConfig{
			LociManager: lociManager,
		},
	}

	gRPCServer, err := NewServer(cfg, grpc.Creds(serverCreds))
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

	streamingClient := streaming_api.NewStreamingClient(conn)
	replicationClient := replication_api.NewReplicationClient(conn)

	if fn != nil {
		fn()
	}
	return streamingClient, replicationClient, cfg, func() {
		gRPCServer.Stop()
		conn.Close()
		l.Close()
		err := lociManager.RemoveAll()
		fmt.Println(err)
	}
}

func testProduceConsumeStream(
	t *testing.T,
	client streaming_api.StreamingClient,
	_ replication_api.ReplicationClient,
	config *Config,
) {
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

func testReplicateStream(
	t *testing.T,
	streamingClient streaming_api.StreamingClient,
	replicationClient replication_api.ReplicationClient,
	config *Config,
) {
	ctx := context.Background()
	stream, err := streamingClient.ProduceStream(ctx)
	require.NoError(t, err)

	i := 0
	for i = 0; i < lines; i++ {
		err := stream.Send(&streaming_api.ProduceRequest{Locus: locusId, Point: pointId, Frame: []byte(fmt.Sprint(i))})
		require.NoError(t, err)
	}

	for i = 0; i < lines; i++ {
		err := stream.Send(&streaming_api.ProduceRequest{Locus: locusId_2 /** Locus 2 */, Point: pointId, Frame: []byte(fmt.Sprint(i))})
		require.NoError(t, err)
	}

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, 2, len(resp.Points))

	require.Equal(t, checkPoints(resp.Points), true)

	resStream, err := replicationClient.Replicate(ctx, &replication_api.ReplicateRequest{})
	require.NoError(t, err)

	i = 0
	total := 0
	var loci map[string]bool
	loci = make(map[string]bool)
	for {
		resp, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		require.NoError(t, err)
		if _, ok := loci[resp.Locus]; !ok {
			loci[resp.Locus] = false
			i = 0
		}
		if i == lines {
			i = 0
			continue
		}
		b := resp.GetFrame()
		require.Equal(t, fmt.Sprint(i), string(b))
		i++
		total++
	}
	receivedLoci := 0
	for range loci {
		receivedLoci++
	}
	require.Equal(t, 2*lines, total)
	require.Equal(t, receivedLoci, 2)
}

func checkPoints(points []*streaming_api.PointId) bool {
	var loci []string
	loci = make([]string, 2)
	loci[0] = locusId
	loci[1] = locusId_2
	for _, point := range points {
		for locusIndex, locusId := range loci {
			if locusId == point.Locus {
				loci = append(loci[:locusIndex], loci[locusIndex+1:]...)
			}
		}
	}
	return len(loci) == 0
}
