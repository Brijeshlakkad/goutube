package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"testing"

	api "github.com/Brijeshlakkad/goutube/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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

	gRPCServer, err := NewStreamingServer()
	require.NoError(t, err)

	go func() {
		gRPCServer.Serve(l)
	}()

	opts := []grpc.DialOption{grpc.WithInsecure()}
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
