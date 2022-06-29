package streaming_manager_client

import (
	api "github.com/Brijeshlakkad/goutube/api/v1"
	"google.golang.org/grpc"
)

type DisconnectFunc func() error

func NewClient(grpcAddr string) (cli api.StreamingClient, disconnectFunc DisconnectFunc, err error) {
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	disconnectFunc = func() (err error) {
		if conn == nil {
			return
		}

		err = conn.Close()
		return
	}

	if err != nil {
		return
	}

	cli = api.NewStreamingClient(conn)
	return
}
