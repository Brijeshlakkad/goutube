package server

import (
	"bufio"
	"io"
	"log"

	api "github.com/Brijeshlakkad/goutube/api/v1"
	reader "github.com/Brijeshlakkad/goutube/pkg/reader"
	"google.golang.org/grpc"
)

type StreamingManager struct {
	api.UnimplementedStreamingServer
}

func (s *StreamingManager) ConsumeStream(req *api.ConsumeRequest, stream api.Streaming_ConsumeStreamServer) error {
	// File url (will be path in the future)
	url := req.GetUrl()
	fileReader, err := reader.NewFileReader(url, 0)
	defer fileReader.File.Close()
	if err != nil {
		return err
	}
	r := bufio.NewReader(fileReader.File)
	buf := make([]byte, 0, 4*1024)

	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			n, err := r.Read(buf[:cap(buf)])
			buf = buf[:n]
			if n == 0 {
				if err == nil {
					continue
				}
				if err == io.EOF {
					return nil
				}
				log.Fatal(err)
			}
			// process buf
			if err != nil && err != io.EOF {
				log.Fatal(err)
			}
			if err = stream.Send(&api.ConsumeResponse{Frame: buf}); err != nil {
				return err
			}
		}
	}
}

func NewStreamingServer() (*grpc.Server, error) {
	sm := &StreamingManager{}

	gRPCServer := grpc.NewServer()
	api.RegisterStreamingServer(gRPCServer, sm)
	return gRPCServer, nil
}
