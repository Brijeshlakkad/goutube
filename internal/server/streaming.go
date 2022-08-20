package server

import (
	"io"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
)

type StreamingManager struct {
	streaming_api.UnimplementedStreamingServer
	*StreamingConfig
}

const (
	objectWildCard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type StreamingConfig struct {
	Locus      Locus
	Authorizer Authorizer
}

func (s *StreamingManager) ProduceStream(stream streaming_api.Streaming_ProduceStreamServer) error {
	if err := s.Authorizer.Authorize(
		subject(stream.Context()),
		objectWildCard,
		produceAction,
	); err != nil {
		return err
	}
	points := make(map[string]bool)
	defer (func() {
		for pointId, _ := range points {
			_ = s.Locus.ClosePoint(pointId)
		}
	})()
	var lastOffset uint64
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if err := stream.SendAndClose(&streaming_api.ProduceResponse{Offset: lastOffset}); err != nil {
				return err
			}
			return nil
		}
		if err != nil {
			return err
		}
		var pointId string = req.GetPoint()

		points[pointId] = true

		if lastOffset, err = s.Locus.Append(pointId, req.GetFrame()); err != nil {
			return err
		}
	}
}

func (s *StreamingManager) ConsumeStream(req *streaming_api.ConsumeRequest, stream streaming_api.Streaming_ConsumeStreamServer) error {
	if err := s.Authorizer.Authorize(
		subject(stream.Context()),
		objectWildCard,
		consumeAction,
	); err != nil {
		return err
	}
	pointId := req.GetPoint()
	defer s.Locus.ClosePoint(pointId)
	off := int64(0)
	lenWidth := 8
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			buf := make([]byte, lenWidth)
			n, err := s.Locus.ReadAt(pointId, buf, off)
			if err != nil {
				return nil
			}
			off += int64(n)

			size := enc.Uint64(buf)
			buf = make([]byte, size)
			n, err = s.Locus.ReadAt(pointId, buf, off)
			if err != nil {
				return err
			}
			off += int64(n)

			if err := stream.Send(&streaming_api.ConsumeResponse{Frame: buf}); err != nil {
				return err
			}
		}
	}
}

func NewStreamingServer(config *StreamingConfig) (*StreamingManager, error) {
	return &StreamingManager{
		StreamingConfig: config,
	}, nil
}
