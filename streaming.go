package goutube

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

type StreamingConfig struct {
	Locus      *DistributedLoci
	Authorizer *authorizer
}

func (s *StreamingManager) ProduceStream(stream streaming_api.Streaming_ProduceStreamServer) error {
	if err := s.Authorizer.Authorize(
		subject(stream.Context()),
		objectWildCard,
		produceAction,
	); err != nil {
		return err
	}
	points := make(map[string]uint64)
	var lastOffset uint64
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			var resp []*streaming_api.Record
			for pointId, pointOffset := range points {
				record := &streaming_api.Record{
					Point:  pointId,
					Offset: pointOffset,
				}
				_ = s.Locus.ClosePoint(pointId)
				resp = append(resp, record)
			}
			if err := stream.SendAndClose(&streaming_api.ProduceResponse{Records: resp}); err != nil {
				return err
			}
			return nil
		}
		if err != nil {
			return err
		}
		if lastOffset, err = s.Locus.Append(req); err != nil {
			return err
		}
		points[req.GetPoint()] = lastOffset
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
	off := uint64(0)
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
			off += uint64(n)

			size := enc.Uint64(buf)
			buf = make([]byte, size)
			n, err = s.Locus.ReadAt(pointId, buf, off)
			if err != nil {
				return err
			}
			off += uint64(n)

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
