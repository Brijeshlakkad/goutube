package goutube

import (
	"context"
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
	Locus      LocusHelper
	Authorizer *authorizer
}

type LocusHelper interface {
	Append(*streaming_api.ProduceRequest) (uint64, error)
	GetMetadata(string) (PointMetadata, error)
	Read(string, uint64) (uint64, []byte, error)
	ClosePoint(string) error
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

	off := req.GetOffset()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			nextOff, buf, err := s.Locus.Read(pointId, off)
			if err != nil {
				return nil
			}
			off = nextOff

			if err := stream.Send(&streaming_api.ConsumeResponse{Frame: buf}); err != nil {
				return err
			}
		}
	}
}

func (s *StreamingManager) GetMetadata(ctx context.Context, req *streaming_api.MetadataRequest) (*streaming_api.MetadataResponse, error) {
	metadata, err := s.Locus.GetMetadata(req.GetPoint())
	if err != nil {
		return nil, err
	}
	return &streaming_api.MetadataResponse{
		Size: metadata.size,
	}, nil
}

func NewStreamingServer(config *StreamingConfig) (*StreamingManager, error) {
	return &StreamingManager{
		StreamingConfig: config,
	}, nil
}
