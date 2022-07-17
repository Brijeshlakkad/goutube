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
	LociManager LociManager
	Authorizer  Authorizer
}

func (s *StreamingManager) ProduceStream(stream streaming_api.Streaming_ProduceStreamServer) error {
	if err := s.Authorizer.Authorize(
		subject(stream.Context()),
		objectWildCard,
		produceAction,
	); err != nil {
		return err
	}
	points := make(map[string]map[string]bool)
	defer (func() {
		for locusId, pointIds := range points {
			for pointId, _ := range pointIds {
				_ = s.LociManager.ClosePoint(locusId, pointId)
			}
		}
	})()
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			pointIdResp := make([]*streaming_api.PointId, 0, len(points))
			for locusId, pointIds := range points {
				for pointId, _ := range pointIds {
					pointIdResp = append(pointIdResp, &streaming_api.PointId{Locus: locusId, Point: pointId})
				}
			}
			if err := stream.SendAndClose(&streaming_api.ProduceResponse{Points: pointIdResp}); err != nil {
				return err
			}
			return nil
		}
		if err != nil {
			return err
		}
		var locusId string = req.GetLocus()
		var pointId string = req.GetPoint()

		if _, locusExists := points[locusId]; !locusExists {
			points[locusId] = make(map[string]bool)
		}
		points[locusId][pointId] = true

		if _, err = s.LociManager.Append(locusId, pointId, req.GetFrame()); err != nil {
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
	locusId, pointId := req.GetLocus(), req.GetPoint()
	defer s.LociManager.ClosePoint(locusId, pointId)
	off := int64(0)
	lenWidth := 8
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			buf := make([]byte, lenWidth)
			n, err := s.LociManager.ReadAt(locusId, pointId, buf, off)
			if err != nil {
				return nil
			}
			off += int64(n)

			size := enc.Uint64(buf)
			buf = make([]byte, size)
			n, err = s.LociManager.ReadAt(locusId, pointId, buf, off)
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
