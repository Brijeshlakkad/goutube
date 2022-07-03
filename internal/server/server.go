package server

import (
	"context"
	"encoding/binary"
	"io"

	api "github.com/Brijeshlakkad/goutube/api/v1"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"go.opencensus.io/plugin/ocgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

var (
	enc = binary.BigEndian
)

type StreamingManager struct {
	api.UnimplementedStreamingServer
	*Config
}

const (
	objectWildCard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type Config struct {
	LociManager LociManager
	Authorizer  Authorizer
}

func (s *StreamingManager) ProduceStream(stream api.Streaming_ProduceStreamServer) error {
	if err := s.Authorizer.Authorize(
		subject(stream.Context()),
		objectWildCard,
		produceAction,
	); err != nil {
		return err
	}
	points := make(map[string]*api.PointId)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			pointIds := make([]*api.PointId, 0, len(points))
			for _, pointId := range points {
				if err = s.LociManager.ClosePoint(pointId.Locus, pointId.Point); err != nil {
					return err
				}
				pointIds = append(pointIds, pointId)
			}
			if err := stream.SendAndClose(&api.ProduceResponse{Points: pointIds}); err != nil {
				return err
			}
			return nil
		}
		if err != nil {
			return err
		}
		var locusId string = req.GetLocus()
		var pointId string = req.GetPoint()
		if _, ok := points[req.GetPoint()]; !ok {
			// Check if the file is opened
			locusId, pointId, err = s.LociManager.AddPoint(req.GetLocus(), req.GetPoint(), true)
			if err != nil {
				return err
			}
			points[pointId] = &api.PointId{Locus: locusId, Point: pointId}
		}

		if _, err = s.LociManager.Append(locusId, pointId, req.GetFrame()); err != nil {
			return err
		}
	}
}

func (s *StreamingManager) ConsumeStream(req *api.ConsumeRequest, stream api.Streaming_ConsumeStreamServer) error {
	if err := s.Authorizer.Authorize(
		subject(stream.Context()),
		objectWildCard,
		consumeAction,
	); err != nil {
		return err
	}
	locusId, pointId, err := s.LociManager.AddPoint(req.GetLocus(), req.GetPoint(), true)
	if err != nil {
		return err
	}
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

			if err := stream.Send(&api.ConsumeResponse{Frame: buf}); err != nil {
				return err
			}
		}
	}
}

func NewStreamingServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	sm := &StreamingManager{
		Config: config,
	}
	opts = append(opts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_auth.StreamServerInterceptor(authenticate),
			)),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_auth.UnaryServerInterceptor(authenticate),
			)),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	)
	gRPCServer := grpc.NewServer(opts...)
	api.RegisterStreamingServer(gRPCServer, sm)
	return gRPCServer, nil
}

type LociManager interface {
	Open(string, string) error
	AddPoint(string, string, bool) (string, string, error)
	Append(string, string, []byte) (uint64, error)
	Read(string, string, uint64) ([]byte, error)
	ReadAt(string, string, []byte, int64) (int, error)
	ClosePoint(string, string) error
}

// Interceptor reading the subject out of the client’s cert and writing it to the RPC’s context.
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
