package server

import (
	"bufio"
	"context"
	"io"
	"log"

	api "github.com/Brijeshlakkad/goutube/api/v1"
	reader "github.com/Brijeshlakkad/goutube/pkg/reader"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"go.opencensus.io/plugin/ocgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type StreamingManager struct {
	api.UnimplementedStreamingServer
	*Config
}

const (
	objectWildCard = "*"
	consumeAction  = "consume"
)

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type Config struct {
	Authorizer Authorizer
}

func (s *StreamingManager) ConsumeStream(req *api.ConsumeRequest, stream api.Streaming_ConsumeStreamServer) error {
	if err := s.Authorizer.Authorize(
		subject(stream.Context()),
		objectWildCard,
		consumeAction,
	); err != nil {
		return err
	}
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
