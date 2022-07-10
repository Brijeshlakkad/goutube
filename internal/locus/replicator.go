package locus

import (
	"context"
	"sync"

	replication_api "github.com/Brijeshlakkad/goutube/api/replication/v1"
	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer streaming_api.StreamingClient

	logger *zap.Logger

	mu      sync.Mutex
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (l *Replicator) Join(id, addr string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.init()

	if l.closed {
		return nil
	}

	if _, ok := l.servers[id]; ok {
		// already replicating!
	}
	l.servers[id] = make(chan struct{})

	go l.replicate(addr, l.servers[id])

	return nil
}

func (l *Replicator) Leave(id string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.init()

	if _, ok := l.servers[id]; !ok {
		return nil
	}
	close(l.servers[id])
	delete(l.servers, id)
	return nil
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
	}
	defer cc.Close()
	client := replication_api.NewReplicationClient(cc)

	ctx := context.Background()
	stream, err := client.Replicate(ctx, &replication_api.ReplicateRequest{})

	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	records := make(chan *replication_api.ReplicateResponse)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv
		}
	}()

	localStream, err := r.LocalServer.ProduceStream(ctx)
	if err != nil {
		r.logError(err, "failed to produce", addr)
		return
	}
	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			err = localStream.Send(
				&streaming_api.ProduceRequest{
					Locus: record.Locus,
					Point: record.Point,
					Frame: record.Frame,
				},
			)
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

// If users need access to the errors, a technique we can use to expose these errors is to export an error channel and send the errors into it for users to receive and handle.
func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
