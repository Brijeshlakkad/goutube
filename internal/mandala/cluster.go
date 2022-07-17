package mandala

import (
	"sync"

	"go.uber.org/zap"
)

type Cluster struct {
	*Server
	logger *zap.Logger

	StreamLayer StreamLayer
	mu          sync.Mutex
	Closed      bool
	close       chan struct{}
}

func NewCluster() (*Cluster, error) {
	return &Cluster{
		logger: zap.L().Named("replicator"),
		close:  make(chan struct{}),
	}, nil
}

func (r *Cluster) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Closed {
		return nil
	}
	r.Closed = true
	close(r.close)
	return nil
}

// If users need access to the errors, a technique we can use to expose these errors is to export an error channel and send the errors into it for users to receive and handle.
func (r *Cluster) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
