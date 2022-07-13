package locus

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
)

type DistributedLoci struct {
	config Config
	loci   *LociManager
	*replicator
	*StreamLayer
}

func NewDistributedLoci(dataDir string, config Config) (
	*DistributedLoci,
	error,
) {
	l := &DistributedLoci{
		config: config,
	}
	if err := l.setupLociManager(dataDir); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *DistributedLoci) setupLociManager(dataDir string) error {
	lociDir := filepath.Join(dataDir, "loci")
	// Create a hierarchy of directories if necessary
	if err := os.MkdirAll(lociDir, 0755); err != nil {
		return err
	}
	var err error
	l.loci, err = NewLociManager(lociDir, l.config)
	return err
}

func (l *DistributedLoci) AddPoint(locusId string, pointId string, open bool) (string, string, error) {
	return l.loci.AddPoint(locusId, pointId, open)
}

func (l *DistributedLoci) GetLoci() []string {
	return l.loci.GetLoci()
}

func (l *DistributedLoci) GetPoints(locusId string) []string {
	return l.loci.GetPoints(locusId)
}

func (l *DistributedLoci) get(locusId string) (*Locus, error) {
	return l.loci.get(locusId)
}

func (l *DistributedLoci) Open(locusId string, pointId string) error {
	return l.loci.Open(locusId, pointId)
}

func (l *DistributedLoci) Append(locusId string, pointId string, b []byte) (pos uint64, err error) {
	return l.loci.Append(locusId, pointId, b)
}

func (l *DistributedLoci) Read(locusId string, pointId string, pos uint64) ([]byte, error) {
	return l.loci.Read(locusId, pointId, pos)
}

func (l *DistributedLoci) ReadAt(locusId string, pointId string, b []byte, off int64) (int, error) {
	return l.loci.ReadAt(locusId, pointId, b, off)
}

func (l *DistributedLoci) Close(locusId string) error {
	return l.loci.Close(locusId)
}

func (l *DistributedLoci) ClosePoint(locusId string, pointId string) error {
	return l.loci.ClosePoint(locusId, pointId)
}

func (l *DistributedLoci) CloseAll() error {
	return l.loci.CloseAll()
}

func (l *DistributedLoci) Remove(locusId string) error {
	return l.loci.Remove(locusId)
}

func (l *DistributedLoci) RemoveAll() error {
	return l.loci.RemoveAll()
}

type replicator struct {
	logger *zap.Logger

	mu      sync.Mutex
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

func (r *replicator) init() {
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

func (l *replicator) Join(id, addr string) error {
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

	return nil
}

func (l *replicator) Leave(id string) error {
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

func (r *replicator) Close() error {
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
func (r *replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

const LociRPC = 1

func (s *StreamLayer) Dial(
	addr string,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	// identify to mux this is a loci rpc
	_, err = conn.Write([]byte{byte(LociRPC)})
	if err != nil {
		return nil, err
	}
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, err
}

func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if bytes.Compare([]byte{byte(LociRPC)}, b) != 0 {
		return nil, fmt.Errorf("not a loci rpc")
	}
	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}
	return conn, nil
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
