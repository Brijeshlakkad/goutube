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

	"github.com/Brijeshlakkad/goutube/internal/mandala"
)

type DistributedLoci struct {
	config  Config
	loci    *LociManager
	mandala *mandala.Cluster

	mu sync.Mutex
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
	if err := l.setupMandala(); err != nil {
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

func (l *DistributedLoci) setupMandala() error {
	var err error
	l.mandala, err = mandala.NewCluster()
	l.mandala.StreamLayer = l.config.Distributed.StreamLayer
	return err
}

func (l *DistributedLoci) GetLoci() []string {
	return l.loci.GetLoci()
}

func (l *DistributedLoci) GetPoints(locusId string) []string {
	return l.loci.GetPoints(locusId)
}

func (l *DistributedLoci) Append(locusId string, pointId string, b []byte) (pos uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.loci.Append(locusId, pointId, b)
}

func (l *DistributedLoci) Read(locusId string, pointId string, pos uint64) ([]byte, error) {
	return l.loci.Read(locusId, pointId, pos)
}

func (l *DistributedLoci) ReadAt(locusId string, pointId string, b []byte, off int64) (int, error) {
	return l.loci.ReadAt(locusId, pointId, b, off)
}

func (l *DistributedLoci) Close(locusId string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.loci.Close(locusId); err != nil {
		return err
	}
	return l.mandala.Close()
}

func (l *DistributedLoci) ClosePoint(locusId string, pointId string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.loci.ClosePoint(locusId, pointId)
}

func (l *DistributedLoci) CloseAll() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.loci.CloseAll()
}

func (l *DistributedLoci) Remove(locusId string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.loci.Remove(locusId)
}

func (l *DistributedLoci) RemoveAll() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.loci.RemoveAll()
}

func (l *DistributedLoci) Join(id, addr string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.mandala.Closed {
		return nil
	}

	return l.mandala.AddPeer(id, addr)
}

func (l *DistributedLoci) Leave(id string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.mandala.RemovePeer(id)
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
