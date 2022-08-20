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

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"

	"google.golang.org/protobuf/proto"
)

type DistributedLoci struct {
	config Config
	loci   *LociManager

	arc *Arc

	mu sync.Mutex
}

func NewDistributedLoci(dataDir string, config Config) (
	*DistributedLoci,
	error,
) {
	d := &DistributedLoci{
		config: config,
	}
	if err := d.setupLociManager(dataDir); err != nil {
		return nil, err
	}
	arcConfig := ArcConfig{
		StreamLayer: config.Distributed.StreamLayer,
		fsm:         &fsm{d.loci},
	}
	d.arc = NewArc(arcConfig)
	return d, nil
}

func (d *DistributedLoci) setupLociManager(dataDir string) error {
	lociDir := filepath.Join(dataDir, "loci")
	// Create a hierarchy of directories if necessary
	if err := os.MkdirAll(lociDir, 0755); err != nil {
		return err
	}
	var err error
	d.loci, err = NewLociManager(lociDir, d.config)
	return err
}

func (d *DistributedLoci) GetLoci() []string {
	return d.loci.GetLoci()
}

func (d *DistributedLoci) GetPoints(locusId string) []string {
	return d.loci.GetPoints(locusId)
}

func (d *DistributedLoci) Append(locusId string, pointId string, b []byte) (pos uint64, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	apply, err := d.apply(AppendRequestType, &streaming_api.ProduceRequest{Locus: locusId, Point: pointId, Frame: b})
	if err != nil {
		return 0, err
	}
	return apply.(*streaming_api.ProduceResponse).Offset, nil
}

func (d *DistributedLoci) apply(reqType RequestType, req proto.Message) (
	interface{},
	error,
) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	commandPromise := d.arc.Apply(buf.Bytes(), timeout)
	if err := commandPromise.Error(); err != nil {
		return nil, err
	}
	res := commandPromise.Response().(*CommandResponse)
	return res.Response, nil
}

func (d *DistributedLoci) Read(locusId string, pointId string, pos uint64) ([]byte, error) {
	return d.loci.Read(locusId, pointId, pos)
}

func (d *DistributedLoci) ReadAt(locusId string, pointId string, b []byte, off int64) (int, error) {
	return d.loci.ReadAt(locusId, pointId, b, off)
}

func (d *DistributedLoci) Close(locusId string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.loci.Close(locusId)
}

func (d *DistributedLoci) ClosePoint(locusId string, pointId string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.loci.ClosePoint(locusId, pointId)
}

func (d *DistributedLoci) CloseAll() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.loci.CloseAll()
}

func (d *DistributedLoci) Remove(locusId string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.loci.Remove(locusId)
}

func (d *DistributedLoci) RemoveAll() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.loci.RemoveAll()
}

func (d *DistributedLoci) Join(rpcAddr string, vNodeCount int) error {
	return d.arc.Join(rpcAddr, vNodeCount)
}

func (d *DistributedLoci) Leave(rpcAddr string) error {
	return d.arc.Leave(rpcAddr)
}

type LocusStreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *LocusStreamLayer {
	return &LocusStreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

const RingRPC = 1

func (s *LocusStreamLayer) Dial(address ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(address))
	if err != nil {
		return nil, err
	}
	// identify to mux this is a raft rpc
	_, err = conn.Write([]byte{byte(RingRPC)})
	if err != nil {
		return nil, err
	}
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, err
}

func (s *LocusStreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if bytes.Compare([]byte{byte(RingRPC)}, b) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
	}
	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}
	return conn, nil
}

func (s *LocusStreamLayer) Close() error {
	return s.ln.Close()
}

func (s *LocusStreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}

var _ FSM = (*fsm)(nil)

type fsm struct {
	loci *LociManager
}

// Apply Invokes this method after committing a log entry.
func (l *fsm) Apply(record *CommandRequest) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return l.applyAppend(buf[1:])
	}
	return nil
}

func (l *fsm) applyAppend(b []byte) interface{} {
	var req streaming_api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	offset, err := l.loci.Append(req.GetLocus(), req.Point, req.GetFrame())
	if err != nil {
		return err
	}
	return &streaming_api.ProduceResponse{Offset: offset}
}
