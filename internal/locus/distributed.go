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
	"github.com/Brijeshlakkad/goutube/internal/locus/pointcron"

	"google.golang.org/protobuf/proto"
)

type DistributedLoci struct {
	config Config
	locus  *Locus

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
		fsm:         &fsm{d.locus},
	}
	d.arc = NewArc(arcConfig)
	return d, nil
}

func (d *DistributedLoci) setupLociManager(dataDir string) error {
	lociDir := filepath.Join(dataDir, "locus")
	// Create a hierarchy of directories if necessary
	if err := os.MkdirAll(lociDir, 0755); err != nil {
		return err
	}
	d.config.Point.pointScheduler = pointcron.NewPointScheduler(pointcron.Config{
		CloseTimeout: d.config.Point.CloseTimeout,
		TickTime:     d.config.Point.TickTime,
	})
	d.config.Point.pointScheduler.StartAsync()

	var err error
	d.locus, err = NewLocus(lociDir, d.config)
	return err
}

func (d *DistributedLoci) GetPoints() []string {
	return d.locus.GetPoints()
}

func (d *DistributedLoci) Append(pointId string, b []byte) (pos uint64, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	apply, err := d.apply(AppendRequestType, &streaming_api.ProduceRequest{Point: pointId, Frame: b})
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

func (d *DistributedLoci) Read(pointId string, pos uint64) ([]byte, error) {
	return d.locus.Read(pointId, pos)
}

func (d *DistributedLoci) ReadAt(pointId string, b []byte, off int64) (int, error) {
	return d.locus.ReadAt(pointId, b, off)
}

func (d *DistributedLoci) ClosePoint(pointId string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.locus.Close(pointId)
}

func (d *DistributedLoci) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.locus.CloseAll()
}

func (d *DistributedLoci) Remove() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.locus.RemoveAll()
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
	locus *Locus
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
	_, offset, err := l.locus.Append(req.GetPoint(), req.GetFrame())
	if err != nil {
		return err
	}
	return &streaming_api.ProduceResponse{Offset: offset}
}
