package goutube

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/Brijeshlakkad/goutube/pointcron"
	"github.com/Brijeshlakkad/ring"
	"github.com/hashicorp/go-hclog"

	"google.golang.org/protobuf/proto"
)

var (
	ErrMaxChunkSizeInvalid = errors.New("configuration error: max chunk size cannot be zero")
)

type DistributedLoci struct {
	config Config
	locus  *Locus
	store  Store
	logger hclog.Logger

	ring *ring.Ring
	arc  *Arc

	mu sync.RWMutex

	bundler *RequestBundler
}

func NewDistributedLoci(dataDir string, config Config) (
	*DistributedLoci,
	error,
) {
	if config.Distributed.MaxChunkSize <= 0 {
		return nil, ErrMaxChunkSizeInvalid
	}
	if config.Distributed.Logger == nil {
		config.Distributed.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "distributed-loci",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	d := &DistributedLoci{
		config:  config,
		bundler: &RequestBundler{},
		ring:    config.Distributed.Ring,
	}
	var err error

	if err = d.setupLocus(dataDir); err != nil {
		return nil, err
	}
	if err = d.setupStore(dataDir); err != nil {
		return nil, err
	}

	arcConfig := ArcConfig{
		StreamLayer: config.Distributed.StreamLayer,
		fsm: &fsm{
			locus: d.locus,
		},
		store:   d.store,
		Bundler: d.bundler,
	}
	d.arc, err = NewArc(arcConfig)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (d *DistributedLoci) setupLocus(dataDir string) error {
	lociDir, err := createDirectory(dataDir, "locus")
	if err != nil {
		return err
	}
	d.config.Point.pointScheduler = pointcron.NewPointScheduler(pointcron.Config{
		CloseTimeout: d.config.Point.CloseTimeout,
		TickTime:     d.config.Point.TickTime,
	})
	d.config.Point.pointScheduler.StartAsync()

	d.locus, err = NewLocus(lociDir, d.config)
	return err
}

func (d *DistributedLoci) setupStore(dataDir string) (err error) {
	logDir, err := createDirectory(dataDir, "log")
	if err != nil {
		return err
	}
	d.store, err = NewInMomoryPointStore(logDir)
	return err
}

func (d *DistributedLoci) GetPoints() []string {
	return d.locus.GetPoints()
}

func (d *DistributedLoci) Append(record *streaming_api.ProduceRequest) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	apply, err := d.apply(AppendRequestType, record.Point, record.Frame)
	if err != nil {
		return 0, err
	}
	records := apply.(*streaming_api.ProduceResponse).Records
	if len(records) > 0 {
		return records[0].Offset, nil
	}
	return 0, nil
}

func (d *DistributedLoci) apply(reqType RequestType, key interface{}, value interface{}) (
	interface{},
	error,
) {
	b, err := d.bundler.Build(reqType, key, value)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	commandPromise := d.arc.Apply(b, timeout)
	if err := commandPromise.Error(); err != nil {
		return nil, err
	}
	res := commandPromise.Response().(*RecordResponse)
	return res.Response, nil
}

func (d *DistributedLoci) ReadWithLimit(pointId string, pos uint64, chunkSize uint64, limit uint64) (uint64, []byte, error) {
	return d.locus.Read(pointId, pos, chunkSize, limit)
}

func (d *DistributedLoci) Read(pointId string, pos uint64) (uint64, []byte, error) {
	return d.locus.Read(pointId, pos, 0, 0)
}

func (d *DistributedLoci) ReadAt(pointId string, b []byte, off uint64) (int, error) {
	return d.locus.ReadAt(pointId, b, off)
}

func (d *DistributedLoci) GetMetadata(pointId string) (PointMetadata, error) {
	return d.locus.GetMetadata(pointId)
}

func (d *DistributedLoci) ClosePoint(pointId string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.locus.Close(pointId)
}

// OnChange allows to get notified when new server joins the ring at the position next on the ring to this server.
// Hence, this server needs to send off points that should not be handled by this server anymore.
func (d *DistributedLoci) OnChange(batch []ring.ShardResponsibility) {
	d.arc.onResponsibilityChange(batch, d.GetPoints())
}

func (d *DistributedLoci) Shutdown() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	promise := d.arc.Shutdown()
	if err := promise.Error(); err != nil {
		return err
	}
	res := promise.Response()
	if err, ok := res.(error); ok {
		return err
	}
	return d.locus.CloseAll()
}

func (d *DistributedLoci) Remove() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.locus.RemoveAll()
}

func (d *DistributedLoci) Join(rpcAddr string, rule ParticipationRule) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.canArcJoin(rule) {
		return d.arc.join(rpcAddr)
	}
	return nil
}

func (d *DistributedLoci) Leave(rpcAddr string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.arc.leave(rpcAddr)
}

func (d *DistributedLoci) canArcJoin(rule ParticipationRule) bool {
	return d.config.Distributed.Rule == LeaderRule && rule == FollowerRule
}

func (d *DistributedLoci) GetServers(req *streaming_api.GetServersRequest) ([]*streaming_api.Server, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var servers []*streaming_api.Server

	serverList := d.ring.GetLoadBalancers()

	for _, serverAddr := range serverList {
		servers = append(servers, &streaming_api.Server{
			RpcAddr: serverAddr,
		})
	}
	return servers, nil
}

func (d *DistributedLoci) GetFollowers(req *streaming_api.GetFollowersRequest) ([]*streaming_api.Server, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var servers []*streaming_api.Server

	serverList := d.arc.GetFollowers()

	// Include the leader as well.
	for _, server := range serverList {
		servers = append(servers, &streaming_api.Server{
			RpcAddr: string(server.Address),
		})
	}
	return servers, nil
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
func (f *fsm) Apply(record *RecordRequest) *FSMRecordResponse {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}
	return nil
}

func (f *fsm) applyAppend(b []byte) *FSMRecordResponse {
	var req streaming_api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return &FSMRecordResponse{Response: err}
	}
	nextOffset, offset, err := f.locus.Append(req.GetPoint(), req.GetFrame())
	if err != nil {
		return &FSMRecordResponse{Response: err}
	}
	return &FSMRecordResponse{
		StoreKey:   req.Point,
		StoreValue: nextOffset,
		Response: &streaming_api.ProduceResponse{Records: []*streaming_api.Record{
			{
				Point:  req.Point,
				Offset: offset,
			},
		}},
	}
}

func (f *fsm) Read(key string, offset uint64) (uint64, []byte, error) {
	return f.locus.Read(key, offset, 0, 0)
}

type RequestBundler struct {
}

func (rb *RequestBundler) Build(header interface{}, key interface{}, value interface{}) (
	[]byte,
	error,
) {
	req := &streaming_api.ProduceRequest{Point: key.(string), Frame: value.([]byte)}
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(header.(RequestType))})
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
	return buf.Bytes(), nil
}
