package goutube

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/Brijeshlakkad/ring"
	"github.com/hashicorp/go-hclog"
)

var (
	// ErrArcShutdown is returned when operations are requested against an
	// inactive Raft.
	ErrArcShutdown = errors.New("arc is already shutdown")

	// ErrEnqueueTimeout is returned when a command fails due to a timeout.
	ErrEnqueueTimeout = errors.New("timed out enqueuing operation")

	// ErrStoreNullPointer is returned when the provided ArcConfig has nil Log
	ErrStoreNullPointer = errors.New("store cannot be nil")

	// ErrFSMNullPointer is returned when the provided ArcConfig has nil FSM
	ErrFSMNullPointer = errors.New("FSM cannot be nil")
)

type Arc struct {
	ArcConfig
	*arcState
	transport *Transport
	// Shutdown channel to exit, protected to prevent concurrent exits
	shutdown           bool
	shutdownCh         chan struct{}
	shutdownLock       sync.Mutex
	applyCh            chan *RecordPromise
	rpcCh              <-chan RPC
	replicateStateLock sync.Mutex
	Dir                string
	logger             hclog.Logger

	transferPointLocks map[string]*sync.Mutex
}

type ArcConfig struct {
	fsm FSM
	// Dialer
	StreamLayer StreamLayer
	Logger      hclog.Logger
	// Timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	Timeout time.Duration

	store Store

	MaxChunkSize uint64

	Bundler Bundler
}

func NewArc(config ArcConfig) (*Arc, error) {
	logger := config.Logger
	if logger == nil {
		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "goutube-arc",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	if config.store == nil {
		return nil, ErrStoreNullPointer
	}
	if config.fsm == nil {
		return nil, ErrFSMNullPointer
	}
	if config.MaxChunkSize == 0 {
		config.MaxChunkSize = 512
	}
	transport := NewTransportWithConfig(
		&TransportConfig{
			Stream:  config.StreamLayer,
			Logger:  logger,
			Timeout: config.Timeout,
		},
	)
	arc := &Arc{
		ArcConfig:  config,
		transport:  transport,
		rpcCh:      transport.Consumer(),
		shutdownCh: make(chan struct{}),
		applyCh:    make(chan *RecordPromise),
		arcState: &arcState{
			replicateState: make(map[string]*Follower),
		},
		logger:             logger,
		transferPointLocks: make(map[string]*sync.Mutex),
	}

	go arc.runFSM()
	go arc.runThisPeer()

	return arc, nil
}

func (arc *Arc) runThisPeer() {
	for {
		select {
		case rpc := <-arc.rpcCh:
			arc.processRPC(rpc)
		case <-arc.shutdownCh:
			return
		}
	}
}

func (arc *Arc) processRPC(rpc RPC) {
	var nextOffset uint64
	switch req := rpc.Command.(type) {
	case *RecordEntriesRequest:
		if len(req.Entries) > 0 {
			for _, entry := range req.Entries {
				resp := arc.fsm.Apply(entry)
				if resp.StoreValue != nil {
					nextOffset = resp.StoreValue.(uint64)
				}
			}
		}
		rpc.Respond(&RecordEntriesResponse{LastOff: nextOffset}, nil)
	case *RecordRequest:
		resp := arc.fsm.Apply(req)
		if resp.StoreValue != nil {
			nextOffset = resp.StoreValue.(uint64)
		}
		rpc.Respond(&RecordResponse{LastOff: nextOffset}, nil)
	case *GetServersRequest:
		b, err := json.Marshal(arc.GetFollowers())
		if err != nil {
			rpc.Respond(nil, err)
		}
		rpc.Respond(&GetServersResponse{Response: b}, nil)
	}
}

func (arc *Arc) getPeerFollowers(target ServerAddress) ([]Server, error) {
	var out GetServersResponse
	if err := arc.transport.SendGetServersRequest(target, &GetServersRequest{}, &out); err != nil {
		arc.logger.Error("Server couldn't handle GetFollowers request", "peer", target, "error", err)
		return []Server{}, err
	}
	if out.Response == nil {
		return []Server{}, nil
	}

	var servers []Server
	if err := json.Unmarshal(out.Response.([]byte), &servers); err != nil {
		return []Server{}, err
	}
	return servers, nil
}

func (arc *Arc) Apply(data []byte, timeout time.Duration) *RecordPromise {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	// Create a log future, no index or term yet
	recordPromise := &RecordPromise{
		req: &RecordRequest{
			Data: data,
		},
		resp: &RecordResponse{},
	}
	recordPromise.init()

	select {
	case <-timer:
		recordPromise.respondError(ErrEnqueueTimeout)
		return recordPromise
	case <-arc.shutdownCh:
		recordPromise.respondError(ErrArcShutdown)
		return recordPromise
	case arc.applyCh <- recordPromise:
		return recordPromise
	}
}

func (arc *Arc) join(rpcAddr string) error {
	arc.replicateStateLock.Lock()
	defer arc.replicateStateLock.Unlock()

	if _, ok := arc.replicateState[rpcAddr]; ok {
		return nil
	}

	s, err := NewFollower(ServerAddress(rpcAddr))
	if err != nil {
		return err
	}

	arc.replicateState[rpcAddr] = s

	go arc.replicate(s)

	return nil
}

func (arc *Arc) leave(rpcAddr string) error {
	arc.replicateStateLock.Lock()
	defer arc.replicateStateLock.Unlock()

	if server, ok := arc.replicateState[rpcAddr]; ok {
		close(server.stopCh)
		delete(arc.replicateState, rpcAddr)
	}
	return nil
}

// Shutdown is used to stop the Arc background routines.
// This is not a graceful operation. Provides a future that
// can be used to block until all background routines have exited.
func (arc *Arc) Shutdown() Promise {
	arc.shutdownLock.Lock()
	defer arc.shutdownLock.Unlock()

	if !arc.shutdown {
		close(arc.shutdownCh)
		arc.shutdown = true
		return &shutdownPromise{arc}
	}

	// avoid closing transport twice
	return &shutdownPromise{nil}
}

type arcState struct {
	replicateState map[string]*Follower

	// Tracks running goroutines
	routinesGroup sync.WaitGroup
}

// Start a goroutine and properly handle the race between a routine
// starting and incrementing, and exiting and decrementing.
func (state *arcState) goFunc(f func()) {
	state.routinesGroup.Add(1)
	go func() {
		defer state.routinesGroup.Done()
		f()
	}()
}

func (state *arcState) waitShutdown() {
	state.routinesGroup.Wait()
}

// GetFollowers gets the addresses of its loadbalancers.
func (state *arcState) GetFollowers() []Server {
	var servers []Server
	for _, server := range state.replicateState {
		servers = append(servers, server.peer)
	}
	return servers
}

func (arc *Arc) onResponsibilityChange(batch []ring.ShardResponsibility, keys []string) {
	go func() {
		// Transfer all responsibilities to the new node.
		for _, responsibility := range batch {
			arc.Transfer(&responsibility, keys)
		}
	}()
}

// Transfer transfers the responsibility of any point to the responsible new server due to resharding.
func (arc *Arc) Transfer(rt *ring.ShardResponsibility, keys []string) {
	ch := make(chan []*RecordRequest, 1)
	peerAddress := rt.ResponsibleNodeTags()[rpcAddressRingTag]
	pipeline, err := arc.transport.PrepareCommandTransport(ServerAddress(peerAddress))
	if err != nil {
		arc.logger.Error("failed to prepare command transport pipeline", "peer", peerAddress, "error", err)
		return
	}

	go func() {
		// Sends off the chuck to the responsible server (leader).
	SENDER:
		for {
			select {
			case entries, ok := <-ch:
				if !ok {
					break SENDER
				}
				req := &RecordEntriesRequest{
					Entries: entries,
				}

				out := new(RecordEntriesResponse)
				_, err = pipeline.SendRecordEntriesRequest(req, out)
				if err != nil {
					arc.logger.Error("failed to pipeline commands", "peer", peerAddress, "error", err)
					return
				}

				// Wait for response
				respCh := pipeline.Consumer()
				select {
				case promise := <-respCh:
					err = promise.Error()
					if err != nil {
						arc.logger.Error("server couldn't handle the command", "peer", peerAddress, "error", err)
						return
					}

				}
			default:
			}
		}
	}()
	for _, key := range keys {
		if rt.Transfer(key) {
			arc.reader(key, ch)
		}
	}
}

// reader reads the chuck into provided channel.
func (arc *Arc) reader(key string, ch chan []*RecordRequest) {
	unlockPoint := arc.lockPoint(key)
	defer unlockPoint()

	offset := uint64(0)
	// Reads this point from the local to be sent over network.
READER:
	for {
		var entries []*RecordRequest
		var chunkSize uint64
		fetchNext := false

	CHUNK_READER:
		for chunkSize < arc.MaxChunkSize {
			var chunk []byte
			nextOffset, chunk, err := arc.fsm.Read(key, offset)
			if err != nil {
				fetchNext = true
				break CHUNK_READER
			}
			if offset == nextOffset {
				fetchNext = true
				break CHUNK_READER
			}
			if len(chunk) > 0 {
				offset = nextOffset
				data, err := arc.Bundler.Build(AppendRequestType, key, chunk)
				if err != nil {
					arc.logger.Error("failed to build request", "error", err)
					return
				}
				entries = append(entries, &RecordRequest{
					Data: data,
				})

				chunkSize += uint64(len(data))
			} else {
				fetchNext = true
				break CHUNK_READER
			}
		}
		ch <- entries
		if fetchNext {
			break READER
		}
	}
}

// lockPoint locks the provided key in transferPointLocks.
func (arc *Arc) lockPoint(key string) func() {
	_, ok := arc.transferPointLocks[key]
	if !ok {
		arc.transferPointLocks[key] = new(sync.Mutex)
	}
	arc.transferPointLocks[key].Lock()

	return arc.transferPointLocks[key].Unlock
}
