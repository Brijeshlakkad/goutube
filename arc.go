package goutube

import (
	"errors"
	"sync"
	"time"

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
	arcState

	fsm FSM
	// Dialer
	StreamLayer StreamLayer
	logger      hclog.Logger
	// Timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	Timeout      time.Duration
	store        Store
	MaxChunkSize uint64
	transport    *Transport

	// Shutdown channel to exit, protected to prevent concurrent exits
	shutdown           bool
	shutdownCh         chan struct{}
	shutdownLock       sync.Mutex
	applyCh            chan *RecordPromise
	rpcCh              <-chan RPC
	replicateStateLock sync.Mutex
	Dir                string
	bundler            Bundler
	bootStrap          bool
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

	Bootstrap bool
}

func NewArc(config ArcConfig) (*Arc, error) {
	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
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
			Logger:  config.Logger,
			Timeout: config.Timeout,
		},
	)
	arc := &Arc{
		transport:    transport,
		rpcCh:        transport.Consumer(),
		fsm:          config.fsm,
		StreamLayer:  config.StreamLayer,
		logger:       config.Logger,
		Timeout:      config.Timeout,
		store:        config.store,
		MaxChunkSize: config.MaxChunkSize,
		shutdownCh:   make(chan struct{}),
		applyCh:      make(chan *RecordPromise),
		arcState: arcState{
			replicateState: make(map[string]*Follower),
		},
		bundler:   config.Bundler,
		bootStrap: config.Bootstrap,
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
				nextOffset = resp.StoreValue.(uint64)
			}
		}
		rpc.Respond(&RecordEntriesResponse{LastOff: nextOffset}, nil)
	case *RecordRequest:
		resp := arc.fsm.Apply(req)
		nextOffset = resp.StoreValue.(uint64)
		rpc.Respond(&RecordResponse{LastOff: nextOffset}, nil)
	}
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

func (arc *Arc) join(rpcAddr string, vNodeCount int) error {
	arc.replicateStateLock.Lock()
	defer arc.replicateStateLock.Unlock()

	s, err := NewFollower(ServerAddress(rpcAddr))
	if err != nil {
		return err
	}

	arc.replicateState[rpcAddr] = s

	if arc.bootStrap {
		go arc.replicate(s)
	}

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

// Shutdown is used to stop the Raft background routines.
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

	// protects 4 next fields
	lastLock sync.Mutex

	// Cache the latest log
	lastLogIndex uint64
}

// Start a goroutine and properly handle the race between a routine
// starting and incrementing, and exiting and decrementing.
func (r *arcState) goFunc(f func()) {
	r.routinesGroup.Add(1)
	go func() {
		defer r.routinesGroup.Done()
		f()
	}()
}

func (r *arcState) waitShutdown() {
	r.routinesGroup.Wait()
}

func (r *arcState) setLastLog(index uint64) {
	r.lastLock.Lock()
	r.lastLogIndex = index
	r.lastLock.Unlock()
}

func (r *arcState) getLastLog() (index uint64) {
	r.lastLock.Lock()
	index = r.lastLogIndex
	r.lastLock.Unlock()
	return
}
