package locus

import (
	"errors"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
)

type Arc struct {
	transport *Transport
	// Used for our logging
	logger hclog.Logger

	State

	fsm FSM

	// Shutdown channel to exit, protected to prevent concurrent exits
	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	applyCh chan *CommandPromise

	rpcCh              <-chan RPC
	replicateStateLock sync.Mutex
}

var (
	// ErrArcShutdown is returned when operations are requested against an
	// inactive Raft.
	ErrArcShutdown = errors.New("arc is already shutdown")

	// ErrEnqueueTimeout is returned when a command fails due to a timeout.
	ErrEnqueueTimeout = errors.New("timed out enqueuing operation")
)

type ArcConfig struct {
	fsm FSM
	// Dialer
	StreamLayer StreamLayer
	Logger      hclog.Logger
	// Timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	Timeout time.Duration
}

func NewArc(config ArcConfig) *Arc {
	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "goutube-arc",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	transport := NewTransportWithConfig(
		&TransportConfig{
			Stream:  config.StreamLayer,
			Logger:  config.Logger,
			Timeout: config.Timeout,
		},
	)
	arc := &Arc{
		transport:  transport,
		rpcCh:      transport.Consumer(),
		fsm:        config.fsm,
		shutdownCh: make(chan struct{}),
		applyCh:    make(chan *CommandPromise),
		State: State{
			replicateState: make(map[string]*Follower),
		},
		logger: config.Logger,
	}

	go arc.runFSM()
	go arc.runThisPeer()

	return arc
}

func (arc *Arc) runThisPeer() {
	for {
		select {
		case rpc := <-arc.rpcCh:
			arc.processRPC(rpc)
		}
	}
}

func (arc *Arc) processRPC(rpc RPC) {
	rpc.Respond(arc.fsm.Apply(rpc.Command.(*CommandRequest)), nil)
}

func (arc *Arc) Apply(data []byte, timeout time.Duration) *CommandPromise {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	// Create a log future, no index or term yet
	commandPromise := NewCommandPromise(&CommandRequest{
		Data: data,
	})

	select {
	case <-timer:
		return commandPromise.respondError(ErrEnqueueTimeout)
	case <-arc.shutdownCh:
		return commandPromise.respondError(ErrArcShutdown)
	case arc.applyCh <- commandPromise:
		return commandPromise
	}
}

func (arc *Arc) Join(rpcAddr string, vNodeCount int) error {
	arc.replicateStateLock.Lock()
	defer arc.replicateStateLock.Unlock()

	s, err := NewFollower(ServerAddress(rpcAddr))
	if err != nil {
		return err
	}

	arc.replicateState[rpcAddr] = s

	go arc.replicate(s)

	return nil
}

func (arc *Arc) Leave(rpcAddr string) error {
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

type State struct {
	replicateState map[string]*Follower

	// Tracks running goroutines
	routinesGroup sync.WaitGroup
}

// Start a goroutine and properly handle the race between a routine
// starting and incrementing, and exiting and decrementing.
func (r *State) goFunc(f func()) {
	r.routinesGroup.Add(1)
	go func() {
		defer r.routinesGroup.Done()
		f()
	}()
}

func (r *State) waitShutdown() {
	r.routinesGroup.Wait()
}
