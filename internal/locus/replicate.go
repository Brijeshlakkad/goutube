package locus

import (
	"sync"
)

type Follower struct {
	// peer contains the network address and ID of the remote follower.
	peer Server
	// peerLock protects 'peer'
	peerLock sync.RWMutex

	triggerCh     chan *CommandRequest
	triggerChLock sync.RWMutex

	// stopCh is notified/closed when this leader steps down or the follower is
	// removed from the cluster.
	stopCh chan uint64
}

func NewFollower(ServerAddress ServerAddress) (*Follower, error) {
	return &Follower{
		peer: Server{
			Address: ServerAddress,
		},
		stopCh:    make(chan uint64, 1),
		triggerCh: make(chan *CommandRequest, 1),
	}, nil
}

func (arc *Arc) replicate(s *Follower) {
	s.peerLock.RLock()
	peer := s.peer
	s.peerLock.RUnlock()

	for {
		select {
		case req := <-s.triggerCh:
			func() {
				pipeline, err := arc.transport.PrepareCommandTransport(peer.Address)
				// rpcCh := pipeline.Consumer()
				if err != nil {
					arc.logger.Error("failed to pipeline appendEntries", "peer", s.peer, "error", err)
					return
				}
				// Pipeline the append entries
				out := new(CommandResponse)
				_, err = pipeline.SendCommand(req, out)
				if err != nil {
					arc.logger.Error("failed to pipeline commands", "peer", s.peer, "error", err)
					return
				}
			}()
		case <-s.stopCh:
			return
		}
	}
}
