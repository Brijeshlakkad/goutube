package goutube

import (
	"sync"
)

type Follower struct {
	// peer contains the network address and ID of the remote follower.
	peer Server
	// peerLock protects 'peer'
	peerLock sync.RWMutex

	triggerCh chan interface{}

	// stopCh is notified/closed when this leader steps down or the follower is
	// removed from the cluster.
	stopCh chan uint64

	// store keeps the track of the state of other servers are for the respective key.
	store map[string]uint64

	triggerChLock sync.Mutex
}

func NewFollower(ServerAddress ServerAddress) (*Follower, error) {
	return &Follower{
		peer: Server{
			Address: ServerAddress,
		},
		stopCh:    make(chan uint64, 1),
		triggerCh: make(chan interface{}, 1),
		store:     make(map[string]uint64),
	}, nil
}

func (arc *Arc) replicate(s *Follower) {
	for {
		select {
		case storeKeyI := <-s.triggerCh:
			storeKey := storeKeyI.(string)
			nextOffset := s.store[storeKey]
			arc.appendEntries(s, storeKey, nextOffset)
		case <-s.stopCh:
			return
		}
	}
}

func (arc *Arc) appendEntries(s *Follower, key string, nextOffset uint64) {
	s.peerLock.RLock()
	peer := s.peer
	s.peerLock.RUnlock()

	s.triggerChLock.Lock()
	defer s.triggerChLock.Unlock()
	var err error
	pipeline, err := arc.transport.PrepareCommandTransport(peer.Address)
	if err != nil {
		arc.logger.Error("failed to pipeline appendEntries", "peer", s.peer, "error", err)
		return
	}
	// Pipeline the append entries
	lastOffset, err := arc.store.GetPointEvent(key)

	if err != nil {
		arc.logger.Error("Error while synchronizing the loadbalancers", "error", err)
	}
	var entries []*RecordRequest
	for nextOffset <= lastOffset {
		var chunk []byte
		var tempNextOffset uint64
		tempNextOffset, chunk, err = arc.fsm.Read(key, nextOffset)
		if err != nil {
			break
		}
		if len(chunk) > 0 {
			nextOffset = tempNextOffset
			data, err := arc.Bundler.Build(AppendRequestType, key, chunk)
			if err != nil {
				arc.logger.Error("failed to build request", "peer", s.peer, "error", err)
				return
			}
			entries = append(entries, &RecordRequest{
				Data: data,
			})
		}
	}

	if len(entries) > 0 {
		req := &RecordEntriesRequest{
			Entries: entries,
		}

		out := new(RecordEntriesResponse)
		_, err = pipeline.SendRecordEntriesRequest(req, out)
		if err != nil {
			arc.logger.Error("failed to pipeline commands", "peer", s.peer, "error", err)
			return
		}

		// Wait for response
		respCh := pipeline.Consumer()
		select {
		case promise := <-respCh:
			err = promise.Error()
			if err != nil {
				arc.logger.Error("server couldn't handle the command", "peer", s.peer, "error", err)
				return
			}

			s.store[key] = promise.Response().(*RecordEntriesResponse).LastOff
		}
	}
}
