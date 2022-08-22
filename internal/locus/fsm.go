package locus

type FSM interface {
	Apply(command *RecordRequest) *FSMRecordResponse
	Read(key string, offset uint64) (uint64, []byte, error)
}

func (arc *Arc) runFSM() {
	applySingle := func(cp *RecordPromise) interface{} {
		var resp *FSMRecordResponse

		// Make sure we send a response
		defer func() {
			// Invoke the promise if given
			if cp != nil {
				cp.respond(resp.Response)
				cp.respondError(nil)
			}
		}()

		resp = arc.fsm.Apply(cp.req)

		if err := arc.store.AddPointEvent(resp.StoreKey.(string), resp.StoreValue.(uint64)); err != nil {
			arc.logger.Error("failed to add event to the store", "error", err)
		}

		return resp.StoreKey
	}

	for {
		var cp *RecordPromise
		select {
		case cp = <-arc.applyCh:
			key := applySingle(cp)

			// Async notifying replicas.
			arc.notifyFollowers(key)
		case <-arc.shutdownCh:
			return
		}
	}
}

func (arc *Arc) notifyFollowers(key interface{}) {
	// Iterate through all the replicas to notify of the change.
	for _, follower := range arc.replicateState {
		asyncNotifyCh(follower.triggerCh, key)
	}
}

func asyncNotifyCh(ch chan interface{}, key interface{}) {
	select {
	case ch <- key:
	default:
	}
}
