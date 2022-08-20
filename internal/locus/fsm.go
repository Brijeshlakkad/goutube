package locus

type FSM interface {
	Apply(command *CommandRequest) interface{}
}

func (arc *Arc) runFSM() {
	applySingle := func(cp *CommandPromise) {
		var resp interface{}

		// Make sure we send a response
		defer func() {
			// Invoke the promise if given
			if cp != nil {
				cp.respond(resp)
				cp.respondError(nil)
			}
		}()

		resp = arc.fsm.Apply(cp.req)
	}

	for {
		var cp *CommandPromise
		select {
		case cp = <-arc.applyCh:
			applySingle(cp)

			// Synchronous notifying other replicas.
			arc.replicateStateLock.Lock()
			// Iterate through all the replicas to notify of the change.
			for _, server := range arc.replicateState {
				server.triggerCh <- cp.req
			}
			arc.replicateStateLock.Unlock()
		case <-arc.shutdownCh:
			return
		}
	}
}
