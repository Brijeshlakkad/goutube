package pointcron

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type job struct {
	mu *sync.RWMutex
	jobFunction

	getLastAccessed time.Time
	afterTime       time.Duration
	startAtTime     time.Time // optional time at which the job starts
	error           error
}

func newJob(afterTime time.Duration) *job {
	ctx, cancel := context.WithCancel(context.Background())
	runState := atomic.Value{}
	runState.Store(false)

	job := &job{
		mu:          new(sync.RWMutex),
		startAtTime: time.Now().Add(afterTime),
		afterTime:   afterTime,
		jobFunction: jobFunction{
			ctx:      ctx,
			cancel:   cancel,
			runState: runState,
		},
	}
	return job
}

func (j *job) getStartAtTime() time.Time {
	return j.startAtTime
}

func (j *job) setStartAtTime(t time.Time) {
	j.startAtTime = t
}

// you must lock the job before calling copy
func (j *job) copy() job {
	return job{
		mu:              new(sync.RWMutex),
		jobFunction:     j.jobFunction,
		startAtTime:     j.startAtTime,
		error:           j.error,
		getLastAccessed: j.getLastAccessed,
	}
}

type jobFunction struct {
	function interface{}        // task's function
	ctx      context.Context    // for cancellation
	cancel   context.CancelFunc // for cancellation
	runState atomic.Value
}

func (jf *jobFunction) isRunning() bool {
	return jf.runState.Load().(bool)
}

func (jf *jobFunction) setRunState(state bool) {
	jf.runState.Store(state)
}

func (jf *jobFunction) copy() jobFunction {
	return jobFunction{
		function: jf.function,
		ctx:      jf.ctx,
		cancel:   jf.cancel,
		runState: jf.runState,
	}
}
