package pointcron

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

type executor struct {
	jobFunctions   chan jobFunction
	stopCh         chan struct{}
	maxRunningJobs *semaphore.Weighted
}

func newExecutor() executor {
	return executor{
		jobFunctions: make(chan jobFunction, 1),
		stopCh:       make(chan struct{}, 1),
	}
}

func (e *executor) start() {
	stopCtx, cancel := context.WithCancel(context.Background())
	runningJobsWg := sync.WaitGroup{}

	for {
		select {
		case f := <-e.jobFunctions:
			runningJobsWg.Add(1)
			go func() {
				defer runningJobsWg.Done()

				if e.maxRunningJobs != nil {
					if !e.maxRunningJobs.TryAcquire(1) {
						// If a limit on maximum concurrent jobs is set
						// and the limit is reached, a job will wait to try and run
						// until a spot in the limit is freed up.
						for {
							select {
							case <-stopCtx.Done():
								return
							case <-f.ctx.Done():
								return
							default:
							}

							if e.maxRunningJobs.TryAcquire(1) {
								break
							}
						}
					}

					defer e.maxRunningJobs.Release(1)
				}

				f.setRunState(true)
				callJobFunc(f.function)
				f.setRunState(false)
			}()
		case <-e.stopCh:
			cancel()
			runningJobsWg.Wait()
			e.stopCh <- struct{}{}
			return
		}
	}
}

func (e *executor) stop() {
	e.stopCh <- struct{}{}
	<-e.stopCh
}
