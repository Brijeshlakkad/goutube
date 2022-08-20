package pointcron

import (
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

type State int64

const (
	STALE State = iota
	RUNNING
	EXHAUSTED
)

type Config struct {
	CloseTimeout time.Duration
	TickTime     time.Duration
}

type PointScheduler struct {
	config  Config
	refresh chan PointJob
	state   State

	runningMutex   *sync.RWMutex
	schedulerMutex *sync.RWMutex

	executor *executor // executes jobs passed via chan

	stopChan chan struct{} // stops the scheduler
}

func NewPointScheduler(config Config) *PointScheduler {
	executor := newExecutor()
	p := &PointScheduler{
		config:         config,
		refresh:        make(chan PointJob),
		state:          STALE,
		schedulerMutex: new(sync.RWMutex),
		runningMutex:   new(sync.RWMutex),
		executor:       &executor,
	}
	return p
}

// SetMaxConcurrentJobs limits how many jobs can be running at the same time.
// This is useful when running resource intensive jobs and a precise start time is not critical.
func (p *PointScheduler) SetMaxConcurrentJobs(n int) {
	p.executor.maxRunningJobs = semaphore.NewWeighted(int64(n))
}

// StartBlocking starts all jobs and blocks the current thread.
// This blocking method can be stopped with Stop() from a separate goroutine.
func (p *PointScheduler) StartBlocking() {
	p.StartAsync()
	<-p.stopChan
}

// StartAsync starts all jobs without blocking the current thread
func (p *PointScheduler) StartAsync() {
	if !p.IsRunning() {
		go p.executor.start()
		p.setRunning()
	}
}

func (p *PointScheduler) setRunning() {
	p.runningMutex.Lock()
	defer p.runningMutex.Unlock()
	p.state = RUNNING
}

// IsRunning returns true if the scheduler is running
func (p *PointScheduler) IsRunning() bool {
	p.runningMutex.RLock()
	defer p.runningMutex.RUnlock()
	return p.state == RUNNING
}

func (p *PointScheduler) Enqueue(point PointJob) {
	if point == nil {
		return
	}
	job := newJob(p.config.CloseTimeout)
	job.function = point.Close
	job.getLastAccessed = time.Now()

	if p.IsRunning() {
		p.runContinuous(job)
	}
}

func (p *PointScheduler) runContinuous(job *Job) {
	time.AfterFunc(job.afterTime, func() {
		// Only one time call
		p.run(job)
	})
}

type nextRun struct {
	duration time.Duration
	dateTime time.Time
}

// scheduleNextRun Compute the instant when this Job should run next
func (p *PointScheduler) scheduleNextRun(job *Job) nextRun {
	now := time.Now()

	next := p.durationToNextRun(now, job)
	return next
}

// durationToNextRun calculate how much time to the next run, depending on unit
func (p *PointScheduler) durationToNextRun(lastRun time.Time, job *Job) nextRun {
	lastAccess := job.getLastAccessed

	// Cancel this scheduling if the job had recently accessed.
	if time.Now().Sub(lastAccess).Seconds() < p.config.CloseTimeout.Seconds() {
		return nextRun{}
	}

	// job can be scheduled with .StartAt()
	if job.getStartAtTime().After(lastRun) {
		return nextRun{duration: job.getStartAtTime().Sub(time.Now()), dateTime: job.getStartAtTime()}
	}
	return nextRun{}
}

func (p *PointScheduler) run(job *Job) {
	if !p.IsRunning() {
		return
	}

	job.mu.Lock()
	defer job.mu.Unlock()

	p.executor.jobFunctions <- job.jobFunction.copy()
}
