package pointcron_test

import (
	"testing"
	"time"

	"github.com/Brijeshlakkad/goutube/internal/locus/pointcron"
)

const closeTimeout = 3 * time.Second

func TestJobScheduledExecution(t *testing.T) {
	config := pointcron.Config{}
	config.CloseTimeout = closeTimeout
	config.TickTime = time.Second
	s := pointcron.NewPointScheduler(config)
	point := &PointJobImpl{
		semaphore: make(chan bool),
	}
	s.StartAsync()
	s.SetMaxConcurrentJobs(10)
	s.Enqueue(point)
	select {
	case <-time.After(4 * time.Second):
		t.Fatal("job did not run at the scheduled time")
	case <-point.semaphore:
		// test passed
	}

}

type PointJobImpl struct {
	semaphore chan bool
}

func (p *PointJobImpl) Close() error {
	p.semaphore <- true
	return nil
}

func (p *PointJobImpl) GetLastAccessed() time.Time {
	return time.Unix(time.Now().Unix()-int64(closeTimeout.Seconds()), 0)
}
