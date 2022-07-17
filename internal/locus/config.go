package locus

import (
	"time"

	"github.com/Brijeshlakkad/goutube/internal/locus/pointcron"
)

type Config struct {
	Distributed struct {
		BindAdr     string
		LocalID     string
		Bootstrap   bool
		StreamLayer *StreamLayer
	}
	Point struct {
		TickTime       time.Duration
		CloseTimeout   time.Duration
		pointScheduler *pointcron.PointScheduler
	}
}
