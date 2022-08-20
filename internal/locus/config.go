package locus

import (
	"time"

	"github.com/Brijeshlakkad/goutube/internal/locus/pointcron"
)

type Config struct {
	Distributed struct {
		LocalID     string
		Bootstrap   bool
		BindAdr     string
		StreamLayer *LocusStreamLayer
	}
	Point struct {
		TickTime       time.Duration
		CloseTimeout   time.Duration
		pointScheduler *pointcron.PointScheduler
	}
}
