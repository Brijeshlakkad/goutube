package locus

import (
	"time"

	"github.com/Brijeshlakkad/goutube/internal/locus/pointcron"
	"github.com/hashicorp/go-hclog"
)

type Config struct {
	Distributed struct {
		LocalID      string
		Bootstrap    bool
		BindAdr      string
		StreamLayer  *LocusStreamLayer
		StoreAddress string
		Logger       hclog.Logger
	}
	Point struct {
		TickTime       time.Duration
		CloseTimeout   time.Duration
		pointScheduler *pointcron.PointScheduler
	}
}
