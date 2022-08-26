package goutube

import (
	"github.com/Brijeshlakkad/ring"
	"time"

	"github.com/Brijeshlakkad/goutube/pointcron"
	"github.com/hashicorp/go-hclog"
)

type Config struct {
	Distributed struct {
		LocalID      string
		StreamLayer  *LocusStreamLayer
		StoreAddress string
		Logger       hclog.Logger
		Rule         ParticipationRule
		BindAddress  string
		Ring         *ring.Ring
	}
	Point struct {
		TickTime       time.Duration
		CloseTimeout   time.Duration
		pointScheduler *pointcron.PointScheduler
	}
}
