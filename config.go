package goutube

import (
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
	}
	Point struct {
		TickTime       time.Duration
		CloseTimeout   time.Duration
		pointScheduler *pointcron.PointScheduler
	}
}
