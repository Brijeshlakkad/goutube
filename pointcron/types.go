package pointcron

import (
	"time"
)

type PointJob interface {
	Close() error
	GetLastAccessed() time.Time
}
