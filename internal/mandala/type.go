package mandala

import (
	"net"
	"time"
)

type StreamLayer interface {
	net.Listener

	Dial(string, time.Duration) (net.Conn, error)
}
