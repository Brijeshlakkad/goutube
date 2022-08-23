package goutube

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

// ServerID is a unique string identifying a server for all time.
type ServerID string

// ServerAddress is a network address for a server that a transport can contact.
type ServerAddress string

// Server tracks the information about a single server in a configuration.
type Server struct {
	// Address is its network address that a transport can contact.
	Address ServerAddress
}

type Store interface {
	AddPointEvent(pointId string, offset uint64) error
	GetPointEvent(pointId string) (uint64, error)
}

type Bundler interface {
	Build(header interface{}, key interface{}, value interface{}) ([]byte, error)
}
