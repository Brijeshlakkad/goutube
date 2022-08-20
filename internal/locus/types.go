package locus

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
