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

type ParticipationRule uint8

const (
	StandaloneLeaderRule ParticipationRule = iota // StandaloneLeaderRule server only participates in the sharding and doesn't need replication.
	LeaderRule                                    // LeaderRule server participates in the sharding, but also has followers for replication.
	FollowerRule                                  // FollowerRule server only participates in the replication.
	LoadBalancerRule                              // LoadBalancerRule server acts as an entry point to servers with other rules.
)

// ReplicationClusterHandler interface to get notified when a new member joins or existing member leaves the cluster of replication.
type ReplicationClusterHandler interface {
	Join(rpcAddr string, rule ParticipationRule) error
	Leave(rpcAddr string) error
}
