package discovery

import (
	"net"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// Type wrapping Serf to provide discovery and cluster membership to our service.
type Membership struct {
	Config
	handler Handler
	cluster *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := c.setupCluster(); err != nil {
		return nil, err
	}
	return c, nil
}

func (m *Membership) setupCluster() error {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	config.EventCh = m.events
	config.Tags = m.Tags
	config.NodeName = m.NodeName
	m.cluster, err = serf.Create(config)
	if err != nil {
		return err
	}

	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		_, err = m.cluster.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

// Runs in a loop reading events sent by Serf into the events channel, handling each incoming event according to the event’s type.
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(
		member.Name,
		member.Tags["rpc_addr"],
	); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(
		member.Name,
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// Returns whether the given Serf member is the local member by checking the members’ names.
func (m *Membership) isLocal(member serf.Member) bool {
	return m.cluster.LocalMember().Name == member.Name
}

// Returns a point-in-time snapshot of the cluster’s Serf members.
func (m *Membership) Members() []serf.Member {
	return m.cluster.Members()
}

// Tells this member to leave the Serf cluster.
func (m *Membership) Leave() error {
	return m.cluster.Leave()
}

// Logs the given error and message.
func (m *Membership) logError(err error, msg string, member serf.Member) {
	log := m.logger.Error
	// logError will log the non-leader errors at the debug level.
	if err == raft.ErrNotLeader {
		log = m.logger.Debug
	}
	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}

type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}
