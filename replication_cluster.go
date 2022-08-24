package goutube

import (
	"net"
	"strconv"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// ring Type wrapping Serf to provide discovery and cluster ring to our service.
type replicationCluster struct {
	ReplicationClusterConfig
	handler ReplicationClusterHandler
	cluster *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

func newReplicationCluster(handler ReplicationClusterHandler, config ReplicationClusterConfig) (*replicationCluster, error) {
	c := &replicationCluster{
		ReplicationClusterConfig: config,
		handler:                  handler,
		logger:                   zap.L().Named("replication-cluster"),
	}
	if err := c.setupCluster(); err != nil {
		return nil, err
	}
	return c, nil
}

func (m *replicationCluster) setupCluster() error {
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
	if m.SeedAddresses != nil && len(m.SeedAddresses) > 0 {
		_, err = m.cluster.Join(m.SeedAddresses, true)
		if err != nil {
			return err
		}
	}

	return nil
}

// Runs in a loop reading events sent by Serf into the events channel, handling each incoming event according to the event’s type.
func (m *replicationCluster) eventHandler() {
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

func (m *replicationCluster) handleJoin(member serf.Member) {
	ruleInt, err := strconv.ParseUint(member.Tags["rule"], 10, 64)
	if err != nil {
		m.logError(err, "failed to join", member)
	}
	rule := ParticipationRule(uint8(ruleInt))
	if err := m.handler.Join(
		member.Tags["rpc_addr"],
		rule,
	); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *replicationCluster) handleLeave(member serf.Member) {
	if err := m.handler.Leave(
		member.Tags["rpc_addr"],
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// Returns whether the given Serf member is the local member by checking the members’ names.
func (m *replicationCluster) isLocal(member serf.Member) bool {
	return m.cluster.LocalMember().Name == member.Name
}

// Members Returns a point-in-time snapshot of the cluster’s Serf members.
func (m *replicationCluster) Members() []serf.Member {
	return m.cluster.Members()
}

// Leave Tells this member to leave the Serf cluster.
func (m *replicationCluster) Leave() error {
	return m.cluster.Leave()
}

// Logs the given error and message.
func (m *replicationCluster) logError(err error, msg string, member serf.Member) {
	log := m.logger.Error
	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}

type ReplicationClusterConfig struct {
	NodeName      string
	BindAddr      string
	Tags          map[string]string
	SeedAddresses []string
}
