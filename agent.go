package goutube

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/Brijeshlakkad/ring"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Agent struct {
	AgentConfig

	mux                cmux.CMux
	loci               *DistributedLoci
	server             *grpc.Server
	ring               *ring.Ring
	replicationCluster *replicationCluster

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type AgentConfig struct {
	DataDir          string
	BindAddr         string
	RPCPort          int
	ReplicationPort  int
	NodeName         string
	SeedAddresses    []string
	VirtualNodeCount int

	ACLModelFile    string
	ACLPolicyFile   string
	ServerTLSConfig *tls.Config // Served to clients.
	PeerTLSConfig   *tls.Config // Servers so they can connect with and replicate each other.

	LeaderAddresses []string          // Addresses of the servers which will set this server as one of its loadbalancers (for replication).
	Rule            ParticipationRule // True, if this server takes part in the ring (peer-to-peer architecture) and/or replication.
	MemberType      ring.MemberType
}

func (c AgentConfig) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func (c AgentConfig) ReplicationRPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.ReplicationPort), nil
}

func NewAgent(config AgentConfig) (*Agent, error) {
	a := &Agent{
		AgentConfig: config,
		shutdowns:   make(chan struct{}),
	}
	setup := []func() error{
		a.setupMux,
		a.setupRing,
		a.setupLoci,
		a.setupReplicationCluster,
		a.setupServer,
	} // Order of the function call matters.
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	go a.serve()
	return a, nil
}

func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}

// Accept both Raft and gRPC connections and then creates the mux with the listener.
// Match connections based on your configured rules.
func (a *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(
		":%d",
		a.RPCPort,
	)
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	a.mux = cmux.New(ln)
	return nil
}

func (a *Agent) setupServer() error {
	authorizer := newAuth(
		a.ACLModelFile,
		a.ACLPolicyFile,
	)
	serverConfig := &ServerConfig{
		StreamingConfig: &StreamingConfig{
			Locus:      a.loci,
			Authorizer: authorizer,
		},
		ResolverHelperConfig: &ResolverHelperConfig{
			GetServerer: a.loci,
		},
	}
	var opts []grpc.ServerOption
	if a.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	var err error
	a.server, err = NewServer(serverConfig, opts...)
	if err != nil {
		return err
	}
	grpcLn := a.mux.Match(cmux.Any())
	go func() {
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()
	return err
}

func (a *Agent) setupRing() error {
	if a.Rule == StandaloneLeaderRule || a.Rule == LeaderRule || a.Rule == LeaderFollowerRule {
		var err error
		a.ring, err = ring.NewRing(ring.Config{
			NodeName:         a.NodeName,
			BindAddr:         a.BindAddr,
			RPCPort:          a.RPCPort,
			VirtualNodeCount: a.VirtualNodeCount,
			SeedAddresses:    a.SeedAddresses,
		})
		return err
	}
	return nil
}

func (a *Agent) setupLoci() error {
	lociLn := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Compare(b, []byte{byte(RingRPC)}) == 0
	})

	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}

	locusConfig := Config{}
	locusConfig.Distributed.StreamLayer = NewStreamLayer(
		lociLn,
		a.ServerTLSConfig,
		a.PeerTLSConfig,
	)
	locusConfig.Distributed.LocalID = a.NodeName
	locusConfig.Distributed.Rule = a.Rule
	// Distributed Locus will use RPC address as its binding address.
	locusConfig.Distributed.BindAddress = rpcAddr
	locusConfig.Distributed.Ring = a.ring

	a.loci, err = NewDistributedLoci(a.DataDir, locusConfig)

	return err
}

func (a *Agent) setupReplicationCluster() error {
	if a.Rule == LeaderRule || a.Rule == FollowerRule || a.Rule == LeaderFollowerRule {
		rpcAddr, err := a.RPCAddr()
		if err != nil {
			return err
		}
		replicationBindAdrr, err := a.ReplicationRPCAddr()
		if err != nil {
			return err
		}
		a.replicationCluster, err = newReplicationCluster(a.loci, ReplicationClusterConfig{
			NodeName:      fmt.Sprintf("replication-%s", a.NodeName),
			BindAddr:      replicationBindAdrr,
			SeedAddresses: a.LeaderAddresses,
			Tags: map[string]string{
				"rpc_addr": rpcAddr,
				"rule":     strconv.Itoa(int(a.Rule)),
			},
		})
		return err
	}
	return nil
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)

	var shutdown []func() error
	if a.ring != nil {
		shutdown = append(shutdown, a.ring.Shutdown)
	}
	if a.replicationCluster != nil {
		shutdown = append(shutdown, a.replicationCluster.Leave)
	}
	shutdown = append(
		shutdown,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.loci.Shutdown,
	)
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
