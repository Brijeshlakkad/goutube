package goutube

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/Brijeshlakkad/ring"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Agent struct {
	AgentConfig

	mux        cmux.CMux
	loci       *DistributedLoci
	server     *grpc.Server
	membership *ring.Ring

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type AgentConfig struct {
	DataDir          string
	BindAddr         string
	RPCPort          int
	NodeName         string
	SeedAddresses    []string
	Bootstrap        bool
	VirtualNodeCount int

	ACLModelFile    string
	ACLPolicyFile   string
	ServerTLSConfig *tls.Config // Served to clients.
	PeerTLSConfig   *tls.Config // Servers so they can connect with and replicate each other.
}

func (c AgentConfig) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func NewAgent(config AgentConfig) (*Agent, error) {
	a := &Agent{
		AgentConfig: config,
		shutdowns:   make(chan struct{}),
	}
	setup := []func() error{
		a.setupMux,
		a.setupMembership,
		a.setupLoci,
		a.setupServer,
	}
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

	lociConfig := Config{}
	lociConfig.Distributed.StreamLayer = NewStreamLayer(
		lociLn,
		a.ServerTLSConfig,
		a.PeerTLSConfig,
	)
	lociConfig.Distributed.BindAdr = rpcAddr
	lociConfig.Distributed.LocalID = a.NodeName
	lociConfig.Distributed.Bootstrap = a.Bootstrap
	a.loci, err = NewDistributedLoci(a.DataDir, lociConfig)

	a.membership.AddListener("locimanager", a.loci)

	return err
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

func (a *Agent) setupMembership() error {
	var err error
	a.membership, err = ring.NewRing(ring.Config{
		NodeName:         a.NodeName,
		BindAddr:         a.BindAddr,
		RPCPort:          a.RPCPort,
		VirtualNodeCount: a.VirtualNodeCount,
		SeedAddresses:    a.SeedAddresses,
	})
	return err
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)

	shutdown := []func() error{
		a.membership.Shutdown,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.loci.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
