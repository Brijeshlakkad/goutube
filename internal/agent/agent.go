package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/Brijeshlakkad/goutube/internal/auth"
	"github.com/Brijeshlakkad/goutube/internal/locus"
	"github.com/Brijeshlakkad/goutube/internal/server"
	"github.com/Brijeshlakkad/ring"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Agent struct {
	Config

	mux        cmux.CMux
	loci       *locus.DistributedLoci
	server     *grpc.Server
	membership *ring.Member

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
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

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
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
		a.Config.RPCPort,
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
		return bytes.Compare(b, []byte{byte(locus.RingRPC)}) == 0
	})
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}

	lociConfig := locus.Config{}
	lociConfig.Distributed.StreamLayer = locus.NewStreamLayer(
		lociLn,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)
	lociConfig.Distributed.BindAdr = rpcAddr
	lociConfig.Distributed.LocalID = a.Config.NodeName
	lociConfig.Distributed.Bootstrap = a.Config.Bootstrap
	a.loci, err = locus.NewDistributedLoci(a.Config.DataDir, lociConfig)

	a.membership.AddListener("locimanager", a.loci)

	// TODO: Bootstrap

	return err
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)
	serverConfig := &server.Config{
		StreamingConfig: &server.StreamingConfig{
			LociManager: a.loci,
			Authorizer:  authorizer,
		},
	}
	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	var err error
	a.server, err = server.NewServer(serverConfig, opts...)
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
	a.membership, err = ring.NewMember(ring.Config{
		NodeName:         a.Config.NodeName,
		BindAddr:         a.Config.BindAddr,
		RPCPort:          a.RPCPort,
		VirtualNodeCount: a.VirtualNodeCount,
		SeedAddresses:    a.Config.SeedAddresses,
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
		a.loci.CloseAll,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
