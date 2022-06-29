package agent

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/Brijeshlakkad/goutube/internal/server"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
)

type Agent struct {
	Config

	mux    cmux.CMux
	server *grpc.Server

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
	RPCPort int
}

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	a.setupServer()
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

func (a *Agent) setupServer() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8079"
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}

	gRPCServer, err := server.NewStreamingServer()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Streaming service is listening on port %s...\n", port)
	err = gRPCServer.Serve(listener)
	fmt.Println("Serve() failed", err)
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)

	return nil
}
