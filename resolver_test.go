package goutube

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"net"
	"testing"
)

func TestResolver(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	tlsConfig, err := SetupTLSConfig(TLSConfig{
		CertFile:      ServerCertFile,
		KeyFile:       ServerKeyFile,
		CAFile:        CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(tlsConfig)

	leader, wantState, teardown := setupTestDistributedLocus(t, 5)
	defer teardown()

	srv, err := NewServer(
		&ServerConfig{
			ResolverHelperConfig: &ResolverHelperConfig{
				GetServerer: leader,
			},
		},
		grpc.Creds(serverCreds))
	require.NoError(t, err)

	go srv.Serve(l)

	conn := &clientConn{}
	tlsConfig, err = SetupTLSConfig(TLSConfig{
		CertFile:      RootClientCertFile,
		KeyFile:       RootClientKeyFile,
		CAFile:        CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	clientCreds := credentials.NewTLS(tlsConfig)
	opts := resolver.BuildOptions{
		DialCreds: clientCreds,
	}
	r := &Resolver{}
	_, err = r.Build(
		resolver.Target{
			Endpoint: l.Addr().String(),
		},
		conn,
		opts,
	)
	require.NoError(t, err)

	require.Equal(t, len(wantState.Addresses), len(conn.state.Addresses))

	conn.state.Addresses = nil
	r.ResolveNow(resolver.ResolveNowOptions{})

	require.Equal(t, len(wantState.Addresses), len(conn.state.Addresses))
}

var _ resolver.ClientConn = (*clientConn)(nil)

type clientConn struct {
	resolver.ClientConn
	state resolver.State
}

func (c *clientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}

func (c *clientConn) ReportError(err error) {}

func (c *clientConn) NewAddress(addrs []resolver.Address) {}

func (c *clientConn) NewServiceConfig(config string) {}

func (c *clientConn) ParseServiceConfig(
	config string,
) *serviceconfig.ParseResult {
	return nil
}

func setupTestDistributedLocus(t *testing.T, followerCount int) (*DistributedLoci, resolver.State, func()) {
	var teardowns []func()

	var expectedServers []resolver.Address

	distributedLoci_Leader, teardown_Leader := setupTestDistributedLoci(t,
		LeaderRule,
		fmt.Sprintf("distributed-locus-leader"),
		[]string{},
		true) // Will be part of the distributedLoci_Leader_1's ring.
	teardowns = append(teardowns, teardown_Leader)

	expectedServers = append(expectedServers, resolver.Address{
		Addr:       distributedLoci_Leader.config.Distributed.StreamLayer.Addr().String(),
		Attributes: attributes.New("is_leader", false),
	})

	for i := 0; i < followerCount; i++ {
		distributedLoci_Follower, teardown_Follower := setupTestDistributedLoci(t,
			FollowerRule,
			fmt.Sprintf("distributed-locus-follower-2-%d", i),
			[]string{},
			false)
		teardowns = append(teardowns, teardown_Follower)

		// Join replication cluster
		err := distributedLoci_Leader.Join(distributedLoci_Follower.config.Distributed.StreamLayer.Addr().String(), distributedLoci_Follower.config.Distributed.Rule)
		require.NoError(t, err)

		expectedServers = append(expectedServers, resolver.Address{
			Addr:       distributedLoci_Follower.config.Distributed.StreamLayer.Addr().String(),
			Attributes: attributes.New("is_leader", false),
		})
	}

	return distributedLoci_Leader, resolver.State{
			Addresses: expectedServers,
		}, func() {
			for _, teardown := range teardowns {
				teardown()
			}
		}
}
