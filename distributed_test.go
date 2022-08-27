package goutube

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	streaming_v1 "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/Brijeshlakkad/goutube/pointcron"
	"github.com/Brijeshlakkad/ring"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestDistributedLoci_Create_Append_Read(t *testing.T) {
	distributedLoci_Leader, teardown_Leader := setupTestDistributedLoci(t, LeaderRule, "distributed-locus-0", &ring.Config{MemberType: ring.ShardMember})
	defer teardown_Leader()

	distributedLoci_Follower, teardown_Follower := setupTestDistributedLoci(t, FollowerRule, "distributed-locus-1", nil)
	defer teardown_Follower()

	err := distributedLoci_Leader.Join(distributedLoci_Follower.config.Distributed.StreamLayer.Addr().String(), distributedLoci_Follower.config.Distributed.Rule)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		record := &streaming_v1.ProduceRequest{
			Point: testPointId,
			Frame: data,
		}
		_, err := distributedLoci_Leader.Append(record)
		require.NoError(t, err)
	}

	// Wait for replication to get completed!
	time.Sleep(5 * time.Second)

	var pos uint64
	for i := uint64(0); i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		read, err := distributedLoci_Follower.Read(testPointId, pos)
		require.NoError(t, err)
		require.Equal(t, data, read)
		pos += uint64(len(data)) + lenWidth
	}
}

func TestDistributedLoci_ParticipationRule(t *testing.T) {
	rules := []ParticipationRule{LeaderRule, LeaderFollowerRule, FollowerRule}
	var teardowns []func()

	defer func() {
		for _, teardown := range teardowns {
			teardown()
		}
	}()

	for i := 0; i < len(rules); i++ {
		distributedLoci_Leader, teardown_Leader := setupTestDistributedLoci(t, LeaderRule, "distributed-locus-0", &ring.Config{MemberType: ring.ShardMember})
		teardowns = append(teardowns, teardown_Leader)

		distributedLoci_Follower, teardown_Follower := setupTestDistributedLoci(t, rules[i], "distributed-locus-1", nil)
		teardowns = append(teardowns, teardown_Follower)

		err := distributedLoci_Leader.Join(distributedLoci_Follower.config.Distributed.StreamLayer.Addr().String(), distributedLoci_Follower.config.Distributed.Rule)
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			data := []byte(fmt.Sprintf("Test data line %d", i))
			record := &streaming_v1.ProduceRequest{
				Point: testPointId,
				Frame: data,
			}
			_, err := distributedLoci_Leader.Append(record)
			require.NoError(t, err)
		}

		// Wait for replication to get completed!
		time.Sleep(5 * time.Second)

		var pos uint64
		_, err = distributedLoci_Follower.Read(testPointId, pos)
		if rules[i] == StandaloneLeaderRule {
			require.Error(t, err)
		} else if rules[i] == LeaderRule {
			require.Error(t, err)
		} else if rules[i] == LeaderFollowerRule {
			require.NoError(t, err)
		} else if rules[i] == FollowerRule {
			require.NoError(t, err)
		}
	}
}

func TestDistributedLoci_GetServers(t *testing.T) {
	distributedLoci_Leader, teardown_Leader := setupTestDistributedLoci(t, LeaderRule, "distributed-locus-0", &ring.Config{MemberType: ring.ShardMember})
	defer teardown_Leader()

	var teardowns []func()
	defer func() {
		for _, teardown := range teardowns {
			teardown()
		}
	}()
	followerCount := 5
	for i := 0; i < followerCount; i++ {
		distributedLoci_Follower, teardown_Follower := setupTestDistributedLoci(t,
			FollowerRule,
			fmt.Sprintf("distributed-locus-follower-%d", i),
			nil)
		teardowns = append(teardowns, teardown_Follower)
		err := distributedLoci_Leader.Join(distributedLoci_Follower.config.Distributed.StreamLayer.Addr().String(), distributedLoci_Follower.config.Distributed.Rule)
		require.NoError(t, err)
	}

	servers, err := distributedLoci_Leader.GetServers("example-key")
	require.NoError(t, err)

	require.Equal(t, followerCount+1, len(servers))
}

func TestDistributedLoci_GetPeerServers(t *testing.T) {
	var teardowns []func()
	defer func() {
		for _, teardown := range teardowns {
			teardown()
		}
	}()

	distributedLoci_Leader_1, teardown_Leader_1 := setupTestDistributedLoci(t,
		LeaderRule,
		"distributed-locus-leader-1",
		&ring.Config{MemberType: ring.ShardMember})
	defer teardown_Leader_1()

	followerCount_1 := 3
	for i := 0; i < followerCount_1; i++ {
		distributedLoci_Follower, teardown_Follower := setupTestDistributedLoci(t,
			FollowerRule,
			fmt.Sprintf("distributed-locus-follower-1-%d", i),
			nil)
		teardowns = append(teardowns, teardown_Follower)

		err := distributedLoci_Leader_1.Join(distributedLoci_Follower.config.Distributed.StreamLayer.Addr().String(), distributedLoci_Follower.config.Distributed.Rule)
		require.NoError(t, err)
	}

	distributedLoci_Leader_2, teardown_Leader_2 := setupTestDistributedLoci(t,
		LeaderRule,
		"distributed-locus-leader-2",
		&ring.Config{MemberType: ring.ShardMember, SeedAddresses: []string{distributedLoci_Leader_1.ring.BindAddr}}) // Will be part of the distributedLoci_Leader_1's ring.
	defer teardown_Leader_2()

	followerCount_2 := 4
	for i := 0; i < followerCount_2; i++ {
		distributedLoci_Follower, teardown_Follower := setupTestDistributedLoci(t,
			FollowerRule,
			fmt.Sprintf("distributed-locus-follower-2-%d", i),
			nil)
		teardowns = append(teardowns, teardown_Follower)

		err := distributedLoci_Leader_2.Join(distributedLoci_Follower.config.Distributed.StreamLayer.Addr().String(), distributedLoci_Follower.config.Distributed.Rule)
		require.NoError(t, err)
	}

	objectKey := "example-key"

	responsibleServer, found := distributedLoci_Leader_1.ring.GetNode(objectKey)
	require.Equal(t, true, found)

	leader_1_RPCAddr, err := distributedLoci_Leader_1.ring.RPCAddr()
	require.NoError(t, err)

	leader_2_RPCAddr, err := distributedLoci_Leader_2.ring.RPCAddr()
	require.NoError(t, err)

	if responsibleServer == leader_1_RPCAddr {
		// If the leader 1 is responsible for objectKey, then request leader 2 to get its followers.
		servers, err := distributedLoci_Leader_2.GetServers(objectKey)
		require.NoError(t, err)
		require.Equal(t, followerCount_1+1, len(servers))
	} else if responsibleServer == leader_2_RPCAddr {
		// If the leader 2 is responsible for objectKey, then request leader 1 to get its followers.
		servers, err := distributedLoci_Leader_1.GetServers(objectKey)
		require.NoError(t, err)
		require.Equal(t, followerCount_2+1, len(servers))
	} else {
		t.Fatalf("the responsible server for the object key: %s is not correct!", objectKey)
	}
}

func setupTestDistributedLoci(t *testing.T,
	rule ParticipationRule,
	localId string,
	ringConfig *ring.Config,
) (*DistributedLoci, func()) {
	t.Helper()

	dataDir, err := ioutil.TempDir("", "distributed-locus-test")
	require.NoError(t, err)

	ports := dynaport.Get(2)
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", ports[0]))
	require.NoError(t, err)

	c := Config{}
	c.Distributed.StreamLayer = &LocusStreamLayer{
		listener,
		nil,
		nil,
	}
	c.Distributed.BindAddress = listener.Addr().String()
	c.Distributed.LocalID = localId
	c.Distributed.Rule = rule
	pointcronConfig := pointcron.Config{}
	pointcronConfig.CloseTimeout = 3 * time.Second
	pointcronConfig.TickTime = time.Second
	c.Point.pointScheduler = pointcron.NewPointScheduler(pointcronConfig)
	c.Point.pointScheduler.StartAsync()

	if ringConfig != nil {
		nodeName := ringConfig.NodeName
		if nodeName == "" {
			nodeName = fmt.Sprintf("replication-%s", localId)
		}
		bindAddr := ringConfig.BindAddr
		if bindAddr == "" {
			bindAddr = fmt.Sprintf("localhost:%d", ports[1])
		}
		rpcPort := ringConfig.RPCPort
		if rpcPort == 0 {
			rpcPort = ports[0]
		}
		virtualNodeCount := ringConfig.VirtualNodeCount
		if virtualNodeCount == 0 {
			virtualNodeCount = 3
		}
		seedAddresses := ringConfig.SeedAddresses
		if seedAddresses == nil {
			seedAddresses = []string{}
		}
		ringInstance, err := ring.NewRing(ring.Config{
			NodeName:         nodeName,
			BindAddr:         bindAddr,
			RPCPort:          rpcPort,
			VirtualNodeCount: virtualNodeCount,
			SeedAddresses:    seedAddresses,
		})
		require.NoError(t, err)
		c.Distributed.Ring = ringInstance
	}

	distributedLoci, err := NewDistributedLoci(dataDir, c)
	require.NoError(t, err)

	return distributedLoci, func() {
		defer os.RemoveAll(dataDir)

		err := distributedLoci.Shutdown()
		require.NoError(t, err)
	}
}
