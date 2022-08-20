package locus

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Brijeshlakkad/goutube/internal/locus/pointcron"
	"github.com/stretchr/testify/require"
)

func TestDistributedLoci_Create_Append_Read(t *testing.T) {
	distributedLoci_Leader := testCreatedDistributedLoci(t)
	distributedLoci_Follower := testCreatedDistributedLoci(t)

	err := distributedLoci_Leader.Join(distributedLoci_Follower.config.Distributed.StreamLayer.Addr().String(), 0)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		_, err := distributedLoci_Leader.Append(locusId, pointId, data)
		require.NoError(t, err)
	}

	// Wait for replication to get completed!
	time.Sleep(3 * time.Second)

	for i := 0; i < 10; i++ {
		var pos uint64
		for i := uint64(0); i < 10; i++ {
			data := []byte(fmt.Sprintf("Test data line %d", i))
			read, err := distributedLoci_Follower.Read(locusId, pointId, pos)
			require.NoError(t, err)
			require.Equal(t, data, read)
			pos += uint64(len(data)) + lenWidth
		}
	}
}

func testCreatedDistributedLoci(t *testing.T) *DistributedLoci {
	dataDir, err := ioutil.TempDir("", "distributed-loci-test")
	require.NoError(t, err)
	defer os.RemoveAll(dataDir)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	c := Config{}
	c.Distributed.StreamLayer = &LocusStreamLayer{
		listener,
		nil,
		nil,
	}
	c.Distributed.LocalID = "distributed-loci-0"
	pointcronConfig := pointcron.Config{}
	pointcronConfig.CloseTimeout = 3 * time.Second
	pointcronConfig.TickTime = time.Second
	c.Point.pointScheduler = pointcron.NewPointScheduler(pointcronConfig)
	c.Point.pointScheduler.StartAsync()

	distributedLoci, err := NewDistributedLoci(dataDir, c)
	require.NoError(t, err)

	return distributedLoci
}
