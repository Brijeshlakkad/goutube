package locus

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	streaming_v1 "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/Brijeshlakkad/goutube/internal/locus/pointcron"
	"github.com/stretchr/testify/require"
)

var (
	pointId = "sample_test_file"
)

func TestDistributedLoci_Create_Append_Read(t *testing.T) {
	dataDir_Leader, err := ioutil.TempDir("", "distributed-locus-test")
	require.NoError(t, err)
	defer os.RemoveAll(dataDir_Leader)

	dataDir_Follower, err := ioutil.TempDir("", "distributed-locus-test")
	require.NoError(t, err)
	defer os.RemoveAll(dataDir_Follower)

	distributedLoci_Leader := setupTestDistributedLoci(t, dataDir_Leader, true, "distributed-locus-0")
	distributedLoci_Follower := setupTestDistributedLoci(t, dataDir_Follower, false, "distributed-locus-1")

	err = distributedLoci_Leader.Join(distributedLoci_Follower.config.Distributed.StreamLayer.Addr().String(), 0)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		record := &streaming_v1.ProduceRequest{
			Point: pointId,
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
		read, err := distributedLoci_Follower.Read(pointId, pos)
		require.NoError(t, err)
		require.Equal(t, data, read)
		pos += uint64(len(data)) + lenWidth
	}
}

func setupTestDistributedLoci(t *testing.T, dataDir string, bootstrap bool, localId string) *DistributedLoci {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	c := Config{}
	c.Distributed.StreamLayer = &LocusStreamLayer{
		listener,
		nil,
		nil,
	}
	c.Distributed.LocalID = localId
	c.Distributed.Bootstrap = bootstrap
	pointcronConfig := pointcron.Config{}
	pointcronConfig.CloseTimeout = 3 * time.Second
	pointcronConfig.TickTime = time.Second
	c.Point.pointScheduler = pointcron.NewPointScheduler(pointcronConfig)
	c.Point.pointScheduler.StartAsync()

	distributedLoci, err := NewDistributedLoci(dataDir, c)
	require.NoError(t, err)

	return distributedLoci
}
