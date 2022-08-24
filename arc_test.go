package goutube

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/Brijeshlakkad/goutube/pointcron"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestArc_FSM(t *testing.T) {
	streamLayer, err := newTCPStreamLayer("localhost:0", nil)
	require.NoError(t, err)

	dataDir, err := ioutil.TempDir("", "arc-test")
	require.NoError(t, err)
	defer os.RemoveAll(dataDir)

	locusDir, err := createDirectory(dataDir, "locus")
	require.NoError(t, err)

	logDir, err := createDirectory(dataDir, "log")
	require.NoError(t, err)

	logStore, err := NewInMomoryPointStore(logDir)
	require.NoError(t, err)

	locus := setupTestLocus(t, locusDir)

	fsm := &fsm{
		locus,
	}

	arc, err := NewArc(ArcConfig{
		StreamLayer: streamLayer,
		fsm:         fsm,
		store:       logStore,
		Bundler:     &RequestBundler{},
	})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		resp, err := apply(arc, AppendRequestType, &streaming_api.ProduceRequest{Point: pointId, Frame: data})
		require.NoError(t, err)

		records := resp.(*streaming_api.ProduceResponse).Records
		require.Equal(t, 1, len(records))
	}

	var pos uint64
	for i := uint64(0); i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		nextOffset, read, err := locus.Read(pointId, pos)
		require.NoError(t, err)
		require.Equal(t, data, read)
		pos += uint64(len(data)) + lenWidth
		require.Equal(t, pos, nextOffset)
	}
}

func TestArc_Followers(t *testing.T) {
	// Leader Arc
	streamLayer, err := newTCPStreamLayer("localhost:0", nil)
	require.NoError(t, err)

	dataDir_Leader, err := ioutil.TempDir("", "arc-test")
	require.NoError(t, err)
	defer os.RemoveAll(dataDir_Leader)

	locusDir_Leader, err := createDirectory(dataDir_Leader, "locus")
	require.NoError(t, err)

	logDir_Leader, err := createDirectory(dataDir_Leader, "log")
	require.NoError(t, err)

	logStore_Leader, err := NewInMomoryPointStore(logDir_Leader)
	require.NoError(t, err)

	locus_Leader := setupTestLocus(t, locusDir_Leader)
	fsm_leader := &fsm{
		locus_Leader,
	}

	arc_leader, err := NewArc(ArcConfig{
		StreamLayer: streamLayer,
		fsm:         fsm_leader,
		store:       logStore_Leader,
		Bundler:     &RequestBundler{},
	})
	require.NoError(t, err)

	// Follower Arc
	streamLayer_Follower, err := newTCPStreamLayer("localhost:0", nil)
	require.NoError(t, err)

	dataDir_Follower, err := ioutil.TempDir("", "arc-test")
	require.NoError(t, err)
	defer os.RemoveAll(dataDir_Follower)

	locusDir_Follower, err := createDirectory(dataDir_Follower, "locus")
	require.NoError(t, err)

	logDir_Follower, err := createDirectory(dataDir_Follower, "log")
	require.NoError(t, err)

	logStore_Follower, err := NewInMomoryPointStore(logDir_Follower)
	require.NoError(t, err)

	locus_Follower := setupTestLocus(t, locusDir_Follower)
	fsm_Follower := &fsm{
		locus_Follower,
	}

	_, err = NewArc(ArcConfig{
		StreamLayer: streamLayer_Follower,
		fsm:         fsm_Follower,
		store:       logStore_Follower,
		Bundler:     &RequestBundler{},
	})
	require.NoError(t, err)

	followerState, err := NewFollower(ServerAddress(streamLayer_Follower.Addr().String()))
	arc_leader.replicateState[streamLayer_Follower.Addr().String()] = followerState
	require.NoError(t, err)

	go arc_leader.replicate(followerState)

	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		resp, err := apply(arc_leader, AppendRequestType, &streaming_api.ProduceRequest{Point: pointId, Frame: data})
		require.NoError(t, err)

		records := resp.(*streaming_api.ProduceResponse).Records
		require.Equal(t, 1, len(records))
	}

	// Wait for replication to get completed!
	time.Sleep(5 * time.Second)

	var pos uint64
	for i := uint64(0); i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		nextOffset, read, err := locus_Follower.Read(pointId, pos)
		require.NoError(t, err)
		require.Equal(t, data, read)
		pos += uint64(len(data)) + lenWidth
		require.Equal(t, pos, nextOffset)
	}
}

func apply(arc *Arc, reqType RequestType, req proto.Message) (
	interface{},
	error,
) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	commandPromise := arc.Apply(buf.Bytes(), timeout)
	if err := commandPromise.Error(); err != nil {
		return nil, err
	}
	res := commandPromise.Response().(*RecordResponse)
	return res.Response, nil
}

func setupTestLocus(t *testing.T, dataDir string) *Locus {
	c := Config{}
	pointcronConfig := pointcron.Config{}
	pointcronConfig.CloseTimeout = 1 * time.Second
	pointcronConfig.TickTime = time.Second
	c.Point.pointScheduler = pointcron.NewPointScheduler(pointcronConfig)
	c.Point.pointScheduler.StartAsync()

	locus, err := NewLocus(dataDir, c)
	require.NoError(t, err)

	return locus
}
