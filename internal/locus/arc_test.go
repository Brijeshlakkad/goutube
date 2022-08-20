package locus

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestArc_FSM(t *testing.T) {
	streamLayer, err := newTCPStreamLayer("localhost:0", nil)
	require.NoError(t, err)

	dataDir, err := ioutil.TempDir("", "arc-test")
	require.NoError(t, err)
	defer os.RemoveAll(dataDir)

	lociManager, err := NewLociManager(dataDir, Config{})
	fsm := &fsm{
		lociManager,
	}

	arc := NewArc(ArcConfig{
		StreamLayer: streamLayer,
		fsm:         fsm,
	})

	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		resp, err := apply(arc, AppendRequestType, &streaming_api.ProduceRequest{Locus: locusId, Point: pointId, Frame: data})
		require.NoError(t, err)

		_ = resp.(*streaming_api.ProduceResponse).Offset
	}

	for i := 0; i < 10; i++ {
		var pos uint64
		for i := uint64(0); i < 10; i++ {
			data := []byte(fmt.Sprintf("Test data line %d", i))
			read, err := lociManager.Read(locusId, pointId, pos)
			require.NoError(t, err)
			require.Equal(t, data, read)
			pos += uint64(len(data)) + lenWidth
		}
	}
}

func TestArc_Followers(t *testing.T) {
	// Leader Arc
	streamLayer, err := newTCPStreamLayer("localhost:0", nil)
	require.NoError(t, err)

	dataDir, err := ioutil.TempDir("", "arc-test")
	require.NoError(t, err)
	defer func(dir string) {
		_ = os.RemoveAll(dir)
	}(dataDir)

	lociManager, err := NewLociManager(dataDir, Config{})
	fsm_leader := &fsm{
		lociManager,
	}

	arc_leader := NewArc(ArcConfig{
		StreamLayer: streamLayer,
		fsm:         fsm_leader,
	})

	// Follower Arc
	streamLayer_Follower, err := newTCPStreamLayer("localhost:0", nil)
	require.NoError(t, err)

	dataDir_Follower, err := ioutil.TempDir("", "arc-test")
	require.NoError(t, err)
	defer func(dir string) {
		_ = os.RemoveAll(dir)
	}(dataDir)

	lociManager_Follower, err := NewLociManager(dataDir_Follower, Config{})
	fsm_Follower := &fsm{
		lociManager_Follower,
	}

	_ = NewArc(ArcConfig{
		StreamLayer: streamLayer_Follower,
		fsm:         fsm_Follower,
	})

	followerState, err := NewFollower(ServerAddress(streamLayer_Follower.Addr().String()))
	arc_leader.replicateState[streamLayer_Follower.Addr().String()] = followerState
	require.NoError(t, err)

	go arc_leader.replicate(followerState)

	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		resp, err := apply(arc_leader, AppendRequestType, &streaming_api.ProduceRequest{Locus: locusId, Point: pointId, Frame: data})
		require.NoError(t, err)

		_ = resp.(*streaming_api.ProduceResponse).Offset
	}

	// Wait for replication to get completed!
	time.Sleep(3 * time.Second)

	var pos uint64
	for i := uint64(0); i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		read, err := lociManager_Follower.Read(locusId, pointId, pos)
		require.NoError(t, err)
		require.Equal(t, data, read)
		pos += uint64(len(data)) + lenWidth
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
	res := commandPromise.Response().(*CommandResponse)
	return res.Response, nil
}
