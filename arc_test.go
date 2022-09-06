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
	"github.com/Brijeshlakkad/ring"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
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
		resp, err := apply(arc, AppendRequestType, &streaming_api.ProduceRequest{Point: testPointId, Frame: data})
		require.NoError(t, err)

		records := resp.(*streaming_api.ProduceResponse).Records
		require.Equal(t, 1, len(records))
	}

	var pos uint64
	for i := uint64(0); i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		nextOffset, read, err := locus.Read(testPointId, pos)
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
		resp, err := apply(arc_leader, AppendRequestType, &streaming_api.ProduceRequest{Point: testPointId, Frame: data})
		require.NoError(t, err)

		records := resp.(*streaming_api.ProduceResponse).Records
		require.Equal(t, 1, len(records))
	}

	// Wait for replication to get completed!
	time.Sleep(5 * time.Second)

	var pos uint64
	for i := uint64(0); i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		nextOffset, read, err := locus_Follower.Read(testPointId, pos)
		require.NoError(t, err)
		require.Equal(t, data, read)
		pos += uint64(len(data)) + lenWidth
		require.Equal(t, pos, nextOffset)
	}
}

func TestArc_TransferResponsibility(t *testing.T) {
	pointId1 := "2"
	pointId2 := "6"

	ports := dynaport.Get(2)

	bindAddr_Peer_0 := fmt.Sprintf("localhost:%d", ports[0])

	hashFunction := &testHashFunction{
		cache: make(map[string]uint64),
	}

	_, locus_Peer_0, teardown_Peer_0 := setupTestArcRing(t, "1",
		2,
		hashFunction,
		&ring.Config{
			BindAddr:   bindAddr_Peer_0,
			MemberType: ring.ShardMember,
		})
	defer teardown_Peer_0()

	hashFunction.cache[pointId1] = 3
	hashFunction.cache[pointId2] = 10

	expectedOffset := uint64(0)
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		nextOffset, offset, err := locus_Peer_0.Append(pointId1, data)
		require.NoError(t, err)
		require.Equal(t, offset, expectedOffset)
		expectedOffset = nextOffset
	}

	expectedOffset = uint64(0)
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		nextOffset, offset, err := locus_Peer_0.Append(pointId2, data)
		require.NoError(t, err)
		require.Equal(t, offset, expectedOffset)
		expectedOffset = nextOffset
	}

	time.Sleep(1 * time.Second)

	bindAddr_Peer_1 := fmt.Sprintf("localhost:%d", ports[1])
	_, locus_Peer_1, teardown_Peer_1 := setupTestArcRing(t, "4",
		7,
		hashFunction,
		&ring.Config{
			BindAddr:      bindAddr_Peer_1,
			MemberType:    ring.ShardMember,
			SeedAddresses: []string{bindAddr_Peer_0},
		})
	defer teardown_Peer_1()

	// Wait for replication to get completed!
	time.Sleep(5 * time.Second)

	var pos uint64
	for i := uint64(0); i < 10; i++ {
		data := []byte(fmt.Sprintf("Test data line %d", i))
		nextOffset, read, err := locus_Peer_1.Read(pointId1, pos)
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

func setupTestArcRing(t *testing.T, localId string, hashValue uint64, thf *testHashFunction, ringConfig *ring.Config) (*Arc, *Locus, func()) {
	t.Helper()

	// Arc
	ports := dynaport.Get(1)

	rpcAddr := fmt.Sprintf("localhost:%d", ports[0])
	streamLayer, err := newTCPStreamLayer(rpcAddr, nil)
	require.NoError(t, err)

	dataDir, err := ioutil.TempDir("", "arc-test")
	require.NoError(t, err)

	locusDir, err := createDirectory(dataDir, "locus")
	require.NoError(t, err)

	logDir, err := createDirectory(dataDir, "log")
	require.NoError(t, err)

	logStore, err := NewInMomoryPointStore(logDir)
	require.NoError(t, err)

	locus := setupTestLocus(t, locusDir)
	fsmInstance := &fsm{
		locus,
	}

	arc, err := NewArc(ArcConfig{
		StreamLayer: streamLayer,
		fsm:         fsmInstance,
		store:       logStore,
		Bundler:     &RequestBundler{},
	})
	require.NoError(t, err)

	rc := &handleResponsibilityChange{
		arc:   arc,
		locus: locus,
	}

	if ringConfig != nil {
		nodeName := ringConfig.NodeName
		if nodeName == "" {
			nodeName = fmt.Sprintf("ring-%s", localId)
		}
		bindAddr := ringConfig.BindAddr
		if bindAddr == "" {
			bindPorts := dynaport.Get(0)
			bindAddr = fmt.Sprintf("localhost:%d", bindPorts[0])
		}
		tags := map[string]string{
			rpcAddressRingTag: rpcAddr,
		}
		virtualNodeCount := ringConfig.VirtualNodeCount
		if virtualNodeCount == 0 {
			virtualNodeCount = 1
		}
		seedAddresses := ringConfig.SeedAddresses
		if seedAddresses == nil {
			seedAddresses = []string{}
		}
		//thf.cache[nodeName] = hashValue
		ringInstance, err := ring.NewRing(ring.Config{
			NodeName:         nodeName,
			BindAddr:         bindAddr,
			Tags:             tags,
			VirtualNodeCount: virtualNodeCount,
			SeedAddresses:    seedAddresses,
			MemberType:       ringConfig.MemberType,
			//HashFunction:     thf,
			Timeout: 20 * time.Second,
		})
		require.NoError(t, err)

		ringInstance.AddResponsibilityChangeListener(localId, rc)
	}

	return arc, locus, func() {
		os.RemoveAll(dataDir)
	}
}

type testHashFunction struct {
	counter uint64
	cache   map[string]uint64
}

func (hf *testHashFunction) Hash(key string) uint64 {
	if _, ok := hf.cache[key]; ok {
		return hf.cache[key]
	}
	hf.counter += 1
	return hf.counter
}

type handleResponsibilityChange struct {
	arc   *Arc
	locus *Locus
}

func (rc *handleResponsibilityChange) OnChange(batch []ring.ShardResponsibility) {
	rc.arc.onResponsibilityChange(batch, rc.locus.GetPoints())
}
