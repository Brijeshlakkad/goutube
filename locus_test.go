package goutube

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/Brijeshlakkad/goutube/pointcron"
	"github.com/stretchr/testify/require"
)

var (
	testWrite = []byte("hello world")
)

func TestLocus(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, locus *Locus,
	){
		"create five pointers":                 testCreatePointers,
		"append and read a record succeeds":    testPointAppendRead,
		"point should found":                   testPointShouldFound,
		"point not found":                      testPointNotFoundErr,
		"append on non-existing point":         testNotExistingPointAppend,
		"remove pointer":                       testRemovePointer,
		"close unnecessary pointer":            testPointCloseAfter,
		"keep pointer open if a recent access": testPointKeepOpen,
	} {
		t.Run(scenario, func(t *testing.T) {
			parentDir, err := ioutil.TempDir("", "locus-test")
			require.NoError(t, err)
			defer os.RemoveAll(parentDir)

			c := Config{}
			c.Distributed.MaxChunkSize = uint64(len(testWrite))
			pointcronConfig := pointcron.Config{}
			pointcronConfig.CloseTimeout = 1 * time.Second
			pointcronConfig.TickTime = time.Second
			c.Point.pointScheduler = pointcron.NewPointScheduler(pointcronConfig)
			c.Point.pointScheduler.StartAsync()

			log, err := NewLocus(parentDir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testCreatePointers(t *testing.T, locus *Locus) {
	pointCount := 5
	for i := 0; i < pointCount; i++ {
		newPointId := fmt.Sprintf("locus-test-file-%d", i)
		_, err := locus.addPoint(newPointId)
		require.NoError(t, err)
	}

	require.Equal(t, pointCount, len(locus.GetPoints()))
}

func testPointAppendRead(t *testing.T, locus *Locus) {
	newPointId := "locus-test-file-0"
	point, err := locus.addPoint(newPointId)
	require.NoError(t, err)

	defer locus.Remove(point.pointId)

	_, pos, err := locus.Append(point.pointId, testWrite)
	require.Equal(t, uint64(0), pos)
	require.NoError(t, err)

	_, b, err := locus.Read(point.pointId, 0, 0, 0)
	require.Equal(t, b, testWrite)
}

func testPointShouldFound(t *testing.T, locus *Locus) {
	newPointId := "locus-test-file-0"

	pId, err := locus.addPoint(newPointId)
	require.NoError(t, err)

	defer locus.Remove(pId.pointId)

	_, err = locus.get(pId.pointId)
	require.NoError(t, err)
}

func testPointNotFoundErr(t *testing.T, locus *Locus) {
	pId := "locus-test-file-0"

	_, err := locus.get(pId)
	apiErr := err.(streaming_api.PointNotFound)
	require.Equal(t, pId, apiErr.PointId)
}

func testNotExistingPointAppend(t *testing.T, locus *Locus) {
	pId := "locus-test-file-0"

	if err := locus.Remove(pId); err != nil {
		require.Error(t, err)
	}

	got := PanicValue(func() {
		_, _, _ = locus.Append(pId, testWrite)
	})
	_, ok := got.(error)
	if ok {
		t.Error("Expected No Error")
	}
}

func testRemovePointer(t *testing.T, locus *Locus) {
	pId := "locus-test-file-0"

	_, err := locus.addPoint(pId)
	require.NoError(t, err)

	err = locus.Remove(pId)

	_, _, err = locus.Read(pId, 0, 0, 0)
	apiErr := err.(streaming_api.PointNotFound)
	require.Equal(t, pId, apiErr.PointId)
}

func testPointCloseAfter(t *testing.T, locus *Locus) {
	pId := "locus-test-file-0"

	point, err := locus.addPoint(pId)
	require.NoError(t, err)

	_, _, err = locus.Append(point.pointId, write)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return point.closed.Load().(bool)
	}, 3*time.Second, 1*time.Second)

	_, _, err = locus.Append(point.pointId, write)
	require.NoError(t, err)
	require.Equal(t, point.closed.Load().(bool), false)

	require.Eventually(t, func() bool {
		return point.closed.Load().(bool)
	}, 3*time.Second, 1*time.Second)
}

func testPointKeepOpen(t *testing.T, locus *Locus) {
	pId := "locus-test-file-0"

	point, err := locus.addPoint(pId)
	require.NoError(t, err)

	_, _, err = locus.Append(point.pointId, write)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	_, _, err = locus.Append(point.pointId, write)
	require.NoError(t, err)

	require.Equal(t, point.closed.Load().(bool), false)

	require.Eventually(t, func() bool {
		return point.closed.Load().(bool)
	}, 3*time.Second, 1*time.Second)
}
