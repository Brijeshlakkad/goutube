package locus

import (
	"fmt"
	"io/ioutil"
	"testing"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	. "github.com/Brijeshlakkad/goutube/internal/test_util"
	"github.com/stretchr/testify/require"
)

var (
	locusId = "goutube-client"
	pointId = "sample_test_file"
)

func TestLocusManager(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, lm *LociManager,
	){
		"create five loci":                  testCreateLoci,
		"append and read a record succeeds": testLocusAppendRead,
		"locus should found":                testLocusShouldFound,
		"locus not found":                   testLocusNotFoundErr,
		"append on non-existing point":      testNotExistingLocusAppend,
		"remove locus":                      testRemoveLocus,
	} {
		t.Run(scenario, func(t *testing.T) {
			parentDir, err := ioutil.TempDir("", "locus-test")
			require.NoError(t, err)
			// defer os.RemoveAll(dir)

			c := Config{}
			loci, err := NewLociManager(parentDir, c)
			require.NoError(t, err)

			fn(t, loci)
		})
	}
}

func testCreateLoci(t *testing.T, lm *LociManager) {
	locusCount := 5
	for i := 0; i < locusCount; i++ {
		newLocusId := fmt.Sprintf("%s-%d", locusId, i)
		_, err := lm.AddLocus(newLocusId)
		require.NoError(t, err)
	}

	require.Equal(t, locusCount, len(lm.List()))
}

func testLocusAppendRead(t *testing.T, lm *LociManager) {
	lId, pId, err := lm.AddPoint(locusId, pointId, true)
	require.NoError(t, err)

	defer lm.Remove(locusId)

	pos, err := lm.Append(lId, pId, testWrite)
	require.Equal(t, uint64(0), pos)
	require.NoError(t, err)

	b, err := lm.Read(lId, pId, 0)
	require.Equal(t, b, testWrite)
}

func testLocusShouldFound(t *testing.T, lm *LociManager) {
	lId, _, err := lm.AddPoint(locusId, pointId, true)
	require.NoError(t, err)

	defer lm.Remove(lId)

	_, err = lm.get(lId)
	require.NoError(t, err)
}

func testLocusNotFoundErr(t *testing.T, lm *LociManager) {
	_, err := lm.get(locusId)
	apiErr := err.(streaming_api.LocusNotFound)
	require.Equal(t, locusId, apiErr.LocusId)
}

func testNotExistingLocusAppend(t *testing.T, lm *LociManager) {
	if err := lm.Remove(locusId); err != nil {
		require.Error(t, err)
	}

	got := PanicValue(func() {
		_, _ = lm.Append(locusId, pointId, testWrite)
	})
	_, ok := got.(error)
	if ok {
		t.Error("Expected No Error")
	}
}

func testRemoveLocus(t *testing.T, lm *LociManager) {
	lId, pId, err := lm.AddPoint(locusId, pointId, true)
	require.NoError(t, err)

	err = lm.Remove(lId)

	_, err = lm.Read(lId, pId, 0)
	apiErr := err.(streaming_api.LocusNotFound)
	require.Equal(t, locusId, apiErr.LocusId)
}
