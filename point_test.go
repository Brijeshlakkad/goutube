package goutube

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write))
)

func TestPointAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "point_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	fi, err := os.Stat(f.Name())

	config := Config{}
	config.Distributed.MaxChunkSize = width
	p, err := newPoint(filepath.Dir(f.Name()), fi.Name(), config)
	require.NoError(t, err)

	testAppend(t, p)
	testRead(t, p)
	testReadAt(t, p)

	p, err = newPoint(filepath.Dir(f.Name()), fi.Name(), config)
	require.NoError(t, err)
	testRead(t, p)
}

func testAppend(t *testing.T, p *Point) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, _, err := p.Append(write)
		require.NoError(t, err)
		require.Equal(t, n, width*i)
	}
}

func testRead(t *testing.T, p *Point) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		nextOffset, read, err := p.Read(pos, 0, 0)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
		require.Equal(t, pos, nextOffset)
	}
}

func testReadAt(t *testing.T, p *Point) {
	t.Helper()
	size := width
	for i, off := uint64(1), uint64(0); i < 4; i++ {
		b := make([]byte, size)
		n, err := p.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, int(size), n)
		off += uint64(n)
	}
}

func TestPointOpenClose(t *testing.T) {
	f, err := ioutil.TempFile("", "point_close_test")
	require.NoError(t, err)
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
		}
	}(f.Name())
	fi, err := os.Stat(f.Name())

	config := Config{}
	config.Distributed.MaxChunkSize = 256
	p, err := newPoint(filepath.Dir(f.Name()), fi.Name(), config)
	require.NoError(t, err)
	_, _, err = p.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = p.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize >= beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
