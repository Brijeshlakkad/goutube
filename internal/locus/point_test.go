package locus

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestPointAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "point_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	fi, err := os.Stat(f.Name())

	p, err := newPoint(filepath.Dir(f.Name()), fi.Name())
	require.NoError(t, err)

	testAppend(t, p)
	testRead(t, p)
	testReadAt(t, p)

	p, err = newPoint(filepath.Dir(f.Name()), fi.Name())
	require.NoError(t, err)
	testRead(t, p)
}

func testAppend(t *testing.T, p *Point) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := p.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, p *Point) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := p.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

func testReadAt(t *testing.T, p *Point) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := p.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		off += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = p.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, write, b)
		require.Equal(t, int(size), n)
		off += int64(n)
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

	p, err := newPoint(filepath.Dir(f.Name()), fi.Name())
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
