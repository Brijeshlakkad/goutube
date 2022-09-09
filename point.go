package goutube

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type Point struct {
	File      *os.File
	pointId   string
	pointDir  string
	pointLock sync.Mutex
	buf       *bufio.Writer
	size      uint64

	closed atomic.Value
	close  chan string

	lastAccessed time.Time
	accessLock   sync.Mutex
	config       Config
}

func newPoint(locusId string, relativePointId string, config Config) (*Point, error) {
	p := new(Point)
	p.pointId = relativePointId
	p.config = config
	p.pointDir = createPointId(locusId, relativePointId)
	p.closed.Store(true)
	p.close = make(chan string)
	return p, nil
}

func (p *Point) Open() error {
	var err error
	p.File, err = os.OpenFile(p.pointDir, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	fi, err := os.Stat(p.pointDir)
	if err != nil {
		return err
	}
	p.size = uint64(fi.Size())
	p.buf = bufio.NewWriter(p.File)
	p.closed.Store(false)
	p.close = make(chan string)
	return nil
}

func (p *Point) Append(b []byte) (n uint64, pos uint64, err error) {
	p.pointLock.Lock()
	defer p.pointLock.Unlock()

	if p.closed.Load().(bool) {
		if err := p.Open(); err != nil {
			return 0, 0, err
		}
	}

	pos = p.size

	w, err := p.buf.Write(b)
	if err != nil {
		return 0, 0, err
	}
	p.size += uint64(w)
	return p.size, pos, nil
}

func (p *Point) Read(pos uint64, chunkSize uint64, limit uint64) (uint64, []byte, error) {
	p.pointLock.Lock()
	defer p.pointLock.Unlock()

	if p.closed.Load().(bool) {
		if err := p.Open(); err != nil {
			return 0, nil, err
		}
	}

	// set limit
	if limit == 0 {
		limit = p.size
	}

	// file doesn't have more reads
	if limit-pos == 0 {
		return 0, nil, io.EOF
	}

	// set size of the buffer
	if chunkSize == 0 {
		chunkSize = p.config.Distributed.MaxChunkSize
		chunkSize = min(limit-pos, chunkSize)
	}

	if err := p.buf.Flush(); err != nil {
		return 0, nil, err
	}
	b := make([]byte, chunkSize) // create buffer with size of minimum of available reads and MaxChunkSize to prevent EOF
	if _, err := p.File.ReadAt(b, int64(pos)); err != nil {
		return 0, nil, err
	}
	return pos + uint64(len(b)), b, nil
}

func (p *Point) ReadAt(b []byte, off uint64) (int, error) {
	p.pointLock.Lock()
	defer p.pointLock.Unlock()

	if p.closed.Load().(bool) {
		if err := p.Open(); err != nil {
			return 0, err
		}
	}

	if err := p.buf.Flush(); err != nil {
		return 0, err
	}
	return p.File.ReadAt(b, int64(off))
}

func (p *Point) GetMetadata() PointMetadata {
	p.pointLock.Lock()
	defer p.pointLock.Unlock()

	return PointMetadata{
		size: p.size,
	}
}

func (p *Point) Close() error {
	p.pointLock.Lock()
	defer p.pointLock.Unlock()

	if p.closed.Load().(bool) {
		return nil
	}

	p.closed.Store(true)
	if p.buf != nil {
		err := p.buf.Flush()
		_, ok := err.(*os.PathError)
		if ok {
			return nil
		}
		if err != nil {
			return err
		}
		return p.File.Close()
	}
	return nil
}

func createPointId(locusId string, relativePointId string) string {
	return filepath.Join(locusId, relativePointId)
}

func (p *Point) GetLastAccessed() time.Time {
	p.accessLock.Lock()
	defer p.accessLock.Unlock()

	return p.lastAccessed
}

func (p *Point) setLastAccessed() {
	p.accessLock.Lock()
	defer p.accessLock.Unlock()

	p.lastAccessed = time.Now()
}

type PointMetadata struct {
	size uint64
}
