package locus

import (
	"bufio"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type Point struct {
	File     *os.File
	pointId  string
	pointDir string
	mu       *sync.Mutex
	buf      *bufio.Writer
	size     uint64
}

func newPoint(locusId string, relativePointId string, open bool) (*Point, error) {
	p := new(Point)
	p.pointId = relativePointId
	p.mu = new(sync.Mutex)
	p.pointDir = createPointId(locusId, relativePointId)

	if !open {
		return p, nil
	}

	return p, p.Open()
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
	return nil
}

func (p *Point) Append(b []byte) (n uint64, pos uint64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pos = p.size
	if err := binary.Write(p.buf, enc, uint64(len(b))); err != nil {
		return 0, 0, err
	}

	w, err := p.buf.Write(b)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	p.size += uint64(w)
	return uint64(w), pos, nil
}

func (p *Point) Read(pos uint64) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := p.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := p.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

func (p *Point) ReadAt(b []byte, off int64) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.buf.Flush(); err != nil {
		return 0, err
	}
	return p.File.ReadAt(b, off)
}

func (p *Point) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

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
