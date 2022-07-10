package locus

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
)

type Locus struct {
	mu sync.RWMutex

	locusId  string
	locusDir string
	Config   Config

	points map[string]*Point
}

func newLocus(parentDir string, locusId string, config Config) (*Locus, error) {
	// Create a hierarchy of directories if necessary
	locusDir := filepath.Join(parentDir, locusId)
	if err := os.MkdirAll(locusDir, os.ModePerm); err != nil {
		return nil, err
	}

	l := &Locus{
		locusId:  locusId,
		locusDir: locusDir,
		Config:   config,
		points:   make(map[string]*Point),
	}
	return l, l.setup()
}

func (l *Locus) setup() error {
	files, err := ioutil.ReadDir(l.locusDir)
	if err != nil {
		return err
	}
	for _, file := range files {
		p, err := newPoint(l.locusDir, file.Name(), false)
		if err != nil {
			return err
		}
		l.points[p.pointId] = p
	}
	return nil
}

func (l *Locus) add(pointId string, open bool) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	p, err := l.get(pointId)
	if err != nil {
		_, ok := err.(streaming_api.PointNotFound)
		if ok {
			p, err = newPoint(l.locusDir, pointId, open)
			if err != nil {
				return "", err
			}

			l.points[p.pointId] = p
		} else {
			return "", err
		}
	}
	if open {
		return p.pointId, p.Open()
	}
	return p.pointId, nil
}

func (l *Locus) GetPoints() []string {
	list := make([]string, 0, len(l.points))

	for pointId, _ := range l.points {
		list = append(list, pointId)
	}
	return list
}

func (l *Locus) get(pointId string) (*Point, error) {
	p, ok := l.points[pointId]
	if !ok {
		return nil, streaming_api.PointNotFound{PointId: pointId}
	}
	return p, nil
}

func (l *Locus) Open(pointId string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	p, err := l.get(pointId)
	if err != nil {
		return err
	}
	return p.Open()
}

func (l *Locus) Append(pointId string, b []byte) (n uint64, pos uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	p, err := l.get(pointId)
	if err != nil {
		return 0, 0, err
	}

	return p.Append(b)
}

func (l *Locus) Read(pointId string, pos uint64) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	p, err := l.get(pointId)
	if err != nil {
		return nil, err
	}

	return p.Read(pos)
}

func (l *Locus) ReadAt(pointId string, b []byte, off int64) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	p, err := l.get(pointId)
	if err != nil {
		return 0, err
	}

	return p.ReadAt(b, off)
}

func (l *Locus) Close(pointId string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	p, err := l.get(pointId)
	if err != nil {
		return err
	}
	return p.Close()
}

func (l *Locus) CloseAll() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, point := range l.points {
		if err := point.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Locus) Remove(pointId string) error {
	if err := l.Close(pointId); err != nil {
		return err
	}
	delete(l.points, pointId)
	return nil
}

func (l *Locus) RemoveAll() error {
	if err := l.CloseAll(); err != nil {
		return err
	}
	return os.RemoveAll(l.locusDir)
}

func (l *Locus) Reset() error {
	if err := l.RemoveAll(); err != nil {
		return err
	}
	return l.setup()
}
