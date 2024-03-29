package goutube

import (
	"io/ioutil"
	"os"
	"sync"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
)

type Locus struct {
	mu sync.RWMutex

	locusDir string
	Config   Config

	points map[string]*Point
}

func NewLocus(locusDir string, config Config) (*Locus, error) {
	l := &Locus{
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
		p, err := newPoint(l.locusDir, file.Name(), l.Config)
		if err != nil {
			return err
		}
		l.points[p.pointId] = p
	}
	return nil
}

func (l *Locus) addPoint(pointId string) (*Point, error) {
	p, err := l.get(pointId)
	if err != nil {
		_, ok := err.(streaming_api.PointNotFound)
		if ok {
			p, err = newPoint(l.locusDir, pointId, l.Config)
			if err != nil {
				return nil, err
			}
			l.points[p.pointId] = p
			return p, nil
		}
		return nil, err
	}
	return p, nil
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

func (l *Locus) Append(pointId string, b []byte) (n uint64, pos uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	point, err := l.get(pointId)
	if err != nil {
		if _, ok := err.(streaming_api.PointNotFound); ok {
			point, err = l.addPoint(pointId)
		} else {
			return 0, 0, nil
		}
	}

	defer l.Config.Point.pointScheduler.Enqueue(point)

	return point.Append(b)
}

func (l *Locus) Read(pointId string, pos uint64, chunkSize uint64, limit uint64) (uint64, []byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	point, err := l.get(pointId)
	if err != nil {
		return 0, nil, err
	}
	defer l.Config.Point.pointScheduler.Enqueue(point)

	return point.Read(pos, chunkSize, limit)
}

func (l *Locus) ReadAt(pointId string, b []byte, off uint64) (int, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	point, err := l.get(pointId)
	if err != nil {
		return 0, err
	}
	defer l.Config.Point.pointScheduler.Enqueue(point)

	return point.ReadAt(b, off)
}

func (l *Locus) GetMetadata(pointId string) (PointMetadata, error) {
	point, err := l.get(pointId)
	if err != nil {
		return PointMetadata{}, err
	}
	return point.GetMetadata(), nil
}

func (l *Locus) Close(pointId string) error {
	p, err := l.get(pointId)
	if err != nil {
		return err
	}
	return p.Close()
}

func (l *Locus) CloseAll() error {
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
	l.points = make(map[string]*Point)
	return os.RemoveAll(l.locusDir)
}

func (l *Locus) Reset() error {
	if err := l.RemoveAll(); err != nil {
		return err
	}
	return l.setup()
}
