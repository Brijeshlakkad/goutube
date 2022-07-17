package locus

import (
	"io/ioutil"
	"os"
	"sync"

	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/Brijeshlakkad/goutube/internal/locus/pointcron"
)

type LociManager struct {
	mu     *sync.Mutex
	Config Config

	Dir  string
	loci map[string]*Locus
}

func NewLociManager(dir string, config Config) (*LociManager, error) {
	config.Point.pointScheduler = pointcron.NewPointScheduler(pointcron.Config{
		CloseTimeout: config.Point.CloseTimeout,
		TickTime:     config.Point.TickTime,
	})
	config.Point.pointScheduler.StartAsync()

	l := &LociManager{
		Config: config,
		Dir:    dir,
		mu:     new(sync.Mutex),
		loci:   make(map[string]*Locus),
	}
	return l, l.setup()
}

func (l *LociManager) setup() error {
	directories, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	for _, directory := range directories {
		lo, err := newLocus(l.Dir, directory.Name(), l.Config)
		if err != nil {
			return err
		}
		l.loci[lo.locusId] = lo
	}
	return nil
}

func (l *LociManager) addLocus(locusId string) (*Locus, error) {
	lo, err := newLocus(l.Dir, locusId, l.Config)
	if err != nil {
		return nil, err
	}

	l.loci[lo.locusId] = lo
	return lo, nil
}

func (l *LociManager) GetLoci() []string {
	list := make([]string, 0, len(l.loci))

	for locusId, _ := range l.loci {
		list = append(list, locusId)
	}
	return list
}

func (l *LociManager) GetPoints(locusId string) []string {
	lo, err := l.get(locusId)
	if err != nil {
		return nil
	}

	return lo.GetPoints()
}

func (l *LociManager) get(locusId string) (*Locus, error) {
	p, ok := l.loci[locusId]
	if !ok {
		return nil, streaming_api.LocusNotFound{LocusId: locusId}
	}
	return p, nil
}

func (l *LociManager) Append(locusId string, pointId string, b []byte) (pos uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lo, err := l.get(locusId)
	if _, ok := err.(streaming_api.LocusNotFound); ok {
		lo, err = l.addLocus(locusId)

		l.loci[lo.locusId] = lo
	}

	_, pos, err = lo.Append(pointId, b)
	return pos, err
}

func (l *LociManager) Read(locusId string, pointId string, pos uint64) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lo, err := l.get(locusId)
	if err != nil {
		return nil, err
	}

	return lo.Read(pointId, pos)
}

func (l *LociManager) ReadAt(locusId string, pointId string, b []byte, off int64) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lo, err := l.get(locusId)
	if err != nil {
		return 0, err
	}

	return lo.ReadAt(pointId, b, off)
}

func (l *LociManager) Close(locusId string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	lo, err := l.get(locusId)
	if err != nil {
		return err
	}
	return lo.CloseAll()
}

func (l *LociManager) ClosePoint(locusId string, pointId string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	lo, err := l.get(locusId)
	if err != nil {
		return err
	}
	return lo.Close(pointId)
}

func (l *LociManager) CloseAll() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, locus := range l.loci {
		if err := locus.CloseAll(); err != nil {
			return err
		}
	}
	return nil
}

func (l *LociManager) Remove(locusId string) error {
	lo, err := l.get(locusId)
	if err != nil {
		return err
	}
	if err := lo.RemoveAll(); err != nil {
		return err
	}
	delete(l.loci, locusId)
	return nil
}

func (l *LociManager) RemoveAll() error {
	if err := l.CloseAll(); err != nil {
		return err
	}
	for locusId, locus := range l.loci {
		if err := locus.RemoveAll(); err != nil {
			return err
		}
		delete(l.loci, locusId)
	}
	return os.RemoveAll(l.Dir)
}

func (l *LociManager) Reset() error {
	if err := l.RemoveAll(); err != nil {
		return err
	}
	return l.setup()
}
