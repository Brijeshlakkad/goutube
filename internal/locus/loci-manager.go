package locus

import (
	"io/ioutil"
	"os"
	"sync"

	api "github.com/Brijeshlakkad/goutube/api/v1"
)

type LociManager struct {
	mu     *sync.Mutex
	Config Config

	Dir  string
	loci map[string]*Locus
}

func NewLociManager(dir string, config Config) (*LociManager, error) {
	// Create a hierarchy of directories if necessary
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
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

func (l *LociManager) AddLocus(locusId string) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lo, err := l.get(locusId)
	if err != nil {
		_, ok := err.(api.LocusNotFound)
		if ok {
			lo, err = newLocus(l.Dir, locusId, l.Config)
			if err != nil {
				return "", err
			}

			l.loci[lo.locusId] = lo
			return lo.locusId, nil
		}
		return "", err
	}
	return lo.locusId, err
}

func (l *LociManager) AddPoint(locusId string, pointId string, open bool) (string, string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lo, err := l.get(locusId)
	if err != nil {
		_, ok := err.(api.LocusNotFound)
		if ok {
			lo, err = newLocus(l.Dir, locusId, l.Config)
			if err != nil {
				return "", "", err
			}

			l.loci[lo.locusId] = lo
		} else {
			return "", "", err
		}
	}
	pointId, err = lo.add(pointId, open)
	return lo.locusId, pointId, err
}

func (l *LociManager) List() []*Locus {
	list := make([]*Locus, 0, len(l.loci))

	for _, locus := range l.loci {
		list = append(list, locus)
	}
	return list
}

func (l *LociManager) get(locusId string) (*Locus, error) {
	p, ok := l.loci[locusId]
	if !ok {
		return nil, api.LocusNotFound{LocusId: locusId}
	}
	return p, nil
}

func (l *LociManager) Open(locusId string, pointId string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	lo, err := l.get(locusId)
	if err != nil {
		return nil
	}

	return lo.Open(pointId)
}

func (l *LociManager) Append(locusId string, pointId string, b []byte) (pos uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lo, err := l.get(locusId)
	if err != nil {
		return 0, nil
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
