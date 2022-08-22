package locus

import (
	"errors"
	"sync"

	"github.com/Brijeshlakkad/goutube/internal/log"
)

var (
	ErrPointNotFoundInMemory = errors.New("point not found in the store")
)

// InMemoryPointStore manages points and its last offset
type InMemoryPointStore struct {
	client map[string]*inMemoryPointValue
	log    *log.Log // Backup to warm up the cache
}

type inMemoryLogPoint struct {
	Point  string
	Offset uint64
}

type inMemoryPointValue struct {
	PointOffset uint64
	LogOffset   uint64
	mu          sync.Mutex
}

func (store *InMemoryPointStore) AddPointEvent(pointId string, offset uint64) error {
	logOffset, err := store.log.Append(&log.Record{Value: &inMemoryLogPoint{Point: pointId, Offset: offset}})
	if err != nil {
		return err
	}

	if _, ok := store.client[pointId]; !ok {
		store.client[pointId] = &inMemoryPointValue{}
	}

	store.client[pointId].mu.Lock()
	defer store.client[pointId].mu.Unlock()

	// Store both log and point offsets.
	store.client[pointId].PointOffset = offset
	store.client[pointId].LogOffset = logOffset

	return nil
}

func (store *InMemoryPointStore) GetPointEvent(pointId string) (uint64, error) {
	if _, ok := store.client[pointId]; !ok {
		return 0, ErrPointNotFoundInMemory
	}
	store.client[pointId].mu.Lock()
	defer store.client[pointId].mu.Unlock()
	return store.client[pointId].PointOffset, nil
}

func NewInMomoryPointStore(dir string) (*InMemoryPointStore, error) {
	logStore, err := log.NewLog(dir, log.Config{})
	if err != nil {
		return nil, err
	}

	return &InMemoryPointStore{
		client: make(map[string]*inMemoryPointValue),
		log:    logStore,
	}, nil
}
