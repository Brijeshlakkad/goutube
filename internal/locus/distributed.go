package locus

import (
	"os"
	"path/filepath"
)

const RaftRPC = 1

type DistributedLociManager struct {
	config      Config
	lociManager *LociManager
}

func NewDistributedPoint(dataDir string, config Config) (*DistributedLociManager, error) {
	l := &DistributedLociManager{
		config: config,
	}
	return l, l.setupLog(dataDir)
}

func (l *DistributedLociManager) setupLog(dataDir string) error {
	locusMDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(locusMDir, 0755); err != nil {
		return err
	}
	var err error
	l.lociManager, err = NewLociManager(locusMDir, l.config)
	return err
}

func (l *DistributedLociManager) Join(id, addr string) error {
	return nil
}

func (l *DistributedLociManager) Leave(id string) error {
	return nil
}

func (l *DistributedLociManager) Close() error {
	return l.lociManager.CloseAll()
}
