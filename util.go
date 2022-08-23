package goutube

import (
	"os"
	"path/filepath"
)

// min returns the minimum.
func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

// max returns the maximum.
func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

// createDirectory creates the directory under parentDir.
func createDirectory(parentDir, dataDir string) (string, error) {
	dir := filepath.Join(parentDir, dataDir)
	// Create a hierarchy of directories if necessary
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	return dir, nil
}
