package reader

import (
	"os"
)

type Reader struct {
	Pos int64
}

type FileReader struct {
	Reader *Reader
	File   *os.File
}

func NewFileReader(filePath string, pos int64) (*FileReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	return &FileReader{File: file, Reader: &Reader{Pos: pos}}, nil
}
