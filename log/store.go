package log

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(record *Record) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reqBodyBytes := new(bytes.Buffer)
	err = json.NewEncoder(reqBodyBytes).Encode(record)
	if err != nil {
		return 0, 0, err
	}

	pos = s.size

	if err := binary.Write(s.buf, enc, uint64(reqBodyBytes.Len())); err != nil {
		return 0, 0, err
	}

	w, err := s.buf.Write(reqBodyBytes.Bytes())
	if err != nil {
		return 0, 0, err
	}
	err = s.buf.Flush()
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) (*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	record := &Record{}
	buffer := new(bytes.Buffer)
	buffer.Write(b)
	err := json.NewDecoder(buffer).Decode(record)
	if err != nil {
		return nil, err
	}

	return record, nil
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
