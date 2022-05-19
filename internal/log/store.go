package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// 字节序，低地址位存放高位字节 11 => 16进制存储 0000 0000 0000 000b
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
	// in case the file has existing data
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

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	// pos 数据存放的位置
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	// write the length of record with bigEndian,so that we know how many bytes to read
	// uint64 occupy 8 bytes and `BigEndian` is similar to 0000 0000 0000 000b
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// with buffered writer can improve performance
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// Leave the space for storing record size and index
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// ensure record flush to disk
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
	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// persists any buffered data before closing the file
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
