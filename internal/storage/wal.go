package storage

import (
	"atlas/internal/data"
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
)

type WalConfig struct {
	Dir     string
	MaxLogs int
}

// Write Ahead Log
type Wal struct {
	file          *os.File
	index         []int
	currentOffset int
	log           *zap.Logger
}

const (
	defaultFilePermission = 0644
	defaultDirPermission  = 0755
	walFileExtension      = ".wal"
)

func NewWal(log *zap.Logger, filename string) (*Wal, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, defaultFilePermission)
	if err != nil {
		return nil, err
	}

	return &Wal{file: file, log: log}, nil
}

// RestoreEntries scans every .wal file in dir and returns the most recent
// entry per key. Files are left in place; compaction handles cleanup.
func RestoreEntries(log *zap.Logger, dir string) (data.EntryLookup, error) {
	dirFiles, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return data.EntryLookup{}, nil
		}
		log.Warn("read WAL directory failed",
			zap.String("dir", dir), zap.Error(err))
		return nil, err
	}

	lookup := make(data.EntryLookup)
	for _, file := range dirFiles {
		if file.IsDir() || !strings.HasSuffix(file.Name(), walFileExtension) {
			continue
		}

		path := filepath.Join(dir, file.Name())
		if err := mergeEntriesFromFile(log, path, lookup); err != nil {
			return nil, err
		}
	}
	return lookup, nil
}

func (wal *Wal) Count() int {
	return len(wal.index)
}

func (wal *Wal) Append(entry *data.Entry) error {
	serialized := entry.Serialize()
	written, err := wal.file.Write([]byte(serialized))
	if err != nil {
		return err
	}

	if written < len(serialized) {
		return errors.New("failed appending to WAL - partially written new entry")
	}

	wal.currentOffset += written
	wal.index = append(wal.index, wal.currentOffset)
	return nil
}

func (wal *Wal) CloseAndGetEntries() ([]*data.Entry, error) {
	result, err := getFileEntriesFromIndex(wal.file, wal.index)
	if err != nil {
		return nil, err
	}

	if err := wal.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

func (wal *Wal) Close() error {
	return wal.file.Close()
}

func mergeEntriesFromFile(log *zap.Logger, path string, lookup data.EntryLookup) error {
	file, err := os.Open(path)
	if err != nil {
		log.Warn("open WAL file failed",
			zap.String("file", path), zap.Error(err))
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), data.MaxEntrySize)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		entry, err := data.DeserializeEntry(line)
		if err != nil {
			log.Warn("skip malformed WAL line",
				zap.String("file", path), zap.Error(err))
			continue
		}

		cached, exists := lookup[entry.Key()]
		if !exists || entry.Timestamp() > cached.Timestamp() {
			lookup[entry.Key()] = entry
		}
	}
	return scanner.Err()
}
