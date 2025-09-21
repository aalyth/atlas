package storage

import (
	"atlas/internal/common"
	"atlas/pkg/logger"
	"errors"
	"os"
)

type WalConfig struct {
	Dir     string
	MaxLogs int
}

// Write Ahead Log
type Wal struct {
	file          *os.File
	filename      string
	index         []int64
	currentOffset int64
}

const defaultFilePermission = 0644

func CreateWal(filename string) (*Wal, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, defaultFilePermission)
	if err != nil {
		return nil, err
	}

	return &Wal{file, filename, nil, 0}, nil
}

func RestoreWal(filename string) (*Wal, error) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0)
	if err != nil {
		logger.Error("Failed restoring WAL file (%s): %v", file, err)
		return nil, err
	}

}

func (wal *Wal) Count() int {
	return len(wal.index)
}

func (wal *Wal) Append(entry *common.Entry) error {
	serialized := entry.Serialize()
	written, err := wal.file.Write([]byte(serialized))
	if err != nil {
		return err
	}

	if written < len(serialized) {
		return errors.New("Failed appending to WAL - partially written new entry")
	}

	wal.currentOffset += int64(written)
	wal.index = append(wal.index, wal.currentOffset)
	return nil
}

func (wal *Wal) CloseAndGetEntries() ([]*common.Entry, error) {
	result, err := getFileEntries(wal.file, wal.index)
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
