package storage

import (
	"atlas/internal/common"
	"errors"
	"os"
)

type Wal struct {
	file          *os.File
	filename      string
	index         []int64
	currentOffset int64
}

const defaultFilePermission = 0644

func CreateWriteAheadLog(filename string) (*Wal, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, defaultFilePermission)
	if err != nil {
		return nil, err
	}

	return &Wal{file, filename, nil, 0}, nil
}

func (wal *Wal) Append(entry common.Entry) error {
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
