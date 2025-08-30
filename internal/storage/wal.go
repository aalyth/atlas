package storage

import (
	"atlas/internal/common"
	"io"
	"os"
)

type Wal struct {
	file     *os.File
	filename string
	index    []int64
}

const defaultFilePermission = 0644

func CreateWriteAheadLog(filename string) (*Wal, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, defaultFilePermission)
	if err != nil {
		return nil, err
	}

	return &Wal{file, filename, nil}, nil
}

func (wal *Wal) Append(entry common.Entry) error {
	offset, err := wal.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	serialized := entry.Serialize()
	if _, err := wal.file.Write([]byte(serialized)); err != nil {
		return err
	}

	wal.index = append(wal.index, offset)
	return nil
}

func (wal *Wal) CloseAndGetEntries() ([]*common.Entry, error) {
	result, err := wal.getEntries()
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

func (wal *Wal) getEntries() ([]*common.Entry, error) {
	if _, err := wal.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	result, err := wal.getEntriesFromCurrentFileOffset()
	if err != nil {
		if _, err := wal.file.Seek(0, io.SeekEnd); err != nil {
			return nil, err
		}

	}
	return result, nil
}

func (wal *Wal) getEntriesFromCurrentFileOffset() ([]*common.Entry, error) {
	if len(wal.index) == 0 {
		return []*common.Entry{}, nil
	}

	fileSize, err := fileSize(wal.file)
	if err != nil {
		return nil, nil
	}

	offsetIndex := append(wal.index, fileSize)

	var result []*common.Entry = nil
	for i, offset := range offsetIndex[1:] {
		prevOffset := offsetIndex[i]

		bufferSize := offset - prevOffset
		buffer := make([]byte, bufferSize)
		if _, err := wal.file.Read(buffer); err != nil {
			return nil, err
		}

		entry, err := common.DeserializeEntry(string(buffer))
		if err != nil {
			return nil, err
		}

		result = append(result, entry)
	}

	return nil, nil
}

func fileSize(file *os.File) (int64, error) {
	stat, err := file.Stat()
	if err != nil {
		return -1, err
	}

	return stat.Size(), nil
}
