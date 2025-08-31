package storage

import (
	"atlas/internal/common"
	"io"
	"os"
)

func getFileEntries(file *os.File, entryIndex []int64) ([]*common.Entry, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	result, err := getEntriesFromCurrentFileOffset(file, entryIndex)
	if err != nil {
		if _, err := file.Seek(0, io.SeekEnd); err != nil {
			return nil, err
		}

	}
	return result, nil
}

func getEntriesFromCurrentFileOffset(file *os.File, entryIndex []int64) ([]*common.Entry, error) {
	if len(entryIndex) == 0 {
		return []*common.Entry{}, nil
	}

	entryIndex = append([]int64{0}, entryIndex...)

	var result []*common.Entry = nil
	for i, offset := range entryIndex[1:] {
		prevOffset := entryIndex[i]

		bufferSize := offset - prevOffset
		buffer := make([]byte, bufferSize)
		if _, err := file.Read(buffer); err != nil {
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
