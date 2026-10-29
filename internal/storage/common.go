package storage

import (
	"atlas/internal/data"
	"io"
	"os"
)

func getFileEntriesFromIndex(file *os.File, entryIndex []int) ([]*data.Entry, error) {
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

func getEntriesFromCurrentFileOffset(file *os.File, entryIndex []int) ([]*data.Entry, error) {
	if len(entryIndex) == 0 {
		return []*data.Entry{}, nil
	}

	entryIndex = append([]int{0}, entryIndex...)

	var result []*data.Entry = nil
	for i, offset := range entryIndex[1:] {
		prevOffset := entryIndex[i]

		bufferSize := offset - prevOffset
		buffer := make([]byte, bufferSize)
		if _, err := file.Read(buffer); err != nil {
			return nil, err
		}

		entry, err := data.DeserializeEntry(string(buffer))
		if err != nil {
			return nil, err
		}

		result = append(result, entry)
	}
	return result, nil
}
