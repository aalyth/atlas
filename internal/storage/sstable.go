package storage

import (
	"atlas/internal/data"
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
)

// Sorted String Table
type SSTable struct {
	file     *os.File
	filename string
	index    []int
	minKey   string
	maxKey   string
}

type SSTableBuilder struct {
	file     *os.File
	filename string
	offset   int
	index    []int
	minKey   string
	maxKey   string
}

type SSTableIterator struct {
	table     *SSTable
	peekCache *data.Entry
	position  int
}

func NewSSTableBuilder(filename string) (*SSTableBuilder, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &SSTableBuilder{
		file:     file,
		filename: filename,
		offset:   0,
		index:    nil,
		minKey:   "",
		maxKey:   "",
	}, nil
}

func (builder *SSTableBuilder) AddSorted(entry *data.Entry) error {
	serialized := entry.Serialize()
	written, err := builder.file.Write([]byte(serialized))
	if err != nil {
		return err
	}

	if written < len(serialized) {
		return errors.New("Failed adding to SSTableBuilder - partially written entry")
	}

	if builder.minKey > entry.Key() || builder.minKey == "" {
		builder.minKey = entry.Key()
	}

	if builder.maxKey < entry.Key() || builder.maxKey == "" {
		builder.maxKey = entry.Key()
	}

	builder.offset += written
	builder.index = append(builder.index, builder.offset)
	return nil
}

func (builder *SSTableBuilder) Build() *SSTable {
	return &SSTable{
		file:     builder.file,
		filename: builder.filename,
		index:    builder.index,
		minKey:   builder.minKey,
		maxKey:   builder.maxKey,
	}
}

func NewSSTable(filename string, entries []*data.Entry) (*SSTable, error) {
	if len(entries) == 0 {
		return nil, errors.New("Failed craeting SSTable - at least 1 entry is required")
	}

	if !slices.IsSortedFunc(entries, data.CompareEntries) {
		slices.SortFunc(entries, data.CompareEntries)
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	var offset int = 0
	var index []int = nil
	minKey := entries[0].Key()
	maxKey := entries[0].Key()
	for _, entry := range entries {
		if entry.Key() < minKey {
			minKey = entry.Key()
		}

		if entry.Key() > maxKey {
			maxKey = entry.Key()
		}

		serialized := entry.Serialize()
		written, err := file.Write([]byte(serialized))
		if err != nil {
			if err := file.Close(); err != nil {
				return nil, err
			}
			return nil, err
		}

		if written < len(serialized) {
			if err := file.Close(); err != nil {
				return nil, err
			}
			return nil, errors.New("Failed creating SSTable - partially written entry")
		}

		offset += written
		index = append(index, offset)
	}

	return &SSTable{
		file:     file,
		filename: filename,
		index:    index,
		minKey:   minKey,
		maxKey:   maxKey,
	}, nil
}

func RestoreSSTable(filePath string) (*SSTable, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("SSTable file does not exist: %s", filePath)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var index []int
	var currentOffset int = 0

	minKey := ""
	maxKey := ""
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), data.MaxEntrySize)
	for scanner.Scan() {
		line := scanner.Text()

		// +1 for the newline
		lineLength := len(line) + 1

		if strings.TrimSpace(line) == "" {
			currentOffset += lineLength
			continue
		}

		entry, err := data.DeserializeEntry(line)
		if err != nil {
			return nil, err
		}

		index = append(index, currentOffset)
		currentOffset += lineLength

		if minKey == "" || entry.Key() < minKey {
			minKey = entry.Key()
		}

		if maxKey == "" || entry.Key() > maxKey {
			maxKey = entry.Key()
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	sstableFile, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return &SSTable{
		file:     sstableFile,
		filename: filePath,
		index:    index,
		minKey:   minKey,
		maxKey:   maxKey,
	}, nil
}

func (table *SSTable) Get(key string) (*data.Entry, bool, error) {
	if key < table.minKey || key > table.maxKey {
		return nil, false, nil
	}

	left := 0
	right := len(table.index) - 1
	for left <= right {
		mid := left + (right-left)/2

		entry, err := table.getEntryAt(mid)
		if err != nil {
			return nil, false, err
		}

		cmp := strings.Compare(key, entry.Key())
		if cmp == 0 {
			return entry, true, nil
		} else if cmp < 0 {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	return nil, false, nil
}

func (table *SSTable) Entries() ([]*data.Entry, error) {
	if len(table.index) == 0 {
		return []*data.Entry{}, nil
	}

	if _, err := table.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	offsetIndex := append([]int{0}, table.index...)

	var result []*data.Entry = nil
	for i, offset := range offsetIndex[1:] {
		prevOffset := offsetIndex[i]

		bufferSize := offset - prevOffset
		buffer := make([]byte, bufferSize)
		if _, err := table.file.Read(buffer); err != nil {
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

func (table *SSTable) Iterator() *SSTableIterator {
	return &SSTableIterator{
		table:     table,
		peekCache: nil,
		position:  0,
	}
}

func (iter *SSTableIterator) IsEmpty() bool {
	return iter.position >= len(iter.table.index)
}

func (iter *SSTableIterator) Peek() (*data.Entry, bool, error) {
	if iter.IsEmpty() {
		return nil, false, nil
	}

	if iter.peekCache != nil {
		return iter.peekCache, true, nil
	}

	entry, err := iter.table.getEntryAt(iter.position)
	if err != nil {
		return nil, false, err
	}

	iter.peekCache = entry
	return entry, true, nil
}

func (iter *SSTableIterator) Advance() (*data.Entry, bool, error) {
	if iter.IsEmpty() {
		return nil, false, nil
	}

	if iter.peekCache != nil {
		result := iter.peekCache
		iter.peekCache = nil
		iter.position += 1
		return result, true, nil
	}

	result, present, err := iter.Peek()
	if err != nil {
		return nil, false, err
	}

	if !present {
		return nil, false, nil
	}

	iter.peekCache = nil
	iter.position += 1
	return result, true, nil
}

func (table *SSTable) getEntryAt(offsetIdx int) (*data.Entry, error) {
	var start int
	if offsetIdx == 0 {
		start = 0
	} else {
		start = table.index[offsetIdx-1]
	}

	end := table.index[offsetIdx]
	if _, err := table.file.Seek(int64(start), io.SeekStart); err != nil {
		return nil, err
	}

	bufferSize := end - start
	buffer := make([]byte, bufferSize)
	read, err := table.file.Read(buffer)
	if err != nil {
		return nil, err
	}

	if read < bufferSize {
		return nil, errors.New("Failed getting SSTable entry - read partial data from file")
	}

	return data.DeserializeEntry(string(buffer))
}
