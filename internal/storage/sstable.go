package storage

import (
	"atlas/internal/common"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
)

// Sorted String Table
type SSTable struct {
	file     *os.File
	filename string
	index    []int64
	minKey   string
	maxKey   string
}

type SSTableBuilder struct {
	file     *os.File
	filename string
	offset   int64
	index    []int64
	minKey   string
	maxKey   string
}

type SSTableIterator struct {
	table     *SSTable
	peekCache *common.Entry
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

func (builder *SSTableBuilder) AddSorted(entry *common.Entry) error {
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

	builder.offset += int64(written)
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

func NewSSTable(filename string, entries []*common.Entry) (*SSTable, error) {
	if len(entries) == 0 {
		return nil, errors.New("Failed craeting SSTable - at least 1 entry is required")
	}

	if !slices.IsSortedFunc(entries, common.CompareEntries) {
		slices.SortFunc(entries, common.CompareEntries)
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	var offset int64 = 0
	var index []int64 = nil
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

		offset += int64(written)
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

func (table *SSTable) Get(key string) (*common.Entry, bool, error) {
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

func (table *SSTable) Entries() ([]*common.Entry, error) {
	if len(table.index) == 0 {
		return []*common.Entry{}, nil
	}

	offsetIndex := append([]int64{0}, table.index...)

	var result []*common.Entry = nil
	for i, offset := range offsetIndex[1:] {
		prevOffset := offsetIndex[i]

		bufferSize := offset - prevOffset
		buffer := make([]byte, bufferSize)
		if _, err := table.file.Read(buffer); err != nil {
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

func (iter *SSTableIterator) Peek() (*common.Entry, bool, error) {
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

func (iter *SSTableIterator) Advance() (*common.Entry, bool, error) {
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

func (table *SSTable) getEntryAt(offsetIdx int) (*common.Entry, error) {
	var start int64
	if offsetIdx == 0 {
		start = 0
	} else {
		start = table.index[offsetIdx-1]
	}

	end := table.index[offsetIdx]
	if _, err := table.file.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}

	bufferSize := end - start
	buffer := make([]byte, bufferSize)
	read, err := table.file.Read(buffer)
	if err != nil {
		return nil, err
	}

	if int64(read) < bufferSize {
		return nil, errors.New("Failed getting SSTable entry - read partial data from file")
	}

	return common.DeserializeEntry(string(buffer))
}
