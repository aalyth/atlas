package storage

import (
	"atlas/internal/common"
	"atlas/pkg/utils"
	"crypto/aes"
	"errors"
	"path"
	"slices"
	"strconv"
	"time"
)

type LsmLevelConfig struct {
	maxFileByteSize uint64
}

type LsmConfig struct {
	rootDir     string
	levels      uint
	levelConfig []LsmLevelConfig
}

type Lsm struct {
	levels [][]*SSTable
	config LsmConfig
}

func (lsm *Lsm) MergeWal(wal *Wal) error {
	return nil
}

func (lsm *Lsm) Get(key string) (*common.Entry, bool, error) {
	for _, level := range lsm.levels {
		for _, table := range level {
			if key > table.maxKey {
				continue
			}

			if key < table.minKey {
				break
			}

			entry, contains, err := table.Get(key)
			if err != nil {
				return nil, false, err
			}

			if contains {
				return entry, true, nil
			}
		}
	}
	return nil, false, nil
}

func (lsm *Lsm) mergeFirstLevel(walEntries []*common.Entry) error {
	resultEntries := walEntries
	for _, table := range lsm.levels[0] {
		entries, err := getFileEntries(table.file, table.index)
		if err != nil {
			return err
		}

		resultEntries = append(resultEntries, entries...)
	}

	resultEntries = deduplicateAndFilterEntries(resultEntries)
	slices.SortFunc(resultEntries, common.CompareEntries)

	table, err := NewSSTable(lsm.getNewSSTableFilename(0), resultEntries)
	if err != nil {
		return err
	}

	// TODO:
	// [ ] old table cleanup
	// [ ] split the new level on smaller SSTables
	lsm.levels[0] = []*SSTable{table}
	return nil
}

func (lsm *Lsm) getNewSSTableFilename(tableLevel int64) string {
	timestamp := time.Now().UnixMilli()
	return path.Join(
		lsm.config.rootDir,
		strconv.FormatInt(tableLevel, 10),
		strconv.FormatInt(timestamp, 10),
	)
}

func deduplicateAndFilterEntries(entries []*common.Entry) []*common.Entry {
	if len(entries) == 0 {
		return entries
	}

	latestEntries := make(map[string]*common.Entry)
	for _, entry := range entries {
		key := entry.Key()
		latestEntry, exists := latestEntries[key]
		if !exists {
			latestEntries[key] = entry
		}

		if entry.Timestamp() > latestEntry.Timestamp() {
			latestEntries[key] = entry
		}
	}

	var result []*common.Entry
	for _, entry := range latestEntries {
		if !entry.IsDead() {
			result = append(result, entry)
		}
	}

	return result
}

// func getNextMinValue(walEntries []*common.Entry, iterators []*SSTableIterator) (*common.Entry, error) {}

func (config *LsmConfig) verify() error {
	if len(config.levelConfig) != int(config.levels) {
		return errors.New("Invalid LSM config - level configurations must match the number of levels")
	}
	if config.levels == 0 {
		return errors.New("Invalid LSM config - LSM trees need at least 1 level")
	}
	return nil
}
