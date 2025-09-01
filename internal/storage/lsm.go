package storage

import (
	"atlas/internal/common"
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

func (lsm *Lsm) Merge(wal *Wal) error {
	entries, err := wal.CloseAndGetEntries()
	if err != nil {
		return err
	}

	err = lsm.mergeFirstLevel(entries)
	if err != nil {
		return err
	}

	// TODO: subsequent higher level merging
	return nil
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

	var entryBuckets [][]*common.Entry
	var currentBucket []*common.Entry
	var currentBucketSize uint64 = 0
	firstLevelMaxSize := lsm.config.levelConfig[0].maxFileByteSize
	for _, entry := range resultEntries {
		currentBucketSize += uint64(len(entry.Serialize()))
		currentBucket = append(currentBucket, entry)
		if currentBucketSize >= firstLevelMaxSize {
			entryBuckets = append(entryBuckets, currentBucket)
			currentBucket = nil
			currentBucketSize = 0
		}
	}

	if len(currentBucket) > 0 {
		entryBuckets = append(entryBuckets, currentBucket)
	}

	tables := make([]*SSTable, len(entryBuckets))
	for i, entryBucket := range entryBuckets {
		var err error
		tables[i], err = NewSSTable(lsm.getNewSSTableFilename(0), entryBucket)
		if err != nil {
			return err
		}
	}

	// TODO:
	// [ ] old table cleanup
	lsm.levels[0] = tables
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
