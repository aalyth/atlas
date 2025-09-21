package storage

import (
	"atlas/internal/common"
	"errors"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"time"
)

type LsmLevelConfig struct {
	MaxFileSize uint64
	MaxTables   int
}

type LsmConfig struct {
	Dir    string
	Levels []LsmLevelConfig
}

type Lsm struct {
	levels [][]*SSTable
	config LsmConfig
}

var sstableRegex = regexp.MustCompile(`^(\d+)\.sstable$`)

func InitializeLsm(config LsmConfig) (*Lsm, error) {
	if err := config.verify(); err != nil {
		return nil, err
	}

	stat, err := os.Stat(config.Dir)
	if os.IsNotExist(err) {
		return createNewLsm(config)
	}

	if !stat.IsDir() {
		return nil, errors.New("Invalid LSM config - root path is not directory")
	}

	return restoreLsm(config)
}

func createNewLsm(config LsmConfig) (*Lsm, error) {
	var levels [][]*SSTable
	for idx := range config.Levels {
		dirName := strconv.FormatInt(int64(idx), 10)
		if err := os.Mkdir(dirName, 0755); err != nil {
			return nil, err
		}

		levels = append(levels, nil)
	}
	return &Lsm{
		levels: levels,
		config: config,
	}, nil
}

func restoreLsm(config LsmConfig) (*Lsm, error) {
	levels := make([][]*SSTable, len(config.Levels))
	for levelIdx := range config.Levels {
		levelDir := filepath.Join(config.Dir, strconv.Itoa(levelIdx))

		sstables, err := restoreSSTablesFromDirectory(levelDir)
		if err != nil {
			return nil, err
		}

		levels[levelIdx] = sstables
	}

	return &Lsm{
		levels: levels,
		config: config,
	}, nil
}

func restoreSSTablesFromDirectory(dir string) ([]*SSTable, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return []*SSTable{}, nil
	}

	dirFiles, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var sstables []*SSTable
	for _, entry := range dirFiles {
		if entry.IsDir() {
			continue
		}

		matches := sstableRegex.FindStringSubmatch(entry.Name())
		if len(matches) != 2 {
			continue
		}

		filePath := filepath.Join(dir, entry.Name())
		sstable, err := RestoreSSTable(filePath)
		if err != nil {
			return nil, err
		}

		sstables = append(sstables, sstable)
	}
	return sstables, nil
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
	firstLevelMaxSize := lsm.config.Levels[0].MaxFileSize
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
		lsm.config.Dir,
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

func (config *LsmConfig) verify() error {
	if len(config.Levels) == 0 {
		return errors.New("Invalid LSM config - LSM trees need at least 1 level")
	}
	return nil
}
