package engine

import (
	"atlas/internal/common"
	"atlas/internal/storage"
	"atlas/pkg/logger"
	"fmt"
	"os"
	"path"
	"time"
)

type AtlasConfig struct {
	Lsm storage.LsmConfig
	Wal storage.WalConfig
}

type Atlas struct {
	wal *storage.Wal
	lsm *storage.Lsm

	cache  map[string]*common.Entry
	config AtlasConfig
}

func NewAtlas(config AtlasConfig) (*Atlas, error) {
	if err := os.MkdirAll(config.Wal.Dir, 0755); err != nil {
		logger.Error("Failed creating WAL directory: %v", err)
		return nil, err
	}

	if err := os.MkdirAll(config.Lsm.Dir, 0755); err != nil {
		logger.Error("Failed creating LSM directory: %v", err)
		return nil, err
	}

	walFilename := buildWalFilename(config)
	wal, err := storage.CreateWal(walFilename)
	if err != nil {
		return nil, err
	}

	lsm, err := storage.InitializeLsm(config.Lsm)
	if err != nil {
		return nil, err
	}

	return &Atlas{
		wal:    wal,
		lsm:    lsm,
		cache:  make(map[string]*common.Entry),
		config: config,
	}, nil
}

func (atlas *Atlas) Get(key string) (*common.Entry, bool, error) {
	entry, cached := atlas.cache[key]
	if cached {
		return filterResponse(entry, nil)
	}

	entry, contained, err := atlas.lsm.Get(key)
	if err != nil {
		return nil, false, err
	}

	if contained {
		atlas.cache[key] = entry
		return filterResponse(entry, nil)
	}
	return nil, false, nil
}

func (atlas *Atlas) Insert(key, value string) error {
	return atlas.updateEntry(common.NewEntry(key, value))
}

func (atlas *Atlas) Delete(key string) error {
	return atlas.updateEntry(common.NewEmptyEntry(key))
}

func (atlas *Atlas) updateEntry(entry *common.Entry) error {
	err := atlas.wal.Append(entry)
	if err != nil {
		return err
	}

	atlas.cache[entry.Key()] = entry
	return nil
}

func buildWalFilename(config AtlasConfig) string {
	timestamp := time.Now().UnixMilli()
	filename := fmt.Sprintf("%d.wal", timestamp)
	return path.Join(config.Wal.Dir, filename)
}

func filterResponse(entry *common.Entry, err error) (*common.Entry, bool, error) {
	if err != nil {
		return nil, false, err
	}

	if entry == nil || entry.IsDead() {
		return nil, false, nil
	}
	return entry, true, nil
}
