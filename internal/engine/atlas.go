package engine

import (
	"atlas/internal/data"
	"atlas/internal/storage"
	"fmt"
	"os"
	"path"
	"time"

	"go.uber.org/zap"
)

type AtlasConfig struct {
	Lsm storage.LsmConfig
	Wal storage.WalConfig
}

type Atlas struct {
	wal *storage.Wal
	lsm *storage.Lsm

	cache  map[string]*data.Entry
	config AtlasConfig
	log    *zap.Logger
}

func NewAtlas(log *zap.Logger, config AtlasConfig) (*Atlas, error) {
	if err := os.MkdirAll(config.Wal.Dir, 0755); err != nil {
		log.Error("create WAL dir failed",
			zap.String("dir", config.Wal.Dir), zap.Error(err))
		return nil, err
	}

	if err := os.MkdirAll(config.Lsm.Dir, 0755); err != nil {
		log.Error("create LSM dir failed",
			zap.String("dir", config.Lsm.Dir), zap.Error(err))
		return nil, err
	}

	walLog := log.With(zap.String("component", "wal"))

	cache, err := storage.RestoreEntries(walLog, config.Wal.Dir)
	if err != nil {
		return nil, err
	}

	walFilename := buildWalFilename(config)
	wal, err := storage.NewWal(walLog, walFilename)
	if err != nil {
		return nil, err
	}

	lsm, err := storage.InitializeLsm(config.Lsm)
	if err != nil {
		return nil, err
	}

	if cache == nil {
		cache = make(map[string]*data.Entry)
	}

	return &Atlas{
		wal:    wal,
		lsm:    lsm,
		cache:  cache,
		config: config,
		log:    log,
	}, nil
}

func (atlas *Atlas) Get(key string) (*data.Entry, bool, error) {
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
	entry, err := data.NewEntry(key, value)
	if err != nil {
		return err
	}
	return atlas.updateEntry(entry)
}

func (atlas *Atlas) Delete(key string) error {
	entry, err := data.NewEmptyEntry(key)
	if err != nil {
		return err
	}
	return atlas.updateEntry(entry)
}

func (atlas *Atlas) updateEntry(entry *data.Entry) error {
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

func filterResponse(entry *data.Entry, err error) (*data.Entry, bool, error) {
	if err != nil {
		return nil, false, err
	}

	if entry == nil || entry.IsDead() {
		return nil, false, nil
	}
	return entry, true, nil
}
