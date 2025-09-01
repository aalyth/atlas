package engine

import (
	"atlas/internal/common"
	"atlas/internal/storage"
)

type Atlas struct {
	log *storage.Wal
	lsm *storage.Lsm

	cache map[string]*common.Entry

	logDir      string
	maxLogCount uint
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
	err := atlas.log.Append(entry)
	if err != nil {
		return err
	}

	atlas.cache[entry.Key()] = entry
	return nil
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
