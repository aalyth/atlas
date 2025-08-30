package engine

import (
	"atlas/internal/common"
	"atlas/internal/storage"
)

type Atlas struct {
	currentLog storage.Wal
	lsm        storage.Lsm

	cache map[string]common.Entry

	dirs struct {
		root string
		log  string
		lsm  string
	}
}
