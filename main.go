package main

import (
	"atlas/internal/engine"
	"atlas/internal/storage"
	"fmt"
	"log"
)

const (
	_         = iota
	kb uint64 = 1 << (10 * iota)
	mb
	gb
)

func main() {
	_, err := engine.NewAtlas(engine.AtlasConfig{
		Lsm: storage.LsmConfig{
			Dir: "~/atlas/lsm",
			Levels: []storage.LsmLevelConfig{
				{MaxFileSize: 10 * kb},
				{MaxFileSize: 100 * kb},
				{MaxFileSize: 1 * mb},
				{MaxFileSize: 10 * mb},
				{MaxFileSize: 100 * mb},
			},
		},
		Wal: storage.WalConfig{
			Dir:     "~/atlas/wal",
			MaxLogs: -1,
		},
	})

	if err != nil {
		log.Fatal("Failed booting up Atlas engine: %v", err)
	}

	fmt.Printf("Hello world\n")
}
