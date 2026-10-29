package main

import (
	"atlas/internal/engine"
	"atlas/internal/storage"
	"log"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

const (
	_         = iota
	kb uint64 = 1 << (10 * iota)
	mb
	gb
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed building logger: %v", err)
	}
	defer logger.Sync()

	dataDir := os.Getenv("ATLAS_ROOT")
	if dataDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			logger.Fatal("resolve home dir failed", zap.Error(err))
		}
		dataDir = filepath.Join(home, "atlas")
	}

	atlasCfg := engine.AtlasConfig{
		Lsm: storage.LsmConfig{
			Dir: filepath.Join(dataDir, "lsm"),
			Levels: []storage.LsmLevelConfig{
				{MaxFileSize: 10 * kb},
				{MaxFileSize: 100 * kb},
				{MaxFileSize: 1 * mb},
				{MaxFileSize: 10 * mb},
				{MaxFileSize: 100 * mb},
			},
		},
		Wal: storage.WalConfig{
			Dir:     filepath.Join(dataDir, "wal"),
			MaxLogs: -1,
		},
	}

	serverCfg := engine.AtlasServerConfig{
		Engine: atlasCfg,
		Port:   8080,
	}

	server, err := engine.NewAtlasServer(logger, serverCfg)
	if err != nil {
		logger.Fatal("boot Atlas engine failed", zap.Error(err))
	}

	server.Start()
}
