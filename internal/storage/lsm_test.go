package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGivenMissingDir_whenInitializeLsm_thenCreatesLevelDirs(t *testing.T) {
	root := filepath.Join(t.TempDir(), "lsm")
	cfg := LsmConfig{
		Dir: root,
		Levels: []LsmLevelConfig{
			{MaxFileSize: 1024},
			{MaxFileSize: 4096},
		},
	}

	lsm, err := InitializeLsm(cfg)
	if err != nil {
		t.Fatalf("InitializeLsm: %v", err)
	}
	if lsm == nil {
		t.Fatal("nil lsm")
	}

	for i := range cfg.Levels {
		path := filepath.Join(root, levelDirName(i))
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("level %d dir missing: %v", i, err)
			continue
		}
		if !info.IsDir() {
			t.Errorf("level %d path is not a directory", i)
		}
	}
}

func TestGivenZeroLevels_whenInitializeLsm_thenReturnsError(t *testing.T) {
	cfg := LsmConfig{Dir: t.TempDir(), Levels: nil}
	if _, err := InitializeLsm(cfg); err == nil {
		t.Error("expected error for empty levels, got nil")
	}
}

func TestGivenExistingLsmDir_whenInitializeLsm_thenRestoresWithoutError(t *testing.T) {
	root := filepath.Join(t.TempDir(), "lsm")
	cfg := LsmConfig{
		Dir:    root,
		Levels: []LsmLevelConfig{{MaxFileSize: 1024}},
	}

	if _, err := InitializeLsm(cfg); err != nil {
		t.Fatalf("first init: %v", err)
	}
	lsm, err := InitializeLsm(cfg)
	if err != nil {
		t.Fatalf("restore init: %v", err)
	}
	if _, ok, _ := lsm.Get("nope"); ok {
		t.Error("unexpected hit on empty LSM")
	}
}

// helper: matches the on-disk level naming used by InitializeLsm
func levelDirName(idx int) string {
	return filepathItoa(idx)
}

func filepathItoa(i int) string {
	// inlined strconv.Itoa to avoid an import just for the helper
	if i == 0 {
		return "0"
	}
	digits := ""
	for i > 0 {
		digits = string(rune('0'+i%10)) + digits
		i /= 10
	}
	return digits
}
