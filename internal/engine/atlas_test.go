package engine

import (
	"atlas/internal/storage"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"
)

func newTestAtlasConfig(t *testing.T) AtlasConfig {
	t.Helper()
	root := t.TempDir()
	return AtlasConfig{
		Lsm: storage.LsmConfig{
			Dir:    filepath.Join(root, "lsm"),
			Levels: []storage.LsmLevelConfig{{MaxFileSize: 4096}},
		},
		Wal: storage.WalConfig{
			Dir:     filepath.Join(root, "wal"),
			MaxLogs: -1,
		},
	}
}

func TestGivenInsertedKey_whenGet_thenReturnsValue(t *testing.T) {
	atlas, err := NewAtlas(zaptest.NewLogger(t), newTestAtlasConfig(t))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}

	if err := atlas.Insert("k", "v"); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	entry, ok, err := atlas.Get("k")
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if v, _ := entry.Value(); v != "v" {
		t.Errorf("got %q want %q", v, "v")
	}
}

func TestGivenDeletedKey_whenGet_thenReturnsMiss(t *testing.T) {
	atlas, err := NewAtlas(zaptest.NewLogger(t), newTestAtlasConfig(t))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}

	_ = atlas.Insert("k", "v")
	time.Sleep(2 * time.Millisecond)
	_ = atlas.Delete("k")

	if _, ok, _ := atlas.Get("k"); ok {
		t.Error("expected miss after delete, got hit")
	}
}

func TestGivenWrittenKeys_whenRebooted_thenCacheReplaysFromWal(t *testing.T) {
	cfg := newTestAtlasConfig(t)
	log := zaptest.NewLogger(t)

	atlas, err := NewAtlas(log, cfg)
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}

	if err := atlas.Insert("k", "v"); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := atlas.wal.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	time.Sleep(2 * time.Millisecond) // avoid wal filename collision

	reopened, err := NewAtlas(log, cfg)
	if err != nil {
		t.Fatalf("re-open NewAtlas: %v", err)
	}

	entry, ok, err := reopened.Get("k")
	if err != nil || !ok {
		t.Fatalf("expected replay hit, ok=%v err=%v", ok, err)
	}
	if v, _ := entry.Value(); v != "v" {
		t.Errorf("got %q want %q after replay", v, "v")
	}
}

func TestGivenInvalidKey_whenInsert_thenReturnsValidationError(t *testing.T) {
	atlas, err := NewAtlas(zaptest.NewLogger(t), newTestAtlasConfig(t))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}

	if err := atlas.Insert("", "v"); err == nil {
		t.Error("expected error for empty key, got nil")
	}
}
