package storage

import (
	"atlas/internal/data"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"
)

func TestGivenAppendedEntries_whenRestoreEntries_thenLatestPerKeyReturned(t *testing.T) {
	log := zaptest.NewLogger(t)
	dir := t.TempDir()

	wal, err := NewWal(log, filepath.Join(dir, "1.wal"))
	if err != nil {
		t.Fatalf("NewWal: %v", err)
	}

	first, _ := data.NewEntry("k", "v1")
	if err := wal.Append(first); err != nil {
		t.Fatalf("append v1: %v", err)
	}

	time.Sleep(2 * time.Millisecond)

	second, _ := data.NewEntry("k", "v2")
	if err := wal.Append(second); err != nil {
		t.Fatalf("append v2: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	lookup, err := RestoreEntries(log, dir)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}

	got, ok := lookup["k"]
	if !ok {
		t.Fatal("key 'k' missing from lookup")
	}
	if v, _ := got.Value(); v != "v2" {
		t.Errorf("expected v2 (latest), got %q", v)
	}
}

func TestGivenMultipleWalFiles_whenRestoreEntries_thenMergesAcrossFiles(t *testing.T) {
	log := zaptest.NewLogger(t)
	dir := t.TempDir()

	first, _ := NewWal(log, filepath.Join(dir, "1.wal"))
	a, _ := data.NewEntry("a", "1")
	_ = first.Append(a)
	_ = first.Close()

	time.Sleep(2 * time.Millisecond)

	second, _ := NewWal(log, filepath.Join(dir, "2.wal"))
	b, _ := data.NewEntry("b", "2")
	aNew, _ := data.NewEntry("a", "1-updated")
	_ = second.Append(b)
	_ = second.Append(aNew)
	_ = second.Close()

	lookup, err := RestoreEntries(log, dir)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}

	if v, _ := lookup["a"].Value(); v != "1-updated" {
		t.Errorf("a: expected latest value, got %q", v)
	}
	if v, _ := lookup["b"].Value(); v != "2" {
		t.Errorf("b: expected 2, got %q", v)
	}
}

func TestGivenMissingDir_whenRestoreEntries_thenReturnsEmptyLookup(t *testing.T) {
	log := zaptest.NewLogger(t)
	lookup, err := RestoreEntries(log, filepath.Join(t.TempDir(), "nope"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(lookup) != 0 {
		t.Errorf("expected empty lookup, got %d entries", len(lookup))
	}
}
