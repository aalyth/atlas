package storage

import (
	"atlas/internal/data"
	"path/filepath"
	"testing"
)

func mustEntry(t *testing.T, k, v string) *data.Entry {
	t.Helper()
	e, err := data.NewEntry(k, v)
	if err != nil {
		t.Fatalf("NewEntry(%q,%q): %v", k, v, err)
	}
	return e
}

func TestGivenUnsortedEntries_whenNewSSTable_thenEntriesReturnedSorted(t *testing.T) {
	entries := []*data.Entry{
		mustEntry(t, "c", "3"),
		mustEntry(t, "a", "1"),
		mustEntry(t, "b", "2"),
	}

	table, err := NewSSTable(filepath.Join(t.TempDir(), "t.sstable"), entries)
	if err != nil {
		t.Fatalf("NewSSTable: %v", err)
	}

	got, err := table.Entries()
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}

	wantKeys := []string{"a", "b", "c"}
	if len(got) != len(wantKeys) {
		t.Fatalf("entries len: got %d want %d", len(got), len(wantKeys))
	}
	for i, key := range wantKeys {
		if got[i].Key() != key {
			t.Errorf("position %d: got %q want %q", i, got[i].Key(), key)
		}
	}
}

func TestGivenExistingKey_whenGet_thenReturnsEntry(t *testing.T) {
	table, err := NewSSTable(filepath.Join(t.TempDir(), "t.sstable"), []*data.Entry{
		mustEntry(t, "a", "1"),
		mustEntry(t, "b", "2"),
		mustEntry(t, "c", "3"),
	})
	if err != nil {
		t.Fatalf("NewSSTable: %v", err)
	}

	entry, ok, err := table.Get("b")
	if err != nil || !ok {
		t.Fatalf("Get b: ok=%v err=%v", ok, err)
	}
	if v, _ := entry.Value(); v != "2" {
		t.Errorf("got %q want %q", v, "2")
	}
}

func TestGivenMissingKey_whenGet_thenReturnsFalse(t *testing.T) {
	table, err := NewSSTable(filepath.Join(t.TempDir(), "t.sstable"), []*data.Entry{
		mustEntry(t, "a", "1"),
		mustEntry(t, "c", "3"),
	})
	if err != nil {
		t.Fatalf("NewSSTable: %v", err)
	}

	if _, ok, _ := table.Get("b"); ok {
		t.Error("expected miss for key not in table range")
	}
	if _, ok, _ := table.Get("z"); ok {
		t.Error("expected miss for key past max")
	}
}

func TestGivenSSTable_whenIteratorAdvances_thenReturnsEntriesInOrder(t *testing.T) {
	table, err := NewSSTable(filepath.Join(t.TempDir(), "t.sstable"), []*data.Entry{
		mustEntry(t, "a", "1"),
		mustEntry(t, "b", "2"),
	})
	if err != nil {
		t.Fatalf("NewSSTable: %v", err)
	}

	iter := table.Iterator()
	first, _, _ := iter.Advance()
	second, _, _ := iter.Advance()
	if first.Key() != "a" || second.Key() != "b" {
		t.Errorf("iterator order wrong: got %q,%q", first.Key(), second.Key())
	}
	if !iter.IsEmpty() {
		t.Error("expected iterator empty after consuming all entries")
	}
}
