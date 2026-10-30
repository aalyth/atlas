package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestWalRestore_givenValidFile(t *testing.T) {
	wal := newTestWal(t, 0)
	walModifyAndClose(t, wal, []func(*Wal){
		func(w *Wal) { w.Put("1", []byte{}) },
		func(w *Wal) { w.Put("2", []byte("some data")) },
		func(w *Wal) { w.Del("1") },
	})
	restoreAndVerifyWal(t, wal.file.Name(), []Entry{
		{op: OpPut, key: "1", value: []byte{}},
		{op: OpPut, key: "2", value: []byte("some data")},
		{op: OpDel, key: "1", value: nil},
	})
}

func TestWalAppend_givenValidFile(t *testing.T) {
	lsn := uint64(7)
	initOps := []func(*Wal){
		func(w *Wal) { w.Put("1", []byte("some data")) },
		func(w *Wal) { w.Del("1") },
	}
	modifyOps := []func(*Wal){
		func(w *Wal) { w.Put("2", []byte("more data")) },
	}
	expected := []Entry{
		{op: OpPut, key: "1", value: []byte("some data")},
		{op: OpDel, key: "1"},
		{op: OpPut, key: "2", value: []byte("more data")},
	}

	wal := initRestoreModifyVerifyWal(t, newTestWal(t, lsn), initOps, modifyOps, expected)
	if expLsn := lsn + 3; wal.lsn != expLsn {
		t.Fatalf("bad wal lsn: %d/%d", wal.lsn, expLsn)
	}
}

func TestWalRestore_givenTornFile(t *testing.T) {
	init := []func(*Wal){
		func(w *Wal) { w.Put("1", []byte("some data")) },
		func(w *Wal) { w.Put("2", []byte("some data")) },
	}
	modify := []func(*Wal){
		func(w *Wal) { w.Put("new", []byte("some data")) },
	}
	expected := []Entry{
		{op: OpPut, key: "1", value: []byte("some data")},
		{op: OpPut, key: "new", value: []byte("some data")},
	}

	last := Entry{op: OpPut, key: "2", value: []byte("some data")}
	size := recordHeaderSize + len(last.serialize())
	for cut := 1; cut < size; cut++ {
		t.Run(fmt.Sprintf("cut=%d", cut), func(t *testing.T) {
			tear := func(w *Wal) { w.file.Truncate(int64(2*size - cut)) }
			ops := append(slices.Clone(init), tear)
			initRestoreModifyVerifyWal(t, newTestWal(t, 0), ops, modify, expected)
		})
	}
}

func TestWalRestore_givenCorruptedFile(t *testing.T) {
	init := []func(*Wal){
		func(w *Wal) { w.Put("1", []byte("some data")) },
		func(w *Wal) { w.Put("2", []byte("some data")) },
		func(w *Wal) { w.Put("3", []byte("some data")) },
	}
	modify := []func(*Wal){
		func(w *Wal) { w.Put("new", []byte("some data")) },
	}
	entries := []Entry{
		{op: OpPut, key: "1", value: []byte("some data")},
		{op: OpPut, key: "2", value: []byte("some data")},
		{op: OpPut, key: "3", value: []byte("some data")},
	}
	added := Entry{op: OpPut, key: "new", value: []byte("some data")}

	size := recordHeaderSize + len(entries[0].serialize())
	offsets := map[string]int{"crc": 0, "lsn": 4, "len": 12, "entry": recordHeaderSize}
	for name, offset := range offsets {
		for idx := 1; idx < len(entries); idx++ {
			t.Run(fmt.Sprintf("%s/record=%d", name, idx), func(t *testing.T) {
				flip := func(w *Wal) { flipByte(w, int64(idx*size+offset)) }
				ops := append(slices.Clone(init), flip)
				expected := append(slices.Clone(entries[:idx]), added)
				wal := newTestWal(t, 0)
				initRestoreModifyVerifyWal(t, wal, ops, modify, expected)
			})
		}
	}
}

func TestWalRestore_givenAppendedGarbage(t *testing.T) {
	init := []func(*Wal){
		func(w *Wal) { w.Put("1", []byte("some data")) },
		func(w *Wal) { w.Put("2", []byte("some data")) },
	}
	modify := []func(*Wal){
		func(w *Wal) { w.Put("new", []byte("some data")) },
	}
	expected := []Entry{
		{op: OpPut, key: "1", value: []byte("some data")},
		{op: OpPut, key: "2", value: []byte("some data")},
		{op: OpPut, key: "new", value: []byte("some data")},
	}

	hugeLen := make([]byte, recordHeaderSize, recordHeaderSize+4)
	binary.LittleEndian.PutUint32(hugeLen[12:], 0x7fffffff)
	hugeLen = append(hugeLen, 1, 2, 3, 4)

	badCrc, err := newRecord(99, Entry{op: OpPut, key: "4", value: []byte("some data")})
	if err != nil {
		t.Fatalf("cannot create record: %v", err)
	}
	badCrc.crc ^= 0xffffffff

	garbage := map[string][]byte{
		"short":       {0xde, 0xad},
		"zeroes":      make([]byte, 32),
		"header only": make([]byte, recordHeaderSize),
		"huge length": hugeLen,
		"bad crc":     badCrc.serialize(),
	}

	for name, tail := range garbage {
		t.Run(name, func(t *testing.T) {
			write := func(w *Wal) { w.file.Write(tail) }
			ops := append(slices.Clone(init), write)
			initRestoreModifyVerifyWal(t, newTestWal(t, 0), ops, modify, expected)
		})
	}
}

func FuzzDeserializeRecord(f *testing.F) {
	rec, err := newRecord(12, Entry{op: OpPut, key: "test", value: []byte("mock data")})
	if err != nil {
		f.Fatalf("cannot create record: %v", err)
	}
	f.Add(rec.serialize())

	f.Fuzz(func(t *testing.T, data []byte) {
		deserializeRecord(bytes.NewReader(data))
	})
}

func FuzzDeserializeEntry(f *testing.F) {
	put := Entry{op: OpPut, key: "test", value: []byte("mock data"), timestamp: 1}
	del := Entry{op: OpDel, key: "test", timestamp: 2}
	f.Add(put.serialize())
	f.Add(del.serialize())
	f.Add([]byte{})
	f.Add(append(make([]byte, 9), 200, 1))

	f.Fuzz(func(t *testing.T, data []byte) {
		original, err := deserializeEntry(data)
		if err != nil {
			return
		}
		if original.op == OpDel {
			original.value = nil
		}
		testEntrySerde(t, original)
	})
}

func TestRoundTrip_givenRecord_whenSerde(t *testing.T) {
	original, err := newRecord(12, Entry{
		key:       "test",
		value:     []byte("mock data"),
		timestamp: uint64(time.Now().UnixMilli()),
	})
	if err != nil {
		t.Fatalf("cannot create record: %v", err)
	}

	received, _, err := deserializeRecord(bytes.NewReader(original.serialize()))
	if err != nil {
		t.Fatalf("cannot deserialize record: %v", err)
	}

	if !recordsEqual(original, received) {
		t.Fatalf("mismatching records: original: %#v; received: %#v", original, received)
	}
}

func TestRoundTrip_givenEntryWithValue_whenSerde(t *testing.T) {
	testEntrySerde(t, &Entry{
		key:   "test",
		value: []byte("mock data"),
	})
}

func TestRoundTrip_givenEntryWithoutValue_whenSerde(t *testing.T) {
	testEntrySerde(t, &Entry{
		key:   "test",
		value: nil,
	})
}

func TestNoValue_givenDeletedEntry_whenSerialized(t *testing.T) {
	original := &Entry{
		op:    OpDel,
		value: []byte("not nil"),
	}
	received, err := deserializeEntry(original.serialize())
	if err != nil {
		t.Fatalf("bad entry deserialization: %v", err)
	}
	if len(received.value) != 0 {
		t.Fatalf("read value must be 0-sized")
	}
}

func newTestWal(t *testing.T, lsn uint64) *Wal {
	t.Helper()
	wal, err := CreateWal(filepath.Join(t.TempDir(), "wal"), lsn)
	if err != nil {
		t.Fatalf("create wal: %v", err)
	}
	return wal
}

func walModifyAndClose(t *testing.T, wal *Wal, ops []func(*Wal)) {
	t.Helper()
	for _, op := range ops {
		op(wal)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("wal close: %v", err)
	}
}

func restoreAndVerifyWal(t *testing.T, path string, expected []Entry) *Wal {
	t.Helper()
	var received []Entry
	wal, err := RestoreWal(path, 0, func(e *Entry) { received = append(received, *e) })
	if err != nil {
		t.Fatalf("wal restore: %v", err)
	}
	t.Cleanup(func() { wal.Close() })

	if len(received) != len(expected) {
		t.Fatalf("restored entry count mismatch: %d/%d", len(received), len(expected))
	}
	for i, exp := range expected {
		rcv := received[i]
		if rcv.op != exp.op || rcv.key != exp.key || !bytes.Equal(rcv.value, exp.value) {
			t.Fatalf("entry mismatch: expected: %#v; received: %#v", exp, rcv)
		}
	}
	return wal
}

func initRestoreModifyVerifyWal(
	t *testing.T,
	wal *Wal,
	initOps, modifyOps []func(*Wal),
	expected []Entry,
) *Wal {
	t.Helper()
	path := wal.file.Name()
	walModifyAndClose(t, wal, initOps)

	restored, err := RestoreWal(path, 0, func(*Entry) {})
	if err != nil {
		t.Fatalf("wal restore: %v", err)
	}
	walModifyAndClose(t, restored, modifyOps)
	return restoreAndVerifyWal(t, path, expected)
}

func flipByte(w *Wal, offset int64) error {
	b := make([]byte, 1)
	if _, err := w.file.ReadAt(b, offset); err != nil {
		return err
	}
	b[0] ^= 0xff
	_, err := w.file.WriteAt(b, offset)
	return err
}

func testEntrySerde(t *testing.T, original *Entry) {
	received, err := deserializeEntry(original.serialize())
	if err != nil {
		t.Fatalf("bad entry deserialization: %v", err)
	}
	if !entriesEqual(original, received) {
		t.Fatalf("mismatching entries: original: %#v; received: %#v", original, received)
	}
}

func recordsEqual(left, right *record) bool {
	return left.crc == right.crc &&
		left.lsn == right.lsn &&
		left.len == right.len &&
		entriesEqual(&left.entry, &right.entry)
}

func entriesEqual(left, right *Entry) bool {
	return left.op == right.op &&
		left.key == right.key &&
		bytes.Equal(left.value, right.value) &&
		left.timestamp == right.timestamp
}
