package wal

import (
	"bytes"
	"testing"
	"time"
)

func TestRoundTrip_givenRecordWithData_whenSerde(t *testing.T) {
	testEntrySerde(t, &Entry{
		key:       "test",
		data:      []byte("mock data"),
		timestamp: uint64(time.Now().UnixMilli()),
	})
}

func TestRoundTrip_givenRecordWithoutData_whenSerde(t *testing.T) {
	testEntrySerde(t, &Entry{
		key:       "test",
		data:      nil,
		timestamp: uint64(time.Now().UnixMilli()),
	})
}

func testEntrySerde(t *testing.T, entry *Entry) {
	received, err := deserializeEntry(entry.serialize())
	if err != nil {
		t.Fatalf("failed deserializing entry: %v", err)
	}
	assertEntriesEqual(t, entry, received)
}

func assertEntriesEqual(t *testing.T, left, right *Entry) {
	if left.key != right.key ||
		!bytes.Equal(left.data, right.data) ||
		left.timestamp != right.timestamp {
		t.Fatalf("mismatch after entry round trip - left: %#v; right: %#v", left, right)
	}
}
