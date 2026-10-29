package data

import (
	"errors"
	"strings"
	"testing"
)

func TestGivenAliveEntry_whenRoundTripped_thenAllFieldsPreserved(t *testing.T) {
	entry, err := NewEntry("user:42", "alice")
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}

	restored, err := DeserializeEntry(entry.Serialize())
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}

	assertEntryEqual(t, restored, entry)
}

func TestGivenValueWithPipes_whenRoundTripped_thenValuePreserved(t *testing.T) {
	entry, err := NewEntry("k", "a|b|c")
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}

	restored, err := DeserializeEntry(entry.Serialize())
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}

	assertEntryEqual(t, restored, entry)
}

func TestGivenKeyWithPipes_whenRoundTripped_thenKeyPreserved(t *testing.T) {
	entry, err := NewEntry("ns|user|42", "v")
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}

	restored, err := DeserializeEntry(entry.Serialize())
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}

	assertEntryEqual(t, restored, entry)
}

func TestGivenTombstone_whenRoundTripped_thenStaysDeadWithEmptyValue(t *testing.T) {
	entry, err := NewEmptyEntry("user:42")
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}

	restored, err := DeserializeEntry(entry.Serialize())
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}

	if !restored.IsDead() {
		t.Fatal("expected tombstone, got alive entry")
	}
	if v, alive := restored.Value(); alive || v != "" {
		t.Errorf("expected dead value, got %q alive=%v", v, alive)
	}
}

func TestGivenEmptyKey_whenNewEntry_thenReturnsErrEmptyKey(t *testing.T) {
	_, err := NewEntry("", "v")
	if !errors.Is(err, ErrEmptyKey) {
		t.Errorf("got %v, want ErrEmptyKey", err)
	}
}

func TestGivenOversizedKey_whenNewEntry_thenReturnsErrKeyTooLarge(t *testing.T) {
	_, err := NewEntry(strings.Repeat("k", MaxKeySize+1), "v")
	if !errors.Is(err, ErrKeyTooLarge) {
		t.Errorf("got %v, want ErrKeyTooLarge", err)
	}
}

func TestGivenOversizedValue_whenNewEntry_thenReturnsErrValueTooLarge(t *testing.T) {
	_, err := NewEntry("k", strings.Repeat("v", MaxValueSize+1))
	if !errors.Is(err, ErrValueTooLarge) {
		t.Errorf("got %v, want ErrValueTooLarge", err)
	}
}

func TestGivenNewlineInKey_whenNewEntry_thenReturnsErrInvalidKey(t *testing.T) {
	_, err := NewEntry("bad\nkey", "v")
	if !errors.Is(err, ErrInvalidKey) {
		t.Errorf("got %v, want ErrInvalidKey", err)
	}
}

func TestGivenNewlineInValue_whenNewEntry_thenReturnsErrInvalidValue(t *testing.T) {
	_, err := NewEntry("k", "bad\nval")
	if !errors.Is(err, ErrInvalidValue) {
		t.Errorf("got %v, want ErrInvalidValue", err)
	}
}

func TestGivenMalformedSerializedInput_whenDeserialized_thenReturnsError(t *testing.T) {
	cases := map[string]string{
		"empty":           "",
		"missingFields":   "123|1|",
		"badTimestamp":    "abc|1|3|key",
		"badLiveness":     "123|x|3|keyval",
		"keyLengthTooBig": "123|1|99|short",
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DeserializeEntry(input); err == nil {
				t.Errorf("expected error for %q, got nil", input)
			}
		})
	}
}

func assertEntryEqual(t *testing.T, got, want *Entry) {
	t.Helper()
	if got.Key() != want.Key() {
		t.Errorf("key mismatch: got %q want %q", got.Key(), want.Key())
	}
	if got.IsDead() != want.IsDead() {
		t.Errorf("liveness mismatch: got %v want %v", got.IsDead(), want.IsDead())
	}
	if got.Timestamp() != want.Timestamp() {
		t.Errorf("timestamp mismatch: got %d want %d", got.Timestamp(), want.Timestamp())
	}
	gotVal, _ := got.Value()
	wantVal, _ := want.Value()
	if gotVal != wantVal {
		t.Errorf("value mismatch: got %q want %q", gotVal, wantVal)
	}
}
