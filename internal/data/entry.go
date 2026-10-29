package data

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Entry struct {
	key       string
	value     string
	isDead    bool
	timestamp int64
}

type EntryLookup map[string]*Entry

const (
	fieldDelimiter = "|"
	// MaxEntrySize bounds a single serialized entry. Storage scanners are
	// sized to read up to this length, so values above it would be silently
	// dropped on restore — we reject at the write boundary instead.
	MaxEntrySize = 1 << 20 // 1 MiB
	// framingOverhead is a conservative upper bound on the bytes Serialize
	// adds around the key/value payload (timestamp, flag, keylen, delimiters,
	// newline). Used to derive the payload budget for validation.
	framingOverhead = 64
)

var (
	ErrEmptyKey            = errors.New("entry key must not be empty")
	ErrEntryTooLarge       = fmt.Errorf("entry exceeds %d bytes", MaxEntrySize)
	ErrInvalidKey          = errors.New("entry key contains forbidden newline")
	ErrInvalidValue        = errors.New("entry value contains forbidden newline")
	ErrMalformedEntry      = errors.New("failed deserializing entry - malformed format")
	ErrInvalidTimestamp    = errors.New("failed deserializing entry - invalid timestamp")
	ErrInvalidLivenessFlag = errors.New("failed deserializing entry - invalid liveness flag")
	ErrInvalidKeyLength    = errors.New("failed deserializing entry - invalid key length")
)

func NewEntry(key, value string) (*Entry, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if err := validateValue(value); err != nil {
		return nil, err
	}
	if len(key)+len(value)+framingOverhead > MaxEntrySize {
		return nil, ErrEntryTooLarge
	}
	return &Entry{
		key:       key,
		value:     value,
		isDead:    false,
		timestamp: time.Now().UnixMilli(),
	}, nil
}

func NewEmptyEntry(key string) (*Entry, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if len(key)+framingOverhead > MaxEntrySize {
		return nil, ErrEntryTooLarge
	}
	return &Entry{
		key:       key,
		value:     "",
		isDead:    true,
		timestamp: time.Now().UnixMilli(),
	}, nil
}

func CompareEntries(e1 *Entry, e2 *Entry) int {
	return strings.Compare(e1.key, e2.key)
}

func DeserializeEntry(serialized string) (*Entry, error) {
	trimmed := strings.TrimRight(serialized, "\n")
	parts := strings.SplitN(trimmed, fieldDelimiter, 4)
	if len(parts) != 4 {
		return nil, ErrMalformedEntry
	}

	timestamp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, ErrInvalidTimestamp
	}

	var isDead bool
	switch parts[1] {
	case "1":
		isDead = false
	case "0":
		isDead = true
	default:
		return nil, ErrInvalidLivenessFlag
	}

	keyLen, err := strconv.Atoi(parts[2])
	if err != nil || keyLen < 0 || keyLen > len(parts[3]) {
		return nil, ErrInvalidKeyLength
	}

	key := parts[3][:keyLen]
	if key == "" {
		return nil, ErrEmptyKey
	}

	value := parts[3][keyLen:]
	if isDead {
		value = ""
	}

	return &Entry{
		key:       key,
		value:     value,
		isDead:    isDead,
		timestamp: timestamp,
	}, nil
}

func (entry *Entry) Key() string      { return entry.key }
func (entry *Entry) Timestamp() int64 { return entry.timestamp }
func (entry *Entry) IsDead() bool     { return entry.isDead }

func (entry *Entry) Value() (string, bool) {
	if entry.isDead {
		return "", false
	}
	return entry.value, true
}

func (entry *Entry) Kill() {
	entry.isDead = true
	entry.value = ""
	entry.timestamp = time.Now().UnixMilli()
}

// Serialize format: "<timestamp>|<liveness>|<keylen>|<key><value>\n".
// The key and value are concatenated; key length is encoded so both fields
// may contain '|'. Newlines remain forbidden — they terminate the record.
func (entry *Entry) Serialize() string {
	liveness := "1"
	if entry.isDead {
		liveness = "0"
	}
	return fmt.Sprintf("%d|%s|%d|%s%s\n",
		entry.timestamp, liveness, len(entry.key), entry.key, entry.value,
	)
}

func validateKey(key string) error {
	if key == "" {
		return ErrEmptyKey
	}
	if strings.ContainsRune(key, '\n') {
		return ErrInvalidKey
	}
	return nil
}

func validateValue(value string) error {
	if strings.ContainsRune(value, '\n') {
		return ErrInvalidValue
	}
	return nil
}
