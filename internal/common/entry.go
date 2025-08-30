package common

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Entry struct {
	key       string
	value     string
	isDead    bool
	timestamp int64
}

const keyValueDelimiter = "|"

func NewEntry(key, value string) *Entry {
	return &Entry{
		key:       key,
		value:     value,
		isDead:    false,
		timestamp: time.Now().UnixMilli(),
	}
}

func NewEmptyEntry(key string) *Entry {
	return &Entry{
		key:       key,
		value:     "",
		isDead:    true,
		timestamp: time.Now().UnixMilli(),
	}
}

func (entry *Entry) Key() string {
	return entry.key
}

func (entry *Entry) Value() (string, bool) {
	if entry.isDead {
		return "", false
	}
	return entry.value, true
}

func (entry *Entry) Kill() {
	entry.isDead = true
	entry.value = ""
}

func (entry *Entry) Serialize() string {
	if entry.isDead {
		return fmt.Sprintf("%s\n", entry.key)
	}
	return fmt.Sprintf("%s%s%s\n",
		entry.key, keyValueDelimiter, entry.value,
	)
}

func DeserializeEntry(serialized string) (*Entry, error) {
	split := strings.Split(serialized, keyValueDelimiter)
	if len(split) == 0 || len(split) > 2 {
		return nil, errors.New("Failed deseiralizing entry - invalid format")
	}

	isDead := len(split) == 1

	var value string
	if isDead {
		value = ""
	} else {
		value = split[1]
	}

	return &Entry{
		key:       split[0],
		value:     value,
		isDead:    isDead,
		timestamp: time.Now().UnixMilli(),
	}, nil
}
