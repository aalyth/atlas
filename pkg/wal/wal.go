package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

const (
	minusOne uint32 = 0xffffffff

	// 4 bytes klen, 4 bytes dlen, 8 bytes timestamp
	entryMetadataSize int = 4 + 4 + 8
)

type (
	Entry struct {
		key       string
		data      []byte
		timestamp uint64
	}
	Wal struct {
		file *os.File
	}

	// [checksum][LSN][len][key + '\0'][data][timestamp]
	record struct {
		checksum uint32
		lsn      uint32
		len      uint32
		entry    Entry
	}
)

func CreateWal(path string) (*Wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_SYNC, 0)
	if err != nil {
		return nil, err
	}
	return &Wal{file: f}, nil
}

func RestoreWal(path string) (*Wal, error) {
	return nil, nil
}

// Serializes the record in the following binary format:
// [key len][key][data len][data][timestamp]
// If the entry is a tombstone, then data len < 0.
func (e *Entry) serialize() []byte {
	buf := make([]byte, 0, len(e.key)+1+len(e.data)+8)
	buf = append(buf, []byte(e.key)...)
	buf = append(buf, '\x00')

	if e.data != nil {
		buf = append(buf, e.data...)
	}
	return binary.LittleEndian.AppendUint64(buf, e.timestamp)
}

func deserializeEntry(b []byte) (*Entry, error) {
	key, rest, found := bytes.Cut(b, []byte{'\x00'})
	if !found {
		return nil, errors.New("cannot deserialize entry - unterminated string in buffer")
	}
	if len(rest) < 8 {
		return nil, fmt.Errorf("invalid entry format - not enough bytes for timestamp: %v", len(rest))
	}

	dlen := len(rest) - 8
	data := rest[0:dlen]
	timestamp := binary.LittleEndian.Uint64(rest[dlen:])
	return &Entry{key: string(key), data: data, timestamp: timestamp}, nil
}
