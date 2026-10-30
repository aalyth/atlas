package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

type Operation = byte

const (
	OpPut Operation = iota
	OpDel
)

const (
	walDefaultPerm = os.FileMode(0644)
	// checksum + lsn + len size
	recordHeaderSize int = 4 + 8 + 4
)

var (
	castagnoli         = crc32.MakeTable(crc32.Castagnoli)
	errCorruptedRecord = errors.New("corrupted record checksum")
)

type (
	Entry struct {
		op        Operation
		key       string
		value     []byte
		timestamp uint64
	}
	Wal struct {
		file *os.File
		lsn  uint64
	}

	// [checksum][LSN][len][timestamp][op][klen][key][data]
	record struct {
		crc   uint32
		lsn   uint64
		len   uint32
		entry Entry
	}
)

func CreateWal(path string, lsn uint64) (*Wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_SYNC, walDefaultPerm)
	if err != nil {
		return nil, err
	}
	return &Wal{file: f, lsn: lsn}, nil
}

func RestoreWal(path string, defaultLsn uint64, callback func(*Entry)) (*Wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_SYNC, 0)
	if err != nil {
		return nil, err
	}

	rcb := func(r *record) {
		callback(&r.entry)
		defaultLsn = r.lsn + 1
	}
	if err = forEachFileRecord(f, rcb); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal restore entries: %w", err)
	}
	return &Wal{file: f, lsn: defaultLsn}, nil
}

func (w *Wal) Close() error {
	return w.file.Close()
}

func (w *Wal) Put(key string, value []byte) error {
	return w.append(Entry{
		op:        OpPut,
		key:       key,
		value:     value,
		timestamp: uint64(time.Now().UnixMilli()),
	})
}

func (w *Wal) Del(key string) error {
	return w.append(Entry{
		op:        OpDel,
		key:       key,
		timestamp: uint64(time.Now().UnixMilli()),
	})
}

func (w *Wal) append(entry Entry) error {
	record, err := newRecord(w.lsn, entry)
	if err != nil {
		return err
	}
	ser := record.serialize()
	if n, err := w.file.Write(ser); err != nil || n < len(ser) {
		return fmt.Errorf("failed or incomplete write: %w", err)
	}
	w.lsn += 1
	return nil
}

// Reads each record within the file. Leaves the file cursor at the end or at the
// first offset, where an invalid record is encountered.
func forEachFileRecord(f *os.File, callback func(*record)) error {
	offset := 0
	for {
		record, n, err := deserializeRecord(f)
		if err == io.EOF || errors.Is(err, errCorruptedRecord) {
			break
		}
		offset += n
		if err != nil {
			return err
		}
		callback(record)
	}
	f.Seek(int64(offset), io.SeekStart)
	return nil
}

func newRecord(lsn uint64, entry Entry) (*record, error) {
	e := entry.serialize()
	crc, err := checksum(lsn, e)
	if err != nil {
		return nil, err
	}
	return &record{
		crc:   crc,
		lsn:   lsn,
		len:   uint32(len(e)),
		entry: entry,
	}, nil
}

func checksum(lsn uint64, entry []byte) (uint32, error) {
	h := crc32.New(castagnoli)
	if n, err := h.Write(binary.LittleEndian.AppendUint64(nil, lsn)); err != nil || n < 4 {
		return 0, fmt.Errorf("cannot generate record checksum: %w", err)
	}
	l := uint32(len(entry))
	if n, err := h.Write(binary.LittleEndian.AppendUint32(nil, l)); err != nil || n < 4 {
		return 0, fmt.Errorf("cannot generate record checksum: %w", err)
	}
	if n, err := h.Write(entry); err != nil || n < len(entry) {
		return 0, fmt.Errorf("cannot generate record checksum: %w", err)
	}
	return h.Sum32(), nil
}

func (r *record) serialize() []byte {
	buf := make([]byte, 0, recordHeaderSize)
	buf = binary.LittleEndian.AppendUint32(buf, r.crc)
	buf = binary.LittleEndian.AppendUint64(buf, r.lsn)
	buf = binary.LittleEndian.AppendUint32(buf, r.len)
	return append(buf, r.entry.serialize()...)
}

func deserializeRecord(r io.Reader) (*record, int, error) {
	header := make([]byte, recordHeaderSize)
	n, err := r.Read(header)
	if err != nil {
		return nil, n, err
	}
	if n < recordHeaderSize {
		return nil, n, io.EOF
	}

	crc := binary.LittleEndian.Uint32(header[0:])
	lsn := binary.LittleEndian.Uint64(header[4:])
	entryLen := binary.LittleEndian.Uint32(header[12:])

	rawEntry, err := io.ReadAll(io.LimitReader(r, int64(entryLen)))
	readTotal := n + len(rawEntry)
	if err != nil {
		return nil, readTotal, err
	}

	c, err := checksum(lsn, rawEntry)
	if err != nil {
		return nil, readTotal, fmt.Errorf("cannot validate checksum: %w", err)
	}
	if crc != c {
		return nil, readTotal, errCorruptedRecord
	}

	entry, err := deserializeEntry(rawEntry)
	if err != nil || entry == nil {
		return nil, readTotal, fmt.Errorf("invalid entry(%v): %w", entry, err)
	}

	return &record{
		crc:   crc,
		lsn:   lsn,
		len:   entryLen,
		entry: *entry,
	}, readTotal, nil
}

func (e *Entry) serialize() []byte {
	buf := make([]byte, 0, 8+1+8+len(e.key)+len(e.value))
	buf = binary.LittleEndian.AppendUint64(buf, e.timestamp)
	buf = append(buf, e.op)
	buf = binary.AppendUvarint(buf, uint64(len(e.key)))
	buf = append(buf, []byte(e.key)...)
	if e.op != OpDel {
		buf = append(buf, e.value...)
	}
	return buf
}

func deserializeEntry(b []byte) (*Entry, error) {
	if len(b) < 9 {
		return nil, fmt.Errorf("bad entry header size: %d", len(b))
	}

	timestamp := binary.LittleEndian.Uint64(b)
	op := b[8]

	b = b[9:]
	klen, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, errors.New("bad key length format")
	}

	b = b[n:]
	if klen > uint64(len(b)) {
		return nil, fmt.Errorf("key len larger than buffer: %d/%d", klen, len(b))
	}

	key := string(b[:klen])
	value := b[klen:]
	return &Entry{op: op, key: key, value: value, timestamp: timestamp}, nil
}
