package storage

import "os"

// Sorted String Table
type SSTable struct {
	file     *os.File
	filename string
	index    []int64
	minKey   string
	maxKey   string
}
