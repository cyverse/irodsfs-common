package io

// Reader helps data read
type Reader interface {
	GetPath() string

	// io.ReaderAt
	ReadAt(buffer []byte, offset int64) (int, error)
	GetAvailable(offset int64) int64 // -1 for unknown

	GetPendingError() error
	Release()
}
