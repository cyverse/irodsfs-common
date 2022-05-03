package io

// Reader helps data read
type Reader interface {
	GetPath() string

	ReadAt(buffer []byte, offset int64) (int, error)
	GetPendingError() error
	Release()
}
