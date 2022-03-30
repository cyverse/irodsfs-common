package io

// Reader helps data read
type Reader interface {
	GetPath() string

	ReadAt(offset int64, length int) ([]byte, error)
	GetPendingError() error
	Release()
}
