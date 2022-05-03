package io

// Writer helps data write
type Writer interface {
	GetPath() string

	WriteAt(data []byte, offset int64) (int, error)
	Flush() error
	GetPendingError() error
	Release()
}
