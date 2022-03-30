package io

// Writer helps data write
type Writer interface {
	GetPath() string

	WriteAt(offset int64, data []byte) error
	Flush() error
	GetPendingError() error
	Release()
}
