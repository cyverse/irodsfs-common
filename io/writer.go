package io

// Writer helps data write
type Writer interface {
	GetPath() string

	// io.WriterAt
	WriteAt(data []byte, offset int64) (int, error)

	Flush() error
	GetPendingError() error
	Release()
}
