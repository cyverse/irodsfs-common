package io

import "github.com/cyverse/irodsfs-common/irods"

// Writer helps data write
type Writer interface {
	GetFSClient() irods.IRODSFSClient
	GetPath() string

	// io.WriterAt
	WriteAt(data []byte, offset int64) (int, error)

	Flush() error
	GetPendingError() error
	Release()
}
