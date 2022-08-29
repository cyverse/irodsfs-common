package io

import "github.com/cyverse/irodsfs-common/irods"

// Reader helps data read
type Reader interface {
	GetFSClient() irods.IRODSFSClient
	GetPath() string

	// io.ReaderAt
	ReadAt(buffer []byte, offset int64) (int, error)
	GetAvailable(offset int64) int64 // -1 for unknown

	GetPendingError() error
	Release()
}
