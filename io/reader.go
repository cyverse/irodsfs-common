package io

import "github.com/cyverse/irodsfs-common/irods"

// Reader helps data read
type Reader interface {
	GetFSClient() irods.IRODSFSClient
	GetPath() string
	GetChecksum() string
	GetSize() int64

	// io.ReaderAt
	ReadAt(buffer []byte, offset int64) (int, error)
	GetAvailable(offset int64) int64 // -1 for unknown

	GetError() error
	Release()
}
