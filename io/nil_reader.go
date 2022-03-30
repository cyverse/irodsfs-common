package io

import (
	"fmt"

	"github.com/cyverse/irodsfs-common/irods"
)

// NilReader does nothing for read
type NilReader struct {
	path       string
	fileHandle irods.IRODSFSFileHandle
}

// NewNilReader create a new NilReader
func NewNilReader(fileHandle irods.IRODSFSFileHandle) *NilReader {
	entry := fileHandle.GetEntry()

	nilReader := &NilReader{
		path:       entry.Path,
		fileHandle: fileHandle,
	}

	return nilReader
}

// Release releases all resources
func (reader *NilReader) Release() {
}

// ReadAt reads data
func (reader *NilReader) ReadAt(offset int64, length int) ([]byte, error) {
	return nil, fmt.Errorf("failed to read data using NilReader - %s, offset %d, length %d", reader.path, offset, length)
}

func (reader *NilReader) GetPendingError() error {
	return nil
}
