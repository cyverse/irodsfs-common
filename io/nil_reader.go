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

// GetPath returns path of the file
func (reader *NilReader) GetPath() string {
	return reader.path
}

// ReadAt reads data
func (reader *NilReader) ReadAt(buffer []byte, offset int64) (int, error) {
	return 0, fmt.Errorf("failed to read data using NilReader - %s, offset %d, length %d", reader.path, offset, len(buffer))
}

func (reader *NilReader) GetPendingError() error {
	return nil
}
