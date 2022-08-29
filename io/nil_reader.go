package io

import (
	"fmt"

	"github.com/cyverse/irodsfs-common/irods"
)

// NilReader does nothing for read
type NilReader struct {
	fsClient   irods.IRODSFSClient
	path       string
	fileHandle irods.IRODSFSFileHandle
}

// NewNilReader create a new NilReader
func NewNilReader(fsClient irods.IRODSFSClient, fileHandle irods.IRODSFSFileHandle) Reader {
	entry := fileHandle.GetEntry()

	nilReader := &NilReader{
		fsClient:   fsClient,
		path:       entry.Path,
		fileHandle: fileHandle,
	}

	return nilReader
}

// Release releases all resources
func (reader *NilReader) Release() {
}

// GetFSClient returns fs client
func (reader *NilReader) GetFSClient() irods.IRODSFSClient {
	return reader.fsClient
}

// GetPath returns path of the file
func (reader *NilReader) GetPath() string {
	return reader.path
}

// ReadAt reads data
func (reader *NilReader) ReadAt(buffer []byte, offset int64) (int, error) {
	return 0, fmt.Errorf("failed to read data using NilReader - %s, offset %d, length %d", reader.path, offset, len(buffer))
}

// GetAvailable returns available data len
func (reader *NilReader) GetAvailable(offset int64) int64 {
	return 0
}

// GetPendingError returns errors pending
func (reader *NilReader) GetPendingError() error {
	return nil
}
