package io

import (
	"github.com/cyverse/irodsfs-common/irods"
	"golang.org/x/xerrors"
)

// NilReader does nothing for read
type NilReader struct {
	fsClient   irods.IRODSFSClient
	path       string
	checksum   string
	size       int64
	fileHandle irods.IRODSFSFileHandle
}

// NewNilReader create a new NilReader
func NewNilReader(fsClient irods.IRODSFSClient, fileHandle irods.IRODSFSFileHandle) Reader {
	entry := fileHandle.GetEntry()

	nilReader := &NilReader{
		fsClient:   fsClient,
		path:       entry.Path,
		checksum:   entry.CheckSum,
		size:       entry.Size,
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

// GetChecksum returns checksum of the file
func (reader *NilReader) GetChecksum() string {
	return reader.checksum
}

// GetSize returns size of the file
func (reader *NilReader) GetSize() int64 {
	return reader.size
}

// ReadAt reads data
func (reader *NilReader) ReadAt(buffer []byte, offset int64) (int, error) {
	return 0, xerrors.Errorf("failed to read data from %s, offset %d, length %d", reader.path, offset, len(buffer))
}

// GetAvailable returns available data len
func (reader *NilReader) GetAvailable(offset int64) int64 {
	return 0
}

// GetError returns error if exists
func (reader *NilReader) GetError() error {
	return nil
}
