package io

import (
	"github.com/cyverse/irodsfs-common/irods"
	"golang.org/x/xerrors"
)

// NilWriter does nothing for write
type NilWriter struct {
	fsClient   irods.IRODSFSClient
	path       string
	fileHandle irods.IRODSFSFileHandle
}

// NewNilWriter create a new NilWriter
func NewNilWriter(fsClient irods.IRODSFSClient, fileHandle irods.IRODSFSFileHandle) Writer {
	entry := fileHandle.GetEntry()

	nilWriter := &NilWriter{
		fsClient:   fsClient,
		path:       entry.Path,
		fileHandle: fileHandle,
	}

	return nilWriter
}

// Release releases all resources
func (writer *NilWriter) Release() {
}

// GetFSClient returns fs client
func (writer *NilWriter) GetFSClient() irods.IRODSFSClient {
	return writer.fsClient
}

// GetPath returns path of the file
func (writer *NilWriter) GetPath() string {
	return writer.path
}

// WriteAt writes data
func (writer *NilWriter) WriteAt(data []byte, offset int64) (int, error) {
	return 0, xerrors.Errorf("failed to writer data to %s, offset %d, length %d", writer.path, offset, len(data))
}

func (writer *NilWriter) Flush() error {
	return nil
}

func (writer *NilWriter) GetError() error {
	return nil
}
