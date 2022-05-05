package io

import (
	"fmt"

	"github.com/cyverse/irodsfs-common/irods"
)

// NilWriter does nothing for write
type NilWriter struct {
	path       string
	fileHandle irods.IRODSFSFileHandle
}

// NewNilWriter create a new NilWriter
func NewNilWriter(fileHandle irods.IRODSFSFileHandle) Writer {
	entry := fileHandle.GetEntry()

	nilWriter := &NilWriter{
		path:       entry.Path,
		fileHandle: fileHandle,
	}

	return nilWriter
}

// Release releases all resources
func (writer *NilWriter) Release() {
}

// GetPath returns path of the file
func (writer *NilWriter) GetPath() string {
	return writer.path
}

// WriteAt writes data
func (writer *NilWriter) WriteAt(data []byte, offset int64) (int, error) {
	return 0, fmt.Errorf("failed to write data using NilWriter - %s, offset %d, length %d", writer.path, offset, len(data))
}

func (writer *NilWriter) Flush() error {
	return nil
}

func (writer *NilWriter) GetPendingError() error {
	return nil
}
