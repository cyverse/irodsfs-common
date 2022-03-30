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
func NewNilWriter(fileHandle irods.IRODSFSFileHandle) *NilWriter {
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

// WriteAt writes data
func (writer *NilWriter) WriteAt(offset int64, data []byte) error {
	return fmt.Errorf("failed to write data using NilWriter - %s, offset %d, length %d", writer.path, offset, len(data))
}

func (writer *NilWriter) Flush() error {
	return nil
}

func (writer *NilWriter) GetPendingError() error {
	return nil
}
