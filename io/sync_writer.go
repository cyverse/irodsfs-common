package io

import (
	"github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
)

// SyncWriter helps sync write
type SyncWriter struct {
	fsClient   irods.IRODSFSClient
	path       string
	fileHandle irods.IRODSFSFileHandle
}

// NewSyncWriter create a new SyncWriter
func NewSyncWriter(fsClient irods.IRODSFSClient, fileHandle irods.IRODSFSFileHandle) Writer {
	entry := fileHandle.GetEntry()

	syncWriter := &SyncWriter{
		fsClient:   fsClient,
		path:       entry.Path,
		fileHandle: fileHandle,
	}

	return syncWriter
}

// Release releases all resources
func (writer *SyncWriter) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "SyncWriter",
		"function": "Release",
	})

	defer utils.StackTraceFromPanic(logger)

	writer.Flush()
}

// GetFSClient returns fs client
func (writer *SyncWriter) GetFSClient() irods.IRODSFSClient {
	return writer.fsClient
}

// GetPath returns path of the file
func (writer *SyncWriter) GetPath() string {
	return writer.path
}

// WriteAt writes data
func (writer *SyncWriter) WriteAt(data []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "SyncWriter",
		"function": "WriteAt",
	})

	defer utils.StackTraceFromPanic(logger)

	if len(data) == 0 || offset < 0 {
		return 0, nil
	}

	logger.Debugf("Sync Writing - %s, offset %d, length %d", writer.path, offset, len(data))

	writeLen, err := writer.fileHandle.WriteAt(data, offset)
	if err != nil {
		return 0, err
	}

	return writeLen, nil
}

func (writer *SyncWriter) Flush() error {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "SyncWriter",
		"function": "Flush",
	})

	defer utils.StackTraceFromPanic(logger)

	return writer.fileHandle.Flush()
}

func (writer *SyncWriter) GetError() error {
	return nil
}
