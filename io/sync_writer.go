package io

import (
	"github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-common/report"
	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
)

// SyncWriter helps sync write
type SyncWriter struct {
	path       string
	fileHandle irods.IRODSFSFileHandle

	reportClient report.IRODSFSInstanceReportClient
}

// NewSyncWriter create a new SyncWriter
func NewSyncWriter(fileHandle irods.IRODSFSFileHandle, reportClient report.IRODSFSInstanceReportClient) *SyncWriter {
	entry := fileHandle.GetEntry()

	syncWriter := &SyncWriter{
		path:       entry.Path,
		fileHandle: fileHandle,

		reportClient: reportClient,
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

// GetPath returns path of the file
func (writer *SyncWriter) GetPath() string {
	return writer.path
}

// WriteAt writes data
func (writer *SyncWriter) WriteAt(offset int64, data []byte) error {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "SyncWriter",
		"function": "WriteAt",
	})

	defer utils.StackTraceFromPanic(logger)

	if len(data) == 0 || offset < 0 {
		return nil
	}

	logger.Infof("Sync Writing - %s, offset %d, length %d", writer.path, offset, len(data))

	err := writer.fileHandle.WriteAt(offset, data)
	if err != nil {
		logger.WithError(err).Errorf("failed to write data - %s, offset %d, length %d", writer.path, offset, len(data))
		return err
	}

	// Report
	if writer.reportClient != nil {
		writer.reportClient.FileAccess(writer.fileHandle, offset, int64(len(data)))
	}

	return nil
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

func (writer *SyncWriter) GetPendingError() error {
	return nil
}
