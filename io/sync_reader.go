package io

import (
	"io"

	"github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-common/report"
	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
)

// SyncReader helps sync read
type SyncReader struct {
	fsClient   irods.IRODSFSClient
	path       string
	fileHandle irods.IRODSFSFileHandle

	reportClient report.IRODSFSInstanceReportClient
}

// NewSyncReader create a new SyncReader
func NewSyncReader(fsClient irods.IRODSFSClient, fileHandle irods.IRODSFSFileHandle, reportClient report.IRODSFSInstanceReportClient) Reader {
	entry := fileHandle.GetEntry()

	syncReader := &SyncReader{
		fsClient:   fsClient,
		path:       entry.Path,
		fileHandle: fileHandle,

		reportClient: reportClient,
	}

	return syncReader
}

// Release releases all resources
func (reader *SyncReader) Release() {
}

// GetFSClient returns fs client
func (reader *SyncReader) GetFSClient() irods.IRODSFSClient {
	return reader.fsClient
}

// GetPath returns path of the file
func (reader *SyncReader) GetPath() string {
	return reader.path
}

// ReadAt reads data
func (reader *SyncReader) ReadAt(buffer []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "SyncReader",
		"function": "ReadAt",
	})

	defer utils.StackTraceFromPanic(logger)

	if len(buffer) <= 0 || offset < 0 {
		return 0, nil
	}

	logger.Debugf("Sync Reading - %s, offset %d, length %d", reader.path, offset, len(buffer))

	readLen, err := reader.fileHandle.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
		logger.WithError(err).Errorf("failed to read data - %s, offset %d, length %d", reader.path, offset, len(buffer))
		return 0, err
	}

	// Report
	if reader.reportClient != nil {
		reader.reportClient.FileAccess(reader.fileHandle, offset, int64(readLen))
	}

	// may return EOF as well
	return readLen, err
}

// GetAvailable returns available data len
func (reader *SyncReader) GetAvailable(offset int64) int64 {
	return reader.fileHandle.GetAvailable(offset)
}

func (reader *SyncReader) GetPendingError() error {
	return nil
}
