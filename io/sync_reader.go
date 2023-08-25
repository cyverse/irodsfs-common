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
	checksum   string
	fileHandle irods.IRODSFSFileHandle

	reportClient report.IRODSFSInstanceReportClient
}

// NewSyncReader create a new SyncReader
func NewSyncReader(fsClient irods.IRODSFSClient, fileHandle irods.IRODSFSFileHandle, reportClient report.IRODSFSInstanceReportClient) Reader {
	entry := fileHandle.GetEntry()

	syncReader := &SyncReader{
		fsClient:   fsClient,
		path:       entry.Path,
		checksum:   entry.CheckSum,
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

// GetChecksum returns checksum of the file
func (reader *SyncReader) GetChecksum() string {
	return reader.checksum
}

// GetSize returns size of the file
func (reader *SyncReader) GetSize() int64 {
	return reader.fileHandle.GetEntry().Size
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

	if offset >= reader.fileHandle.GetEntry().Size {
		return 0, io.EOF
	}

	logger.Debugf("Sync Reading - %s, offset %d, length %d", reader.path, offset, len(buffer))

	readLen, err := reader.fileHandle.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
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

func (reader *SyncReader) GetError() error {
	return nil
}
