package io

import (
	"github.com/cyverse/irodsfs-common/irods"
	log "github.com/sirupsen/logrus"
)

// SyncReader helps sync read
type SyncReader struct {
	path       string
	fileHandle irods.IRODSFSFileHandle
}

// NewSyncReader create a new SyncReader
func NewSyncReader(fileHandle irods.IRODSFSFileHandle) *SyncReader {
	entry := fileHandle.GetEntry()

	syncReader := &SyncReader{
		path:       entry.Path,
		fileHandle: fileHandle,
	}

	return syncReader
}

// Release releases all resources
func (reader *SyncReader) Release() {
}

// ReadAt reads data
func (reader *SyncReader) ReadAt(offset int64, length int) ([]byte, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "SyncReader",
		"function": "ReadAt",
	})

	if length <= 0 || offset < 0 {
		return []byte{}, nil
	}

	logger.Infof("Sync Reading - %s, offset %d, length %d", reader.path, offset, length)

	data, err := reader.fileHandle.ReadAt(offset, length)
	if err != nil {
		logger.WithError(err).Errorf("failed to read data - %s, offset %d, length %d", reader.path, offset, length)
		return nil, err
	}

	return data, nil
}

func (reader *SyncReader) GetPendingError() error {
	return nil
}
