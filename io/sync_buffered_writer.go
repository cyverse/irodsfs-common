package io

import (
	"bytes"
	"sync"

	"github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
)

// SyncBufferedWriter is a writer that buffers data in RAM before write
type SyncBufferedWriter struct {
	fsClient irods.IRODSFSClient
	path     string

	buffer                   bytes.Buffer
	bufferSize               int
	currentBufferStartOffset int64
	bufferMutex              sync.Mutex

	baseWriter Writer
}

// NewSyncBufferedWriter creates a SyncBufferedWriter
func NewSyncBufferedWriter(writer Writer, bufferSize int) Writer {
	return &SyncBufferedWriter{
		fsClient: writer.GetFSClient(),
		path:     writer.GetPath(),

		buffer:                   bytes.Buffer{},
		bufferSize:               bufferSize,
		currentBufferStartOffset: 0,

		baseWriter: writer,
	}
}

// Release releases all resources
func (writer *SyncBufferedWriter) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "SyncBufferedWriter",
		"function": "Release",
	})

	defer utils.StackTraceFromPanic(logger)

	writer.Flush()

	if writer.baseWriter != nil {
		writer.baseWriter.Release()
		writer.baseWriter = nil
	}
}

// GetFSClient returns fs client
func (writer *SyncBufferedWriter) GetFSClient() irods.IRODSFSClient {
	return writer.fsClient
}

// GetPath returns path of the file
func (writer *SyncBufferedWriter) GetPath() string {
	return writer.path
}

// Flush flushes buffered data
func (writer *SyncBufferedWriter) Flush() error {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "SyncBufferedWriter",
		"function": "Flush",
	})

	defer utils.StackTraceFromPanic(logger)

	// empty buffer
	if writer.buffer.Len() > 0 {
		_, err := writer.baseWriter.WriteAt(writer.buffer.Bytes(), writer.currentBufferStartOffset)
		if err != nil {
			logger.Error(err)
			return err
		}
	}

	writer.currentBufferStartOffset = 0
	writer.buffer.Reset()

	return writer.baseWriter.Flush()
}

// Write writes data
func (writer *SyncBufferedWriter) WriteAt(data []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "SyncBufferedWriter",
		"function": "WriteAt",
	})

	defer utils.StackTraceFromPanic(logger)

	if len(data) == 0 || offset < 0 {
		return 0, nil
	}

	writer.bufferMutex.Lock()
	defer writer.bufferMutex.Unlock()

	// check if data is continuous from prior write
	if writer.buffer.Len() > 0 {
		// has data
		if writer.currentBufferStartOffset+int64(writer.buffer.Len()) != offset {
			// not continuous
			// send out
			_, err := writer.baseWriter.WriteAt(writer.buffer.Bytes(), writer.currentBufferStartOffset)
			if err != nil {
				logger.Error(err)
				return 0, err
			}

			writer.currentBufferStartOffset = 0
			writer.buffer.Reset()

			// write to buffer
			_, err = writer.buffer.Write(data)
			if err != nil {
				logger.WithError(err).Errorf("failed to buffer data for file %s, offset %d, length %d", writer.path, offset, len(data))
				return 0, err
			}

			writer.currentBufferStartOffset = offset
		} else {
			// continuous
			// write to buffer
			_, err := writer.buffer.Write(data)
			if err != nil {
				logger.WithError(err).Errorf("failed to buffer data for file %s, offset %d, length %d", writer.path, offset, len(data))
				return 0, err
			}
		}
	} else {
		// write to buffer
		_, err := writer.buffer.Write(data)
		if err != nil {
			logger.WithError(err).Errorf("failed to buffer data for file %s, offset %d, length %d", writer.path, offset, len(data))
			return 0, err
		}

		writer.currentBufferStartOffset = offset
	}

	if writer.buffer.Len() >= writer.bufferSize {
		// Spill to disk cache
		_, err := writer.baseWriter.WriteAt(writer.buffer.Bytes(), writer.currentBufferStartOffset)
		if err != nil {
			logger.Error(err)
			return 0, err
		}

		writer.currentBufferStartOffset = 0
		writer.buffer.Reset()
	}

	return len(data), nil
}

// GetPendingError returns pending errors
func (writer *SyncBufferedWriter) GetPendingError() error {
	if writer.baseWriter != nil {
		return writer.baseWriter.GetPendingError()
	}
	return nil
}
