package io

import (
	"bytes"
	"sync"

	"github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

// SyncBufferedWriter is a writer that buffers data in RAM before write
type SyncBufferedWriter struct {
	baseWriter Writer
	fsClient   irods.IRODSFSClient
	path       string

	buffer                   *bytes.Buffer
	bufferSize               int
	currentBufferStartOffset int64
	mutex                    sync.Mutex
}

// NewSyncBufferedWriter creates a SyncBufferedWriter
func NewSyncBufferedWriter(writer Writer, bufferSize int) Writer {
	return &SyncBufferedWriter{
		baseWriter: writer,
		fsClient:   writer.GetFSClient(),
		path:       writer.GetPath(),

		buffer:                   &bytes.Buffer{},
		bufferSize:               bufferSize,
		currentBufferStartOffset: 0,
		mutex:                    sync.Mutex{},
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

	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	if writer.buffer != nil {
		writer.buffer = nil
	}

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

func (writer *SyncBufferedWriter) spillBuffer() error {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "SyncBufferedWriter",
		"function": "Flush",
	})

	defer utils.StackTraceFromPanic(logger)

	// we don't lock here

	if writer.buffer.Len() > 0 {
		_, err := writer.baseWriter.WriteAt(writer.buffer.Bytes(), writer.currentBufferStartOffset)
		if err != nil {
			return err
		}

		// allocate a new buffer, old buffer will be passed to baseWriter
		writer.buffer = &bytes.Buffer{}
	}

	writer.currentBufferStartOffset = 0
	return nil
}

// Flush flushes buffered data
func (writer *SyncBufferedWriter) Flush() error {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "SyncBufferedWriter",
		"function": "Flush",
	})

	defer utils.StackTraceFromPanic(logger)

	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	// empty buffer
	err := writer.spillBuffer()
	if err != nil {
		return err
	}

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

	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	// check if data is continuous from prior write
	if writer.buffer.Len() > 0 {
		// has data
		if writer.currentBufferStartOffset+int64(writer.buffer.Len()) != offset {
			// offsets are not continuous
			// empty buffer
			err := writer.spillBuffer()
			if err != nil {
				return 0, err
			}

			// write to buffer
			_, err = writer.buffer.Write(data)
			if err != nil {
				return 0, xerrors.Errorf("failed to write data to buffer for %s, offset %d, length %d: %w", writer.path, offset, len(data), err)
			}

			writer.currentBufferStartOffset = offset
		} else {
			// continuous
			// write to buffer
			_, err := writer.buffer.Write(data)
			if err != nil {
				return 0, xerrors.Errorf("failed to write data to buffer for %s, offset %d, length %d: %w", writer.path, offset, len(data), err)
			}
		}
	} else {
		// write to buffer
		_, err := writer.buffer.Write(data)
		if err != nil {
			return 0, xerrors.Errorf("failed to write data to buffer for %s, offset %d, length %d: %w", writer.path, offset, len(data), err)
		}

		writer.currentBufferStartOffset = offset
	}

	if writer.buffer.Len() >= writer.bufferSize {
		// empty buffer
		err := writer.spillBuffer()
		if err != nil {
			return 0, err
		}
	}

	return len(data), nil
}

// GetError returns error
func (writer *SyncBufferedWriter) GetError() error {
	if writer.baseWriter != nil {
		return writer.baseWriter.GetError()
	}
	return nil
}
