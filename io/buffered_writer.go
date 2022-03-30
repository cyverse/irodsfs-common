package io

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
)

const (
	bufferedWriterBufferSizeMax int = 1024 * 1024 * 8 // 8MB
)

// BufferedWriter is a writer that buffers data in RAM before write
type BufferedWriter struct {
	path string

	buffer                   bytes.Buffer
	currentBufferStartOffset int64
	bufferMutex              sync.Mutex

	writer Writer
}

// NewBufferedWriter creates a BufferedWriter
func NewBufferedWriter(path string, writer Writer) *BufferedWriter {
	return &BufferedWriter{
		path: path,

		buffer:                   bytes.Buffer{},
		currentBufferStartOffset: 0,

		writer: writer,
	}
}

// Release releases all resources
func (writer *BufferedWriter) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "BufferedWriter",
		"function": "Release",
	})

	defer utils.StackTraceFromPanic(logger)

	writer.Flush()

	if writer.writer != nil {
		writer.writer.Release()
		writer.writer = nil
	}
}

// Flush flushes buffered data
func (writer *BufferedWriter) Flush() error {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "BufferedWriter",
		"function": "Flush",
	})

	defer utils.StackTraceFromPanic(logger)

	// empty buffer
	if writer.buffer.Len() > 0 {
		err := writer.writer.WriteAt(writer.currentBufferStartOffset, writer.buffer.Bytes())
		if err != nil {
			logger.Error(err)
			return err
		}
	}

	writer.currentBufferStartOffset = 0
	writer.buffer.Reset()

	return writer.writer.Flush()
}

// Write writes data
func (writer *BufferedWriter) WriteAt(offset int64, data []byte) error {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "BufferedWriter",
		"function": "WriteAt",
	})

	defer utils.StackTraceFromPanic(logger)

	if writer.writer == nil {
		return fmt.Errorf("failed to write data to nil writer")
	}

	if len(data) == 0 || offset < 0 {
		return nil
	}

	writer.bufferMutex.Lock()
	defer writer.bufferMutex.Unlock()

	// check if data is continuous from prior write
	if writer.buffer.Len() > 0 {
		// has data
		if writer.currentBufferStartOffset+int64(writer.buffer.Len()) != offset {
			// not continuous
			// send out
			err := writer.writer.WriteAt(writer.currentBufferStartOffset, writer.buffer.Bytes())
			if err != nil {
				logger.Error(err)
				return err
			}

			writer.currentBufferStartOffset = 0
			writer.buffer.Reset()

			// write to buffer
			_, err = writer.buffer.Write(data)
			if err != nil {
				logger.WithError(err).Errorf("failed to buffer data for file %s, offset %d, length %d", writer.path, offset, len(data))
				return err
			}

			writer.currentBufferStartOffset = offset
		} else {
			// continuous
			// write to buffer
			_, err := writer.buffer.Write(data)
			if err != nil {
				logger.WithError(err).Errorf("failed to buffer data for file %s, offset %d, length %d", writer.path, offset, len(data))
				return err
			}
		}
	} else {
		// write to buffer
		_, err := writer.buffer.Write(data)
		if err != nil {
			logger.WithError(err).Errorf("failed to buffer data for file %s, offset %d, length %d", writer.path, offset, len(data))
			return err
		}

		writer.currentBufferStartOffset = offset
	}

	if writer.buffer.Len() >= bufferedWriterBufferSizeMax {
		// Spill to disk cache
		err := writer.writer.WriteAt(writer.currentBufferStartOffset, writer.buffer.Bytes())
		if err != nil {
			logger.Error(err)
			return err
		}

		writer.currentBufferStartOffset = 0
		writer.buffer.Reset()
	}

	return nil
}

// GetPendingError returns pending errors
func (writer *BufferedWriter) GetPendingError() error {
	if writer.writer != nil {
		return writer.writer.GetPendingError()
	}
	return nil
}
