package io

import (
	"io"
	"sync"

	"github.com/cyverse/irodsfs-common/utils"
	"github.com/eikenb/pipeat"
	log "github.com/sirupsen/logrus"
)

type writeData struct {
	startOffset int64
	pipeReader  *pipeat.PipeReaderAt
	pipeWriter  *pipeat.PipeWriterAt
	waiter      *sync.WaitGroup
}

// AsyncWriter helps async write
type AsyncWriter struct {
	path string

	baseWriter   Writer
	writeSize    int
	localPipeDir string

	currentWriteDataBuffer *writeData
	mutex                  sync.Mutex // lock for current data

	pendingErrors      []error
	pendingErrorsMutex sync.Mutex
}

// NewAsyncWriter create a new AsyncWriter
// example sizes
// writeSize = 64KB
func NewAsyncWriter(writer Writer, writeSize int, localPipeDir string) Writer {
	return &AsyncWriter{
		path: writer.GetPath(),

		baseWriter:   writer,
		writeSize:    writeSize,
		localPipeDir: localPipeDir,

		currentWriteDataBuffer: nil,

		pendingErrors: []error{},
	}
}

// Release releases all resources
func (writer *AsyncWriter) Release() {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	writer.releaseDataBuffer()
}

// GetPath returns path
func (writer *AsyncWriter) GetPath() string {
	return writer.path
}

// Write writes data
func (writer *AsyncWriter) WriteAt(data []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncWriter",
		"function": "WriteAt",
	})

	defer utils.StackTraceFromPanic(logger)

	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	if len(data) == 0 || offset < 0 {
		return 0, nil
	}

	logger.Infof("Writing data - %s, offset %d, length %d", writer.path, offset, len(data))

	// any pending
	err := writer.GetPendingError()
	if err != nil {
		logger.WithError(err).Errorf("failed to write - %v", err)
		return 0, err
	}

	err = writer.newDataBuffer(offset)
	if err != nil {
		return 0, err
	}

	writeLen, err := writer.currentWriteDataBuffer.pipeWriter.Write(data)
	if err != nil {
		return writeLen, err
	}

	// any pending
	err = writer.GetPendingError()
	if err != nil {
		logger.WithError(err).Errorf("failed to write - %v", err)
		return 0, err
	}

	return writeLen, nil
}

// Flush flushes buffered data
func (writer *AsyncWriter) Flush() error {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncWriter",
		"function": "Flush",
	})

	defer utils.StackTraceFromPanic(logger)

	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	writer.releaseDataBuffer()

	err := writer.baseWriter.Flush()
	if err != nil {
		return err
	}

	// any pending
	err = writer.GetPendingError()
	if err != nil {
		logger.WithError(err).Errorf("failed to write - %v", err)
		return err
	}

	return nil
}

// GetPendingError returns pending errors
func (writer *AsyncWriter) GetPendingError() error {
	writer.pendingErrorsMutex.Lock()
	defer writer.pendingErrorsMutex.Unlock()

	if len(writer.pendingErrors) > 0 {
		return writer.pendingErrors[0]
	}
	return nil
}

func (writer *AsyncWriter) addAsyncError(err error) {
	writer.pendingErrorsMutex.Lock()
	defer writer.pendingErrorsMutex.Unlock()

	writer.pendingErrors = append(writer.pendingErrors, err)
}

func (writer *AsyncWriter) newDataBuffer(offset int64) error {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncWriter",
		"function": "newDataBuffer",
	})

	defer utils.StackTraceFromPanic(logger)

	// unload first
	if writer.currentWriteDataBuffer != nil {
		currentOffset := writer.currentWriteDataBuffer.startOffset + writer.currentWriteDataBuffer.pipeWriter.GetWrittenBytes()
		if currentOffset != offset {
			writer.releaseDataBuffer()
		} else {
			// it's already loaded
			return nil
		}
	}

	logger.Infof("Creating a new buffer - %s, offset %d", writer.path, offset)

	pipeReader, pipeWriter, err := pipeat.PipeInDir(writer.localPipeDir)
	if err != nil {
		logger.WithError(err).Errorf("failed to create a pipe on a dir %s", writer.localPipeDir)
		return err
	}

	waiter := sync.WaitGroup{}
	waiter.Add(1)

	go func() {
		var ioErr error

		readBuffer := make([]byte, writer.writeSize)
		for {
			currentOffset := writer.currentWriteDataBuffer.startOffset + writer.currentWriteDataBuffer.pipeReader.GetReadedBytes()
			readLen, readErr := writer.currentWriteDataBuffer.pipeReader.Read(readBuffer)
			if readLen > 0 {
				_, writeErr := writer.baseWriter.WriteAt(readBuffer[:readLen], currentOffset)
				if writeErr != nil {
					logger.Error(writeErr)
					writer.addAsyncError(writeErr)
					ioErr = writeErr
					break
				}
			}

			if readErr != nil {
				if readErr == io.EOF {
					break
				} else {
					logger.Error(readErr)
					writer.addAsyncError(readErr)
					ioErr = readErr
					break
				}
			}
		}

		pipeReader.CloseWithError(ioErr)

		logger.Infof("Wrote a buffer - %s, offset %d", writer.path, writer.currentWriteDataBuffer.startOffset)

		waiter.Done()
	}()

	writer.currentWriteDataBuffer = &writeData{
		startOffset: offset,
		pipeReader:  pipeReader,
		pipeWriter:  pipeWriter,
		waiter:      &waiter,
	}

	return nil
}

func (writer *AsyncWriter) releaseDataBuffer() {
	if writer.currentWriteDataBuffer != nil {
		writer.currentWriteDataBuffer.pipeWriter.Close()
		writer.currentWriteDataBuffer.waiter.Wait()
		writer.currentWriteDataBuffer = nil
	}
}
