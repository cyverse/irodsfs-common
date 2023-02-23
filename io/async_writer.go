package io

import (
	"bytes"
	"sync"

	"github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

type writeBlock struct {
	offset int64
	buffer *bytes.Buffer
}

// AsyncWriter helps async write
type AsyncWriter struct {
	baseWriter Writer
	fsClient   irods.IRODSFSClient
	path       string

	pendingWriteBlock     chan *writeBlock
	asyncWriteBlockWaiter sync.WaitGroup

	lastError error
	mutex     sync.Mutex
}

// NewAsyncWriter create a new AsyncWriter
func NewAsyncWriter(writer Writer) Writer {
	asyncWriter := &AsyncWriter{
		baseWriter: writer,
		fsClient:   writer.GetFSClient(),
		path:       writer.GetPath(),

		pendingWriteBlock:     make(chan *writeBlock, 10),
		asyncWriteBlockWaiter: sync.WaitGroup{},

		lastError: nil,
		mutex:     sync.Mutex{},
	}

	asyncWriter.startAsyncWriter()

	return asyncWriter
}

// Release releases all resources
func (writer *AsyncWriter) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncWriter",
		"function": "Release",
	})

	defer utils.StackTraceFromPanic(logger)

	writer.Flush()

	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	close(writer.pendingWriteBlock)

	if writer.baseWriter != nil {
		writer.baseWriter.Release()
		writer.baseWriter = nil
	}
}

// GetFSClient returns fs client
func (writer *AsyncWriter) GetFSClient() irods.IRODSFSClient {
	return writer.fsClient
}

// GetPath returns path
func (writer *AsyncWriter) GetPath() string {
	return writer.path
}

func (writer *AsyncWriter) startAsyncWriter() {
	go func() {
		for block := range writer.pendingWriteBlock {
			writer.mutex.Lock()
			if writer.lastError != nil {
				// skip
				writer.mutex.Unlock()
				writer.asyncWriteBlockWaiter.Done()
				continue
			}
			writer.mutex.Unlock()

			if block.buffer.Len() > 0 {
				bufferData := block.buffer.Bytes()
				_, err := writer.baseWriter.WriteAt(bufferData, block.offset)
				if err != nil {
					writer.mutex.Lock()
					writer.lastError = xerrors.Errorf("failed to write data to %s, offset %d, length %d: %w", writer.path, block.offset, block.buffer.Len(), err)
					writer.mutex.Unlock()
				}
			}

			writer.asyncWriteBlockWaiter.Done()
		}
	}()
}

// Flush flushes buffered data
func (writer *AsyncWriter) Flush() error {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncWriter",
		"function": "Flush",
	})

	defer utils.StackTraceFromPanic(logger)

	// wait until process all pending write blocks
	writer.asyncWriteBlockWaiter.Wait()

	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	err := writer.baseWriter.Flush()
	if err != nil {
		return err
	}

	return writer.lastError
}

// Write writes data
func (writer *AsyncWriter) WriteAt(data []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncWriter",
		"function": "WriteAt",
	})

	defer utils.StackTraceFromPanic(logger)

	if len(data) == 0 || offset < 0 {
		return 0, nil
	}

	writer.mutex.Lock()

	if writer.lastError != nil {
		writer.mutex.Unlock()
		return 0, xerrors.Errorf("failed to schedule writing data to %s, offset %d, length %d: %w", writer.path, offset, len(data), writer.lastError)
	}
	writer.mutex.Unlock()

	block := writeBlock{
		offset: offset,
		buffer: &bytes.Buffer{},
	}

	block.buffer.Write(data)

	logger.Debugf("adding to write queue, off %d", offset)
	writer.asyncWriteBlockWaiter.Add(1)
	writer.pendingWriteBlock <- &block
	logger.Debugf("added to write queue, off %d", offset)

	// do it again
	writer.mutex.Lock()
	if writer.lastError != nil {
		writer.mutex.Unlock()
		return 0, xerrors.Errorf("failed to schedule writing data to %s, offset %d, length %d: %w", writer.path, offset, len(data), writer.lastError)
	}
	writer.mutex.Unlock()

	return len(data), nil
}

// GetError returns error
func (writer *AsyncWriter) GetError() error {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	return writer.lastError
}
