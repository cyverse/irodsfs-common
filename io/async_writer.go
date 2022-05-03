package io

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-common/report"
	"github.com/cyverse/irodsfs-common/utils"
	"github.com/eapache/channels"
	log "github.com/sirupsen/logrus"
)

// AsyncWriter helps async write
type AsyncWriter struct {
	path       string
	fileHandle irods.IRODSFSFileHandle

	buffer               Buffer
	bufferEntryGroupName string

	writeWaitTasks sync.WaitGroup
	writeQueue     channels.Channel

	pendingErrors      []error
	pendingErrorsMutex sync.Mutex

	reportClient report.IRODSFSInstanceReportClient
}

// NewAsyncWriter create a new AsyncWriter
func NewAsyncWriter(fileHandle irods.IRODSFSFileHandle, writeBuffer Buffer, reportClient report.IRODSFSInstanceReportClient) *AsyncWriter {
	entry := fileHandle.GetEntry()

	asyncWriter := &AsyncWriter{
		path:       entry.Path,
		fileHandle: fileHandle,

		buffer:               writeBuffer,
		bufferEntryGroupName: fmt.Sprintf("write:%s:%s", fileHandle.GetID(), entry.Path),

		writeWaitTasks: sync.WaitGroup{},
		writeQueue:     channels.NewInfiniteChannel(),
		pendingErrors:  []error{},

		reportClient: reportClient,
	}

	writeBuffer.CreateEntryGroup(asyncWriter.bufferEntryGroupName)

	go asyncWriter.backgroundWriteTask()

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

	if writer.buffer != nil {
		writer.buffer.DeleteEntryGroup(writer.bufferEntryGroupName)
	}

	writer.writeQueue.Close()
}

// GetPath returns path
func (writer *AsyncWriter) GetPath() string {
	return writer.path
}

func (writer *AsyncWriter) getBufferEntryGroup() BufferEntryGroup {
	return writer.buffer.GetEntryGroup(writer.bufferEntryGroupName)
}

func (writer *AsyncWriter) getBufferEntryKey(offset int64) string {
	return fmt.Sprintf("%d", offset)
}

func (writer *AsyncWriter) getBufferEntryOffset(key string) (int64, error) {
	return strconv.ParseInt(key, 10, 64)
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

	entryKey := writer.getBufferEntryKey(offset)
	entryGroup := writer.getBufferEntryGroup()

	_, err := entryGroup.CreateEntry(entryKey, data)
	if err != nil {
		logger.WithError(err).Errorf("failed to put an entry to buffer - %s, %s", writer.bufferEntryGroupName, entryKey)
		return 0, err
	}

	// schedule background write
	writer.writeWaitTasks.Add(1)
	writer.writeQueue.In() <- entryKey

	// any pending
	err = writer.GetPendingError()
	if err != nil {
		logger.WithError(err).Errorf("failed to write - %s, %v", writer.bufferEntryGroupName, err)
		return 0, err
	}

	return len(data), nil
}

// Flush flushes buffered data
func (writer *AsyncWriter) Flush() error {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncWriter",
		"function": "Flush",
	})

	defer utils.StackTraceFromPanic(logger)

	// wait until all queued tasks complete
	writer.waitForBackgroundWrites()

	// any pending
	err := writer.GetPendingError()
	if err != nil {
		logger.WithError(err).Errorf("failed to write - %s, %v", writer.bufferEntryGroupName, err)
		return err
	}

	return writer.fileHandle.Flush()

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

func (writer *AsyncWriter) waitForBackgroundWrites() {
	writer.writeWaitTasks.Wait()
}

func (writer *AsyncWriter) backgroundWriteTask() {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncWriter",
		"function": "backgroundWriteTask",
	})

	defer utils.StackTraceFromPanic(logger)

	entryGroup := writer.getBufferEntryGroup()

	for {
		outData, channelOpened := <-writer.writeQueue.Out()
		if !channelOpened {
			// channel is closed
			return
		}

		if outData != nil {
			key := outData.(string)

			offset, err := writer.getBufferEntryOffset(key)
			if err != nil {
				logger.WithError(err).Errorf("failed to get entry offset - %s, %s", writer.bufferEntryGroupName, key)
				writer.addAsyncError(err)
				continue
			}

			entry := entryGroup.PopEntry(key)
			if entry == nil {
				err = fmt.Errorf("failed to get an entry - %s, %s", writer.bufferEntryGroupName, key)
				logger.Error(err)
				writer.addAsyncError(err)
				continue
			}

			data := entry.GetData()
			if len(data) != entry.GetSize() && len(data) <= 0 {
				err = fmt.Errorf("failed to get data - %s, %s", writer.bufferEntryGroupName, key)
				logger.Error(err)
				writer.addAsyncError(err)
				continue
			}

			logger.Infof("Async Writing - %s, Offset %d, length %d", writer.path, offset, len(data))

			_, err = writer.fileHandle.WriteAt(data, offset)
			if err != nil {
				logger.WithError(err).Errorf("failed to write data - %s, %d, %d", writer.path, offset, len(data))
				writer.addAsyncError(err)
				continue
			}

			// Report
			if writer.reportClient != nil {
				writer.reportClient.FileAccess(writer.fileHandle, offset, int64(len(data)))
			}

			writer.writeWaitTasks.Done()
		}
	}
}
