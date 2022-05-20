package io

import (
	"fmt"
	"io"
	"sync"

	"github.com/cyverse/irodsfs-common/io/cache"
	"github.com/cyverse/irodsfs-common/utils"
	"github.com/eikenb/pipeat"
	log "github.com/sirupsen/logrus"
)

type readDataBlock struct {
	id               int64
	blockStartOffset int64
	blockSize        int
	pipeReader       *pipeat.PipeReaderAt
	pipeWriter       *pipeat.PipeWriterAt
	waiter           *sync.WaitGroup
}

// AsyncBlockReader helps read in block level
type AsyncBlockReader struct {
	path     string
	checksum string // can be empty

	baseReader   Reader
	blockSize    int
	readSize     int
	blockHelper  *utils.FileBlockHelper
	localPipeDir string

	currentReadDataBlock *readDataBlock
	mutex                sync.Mutex // lock for current block data

	cacheStore cache.CacheStore // can be null

	pendingErrors      []error
	pendingErrorsMutex sync.Mutex
}

// NewAsyncBlockReader create a new AsyncBlockReader
// example sizes
// blockSize = 4MB
// readSize = 64KB
func NewAsyncBlockReader(reader Reader, blockSize int, readSize int, localPipeDir string) Reader {
	return NewAsyncBlockReaderWithCache(reader, blockSize, readSize, "", nil, localPipeDir)
}

// NewAsyncBlockReaderWithCache create a new AsyncBlockReader with cache
func NewAsyncBlockReaderWithCache(reader Reader, blockSize int, readSize int, checksum string, cacheStore cache.CacheStore, localPipeDir string) Reader {
	blockHelper := utils.NewFileBlockHelper(blockSize)

	return &AsyncBlockReader{
		path:     reader.GetPath(),
		checksum: checksum,

		baseReader:   reader,
		blockSize:    blockSize,
		readSize:     readSize,
		blockHelper:  blockHelper,
		localPipeDir: localPipeDir,

		currentReadDataBlock: nil,

		cacheStore: cacheStore,

		pendingErrors: []error{},
	}
}

// Release releases all resources
func (reader *AsyncBlockReader) Release() {
	reader.mutex.Lock()
	defer reader.mutex.Unlock()

	reader.unloadDataBlock()
}

// GetPath returns path of the file
func (reader *AsyncBlockReader) GetPath() string {
	return reader.path
}

// ReadAt reads data
func (reader *AsyncBlockReader) ReadAt(buffer []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncBlockReader",
		"function": "ReadAt",
	})

	defer utils.StackTraceFromPanic(logger)

	reader.mutex.Lock()
	defer reader.mutex.Unlock()

	if len(buffer) <= 0 || offset < 0 {
		return 0, nil
	}

	logger.Infof("Reading data - %s, offset %d, length %d", reader.path, offset, len(buffer))

	// any pending
	err := reader.GetPendingError()
	if err != nil {
		logger.WithError(err).Errorf("failed to read - %v", err)
		return 0, err
	}

	currentOffset := offset
	totalReadLen := 0
	for totalReadLen < len(buffer) {
		blockID := reader.blockHelper.GetBlockIDForOffset(currentOffset)
		err := reader.loadDataBlock(blockID)
		if err != nil {
			return totalReadLen, err
		}

		inBlockOffset := currentOffset - reader.currentReadDataBlock.blockStartOffset
		readLen, err := reader.currentReadDataBlock.pipeReader.ReadAt(buffer[totalReadLen:], inBlockOffset)
		if readLen > 0 {
			totalReadLen += readLen
		}

		if err != nil {
			if err == io.EOF {
				if inBlockOffset+int64(readLen) < int64(reader.blockSize) {
					// if it's not the end of block
					// real EOF
					return totalReadLen, io.EOF
				}
			} else {
				return totalReadLen, err
			}
		}
	}

	// any pending
	err = reader.GetPendingError()
	if err != nil {
		logger.WithError(err).Errorf("failed to read - %v", err)
		return 0, err
	}

	return totalReadLen, nil
}

func (reader *AsyncBlockReader) GetPendingError() error {
	reader.pendingErrorsMutex.Lock()
	defer reader.pendingErrorsMutex.Unlock()

	if len(reader.pendingErrors) > 0 {
		return reader.pendingErrors[0]
	}
	return nil
}

func (reader *AsyncBlockReader) addAsyncError(err error) {
	reader.pendingErrorsMutex.Lock()
	defer reader.pendingErrorsMutex.Unlock()

	reader.pendingErrors = append(reader.pendingErrors, err)
}

func (reader *AsyncBlockReader) loadDataBlock(blockID int64) error {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncBlockReader",
		"function": "loadDataBlock",
	})

	defer utils.StackTraceFromPanic(logger)

	// unload first
	if reader.currentReadDataBlock != nil {
		if reader.currentReadDataBlock.id != blockID {
			reader.unloadDataBlock()
		} else {
			// it's already loaded
			return nil
		}
	}

	logger.Infof("Fetching a block - %s, block id %d", reader.path, blockID)

	blockStartOffset := reader.blockHelper.GetBlockStartOffset(blockID)

	pipeReader, pipeWriter, err := pipeat.PipeInDir(reader.localPipeDir)
	if err != nil {
		logger.WithError(err).Errorf("failed to create a pipe on a dir %s", reader.localPipeDir)
		return err
	}

	waiter := sync.WaitGroup{}
	waiter.Add(1)

	go func() {
		var ioErr error

		useCache := false
		if reader.cacheStore != nil && len(reader.checksum) > 0 {
			useCache = true
		}

		// check cache if enabled
		if useCache {
			blockKey := reader.makeCacheEntryKey(blockID)
			cacheEntry := reader.cacheStore.GetEntry(blockKey)

			if cacheEntry != nil {
				// read from cache
				logger.Infof("Read from cache - %s, block id %d", reader.path, blockID)
				cacheBuffer := make([]byte, reader.blockSize)

				readLen, readErr := cacheEntry.GetData(cacheBuffer[:reader.blockSize], 0)
				if readLen > 0 {
					_, writeErr := pipeWriter.Write(cacheBuffer[:readLen])
					if writeErr != nil {
						logger.Error(writeErr)
						reader.addAsyncError(writeErr)
						ioErr = writeErr
					}
				}

				if readErr != nil {
					logger.Error(readErr)
					reader.addAsyncError(readErr)
					ioErr = readErr
				}

				pipeWriter.CloseWithError(ioErr)

				logger.Infof("Fetched a block from cache - %s, block id %d", reader.path, blockID)
				waiter.Done()
				return
			}
		}

		readBuffer := make([]byte, reader.readSize)
		var cacheBuffer []byte

		if useCache {
			cacheBuffer = make([]byte, reader.blockSize)
		}

		totalReadLen := 0
		for totalReadLen < reader.blockSize {
			currentOffset := blockStartOffset + int64(totalReadLen)
			toCopy := reader.blockSize - totalReadLen
			if toCopy > len(readBuffer) {
				toCopy = len(readBuffer)
			}

			readLen, readErr := reader.baseReader.ReadAt(readBuffer[:toCopy], currentOffset)
			if readLen > 0 {
				_, writeErr := pipeWriter.Write(readBuffer[:readLen])
				totalReadLen += readLen

				if writeErr != nil {
					logger.Error(writeErr)
					reader.addAsyncError(writeErr)
					ioErr = writeErr
					break
				}
			} else {
				break
			}

			if readErr != nil {
				if readErr == io.EOF {
					break
				} else {
					logger.Error(readErr)
					reader.addAsyncError(readErr)
					ioErr = readErr
					break
				}
			}
		}

		pipeWriter.CloseWithError(ioErr)

		logger.Infof("Fetched a block - %s, block id %d", reader.path, blockID)

		// cache
		if useCache {
			blockKey := reader.makeCacheEntryKey(blockID)

			_, cacheErr := reader.cacheStore.CreateEntry(blockKey, reader.path, cacheBuffer[:totalReadLen])
			if cacheErr != nil {
				logger.Error(cacheErr)
			} else {
				if totalReadLen == reader.blockSize && ioErr == io.EOF {
					// EOF
					// save another cache block for EOF
					eofBlockKey := reader.makeCacheEntryKey(blockID + 1)
					_, cacheErr = reader.cacheStore.CreateEntry(eofBlockKey, reader.path, cacheBuffer[:0])
					if cacheErr != nil {
						// just log
						logger.Error(err)
					}
				}
			}
		}

		waiter.Done()
	}()

	reader.currentReadDataBlock = &readDataBlock{
		id:               blockID,
		blockStartOffset: blockStartOffset,
		blockSize:        reader.blockSize,
		pipeReader:       pipeReader,
		pipeWriter:       pipeWriter,
		waiter:           &waiter,
	}

	return nil
}

func (reader *AsyncBlockReader) unloadDataBlock() {
	if reader.currentReadDataBlock != nil {
		reader.currentReadDataBlock.waiter.Wait()
		reader.currentReadDataBlock.pipeReader.Close()
		reader.currentReadDataBlock = nil
	}
}

func (reader *AsyncBlockReader) makeCacheEntryKey(blockID int64) string {
	return fmt.Sprintf("%s:%s:%d", reader.path, reader.checksum, blockID)
}
