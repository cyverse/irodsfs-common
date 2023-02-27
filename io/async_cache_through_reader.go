package io

import (
	"io"
	"sync"

	"github.com/cyverse/irodsfs-common/io/cache"
	"github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

const (
	readBufferSize int = 128 * 1024
)

// AsyncCacheThroughReader helps sync read through cache
type AsyncCacheThroughReader struct {
	baseReaders          []Reader
	availableBaseReaders chan Reader
	fsClient             irods.IRODSFSClient
	path                 string
	checksum             string // can be empty
	size                 int64

	blockHelper *utils.FileBlockHelper
	blockStore  *FileBlockStore
	transferMap *FileBlockTransferMap
	prefetcher  *Prefetcher

	blockRequests     chan *FileBlockTransfer
	asyncReaderWaiter sync.WaitGroup

	lastError error
	terminate bool
	mutex     sync.Mutex
}

func NewAsyncReader(readers []Reader, blockSize int) (Reader, error) {
	return NewAsyncCacheThroughReader(readers, blockSize, nil)
}

// NewAsyncCacheThroughReader create a new AsyncCacheThroughReader
func NewAsyncCacheThroughReader(readers []Reader, blockSize int, cacheStore cache.CacheStore) (Reader, error) {
	asyncReader := &AsyncCacheThroughReader{
		baseReaders:          readers,
		availableBaseReaders: make(chan Reader, 10),
		fsClient:             readers[0].GetFSClient(),
		path:                 readers[0].GetPath(),
		checksum:             readers[0].GetChecksum(),
		size:                 readers[0].GetSize(),

		blockHelper: utils.NewFileBlockHelper(blockSize),
		blockStore:  nil,
		transferMap: NewFileBlockTransferMap(),
		prefetcher:  nil,

		blockRequests:     make(chan *FileBlockTransfer, 5),
		asyncReaderWaiter: sync.WaitGroup{},

		lastError: nil,
		terminate: false,
		mutex:     sync.Mutex{},
	}

	blockStore, err := NewFileBlockStore(cacheStore, asyncReader.path, asyncReader.checksum, blockSize)
	if err != nil {
		return nil, err
	}

	asyncReader.blockStore = blockStore

	for _, reader := range readers {
		asyncReader.availableBaseReaders <- reader
	}

	if len(readers) > 1 {
		asyncReader.prefetcher = NewPrefetcher(blockSize)
	}

	go asyncReader.asyncRequestHandler()
	asyncReader.asyncReaderWaiter.Add(1)

	return asyncReader, nil
}

// Release releases all resources
func (reader *AsyncCacheThroughReader) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncCacheThroughReader",
		"function": "Release",
	})

	defer utils.StackTraceFromPanic(logger)

	reader.mutex.Lock()
	reader.terminate = true
	reader.mutex.Unlock()

	close(reader.blockRequests)
	close(reader.availableBaseReaders)

	reader.transferMap.StopAllTransfers()

	// wait until async reads complete
	reader.asyncReaderWaiter.Wait()

	// clear cache only in RAM
	reader.blockStore.Release()

	for _, baseReader := range reader.baseReaders {
		baseReader.Release()
	}
}

// GetFSClient returns fs client
func (reader *AsyncCacheThroughReader) AddReadersForPrefetching(readers []Reader) {
	if len(reader.baseReaders) == 1 {
		reader.prefetcher = NewPrefetcher(reader.blockStore.GetBlockSize())
	}

	reader.baseReaders = append(reader.baseReaders, readers...)

	for _, r := range readers {
		reader.availableBaseReaders <- r
	}
}

// GetFSClient returns fs client
func (reader *AsyncCacheThroughReader) GetFSClient() irods.IRODSFSClient {
	return reader.fsClient
}

// GetPath returns path of the file
func (reader *AsyncCacheThroughReader) GetPath() string {
	return reader.path
}

// GetChecksum returns checksum of the file
func (reader *AsyncCacheThroughReader) GetChecksum() string {
	return reader.checksum
}

// GetSize returns size of the file
func (reader *AsyncCacheThroughReader) GetSize() int64 {
	return reader.size
}

// ReadAt reads data
func (reader *AsyncCacheThroughReader) ReadAt(buffer []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncCacheThroughReader",
		"function": "ReadAt",
	})

	defer utils.StackTraceFromPanic(logger)

	if len(buffer) <= 0 || offset < 0 {
		return 0, nil
	}

	logger.Debugf("Async reading through cache - %s, offset %d, length %d", reader.path, offset, len(buffer))

	defer reader.checkAndTriggerPrefetch(offset)

	bufferLen := len(buffer)
	totalReadLen := 0
	curOffset := offset
	blockSize := reader.blockHelper.GetBlockSize()

	for totalReadLen < bufferLen {
		blockID := reader.blockHelper.GetBlockIDForOffset(curOffset)
		blockStartOffset := reader.blockHelper.GetBlockStartOffset(blockID)
		inBlockOffset := curOffset - blockStartOffset

		logger.Debugf("Async reading through cache - block %d", blockID)
		if reader.blockStore != nil {
			logger.Debugf("check block %d in block store", blockID)
			block := reader.blockStore.Get(blockID)
			if block != nil {
				logger.Debugf("read block %d from block store", blockID)
				// read from cache
				blockData := block.buffer.Bytes()
				copiedLen := copy(buffer[totalReadLen:], blockData[inBlockOffset:])
				if copiedLen > 0 {
					curOffset += int64(copiedLen)
					totalReadLen += copiedLen
				}

				if inBlockOffset+int64(copiedLen) == int64(block.buffer.Len()) {
					// read block fully
					if block.eof {
						return totalReadLen, io.EOF
					}
				}

				continue
			}
		}

		// failed to read from block store
		// read from base
		blockReadLen := blockSize - int(inBlockOffset)
		bufferLeftLen := bufferLen - totalReadLen

		readLenFromBase := blockReadLen
		if readLenFromBase > bufferLeftLen {
			readLenFromBase = bufferLeftLen
		}

		readLen, err := reader.readAtBase(buffer[totalReadLen:totalReadLen+readLenFromBase], curOffset)
		if readLen > 0 {
			curOffset += int64(readLen)
			totalReadLen += readLen
		}

		if err != nil {
			// err may be EOF
			if err == io.EOF {
				return int(curOffset - offset), err
			}
			return int(curOffset - offset), err
		}
	}

	return int(curOffset - offset), nil
}

// aligns to the block boundary
func (reader *AsyncCacheThroughReader) readAtBase(buffer []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncCacheThroughReader",
		"function": "readAtBase",
	})

	defer utils.StackTraceFromPanic(logger)

	if len(buffer) <= 0 || offset < 0 {
		return 0, nil
	}

	logger.Debugf("reading  - %s, offset %d, length %d", reader.path, offset, len(buffer))

	bufferLen := len(buffer)

	blockID := reader.blockHelper.GetBlockIDForOffset(offset)
	blockStartOffset := reader.blockHelper.GetBlockStartOffset(blockID)
	inBlockOffset := int(offset - blockStartOffset)

	logger.Debugf("scheduling a new transfer - block %d", blockID)
	transfer := reader.transferMap.Get(blockID)
	if transfer == nil {
		// schedule
		transfer = reader.scheduleBlockTransfer(blockID)
	}

	if transfer == nil {
		return 0, xerrors.Errorf("failed to schedule block %d", blockID)
	}

	// wait for read
	logger.Debugf("waiting for data - offset %d", inBlockOffset+bufferLen)
	ok := transfer.WaitForData(inBlockOffset + bufferLen)
	if !ok {
		// read failed
		return 0, xerrors.Errorf("failed to read block %d, transfer failed", blockID)
	}

	logger.Debugf("reading from transfer - block %d", blockID)
	copiedLen, err := transfer.CopyTo(buffer, inBlockOffset)
	logger.Debugf("read from transfer - block %d, len %d, eof %t", blockID, copiedLen, err == io.EOF)
	if err != nil && err != io.EOF {
		return copiedLen, err
	}

	// may return io.EOF
	return copiedLen, err
}

// GetAvailable returns available data len
func (reader *AsyncCacheThroughReader) GetAvailable(offset int64) int64 {
	blockID := reader.blockHelper.GetBlockIDForOffset(offset)
	blockStartOffset := reader.blockHelper.GetBlockStartOffset(blockID)
	inBlockOffset := offset - blockStartOffset

	if reader.blockStore.Contains(blockID) {
		return int64(reader.blockStore.GetBlockSize()) - inBlockOffset
	}

	// get from base
	return reader.getAvailableBase(offset)
}

func (reader *AsyncCacheThroughReader) getAvailableBase(offset int64) int64 {
	blockID := reader.blockHelper.GetBlockIDForOffset(offset)
	blockStartOffset := reader.blockHelper.GetBlockStartOffset(blockID)
	inBlockOffset := int(offset - blockStartOffset)

	transfer := reader.transferMap.Get(blockID)
	if transfer == nil {
		return -1
	}

	bufferLen := transfer.GetBufferLen()
	return int64(bufferLen) - int64(inBlockOffset)
}

func (reader *AsyncCacheThroughReader) GetError() error {
	reader.mutex.Lock()
	defer reader.mutex.Unlock()

	return reader.lastError
}

func (reader *AsyncCacheThroughReader) scheduleBlockTransfer(blockID int64) *FileBlockTransfer {
	reader.mutex.Lock()
	if reader.terminate {
		reader.mutex.Unlock()
		return nil
	}
	reader.mutex.Unlock()

	transfer := NewFileBlockTransfer(blockID)
	reader.transferMap.Put(transfer)

	reader.blockRequests <- transfer
	return transfer
}

func (reader *AsyncCacheThroughReader) checkAndTriggerPrefetch(offset int64) {
	if reader.prefetcher == nil {
		return
	}

	prefetchBlockIDs := reader.prefetcher.Determine(offset, reader.size)
	for _, prefetchBlockID := range prefetchBlockIDs {
		// trigger
		if reader.transferMap.Contains(prefetchBlockID) {
			continue
		}

		if reader.blockStore.Contains(prefetchBlockID) {
			continue
		}

		// block does not exist in cache / transfer map
		reader.scheduleBlockTransfer(prefetchBlockID)
	}
}

func (reader *AsyncCacheThroughReader) asyncRequestHandler() {
	for transfer := range reader.blockRequests {
		reader.mutex.Lock()
		terminate := reader.terminate
		reader.mutex.Unlock()

		if terminate {
			break
		}

		availableReader, ok := <-reader.availableBaseReaders
		if !ok {
			break
		}

		reader.startAsyncTransfer(transfer, availableReader)
	}

	reader.asyncReaderWaiter.Done()
}

func (reader *AsyncCacheThroughReader) startAsyncTransfer(transfer *FileBlockTransfer, baseReader Reader) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncCacheThroughReader",
		"function": "startAsyncTransfer",
	})

	defer utils.StackTraceFromPanic(logger)

	go func(r *AsyncCacheThroughReader, t *FileBlockTransfer, br Reader) {
		blockSize := r.blockHelper.GetBlockSize()
		blockID := t.GetBlockID()
		blockStartOffset := r.blockHelper.GetBlockStartOffset(blockID)

		defer func() {
			r.transferMap.Remove(blockID)

			r.mutex.Lock()
			terminate := r.terminate
			r.mutex.Unlock()

			if !terminate {
				r.availableBaseReaders <- br
			}
		}()

		totalReadLen := 0

		buffer := make([]byte, readBufferSize)

		logger.Debugf("block %d transfer start", blockID)

		eof := false
		curOffset := blockStartOffset
		for totalReadLen < blockSize {
			if t.IsFailed() {
				return
			}

			readLen, err := br.ReadAt(buffer, curOffset)
			if readLen > 0 {
				t.Write(buffer[:readLen])
				totalReadLen += readLen
				curOffset += int64(readLen)
			}

			if err != nil {
				if err == io.EOF {
					eof = true
					break
				}

				t.MarkFailed()
				r.mutex.Lock()
				r.lastError = err
				r.mutex.Unlock()
				return
			}
		}

		t.MarkCompleted(eof)

		logger.Debugf("block %d transfer done", blockID)

		fileBlock := NewFileBlockWithBuffer(blockID, t.GetBuffer(), t.IsEOF())

		err := r.blockStore.Put(fileBlock)
		if err != nil {
			r.mutex.Lock()
			r.lastError = err
			r.mutex.Unlock()
			return
		}
		logger.Debugf("block %d cached", blockID)
	}(reader, transfer, baseReader)
}
