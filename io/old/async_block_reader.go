package old

/*
import (
	"container/list"
	"fmt"
	"io"
	"sync"

	"github.com/cyverse/irodsfs-common/io/cache"
	"github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-common/utils"
	"github.com/eikenb/pipeat"
	log "github.com/sirupsen/logrus"
)

const (
	// after this point, you can't stop reading the block
	allowedBlockReadStopRatio float32 = 0.7
	farFetchedBlockDistance   int64   = 3
	prefetchBlockReadRatio    float32 = 0.5
)

type readBlock struct {
	id                int64
	blockStartOffset  int64
	baseReader        Reader
	pipeReader        *pipeat.PipeReaderAt
	pipeWriter        *pipeat.PipeWriterAt
	waiter            *sync.WaitGroup
	terminated        bool
	prefetchTriggered bool // tell if prefetching the next block is triggered
}

// AsyncBlockReader helps read in block level
type AsyncBlockReader struct {
	fsClient        irods.IRODSFSClient
	path            string
	checksum        string // can be empty
	blockSize       int
	readSize        int
	blockHelper     *utils.FileBlockHelper
	localPipeDir    string
	prefetchEnabled bool

	readers          *list.List // Reader
	readerWaiter     *sync.Cond
	dataBlockMap     map[int64]*readBlock
	blockReaderMutex sync.Mutex // lock for blocks and readers

	cacheStore cache.CacheStore // can be null

	lastError error
	mutex     sync.Mutex
}

// NewAsyncBlockReader create a new AsyncBlockReader
// example sizes
// blockSize = 4MB
// readSize = 64KB
func NewAsyncBlockReader(reader Reader, blockSize int, readSize int, localPipeDir string) Reader {
	return NewAsyncBlockReaderWithCache([]Reader{reader}, blockSize, readSize, "", nil, localPipeDir)
}

// NewAsyncBlockReaderWithCache create a new AsyncBlockReader with cache
func NewAsyncBlockReaderWithCache(readers []Reader, blockSize int, readSize int, checksum string, cacheStore cache.CacheStore, localPipeDir string) Reader {
	blockHelper := utils.NewFileBlockHelper(blockSize)

	readerList := list.New()
	for _, reader := range readers {
		readerList.PushBack(reader)
	}

	prefetchEnabled := false
	if len(readers) > 1 {
		prefetchEnabled = true
	}

	reader := &AsyncBlockReader{
		fsClient:        readers[0].GetFSClient(),
		path:            readers[0].GetPath(),
		checksum:        checksum,
		blockSize:       blockSize,
		readSize:        readSize,
		blockHelper:     blockHelper,
		localPipeDir:    localPipeDir,
		prefetchEnabled: prefetchEnabled,

		readers:      readerList,
		dataBlockMap: map[int64]*readBlock{},

		cacheStore: cacheStore,

		lastError: nil,
	}

	reader.readerWaiter = sync.NewCond(&reader.blockReaderMutex)
	return reader
}

// Release releases all resources
func (reader *AsyncBlockReader) Release() {
	reader.releaseAllDataBlocks()
}

// AddReadersForPrefetching adds readers for prefetching
func (reader *AsyncBlockReader) AddReadersForPrefetching(readers []Reader) {
	reader.blockReaderMutex.Lock()
	defer reader.blockReaderMutex.Unlock()

	for _, r := range readers {
		reader.readers.PushBack(r)
	}

	if reader.readers.Len() > 1 {
		reader.prefetchEnabled = true
	}
}

// GetFSClient returns fs client
func (reader *AsyncBlockReader) GetFSClient() irods.IRODSFSClient {
	return reader.fsClient
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

	if len(buffer) <= 0 || offset < 0 {
		return 0, nil
	}

	logger.Debugf("Reading data - %s, offset %d, length %d", reader.path, offset, len(buffer))

	// any err
	err := reader.GetError()
	if err != nil {
		logger.WithError(err).Errorf("failed to read - %v", err)
		return 0, err
	}

	currentOffset := offset
	totalReadLen := 0
	for totalReadLen < len(buffer) {
		blockID := reader.blockHelper.GetBlockIDForOffset(currentOffset)

		logger.Debugf("downloading a data block %d", blockID)
		dataBlock, err := reader.getDataBlock(blockID)
		if err != nil {
			return totalReadLen, err
		}

		inBlockOffset := currentOffset - dataBlock.blockStartOffset
		readLen, err := dataBlock.pipeReader.ReadAt(buffer[totalReadLen:], inBlockOffset)
		if readLen > 0 {
			totalReadLen += readLen
		}

		logger.Debugf("current block pipe Written: %d, Read: %d", dataBlock.pipeWriter.GetWrittenBytes(), dataBlock.pipeReader.GetReadedBytes())

		// prefetch
		if reader.prefetchEnabled && !dataBlock.prefetchTriggered {
			prefetchStartInBlockOffset := int64(float32(reader.blockSize) * prefetchBlockReadRatio)
			if inBlockOffset > prefetchStartInBlockOffset {
				availableReaders := 0
				reader.blockReaderMutex.Lock()
				availableReaders = reader.readers.Len()
				reader.blockReaderMutex.Unlock()

				if availableReaders > 0 {
					// start prefetch
					logger.Debugf("prefetching a data block %d", blockID+1)
					reader.getDataBlock(blockID + 1)

					// mark prefetch is triggered
					dataBlock.prefetchTriggered = true
				}
			}
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

	// any error
	err = reader.GetError()
	if err != nil {
		logger.WithError(err).Errorf("failed to read - %v", err)
		return 0, err
	}

	return totalReadLen, nil
}

func (reader *AsyncBlockReader) GetAvailable(offset int64) int64 {
	if offset < 0 {
		return -1
	}

	reader.blockReaderMutex.Lock()
	defer reader.blockReaderMutex.Unlock()

	blockID := reader.blockHelper.GetBlockIDForOffset(offset)
	if dataBlock, ok := reader.dataBlockMap[blockID]; ok {
		// found!
		writtenLen := dataBlock.pipeWriter.GetWrittenBytes()
		inBlockOffset := offset - dataBlock.blockStartOffset
		avail := writtenLen - inBlockOffset
		if avail >= 0 {
			return avail
		}
		return -1
	}

	return -1
}

func (reader *AsyncBlockReader) GetError() error {
	reader.mutex.Lock()
	defer reader.mutex.Unlock()

	if reader.lastError != nil {
		return reader.lastError
	}
	return nil
}

func (reader *AsyncBlockReader) getDataBlock(blockID int64) (*readBlock, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncBlockReader",
		"function": "getDataBlock",
	})

	defer utils.StackTraceFromPanic(logger)

	reader.blockReaderMutex.Lock()

	// return
	if dataBlock, ok := reader.dataBlockMap[blockID]; ok {
		// found!
		reader.blockReaderMutex.Unlock()
		return dataBlock, nil
	}

	reader.blockReaderMutex.Unlock()

	//reader.releaseFarFetchedDataBlocks(blockID)

	reader.blockReaderMutex.Lock()

	for reader.readers.Len() == 0 {
		reader.readerWaiter.Wait()
	}

	// pop a baseReader first
	var baseReader Reader
	frontElem := reader.readers.Front()
	if frontElem != nil {
		frontElemObj := reader.readers.Remove(frontElem)
		if frontReader, ok := frontElemObj.(Reader); ok {
			baseReader = frontReader
		}
	}

	if baseReader == nil {
		return nil, fmt.Errorf("no reader is available")
	}

	reader.blockReaderMutex.Unlock()

	dataBlock, err := reader.newDataBlock(baseReader, blockID)
	if err != nil {
		return nil, err
	}

	return dataBlock, nil
}

func (reader *AsyncBlockReader) newDataBlock(baseReader Reader, blockID int64) (*readBlock, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "AsyncBlockReader",
		"function": "newDataBlock",
	})

	defer utils.StackTraceFromPanic(logger)

	logger.Debugf("Fetching a block - %s, block id %d", baseReader.GetPath(), blockID)

	blockStartOffset := reader.blockHelper.GetBlockStartOffset(blockID)

	pipeReader, pipeWriter, err := pipeat.AsyncWriterPipeInDir(reader.localPipeDir)
	if err != nil {
		logger.WithError(err).Errorf("failed to create a pipe on a dir %s", reader.localPipeDir)
		return nil, err
	}

	waiter := sync.WaitGroup{}
	waiter.Add(1)

	dataBlock := &readBlock{
		id:                blockID,
		blockStartOffset:  blockStartOffset,
		baseReader:        baseReader,
		pipeReader:        pipeReader,
		pipeWriter:        pipeWriter,
		waiter:            &waiter,
		terminated:        false,
		prefetchTriggered: false,
	}

	reader.blockReaderMutex.Lock()
	reader.dataBlockMap[blockID] = dataBlock
	reader.blockReaderMutex.Unlock()

	go func() {
		var ioErr error

		useCache := false
		if reader.cacheStore != nil && len(reader.checksum) > 0 {
			useCache = true
		}

		metrics := reader.fsClient.GetMetrics()

		// check cache if enabled
		if useCache {
			blockKey := reader.makeCacheEntryKey(blockID)
			cacheEntry := reader.cacheStore.GetEntry(blockKey)
			if cacheEntry != nil {
				// read from cache
				logger.Debugf("Read from cache - %s, block id %d", reader.path, blockID)

				if metrics != nil {
					metrics.IncreaseCounterForCacheHit(1)
				}

				_, readErr := cacheEntry.ReadData(pipeWriter, 0)
				if readErr != nil {
					logger.Error(readErr)
					reader.addAsyncError(readErr)
					ioErr = readErr
				}

				pipeWriter.CloseWithError(ioErr)

				// return reader
				reader.blockReaderMutex.Lock()
				reader.readers.PushBack(dataBlock.baseReader)
				reader.readerWaiter.Broadcast()
				reader.blockReaderMutex.Unlock()

				dataBlock.baseReader = nil
				dataBlock.terminated = true

				logger.Debugf("Fetched a block from cache - %s, block id %d", reader.path, blockID)
				waiter.Done()
				return
			}
		}

		if metrics != nil {
			metrics.IncreaseCounterForCacheMiss(1)
		}

		// can read data up to 4x of reader.readSize
		readMax := reader.readSize * 4

		readBuffer := make([]byte, readMax)
		var cacheBuffer []byte

		if useCache {
			cacheBuffer = make([]byte, reader.blockSize)
		}

		totalReadLen := 0
		terminated := false
		stoppableLenMax := int(float32(reader.blockSize) * allowedBlockReadStopRatio)

		for totalReadLen < reader.blockSize {
			if dataBlock.terminated && totalReadLen < stoppableLenMax {
				terminated = true
				break
			}

			currentOffset := blockStartOffset + int64(totalReadLen)
			toCopy := reader.blockSize - totalReadLen

			if toCopy > reader.readSize {
				toCopy = reader.readSize
			}

			// check available len to adjust read size
			avail := reader.GetAvailable(currentOffset)
			if avail > int64(reader.readSize) {
				// has more data!
				if avail > int64(reader.blockSize-totalReadLen) {
					// can't cross the current block
					toCopy = reader.blockSize - totalReadLen
				} else {
					toCopy = int(avail)
				}

				// can't exceed readMax
				if toCopy > readMax {
					toCopy = readMax
				}
			}

			readLen, readErr := baseReader.ReadAt(readBuffer[:toCopy], currentOffset)
			if readLen > 0 {
				_, writeErr := pipeWriter.Write(readBuffer[:readLen])
				if useCache {
					// copy to cacheBuffer
					copy(cacheBuffer[totalReadLen:], readBuffer[:readLen])
				}

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

		// return reader
		reader.blockReaderMutex.Lock()
		reader.readers.PushBack(dataBlock.baseReader)
		reader.readerWaiter.Broadcast()
		reader.blockReaderMutex.Unlock()

		dataBlock.baseReader = nil

		pipeWriter.CloseWithError(ioErr)

		dataBlock.terminated = true

		if terminated {
			logger.Debugf("Terminated fetching a block - %s, block id %d", reader.path, blockID)
		} else {
			logger.Debugf("Fetched a block - %s, block id %d", reader.path, blockID)

			// cache if it fetched a whole block content
			if useCache {
				// save cache asynchronously
				go func() {
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
				}()
			}
		}

		waiter.Done()
	}()

	return dataBlock, nil
}

func (reader *AsyncBlockReader) releaseAllDataBlocks() int {
	reader.blockReaderMutex.Lock()

	count := 0

	// terminate all first
	for _, dataBlock := range reader.dataBlockMap {
		dataBlock.terminated = true
	}

	// wait
	for _, dataBlock := range reader.dataBlockMap {
		reader.blockReaderMutex.Unlock()

		dataBlock.waiter.Wait()
		dataBlock.pipeReader.Close()

		reader.blockReaderMutex.Lock()
	}

	// delete
	for _, dataBlock := range reader.dataBlockMap {
		delete(reader.dataBlockMap, dataBlock.id)
		count++
	}

	reader.blockReaderMutex.Unlock()
	return count
}

func (reader *AsyncBlockReader) releaseFarFetchedDataBlocks(currentBlockID int64) int {
	terminatedBlockIDs := []int64{}

	reader.blockReaderMutex.Lock()

	// terminate all first
	for _, dataBlock := range reader.dataBlockMap {
		distance := currentBlockID - dataBlock.id
		if distance < 0 {
			distance *= -1
		}

		if distance >= farFetchedBlockDistance {
			dataBlock.terminated = true
			terminatedBlockIDs = append(terminatedBlockIDs, dataBlock.id)
		}
	}

	// wait and delete
	for _, blockID := range terminatedBlockIDs {
		dataBlock := reader.dataBlockMap[blockID]
		if dataBlock != nil {
			reader.blockReaderMutex.Unlock()

			dataBlock.waiter.Wait()
			dataBlock.pipeReader.Close()

			reader.blockReaderMutex.Lock()
			delete(reader.dataBlockMap, dataBlock.id)
		}
	}

	reader.blockReaderMutex.Unlock()
	return len(terminatedBlockIDs)
}

func (reader *AsyncBlockReader) makeCacheEntryKey(blockID int64) string {
	return fmt.Sprintf("%s:%s:%d", reader.path, reader.checksum, blockID)
}
*/
