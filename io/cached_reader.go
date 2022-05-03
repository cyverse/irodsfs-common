package io

import (
	"fmt"
	"io"
	"sync"

	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
)

// CachedReader helps read through cache
type CachedReader struct {
	path     string
	checksum string

	cacheStore  CacheStore
	reader      Reader
	blockHelper *utils.FileBlockHelper
}

const (
	cachedReaderBlockSize int = 1024 * 1024 // 1MB
)

// NewCachedReader create a new CachedReader
func NewCachedReader(checksum string, cacheStore CacheStore, reader Reader) *CachedReader {
	cacheReader := &CachedReader{
		path:     reader.GetPath(),
		checksum: checksum,

		cacheStore:  cacheStore,
		reader:      reader,
		blockHelper: utils.NewFileBlockHelper(cachedReaderBlockSize),
	}

	return cacheReader
}

// Release releases all resources
func (reader *CachedReader) Release() {
	if reader.cacheStore != nil {
		// there can be multiple readers for the same path
		//reader.Cache.DeleteAllEntriesForGroup(reader.Path)
		reader.cacheStore = nil
	}

	if reader.reader != nil {
		reader.reader.Release()
		reader.reader = nil
	}
}

// GetPath returns path of the file
func (reader *CachedReader) GetPath() string {
	return reader.path
}

func (reader *CachedReader) getCacheEntryKey(blockID int64) string {
	return fmt.Sprintf("%s:%s:%d", reader.path, reader.checksum, blockID)
}

// ReadAt reads data
func (reader *CachedReader) ReadAt(buffer []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "CacheReader",
		"function": "ReadAt",
	})

	if len(buffer) <= 0 || offset < 0 {
		return 0, nil
	}

	logger.Infof("Reading through cache - %s, offset %d, length %d", reader.path, offset, len(buffer))

	blockSize := reader.blockHelper.GetBlockSize()
	blockIDs := reader.blockHelper.GetBlockIDs(offset, len(buffer))

	mutex := sync.Mutex{}
	totalReadLen := 0
	eof := false
	errs := []error{}

	wait := sync.WaitGroup{}
	for _, blockID := range blockIDs {
		wait.Add(1)
		go func(blockID int64) {
			blockReadOffset, _ := reader.blockHelper.GetBlockRange(offset, len(buffer), blockID)
			blockStartOffset := reader.blockHelper.GetBlockStartOffset(blockID)

			if blockStartOffset != blockReadOffset {
				// need to allocate a new buffer
				newBuffer := make([]byte, blockSize)
				readLen, err := reader.ReadBlock(newBuffer, blockID)
				if err != nil && err != io.EOF {
					mutex.Lock()
					errs = append(errs, err)
					mutex.Unlock()

					wait.Done()
					return
				}

				// copy to buffer
				newBufferOffset := blockReadOffset - blockStartOffset
				copyLen := copy(buffer[blockReadOffset-offset:], newBuffer[newBufferOffset:newBufferOffset+int64(readLen)])

				mutex.Lock()
				totalReadLen += copyLen
				if err == io.EOF {
					eof = true
				}
				mutex.Unlock()

				wait.Done()
				return
			}

			// use buffer directly
			readLen, err := reader.ReadBlock(buffer[blockReadOffset-offset:], blockID)
			if err != nil && err != io.EOF {
				mutex.Lock()
				errs = append(errs, err)
				mutex.Unlock()

				wait.Done()
				return
			}

			mutex.Lock()
			totalReadLen += readLen
			if err == io.EOF {
				eof = true
			}
			mutex.Unlock()

			wait.Done()
		}(blockID)
	}

	wait.Wait()

	if len(errs) > 0 {
		return 0, errs[0]
	}

	if eof {
		return totalReadLen, io.EOF
	}
	return totalReadLen, nil
}

// readBlock reads block data
func (reader *CachedReader) ReadBlock(buffer []byte, blockID int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "CacheReader",
		"function": "ReadBlock",
	})

	if len(buffer) <= 0 || blockID < 0 {
		return 0, nil
	}

	logger.Infof("Reading %s, block %d", reader.path, blockID)

	blockKey := reader.getCacheEntryKey(blockID)
	blockSize := reader.blockHelper.GetBlockSize()

	cacheEntry := reader.cacheStore.GetEntry(blockKey)
	if cacheEntry == nil {
		// read from remote, through cache
		logger.Infof("cache for block %d not found - read from remote", blockID)

		blockOffset := reader.blockHelper.GetBlockStartOffset(blockID)

		readBuffer := buffer
		allocatedNewBuffer := false
		if len(buffer) < blockSize {
			// allocate a new buffer
			readBuffer = make([]byte, blockSize)
			allocatedNewBuffer = true
		}

		blockDataLen, err := reader.reader.ReadAt(readBuffer, blockOffset)
		if err != nil && err != io.EOF {
			return 0, err
		}

		// save to cache
		_, cacheErr := reader.cacheStore.CreateEntry(blockKey, reader.path, readBuffer[:blockDataLen])
		if cacheErr != nil {
			// just log
			logger.Error(err)
		}

		if allocatedNewBuffer {
			// copy back to buffer
			// may return EOF as well
			readLen := copy(buffer, readBuffer)
			return readLen, err
		}

		// may return EOF as well
		return blockDataLen, err
	}

	// read from cache
	cacheDataLen, err := cacheEntry.GetData(buffer)
	if err != nil {
		return 0, err
	}

	if cacheDataLen < blockSize {
		// EOF
		return cacheDataLen, io.EOF
	}
	return cacheDataLen, nil
}

func (reader *CachedReader) GetPendingError() error {
	return nil
}
