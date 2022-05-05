package io

import (
	"fmt"
	"io"

	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
)

// CachedReader helps read through cache
type CachedReader struct {
	path     string
	checksum string

	cacheStore CacheStore
	reader     Reader
}

// NewCachedReader create a new CachedReader
func NewCachedReader(checksum string, cacheStore CacheStore, reader Reader, blockSize int) Reader {
	cacheReader := &CachedReader{
		path:     reader.GetPath(),
		checksum: checksum,

		cacheStore: cacheStore,
	}

	cachedBlockFetcher := func(baseReader Reader, buffer []byte, blockID int64, blockSize int) (int, error) {
		return cacheReader.blockFetcher(baseReader, buffer, blockID, blockSize)
	}

	blockReader := NewBlockReader(reader, blockSize, cachedBlockFetcher)
	cacheReader.reader = blockReader

	return cacheReader
}

// Release releases all resources
func (reader *CachedReader) Release() {
	if reader.cacheStore != nil {
		// there can be multiple readers for the same path
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
	return reader.reader.ReadAt(buffer, offset)
}

// buffer must be large enough to hold block data
func (reader *CachedReader) blockFetcher(baseReader Reader, buffer []byte, blockID int64, blockSize int) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "CachedReader",
		"function": "blockFetcher",
	})

	logger.Infof("Fetching a block through cache - %s, block id %d", baseReader.GetPath(), blockID)

	blockKey := reader.getCacheEntryKey(blockID)
	blockHelper := utils.NewFileBlockHelper(blockSize)

	cacheEntry := reader.cacheStore.GetEntry(blockKey)
	if cacheEntry == nil {
		// read from remote, through cache
		logger.Infof("cache for block %d not found - read from remote", blockID)

		blockStartOffset := blockHelper.GetBlockStartOffset(blockID)

		readLen, err := baseReader.ReadAt(buffer[:blockSize], blockStartOffset)
		if err != nil && err != io.EOF {
			return 0, err
		}

		// save to cache
		_, cacheErr := reader.cacheStore.CreateEntry(blockKey, reader.path, buffer[:readLen])
		if cacheErr != nil {
			// just log
			logger.Error(err)
		}

		if err == io.EOF && readLen == blockSize {
			// EOF
			// save another cache block for EOF
			eofBlockKey := reader.getCacheEntryKey(blockID + 1)
			_, cacheErr := reader.cacheStore.CreateEntry(eofBlockKey, reader.path, buffer[:0])
			if cacheErr != nil {
				// just log
				logger.Error(err)
			}
		}

		// may return EOF as well
		return readLen, err
	}

	// read from cache
	logger.Infof("cache for block %d found - read from cache", blockID)
	readLen, err := cacheEntry.GetData(buffer[:blockSize], 0)
	if err != nil && err != io.EOF {
		return 0, err
	}

	return readLen, err
}

func (reader *CachedReader) GetPendingError() error {
	return reader.reader.GetPendingError()
}
