package io

import (
	"fmt"

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
func NewCachedReader(path string, checksum string, cacheStore CacheStore, reader Reader) *CachedReader {
	cacheReader := &CachedReader{
		path:     path,
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

func (reader *CachedReader) getCacheEntryKey(blockID int64) string {
	return fmt.Sprintf("%s:%s:%d", reader.path, reader.checksum, blockID)
}

func (reader *CachedReader) getBlockIDs(offset int64, length int) []int64 {
	first, last := reader.blockHelper.GetFirstAndLastBlockIDForRW(offset, length)

	ids := []int64{}
	for i := first; i <= last; i++ {
		ids = append(ids, i)
	}
	return ids
}

// ReadAt reads data
func (reader *CachedReader) ReadAt(offset int64, length int) ([]byte, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "CacheReader",
		"function": "ReadAt",
	})

	if length <= 0 || offset < 0 {
		return []byte{}, nil
	}

	logger.Infof("Reading through cache - %s, offset %d, length %d", reader.path, offset, length)

	blockIDs := reader.getBlockIDs(offset, length)
	dataRead := 0
	readBuffer := make([]byte, length)
	for _, blockID := range blockIDs {
		blockKey := reader.getCacheEntryKey(blockID)
		cacheEntry := reader.cacheStore.GetEntry(blockKey)

		var cacheData []byte
		if cacheEntry == nil {
			logger.Infof("cache for block %s not found -- read from remote", blockKey)

			blockOffset := reader.blockHelper.GetBlockStartOffsetForBlockID(blockID)
			blockData, err := reader.reader.ReadAt(blockOffset, cachedReaderBlockSize)
			if err != nil {
				return nil, err
			}

			if len(blockData) == 0 {
				// EOF?
				break
			}

			cacheData = blockData
			_, err = reader.cacheStore.CreateEntry(blockKey, reader.path, blockData)
			if err != nil {
				// just log
				logger.Error(err)
			}
		} else {
			cacheEntryData, err := cacheEntry.GetData()
			if err != nil {
				return nil, err
			}

			cacheData = cacheEntryData
		}

		inBlockOffset, _ := reader.blockHelper.GetInBlockOffsetAndLength(offset+int64(dataRead), length-dataRead)
		inBlockLength := length - dataRead
		if inBlockLength > (len(cacheData) - inBlockOffset) {
			inBlockLength = len(cacheData) - inBlockOffset
		}

		copy(readBuffer[dataRead:], cacheData[inBlockOffset:inBlockOffset+inBlockLength])
		dataRead += inBlockLength

		if len(cacheData) != cachedReaderBlockSize {
			// EOF
			break
		}
	}

	return readBuffer[:dataRead], nil
}

func (reader *CachedReader) GetPendingError() error {
	return nil
}
