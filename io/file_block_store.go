package io

import (
	"bytes"
	"fmt"

	"github.com/cyverse/irodsfs-common/io/cache"
	"github.com/cyverse/irodsfs-common/utils"
	lrucache "github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

const (
	readBlockStoreCache int = 5
)

type FileBlock struct {
	blockID int64
	buffer  *bytes.Buffer
	eof     bool // is eof?
}

func NewFileBlock(blockID int64) *FileBlock {
	return &FileBlock{
		blockID: blockID,
		buffer:  &bytes.Buffer{},
		eof:     false,
	}
}

func NewFileBlockWithBuffer(blockID int64, buffer *bytes.Buffer, eof bool) *FileBlock {
	return &FileBlock{
		blockID: blockID,
		buffer:  buffer,
		eof:     eof,
	}
}

type FileBlockStore struct {
	path     string
	checksum string

	cacheStore cache.CacheStore // can be null
	lruCache   *lrucache.Cache
	blockSize  int
}

func NewFileBlockStore(cacheStore cache.CacheStore, path string, checksum string, blockSize int) (*FileBlockStore, error) {
	fileBlockStore := &FileBlockStore{
		path:     path,
		checksum: checksum,

		cacheStore: cacheStore,
		blockSize:  blockSize,
	}

	lruCache, err := lrucache.NewWithEvict(readBlockStoreCache, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to create LRU cache: %w", err)
	}
	fileBlockStore.lruCache = lruCache
	return fileBlockStore, nil
}

func (store *FileBlockStore) Release() {
	store.lruCache.Purge()
	store.cacheStore = nil
}

func (store *FileBlockStore) GetBlockSize() int {
	return store.blockSize
}

func (store *FileBlockStore) Contains(blockID int64) bool {
	if store.lruCache.Contains(blockID) {
		return true
	}

	if store.cacheStore != nil {
		entryKey := store.makeCacheKey(blockID)
		return store.cacheStore.HasEntry(entryKey)
	}

	return false
}

func (store *FileBlockStore) Get(blockID int64) *FileBlock {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "FileBlockStore",
		"function": "Get",
	})

	defer utils.StackTraceFromPanic(logger)

	if block, ok := store.lruCache.Get(blockID); ok {
		return block.(*FileBlock)
	}

	if store.cacheStore != nil {
		entryKey := store.makeCacheKey(blockID)
		logger.Debugf("check cache %s", entryKey)
		cacheEntry := store.cacheStore.GetEntry(entryKey)
		if cacheEntry != nil {
			block := NewFileBlock(blockID)

			blockLen, err := cacheEntry.ReadData(block.buffer, 0)
			if blockLen < store.blockSize {
				block.eof = true
			}

			if err != nil {
				cacheErr := xerrors.Errorf("failed to read data from cache: %w", err)
				logger.Error(cacheErr)
				return nil
			}

			// copy to LRU cache
			store.lruCache.Add(blockID, block)
			return block
		}
	}

	return nil
}

func (store *FileBlockStore) Put(block *FileBlock) error {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "FileBlockStore",
		"function": "Put",
	})

	defer utils.StackTraceFromPanic(logger)

	store.lruCache.Add(block.blockID, block)

	if store.cacheStore != nil {
		cacheKey := store.makeCacheKey(block.blockID)

		_, cacheErr := store.cacheStore.CreateEntry(cacheKey, store.path, block.buffer.Bytes())
		if cacheErr != nil {
			return xerrors.Errorf("failed to create cache entry: %w", cacheErr)
		}

		if block.buffer.Len() == store.blockSize && block.eof {
			// save another cache block for EOF
			eofBlockKey := store.makeCacheKey(block.blockID + 1)

			_, eofCacheErr := store.cacheStore.CreateEntry(eofBlockKey, store.path, []byte{})
			if eofCacheErr != nil {
				return xerrors.Errorf("failed to create cache entry: %w", eofCacheErr)
			}
		}
	}

	return nil
}

func (store *FileBlockStore) makeCacheKey(blockID int64) string {
	return fmt.Sprintf("%s:%s:%d", store.path, store.checksum, blockID)
}
