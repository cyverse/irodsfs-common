package io

import (
	"io"
	"sync"

	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
)

type blockDataInfo struct {
	id   int64
	data []byte
	eof  bool
}

// BlockFetcherFunc is a function prototype for block fetcher
type BlockFetcherFunc func(baseReader Reader, buffer []byte, blockID int64, blockSize int) (int, error)

// BlockReader helps read in block level
type BlockReader struct {
	path string

	reader      Reader
	blockHelper *utils.FileBlockHelper

	readBuffer   []byte
	currentBlock *blockDataInfo
	mutex        sync.Mutex // lock for read buffer and block data

	blockFetcher BlockFetcherFunc
}

// NewCachedReader create a new CachedReader
func NewBlockReader(reader Reader, blockSize int, fetcher BlockFetcherFunc) Reader {
	if fetcher == nil {
		fetcher = NaiveBlockFetcher
	}

	blockReader := &BlockReader{
		path: reader.GetPath(),

		reader:      reader,
		blockHelper: utils.NewFileBlockHelper(blockSize),

		readBuffer:   make([]byte, blockSize),
		currentBlock: nil,

		blockFetcher: fetcher,
	}

	return blockReader
}

// Release releases all resources
func (reader *BlockReader) Release() {
	if reader.reader != nil {
		reader.reader.Release()
		reader.reader = nil
	}
}

// GetPath returns path of the file
func (reader *BlockReader) GetPath() string {
	return reader.path
}

// ReadAt reads data
func (reader *BlockReader) ReadAt(buffer []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "BlockReader",
		"function": "ReadAt",
	})

	if len(buffer) <= 0 || offset < 0 {
		return 0, nil
	}

	logger.Infof("Reading data - %s, offset %d, length %d", reader.path, offset, len(buffer))

	blockSize := reader.blockHelper.GetBlockSize()
	blockIDs := reader.blockHelper.GetBlockIDs(offset, len(buffer))

	currentOffset := offset
	totalReadLen := 0
	for _, blockID := range blockIDs {
		blockStartOffset := reader.blockHelper.GetBlockStartOffset(blockID)
		blockOffset, blockLen := reader.blockHelper.GetBlockRange(offset, len(buffer), blockID)

		if blockOffset == blockStartOffset && blockLen == blockSize {
			// read full block
			readLen, err := reader.readBlockWithoutCache(buffer[totalReadLen:], blockID)
			if err != nil && err != io.EOF {
				return 0, err
			}

			totalReadLen += readLen
			currentOffset += int64(readLen)

			if err == io.EOF {
				return totalReadLen, io.EOF
			}
		} else {
			// read partial block
			inBlockOffset := int(currentOffset - blockStartOffset)
			readLen, err := reader.readBlockWithCache(buffer[totalReadLen:], blockID, inBlockOffset)
			if err != nil && err != io.EOF {
				return 0, err
			}

			totalReadLen += readLen
			currentOffset += int64(readLen)

			if err == io.EOF {
				return totalReadLen, io.EOF
			}
		}

	}

	return totalReadLen, nil
}

func (reader *BlockReader) readBlockWithCache(buffer []byte, blockID int64, inBlockOffset int) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "BlockReader",
		"function": "readBlockWithCache",
	})

	logger.Infof("Reading a block data - %s, block id %d, in block offset %d", reader.path, blockID, inBlockOffset)

	reader.mutex.Lock()
	defer reader.mutex.Unlock()

	if reader.currentBlock == nil || reader.currentBlock.id != blockID {
		// has no data in memory cache
		readLen, err := reader.blockFetcher(reader.reader, reader.readBuffer, blockID, reader.blockHelper.GetBlockSize())
		if err != nil && err != io.EOF {
			return 0, err
		}

		reader.currentBlock = &blockDataInfo{
			id:   blockID,
			data: reader.readBuffer[:readLen],
			eof:  err == io.EOF,
		}
	}

	// read from memory cache
	copyLen := copy(buffer, reader.currentBlock.data[inBlockOffset:])

	if reader.currentBlock.eof && inBlockOffset+copyLen == len(reader.currentBlock.data) {
		// eof
		return copyLen, io.EOF
	}

	// not eof
	return copyLen, nil
}

func (reader *BlockReader) readBlockWithoutCache(buffer []byte, blockID int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "BlockReader",
		"function": "readBlockWithoutCache",
	})

	logger.Infof("Reading a block data - %s, block id %d", reader.path, blockID)

	reader.mutex.Lock()
	defer reader.mutex.Unlock()

	if reader.currentBlock != nil && reader.currentBlock.id == blockID {
		// copy
		copyLen := copy(buffer, reader.currentBlock.data)
		if reader.currentBlock.eof && copyLen == len(reader.currentBlock.data) {
			// eof
			return copyLen, io.EOF
		}

		// not eof
		return copyLen, nil
	}

	// fetch
	readLen, err := reader.blockFetcher(reader.reader, buffer, blockID, reader.blockHelper.GetBlockSize())
	if err != nil && err != io.EOF {
		return 0, err
	}

	// not eof
	return readLen, err
}

func (reader *BlockReader) GetPendingError() error {
	return nil
}

// buffer must be large enough to hold block data
func NaiveBlockFetcher(baseReader Reader, buffer []byte, blockID int64, blockSize int) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"function": "NaiveBlockFetcher",
	})

	logger.Infof("Fetching a block - %s, block id %d", baseReader.GetPath(), blockID)

	blockHelper := utils.NewFileBlockHelper(blockSize)
	blockStartOffset := blockHelper.GetBlockStartOffset(blockID)

	readLen, err := baseReader.ReadAt(buffer[:blockSize], blockStartOffset)
	if err != nil && err != io.EOF {
		return 0, err
	}

	return readLen, err
}
