package util

// FileBlockHelper helps block/offset related calculation
type FileBlockHelper struct {
	blockSize int
}

func NewFileBlockHelper(blockSize int) *FileBlockHelper {
	return &FileBlockHelper{
		blockSize: blockSize,
	}
}

// GetBlockSize returns block size
func (helper *FileBlockHelper) GetBlockSize() int {
	return helper.blockSize
}

// IsAligned checks if offset is aligned to block start
func (helper *FileBlockHelper) IsAligned(offset int64) bool {
	blockID := offset / int64(helper.blockSize)
	blockStartOffset := helper.GetBlockStart(blockID)
	return blockStartOffset == offset
}

// GetBlockID returns block index for a given offset
func (helper *FileBlockHelper) GetBlockID(offset int64) int64 {
	return offset / int64(helper.blockSize)
}

// GetBlockOffset returns offset within a block for a given absolute offset
func (helper *FileBlockHelper) GetBlockOffset(offset int64) int64 {
	return offset % int64(helper.blockSize)
}

// GetBlockStart returns block start offset for a given block ID
func (helper *FileBlockHelper) GetBlockStart(blockID int64) int64 {
	return int64(blockID) * int64(helper.blockSize)
}

// GetBlockRange returns offset and length for given block, within given offset and length
func (helper *FileBlockHelper) GetBlockRange(offset int64, length int, blockID int64) (int64, int) {
	blockStartOffset := helper.GetBlockStart(blockID)

	if blockStartOffset+int64(helper.blockSize) <= offset || blockStartOffset >= offset+int64(length) {
		// nothing to read
		return 0, 0
	}

	startOffset := max(blockStartOffset, offset)
	endOffset := min(blockStartOffset+int64(helper.blockSize), offset+int64(length))
	return startOffset, int(endOffset - startOffset)
}

// GetBlockIDs returns first and last block id for the given range
func (helper *FileBlockHelper) GetBlockIDs(offset int64, length int) (int64, int64) {
	first := helper.GetBlockID(offset)
	last := helper.GetBlockID(offset + int64(length-1))
	return first, max(first, last)
}

// GetAllBlockIDs returns all block ids for the given range
func (helper *FileBlockHelper) GetAllBlockIDs(offset int64, length int) []int64 {
	first, last := helper.GetBlockIDs(offset, length)

	ids := make([]int64, 0, last-first+1)
	for i := first; i <= last; i++ {
		ids = append(ids, i)
	}
	return ids
}

// GetLastBlockID returns the last block of the file
func (helper *FileBlockHelper) GetLastBlockID(size int64) int64 {
	if size <= 0 {
		return 0
	}
	return helper.GetBlockID(size - 1)
}
