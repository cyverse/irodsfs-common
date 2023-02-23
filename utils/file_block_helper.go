package utils

// FileBlockHelper helps block/offset related calculation
type FileBlockHelper struct {
	blockSize int
}

func NewFileBlockHelper(blockSize int) *FileBlockHelper {
	return &FileBlockHelper{
		blockSize: blockSize,
	}
}

// MinOffset returns min value between val1 and val2
func (helper *FileBlockHelper) Min(val1 int64, val2 int64) int64 {
	if val1 <= val2 {
		return val1
	}
	return val2
}

// Max returns max value between val1 and val2
func (helper *FileBlockHelper) Max(val1 int64, val2 int64) int64 {
	if val1 >= val2 {
		return val1
	}
	return val2
}

// GetBlockSize returns block size
func (helper *FileBlockHelper) GetBlockSize() int {
	return helper.blockSize
}

// IsAligned checks if offset is aligned to block start
func (helper *FileBlockHelper) IsAlignedToBlockStart(offset int64) bool {
	blockID := offset / int64(helper.blockSize)
	blockStartOffset := helper.GetBlockStartOffset(blockID)
	return blockStartOffset == offset
}

// GetBlockIDForOffset returns block index
func (helper *FileBlockHelper) GetBlockIDForOffset(offset int64) int64 {
	blockID := offset / int64(helper.blockSize)
	return blockID
}

// GetBlockStartOffset returns block start offset
func (helper *FileBlockHelper) GetBlockStartOffset(blockID int64) int64 {
	return int64(blockID) * int64(helper.blockSize)
}

// GetBlockRange returns offset and length for given block, within given offset and length
func (helper *FileBlockHelper) GetBlockRange(offset int64, length int, blockID int64) (int64, int) {
	blockStartOffset := helper.GetBlockStartOffset(blockID)

	if blockStartOffset+int64(helper.blockSize) <= offset || blockStartOffset >= offset+int64(length) {
		// nothing to read
		return 0, 0
	}

	startOffset := helper.Max(blockStartOffset, offset)
	endOffset := helper.Min(blockStartOffset+int64(helper.blockSize), offset+int64(length))

	return startOffset, int(endOffset - startOffset)
}

// GetFirstAndLastBlockID returns first and last block id
func (helper *FileBlockHelper) GetFirstAndLastBlockID(offset int64, length int) (int64, int64) {
	first := helper.GetBlockIDForOffset(offset)
	last := helper.GetBlockIDForOffset(offset + int64(length-1))
	if last < first {
		last = first
	}
	return first, last
}

// GetBlockIDs returns all block ids
func (helper *FileBlockHelper) GetBlockIDs(offset int64, length int) []int64 {
	first, last := helper.GetFirstAndLastBlockID(offset, length)

	ids := []int64{}
	for i := first; i <= last; i++ {
		ids = append(ids, i)
	}
	return ids
}

// GetLastBlockID returns the last block of the file
func (helper *FileBlockHelper) GetLastBlockID(size int64) int64 {
	return helper.GetBlockIDForOffset(size - 1)
}
