package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileBlockHelper(t *testing.T) {
	t.Run("test MinMax", testMinMax)
	t.Run("test AlignedBlock", testAlignedBlock)
	t.Run("test BlockIDForOffset", testBlockIDForOffset)
	t.Run("test BlockRange", testBlockRange)
	t.Run("test FirstAndLastBlockID", testFirstAndLastBlockID)
}

func testMinMax(t *testing.T) {
	v1 := int64(100000)
	v2 := int64(200000)

	testBlockSize := 10000

	helper := NewFileBlockHelper(testBlockSize)
	vMin := helper.Min(v1, v2)
	vMax := helper.Max(v1, v2)

	assert.Equal(t, v1, vMin)
	assert.Equal(t, v2, vMax)
}

func testAlignedBlock(t *testing.T) {
	testBlockSize := 10000

	helper := NewFileBlockHelper(testBlockSize)

	result1 := helper.IsAlignedToBlockStart(int64(testBlockSize * 7))
	assert.True(t, result1)

	result2 := helper.IsAlignedToBlockStart(int64(testBlockSize*7 + 1))
	assert.False(t, result2)

	result3 := helper.IsAlignedToBlockStart(int64(testBlockSize*7 - 1))
	assert.False(t, result3)

	result4 := helper.IsAlignedToBlockStart(int64(testBlockSize * 88))
	assert.True(t, result4)
}

func testBlockIDForOffset(t *testing.T) {
	testBlockSize := 10000

	helper := NewFileBlockHelper(testBlockSize)

	result1 := helper.GetBlockIDForOffset(10)
	assert.Equal(t, int64(0), result1)

	result2 := helper.GetBlockIDForOffset(int64(testBlockSize*7 + 1))
	assert.Equal(t, int64(7), result2)

	result3 := helper.GetBlockIDForOffset(int64(testBlockSize*7 - 1))
	assert.Equal(t, int64(6), result3)

	result4 := helper.GetBlockIDForOffset(int64(testBlockSize * 88))
	assert.Equal(t, int64(88), result4)
}

func testBlockRange(t *testing.T) {
	testBlockSize := 10000

	helper := NewFileBlockHelper(testBlockSize)

	off1, len1 := helper.GetBlockRange(10, 100, 0)
	assert.Equal(t, int64(10), off1)
	assert.Equal(t, 100, len1)

	off2, len2 := helper.GetBlockRange(9000, 2000, 0)
	assert.Equal(t, int64(9000), off2)
	assert.Equal(t, 1000, len2)

	off3, len3 := helper.GetBlockRange(9000, 2000, 1)
	assert.Equal(t, int64(10000), off3)
	assert.Equal(t, 1000, len3)
}

func testFirstAndLastBlockID(t *testing.T) {
	testBlockSize := 10000

	helper := NewFileBlockHelper(testBlockSize)

	first1, last1 := helper.GetFirstAndLastBlockID(10, 100)
	assert.Equal(t, int64(0), first1)
	assert.Equal(t, int64(0), last1)

	first2, last2 := helper.GetFirstAndLastBlockID(0, 10000)
	assert.Equal(t, int64(0), first2)
	assert.Equal(t, int64(0), last2)

	first3, last3 := helper.GetFirstAndLastBlockID(10, 10000)
	assert.Equal(t, int64(0), first3)
	assert.Equal(t, int64(1), last3)

	first4, last4 := helper.GetFirstAndLastBlockID(10000, 10000)
	assert.Equal(t, int64(1), first4)
	assert.Equal(t, int64(1), last4)

	first5, last5 := helper.GetFirstAndLastBlockID(300, 100000)
	assert.Equal(t, int64(0), first5)
	assert.Equal(t, int64(10), last5)

}
