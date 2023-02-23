package io

import (
	"sync"

	"github.com/cyverse/irodsfs-common/utils"
)

const (
	prefetchTriggerRatio float32 = 0.3 // determine when to start prefetch
)

type Prefetcher struct {
	prefetchMap map[int64]bool
	blockHelper *utils.FileBlockHelper
	mutex       sync.Mutex
}

func NewPrefetcher(blockSize int) *Prefetcher {
	return &Prefetcher{
		prefetchMap: map[int64]bool{},
		blockHelper: utils.NewFileBlockHelper(blockSize),
		mutex:       sync.Mutex{},
	}
}

func (prefetcher *Prefetcher) Determine(offset int64, size int64) []int64 {
	blockID := prefetcher.blockHelper.GetBlockIDForOffset(offset)
	blockStartOffset := prefetcher.blockHelper.GetBlockStartOffset(blockID)
	inBlockOffset := int(offset - blockStartOffset)
	blockSize := prefetcher.blockHelper.GetBlockSize()
	lastBlockID := prefetcher.blockHelper.GetLastBlockID(size)

	// do prefetch when current offset passed certain point, e.g., 30% of the block
	triggerPoint := float32(blockSize) * prefetchTriggerRatio
	if inBlockOffset < int(triggerPoint) {
		return nil
	}

	targetBlockID := blockID + 1
	// if current block is the last, prefetch the first block (e.g., zip has entry footer)
	if blockID >= lastBlockID {
		targetBlockID = 0
	}

	prefetcher.mutex.Lock()
	defer prefetcher.mutex.Unlock()

	// if target block is already prefetched
	if _, ok := prefetcher.prefetchMap[targetBlockID]; ok {
		return nil
	}

	// otherwise
	prefetcher.prefetchMap[targetBlockID] = true
	return []int64{targetBlockID}
}
