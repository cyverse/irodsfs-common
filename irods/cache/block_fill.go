package cache

import (
	"sync"
	"time"
)

// BlockFill tracks a cache entry being fetched, so that others needing the same
// entry wait for it instead of fetching it again. The one who began the fill
// must end it with Complete or Abort.
type BlockFill struct {
	mgr  *MemoryCacheManager
	key  string
	done chan struct{}
	once sync.Once
	data []byte
}

// BeginFill registers a fill for key. It returns true when the caller now owns
// the fill and must fetch the entry, or false with the fill already in flight,
// whose result the caller can wait for.
func (mgr *MemoryCacheManager) BeginFill(key string) (*BlockFill, bool) {
	mgr.fillMu.Lock()
	defer mgr.fillMu.Unlock()

	if fill, ok := mgr.fills[key]; ok {
		return fill, false
	}

	fill := &BlockFill{
		mgr:  mgr,
		key:  key,
		done: make(chan struct{}),
	}
	mgr.fills[key] = fill
	return fill, true
}

// Complete ends the fill with data and wakes the waiters. Waiters share data,
// so it must not be modified afterwards.
func (fill *BlockFill) Complete(data []byte) {
	fill.finish(data)
}

// Abort ends the fill without data, so the waiters fetch the entry themselves.
func (fill *BlockFill) Abort() {
	fill.finish(nil)
}

func (fill *BlockFill) finish(data []byte) {
	fill.once.Do(func() {
		fill.data = data

		fill.mgr.fillMu.Lock()
		if fill.mgr.fills[fill.key] == fill {
			delete(fill.mgr.fills, fill.key)
		}
		fill.mgr.fillMu.Unlock()

		close(fill.done)
	})
}

// Wait waits up to timeout for the fill to end. It returns the data and true
// when the fill completed, or false when it was aborted or took too long.
func (fill *BlockFill) Wait(timeout time.Duration) ([]byte, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-fill.done:
		return fill.data, fill.data != nil
	case <-timer.C:
		return nil, false
	}
}
