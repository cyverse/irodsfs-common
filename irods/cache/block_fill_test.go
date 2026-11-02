package cache

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBlockFillManager(t *testing.T) *MemoryCacheManager {
	mgr, err := NewMemoryCacheManager(&MemoryCacheConfig{
		NumCounters: 1000,
		MaxCost:     1024 * 1024,
		BufferItems: 64,
		TTL:         1 * time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(mgr.Release)
	return mgr
}

func TestBlockFillCompleteWakesWaitersWithData(t *testing.T) {
	mgr := newTestBlockFillManager(t)

	fill, owner := mgr.BeginFill("key1")
	require.True(t, owner)

	var wg sync.WaitGroup
	results := make([][]byte, 3)
	for i := range results {
		waitFill, waitOwner := mgr.BeginFill("key1")
		require.False(t, waitOwner)
		require.Same(t, fill, waitFill)

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data, ok := waitFill.Wait(5 * time.Second)
			assert.True(t, ok)
			results[i] = data
		}(i)
	}

	fill.Complete([]byte("block data"))
	wg.Wait()

	for _, data := range results {
		assert.Equal(t, []byte("block data"), data)
	}

	// A finished fill is no longer in flight
	_, owner = mgr.BeginFill("key1")
	assert.True(t, owner)
}

func TestBlockFillAbortReturnsNoData(t *testing.T) {
	mgr := newTestBlockFillManager(t)

	fill, owner := mgr.BeginFill("key1")
	require.True(t, owner)

	fill.Abort()
	data, ok := fill.Wait(5 * time.Second)
	assert.False(t, ok)
	assert.Nil(t, data)

	// Ending a fill again changes nothing
	fill.Complete([]byte("late"))
	data, ok = fill.Wait(5 * time.Second)
	assert.False(t, ok)
	assert.Nil(t, data)
}

func TestBlockFillWaitTimesOut(t *testing.T) {
	mgr := newTestBlockFillManager(t)

	fill, owner := mgr.BeginFill("key1")
	require.True(t, owner)
	defer fill.Abort()

	waitFill, owner := mgr.BeginFill("key1")
	require.False(t, owner)

	data, ok := waitFill.Wait(10 * time.Millisecond)
	assert.False(t, ok)
	assert.Nil(t, data)
}

func TestBlockFillKeysAreIndependent(t *testing.T) {
	mgr := newTestBlockFillManager(t)

	fill1, owner1 := mgr.BeginFill("key1")
	fill2, owner2 := mgr.BeginFill("key2")
	assert.True(t, owner1)
	assert.True(t, owner2)
	assert.NotSame(t, fill1, fill2)

	fill1.Abort()
	fill2.Abort()
}
