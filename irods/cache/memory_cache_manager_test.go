package cache

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemoryCacheManagerPut(t *testing.T) {
	config := &MemoryCacheConfig{
		NumCounters: 1000,
		MaxCost:     1024 * 1024,
		BufferItems: 64,
		TTL:         1 * time.Hour,
	}

	mgr, err := NewMemoryCacheManager(config)
	assert.NoError(t, err)
	defer mgr.Release()

	data := []byte("test data")
	entry, err := mgr.Put("key1", data, true)
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, "key1", entry.GetKey())
	assert.Equal(t, len(data), entry.GetSize())
}

func TestMemoryCacheManagerGet(t *testing.T) {
	config := &MemoryCacheConfig{
		NumCounters: 1000,
		MaxCost:     1024 * 1024,
		BufferItems: 64,
		TTL:         1 * time.Hour,
	}

	mgr, err := NewMemoryCacheManager(config)
	assert.NoError(t, err)
	defer mgr.Release()

	data := []byte("test data")
	_, err = mgr.Put("key1", data, true)
	assert.NoError(t, err)

	entry := mgr.Get("key1")
	assert.NotNil(t, entry)
	assert.Equal(t, "key1", entry.GetKey())
	assert.Equal(t, len(data), entry.GetSize())

	entry = mgr.Get("nonexistent")
	assert.Nil(t, entry)
}

func TestMemoryCacheManagerGetData(t *testing.T) {
	config := &MemoryCacheConfig{
		NumCounters: 1000,
		MaxCost:     1024 * 1024,
		BufferItems: 64,
		TTL:         1 * time.Hour,
	}

	mgr, err := NewMemoryCacheManager(config)
	assert.NoError(t, err)
	defer mgr.Release()

	data := []byte("hello world")
	_, err = mgr.Put("key1", data, true)
	assert.NoError(t, err)

	entry := mgr.Get("key1")
	assert.NotNil(t, entry)

	slice, err := entry.GetData(0)
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello world"), slice)

	slice, err = entry.GetData(6)
	assert.NoError(t, err)
	assert.Equal(t, []byte("world"), slice)
}

func TestMemoryCacheManagerGetDataWriteTo(t *testing.T) {
	config := &MemoryCacheConfig{
		NumCounters: 1000,
		MaxCost:     1024 * 1024,
		BufferItems: 64,
		TTL:         1 * time.Hour,
	}

	mgr, err := NewMemoryCacheManager(config)
	assert.NoError(t, err)
	defer mgr.Release()

	data := []byte("hello world")
	_, err = mgr.Put("key1", data, true)
	assert.NoError(t, err)

	entry := mgr.Get("key1")
	assert.NotNil(t, entry)

	var buf bytes.Buffer
	n, err := entry.GetDataWriteTo(&buf, 0)
	assert.NoError(t, err)
	assert.Equal(t, 11, n)
	assert.Equal(t, "hello world", buf.String())

	buf.Reset()
	n, err = entry.GetDataWriteTo(&buf, 6)
	assert.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "world", buf.String())
}

func TestMemoryCacheManagerDelete(t *testing.T) {
	config := &MemoryCacheConfig{
		NumCounters: 1000,
		MaxCost:     1024 * 1024,
		BufferItems: 64,
		TTL:         1 * time.Hour,
	}

	mgr, err := NewMemoryCacheManager(config)
	assert.NoError(t, err)
	defer mgr.Release()

	data := []byte("test data")
	_, err = mgr.Put("key1", data, true)
	assert.NoError(t, err)

	assert.True(t, mgr.Has("key1"))
	mgr.Delete("key1", true)
	assert.False(t, mgr.Has("key1"))
}

func TestMemoryCacheManagerMultipleEntries(t *testing.T) {
	config := &MemoryCacheConfig{
		NumCounters: 1000,
		MaxCost:     1024 * 1024,
		BufferItems: 64,
		TTL:         1 * time.Hour,
	}

	mgr, err := NewMemoryCacheManager(config)
	assert.NoError(t, err)
	defer mgr.Release()

	mgr.Put("key1", []byte("data1"), true)
	mgr.Put("key2", []byte("data2"), true)
	mgr.Put("key3", []byte("data3"), true)

	assert.True(t, mgr.Has("key1"))
	assert.True(t, mgr.Has("key2"))
	assert.True(t, mgr.Has("key3"))
	assert.Equal(t, 3, mgr.GetCount())

	mgr.Delete("key2", true)
	assert.True(t, mgr.Has("key1"))
	assert.False(t, mgr.Has("key2"))
	assert.True(t, mgr.Has("key3"))
	assert.Equal(t, 2, mgr.GetCount())
}

func TestMemoryCacheManagerCount(t *testing.T) {
	config := &MemoryCacheConfig{
		NumCounters: 1000,
		MaxCost:     1024 * 1024,
		BufferItems: 64,
		TTL:         1 * time.Hour,
	}

	mgr, err := NewMemoryCacheManager(config)
	assert.NoError(t, err)
	defer mgr.Release()

	assert.Equal(t, 0, mgr.GetCount())

	mgr.Put("key1", []byte("data1"), true)
	assert.Equal(t, 1, mgr.GetCount())

	mgr.Put("key2", []byte("data2"), true)
	assert.Equal(t, 2, mgr.GetCount())

	mgr.Delete("key1", true)
	assert.Equal(t, 1, mgr.GetCount())
}

func TestMemoryCacheManagerTotalSize(t *testing.T) {
	config := &MemoryCacheConfig{
		NumCounters: 1000,
		MaxCost:     1024 * 1024,
		BufferItems: 64,
		TTL:         1 * time.Hour,
	}

	mgr, err := NewMemoryCacheManager(config)
	assert.NoError(t, err)
	defer mgr.Release()

	assert.Equal(t, int64(0), mgr.GetTotalSize())

	mgr.Put("key1", []byte("hello"), true) // 5 bytes
	assert.Equal(t, int64(5), mgr.GetTotalSize())

	mgr.Put("key2", []byte("world!"), true) // 6 bytes
	assert.Equal(t, int64(11), mgr.GetTotalSize())

	mgr.Delete("key1", true)
	assert.Equal(t, int64(6), mgr.GetTotalSize())

	mgr.Delete("key2", true)
	assert.Equal(t, int64(0), mgr.GetTotalSize())
}

func TestMemoryCacheManagerAvailableSize(t *testing.T) {
	maxCost := int64(1000)
	config := &MemoryCacheConfig{
		NumCounters: 1000,
		MaxCost:     maxCost,
		BufferItems: 64,
		TTL:         1 * time.Hour,
	}

	mgr, err := NewMemoryCacheManager(config)
	assert.NoError(t, err)
	defer mgr.Release()

	assert.Equal(t, maxCost, mgr.GetMaxSize())
	assert.Equal(t, maxCost, mgr.GetAvailableSize())

	mgr.Put("key1", make([]byte, 300), true)
	assert.Equal(t, int64(700), mgr.GetAvailableSize())

	mgr.Put("key2", make([]byte, 200), true)
	assert.Equal(t, int64(500), mgr.GetAvailableSize())

	mgr.Delete("key1", true)
	assert.Equal(t, int64(800), mgr.GetAvailableSize())

	mgr.Clear(true)
	assert.Equal(t, maxCost, mgr.GetAvailableSize())
}

func TestMemoryCacheManagerClear(t *testing.T) {
	config := &MemoryCacheConfig{
		NumCounters: 1000,
		MaxCost:     1024 * 1024,
		BufferItems: 64,
		TTL:         1 * time.Hour,
	}

	mgr, err := NewMemoryCacheManager(config)
	assert.NoError(t, err)
	defer mgr.Release()

	mgr.Put("key1", []byte("data1"), true)
	mgr.Put("key2", []byte("data2"), true)
	mgr.Put("key3", []byte("data3"), true)

	assert.Equal(t, 3, mgr.GetCount())

	mgr.Clear(true)
	assert.Equal(t, 0, mgr.GetCount())
	assert.False(t, mgr.Has("key1"))
}
