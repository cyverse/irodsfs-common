package cache

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dgraph-io/ristretto"
)

// MemoryCacheEntry represents an in-memory file block cache entry
type MemoryCacheEntry struct {
	key          string
	data         []byte
	size         int
	creationTime time.Time
	cost         int64
}

// NewMemoryCacheEntry creates a new MemoryCacheEntry (shallow copy - references data slice)
func NewMemoryCacheEntry(key string, data []byte) *MemoryCacheEntry {
	return &MemoryCacheEntry{
		key:          key,
		data:         data,
		size:         len(data),
		creationTime: time.Now(),
		cost:         int64(len(data)),
	}
}

// NewMemoryCacheEntryCopy creates a new MemoryCacheEntry with deep copy of data
func NewMemoryCacheEntryCopy(key string, data []byte) *MemoryCacheEntry {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return &MemoryCacheEntry{
		key:          key,
		data:         dataCopy,
		size:         len(data),
		creationTime: time.Now(),
		cost:         int64(len(data)),
	}
}

// GetKey returns the cache key
func (entry *MemoryCacheEntry) GetKey() string {
	return entry.key
}

// GetSize returns the size of the cached data
func (entry *MemoryCacheEntry) GetSize() int {
	return entry.size
}

// GetCreatedAt returns when the entry was created
func (entry *MemoryCacheEntry) GetCreatedAt() time.Time {
	return entry.creationTime
}

// GetData retrieves a slice of data starting from offset
func (entry *MemoryCacheEntry) GetData(offset int) ([]byte, error) {
	if offset < 0 || offset >= len(entry.data) {
		return nil, fmt.Errorf("offset %d out of bounds (size %d)", offset, len(entry.data))
	}
	return entry.data[offset:], nil
}

// GetDataWriteTo writes data to writer starting from offset
func (entry *MemoryCacheEntry) GetDataWriteTo(w io.Writer, offset int) (int, error) {
	if offset < 0 || offset >= len(entry.data) {
		return 0, fmt.Errorf("offset %d out of bounds (size %d)", offset, len(entry.data))
	}
	return w.Write(entry.data[offset:])
}

// MemoryCacheConfig holds configuration for memory cache
type MemoryCacheConfig struct {
	NumCounters int64
	MaxCost     int64
	BufferItems int64
	TTL         time.Duration
	Name        string
}

func NewDefaultMemoryCacheConfig(name string) *MemoryCacheConfig {
	return &MemoryCacheConfig{
		NumCounters: 50000000,
		MaxCost:     100 * 1024 * 1024 * 1024, // 100GB
		BufferItems: 512,
		TTL:         12 * time.Hour,
		Name:        name,
	}
}

// MemoryCacheManager manages in-memory Ristretto cache
type MemoryCacheManager struct {
	cache      *ristretto.Cache
	config     *MemoryCacheConfig
	mu         sync.RWMutex
	totalCount int
	totalSize  int64
	stopOnce   sync.Once
}

// NewMemoryCacheManager creates a new MemoryCacheManager with Ristretto backend
func NewMemoryCacheManager(config *MemoryCacheConfig) (*MemoryCacheManager, error) {
	if config == nil {
		return nil, errors.New("config is null")
	}

	if len(config.Name) == 0 {
		return nil, errors.New("name is empty")
	}

	mgr := &MemoryCacheManager{
		config:     config,
		totalCount: 0,
		totalSize:  0,
	}

	onRemove := func(entry *MemoryCacheEntry) {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		mgr.totalCount--
		mgr.totalSize -= int64(entry.size)
	}

	cacheConfig := &ristretto.Config{
		NumCounters: config.NumCounters,
		MaxCost:     config.MaxCost,
		BufferItems: config.BufferItems,
		OnExit: func(val interface{}) {
			if entry, ok := val.(*MemoryCacheEntry); ok {
				onRemove(entry)
			}
		},
		OnReject: func(item *ristretto.Item) {
			if entry, ok := item.Value.(*MemoryCacheEntry); ok {
				onRemove(entry)
			}
		},
	}

	cache, err := ristretto.NewCache(cacheConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Ristretto cache")
	}

	mgr.cache = cache
	return mgr, nil
}

// Release closes the cache and cleanup resources
func (mgr *MemoryCacheManager) Release() {
	mgr.stopOnce.Do(func() {
		if mgr.cache != nil {
			mgr.cache.Close()
		}
	})
}

// GetSizeCap returns maximum total cache size
func (mgr *MemoryCacheManager) GetMaxSize() int64 {
	return mgr.config.MaxCost
}

// GetCount returns number of entries in cache
func (mgr *MemoryCacheManager) GetCount() int {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.totalCount
}

// GetTotalSize returns total size of all entries
func (mgr *MemoryCacheManager) GetTotalSize() int64 {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.totalSize
}

// GetAvailableSize returns remaining capacity
func (mgr *MemoryCacheManager) GetAvailableSize() int64 {
	return mgr.GetMaxSize() - mgr.GetTotalSize()
}

// Clear removes all cache entries
func (mgr *MemoryCacheManager) Clear(wait bool) {
	// OnEvict callbacks will handle statistics update for each item
	mgr.cache.Clear()

	if wait {
		mgr.cache.Wait()
	}
}

// Put creates a new cache entry with data (shallow reference), optionally waiting for it to be stored
// Use PutCopy() if you need to copy the data to avoid external modifications
func (mgr *MemoryCacheManager) Put(key string, data []byte, wait bool) (*MemoryCacheEntry, error) {
	entry := NewMemoryCacheEntry(key, data)

	mgr.mu.Lock()
	mgr.totalCount++
	mgr.totalSize += int64(entry.size)
	mgr.mu.Unlock()

	if !mgr.cache.SetWithTTL(key, entry, entry.cost, mgr.config.TTL) {
		mgr.mu.Lock()
		mgr.totalCount--
		mgr.totalSize -= int64(entry.size)
		mgr.mu.Unlock()
		return nil, errors.New("failed to add entry to cache (queue full)")
	}

	if wait {
		mgr.cache.Wait()
	}

	return entry, nil
}

// PutCopy creates a new cache entry with a deep copy of data, optionally waiting for it to be stored
func (mgr *MemoryCacheManager) PutCopy(key string, data []byte, wait bool) (*MemoryCacheEntry, error) {
	entry := NewMemoryCacheEntryCopy(key, data)

	mgr.mu.Lock()
	mgr.totalCount++
	mgr.totalSize += int64(entry.size)
	mgr.mu.Unlock()

	if !mgr.cache.SetWithTTL(key, entry, entry.cost, mgr.config.TTL) {
		mgr.mu.Lock()
		mgr.totalCount--
		mgr.totalSize -= int64(entry.size)
		mgr.mu.Unlock()
		return nil, errors.New("failed to add entry to cache (queue full)")
	}

	if wait {
		mgr.cache.Wait()
	}

	return entry, nil
}

// Has checks if a key exists in cache
func (mgr *MemoryCacheManager) Has(key string) bool {
	_, found := mgr.cache.Get(key)
	return found
}

// GetEntry retrieves a cache entry by key
func (mgr *MemoryCacheManager) Get(key string) *MemoryCacheEntry {
	val, found := mgr.cache.Get(key)
	if !found {
		return nil
	}

	entry, ok := val.(*MemoryCacheEntry)
	if !ok {
		return nil
	}

	return entry
}

// Delete removes a cache entry by key
func (mgr *MemoryCacheManager) Delete(key string, wait bool) {
	mgr.cache.Del(key)
	// Wait ensures onEvict callback completes before returning
	if wait {
		mgr.cache.Wait()
	}
}
