package cache

import (
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/irodsfs-common/utils"
	lrucache "github.com/hashicorp/golang-lru"
)

// DiskCacheEntry implements CacheEntry
type DiskCacheEntry struct {
	key          string
	group        string
	size         int
	creationTime time.Time
	filePath     string
}

// NewDiskCacheEntry creates a new DiskCacheEntry
func NewDiskCacheEntry(cache *DiskCacheStore, key string, group string, data []byte) (*DiskCacheEntry, error) {
	// write to disk
	hash := utils.GetSHA1Sum(key)
	filePath := path.Join(cache.GetRootPath(), hash)

	err := os.WriteFile(filePath, data, 0666)
	if err != nil {
		writeErr := errors.Wrapf(err, "failed to write cache file %q", filePath)
		return nil, writeErr
	}

	return &DiskCacheEntry{
		key:          key,
		group:        group,
		size:         len(data),
		creationTime: time.Now(),
		filePath:     filePath,
	}, nil
}

// GetKey returns key of the entry
func (entry *DiskCacheEntry) GetKey() string {
	return entry.key
}

// GetKey returns group of the entry
func (entry *DiskCacheEntry) GetGroup() string {
	return entry.group
}

// GetKey returns the size of the entry
func (entry *DiskCacheEntry) GetSize() int {
	return entry.size
}

// GetKey returns creation time of the entry
func (entry *DiskCacheEntry) GetCreationTime() time.Time {
	return entry.creationTime
}

// GetKey returns data of the entry
func (entry *DiskCacheEntry) GetData(buffer []byte, inBlockOffset int) (int, error) {
	f, err := os.Open(entry.filePath)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to open cache file %q", entry.filePath)
	}
	defer f.Close()

	f.Seek(int64(inBlockOffset), io.SeekStart)

	totalRead := 0
	toRead := len(buffer)
	for totalRead < toRead {
		readLen, err := f.Read(buffer[totalRead:])
		if err != nil && err != io.EOF {
			return 0, errors.Wrapf(err, "failed to read data from cache file %q", entry.filePath)
		}
		totalRead += readLen

		if err == io.EOF {
			return totalRead, io.EOF
		}
	}

	return totalRead, nil
}

// ReadData returns data of the entry
func (entry *DiskCacheEntry) ReadData(writer io.Writer, inBlockOffset int) (int, error) {
	f, err := os.Open(entry.filePath)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to open cache file %q", entry.filePath)
	}
	defer f.Close()

	_, err = f.Seek(int64(inBlockOffset), io.SeekStart)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to seek cache file %q", entry.filePath)
	}

	copied, err := io.Copy(writer, f)
	if err != nil {
		return int(copied), errors.Wrapf(err, "failed to copy data from cache file %q", entry.filePath)
	}

	return int(copied), err
}

func (entry *DiskCacheEntry) deleteDataFile() error {
	err := os.Remove(entry.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return errors.Wrapf(err, "failed to remove cache file %q", entry.filePath)
	}
	return nil
}

// DiskCacheStore implements CacheStore
type DiskCacheStore struct {
	entrySizeCap   int
	sizeCap        int64
	entryNumberCap int
	rootPath       string
	cache          *lrucache.Cache
	groups         map[string]map[string]bool // key = group name, value = cache keys for a group
	mutex          sync.Mutex
}

// NewDiskCacheStore creates a new DiskCacheStore
func NewDiskCacheStore(sizeCap int64, entrySizeCap int, rootPath string) (CacheStore, error) {
	err := os.MkdirAll(rootPath, 0777)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to make dir %q", rootPath)
	}

	var maxCacheEntryNum int = int(sizeCap / int64(entrySizeCap))

	diskCache := &DiskCacheStore{
		entrySizeCap:   entrySizeCap,
		sizeCap:        sizeCap,
		entryNumberCap: maxCacheEntryNum,
		rootPath:       rootPath,
		cache:          nil,
		groups:         map[string]map[string]bool{},
	}

	err = diskCache.clearCacheFiles()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to clear existing cache files")
	}

	lruCache, err := lrucache.NewWithEvict(maxCacheEntryNum, diskCache.onEvicted)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create LRU cache")
	}

	diskCache.cache = lruCache
	return diskCache, nil
}

// Release releases resources
func (store *DiskCacheStore) Release() {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	// clear
	store.groups = map[string]map[string]bool{}
	store.cache.Purge()

	os.RemoveAll(store.rootPath)
}

func (store *DiskCacheStore) clearCacheFiles() error {
	files, err := os.ReadDir(store.rootPath)
	if err != nil {
		return errors.Wrapf(err, "failed to read cache dir %q", store.rootPath)
	}

	removeErrors := []error{}

	for _, file := range files {
		filePath := path.Join(store.rootPath, file.Name())
		err := os.RemoveAll(filePath)
		if err != nil {
			rerr := errors.Wrapf(err, "failed to remove cache file or dir %q", filePath)
			removeErrors = append(removeErrors, rerr)
		}
	}

	if len(removeErrors) > 0 {
		return errors.Join(removeErrors...)
	}

	return nil
}

// GetEntrySizeCap returns entry size cap
func (store *DiskCacheStore) GetEntrySizeCap() int {
	return store.entrySizeCap
}

// GetSizeCap returns size cap
func (store *DiskCacheStore) GetSizeCap() int64 {
	return store.sizeCap
}

// GetRootPath returns root path of disk cache
func (store *DiskCacheStore) GetRootPath() string {
	return store.rootPath
}

// GetTotalEntries returns total number of entries in cache
func (store *DiskCacheStore) GetTotalEntries() int {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	return store.cache.Len()
}

// GetTotalEntrySize returns total size of entries in cache
func (store *DiskCacheStore) GetTotalEntrySize() int64 {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	return int64(store.cache.Len()) * int64(store.entrySizeCap)
}

// GetAvailableSize returns available disk space
func (store *DiskCacheStore) GetAvailableSize() int64 {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	availableEntries := store.entryNumberCap - store.cache.Len()
	return int64(availableEntries) * int64(store.entrySizeCap)
}

// DeleteAllEntries deletes all entries
func (store *DiskCacheStore) DeleteAllEntries() {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	// clear
	store.groups = map[string]map[string]bool{}

	store.cache.Purge()
}

// DeleteAllEntriesForGroup deletes all entries in the given group
func (store *DiskCacheStore) DeleteAllEntriesForGroup(group string) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if cacheGroup, ok := store.groups[group]; ok {
		for key := range cacheGroup {
			store.cache.Remove(key)
		}
	}
}

// GetEntryKeys returns all entry keys
func (store *DiskCacheStore) GetEntryKeys() []string {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	keys := []string{}
	for _, key := range store.cache.Keys() {
		if strkey, ok := key.(string); ok {
			keys = append(keys, strkey)
		}
	}
	return keys
}

// GetEntryKeysForGroup returns all entry keys for the given group
func (store *DiskCacheStore) GetEntryKeysForGroup(group string) []string {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	keys := []string{}
	if cacheGroup, ok := store.groups[group]; ok {
		for key := range cacheGroup {
			if store.cache.Contains(key) {
				keys = append(keys, key)
			}
		}
	}
	return keys
}

// CreateEntry creates a new entry
func (store *DiskCacheStore) CreateEntry(key string, group string, data []byte) (CacheEntry, error) {
	if store.entrySizeCap < len(data) {
		return nil, errors.Newf("requested data %d is larger than entry size cap %d", len(data), store.entrySizeCap)
	}

	entry, err := NewDiskCacheEntry(store, key, group, data)
	if err != nil {
		return nil, err
	}

	store.mutex.Lock()
	defer store.mutex.Unlock()

	store.cache.Add(key, entry)

	if cacheGroup, ok := store.groups[group]; ok {
		cacheGroup[key] = true
	} else {
		cacheGroup = map[string]bool{}
		cacheGroup[key] = true
		store.groups[group] = cacheGroup
	}

	return entry, nil
}

// HasEntry checks if the entry for the given key is present
func (store *DiskCacheStore) HasEntry(key string) bool {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	return store.cache.Contains(key)
}

// GetEntry returns an entry with the given key
func (store *DiskCacheStore) GetEntry(key string) CacheEntry {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if entry, ok := store.cache.Get(key); ok {
		if cacheEntry, ok := entry.(*DiskCacheEntry); ok {
			return cacheEntry
		}
	}

	return nil
}

// DeleteEntry deletes an entry with the given key
func (store *DiskCacheStore) DeleteEntry(key string) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	store.cache.Remove(key)
}

func (store *DiskCacheStore) onEvicted(key interface{}, entry interface{}) {
	if cacheEntry, ok := entry.(*DiskCacheEntry); ok {
		cacheEntry.deleteDataFile()

		if cacheGroup, ok := store.groups[cacheEntry.group]; ok {
			delete(cacheGroup, cacheEntry.key)

			if len(cacheGroup) == 0 {
				delete(store.groups, cacheEntry.group)
			}
		}
	}
}
