package stagingfs

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
	"github.com/dgraph-io/badger/v3"
	log "github.com/sirupsen/logrus"
)

// ErrQuotaExceeded is returned by ensureQuota when staging space cannot be freed sufficiently.
// Callers may use errors.Is to detect this and fall back to a non-staging path.
var ErrQuotaExceeded = errors.New("staging quota exceeded")

// PathHolder is implemented by any object that holds a staging path reference and
// must be notified when the path changes (e.g. due to a rename while the handle is open).
type PathHolder interface {
	UpdateStagingPath(newPath string)
}

// StagingClient defines the minimal interface that StagingFS needs from the backend storage
type StagingClient interface {
	DownloadFileParallel(irodsPath string, localPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error
	UploadFileParallel(localPath string, irodsPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error
	RenameFileToFile(srcPath string, destPath string) error
	RenameDirToDir(srcPath string, destPath string) error
	RemoveFile(path string, force bool) error
	MakeDir(path string, recurse bool) error
	RemoveDir(path string, recurse bool, force bool) error
}

// SyncErrorHandler is called when a background sync fails for an item
type SyncErrorHandler func(meta *StagingMetadata, err error)

// StagingFSConfig holds configuration for StagingFS
type StagingFSConfig struct {
	LocalRootPath    string           // Local base directory for staging (e.g., /staging)
	Client           StagingClient    // Backend storage client
	SyncInterval     time.Duration    // How often the background worker runs (default: 5s)
	GracePeriod      time.Duration    // Items older than this are synced (default: 10s)
	MaxDataSize      int64            // Max total disk usage for staged data (default: 10GB, 0 = use default)
	MaxCacheFileSize int64            // Files larger than this are not kept as read cache after sync (default: 1GB, 0 = cache all)
	OnSyncError      SyncErrorHandler // Called when background sync fails for an item (optional)
}

const DefaultMaxDataSize = 10 * 1024 * 1024 * 1024     // 10GB
const DefaultMaxCacheFileSize = 1 * 1024 * 1024 * 1024 // 1GB

const MaxSyncFailCount = 3

// StagingFS manages local file staging and metadata tracking
type StagingFS struct {
	config           *StagingFSConfig
	sm               *StagingStateManager
	client           StagingClient
	stopCh           chan struct{}
	stopOnce         sync.Once
	workerWg         sync.WaitGroup
	sizeMutex        sync.Mutex
	currentSize      int64 // current total staged data size (dirty + cached)
	maxSize          int64 // max allowed data size
	maxCacheFileSize int64 // files larger than this skip the read cache after sync
	failedMutex      sync.Mutex
	failedItems      map[string]*StagingMetadata // items that exceeded max retry count
	cacheMutex       sync.Mutex
	cachedItems      map[string]*StagingMetadata // files that are synced and kept as read cache
	cachedDirs       map[string]*StagingMetadata // directories synced while backend listings may still be stale
	refMu            sync.Mutex
	openRefs         map[string]int // path → number of open write handles; sync skips these paths
	handlesMu        sync.Mutex
	handles          map[string][]PathHolder // path → open write handles (for rename path propagation)
	pathSizesMu      sync.Mutex
	pathSizes        map[string]int64 // per-path tracked sizes for accurate currentSize accounting
}

// NewStagingFS creates a new StagingFS with memory-only state manager
func NewStagingFS(config *StagingFSConfig) (*StagingFS, error) {
	if config == nil {
		return nil, errors.New("config is required")
	}
	if config.LocalRootPath == "" {
		return nil, errors.New("LocalRootPath is required")
	}
	if config.Client == nil {
		return nil, errors.New("Client is required")
	}

	if err := os.MkdirAll(config.LocalRootPath, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create root directory")
	}

	metaPath := filepath.Join(config.LocalRootPath, "meta")
	if err := os.MkdirAll(metaPath, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create meta directory")
	}

	dataPath := filepath.Join(config.LocalRootPath, "data")
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create data directory")
	}

	sm := NewStagingStateManager()

	maxSize := config.MaxDataSize
	if maxSize == 0 {
		maxSize = DefaultMaxDataSize
	}

	maxCacheFileSize := config.MaxCacheFileSize
	if maxCacheFileSize == 0 {
		maxCacheFileSize = DefaultMaxCacheFileSize
	}

	sf := &StagingFS{
		config:           config,
		sm:               sm,
		client:           config.Client,
		stopCh:           make(chan struct{}),
		maxSize:          maxSize,
		maxCacheFileSize: maxCacheFileSize,
		failedItems:      make(map[string]*StagingMetadata),
		cachedItems:      make(map[string]*StagingMetadata),
		cachedDirs:       make(map[string]*StagingMetadata),
		openRefs:         make(map[string]int),
		handles:          make(map[string][]PathHolder),
		pathSizes:        make(map[string]int64),
	}

	sf.currentSize = sf.computeDataDirSize()
	sf.cleanOrphanFiles()
	sf.registerDefaultHandler()
	sf.startBackgroundWorker()

	return sf, nil
}

// NewStagingFSWithPersistence creates a new StagingFS with Badger persistence
func NewStagingFSWithPersistence(config *StagingFSConfig) (*StagingFS, error) {
	if config == nil {
		return nil, errors.New("config is required")
	}
	if config.LocalRootPath == "" {
		return nil, errors.New("LocalRootPath is required")
	}
	if config.Client == nil {
		return nil, errors.New("Client is required")
	}

	if err := os.MkdirAll(config.LocalRootPath, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create root directory")
	}

	metaPath := filepath.Join(config.LocalRootPath, "meta")
	if err := os.MkdirAll(metaPath, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create meta directory")
	}

	dataPath := filepath.Join(config.LocalRootPath, "data")
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create data directory")
	}

	// Open Badger database
	opts := badger.DefaultOptions(metaPath)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open Badger database")
	}

	sm := NewStagingStateManagerWithPersistence(db)
	if err := sm.Restore(); err != nil {
		return nil, errors.Wrap(err, "failed to restore from Badger")
	}

	maxSize := config.MaxDataSize
	if maxSize == 0 {
		maxSize = DefaultMaxDataSize
	}

	maxCacheFileSize := config.MaxCacheFileSize
	if maxCacheFileSize == 0 {
		maxCacheFileSize = DefaultMaxCacheFileSize
	}

	sf := &StagingFS{
		config:           config,
		sm:               sm,
		client:           config.Client,
		stopCh:           make(chan struct{}),
		maxSize:          maxSize,
		maxCacheFileSize: maxCacheFileSize,
		failedItems:      make(map[string]*StagingMetadata),
		cachedItems:      make(map[string]*StagingMetadata),
		cachedDirs:       make(map[string]*StagingMetadata),
		openRefs:         make(map[string]int),
		handles:          make(map[string][]PathHolder),
		pathSizes:        make(map[string]int64),
	}

	sf.currentSize = sf.computeDataDirSize()
	sf.cleanOrphanFiles()
	sf.registerDefaultHandler()
	sf.startBackgroundWorker()

	return sf, nil
}

// getLocalDataPath returns the local file path for an iRODS path
// Converts /iplant/home/user/test.txt to /staging/data/iplant/home/user/test.txt
func (sf *StagingFS) getLocalDataPath(path string) string {
	// Remove leading slash if present
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	return filepath.Join(sf.config.LocalRootPath, "data", path)
}

// Create creates a new file
func (sf *StagingFS) Create(path string) error {
	if err := sf.sm.Create(path); err != nil {
		return err
	}

	localPath := sf.getLocalDataPath(path)
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return errors.Wrap(err, "failed to create parent directory")
	}

	// Create empty file
	f, err := os.Create(localPath)
	if err != nil {
		sf.sm.Delete(path) // Cleanup on error
		return errors.Wrap(err, "failed to create local file")
	}
	f.Close()

	return nil
}

// AcquireRef increments the open-handle ref count for path, preventing sync from touching it.
func (sf *StagingFS) AcquireRef(path string) {
	sf.refMu.Lock()
	sf.openRefs[path]++
	sf.refMu.Unlock()
}

// ReleaseRef decrements the open-handle ref count for path.
func (sf *StagingFS) ReleaseRef(path string) {
	sf.refMu.Lock()
	sf.openRefs[path]--
	if sf.openRefs[path] <= 0 {
		delete(sf.openRefs, path)
	}
	sf.refMu.Unlock()
}

// RegisterHandle records a write handle so it can be notified on rename.
func (sf *StagingFS) RegisterHandle(path string, h PathHolder) {
	sf.handlesMu.Lock()
	sf.handles[path] = append(sf.handles[path], h)
	sf.handlesMu.Unlock()
}

// UnregisterHandle removes a write handle from the registry (called on Close).
func (sf *StagingFS) UnregisterHandle(path string, h PathHolder) {
	sf.handlesMu.Lock()
	list := sf.handles[path]
	for i, entry := range list {
		if entry == h {
			sf.handles[path] = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(sf.handles[path]) == 0 {
		delete(sf.handles, path)
	}
	sf.handlesMu.Unlock()
}

// hasOpenRef returns true if path currently has open write handles.
func (sf *StagingFS) hasOpenRef(path string) bool {
	sf.refMu.Lock()
	defer sf.refMu.Unlock()
	return sf.openRefs[path] > 0
}

// OpenForWrite opens a file for writing only.
// If bulk is true, the file is registered as ActionBulkUpload and deleted after sync (not cached).
func (sf *StagingFS) OpenForWrite(path string, bulk bool) (*os.File, error) {
	sf.sm.WaitForSync(path)

	if err := sf.ensureQuota(0); err != nil {
		return nil, err
	}

	localPath := sf.getLocalDataPath(path)

	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create parent directory")
	}

	if bulk {
		if err := sf.sm.CreateBulkUpload(path); err != nil {
			return nil, err
		}
	} else {
		meta := sf.sm.Get(path)
		if meta == nil {
			if err := sf.sm.Create(path); err != nil {
				return nil, err
			}
		} else if meta.Action != ActionUpload {
			if err := sf.sm.Modify(path); err != nil {
				return nil, err
			}
		}
	}

	f, err := os.OpenFile(localPath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open local file for writing")
	}

	sf.AcquireRef(path)
	return f, nil
}

// OpenForRead opens a staged file for reading only. Returns an error if the file
// is not present locally in staging.
func (sf *StagingFS) OpenForRead(path string) (*os.File, error) {
	localPath := sf.getLocalDataPath(path)

	f, err := os.Open(localPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open local file for reading")
	}

	return f, nil
}

// TruncateFile truncates a staged file to the given size.
func (sf *StagingFS) TruncateFile(path string, size int64) error {
	sf.sm.WaitForSync(path)

	localPath := sf.getLocalDataPath(path)

	if err := os.Truncate(localPath, size); err != nil {
		return errors.Wrap(err, "failed to truncate local file")
	}

	sf.setPathSize(path, size)

	// Update last modified time to reset grace period
	meta := sf.sm.Get(path)
	if meta != nil {
		meta.LastModifiedAt = time.Now()
	}

	return nil
}

// OpenForReadWrite opens a file for reading and writing (downloads from iRODS first).
// If bulk is true, the file is registered as ActionBulkUpload and deleted after sync (not cached).
func (sf *StagingFS) OpenForReadWrite(path string, bulk bool) (*os.File, error) {
	sf.sm.WaitForSync(path)

	if err := sf.ensureQuota(0); err != nil {
		return nil, err
	}

	localPath := sf.getLocalDataPath(path)

	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
			return nil, errors.Wrap(err, "failed to create parent directory")
		}

		if err := sf.client.DownloadFileParallel(path, localPath, 4, nil); err != nil {
			return nil, errors.Wrapf(err, "failed to download file from iRODS: %s", path)
		}

		if info, err := os.Stat(localPath); err == nil {
			sf.setPathSize(path, info.Size())
		}
	}

	if bulk {
		if err := sf.sm.CreateBulkUpload(path); err != nil {
			return nil, err
		}
	} else {
		meta := sf.sm.Get(path)
		if meta == nil || meta.Action != ActionUpload {
			if err := sf.sm.Modify(path); err != nil {
				return nil, err
			}
		}
	}

	f, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open local file for reading and writing")
	}

	sf.AcquireRef(path)
	return f, nil
}

// Rename renames a file
func (sf *StagingFS) Rename(oldPath, newPath string) error {
	syncNow, err := sf.sm.Rename(oldPath, newPath)
	if err != nil {
		return err
	}

	// Update cachedItems
	sf.cacheMutex.Lock()
	if cached, exists := sf.cachedItems[oldPath]; exists {
		delete(sf.cachedItems, oldPath)
		cached.Path = newPath
		sf.cachedItems[newPath] = cached
	}
	sf.cacheMutex.Unlock()

	// Update per-path size tracking
	sf.pathSizesMu.Lock()
	if size, exists := sf.pathSizes[oldPath]; exists {
		delete(sf.pathSizes, oldPath)
		sf.pathSizes[newPath] = size
	}
	sf.pathSizesMu.Unlock()

	// Move open refs and collect handles to notify (outside handlesMu to avoid deadlock)
	sf.refMu.Lock()
	if count, exists := sf.openRefs[oldPath]; exists {
		delete(sf.openRefs, oldPath)
		sf.openRefs[newPath] += count
	}
	sf.refMu.Unlock()

	sf.handlesMu.Lock()
	movedHandles := sf.handles[oldPath]
	delete(sf.handles, oldPath)
	if len(movedHandles) > 0 {
		sf.handles[newPath] = append(sf.handles[newPath], movedHandles...)
	}
	sf.handlesMu.Unlock()

	// Notify handles of the new path (after releasing handlesMu to avoid deadlock with Close)
	for _, h := range movedHandles {
		h.UpdateStagingPath(newPath)
	}

	oldLocalPath := sf.getLocalDataPath(oldPath)
	newLocalPath := sf.getLocalDataPath(newPath)

	if _, err := os.Stat(oldLocalPath); err != nil {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(newLocalPath), 0755); err != nil {
		return errors.Wrap(err, "failed to create parent directory")
	}

	if err := os.Rename(oldLocalPath, newLocalPath); err != nil {
		return errors.Wrap(err, "failed to rename local file")
	}

	_ = syncNow
	return nil
}

// RenameDir renames a directory
func (sf *StagingFS) RenameDir(oldPath, newPath string) error {
	syncNow, err := sf.sm.RenameDir(oldPath, newPath)
	if err != nil {
		return err
	}

	oldPrefix := oldPath + "/"

	// Update cachedItems under oldPath
	sf.cacheMutex.Lock()
	for p, cached := range sf.cachedItems {
		if p == oldPath || strings.HasPrefix(p, oldPrefix) {
			delete(sf.cachedItems, p)
			updated := newPath + p[len(oldPath):]
			cached.Path = updated
			sf.cachedItems[updated] = cached
		}
	}
	for p, cached := range sf.cachedDirs {
		if p == oldPath || strings.HasPrefix(p, oldPrefix) {
			delete(sf.cachedDirs, p)
			updated := newPath + p[len(oldPath):]
			cached.Path = updated
			sf.cachedDirs[updated] = cached
		}
	}
	sf.cacheMutex.Unlock()

	// Update per-path size tracking under oldPath
	sf.pathSizesMu.Lock()
	for p, size := range sf.pathSizes {
		if p == oldPath || strings.HasPrefix(p, oldPrefix) {
			delete(sf.pathSizes, p)
			sf.pathSizes[newPath+p[len(oldPath):]] = size
		}
	}
	sf.pathSizesMu.Unlock()

	// Move open refs for all paths under oldPath
	sf.refMu.Lock()
	for p, count := range sf.openRefs {
		if p == oldPath || strings.HasPrefix(p, oldPrefix) {
			delete(sf.openRefs, p)
			sf.openRefs[newPath+p[len(oldPath):]] += count
		}
	}
	sf.refMu.Unlock()

	// Collect and move handles under oldPath
	var toNotify []struct {
		h       PathHolder
		newPath string
	}
	sf.handlesMu.Lock()
	for p, list := range sf.handles {
		if p == oldPath || strings.HasPrefix(p, oldPrefix) {
			updated := newPath + p[len(oldPath):]
			delete(sf.handles, p)
			sf.handles[updated] = append(sf.handles[updated], list...)
			for _, h := range list {
				toNotify = append(toNotify, struct {
					h       PathHolder
					newPath string
				}{h, updated})
			}
		}
	}
	sf.handlesMu.Unlock()

	for _, n := range toNotify {
		n.h.UpdateStagingPath(n.newPath)
	}

	oldLocalPath := sf.getLocalDataPath(oldPath)
	newLocalPath := sf.getLocalDataPath(newPath)

	if _, err := os.Stat(oldLocalPath); err != nil {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(newLocalPath), 0755); err != nil {
		return errors.Wrap(err, "failed to create parent directory")
	}

	if err := os.Rename(oldLocalPath, newLocalPath); err != nil {
		return errors.Wrap(err, "failed to rename local directory")
	}

	_ = syncNow
	return nil
}

// Delete deletes a file
func (sf *StagingFS) Delete(path string) error {
	return sf.DeleteWithForce(path, false)
}

// DeleteWithForce deletes a file and preserves the force option for sync.
func (sf *StagingFS) DeleteWithForce(path string, force bool) error {
	if err := sf.sm.DeleteWithForce(path, force); err != nil {
		return err
	}

	// Remove from cached items if present
	sf.cacheMutex.Lock()
	delete(sf.cachedItems, path)
	sf.cacheMutex.Unlock()

	localPath := sf.getLocalDataPath(path)

	sf.removePathSize(path) // must be called before os.Remove for stat fallback
	if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "failed to delete local file")
	}

	return nil
}

// Mkdir creates a directory
func (sf *StagingFS) Mkdir(path string) error {
	if err := sf.sm.Mkdir(path); err != nil {
		return err
	}

	localPath := sf.getLocalDataPath(path)
	if err := os.MkdirAll(localPath, 0755); err != nil {
		sf.sm.Rmdir(path, true, true) // Cleanup on error
		return errors.Wrap(err, "failed to create local directory")
	}

	return nil
}

// RmdirWithOptions removes a directory and preserves its removal options for sync.
func (sf *StagingFS) Rmdir(path string, recurse bool, force bool) error {
	syncNow, err := sf.sm.Rmdir(path, recurse, force)
	if err != nil {
		return err
	}

	sf.cacheMutex.Lock()
	for cachedPath := range sf.cachedDirs {
		if pathInSubtree(cachedPath, path) {
			delete(sf.cachedDirs, cachedPath)
		}
	}
	sf.cacheMutex.Unlock()

	localPath := sf.getLocalDataPath(path)
	if err := os.RemoveAll(localPath); err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "failed to delete local directory")
	}

	// If immediate sync occurred, handler was already called by StagingStateManager
	// No additional cleanup needed here
	_ = syncNow

	return nil
}

// SyncAll performs all pending operations
func (sf *StagingFS) SyncAll() error {
	if err := sf.sm.SyncAll(); err != nil {
		return err
	}

	// Clean up local files after successful sync
	dataPath := filepath.Join(sf.config.LocalRootPath, "data")
	if err := os.RemoveAll(dataPath); err != nil {
		return errors.Wrap(err, "failed to clean up data directory")
	}

	sf.sizeMutex.Lock()
	sf.currentSize = 0
	sf.sizeMutex.Unlock()

	sf.pathSizesMu.Lock()
	sf.pathSizes = make(map[string]int64)
	sf.pathSizesMu.Unlock()

	return os.MkdirAll(dataPath, 0755)
}

// SyncOld syncs items older than grace period (10 seconds)
func (sf *StagingFS) SyncOld(gracePeriod time.Duration) error {
	// Get old paths before sync
	now := time.Now()
	var oldPaths []string
	all := sf.sm.GetAll()
	for path, meta := range all {
		if now.Sub(meta.LastModifiedAt) >= gracePeriod {
			oldPaths = append(oldPaths, path)
		}
	}

	// Sync old items
	if err := sf.sm.SyncOld(gracePeriod); err != nil {
		return err
	}

	// Clean up only the synced local files
	for _, path := range oldPaths {
		localPath := sf.getLocalDataPath(path)
		if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
			return errors.Wrapf(err, "failed to delete local file %s", path)
		}
	}

	return nil
}

// registerDefaultHandler registers the default iRODS operation handler
func (sf *StagingFS) registerDefaultHandler() {
	handler := func(meta *StagingMetadata) error {
		switch meta.Action {
		case ActionUpload, ActionBulkUpload:
			// Upload file to iRODS in parallel
			localPath := sf.getLocalDataPath(meta.Path)

			if err := sf.client.UploadFileParallel(localPath, meta.Path, 4, nil); err != nil {
				return errors.Wrapf(err, "failed to upload file in iRODS: %s", meta.Path)
			}

		case ActionRename:
			// Rename file in iRODS
			if err := sf.client.RenameFileToFile(meta.OldPath, meta.Path); err != nil {
				return errors.Wrapf(err, "failed to rename file in iRODS: %s -> %s", meta.OldPath, meta.Path)
			}

		case ActionRenameDir:
			// Rename directory in iRODS
			if err := sf.client.RenameDirToDir(meta.OldPath, meta.Path); err != nil {
				return errors.Wrapf(err, "failed to rename directory in iRODS: %s -> %s", meta.OldPath, meta.Path)
			}

		case ActionDelete:
			// Delete file from iRODS
			if err := sf.client.RemoveFile(meta.Path, meta.Force); err != nil {
				return errors.Wrapf(err, "failed to delete file in iRODS: %s", meta.Path)
			}

		case ActionMkdir:
			// Create directory in iRODS
			if err := sf.client.MakeDir(meta.Path, true); err != nil {
				return errors.Wrapf(err, "failed to create directory in iRODS: %s", meta.Path)
			}

		case ActionRmdir:
			// Remove directory from iRODS
			if err := sf.client.RemoveDir(meta.Path, meta.Recurse, meta.Force); err != nil {
				return errors.Wrapf(err, "failed to remove directory in iRODS: %s", meta.Path)
			}
		}

		return nil
	}

	sf.sm.RegisterActionHandler(handler)
}

// RegisterActionHandler registers a custom handler for iRODS operations
func (sf *StagingFS) RegisterActionHandler(handler ActionHandler) {
	sf.sm.RegisterActionHandler(handler)
}

// Close syncs all pending data, stops the background worker, and closes the staging filesystem
func (sf *StagingFS) Close() error {
	sf.stopOnce.Do(func() {
		close(sf.stopCh)
	})
	// The worker may already be inside syncOldItems when stopCh is closed.
	// Keep the backend client and staging state alive until that pass exits.
	sf.workerWg.Wait()

	if err := sf.SyncAll(); err != nil {
		log.WithError(err).Warnf("failed to sync all staged data on close")
	}

	if sf.sm.db != nil {
		if err := sf.sm.db.Close(); err != nil {
			return err
		}
	}

	// Remove staging directory after successful sync and DB close
	if sf.config.LocalRootPath != "" {
		os.RemoveAll(sf.config.LocalRootPath)
	}

	return nil
}

// startBackgroundWorker launches a goroutine that periodically syncs old items
func (sf *StagingFS) startBackgroundWorker() {
	syncInterval := sf.config.SyncInterval
	if syncInterval <= 0 {
		syncInterval = 5 * time.Second
	}

	gracePeriod := sf.config.GracePeriod
	if gracePeriod <= 0 {
		gracePeriod = 10 * time.Second
	}

	sf.workerWg.Add(1)
	go func() {
		defer sf.workerWg.Done()
		ticker := time.NewTicker(syncInterval)
		defer ticker.Stop()

		for {
			select {
			case <-sf.stopCh:
				return
			case <-ticker.C:
				sf.syncOldItems(gracePeriod)
			}
		}
	}()
}

// syncOldItems syncs items individually, reporting errors via callback without stopping
func (sf *StagingFS) syncOldItems(gracePeriod time.Duration) {
	attempted := make(map[string]bool)
	for {
		processed := false
		for _, meta := range sf.sm.getSyncCandidates(gracePeriod, false) {
			if attempted[meta.OperationID] {
				continue
			}
			attempted[meta.OperationID] = true
			processed = true

			// Skip files that currently have open write handles to avoid syncing mid-write
			if sf.hasOpenRef(meta.Path) {
				continue
			}

			if err := sf.sm.syncOne(meta); err != nil {
				log.WithError(err).Warnf("background sync failed for %s (%s), attempt %d", meta.Path, meta.Action, meta.SyncFailCount)

				if sf.config.OnSyncError != nil {
					sf.config.OnSyncError(meta, err)
				}

				if meta.SyncFailCount >= MaxSyncFailCount {
					sf.failedMutex.Lock()
					sf.failedItems[meta.Path] = meta
					sf.failedMutex.Unlock()

					sf.sm.markOperationBlockedPublic(meta.OperationID)
				}
				continue
			}

			// BulkUpload: delete local file immediately after sync (no caching).
			// Guard: a concurrent StageForBulkUpload may have already written a new file to
			// this staging path while syncOne was running. Only delete if no new metadata
			// entry was registered in the meantime.
			if meta.Action == ActionBulkUpload {
				if sf.sm.Get(meta.Path) == nil {
					localPath := sf.getLocalDataPath(meta.Path)
					sf.removePathSize(meta.Path)
					os.Remove(localPath)
				}
			} else {
				sf.transitionToCached(meta)
			}
		}
		if !processed {
			return
		}
	}
}

// transitionToCached moves a successfully synced file into the cached items map
func (sf *StagingFS) transitionToCached(meta *StagingMetadata) {
	if meta.Action == ActionMkdir {
		copy := *meta
		copy.FileState = StagingFileCached
		sf.cacheMutex.Lock()
		sf.cachedDirs[meta.Path] = &copy
		sf.cacheMutex.Unlock()
		return
	}

	// Only files with local data can be cached
	if meta.Action != ActionUpload {
		return
	}

	// If a new dirty entry was registered for this path while syncOne was running
	// (e.g. a concurrent OpenForWrite), adding a stale cache entry would be misleading.
	// The next sync cycle will re-cache correctly once the new writes are synced.
	if sf.sm.Get(meta.Path) != nil {
		return
	}

	localPath := sf.getLocalDataPath(meta.Path)
	info, err := os.Stat(localPath)
	if err != nil {
		return
	}

	// Large files are not worth caching locally — delete immediately after sync
	if info.Size() > sf.maxCacheFileSize {
		sf.removePathSize(meta.Path) // must be called before os.Remove for stat fallback
		os.Remove(localPath)
		return
	}

	now := time.Now()
	cached := &StagingMetadata{
		Path:           meta.Path,
		Action:         meta.Action,
		IsNew:          meta.IsNew,
		CreatedAt:      meta.CreatedAt,
		LastModifiedAt: meta.LastModifiedAt,
		FileState:      StagingFileCached,
		LastAccessedAt: now,
	}

	sf.cacheMutex.Lock()
	sf.cachedItems[meta.Path] = cached
	sf.cacheMutex.Unlock()

	// Proactively evict old cached files if total size exceeds the quota
	sf.sizeMutex.Lock()
	overflow := sf.currentSize - sf.maxSize
	sf.sizeMutex.Unlock()
	if overflow > 0 {
		sf.evictCachedOldest(overflow)
	}
}

// GetCachedItems returns all files currently kept as read cache
func (sf *StagingFS) GetCachedItems() map[string]*StagingMetadata {
	sf.cacheMutex.Lock()
	defer sf.cacheMutex.Unlock()

	result := make(map[string]*StagingMetadata, len(sf.cachedItems))
	for k, v := range sf.cachedItems {
		copy := *v
		result[k] = &copy
	}
	return result
}

// GetCachedDirs returns directories recently synced by staging. These entries
// keep directory traversal complete while the backend listing cache catches up.
func (sf *StagingFS) GetCachedDirs() map[string]*StagingMetadata {
	sf.cacheMutex.Lock()
	defer sf.cacheMutex.Unlock()

	result := make(map[string]*StagingMetadata, len(sf.cachedDirs))
	for k, v := range sf.cachedDirs {
		copy := *v
		result[k] = &copy
	}
	return result
}

// GetLocalDataPath returns the local file path for an iRODS path (exported for external use)
func (sf *StagingFS) GetLocalDataPath(path string) string {
	return sf.getLocalDataPath(path)
}

// EvictCachedFile removes a file from the cached items and deletes the local copy.
// This is called after a bulk upload (UploadFile/UploadFileParallel) so the freshly-uploaded
// data does not leave a stale local cache entry. Only affects StagingFileCached entries;
// dirty (pending sync) entries are left untouched.
func (sf *StagingFS) EvictCachedFile(path string) {
	sf.cacheMutex.Lock()
	_, exists := sf.cachedItems[path]
	if exists {
		delete(sf.cachedItems, path)
	}
	sf.cacheMutex.Unlock()

	if !exists {
		return
	}

	localPath := sf.getLocalDataPath(path)
	sf.removePathSize(path) // must be called before os.Remove for stat fallback
	os.Remove(localPath)
}

// StageForBulkUpload copies localPath into the staging directory under irodsPath and registers
// it as ActionBulkUpload. The background sync worker uploads it to iRODS and then immediately
// deletes the local copy (unlike ActionUpload which keeps the file as a read cache).
func (sf *StagingFS) StageForBulkUpload(localPath, irodsPath string) error {
	info, err := os.Stat(localPath)
	if err != nil {
		return errors.Wrap(err, "failed to stat source file")
	}

	if err := sf.ensureQuota(info.Size()); err != nil {
		return err
	}

	// Evict any existing cached entry for this path
	sf.EvictCachedFile(irodsPath)

	stagingPath := sf.getLocalDataPath(irodsPath)
	if err := os.MkdirAll(filepath.Dir(stagingPath), 0755); err != nil {
		return errors.Wrap(err, "failed to create staging directory")
	}

	if err := copyFile(localPath, stagingPath); err != nil {
		return errors.Wrap(err, "failed to copy file to staging")
	}

	sf.setPathSize(irodsPath, info.Size())

	if err := sf.sm.CreateBulkUpload(irodsPath); err != nil {
		os.Remove(stagingPath)
		sf.removePathSize(irodsPath)
		return errors.Wrap(err, "failed to register bulk upload in staging")
	}

	return nil
}

// copyFile copies src to dst atomically via a temp file in the same directory.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}

	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}

	return os.Rename(tmp, dst)
}

// GetLocalFileSize returns the size of the local staged file, or -1 if not found
func (sf *StagingFS) GetLocalFileSize(path string) int64 {
	localPath := sf.getLocalDataPath(path)
	info, err := os.Stat(localPath)
	if err != nil {
		return -1
	}
	return info.Size()
}

// IsRenamedFrom checks if the given path was renamed away by any staging entry.
// Returns true if some entry has OldPath == path (meaning this path no longer exists).
func (sf *StagingFS) IsRenamedFrom(path string) bool {
	return sf.sm.IsRenamedFrom(path)
}

func (sf *StagingFS) GetPendingRenames() []*StagingMetadata {
	return sf.sm.GetPendingRenames()
}

// Get retrieves metadata for a path
func (sf *StagingFS) Get(path string) *StagingMetadata {
	return sf.sm.Get(path)
}

// GetAll retrieves all staged metadata
func (sf *StagingFS) GetAll() map[string]*StagingMetadata {
	return sf.sm.GetAll()
}

// GetFailedItems returns all items that exceeded the max retry count
func (sf *StagingFS) GetFailedItems() map[string]*StagingMetadata {
	sf.failedMutex.Lock()
	defer sf.failedMutex.Unlock()

	result := make(map[string]*StagingMetadata, len(sf.failedItems))
	for k, v := range sf.failedItems {
		result[k] = v
	}
	return result
}

// ClearFailedItems removes all failed items
func (sf *StagingFS) ClearFailedItems() {
	sf.failedMutex.Lock()
	sf.failedItems = make(map[string]*StagingMetadata)
	sf.failedMutex.Unlock()
	sf.sm.retryBlockedOperations()
}

// Clear clears all metadata and local files
func (sf *StagingFS) Clear() error {
	if err := sf.sm.Clear(); err != nil {
		return err
	}

	// Clean up local data directory
	dataPath := filepath.Join(sf.config.LocalRootPath, "data")
	if err := os.RemoveAll(dataPath); err != nil {
		return errors.Wrap(err, "failed to remove data directory")
	}

	sf.sizeMutex.Lock()
	sf.currentSize = 0
	sf.sizeMutex.Unlock()

	sf.pathSizesMu.Lock()
	sf.pathSizes = make(map[string]int64)
	sf.pathSizesMu.Unlock()

	// Recreate data directory
	return os.MkdirAll(dataPath, 0755)
}

// GetCurrentDataSize returns the current total staged data size
func (sf *StagingFS) GetCurrentDataSize() int64 {
	sf.sizeMutex.Lock()
	defer sf.sizeMutex.Unlock()
	return sf.currentSize
}

// GetMaxDataSize returns the configured max data size
func (sf *StagingFS) GetMaxDataSize() int64 {
	return sf.maxSize
}

// GetAvailableDataSize returns remaining disk quota
func (sf *StagingFS) GetAvailableDataSize() int64 {
	sf.sizeMutex.Lock()
	defer sf.sizeMutex.Unlock()
	return sf.maxSize - sf.currentSize
}

// ensureQuota ensures there is room for size additional bytes.
// If not, it evicts cached files (oldest by LastAccessedAt first),
// then force-syncs and deletes pending staging files (oldest by LastModifiedAt first).
func (sf *StagingFS) ensureQuota(size int64) error {
	sf.sizeMutex.Lock()
	overflow := (sf.currentSize + size) - sf.maxSize
	sf.sizeMutex.Unlock()

	if overflow <= 0 {
		return nil
	}

	overflow -= sf.evictCachedOldest(overflow)
	if overflow <= 0 {
		return nil
	}

	overflow -= sf.forceSyncOldest(overflow)
	if overflow <= 0 {
		return nil
	}

	sf.sizeMutex.Lock()
	current := sf.currentSize
	sf.sizeMutex.Unlock()

	return errors.Mark(
		errors.Errorf("staging quota exceeded: current %d + requested %d > max %d", current, size, sf.maxSize),
		ErrQuotaExceeded,
	)
}

// evictCachedOldest removes the oldest cached files (by LastAccessedAt) until needed bytes are freed.
// Returns the number of bytes freed.
func (sf *StagingFS) evictCachedOldest(needed int64) int64 {
	sf.cacheMutex.Lock()
	type kv struct {
		path string
		meta *StagingMetadata
	}
	items := make([]kv, 0, len(sf.cachedItems))
	for p, m := range sf.cachedItems {
		items = append(items, kv{p, m})
	}
	sf.cacheMutex.Unlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].meta.LastAccessedAt.Before(items[j].meta.LastAccessedAt)
	})

	var freed int64
	for _, item := range items {
		if freed >= needed {
			break
		}

		sf.cacheMutex.Lock()
		_, exists := sf.cachedItems[item.path]
		if exists {
			delete(sf.cachedItems, item.path)
		}
		sf.cacheMutex.Unlock()

		if !exists {
			continue
		}

		localPath := sf.getLocalDataPath(item.path)
		freed += sf.getFileSize(item.path)
		sf.removePathSize(item.path)
		os.Remove(localPath)
	}
	return freed
}

// forceSyncOldest force-syncs the oldest pending upload files (by LastModifiedAt) and
// deletes their local copies to free space. Only files with no open refs are synced.
// Returns the number of bytes freed.
func (sf *StagingFS) forceSyncOldest(needed int64) int64 {
	all := sf.sm.GetAll()

	type kv struct {
		path string
		meta *StagingMetadata
	}
	var items []kv
	for p, m := range all {
		if (m.Action == ActionUpload || m.Action == ActionBulkUpload) && !sf.hasOpenRef(p) {
			items = append(items, kv{p, m})
		}
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].meta.LastModifiedAt.Before(items[j].meta.LastModifiedAt)
	})

	var freed int64
	for _, item := range items {
		if freed >= needed {
			break
		}

		if err := sf.sm.syncOne(item.meta); err != nil {
			log.WithError(err).Warnf("force-sync failed for %s during quota eviction", item.path)
			if sf.config.OnSyncError != nil {
				sf.config.OnSyncError(item.meta, err)
			}
			continue
		}

		// Guard: a concurrent write may have re-opened or re-registered this path
		if sf.hasOpenRef(item.path) || sf.sm.Get(item.path) != nil {
			continue
		}

		localPath := sf.getLocalDataPath(item.path)
		freed += sf.getFileSize(item.path)
		sf.removePathSize(item.path)
		os.Remove(localPath)
	}
	return freed
}

// setPathSize sets the tracked size for path and adjusts the global counter by the delta.
// Each call to setPathSize is idempotent with respect to the global total: only the delta
// from the previously-recorded size is applied, so calling it multiple times (Close after
// Truncate, etc.) never double-counts.
func (sf *StagingFS) setPathSize(path string, size int64) {
	sf.pathSizesMu.Lock()
	old := sf.pathSizes[path]
	sf.pathSizes[path] = size
	sf.pathSizesMu.Unlock()
	sf.AdjustDataSize(size - old)
}

// removePathSize removes path from per-path tracking and subtracts its size from the global
// counter. If the path was not tracked (e.g. files from a previous session), it falls back
// to stat-ing the local file. Must be called BEFORE the file is deleted so the fallback stat
// can still find it.
func (sf *StagingFS) removePathSize(path string) {
	sf.pathSizesMu.Lock()
	old, exists := sf.pathSizes[path]
	delete(sf.pathSizes, path)
	sf.pathSizesMu.Unlock()

	if exists {
		sf.subtractDataSize(old)
	} else {
		localPath := sf.getLocalDataPath(path)
		if info, err := os.Stat(localPath); err == nil {
			sf.subtractDataSize(info.Size())
		}
	}
}

// getFileSize returns the tracked size for path, falling back to stat if not in pathSizes.
func (sf *StagingFS) getFileSize(path string) int64 {
	sf.pathSizesMu.Lock()
	size, exists := sf.pathSizes[path]
	sf.pathSizesMu.Unlock()
	if exists {
		return size
	}
	localPath := sf.getLocalDataPath(path)
	if info, err := os.Stat(localPath); err == nil {
		return info.Size()
	}
	return 0
}

// NotifyFileClosed is called when a write handle for path is closed. It reads the actual
// file size from disk and updates per-path tracking so the global counter reflects all
// bytes written via WriteAt since the handle was opened.
func (sf *StagingFS) NotifyFileClosed(path string) {
	localPath := sf.getLocalDataPath(path)
	if info, err := os.Stat(localPath); err == nil {
		sf.setPathSize(path, info.Size())
	}
}

// AdjustDataSize adjusts the tracked total data size by delta bytes.
// Positive delta = new bytes written (not previously counted); negative = bytes removed.
func (sf *StagingFS) AdjustDataSize(delta int64) {
	if delta > 0 {
		sf.addDataSize(delta)
	} else if delta < 0 {
		sf.subtractDataSize(-delta)
	}
}

// addDataSize adds to the tracked data size
func (sf *StagingFS) addDataSize(size int64) {
	sf.sizeMutex.Lock()
	defer sf.sizeMutex.Unlock()
	sf.currentSize += size
}

// subtractDataSize subtracts from the tracked data size
func (sf *StagingFS) subtractDataSize(size int64) {
	sf.sizeMutex.Lock()
	defer sf.sizeMutex.Unlock()
	sf.currentSize -= size
	if sf.currentSize < 0 {
		sf.currentSize = 0
	}
}

// computeDataDirSize walks the data directory and sums file sizes
func (sf *StagingFS) computeDataDirSize() int64 {
	dataPath := filepath.Join(sf.config.LocalRootPath, "data")
	var total int64
	filepath.Walk(dataPath, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

// cleanOrphanFiles removes data files that have no corresponding metadata entry.
// These are leftover from incomplete downloads that crashed before metadata was written.
func (sf *StagingFS) cleanOrphanFiles() {
	dataPath := filepath.Join(sf.config.LocalRootPath, "data")
	metadata := sf.sm.GetAll()

	filepath.Walk(dataPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(dataPath, filePath)
		if err != nil {
			return nil
		}
		irodsPath := "/" + relPath

		if _, exists := metadata[irodsPath]; !exists {
			size := info.Size()
			if removeErr := os.Remove(filePath); removeErr == nil {
				sf.subtractDataSize(size)
			}
		}

		return nil
	})
}
