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
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/dgraph-io/badger/v3"
	log "github.com/sirupsen/logrus"
)

// ErrQuotaExceeded is returned by ensureQuota when staging space cannot be freed sufficiently.
// Callers may use errors.Is to detect this and fall back to a non-staging path.
var ErrQuotaExceeded = errors.New("staging quota exceeded")

// ErrOpenWriteHandles is returned by SyncAll when staged files are still open
// for writing. Syncing those files could upload a partial snapshot and the
// subsequent cleanup would unlink data that is still being written.
var ErrOpenWriteHandles = errors.New("staging files are still open for writing")

// PathHolder is implemented by any object that holds a staging path reference and
// must be notified when the path changes (e.g. due to a rename while the handle is open).
type PathHolder interface {
	UpdateStagingPath(newPath string)
}

// handleRegistration is what StagingFS holds on behalf of one open handle. The
// path is kept current across renames, so the handle always releases exactly
// what it acquired even if it never learned about the rename itself.
type handleRegistration struct {
	path    string
	ownsRef bool // the registration also owns the open ref for path
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

// stagingStatClient is intentionally optional so existing StagingClient
// implementations remain compatible. Without it, successfully uploaded files
// are not retained as local read-cache entries because their freshness cannot
// be verified safely.
type stagingStatClient interface {
	Stat(path string) (*irodsclient_fs.Entry, error)
}

// SyncErrorHandler is called when a background sync fails for an item.
// Handlers are invoked one at a time from a dedicated goroutine that no staging
// operation waits on, so a handler may call back into StagingFS — Close
// included — without deadlocking against the sync that reported the error.
type SyncErrorHandler func(meta *StagingMetadata, err error)

// syncErrorEvent is one queued SyncErrorHandler invocation.
type syncErrorEvent struct {
	meta *StagingMetadata
	err  error
}

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
	currentSize      int64 // staged data size (dirty + cached) held under this root
	externalSize     int64 // bytes charged by holders outside the staging metadata, such as packed directory trees
	reservedSize     int64 // space promised to in-flight requests but not yet counted in currentSize
	maxSize          int64 // max allowed data size
	maxCacheFileSize int64 // files larger than this skip the read cache after sync
	failedMutex      sync.Mutex
	failedItems      map[string]*StagingMetadata // items that exceeded max retry count
	cacheMutex       sync.Mutex
	cachedItems      map[string]*StagingMetadata        // files that are synced and kept as read cache
	cachedDirs       map[string]*StagingMetadata        // directories synced while backend listings may still be stale
	refMu            sync.Mutex                         // guards openRefs, handles and handleRegistrations together
	openRefs         map[string]int                     // path → number of open write handles; sync skips these paths
	syncAllMu        sync.Mutex                         // serializes SyncAll with write-handle setup
	handles          map[string][]PathHolder            // path → open write handles (for rename path propagation)
	handleRegs       map[PathHolder]*handleRegistration // holder → what it holds, so a rename cannot orphan it
	pathSizesMu      sync.Mutex
	pathSizes        map[string]int64 // per-path tracked sizes for accurate currentSize accounting
	syncErrorMu      sync.Mutex
	syncErrorCond    *sync.Cond       // signals queued sync-error callbacks
	syncErrorQueue   []syncErrorEvent // pending SyncErrorHandler invocations
	syncErrorStopped bool             // no further callbacks are queued after Close
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
		handleRegs:       make(map[PathHolder]*handleRegistration),
		pathSizes:        make(map[string]int64),
	}

	sf.currentSize = sf.computeDataDirSize()
	sf.cleanOrphanFiles()
	sf.registerDefaultHandler()
	sf.startSyncErrorNotifier()
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
		handleRegs:       make(map[PathHolder]*handleRegistration),
		pathSizes:        make(map[string]int64),
	}

	sf.currentSize = sf.computeDataDirSize()
	sf.cleanOrphanFiles()
	sf.registerDefaultHandler()
	sf.startSyncErrorNotifier()
	sf.startBackgroundWorker()

	return sf, nil
}

// getLocalDataPath returns the local file path for an iRODS path
// Converts /iplant/home/user/test.txt to /staging/data/iplant/home/user/test.txt
func (sf *StagingFS) getLocalDataPath(path string) string {
	// Canonicalize first: without it a path such as "/../../etc/passwd" would
	// join to a location outside the staging data directory.
	path = cleanPath(path)
	return filepath.Join(sf.config.LocalRootPath, "data", strings.TrimPrefix(path, "/"))
}

// Create creates a new file
func (sf *StagingFS) Create(path string) error {
	path = cleanPath(path)
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
//
// Callers that own a PathHolder should use the Open*For methods and
// ReleaseHandle instead: a ref taken here is keyed by the path string, so a
// rename between this call and the matching ReleaseRef releases the wrong path.
func (sf *StagingFS) AcquireRef(path string) {
	path = cleanPath(path)
	sf.refMu.Lock()
	sf.openRefs[path]++
	sf.refMu.Unlock()
}

// ReleaseRef decrements the open-handle ref count for path.
func (sf *StagingFS) ReleaseRef(path string) {
	path = cleanPath(path)
	sf.refMu.Lock()
	sf.releaseRefUnlocked(path)
	sf.refMu.Unlock()
}

// releaseRefUnlocked drops one open ref for path. The caller must hold refMu.
func (sf *StagingFS) releaseRefUnlocked(path string) {
	sf.openRefs[path]--
	if sf.openRefs[path] <= 0 {
		delete(sf.openRefs, path)
	}
}

// RegisterHandle records a write handle so it can be notified on rename. The
// registration does not own the handle's open ref; the caller keeps releasing
// that with ReleaseRef.
func (sf *StagingFS) RegisterHandle(path string, h PathHolder) {
	path = cleanPath(path)
	sf.refMu.Lock()
	sf.registerHandleUnlocked(path, h, false)
	sf.refMu.Unlock()
}

// UnregisterHandle removes a write handle from the registry (called on Close).
// Only the registration is dropped; an owned open ref is not released, so
// handles opened through the Open*For methods must use ReleaseHandle instead.
func (sf *StagingFS) UnregisterHandle(path string, h PathHolder) {
	path = cleanPath(path)
	sf.refMu.Lock()
	if reg := sf.handleRegs[h]; reg != nil {
		path = reg.path
		delete(sf.handleRegs, h)
	}
	sf.removeHandleUnlocked(path, h)
	sf.refMu.Unlock()
}

// ReleaseHandle drops the open ref and the rename registration that an Open*For
// method took for holder, whatever path they have been renamed to since. It
// reports whether holder still held a registration: a handle that never took
// one (a read-only staged handle, say) cannot release someone else's ref.
func (sf *StagingFS) ReleaseHandle(holder PathHolder) bool {
	sf.refMu.Lock()
	defer sf.refMu.Unlock()

	reg := sf.handleRegs[holder]
	if reg == nil {
		return false
	}
	delete(sf.handleRegs, holder)
	sf.removeHandleUnlocked(reg.path, holder)
	if reg.ownsRef {
		sf.releaseRefUnlocked(reg.path)
	}
	return true
}

// acquireOpenHandle takes the open ref for path and, when holder is not nil,
// registers it for rename notification in the same critical section. Doing both
// at once is what keeps a rename from moving the ref to the new path while the
// handle is still only known by the old one.
func (sf *StagingFS) acquireOpenHandle(path string, holder PathHolder) {
	path = cleanPath(path)
	sf.refMu.Lock()
	sf.openRefs[path]++
	if holder != nil {
		sf.registerHandleUnlocked(path, holder, true)
	}
	sf.refMu.Unlock()
}

// registerHandleUnlocked records holder at path. The caller must hold refMu.
func (sf *StagingFS) registerHandleUnlocked(path string, holder PathHolder, ownsRef bool) {
	sf.handles[path] = append(sf.handles[path], holder)
	sf.handleRegs[holder] = &handleRegistration{path: path, ownsRef: ownsRef}
}

// removeHandleUnlocked drops holder from the registry at path. The caller must hold refMu.
func (sf *StagingFS) removeHandleUnlocked(path string, holder PathHolder) {
	list := sf.handles[path]
	for i, entry := range list {
		if entry == holder {
			sf.handles[path] = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(sf.handles[path]) == 0 {
		delete(sf.handles, path)
	}
}

// hasOpenRef returns true if path currently has open write handles.
func (sf *StagingFS) hasOpenRef(path string) bool {
	path = cleanPath(path)
	sf.refMu.Lock()
	defer sf.refMu.Unlock()
	return sf.openRefs[path] > 0
}

// OpenForWrite opens a file for writing only.
// If bulk is true, the file is registered as ActionBulkUpload and deleted after sync (not cached).
//
// The caller owns the resulting open ref and must release it with ReleaseRef.
// Callers that own a PathHolder should use OpenForWriteFor instead.
func (sf *StagingFS) OpenForWrite(path string, bulk bool) (*os.File, error) {
	return sf.OpenForWriteFor(path, bulk, nil)
}

// OpenForWriteFor opens path for writing on behalf of holder, taking the open
// ref and registering holder for rename notification together. A rename can
// then never move the ref to the new path while holder is still recorded (or
// released) under the old one. ReleaseHandle undoes both.
func (sf *StagingFS) OpenForWriteFor(path string, bulk bool, holder PathHolder) (*os.File, error) {
	path = cleanPath(path)
	// Keep SyncAll from passing its open-ref check until this handle is fully
	// registered. The ref itself protects the file after this method returns.
	sf.syncAllMu.Lock()
	defer sf.syncAllMu.Unlock()

	// The lease waits for an in-flight sync of this path and keeps a new one
	// from starting until the open ref is in place, so no sync can run against
	// the file between registering its metadata and handing out the handle.
	sf.sm.AcquireWriteLease(path)
	defer sf.sm.ReleaseWriteLease(path)

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
		switch {
		case meta == nil:
			if err := sf.sm.Create(path); err != nil {
				return nil, err
			}
		case meta.Action != ActionUpload:
			if err := sf.sm.Modify(path); err != nil {
				return nil, err
			}
		default:
			// The pending upload predates this handle. Restart its grace period
			// so it is not synced while the handle is still being written.
			if err := sf.sm.Touch(path); err != nil {
				return nil, errors.Wrap(err, "failed to refresh staging modification time")
			}
		}
	}

	// Open the staging file read-write even for a write-only request: callers
	// that created a file with O_RDWR (SQLite, for one) read it back through
	// the same handle, and a write-only descriptor fails those reads with
	// EBADF. Which operations are allowed stays a property of the caller's
	// handle, not of this local descriptor.
	f, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open local file for writing")
	}

	sf.acquireOpenHandle(path, holder)
	return f, nil
}

// OpenForRead opens a staged file for reading only. Returns an error if the file
// is not present locally in staging.
func (sf *StagingFS) OpenForRead(path string) (*os.File, error) {
	path = cleanPath(path)
	localPath := sf.getLocalDataPath(path)

	f, err := os.Open(localPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open local file for reading")
	}

	return f, nil
}

// OpenCachedForRead opens a completed staging read-cache entry. The cache lock
// keeps eviction from removing the entry before it is opened, and a successful
// open refreshes its LRU timestamp. found is false when path is not cached.
func (sf *StagingFS) OpenCachedForRead(path string) (file *os.File, metadata *StagingMetadata, found bool, err error) {
	path = cleanPath(path)
	sf.cacheMutex.Lock()
	defer sf.cacheMutex.Unlock()

	cached, found := sf.cachedItems[path]
	if !found {
		return nil, nil, false, nil
	}

	file, err = os.Open(sf.getLocalDataPath(path))
	if err != nil {
		return nil, nil, true, errors.Wrap(err, "failed to open cached file for reading")
	}

	cached.LastAccessedAt = time.Now()
	copy := *cached
	return file, &copy, true, nil
}

// TruncateFile truncates a staged file to the given size.
func (sf *StagingFS) TruncateFile(path string, size int64) error {
	path = cleanPath(path)
	// Hold the path against sync for the whole truncate: waiting for an
	// in-flight sync alone would still let a candidate selected moments earlier
	// upload the file halfway through this truncate.
	sf.sm.AcquireWriteLease(path)
	defer sf.sm.ReleaseWriteLease(path)

	localPath := sf.getLocalDataPath(path)

	if err := os.Truncate(localPath, size); err != nil {
		return errors.Wrap(err, "failed to truncate local file")
	}

	sf.setPathSize(path, size)

	// Update metadata, the operation DAG, and persistent state together so the
	// grace period is measured from this truncate rather than the prior write.
	if err := sf.sm.Touch(path); err != nil {
		return errors.Wrap(err, "failed to update truncate modification time")
	}

	return nil
}

// OpenForReadWrite opens a file for reading and writing (downloads from iRODS first).
// If bulk is true, the file is registered as ActionBulkUpload and deleted after sync (not cached).
func (sf *StagingFS) OpenForReadWrite(path string, bulk bool) (*os.File, error) {
	return sf.OpenForReadWriteFrom(path, path, bulk)
}

// OpenForReadWriteFor opens path for reading and writing on behalf of holder.
// See OpenForWriteFor for why the registration is taken with the open ref.
func (sf *StagingFS) OpenForReadWriteFor(path string, bulk bool, holder PathHolder) (*os.File, error) {
	return sf.OpenForReadWriteFromFor(path, path, bulk, holder)
}

// OpenForReadWriteFrom opens logicalPath for reading and writing, downloading
// its initial data from sourcePath when no local staged copy exists. The paths
// differ while an asynchronous rename is pending: local staging must remain at
// the new logical path while iRODS still exposes the old source path.
func (sf *StagingFS) OpenForReadWriteFrom(logicalPath string, sourcePath string, bulk bool) (*os.File, error) {
	return sf.OpenForReadWriteFromFor(logicalPath, sourcePath, bulk, nil)
}

// OpenForReadWriteFromFor opens logicalPath for reading and writing on behalf of
// holder, downloading from sourcePath when no local staged copy exists.
// See OpenForWriteFor for why the registration is taken with the open ref.
func (sf *StagingFS) OpenForReadWriteFromFor(logicalPath string, sourcePath string, bulk bool, holder PathHolder) (*os.File, error) {
	logicalPath = cleanPath(logicalPath)
	if sourcePath == "" {
		sourcePath = logicalPath
	} else {
		sourcePath = cleanPath(sourcePath)
	}

	// Keep SyncAll from passing its open-ref check until this handle is fully
	// registered. The ref itself protects the file after this method returns.
	sf.syncAllMu.Lock()
	defer sf.syncAllMu.Unlock()

	// See OpenForWrite: the lease spans metadata registration and the open.
	sf.sm.AcquireWriteLease(logicalPath)
	defer sf.sm.ReleaseWriteLease(logicalPath)

	if err := sf.ensureQuota(0); err != nil {
		return nil, err
	}

	localPath := sf.getLocalDataPath(logicalPath)

	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
			return nil, errors.Wrap(err, "failed to create parent directory")
		}

		if err := sf.downloadFileAtomically(sourcePath, localPath); err != nil {
			return nil, errors.Wrapf(err, "failed to download file from iRODS %q", sourcePath)
		}

		if info, err := os.Stat(localPath); err == nil {
			sf.setPathSize(logicalPath, info.Size())
		}
	}

	if bulk {
		if err := sf.sm.CreateBulkUpload(logicalPath); err != nil {
			return nil, err
		}
	} else {
		meta := sf.sm.Get(logicalPath)
		if meta == nil || meta.Action != ActionUpload {
			if err := sf.sm.Modify(logicalPath); err != nil {
				return nil, err
			}
		} else if err := sf.sm.Touch(logicalPath); err != nil {
			// Restart the pending upload's grace period; see OpenForWrite.
			return nil, errors.Wrap(err, "failed to refresh staging modification time")
		}
	}

	f, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open local file for reading and writing")
	}

	sf.acquireOpenHandle(logicalPath, holder)
	return f, nil
}

// downloadFileAtomically prevents an interrupted backend download from leaving
// a partial file at the final staging path. A later open may safely treat the
// final path as complete whenever it exists.
func (sf *StagingFS) downloadFileAtomically(sourcePath string, localPath string) error {
	tmp, err := os.CreateTemp(filepath.Dir(localPath), "."+filepath.Base(localPath)+".download-*")
	if err != nil {
		return errors.Wrap(err, "failed to create temporary download file")
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return errors.Wrap(err, "failed to close temporary download file")
	}
	defer os.Remove(tmpPath)

	if err := sf.client.DownloadFileParallel(sourcePath, tmpPath, 4, nil); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, localPath); err != nil {
		return errors.Wrap(err, "failed to publish downloaded staging file")
	}
	return nil
}

// Rename renames a file
func (sf *StagingFS) Rename(oldPath, newPath string) error {
	oldPath = cleanPath(oldPath)
	newPath = cleanPath(newPath)
	// Reserve both paths for the whole rename. Without this a background worker
	// could run the queued backend rename, or an upload depending on it, while
	// the local file still sits at the old path.
	sf.sm.AcquireWriteLease(oldPath)
	defer sf.sm.ReleaseWriteLease(oldPath)
	sf.sm.AcquireWriteLease(newPath)
	defer sf.sm.ReleaseWriteLease(newPath)

	oldLocalPath := sf.getLocalDataPath(oldPath)
	newLocalPath := sf.getLocalDataPath(newPath)

	// Move local data first: a failure here must leave no renamed state behind.
	renamedLocally := false
	if _, err := os.Stat(oldLocalPath); err == nil {
		if err := os.MkdirAll(filepath.Dir(newLocalPath), 0755); err != nil {
			return errors.Wrap(err, "failed to create parent directory")
		}
		if err := os.Rename(oldLocalPath, newLocalPath); err != nil {
			return errors.Wrap(err, "failed to rename local file")
		}
		renamedLocally = true
	}

	syncNow, err := sf.sm.Rename(oldPath, newPath)
	if err != nil {
		if rollbackErr := rollbackLocalRename(renamedLocally, newLocalPath, oldLocalPath); rollbackErr != nil {
			return errors.CombineErrors(err, rollbackErr)
		}
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

	// Move open refs, handles and their registrations together, so a handle that
	// has not been notified yet still releases the ref it actually holds.
	sf.refMu.Lock()
	if count, exists := sf.openRefs[oldPath]; exists {
		delete(sf.openRefs, oldPath)
		sf.openRefs[newPath] += count
	}
	movedHandles := sf.handles[oldPath]
	delete(sf.handles, oldPath)
	if len(movedHandles) > 0 {
		sf.handles[newPath] = append(sf.handles[newPath], movedHandles...)
	}
	for _, reg := range sf.handleRegs {
		if reg.path == oldPath {
			reg.path = newPath
		}
	}
	sf.refMu.Unlock()

	// Notify handles of the new path (after releasing refMu to avoid deadlock with Close)
	for _, h := range movedHandles {
		h.UpdateStagingPath(newPath)
	}

	_ = syncNow
	return nil
}

// rollbackLocalRename restores a local rename that was already applied when a
// later step of the same logical rename failed.
func rollbackLocalRename(renamed bool, currentPath string, originalPath string) error {
	if !renamed {
		return nil
	}
	if err := os.Rename(currentPath, originalPath); err != nil {
		return errors.Wrapf(err, "failed to restore local data at %q after a failed rename", originalPath)
	}
	return nil
}

// RenameDir renames a directory
func (sf *StagingFS) RenameDir(oldPath, newPath string) error {
	oldPath = cleanPath(oldPath)
	newPath = cleanPath(newPath)
	// Reserve both subtrees for the whole rename, so no descendant operation is
	// synced against a path that only half of this rename has moved yet.
	sf.sm.AcquireWriteLeaseSubtree(oldPath)
	defer sf.sm.ReleaseWriteLeaseSubtree(oldPath)
	sf.sm.AcquireWriteLeaseSubtree(newPath)
	defer sf.sm.ReleaseWriteLeaseSubtree(newPath)

	oldLocalPath := sf.getLocalDataPath(oldPath)
	newLocalPath := sf.getLocalDataPath(newPath)

	// Move local data first: a failure here must leave no renamed state behind.
	renamedLocally := false
	if _, err := os.Stat(oldLocalPath); err == nil {
		if err := os.MkdirAll(filepath.Dir(newLocalPath), 0755); err != nil {
			return errors.Wrap(err, "failed to create parent directory")
		}
		if err := os.Rename(oldLocalPath, newLocalPath); err != nil {
			return errors.Wrap(err, "failed to rename local directory")
		}
		renamedLocally = true
	}

	syncNow, err := sf.sm.RenameDir(oldPath, newPath)
	if err != nil {
		if rollbackErr := rollbackLocalRename(renamedLocally, newLocalPath, oldLocalPath); rollbackErr != nil {
			return errors.CombineErrors(err, rollbackErr)
		}
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

	// Move open refs, handles and their registrations under oldPath together
	var toNotify []struct {
		h       PathHolder
		newPath string
	}
	sf.refMu.Lock()
	for p, count := range sf.openRefs {
		if p == oldPath || strings.HasPrefix(p, oldPrefix) {
			delete(sf.openRefs, p)
			sf.openRefs[newPath+p[len(oldPath):]] += count
		}
	}
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
	for _, reg := range sf.handleRegs {
		if reg.path == oldPath || strings.HasPrefix(reg.path, oldPrefix) {
			reg.path = newPath + reg.path[len(oldPath):]
		}
	}
	sf.refMu.Unlock()

	for _, n := range toNotify {
		n.h.UpdateStagingPath(n.newPath)
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
	path = cleanPath(path)
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
	path = cleanPath(path)
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

// Rmdir removes a directory and preserves its removal options for sync.
// A non-recursive removal behaves like POSIX rmdir: it refuses a directory that
// still holds staged data instead of destroying that data recursively.
func (sf *StagingFS) Rmdir(path string, recurse bool, force bool) error {
	path = cleanPath(path)
	localPath := sf.getLocalDataPath(path)

	if !recurse {
		notEmpty, err := localDirHasEntries(localPath)
		if err != nil {
			return errors.Wrap(err, "failed to inspect local directory")
		}
		if notEmpty {
			// Refuse before any state is queued, so the caller's error leaves
			// the staged data and its pending operations untouched.
			return irodsclient_types.NewCollectionNotEmptyError(path)
		}
	}

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

	if recurse {
		if err := os.RemoveAll(localPath); err != nil && !os.IsNotExist(err) {
			return errors.Wrap(err, "failed to delete local directory")
		}
	} else if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
		if notEmpty, checkErr := localDirHasEntries(localPath); checkErr == nil && notEmpty {
			// Something was staged below path while the removal was queued.
			return irodsclient_types.NewCollectionNotEmptyError(path)
		}
		return errors.Wrap(err, "failed to delete local directory")
	}

	// If immediate sync occurred, handler was already called by StagingStateManager
	// No additional cleanup needed here
	_ = syncNow

	return nil
}

// localDirHasEntries reports whether the local staging directory holds anything.
// A missing directory holds nothing.
func localDirHasEntries(localPath string) (bool, error) {
	entries, err := os.ReadDir(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return len(entries) > 0, nil
}

// Drain uploads everything the staging area holds that can go right now,
// without holding off the clients still using it. It syncs path by path, the
// way the background worker does, so a file a writer holds is the only one left
// behind rather than a reason to stop the whole pass, and local files are
// removed one at a time as their operation completes.
//
// SyncAll is the strict counterpart. It guarantees an empty staging area, and
// pays for that by refusing to run at all while a write handle is open and by
// keeping new ones from opening until it returns. Drain suits a session that is
// still serving; SyncAll suits one that is not, such as a staging area being
// closed.
func (sf *StagingFS) Drain() {
	sf.syncOldItems(0)
}

// SyncAll performs all pending operations
func (sf *StagingFS) SyncAll() error {
	// An open write handle may continue modifying its file after an upload.
	// Refuse the sync instead of uploading a partial snapshot and unlinking the
	// staging tree underneath the handle. Holding syncAllMu also prevents a new
	// handle from being opened between this check and cleanup.
	sf.syncAllMu.Lock()
	defer sf.syncAllMu.Unlock()

	sf.refMu.Lock()
	openPaths := 0
	openHandles := 0
	for _, count := range sf.openRefs {
		if count > 0 {
			openPaths++
			openHandles += count
		}
	}
	sf.refMu.Unlock()
	if openHandles > 0 {
		return errors.Wrapf(
			ErrOpenWriteHandles,
			"cannot sync staging data while %d write handle(s) across %d path(s) remain open",
			openHandles,
			openPaths,
		)
	}

	if err := sf.sm.SyncAll(); err != nil {
		return err
	}

	// Clean up local files after successful sync
	dataPath := filepath.Join(sf.config.LocalRootPath, "data")
	if err := os.RemoveAll(dataPath); err != nil {
		return errors.Wrap(err, "failed to clean up data directory")
	}

	// Only the staged data under this root was removed. Bytes charged by a
	// packed directory tree are still on disk under its own root, so clearing
	// them here would let the quota admit writes the disk cannot hold.
	sf.sizeMutex.Lock()
	sf.currentSize = 0
	sf.sizeMutex.Unlock()

	sf.pathSizesMu.Lock()
	sf.pathSizes = make(map[string]int64)
	sf.pathSizesMu.Unlock()

	return os.MkdirAll(dataPath, 0755)
}

// SyncOld syncs items older than gracePeriod and removes only local files whose
// upload operation actually completed. Pending, blocked, or newly replaced
// metadata keeps its local file for a later retry.
func (sf *StagingFS) SyncOld(gracePeriod time.Duration) error {
	// Keep only file uploads as cleanup candidates. Directory and namespace
	// operations do not own a single local regular file to remove.
	now := time.Now()
	oldUploads := make(map[string]struct{})
	all := sf.sm.GetAll()
	for path, meta := range all {
		if now.Sub(meta.LastModifiedAt) >= gracePeriod &&
			(meta.Action == ActionUpload || meta.Action == ActionBulkUpload) {
			oldUploads[path] = struct{}{}
		}
	}

	// Sync old items
	if err := sf.sm.SyncOld(gracePeriod); err != nil {
		return err
	}

	// A path is safe to clean only when no metadata remains for it. In
	// particular, this preserves data for blocked operations, operations that
	// became too new while syncing, and replacements registered concurrently.
	for path := range oldUploads {
		if sf.sm.Get(path) != nil {
			continue
		}
		localPath := sf.getLocalDataPath(path)
		sf.removePathSize(path)
		if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
			return errors.Wrapf(err, "failed to delete local file %q", path)
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
				return errors.Wrapf(err, "failed to upload file in iRODS %q", meta.Path)
			}

		case ActionRename:
			// Rename file in iRODS
			if err := sf.client.RenameFileToFile(meta.OldPath, meta.Path); err != nil {
				return errors.Wrapf(err, "failed to rename file in iRODS %q -> %q", meta.OldPath, meta.Path)
			}

		case ActionRenameDir:
			// Rename directory in iRODS
			if err := sf.client.RenameDirToDir(meta.OldPath, meta.Path); err != nil {
				return errors.Wrapf(err, "failed to rename directory in iRODS %q -> %q", meta.OldPath, meta.Path)
			}

		case ActionDelete:
			// Delete file from iRODS
			if err := sf.client.RemoveFile(meta.Path, meta.Force); err != nil {
				return errors.Wrapf(err, "failed to delete file in iRODS %q", meta.Path)
			}

		case ActionMkdir:
			// Create directory in iRODS
			if err := sf.client.MakeDir(meta.Path, true); err != nil {
				return errors.Wrapf(err, "failed to create directory in iRODS %q", meta.Path)
			}

		case ActionRmdir:
			// Remove directory from iRODS
			if err := sf.client.RemoveDir(meta.Path, meta.Recurse, meta.Force); err != nil {
				return errors.Wrapf(err, "failed to remove directory in iRODS %q", meta.Path)
			}
		}

		return nil
	}

	sf.sm.RegisterActionHandler(handler)
}

// RegisterActionHandler registers a custom handler for iRODS operations.
// See ActionHandler for the re-entrancy rules a handler must follow.
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
	sf.stopSyncErrorNotifier()

	syncErr := sf.SyncAll()
	if syncErr != nil {
		log.WithError(syncErr).Warnf("failed to sync all staged data on close; preserving staging data")
	}

	var dbCloseErr error
	if sf.sm.db != nil {
		if err := sf.sm.db.Close(); err != nil {
			dbCloseErr = err
		}
	}

	if syncErr != nil {
		return errors.CombineErrors(syncErr, dbCloseErr)
	}
	if dbCloseErr != nil {
		return dbCloseErr
	}

	// Remove staging data only after every pending operation was synced and
	// the persistent metadata database was closed successfully.
	if sf.config.LocalRootPath != "" {
		if err := os.RemoveAll(sf.config.LocalRootPath); err != nil {
			return errors.Wrap(err, "failed to remove staging directory after sync")
		}
	}

	return nil
}

// startSyncErrorNotifier launches the goroutine that delivers SyncErrorHandler
// callbacks. Delivery is deliberately detached from the goroutine that hit the
// error: a callback used to run inline on the background worker, so a handler
// calling Close deadlocked on the very worker it was running on.
func (sf *StagingFS) startSyncErrorNotifier() {
	if sf.config.OnSyncError == nil {
		return
	}

	sf.syncErrorCond = sync.NewCond(&sf.syncErrorMu)
	go func() {
		for {
			sf.syncErrorMu.Lock()
			for len(sf.syncErrorQueue) == 0 && !sf.syncErrorStopped {
				sf.syncErrorCond.Wait()
			}
			if len(sf.syncErrorQueue) == 0 {
				sf.syncErrorMu.Unlock()
				return
			}
			event := sf.syncErrorQueue[0]
			sf.syncErrorQueue = sf.syncErrorQueue[1:]
			sf.syncErrorMu.Unlock()

			sf.config.OnSyncError(event.meta, event.err)
		}
	}()
}

// notifySyncError queues a sync failure for delivery. Queuing never blocks, so
// neither a slow handler nor one that calls back into StagingFS can stall the
// sync that reported the error.
func (sf *StagingFS) notifySyncError(meta *StagingMetadata, err error) {
	if sf.config.OnSyncError == nil {
		return
	}

	sf.syncErrorMu.Lock()
	if !sf.syncErrorStopped {
		sf.syncErrorQueue = append(sf.syncErrorQueue, syncErrorEvent{meta: meta, err: err})
		sf.syncErrorCond.Signal()
	}
	sf.syncErrorMu.Unlock()
}

// stopSyncErrorNotifier lets the notifier exit once the queued callbacks are
// delivered. It never waits for delivery, because Close may itself be called
// from inside a callback.
func (sf *StagingFS) stopSyncErrorNotifier() {
	if sf.config.OnSyncError == nil {
		return
	}

	sf.syncErrorMu.Lock()
	sf.syncErrorStopped = true
	sf.syncErrorCond.Broadcast()
	sf.syncErrorMu.Unlock()
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

			executed, _, err := sf.sm.syncCandidate(meta, gracePeriod, false)
			if err != nil {
				log.WithError(err).Warnf("background sync failed for %s (%s), attempt %d", meta.Path, meta.Action, meta.SyncFailCount)

				sf.notifySyncError(meta, err)

				if meta.SyncFailCount >= MaxSyncFailCount {
					sf.failedMutex.Lock()
					sf.failedItems[meta.Path] = meta
					sf.failedMutex.Unlock()

					sf.sm.markOperationBlockedPublic(meta.OperationID)
				}
				continue
			}
			if !executed {
				// The operation was left for a later pass: another worker took
				// it, a local writer holds its data, or the candidate no longer
				// describes the queued operation. Nothing reached iRODS, so the
				// staged data stays where it is and the path is not recorded as
				// synced.
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

	// A local read-cache is usable only when it has an iRODS freshness stamp
	// captured immediately after its successful upload. If it cannot be
	// captured, prefer re-reading from iRODS over retaining unverifiable data.
	statClient, ok := sf.client.(stagingStatClient)
	if !ok {
		sf.removePathSize(meta.Path)
		_ = os.Remove(localPath)
		return
	}
	remoteEntry, err := statClient.Stat(meta.Path)
	if err != nil || remoteEntry == nil {
		if err != nil {
			log.WithError(err).Warnf("failed to stat synced file %q for staging cache freshness", meta.Path)
		}
		sf.removePathSize(meta.Path)
		_ = os.Remove(localPath)
		return
	}

	now := time.Now()
	cached := &StagingMetadata{
		Path:                 meta.Path,
		Action:               meta.Action,
		IsNew:                meta.IsNew,
		CreatedAt:            meta.CreatedAt,
		LastModifiedAt:       meta.LastModifiedAt,
		FileState:            StagingFileCached,
		LastAccessedAt:       now,
		RemoteSize:           remoteEntry.Size,
		RemoteModifyTime:     remoteEntry.ModifyTime,
		RemoteFreshnessKnown: true,
	}

	sf.cacheMutex.Lock()
	sf.cachedItems[meta.Path] = cached
	sf.cacheMutex.Unlock()

	// Proactively evict old cached files if total size exceeds the quota
	sf.sizeMutex.Lock()
	overflow := sf.usedSizeUnlocked() - sf.maxSize
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
	path = cleanPath(path)
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
	irodsPath = cleanPath(irodsPath)
	info, err := os.Stat(localPath)
	if err != nil {
		return errors.Wrap(err, "failed to stat source file")
	}

	if err := sf.reserveQuota(info.Size()); err != nil {
		return err
	}
	defer sf.releaseQuota(info.Size())

	// Wait for a sync of this path to finish and keep the next one from
	// starting, so the replacement is never uploaded or cleaned up half-staged.
	sf.sm.AcquireWriteLease(irodsPath)
	defer sf.sm.ReleaseWriteLease(irodsPath)

	// Evict any existing cached entry for this path
	sf.EvictCachedFile(irodsPath)

	stagingPath := sf.getLocalDataPath(irodsPath)
	if err := os.MkdirAll(filepath.Dir(stagingPath), 0755); err != nil {
		return errors.Wrap(err, "failed to create staging directory")
	}

	// Copy into a private temporary file first. The replacement must not appear
	// at its final path before its metadata exists: the cleanup that follows a
	// previous bulk upload of the same path deletes the local file whenever no
	// metadata is registered for it, and would delete the new data instead.
	tmpPath, err := copyFileToTemp(localPath, stagingPath)
	if err != nil {
		return errors.Wrap(err, "failed to copy file to staging")
	}
	defer os.Remove(tmpPath) // no-op once published below

	if err := sf.sm.CreateBulkUpload(irodsPath); err != nil {
		return errors.Wrap(err, "failed to register bulk upload in staging")
	}

	meta := sf.sm.Get(irodsPath)
	if err := os.Rename(tmpPath, stagingPath); err != nil {
		if meta != nil {
			_ = sf.sm.DiscardPendingOperation(irodsPath, meta.OperationID)
		}
		return errors.Wrap(err, "failed to publish staged file")
	}

	sf.setPathSize(irodsPath, info.Size())

	return nil
}

// copyFile copies src to dst atomically via a temp file in the same directory.
func copyFile(src, dst string) error {
	tmp, err := copyFileToTemp(src, dst)
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// copyFileToTemp copies src into a uniquely named temporary file next to dst and
// returns its path. The name is unique so that concurrent copies to the same
// destination cannot truncate or publish each other's partial data; the caller
// renames it into place, or removes it.
func copyFileToTemp(src, dst string) (string, error) {
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()

	out, err := os.CreateTemp(filepath.Dir(dst), "."+filepath.Base(dst)+".stage-*")
	if err != nil {
		return "", err
	}
	tmpPath := out.Name()

	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmpPath)
		return "", err
	}

	if err := out.Close(); err != nil {
		os.Remove(tmpPath)
		return "", err
	}

	return tmpPath, nil
}

// GetLocalFileSize returns the size of the local staged file, or -1 if not found
func (sf *StagingFS) GetLocalFileSize(path string) int64 {
	path = cleanPath(path)
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
	path = cleanPath(path)
	return sf.sm.IsRenamedFrom(path)
}

func (sf *StagingFS) GetPendingRenames() []*StagingMetadata {
	return sf.sm.GetPendingRenames()
}

// Get retrieves metadata for a path
func (sf *StagingFS) Get(path string) *StagingMetadata {
	path = cleanPath(path)
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

	// Only the staged data under this root was removed. Bytes charged by a
	// packed directory tree are still on disk under its own root, so clearing
	// them here would let the quota admit writes the disk cannot hold.
	sf.sizeMutex.Lock()
	sf.currentSize = 0
	sf.sizeMutex.Unlock()

	sf.pathSizesMu.Lock()
	sf.pathSizes = make(map[string]int64)
	sf.pathSizesMu.Unlock()

	// Recreate data directory
	return os.MkdirAll(dataPath, 0755)
}

// GetCurrentDataSize returns the current total staged data size, including the
// packed directory trees that share this quota.
func (sf *StagingFS) GetCurrentDataSize() int64 {
	sf.sizeMutex.Lock()
	defer sf.sizeMutex.Unlock()
	return sf.usedSizeUnlocked()
}

// usedSizeUnlocked is every byte charged against the quota, whatever holds it.
// The caller must hold sizeMutex.
func (sf *StagingFS) usedSizeUnlocked() int64 {
	return sf.currentSize + sf.externalSize
}

// GetMaxDataSize returns the configured max data size
func (sf *StagingFS) GetMaxDataSize() int64 {
	return sf.maxSize
}

// GetAvailableDataSize returns remaining disk quota, excluding space already
// promised to in-flight requests.
func (sf *StagingFS) GetAvailableDataSize() int64 {
	sf.sizeMutex.Lock()
	defer sf.sizeMutex.Unlock()
	return sf.maxSize - sf.usedSizeUnlocked() - sf.reservedSize
}

// ensureQuota ensures there is room for size additional bytes without holding on
// to it. Use it only where nothing is written yet (an open, say); anything that
// is about to consume the space must take it with reserveQuota instead.
func (sf *StagingFS) ensureQuota(size int64) error {
	if err := sf.reserveQuota(size); err != nil {
		return err
	}
	sf.releaseQuota(size)
	return nil
}

// reserveQuota makes room for size additional bytes and claims them, so that
// concurrent requests are not all told the same free space is theirs. It evicts
// cached files (oldest by LastAccessedAt first) and then force-syncs pending
// staging files (oldest by LastModifiedAt first) when the space is not already
// free. The caller must pass the same size to releaseQuota once the bytes are
// counted in currentSize, or once the request that needed them has failed.
func (sf *StagingFS) reserveQuota(size int64) error {
	// Reclaiming space can take several rounds: each attempt frees what it can
	// and then re-checks under the lock, since other requests reserve too.
	for attempt := 0; ; attempt++ {
		sf.sizeMutex.Lock()
		overflow := (sf.usedSizeUnlocked() + sf.reservedSize + size) - sf.maxSize
		if overflow <= 0 {
			sf.reservedSize += size
			sf.sizeMutex.Unlock()
			return nil
		}
		current := sf.usedSizeUnlocked()
		sf.sizeMutex.Unlock()

		if attempt >= 2 {
			return errors.Mark(
				errors.Newf("staging quota exceeded: current %d + requested %d > max %d", current, size, sf.maxSize),
				ErrQuotaExceeded,
			)
		}

		freed := sf.evictCachedOldest(overflow)
		if freed < overflow {
			freed += sf.forceSyncOldest(overflow - freed)
		}
		if freed <= 0 {
			return errors.Mark(
				errors.Newf("staging quota exceeded: current %d + requested %d > max %d", current, size, sf.maxSize),
				ErrQuotaExceeded,
			)
		}
	}
}

// ReserveSpace charges size bytes of staging disk to a holder outside the
// staging metadata, such as a packed directory's extracted tree.
//
// The bytes are counted in currentSize, so every other quota decision sees
// them, but no metadata is registered for them. That is deliberate: a packed
// directory must not be evicted or force-synced file by file, since the whole
// point of holding it is to upload it once as an archive. When staging is full
// the reservation therefore fails, and the write that needed it fails with it,
// rather than the tree being quietly uploaded the slow way.
func (sf *StagingFS) ReserveSpace(size int64) error {
	if size <= 0 {
		return nil
	}

	if err := sf.reserveQuota(size); err != nil {
		return err
	}

	sf.sizeMutex.Lock()
	sf.externalSize += size
	sf.sizeMutex.Unlock()

	sf.releaseQuota(size)
	return nil
}

// ReleaseSpace returns bytes charged by ReserveSpace.
func (sf *StagingFS) ReleaseSpace(size int64) {
	if size <= 0 {
		return
	}

	sf.sizeMutex.Lock()
	sf.externalSize -= size
	if sf.externalSize < 0 {
		sf.externalSize = 0
	}
	sf.sizeMutex.Unlock()
}

// releaseQuota gives back a reservation taken by reserveQuota.
func (sf *StagingFS) releaseQuota(size int64) {
	if size == 0 {
		return
	}

	sf.sizeMutex.Lock()
	sf.reservedSize -= size
	if sf.reservedSize < 0 {
		sf.reservedSize = 0
	}
	sf.sizeMutex.Unlock()
}

// ReserveFileGrowth accounts for a staged file growing to newSize before the
// bytes are written, and refuses the growth when the staging quota cannot cover
// it. Without this a single open handle could write past the quota, because the
// file size is otherwise only counted when the handle is closed.
func (sf *StagingFS) ReserveFileGrowth(path string, newSize int64) error {
	path = cleanPath(path)
	current := sf.getFileSize(path)
	if newSize <= current {
		return nil
	}

	growth := newSize - current
	if err := sf.reserveQuota(growth); err != nil {
		return err
	}
	// The bytes move from the reservation into the counted total.
	sf.growPathSize(path, newSize)
	sf.releaseQuota(growth)
	return nil
}

// growPathSize records path growing to newSize. Sizes never shrink here: a
// concurrent writer may already have recorded a larger size. An untracked path
// falls back to its size on disk, which the global counter already includes.
func (sf *StagingFS) growPathSize(path string, newSize int64) {
	sf.pathSizesMu.Lock()
	old, tracked := sf.pathSizes[path]
	if !tracked {
		if info, err := os.Stat(sf.getLocalDataPath(path)); err == nil {
			old = info.Size()
		}
	}
	if newSize <= old {
		sf.pathSizes[path] = old
		sf.pathSizesMu.Unlock()
		return
	}
	sf.pathSizes[path] = newSize
	sf.pathSizesMu.Unlock()

	sf.addDataSize(newSize - old)
}

// evictCachedOldest removes the oldest cached files (by LastAccessedAt) until needed bytes are freed.
// Returns the number of bytes freed.
func (sf *StagingFS) evictCachedOldest(needed int64) int64 {
	// Copy the access times out under the lock. Keeping the metadata pointers
	// instead would race with OpenCachedForRead refreshing LastAccessedAt.
	sf.cacheMutex.Lock()
	type kv struct {
		path           string
		lastAccessedAt time.Time
	}
	items := make([]kv, 0, len(sf.cachedItems))
	for p, m := range sf.cachedItems {
		items = append(items, kv{p, m.LastAccessedAt})
	}
	sf.cacheMutex.Unlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].lastAccessedAt.Before(items[j].lastAccessedAt)
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
			sf.notifySyncError(item.meta, err)
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
	path = cleanPath(path)
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
