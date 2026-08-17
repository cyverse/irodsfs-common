package stagingfs

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dgraph-io/badger/v3"
	log "github.com/sirupsen/logrus"
)

// StagingClient defines the minimal interface that StagingFS needs from the backend storage
type StagingClient interface {
	DownloadFileParallel(irodsPath string, localPath string, taskNum int) error
	UploadFileParallel(localPath string, irodsPath string, taskNum int) error
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
	OnSyncError      SyncErrorHandler // Called when background sync fails for an item (optional)
}

// StagingFS manages local file staging and metadata tracking
type StagingFS struct {
	config   *StagingFSConfig
	sm       *StagingStateManager
	client   StagingClient
	stopCh   chan struct{}
	stopOnce sync.Once
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

	sf := &StagingFS{
		config: config,
		sm:     sm,
		client: config.Client,
		stopCh: make(chan struct{}),
	}

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

	sf := &StagingFS{
		config: config,
		sm:     sm,
		client: config.Client,
		stopCh: make(chan struct{}),
	}

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

// OpenForWrite opens a file for writing only
func (sf *StagingFS) OpenForWrite(path string) (*os.File, error) {
	localPath := sf.getLocalDataPath(path)

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create parent directory")
	}

	// Check if file already exists or needs to be created
	meta := sf.sm.Get(path)
	if meta == nil {
		// File doesn't exist, create metadata
		if err := sf.sm.Create(path); err != nil {
			return nil, err
		}
	} else if meta.Action != ActionUpload {
		// If file exists but isn't in UPLOAD state, mark as modified
		if err := sf.sm.Modify(path); err != nil {
			return nil, err
		}
	}

	f, err := os.OpenFile(localPath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open local file for writing")
	}

	return f, nil
}

// OpenForRead opens a file for reading only (downloads from iRODS first)
func (sf *StagingFS) OpenForRead(path string) (*os.File, error) {
	localPath := sf.getLocalDataPath(path)

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create parent directory")
	}

	// Download file from iRODS if not already present locally
	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		// File doesn't exist locally, download from iRODS
		if err := sf.client.DownloadFileParallel(path, localPath, 4); err != nil {
			return nil, errors.Wrapf(err, "failed to download file from iRODS: %s", path)
		}
	}

	f, err := os.OpenFile(localPath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open local file for reading")
	}

	return f, nil
}

// OpenForReadWrite opens a file for reading and writing (downloads from iRODS first)
func (sf *StagingFS) OpenForReadWrite(path string) (*os.File, error) {
	localPath := sf.getLocalDataPath(path)

	// Download file from iRODS if not already present locally
	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		// Ensure parent directory exists
		if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
			return nil, errors.Wrap(err, "failed to create parent directory")
		}

		// File doesn't exist locally, download from iRODS
		if err := sf.client.DownloadFileParallel(path, localPath, 4); err != nil {
			return nil, errors.Wrapf(err, "failed to download file from iRODS: %s", path)
		}
	}

	// Mark as modified in staging metadata
	meta := sf.sm.Get(path)
	if meta == nil {
		// File doesn't exist in staging, create metadata
		if err := sf.sm.Modify(path); err != nil {
			return nil, err
		}
	} else if meta.Action != ActionUpload {
		// If file exists but isn't in UPLOAD state, mark as modified
		if err := sf.sm.Modify(path); err != nil {
			return nil, err
		}
	}

	f, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open local file for reading and writing")
	}

	return f, nil
}

// Rename renames a file
func (sf *StagingFS) Rename(oldPath, newPath string) error {
	syncNow, err := sf.sm.Rename(oldPath, newPath)
	if err != nil {
		return err
	}

	if syncNow {
		return nil
	}

	oldLocalPath := sf.getLocalDataPath(oldPath)
	newLocalPath := sf.getLocalDataPath(newPath)

	if err := os.MkdirAll(filepath.Dir(newLocalPath), 0755); err != nil {
		return errors.Wrap(err, "failed to create parent directory")
	}

	if err := os.Rename(oldLocalPath, newLocalPath); err != nil {
		return errors.Wrap(err, "failed to rename local file")
	}

	return nil
}

// RenameDir renames a directory
func (sf *StagingFS) RenameDir(oldPath, newPath string) error {
	syncNow, err := sf.sm.RenameDir(oldPath, newPath)
	if err != nil {
		return err
	}

	if syncNow {
		return nil
	}

	oldLocalPath := sf.getLocalDataPath(oldPath)
	newLocalPath := sf.getLocalDataPath(newPath)

	if err := os.MkdirAll(filepath.Dir(newLocalPath), 0755); err != nil {
		return errors.Wrap(err, "failed to create parent directory")
	}

	if err := os.Rename(oldLocalPath, newLocalPath); err != nil {
		return errors.Wrap(err, "failed to rename local directory")
	}

	return nil
}

// Delete deletes a file
func (sf *StagingFS) Delete(path string) error {
	if err := sf.sm.Delete(path); err != nil {
		return err
	}

	localPath := sf.getLocalDataPath(path)
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
		sf.sm.Rmdir(path) // Cleanup on error
		return errors.Wrap(err, "failed to create local directory")
	}

	return nil
}

// Rmdir removes a directory
func (sf *StagingFS) Rmdir(path string) error {
	syncNow, err := sf.sm.Rmdir(path)
	if err != nil {
		return err
	}

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
	// Handler will be invoked by StagingStateManager.SyncAll()
	// Handlers should be registered before calling SyncAll()

	if err := sf.sm.SyncAll(); err != nil {
		return err
	}

	// Clean up local files after successful sync
	dataPath := filepath.Join(sf.config.LocalRootPath, "data")
	if err := os.RemoveAll(dataPath); err != nil {
		return errors.Wrap(err, "failed to clean up data directory")
	}

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
		case ActionUpload:
			// Upload file to iRODS in parallel
			localPath := sf.getLocalDataPath(meta.Path)

			if err := sf.client.UploadFileParallel(localPath, meta.Path, 4); err != nil {
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
			if err := sf.client.RemoveFile(meta.Path, false); err != nil {
				return errors.Wrapf(err, "failed to delete file in iRODS: %s", meta.Path)
			}

		case ActionMkdir:
			// Create directory in iRODS
			if err := sf.client.MakeDir(meta.Path, true); err != nil {
				return errors.Wrapf(err, "failed to create directory in iRODS: %s", meta.Path)
			}

		case ActionRmdir:
			// Remove directory from iRODS
			if err := sf.client.RemoveDir(meta.Path, true, false); err != nil {
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

// Close stops the background worker and closes the staging filesystem
func (sf *StagingFS) Close() error {
	sf.stopOnce.Do(func() {
		close(sf.stopCh)
	})

	if sf.sm.db != nil {
		return sf.sm.db.Close()
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

	go func() {
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
	now := time.Now()
	all := sf.sm.GetAll()

	for _, meta := range all {
		if now.Sub(meta.LastModifiedAt) < gracePeriod {
			continue
		}

		if err := sf.sm.syncOne(meta); err != nil {
			log.Warnf("background sync failed for %s (%s): %v", meta.Path, meta.Action, err)
			if sf.config.OnSyncError != nil {
				sf.config.OnSyncError(meta, err)
			}
			continue
		}

		// Clean up local file after successful sync
		localPath := sf.getLocalDataPath(meta.Path)
		os.Remove(localPath)
	}
}

// GetLocalDataPath returns the local file path for an iRODS path (exported for external use)
func (sf *StagingFS) GetLocalDataPath(path string) string {
	return sf.getLocalDataPath(path)
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
	all := sf.sm.GetAll()
	for _, meta := range all {
		if meta.OldPath == path && (meta.Action == ActionRename || meta.Action == ActionRenameDir) {
			return true
		}
	}
	return false
}

// Get retrieves metadata for a path
func (sf *StagingFS) Get(path string) *StagingMetadata {
	return sf.sm.Get(path)
}

// GetAll retrieves all staged metadata
func (sf *StagingFS) GetAll() map[string]*StagingMetadata {
	return sf.sm.GetAll()
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

	// Recreate data directory
	return os.MkdirAll(dataPath, 0755)
}
