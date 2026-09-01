package irods

import (
	"encoding/binary"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
	irodsclient_metrics "github.com/cyverse/go-irodsclient/irods/metrics"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/irods/cache"
	"github.com/cyverse/irodsfs-common/irods/inode"
	"github.com/cyverse/irodsfs-common/irods/stagingfs"
	"github.com/cyverse/irodsfs-common/util"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

const DefaultMaxCacheFileSize = 10 * 1024 * 1024 * 1024 // 10GB

// IRODSFSClientBufferedConfig holds configuration for IRODSFSClientBuffered
type IRODSFSClientBufferedConfig struct {
	BlockSize int // Block size for read cache in bytes (default: 4MB)

	// Staging settings (leave StagingRootPath empty to disable staging/write support)
	StagingRootPath    string                     // Local path for staging files
	MaxStagingDataSize int64                      // Max disk usage for staged data (0 = use default 10GB)
	MaxCacheFileSize   int64                      // Max size of a single file kept as read cache after sync (0 = use default 1GB)
	SyncInterval       time.Duration              // Background sync interval (default: 5s)
	GracePeriod        time.Duration              // Grace period before sync (default: 10s)
	UsePersistence     bool                       // Use BadgerDB for crash recovery
	OnSyncError        stagingfs.SyncErrorHandler // Optional error callback
}

// IRODSFSClientBuffered wraps IRODSFSClient with block-level read-through caching
// and local staging for write/readwrite modes.
type IRODSFSClientBuffered struct {
	id               string
	fs               *irodsclient_fs.FileSystem
	client           *IRODSFSClientDirect
	maxCacheFileSize int64
	cache            *cache.MemoryCacheManager
	helper           *util.FileBlockHelper
	staging          *stagingfs.StagingFS
	inodeManager     *inode.InodeManager
	logger           *log.Entry
	cacheHit         uint64
	cacheMiss        uint64
}

// NewIRODSFSClientBuffered creates a new IRODSFSClientBuffered with the given config.
// The cache is provided externally so it can be shared across multiple clients.
func NewIRODSFSClientBuffered(fs *irodsclient_fs.FileSystem, cache *cache.MemoryCacheManager, config *IRODSFSClientBufferedConfig) (IRODSFSClient, error) {
	if fs == nil {
		return nil, errors.New("fs is required")
	}
	if cache == nil {
		return nil, errors.New("cache is required")
	}
	if config == nil {
		return nil, errors.New("config is required")
	}

	blockSize := config.BlockSize
	if blockSize <= 0 {
		blockSize = 4 * 1024 * 1024
	}

	maxCacheFileSize := config.MaxCacheFileSize
	if maxCacheFileSize == 0 {
		maxCacheFileSize = DefaultMaxCacheFileSize
	}

	// Create direct client
	client, err := NewIRODSFSClientDirect(fs)
	if err != nil {
		return nil, err
	}
	directClient := client.(*IRODSFSClientDirect)

	// Create staging filesystem (optional)
	var staging *stagingfs.StagingFS
	if config.StagingRootPath != "" {
		stagingConfig := &stagingfs.StagingFSConfig{
			LocalRootPath:    config.StagingRootPath,
			Client:           directClient,
			MaxDataSize:      config.MaxStagingDataSize,
			MaxCacheFileSize: config.MaxCacheFileSize,
			SyncInterval:     config.SyncInterval,
			GracePeriod:      config.GracePeriod,
			OnSyncError:      config.OnSyncError,
		}

		if config.UsePersistence {
			staging, err = stagingfs.NewStagingFSWithPersistence(stagingConfig)
		} else {
			staging, err = stagingfs.NewStagingFS(stagingConfig)
		}
		if err != nil {
			directClient.Release()
			return nil, errors.Wrap(err, "failed to create staging filesystem")
		}
	}

	var inodeManager *inode.InodeManager
	if config.StagingRootPath != "" {
		if config.UsePersistence {
			inodeManager, err = inode.NewInodeManagerWithPersistence(config.StagingRootPath)
			if err != nil {
				if staging != nil {
					staging.Close()
				}
				directClient.Release()
				return nil, errors.Wrap(err, "failed to create inode manager")
			}
		} else {
			inodeManager = inode.NewInodeManager()
		}
	}

	clientID := xid.New().String()
	logger := fs.GetLogger().WithFields(log.Fields{
		"fsclient_buffered_id": clientID,
	})

	return &IRODSFSClientBuffered{
		id:               clientID,
		fs:               fs,
		client:           directClient,
		maxCacheFileSize: maxCacheFileSize,
		cache:            cache,
		inodeManager:     inodeManager,
		helper:           util.NewFileBlockHelper(blockSize),
		staging:          staging,
		logger:           logger,
	}, nil
}

func (c *IRODSFSClientBuffered) Release() error {
	var releaseErr error

	if c.inodeManager != nil {
		releaseErr = errors.CombineErrors(releaseErr, c.inodeManager.Close())
		c.inodeManager = nil
	}

	if c.staging != nil {
		releaseErr = errors.CombineErrors(releaseErr, c.staging.Close())
		c.staging = nil
	}

	if c.client != nil {
		releaseErr = errors.CombineErrors(releaseErr, c.client.Release())
		c.client = nil
	}

	return releaseErr
}

func (c *IRODSFSClientBuffered) Sync() error {
	if c.staging == nil {
		return nil
	}

	c.logger.Info("syncing all staged data to iRODS")

	if err := c.staging.SyncAll(); err != nil {
		return errors.Wrap(err, "failed to sync staged data")
	}

	c.cache.Clear(true)

	return nil
}

func (c *IRODSFSClientBuffered) GetAccount() *irodsclient_types.IRODSAccount {
	return c.client.GetAccount()
}

func (c *IRODSFSClientBuffered) GetApplicationName() string {
	return c.client.GetApplicationName()
}

// GetFSClient returns iRODS fs client
func (c *IRODSFSClientBuffered) GetFSClient() *irodsclient_fs.FileSystem {
	return c.client.fs
}

func (c *IRODSFSClientBuffered) GetStagingFS() *stagingfs.StagingFS {
	return c.staging
}

func (c *IRODSFSClientBuffered) GetOpenConnections() int {
	return c.client.GetOpenConnections()
}

func (c *IRODSFSClientBuffered) GetMetrics() *irodsclient_metrics.IRODSMetrics {
	metrics := c.client.GetMetrics()
	metrics.IncreaseCounterForCacheHit(atomic.LoadUint64(&c.cacheHit))
	metrics.IncreaseCounterForCacheMiss(atomic.LoadUint64(&c.cacheMiss))
	return metrics
}

func (c *IRODSFSClientBuffered) List(dirPath string) ([]*irodsclient_fs.Entry, error) {
	entries, err := c.client.List(dirPath)
	if err != nil {
		entries = []*irodsclient_fs.Entry{}
	}

	if c.staging == nil {
		if err != nil {
			return nil, err
		}
		return entries, nil
	}

	// Build a map for quick lookup and modification
	entryMap := make(map[string]*irodsclient_fs.Entry)
	for _, e := range entries {
		c.reuseStagingInodeID(e)
		entryMap[e.Path] = e
	}

	// Apply staging state
	allMeta := c.staging.GetAll()
	// Successfully uploaded files move from pending metadata into cachedItems.
	// Keep them visible until the backend directory listing catches up.
	for cachedPath, cachedMeta := range c.staging.GetCachedItems() {
		if _, pending := allMeta[cachedPath]; !pending {
			allMeta[cachedPath] = cachedMeta
		}
	}
	for cachedPath, cachedMeta := range c.staging.GetCachedDirs() {
		if _, pending := allMeta[cachedPath]; !pending {
			allMeta[cachedPath] = cachedMeta
		}
	}

	// A directory's own MKDIR may finish before uploads below it. If the backend
	// listing is stale during that window, expose the first staged descendant as
	// an implied directory. Otherwise a recursive rm never descends into it, and
	// the unseen upload can recreate a child after rm has finished walking.
	if err := addImpliedStagingDirectories(entryMap, allMeta, dirPath, c.fs.GetAccount().ClientUser, c.inodeManager); err != nil {
		return nil, err
	}

	// RENAME may be a predecessor of the latest logical operation at the same
	// path (for example RENAME -> UPLOAD). Apply those DAG nodes explicitly so
	// the old name is hidden and the new name remains visible while sync waits.
	for _, meta := range c.staging.GetPendingRenames() {
		if path.Dir(meta.OldPath) == dirPath {
			delete(entryMap, meta.OldPath)
		}
		if path.Dir(meta.Path) != dirPath {
			continue
		}
		if meta.Action == stagingfs.ActionRenameDir {
			if err := c.inodeManager.RenameStagingEntryTree(meta.OldPath, meta.Path); err != nil {
				return nil, err
			}
		} else if err := c.inodeManager.RenameStagingEntry(meta.OldPath, meta.Path); err != nil {
			return nil, err
		}
		inodeID, err := c.inodeManager.CreateOrGetInodeIDForStagingEntry(meta.Path)
		if err != nil {
			return nil, err
		}
		entryType := irodsclient_fs.FileEntry
		if meta.Action == stagingfs.ActionRenameDir {
			entryType = irodsclient_fs.DirectoryEntry
		}
		now := time.Now()
		entryMap[meta.Path] = &irodsclient_fs.Entry{
			ID:         int64(inodeID),
			Type:       entryType,
			Name:       path.Base(meta.Path),
			Path:       meta.Path,
			Owner:      c.fs.GetAccount().ClientUser,
			Size:       max(c.staging.GetLocalFileSize(meta.Path), 0),
			CreateTime: now,
			ModifyTime: meta.LastModifiedAt,
			AccessTime: meta.LastModifiedAt,
		}
	}

	for _, meta := range allMeta {
		entryDir := path.Dir(meta.Path)
		if entryDir != dirPath {
			continue
		}
		if meta.Action == stagingfs.ActionRename || meta.Action == stagingfs.ActionRenameDir {
			continue
		}

		switch meta.Action {
		case stagingfs.ActionUpload, stagingfs.ActionBulkUpload:
			if meta.IsNew {
				inodeID, err := c.inodeManager.CreateOrGetInodeIDForStagingEntry(meta.Path)
				if err != nil {
					return nil, err
				}

				// New file created locally — add to listing
				size := c.staging.GetLocalFileSize(meta.Path)
				if size < 0 {
					size = 0
				}

				entryMap[meta.Path] = &irodsclient_fs.Entry{
					ID:         int64(inodeID),
					Type:       irodsclient_fs.FileEntry,
					Name:       path.Base(meta.Path),
					Path:       meta.Path,
					Owner:      c.fs.GetAccount().ClientUser,
					Size:       size,
					CreateTime: meta.CreatedAt,
					ModifyTime: meta.LastModifiedAt,
					AccessTime: meta.LastModifiedAt,
				}
			} else {
				// Existing file modified — update size
				if e, ok := entryMap[meta.Path]; ok {
					size := c.staging.GetLocalFileSize(meta.Path)
					if size >= 0 {
						e.Size = size
					}
					e.ModifyTime = meta.LastModifiedAt
				}
			}

		case stagingfs.ActionDelete:
			delete(entryMap, meta.Path)

		case stagingfs.ActionMkdir:
			inodeID, err := c.inodeManager.CreateOrGetInodeIDForStagingEntry(meta.Path)
			if err != nil {
				return nil, err
			}

			entryMap[meta.Path] = &irodsclient_fs.Entry{
				ID:         int64(inodeID),
				Type:       irodsclient_fs.DirectoryEntry,
				Name:       path.Base(meta.Path),
				Path:       meta.Path,
				Owner:      c.fs.GetAccount().ClientUser,
				Size:       0,
				CreateTime: meta.CreatedAt,
				ModifyTime: meta.LastModifiedAt,
				AccessTime: meta.LastModifiedAt,
			}

		case stagingfs.ActionRmdir:
			delete(entryMap, meta.Path)

		case stagingfs.ActionRename:
			if err := c.inodeManager.RenameStagingEntry(meta.OldPath, meta.Path); err != nil {
				return nil, err
			}
			inodeID, err := c.inodeManager.CreateOrGetInodeIDForStagingEntry(meta.Path)
			if err != nil {
				return nil, err
			}

			// Remove old path from this dir if present
			if path.Dir(meta.OldPath) == dirPath {
				delete(entryMap, meta.OldPath)
			}

			// Add a new path entry, or retain the issued staging inode if the
			// renamed entry is already visible in iRODS.
			if entry, ok := entryMap[meta.Path]; ok {
				entry.ID = int64(inodeID)
			} else {
				now := time.Now()
				entryMap[meta.Path] = &irodsclient_fs.Entry{
					ID:         int64(inodeID),
					Type:       irodsclient_fs.FileEntry,
					Name:       path.Base(meta.Path),
					Path:       meta.Path,
					Owner:      c.fs.GetAccount().ClientUser,
					Size:       0,
					CreateTime: now,
					ModifyTime: meta.LastModifiedAt,
					AccessTime: meta.LastModifiedAt,
				}
			}

		case stagingfs.ActionRenameDir:
			if err := c.inodeManager.RenameStagingEntryTree(meta.OldPath, meta.Path); err != nil {
				return nil, err
			}
			inodeID, err := c.inodeManager.CreateOrGetInodeIDForStagingEntry(meta.Path)
			if err != nil {
				return nil, err
			}

			if path.Dir(meta.OldPath) == dirPath {
				delete(entryMap, meta.OldPath)
			}

			if entry, ok := entryMap[meta.Path]; ok {
				entry.ID = int64(inodeID)
			} else {
				now := time.Now()
				entryMap[meta.Path] = &irodsclient_fs.Entry{
					ID:         int64(inodeID),
					Type:       irodsclient_fs.DirectoryEntry,
					Name:       path.Base(meta.Path),
					Path:       meta.Path,
					Owner:      c.fs.GetAccount().ClientUser,
					Size:       0,
					CreateTime: now,
					ModifyTime: meta.LastModifiedAt,
					AccessTime: meta.LastModifiedAt,
				}
			}
		}
	}

	// Also remove entries that were renamed away (OldPath in this dir)
	for _, meta := range allMeta {
		if meta.Action == stagingfs.ActionRename || meta.Action == stagingfs.ActionRenameDir {
			if path.Dir(meta.OldPath) == dirPath {
				delete(entryMap, meta.OldPath)
			}
		}
	}

	result := make([]*irodsclient_fs.Entry, 0, len(entryMap))
	for _, e := range entryMap {
		result = append(result, e)
	}
	return result, nil
}

func addImpliedStagingDirectories(entryMap map[string]*irodsclient_fs.Entry, allMeta map[string]*stagingfs.StagingMetadata, dirPath string, owner string, inodeManager *inode.InodeManager) error {
	prefix := strings.TrimSuffix(dirPath, "/") + "/"
	for _, meta := range allMeta {
		if meta.Action == stagingfs.ActionDelete || meta.Action == stagingfs.ActionRmdir {
			continue
		}

		if !strings.HasPrefix(meta.Path, prefix) {
			continue
		}
		rel := strings.TrimPrefix(meta.Path, prefix)
		parts := strings.SplitN(rel, "/", 2)
		if len(parts) < 2 {
			continue
		}

		childPath := path.Join(dirPath, parts[0])
		if _, exists := entryMap[childPath]; exists {
			continue
		}
		inodeID, inodeErr := inodeManager.CreateOrGetInodeIDForStagingEntry(childPath)
		if inodeErr != nil {
			return inodeErr
		}
		entryMap[childPath] = &irodsclient_fs.Entry{
			ID:         int64(inodeID),
			Type:       irodsclient_fs.DirectoryEntry,
			Name:       parts[0],
			Path:       childPath,
			Owner:      owner,
			CreateTime: meta.CreatedAt,
			ModifyTime: meta.LastModifiedAt,
			AccessTime: meta.LastModifiedAt,
		}
	}
	return nil
}

func (c *IRODSFSClientBuffered) Stat(filePath string) (*irodsclient_fs.Entry, error) {
	if c.staging != nil {
		// Check staging state first
		meta := c.staging.Get(filePath)
		if meta != nil {
			switch meta.Action {
			case stagingfs.ActionDelete, stagingfs.ActionRmdir:
				return nil, errors.Newf("file not found: %s", filePath)

			case stagingfs.ActionUpload:
				// Return entry with local file size
				size := c.staging.GetLocalFileSize(filePath)
				if size < 0 {
					size = 0
				}

				if meta.IsNew {
					inodeID, err := c.inodeManager.CreateOrGetInodeIDForStagingEntry(filePath)
					if err != nil {
						return nil, err
					}
					return &irodsclient_fs.Entry{
						ID:         int64(inodeID),
						Type:       irodsclient_fs.FileEntry,
						Name:       path.Base(filePath),
						Path:       filePath,
						Owner:      c.fs.GetAccount().ClientUser,
						Size:       size,
						CreateTime: meta.CreatedAt,
						ModifyTime: meta.LastModifiedAt,
						AccessTime: meta.LastModifiedAt,
					}, nil
				}

				// Modified existing file — get base entry from iRODS, override size
				entry, err := c.client.Stat(filePath)
				if err != nil {
					return nil, err
				}
				entry.Size = size
				entry.ModifyTime = meta.LastModifiedAt
				c.reuseStagingInodeID(entry)
				return entry, nil

			case stagingfs.ActionMkdir:
				inodeID, err := c.inodeManager.CreateOrGetInodeIDForStagingEntry(filePath)
				if err != nil {
					return nil, err
				}
				return &irodsclient_fs.Entry{
					ID:         int64(inodeID),
					Type:       irodsclient_fs.DirectoryEntry,
					Name:       path.Base(filePath),
					Path:       filePath,
					Owner:      c.fs.GetAccount().ClientUser,
					Size:       0,
					CreateTime: meta.CreatedAt,
					ModifyTime: meta.LastModifiedAt,
					AccessTime: meta.LastModifiedAt,
				}, nil
			}
		}

		// Check if this path was renamed away
		if c.staging.IsRenamedFrom(filePath) {
			return nil, errors.Newf("file not found: %s", filePath)
		}
	}

	entry, err := c.client.Stat(filePath)
	if err != nil {
		return nil, err
	}
	c.reuseStagingInodeID(entry)
	return entry, nil
}

func (c *IRODSFSClientBuffered) ExistsDir(dirPath string) bool {
	if c.staging != nil {
		meta := c.staging.Get(dirPath)
		if meta != nil {
			switch meta.Action {
			case stagingfs.ActionRmdir:
				return false
			case stagingfs.ActionMkdir:
				return true
			}
		}
		if c.staging.IsRenamedFrom(dirPath) {
			return false
		}
	}
	return c.client.ExistsDir(dirPath)
}

func (c *IRODSFSClientBuffered) ExistsFile(filePath string) bool {
	if c.staging != nil {
		meta := c.staging.Get(filePath)
		if meta != nil {
			switch meta.Action {
			case stagingfs.ActionDelete:
				return false
			case stagingfs.ActionUpload:
				return true
			}
		}
		if c.staging.IsRenamedFrom(filePath) {
			return false
		}
	}
	return c.client.ExistsFile(filePath)
}

func (c *IRODSFSClientBuffered) RemoveFile(irodsPath string, force bool) error {
	if c.staging != nil {
		if err := c.staging.DeleteWithForce(irodsPath, force); err != nil {
			return err
		}
		c.invalidateFileCacheBlocks(irodsPath)
		return nil
	}
	return c.client.RemoveFile(irodsPath, force)
}

func (c *IRODSFSClientBuffered) RemoveDir(irodsPath string, recurse bool, force bool) error {
	if c.staging != nil {
		return c.staging.Rmdir(irodsPath, recurse, force)
	}
	return c.client.RemoveDir(irodsPath, recurse, force)
}

func (c *IRODSFSClientBuffered) MakeDir(irodsPath string, recurse bool) error {
	if c.staging != nil {
		return c.staging.Mkdir(irodsPath)
	}
	return c.client.MakeDir(irodsPath, recurse)
}

func (c *IRODSFSClientBuffered) RenameDirToDir(srcPath string, destPath string) error {
	if c.staging != nil {
		if err := c.staging.RenameDir(srcPath, destPath); err != nil {
			return err
		}
		if err := c.inodeManager.RenameStagingEntryTree(srcPath, destPath); err != nil {
			return errors.Wrap(err, "failed to rename staging inode entries")
		}
		return nil
	}
	return c.client.RenameDirToDir(srcPath, destPath)
}

func (c *IRODSFSClientBuffered) RenameFileToFile(srcPath string, destPath string) error {
	if c.staging != nil {
		if err := c.staging.Rename(srcPath, destPath); err != nil {
			return err
		}
		if err := c.inodeManager.RenameStagingEntry(srcPath, destPath); err != nil {
			return errors.Wrap(err, "failed to rename staging inode entry")
		}
		c.invalidateFileCacheBlocks(srcPath)
		return nil
	}
	return c.client.RenameFileToFile(srcPath, destPath)
}

func (c *IRODSFSClientBuffered) reuseStagingInodeID(entry *irodsclient_fs.Entry) {
	if entry == nil || c.inodeManager == nil {
		return
	}
	if inodeID, ok := c.inodeManager.GetInodeIDForStagingEntry(entry.Path); ok {
		entry.ID = int64(inodeID)
	}
}

func (c *IRODSFSClientBuffered) CreateFile(path string, mode string) (IRODSFSFileHandle, error) {
	logger := c.logger.WithFields(log.Fields{
		"path": path,
		"mode": mode,
	})

	defer util.StackTraceFromPanic(logger)

	// Invalidate cache before creating (file may be overwritten)
	if err := c.invalidateFileCacheBlocks(path); err != nil {
		logger.WithError(err).Warn("failed to invalidate cache before file creation")
	}

	openMode := irodsclient_types.FileOpenMode(mode)

	// Use staging for write modes
	if c.staging != nil && openMode.IsWrite() {
		f, stagingErr := c.staging.OpenForWrite(path, false)
		if stagingErr != nil {
			if !errors.Is(stagingErr, stagingfs.ErrQuotaExceeded) {
				return nil, stagingErr
			}
			logger.Warnf("staging quota exceeded, falling back to direct iRODS write for %q", path)
		} else {
			if openMode.Truncate() {
				if err := f.Truncate(0); err != nil {
					f.Close()
					return nil, err
				}
			}
			h := newStagedHandleForNewFile(c, f, path, openMode)
			c.staging.RegisterHandle(path, h)
			return h, nil
		}
	}

	// Fallback: no staging, or quota exceeded
	handle, err := c.client.CreateFile(path, mode)
	if err != nil {
		return nil, err
	}

	handleLogger := logger.WithFields(log.Fields{
		"handle_id": handle.GetID(),
	})

	return &IRODSFSClientBufferedFileHandle{
		client:    c,
		handle:    handle,
		cache:     c.cache,
		irodsPath: path,
		helper:    c.helper,
		logger:    handleLogger,
	}, nil
}

func (c *IRODSFSClientBuffered) OpenFile(path string, mode string) (IRODSFSFileHandle, error) {
	logger := c.logger.WithFields(log.Fields{
		"path": path,
		"mode": mode,
	})

	defer util.StackTraceFromPanic(logger)

	openMode := irodsclient_types.FileOpenMode(mode)

	// Use staging for write modes
	if c.staging != nil && openMode.IsWrite() {
		entry, err := c.Stat(path)
		if err != nil {
			return nil, err
		}
		if invalidateErr := c.invalidateFileCacheBlocksHint(path, entry.Size); invalidateErr != nil {
			logger.WithError(invalidateErr).Warn("failed to invalidate cache when opening file for write")
		}

		if openMode.IsRead() {
			f, stagingErr := c.staging.OpenForReadWrite(path, false)
			if stagingErr != nil {
				if !errors.Is(stagingErr, stagingfs.ErrQuotaExceeded) {
					return nil, stagingErr
				}
			} else {
				h := newStagedHandle(c, f, path, openMode, entry)
				c.staging.RegisterHandle(path, h)
				return h, nil
			}
		} else if openMode.Truncate() {
			f, stagingErr := c.staging.OpenForWrite(path, false)
			if stagingErr != nil {
				if !errors.Is(stagingErr, stagingfs.ErrQuotaExceeded) {
					return nil, stagingErr
				}
			} else {
				if err := f.Truncate(0); err != nil {
					f.Close()
					return nil, err
				}
				h := newStagedHandle(c, f, path, openMode, entry)
				c.staging.RegisterHandle(path, h)
				return h, nil
			}
		} else {
			f, stagingErr := c.staging.OpenForReadWrite(path, false)
			if stagingErr != nil {
				if !errors.Is(stagingErr, stagingfs.ErrQuotaExceeded) {
					return nil, stagingErr
				}
			} else {
				h := newStagedHandle(c, f, path, openMode, entry)
				c.staging.RegisterHandle(path, h)
				return h, nil
			}
		}
		// staging quota exceeded — fall through to direct iRODS write
		logger.Warnf("staging quota exceeded, falling back to direct iRODS write for %q", path)
	}

	// Read-only mode: check staging first
	if c.staging != nil {
		meta := c.staging.Get(path)
		if meta != nil && meta.Action == stagingfs.ActionUpload {
			f, err := c.staging.OpenForRead(path)
			if err != nil {
				return nil, err
			}

			entry, err := c.Stat(path)
			if err != nil {
				f.Close()
				return nil, err
			}

			return newStagedHandle(c, f, path, openMode, entry), nil
		}
	}

	// No staging or file not in staging: use cached read path
	handle, err := c.client.OpenFile(path, mode)
	if err != nil {
		return nil, err
	}
	c.validateFileCacheForOpen(path, handle, logger)

	handleLogger := logger.WithFields(log.Fields{
		"handle_id": handle.GetID(),
	})

	return &IRODSFSClientBufferedFileHandle{
		client:    c,
		handle:    handle,
		cache:     c.cache,
		irodsPath: path,
		helper:    c.helper,
		logger:    handleLogger,
	}, nil
}

func (c *IRODSFSClientBuffered) CreateFileBulk(path string, mode string) (IRODSFSFileHandle, error) {
	logger := c.logger.WithFields(log.Fields{
		"path": path,
		"mode": mode,
	})

	defer util.StackTraceFromPanic(logger)

	if err := c.invalidateFileCacheBlocks(path); err != nil {
		logger.WithError(err).Warn("failed to invalidate cache before file creation")
	}

	openMode := irodsclient_types.FileOpenMode(mode)

	if c.staging != nil && openMode.IsWrite() {
		f, stagingErr := c.staging.OpenForWrite(path, true)
		if stagingErr != nil {
			if !errors.Is(stagingErr, stagingfs.ErrQuotaExceeded) {
				return nil, stagingErr
			}
			logger.Warnf("staging quota exceeded, falling back to direct iRODS write for %q", path)
		} else {
			if openMode.Truncate() {
				if err := f.Truncate(0); err != nil {
					f.Close()
					return nil, err
				}
			}
			h := newStagedHandleForNewFile(c, f, path, openMode)
			c.staging.RegisterHandle(path, h)
			return h, nil
		}
	}

	return c.client.CreateFile(path, mode)
}

func (c *IRODSFSClientBuffered) OpenFileBulk(path string, mode string) (IRODSFSFileHandle, error) {
	logger := c.logger.WithFields(log.Fields{
		"path": path,
		"mode": mode,
	})

	defer util.StackTraceFromPanic(logger)

	openMode := irodsclient_types.FileOpenMode(mode)

	if c.staging != nil && openMode.IsWrite() {
		entry, err := c.Stat(path)
		if err != nil {
			return nil, err
		}
		if invalidateErr := c.invalidateFileCacheBlocksHint(path, entry.Size); invalidateErr != nil {
			logger.WithError(invalidateErr).Warn("failed to invalidate cache when opening file for write")
		}

		if openMode.IsRead() {
			f, stagingErr := c.staging.OpenForReadWrite(path, true)
			if stagingErr != nil {
				if !errors.Is(stagingErr, stagingfs.ErrQuotaExceeded) {
					return nil, stagingErr
				}
			} else {
				h := newStagedHandle(c, f, path, openMode, entry)
				c.staging.RegisterHandle(path, h)
				return h, nil
			}
		} else if openMode.Truncate() {
			f, stagingErr := c.staging.OpenForWrite(path, true)
			if stagingErr != nil {
				if !errors.Is(stagingErr, stagingfs.ErrQuotaExceeded) {
					return nil, stagingErr
				}
			} else {
				if err := f.Truncate(0); err != nil {
					f.Close()
					return nil, err
				}
				h := newStagedHandle(c, f, path, openMode, entry)
				c.staging.RegisterHandle(path, h)
				return h, nil
			}
		} else {
			f, stagingErr := c.staging.OpenForReadWrite(path, true)
			if stagingErr != nil {
				if !errors.Is(stagingErr, stagingfs.ErrQuotaExceeded) {
					return nil, stagingErr
				}
			} else {
				h := newStagedHandle(c, f, path, openMode, entry)
				c.staging.RegisterHandle(path, h)
				return h, nil
			}
		}
		// staging quota exceeded — fall through to direct iRODS write
		logger.Warnf("staging quota exceeded, falling back to direct iRODS write for %q", path)
	}

	handle, err := c.client.OpenFile(path, mode)
	if err != nil {
		return nil, err
	}
	c.validateFileCacheForOpen(path, handle, logger)
	return handle, nil
}

func (c *IRODSFSClientBuffered) TruncateFile(path string, size int64) error {
	logger := c.logger.WithField("path", path)
	if err := c.invalidateFileCacheBlocks(path); err != nil {
		logger.WithError(err).Warn("failed to invalidate cache before truncating file")
	}

	if c.staging != nil {
		meta := c.staging.Get(path)
		if meta != nil && meta.Action == stagingfs.ActionUpload {
			return c.staging.TruncateFile(path, size)
		}
	}
	return c.client.TruncateFile(path, size)
}

func (c *IRODSFSClientBuffered) validateFileCacheForOpen(path string, handle IRODSFSFileHandle, logger *log.Entry) {
	entry := handle.GetEntry()
	if entry == nil {
		return
	}

	if handle.IsWriteMode() {
		if err := c.invalidateFileCacheBlocksHint(path, entry.Size); err != nil {
			logger.WithError(err).Warn("failed to invalidate cache when opening file for write")
		}
	} else if handle.IsReadMode() {
		c.validateFileCacheFreshness(path, entry, logger)
	}
}

func (c *IRODSFSClientBuffered) validateFileCacheFreshness(path string, entry *irodsclient_fs.Entry, logger *log.Entry) {
	if !c.isCacheFileFresh(path, entry) {
		if err := c.invalidateFileCacheBlocksHint(path, entry.Size); err != nil {
			logger.WithError(err).Warn("failed to invalidate stale cache")
		}
	}
}

// shouldCacheFile reports whether a complete file is small enough for the block cache.
// A file must fit both the configured per-file limit and the cache itself.
func (c *IRODSFSClientBuffered) shouldCacheFile(fileSize int64) bool {
	maxSize := c.maxCacheFileSize
	cacheMaxSize := c.cache.GetMaxSize()
	if cacheMaxSize < maxSize {
		maxSize = cacheMaxSize
	}

	return maxSize > 0 && fileSize <= maxSize
}

// checkAllBlocksCached returns true if every block of the file is present in the memory cache.
func (c *IRODSFSClientBuffered) checkAllBlocksCached(irodsPath string, fileSize int64) bool {
	if fileSize <= 0 {
		return true
	}
	lastBlock := c.helper.GetLastBlockID(fileSize)
	for blockNum := int64(0); blockNum <= lastBlock; blockNum++ {
		if !c.cache.Has(c.makeCacheKey(irodsPath, blockNum)) {
			return false
		}
	}
	return true
}

// serveFileFromCache feeds an entire file to blockReadyCallback block by block using only
// the memory cache. Each block increments cacheHit. Returns an error if any block has been
// evicted since checkAllBlocksCached was called (rare TTL race).
func (c *IRODSFSClientBuffered) serveFileFromCache(irodsPath string, fileSize int64, blockReadyCallback irodsclient_common.DataObjectBlockCallback, transferCallback irodsclient_common.TransferTrackerCallback) error {
	cacheBlockSize := int64(c.helper.GetBlockSize())
	lastBlock := c.helper.GetLastBlockID(fileSize)

	offset := int64(0)
	for blockNum := int64(0); blockNum <= lastBlock; blockNum++ {
		cacheKey := c.makeCacheKey(irodsPath, blockNum)
		cacheEntry := c.cache.Get(cacheKey)
		if cacheEntry == nil {
			return errors.Newf("cache block %d of %q evicted during serve", blockNum, irodsPath)
		}
		data, err := cacheEntry.GetData(0)
		if err != nil {
			return errors.Wrapf(err, "failed to read cache block %d of %q", blockNum, irodsPath)
		}

		blockEnd := min(offset+cacheBlockSize, fileSize)
		blockData := data[:blockEnd-offset]

		atomic.AddUint64(&c.cacheHit, 1)
		if callbackErr := blockReadyCallback(blockData, offset); callbackErr != nil {
			return callbackErr
		}
		if transferCallback != nil {
			transferCallback("download", blockEnd, fileSize)
		}
		offset = blockEnd
	}
	return nil
}

// CacheFile downloads a file from iRODS into the block cache without writing to local disk
func (c *IRODSFSClientBuffered) CacheFile(irodsPath string, transferCallback irodsclient_common.TransferTrackerCallback) error {
	logger := c.logger.WithFields(log.Fields{
		"irodsPath": irodsPath,
	})

	defer util.StackTraceFromPanic(logger)

	// skip if the file is staged locally (already on disk)
	if c.staging != nil {
		meta := c.staging.Get(irodsPath)
		if meta != nil && meta.Action == stagingfs.ActionUpload {
			return nil
		}
	}

	entry, err := c.client.Stat(irodsPath)
	if err != nil {
		return errors.Wrap(err, "failed to stat file for cache check")
	}
	if !c.shouldCacheFile(entry.Size) {
		_ = c.invalidateFileCacheBlocksHint(irodsPath, entry.Size)
		return nil
	}

	if c.checkAllBlocksCached(irodsPath, entry.Size) {
		if c.isCacheFileFresh(irodsPath, entry) {
			return c.serveFileFromCache(irodsPath, entry.Size, func(_ []byte, _ int64) error { return nil }, transferCallback)
		}
		// File changed on iRODS — drop stale blocks
		c.invalidateFileCacheBlocks(irodsPath)
	}

	blockReadyCallback := func(data []byte, offset int64) error {
		if len(data) > 0 {
			blockNum := c.helper.GetBlockID(offset)
			cacheKey := c.makeCacheKey(irodsPath, blockNum)
			atomic.AddUint64(&c.cacheMiss, 1)
			if _, err := c.cache.PutCopy(cacheKey, data, false); err != nil {
				logger.WithError(err).Warnf("failed to cache block %d", blockNum)
			}
		}
		return nil
	}

	_, err = c.client.fs.DownloadFileParallelWithCallback(irodsPath, "", c.helper.GetBlockSize(), 3, blockReadyCallback, 4, transferCallback)
	if err == nil {
		c.storeCacheFileMeta(irodsPath, entry)
	}
	return err
}

// DownloadFile downloads a file to a local path
func (c *IRODSFSClientBuffered) DownloadFile(irodsPath string, localPath string, transferCallback irodsclient_common.TransferTrackerCallback) error {
	logger := c.logger.WithFields(log.Fields{
		"irodsPath": irodsPath,
		"localPath": localPath,
	})

	defer util.StackTraceFromPanic(logger)

	f, err := os.Create(localPath)
	if err != nil {
		return errors.Wrap(err, "failed to create local file")
	}
	defer f.Close()

	writeCallback := func(data []byte, offset int64) error {
		if len(data) > 0 {
			if _, writeErr := f.WriteAt(data, offset); writeErr != nil {
				return errors.Wrapf(writeErr, "failed to write block at offset %d", offset)
			}
		}
		return nil
	}

	// check if the file is staged locally
	if c.staging != nil {
		meta := c.staging.Get(irodsPath)
		if meta != nil && meta.Action == stagingfs.ActionUpload {
			return c.downloadFromStaging(irodsPath, c.helper.GetBlockSize(), writeCallback, transferCallback)
		}
	}

	entry, err := c.client.Stat(irodsPath)
	if err != nil {
		return errors.Wrap(err, "failed to stat file")
	}
	if !c.shouldCacheFile(entry.Size) {
		_ = c.invalidateFileCacheBlocksHint(irodsPath, entry.Size)
		_, err = c.client.fs.DownloadFileWithCallback(irodsPath, "", c.helper.GetBlockSize(), 3, writeCallback, transferCallback)
		return err
	}

	if c.checkAllBlocksCached(irodsPath, entry.Size) {
		if c.isCacheFileFresh(irodsPath, entry) {
			return c.serveFileFromCache(irodsPath, entry.Size, writeCallback, transferCallback)
		}
		c.invalidateFileCacheBlocks(irodsPath)
	}

	cacheBlockSize := c.helper.GetBlockSize()
	cachedWriteCallback := func(data []byte, offset int64) error {
		if len(data) > 0 {
			blockNum := offset / int64(cacheBlockSize)
			cacheKey := c.makeCacheKey(irodsPath, blockNum)
			atomic.AddUint64(&c.cacheMiss, 1)
			if _, cacheErr := c.cache.PutCopy(cacheKey, data, false); cacheErr != nil {
				logger.WithError(cacheErr).Warnf("failed to cache block %d", blockNum)
			}
		}
		return writeCallback(data, offset)
	}

	_, err = c.client.fs.DownloadFileWithCallback(irodsPath, "", cacheBlockSize, 3, cachedWriteCallback, transferCallback)
	if err == nil {
		c.storeCacheFileMeta(irodsPath, entry)
	}
	return err
}

// DownloadFileParallel downloads a file in parallel to a local path
func (c *IRODSFSClientBuffered) DownloadFileParallel(irodsPath string, localPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	logger := c.logger.WithFields(log.Fields{
		"irodsPath": irodsPath,
		"localPath": localPath,
		"taskNum":   taskNum,
	})

	defer util.StackTraceFromPanic(logger)

	f, err := os.Create(localPath)
	if err != nil {
		return errors.Wrap(err, "failed to create local file")
	}
	defer f.Close()

	writeCallback := func(data []byte, offset int64) error {
		if len(data) > 0 {
			if _, writeErr := f.WriteAt(data, offset); writeErr != nil {
				return errors.Wrapf(writeErr, "failed to write block at offset %d", offset)
			}
		}
		return nil
	}

	// check if the file is staged locally
	if c.staging != nil {
		meta := c.staging.Get(irodsPath)
		if meta != nil && meta.Action == stagingfs.ActionUpload {
			return c.downloadFromStaging(irodsPath, c.helper.GetBlockSize(), writeCallback, transferCallback)
		}
	}

	entry, err := c.client.Stat(irodsPath)
	if err != nil {
		return errors.Wrap(err, "failed to stat file")
	}
	if !c.shouldCacheFile(entry.Size) {
		_ = c.invalidateFileCacheBlocksHint(irodsPath, entry.Size)
		_, err = c.client.fs.DownloadFileParallelWithCallback(irodsPath, "", c.helper.GetBlockSize(), taskNum*3, writeCallback, taskNum, transferCallback)
		return err
	}

	if c.checkAllBlocksCached(irodsPath, entry.Size) {
		if c.isCacheFileFresh(irodsPath, entry) {
			return c.serveFileFromCache(irodsPath, entry.Size, writeCallback, transferCallback)
		}
		c.invalidateFileCacheBlocks(irodsPath)
	}

	cacheBlockSize := c.helper.GetBlockSize()
	cachedWriteCallback := func(data []byte, offset int64) error {
		if len(data) > 0 {
			blockNum := offset / int64(cacheBlockSize)
			cacheKey := c.makeCacheKey(irodsPath, blockNum)
			atomic.AddUint64(&c.cacheMiss, 1)
			if _, cacheErr := c.cache.PutCopy(cacheKey, data, false); cacheErr != nil {
				logger.WithError(cacheErr).Warnf("failed to cache block %d", blockNum)
			}
		}
		return writeCallback(data, offset)
	}

	_, err = c.client.fs.DownloadFileParallelWithCallback(irodsPath, "", cacheBlockSize, taskNum*3, cachedWriteCallback, taskNum, transferCallback)
	if err == nil {
		c.storeCacheFileMeta(irodsPath, entry)
	}
	return err
}

func (c *IRODSFSClientBuffered) DownloadFileWithCallback(irodsPath string, blockSize int, numBlocks int, blockReadyCallback irodsclient_common.DataObjectBlockCallback, transferCallback irodsclient_common.TransferTrackerCallback) error {
	logger := c.logger.WithFields(log.Fields{
		"irodsPath": irodsPath,
		"blockSize": blockSize,
		"numBlocks": numBlocks,
	})

	defer util.StackTraceFromPanic(logger)

	// check if the file is staged locally
	if c.staging != nil {
		meta := c.staging.Get(irodsPath)
		if meta != nil && meta.Action == stagingfs.ActionUpload {
			return c.downloadFromStaging(irodsPath, blockSize, blockReadyCallback, transferCallback)
		}
	}

	cacheBlockSize := c.helper.GetBlockSize()
	if blockSize == cacheBlockSize {
		entry, err := c.client.Stat(irodsPath)
		if err != nil {
			return errors.Wrap(err, "failed to stat file")
		}
		if !c.shouldCacheFile(entry.Size) {
			_ = c.invalidateFileCacheBlocksHint(irodsPath, entry.Size)
			_, err = c.fs.DownloadFileWithCallback(irodsPath, "", blockSize, numBlocks, blockReadyCallback, transferCallback)
			return err
		}

		if c.checkAllBlocksCached(irodsPath, entry.Size) {
			if c.isCacheFileFresh(irodsPath, entry) {
				return c.serveFileFromCache(irodsPath, entry.Size, blockReadyCallback, transferCallback)
			}
			c.invalidateFileCacheBlocks(irodsPath)
		}

		cachedCallback := func(data []byte, offset int64) error {
			if len(data) > 0 {
				blockNum := offset / int64(cacheBlockSize)
				cacheKey := c.makeCacheKey(irodsPath, blockNum)
				atomic.AddUint64(&c.cacheMiss, 1)
				if _, cacheErr := c.cache.PutCopy(cacheKey, data, false); cacheErr != nil {
					logger.WithError(cacheErr).Warnf("failed to cache block %d", blockNum)
				}
			}
			return blockReadyCallback(data, offset)
		}
		_, err = c.fs.DownloadFileWithCallback(irodsPath, "", blockSize, numBlocks, cachedCallback, transferCallback)
		if err == nil {
			c.storeCacheFileMeta(irodsPath, entry)
		}
		return err
	}

	_, err := c.fs.DownloadFileWithCallback(irodsPath, "", blockSize, numBlocks, blockReadyCallback, transferCallback)
	return err
}

func (c *IRODSFSClientBuffered) downloadFromStaging(irodsPath string, blockSize int, blockReadyCallback irodsclient_common.DataObjectBlockCallback, transferCallback irodsclient_common.TransferTrackerCallback) error {
	f, err := c.staging.OpenForRead(irodsPath)
	if err != nil {
		return errors.Wrapf(err, "failed to open staged file %q", irodsPath)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return errors.Wrapf(err, "failed to stat staged file %q", irodsPath)
	}
	fileSize := info.Size()

	buf := make([]byte, blockSize)
	offset := int64(0)

	for offset < fileSize {
		n, readErr := f.ReadAt(buf, offset)
		if n > 0 {
			if blockReadyCallback != nil {
				if callbackErr := blockReadyCallback(buf[:n], offset); callbackErr != nil {
					return callbackErr
				}
			}
			if transferCallback != nil {
				transferCallback("download", offset+int64(n), fileSize)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return errors.Wrapf(readErr, "failed to read staged file %q", irodsPath)
		}
		offset += int64(n)
	}

	return nil
}

func (c *IRODSFSClientBuffered) DownloadFileParallelWithCallback(irodsPath string, blockSize int, numBlocks int, blockReadyCallback irodsclient_common.DataObjectBlockCallback, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	logger := c.logger.WithFields(log.Fields{
		"irodsPath": irodsPath,
		"blockSize": blockSize,
		"numBlocks": numBlocks,
		"taskNum":   taskNum,
	})

	defer util.StackTraceFromPanic(logger)

	// check if the file is staged locally
	if c.staging != nil {
		meta := c.staging.Get(irodsPath)
		if meta != nil && meta.Action == stagingfs.ActionUpload {
			return c.downloadFromStaging(irodsPath, blockSize, blockReadyCallback, transferCallback)
		}
	}

	cacheBlockSize := c.helper.GetBlockSize()
	if blockSize == cacheBlockSize {
		entry, err := c.client.Stat(irodsPath)
		if err != nil {
			return errors.Wrap(err, "failed to stat file")
		}
		if !c.shouldCacheFile(entry.Size) {
			_ = c.invalidateFileCacheBlocksHint(irodsPath, entry.Size)
			_, err = c.fs.DownloadFileParallelWithCallback(irodsPath, "", blockSize, numBlocks, blockReadyCallback, taskNum, transferCallback)
			return err
		}

		if c.checkAllBlocksCached(irodsPath, entry.Size) {
			if c.isCacheFileFresh(irodsPath, entry) {
				return c.serveFileFromCache(irodsPath, entry.Size, blockReadyCallback, transferCallback)
			}
			c.invalidateFileCacheBlocks(irodsPath)
		}

		cachedCallback := func(data []byte, offset int64) error {
			if len(data) > 0 {
				blockNum := offset / int64(cacheBlockSize)
				cacheKey := c.makeCacheKey(irodsPath, blockNum)
				atomic.AddUint64(&c.cacheMiss, 1)
				if _, cacheErr := c.cache.PutCopy(cacheKey, data, false); cacheErr != nil {
					logger.WithError(cacheErr).Warnf("failed to cache block %d", blockNum)
				}
			}
			return blockReadyCallback(data, offset)
		}
		_, err = c.fs.DownloadFileParallelWithCallback(irodsPath, "", blockSize, numBlocks, cachedCallback, taskNum, transferCallback)
		if err == nil {
			c.storeCacheFileMeta(irodsPath, entry)
		}
		return err
	}

	_, err := c.fs.DownloadFileParallelWithCallback(irodsPath, "", blockSize, numBlocks, blockReadyCallback, taskNum, transferCallback)
	return err
}

// UploadFile stages a file for bulk upload. The file is copied into the staging area and
// uploaded to iRODS by the background sync worker. Unlike FUSE-written files (ActionUpload),
// bulk-uploaded files are deleted from local storage immediately after sync (not cached).
// Falls back to direct upload if staging is not configured.
func (c *IRODSFSClientBuffered) UploadFile(localPath string, irodsPath string, transferCallback irodsclient_common.TransferTrackerCallback) error {
	logger := c.logger.WithFields(log.Fields{
		"localPath": localPath,
		"irodsPath": irodsPath,
	})

	defer util.StackTraceFromPanic(logger)

	if c.staging != nil {
		if err := c.staging.StageForBulkUpload(localPath, irodsPath); err != nil {
			return err
		}
	} else {
		if err := c.client.UploadFile(localPath, irodsPath, transferCallback); err != nil {
			return err
		}
	}

	if err := c.invalidateFileCacheBlocks(irodsPath); err != nil {
		logger.WithError(err).Warn("failed to invalidate cache after upload")
	}

	return nil
}

// UploadFileParallel stages a file for bulk upload (parallel). See UploadFile for details.
func (c *IRODSFSClientBuffered) UploadFileParallel(localPath string, irodsPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	logger := c.logger.WithFields(log.Fields{
		"localPath": localPath,
		"irodsPath": irodsPath,
		"taskNum":   taskNum,
	})

	defer util.StackTraceFromPanic(logger)

	if c.staging != nil {
		if err := c.staging.StageForBulkUpload(localPath, irodsPath); err != nil {
			return err
		}
	} else {
		if err := c.client.UploadFileParallel(localPath, irodsPath, taskNum, transferCallback); err != nil {
			return err
		}
	}

	if err := c.invalidateFileCacheBlocks(irodsPath); err != nil {
		logger.WithError(err).Warn("failed to invalidate cache after upload")
	}

	return nil
}

// makeCacheKey creates a cache key for a block
func (c *IRODSFSClientBuffered) makeCacheKey(irodsPath string, blockNum int64) string {
	return "irods:block:" + irodsPath + ":" + strconv.FormatInt(blockNum, 10)
}

// storeCacheFileMeta writes the file's size and modification time into block slot -1.
// This acts as a freshness stamp: before serving from cache, callers compare this against
// the current iRODS entry and invalidate if the file has changed.
func (c *IRODSFSClientBuffered) storeCacheFileMeta(irodsPath string, entry *irodsclient_fs.Entry) {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(entry.Size))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(entry.ModifyTime.UnixNano()))
	// The freshness stamp must be visible before a subsequent read validates
	// the cached blocks. Waiting here also commits any data blocks queued before
	// the stamp, so a fresh stamp can never point at blocks that are not visible.
	if _, err := c.cache.PutCopy(c.makeCacheKey(irodsPath, -1), buf, true); err != nil {
		c.logger.WithError(err).Warnf("failed to store cache meta for %q", irodsPath)
	}
}

// isCacheFileFresh returns true when the meta stored at block -1 matches the given entry.
// Returns false (treat as stale) when the meta block is absent or mismatched.
func (c *IRODSFSClientBuffered) isCacheFileFresh(irodsPath string, entry *irodsclient_fs.Entry) bool {
	metaEntry := c.cache.Get(c.makeCacheKey(irodsPath, -1))
	if metaEntry == nil {
		return false
	}
	data, err := metaEntry.GetData(0)
	if err != nil || len(data) < 16 {
		return false
	}
	cachedSize := int64(binary.LittleEndian.Uint64(data[0:8]))
	cachedModNano := int64(binary.LittleEndian.Uint64(data[8:16]))
	return cachedSize == entry.Size && cachedModNano == entry.ModifyTime.UnixNano()
}

// invalidateFileCacheBlocks removes all cached blocks for a file, discovering the
// file size from staging and iRODS.
func (c *IRODSFSClientBuffered) invalidateFileCacheBlocks(irodsPath string) error {
	return c.invalidateFileCacheBlocksHint(irodsPath, 0)
}

// invalidateFileCacheBlocksHint removes all cached blocks for a file.
// When sizeHint > 0 the iRODS Stat call is skipped; the hint (plus any larger
// staging size) is used directly. Pass the caller's already-known file size to
// avoid a blocking network round-trip (e.g. from a staged handle Close where
// the file has not been synced to iRODS yet).
func (c *IRODSFSClientBuffered) invalidateFileCacheBlocksHint(irodsPath string, sizeHint int64) error {
	logger := c.logger.WithFields(log.Fields{
		"irodsPath": irodsPath,
	})

	defer util.StackTraceFromPanic(logger)

	fileSize := sizeHint

	if c.staging != nil {
		localSize := c.staging.GetLocalFileSize(irodsPath)
		if localSize > fileSize {
			fileSize = localSize
		}
	}

	// Only stat iRODS when the caller has no size hint. This avoids a blocking
	// network call when the size is already known (e.g. staged handle Close).
	if sizeHint == 0 {
		entry, err := c.client.Stat(irodsPath)
		if err == nil && entry != nil && entry.Size > fileSize {
			fileSize = entry.Size
		}
	}

	// If we still don't have file size, use a reasonable upper bound
	if fileSize == 0 {
		fileSize = 4 * 1024 * 1024 * 1024 // 4GB default
	}

	lastBlockID := c.helper.GetLastBlockID(fileSize)

	// Also delete the meta block (-1) so freshness checks don't see stale stamps.
	c.cache.Delete(c.makeCacheKey(irodsPath, -1), false)
	for blockNum := int64(0); blockNum <= lastBlockID; blockNum++ {
		c.cache.Delete(c.makeCacheKey(irodsPath, blockNum), false)
	}

	return nil
}

// IRODSFSClientBufferedFileHandle wraps IRODSFSFileHandle with block-level caching
type IRODSFSClientBufferedFileHandle struct {
	client    *IRODSFSClientBuffered
	handle    IRODSFSFileHandle
	cache     *cache.MemoryCacheManager
	irodsPath string
	helper    *util.FileBlockHelper
	logger    *log.Entry
}

func (h *IRODSFSClientBufferedFileHandle) GetID() string {
	return h.handle.GetID()
}

func (h *IRODSFSClientBufferedFileHandle) GetEntry() *irodsclient_fs.Entry {
	return h.handle.GetEntry()
}

func (h *IRODSFSClientBufferedFileHandle) GetOpenMode() irodsclient_types.FileOpenMode {
	return h.handle.GetOpenMode()
}

func (h *IRODSFSClientBufferedFileHandle) IsReadMode() bool {
	return h.handle.IsReadMode()
}

func (h *IRODSFSClientBufferedFileHandle) IsWriteMode() bool {
	return h.handle.IsWriteMode()
}

func (h *IRODSFSClientBufferedFileHandle) GetAvailable(offset int64) int64 {
	if !h.handle.IsReadMode() {
		return h.handle.GetAvailable(offset)
	}

	entry := h.handle.GetEntry()
	if entry == nil || offset >= entry.Size {
		return 0
	}

	blockSize := int64(h.helper.GetBlockSize())
	var available int64

	blockNum := h.helper.GetBlockID(offset)
	blockStart := h.helper.GetBlockStart(blockNum)

	for blockStart < entry.Size {
		cacheKey := h.makeCacheKey(blockNum)
		cacheEntry := h.cache.Get(cacheKey)
		if cacheEntry == nil {
			break
		}

		blockEnd := min(blockStart+blockSize, entry.Size)
		if available == 0 {
			available = blockEnd - offset
		} else {
			available += blockEnd - blockStart
		}

		blockNum++
		blockStart += blockSize
	}

	if available > 0 {
		return available
	}

	return h.handle.GetAvailable(offset)
}

// ReadAt reads from cache if available, otherwise reads full blocks from the
// underlying handle and caches them. Handles cross-block reads correctly.
func (h *IRODSFSClientBufferedFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	if !h.handle.IsReadMode() {
		return 0, errors.Newf("file is opened with %q mode", h.handle.GetOpenMode())
	}

	defer util.StackTraceFromPanic(h.logger)

	entry := h.handle.GetEntry()
	if offset >= entry.Size {
		return 0, io.EOF
	}
	h.client.validateFileCacheFreshness(h.irodsPath, entry, h.logger)

	// Clamp read to file size
	readLen := int64(len(buffer))
	if offset+readLen > entry.Size {
		readLen = entry.Size - offset
	}

	blockSize := int64(h.helper.GetBlockSize())
	totalCopied := 0

	for int64(totalCopied) < readLen {
		currentOffset := offset + int64(totalCopied)
		blockNum := h.helper.GetBlockID(currentOffset)
		blockStart := h.helper.GetBlockStart(blockNum)
		blockOffset := currentOffset - blockStart
		cacheKey := h.makeCacheKey(blockNum)

		// How much data we need from this block
		availableInBlock := blockSize - blockOffset
		remaining := readLen - int64(totalCopied)
		toCopy := min(availableInBlock, remaining)

		// Clamp to actual file boundary within this block
		blockEnd := min(blockStart+blockSize, entry.Size)
		blockDataLen := blockEnd - blockStart
		if blockOffset >= blockDataLen {
			break
		}
		if blockOffset+toCopy > blockDataLen {
			toCopy = blockDataLen - blockOffset
		}

		// Try cache first
		cacheEntry := h.cache.Get(cacheKey)
		if cacheEntry != nil {
			data, err := cacheEntry.GetData(int(blockOffset))
			if err == nil && len(data) > 0 {
				n := copy(buffer[totalCopied:totalCopied+int(toCopy)], data)
				totalCopied += n
				atomic.AddUint64(&h.client.cacheHit, 1)
				continue
			}
		}

		// Cache miss — read the full block from underlying handle
		atomic.AddUint64(&h.client.cacheMiss, 1)
		blockBuf := make([]byte, blockDataLen)
		n, err := h.handle.ReadAt(blockBuf, blockStart)
		if err != nil && err != io.EOF {
			if totalCopied > 0 {
				return totalCopied, nil
			}
			return 0, err
		}

		// Cache the full block
		if n > 0 {
			if _, cacheErr := h.cache.PutCopy(cacheKey, blockBuf[:n], false); cacheErr != nil {
				h.logger.WithError((cacheErr)).Warn("failed to cache block %d", blockNum)
			}
			// Store freshness stamp on first cache miss so download paths can validate staleness.
			if !h.cache.Has(h.client.makeCacheKey(h.irodsPath, -1)) {
				h.client.storeCacheFileMeta(h.irodsPath, entry)
			}

			// Copy the requested portion to the output buffer
			copyStart := int(blockOffset)
			if copyStart < n {
				copyEnd := min(copyStart+int(toCopy), n)
				copied := copy(buffer[totalCopied:], blockBuf[copyStart:copyEnd])
				totalCopied += copied
			}
		}

		// Short read from underlying — done
		if n < int(blockDataLen) {
			break
		}
	}

	if totalCopied == 0 {
		return 0, io.EOF
	}

	if offset+int64(totalCopied) >= entry.Size {
		return totalCopied, io.EOF
	}

	return totalCopied, nil
}

// WriteAt writes to underlying handle and invalidates affected cache blocks
func (h *IRODSFSClientBufferedFileHandle) WriteAt(data []byte, offset int64) (int, error) {
	defer util.StackTraceFromPanic(h.logger)

	n, err := h.handle.WriteAt(data, offset)
	if err != nil {
		return n, err
	}

	// Invalidate cache blocks that were written
	startBlock, endBlock := h.helper.GetBlockIDs(offset, n)
	endBlock++ // Include the end block

	for blockNum := startBlock; blockNum < endBlock; blockNum++ {
		cacheKey := h.makeCacheKey(blockNum)
		h.cache.Delete(cacheKey, false)
	}

	return n, nil
}

func (h *IRODSFSClientBufferedFileHandle) Truncate(size int64) error {
	defer util.StackTraceFromPanic(h.logger)

	err := h.handle.Truncate(size)
	if err != nil {
		return err
	}

	// Invalidate all cache blocks after truncation
	lastBlockID := h.helper.GetLastBlockID(size)
	for blockNum := int64(0); blockNum <= lastBlockID; blockNum++ {
		cacheKey := h.makeCacheKey(blockNum)
		h.cache.Delete(cacheKey, false)
	}

	return nil
}

func (h *IRODSFSClientBufferedFileHandle) Flush() error {
	return h.handle.Flush()
}

func (h *IRODSFSClientBufferedFileHandle) Close() error {
	return h.handle.Close()
}

func (h *IRODSFSClientBufferedFileHandle) makeCacheKey(blockNum int64) string {
	return "irods:block:" + h.irodsPath + ":" + strconv.FormatInt(blockNum, 10)
}
