package irods

import (
	"io"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_metrics "github.com/cyverse/go-irodsclient/irods/metrics"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/irods/cache"
	"github.com/cyverse/irodsfs-common/irods/stagingfs"
	"github.com/cyverse/irodsfs-common/util"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

// IRODSFSClientBufferedConfig holds configuration for IRODSFSClientBuffered
type IRODSFSClientBufferedConfig struct {
	// Read cache (shared, created externally)
	Cache     *cache.MemoryCacheManager
	BlockSize int // Block size for read cache in bytes (default: 4MB)

	// Staging settings (leave StagingRootPath empty to disable staging/write support)
	StagingRootPath string                     // Local path for staging files
	SyncInterval    time.Duration              // Background sync interval (default: 5s)
	GracePeriod     time.Duration              // Grace period before sync (default: 10s)
	UsePersistence  bool                       // Use BadgerDB for crash recovery
	OnSyncError     stagingfs.SyncErrorHandler // Optional error callback
}

// IRODSFSClientBuffered wraps IRODSFSClient with block-level read-through caching
// and local staging for write/readwrite modes.
type IRODSFSClientBuffered struct {
	id      string
	fs      *irodsclient_fs.FileSystem
	client  *IRODSFSClientDirect
	cache   *cache.MemoryCacheManager
	helper  *util.FileBlockHelper
	staging *stagingfs.StagingFS
	logger  *log.Entry
}

// NewIRODSFSClientBuffered creates a new IRODSFSClientBuffered with the given config.
// The cache is provided externally so it can be shared across multiple clients.
func NewIRODSFSClientBuffered(fs *irodsclient_fs.FileSystem, config *IRODSFSClientBufferedConfig) (IRODSFSClient, error) {
	if fs == nil {
		return nil, errors.New("fs is required")
	}
	if config == nil {
		return nil, errors.New("config is required")
	}
	if config.Cache == nil {
		return nil, errors.New("config.Cache is required")
	}

	blockSize := config.BlockSize
	if blockSize <= 0 {
		blockSize = 4 * 1024 * 1024
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
			LocalRootPath: config.StagingRootPath,
			Client:        directClient,
			SyncInterval:  config.SyncInterval,
			GracePeriod:   config.GracePeriod,
			OnSyncError:   config.OnSyncError,
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

	clientID := xid.New().String()
	logger := fs.GetLogger().WithFields(log.Fields{
		"fsclient_buffered_id": clientID,
	})

	return &IRODSFSClientBuffered{
		id:      clientID,
		fs:      fs,
		client:  directClient,
		cache:   config.Cache,
		helper:  util.NewFileBlockHelper(blockSize),
		staging: staging,
		logger:  logger,
	}, nil
}

func (c *IRODSFSClientBuffered) Release() {
	if c.staging != nil {
		c.staging.Close()
		c.staging = nil
	}

	if c.client != nil {
		c.client.Release()
		c.client = nil
	}
}

func (c *IRODSFSClientBuffered) Sync() error {
	if c.staging == nil {
		return nil
	}

	logger := c.logger.WithFields(log.Fields{
		"method": "Sync",
	})

	logger.Info("syncing all staged data to iRODS")

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

func (c *IRODSFSClientBuffered) GetOpenConnections() int {
	return c.client.GetOpenConnections()
}

func (c *IRODSFSClientBuffered) GetMetrics() *irodsclient_metrics.IRODSMetrics {
	return c.client.GetMetrics()
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
		entryMap[e.Path] = e
	}

	// Apply staging state
	allMeta := c.staging.GetAll()
	for _, meta := range allMeta {
		entryDir := path.Dir(meta.Path)
		if entryDir != dirPath {
			continue
		}

		switch meta.Action {
		case stagingfs.ActionUpload:
			if meta.IsNew {
				// New file created locally — add to listing
				size := c.staging.GetLocalFileSize(meta.Path)
				if size < 0 {
					size = 0
				}
				entryMap[meta.Path] = &irodsclient_fs.Entry{
					Type:       irodsclient_fs.FileEntry,
					Name:       path.Base(meta.Path),
					Path:       meta.Path,
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
			entryMap[meta.Path] = &irodsclient_fs.Entry{
				Type:       irodsclient_fs.DirectoryEntry,
				Name:       path.Base(meta.Path),
				Path:       meta.Path,
				Size:       0,
				CreateTime: meta.CreatedAt,
				ModifyTime: meta.LastModifiedAt,
				AccessTime: meta.LastModifiedAt,
			}

		case stagingfs.ActionRmdir:
			delete(entryMap, meta.Path)

		case stagingfs.ActionRename:
			// Remove old path from this dir if present
			if path.Dir(meta.OldPath) == dirPath {
				delete(entryMap, meta.OldPath)
			}
			// Add new path entry if not already from iRODS
			if _, ok := entryMap[meta.Path]; !ok {
				now := time.Now()
				entryMap[meta.Path] = &irodsclient_fs.Entry{
					Type:       irodsclient_fs.FileEntry,
					Name:       path.Base(meta.Path),
					Path:       meta.Path,
					Size:       0,
					CreateTime: now,
					ModifyTime: meta.LastModifiedAt,
					AccessTime: meta.LastModifiedAt,
				}
			}

		case stagingfs.ActionRenameDir:
			if path.Dir(meta.OldPath) == dirPath {
				delete(entryMap, meta.OldPath)
			}
			if _, ok := entryMap[meta.Path]; !ok {
				now := time.Now()
				entryMap[meta.Path] = &irodsclient_fs.Entry{
					Type:       irodsclient_fs.DirectoryEntry,
					Name:       path.Base(meta.Path),
					Path:       meta.Path,
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

func (c *IRODSFSClientBuffered) Stat(filePath string) (*irodsclient_fs.Entry, error) {
	if c.staging != nil {
		// Check staging state first
		meta := c.staging.Get(filePath)
		if meta != nil {
			switch meta.Action {
			case stagingfs.ActionDelete, stagingfs.ActionRmdir:
				return nil, errors.Errorf("file not found: %s", filePath)

			case stagingfs.ActionUpload:
				// Return entry with local file size
				size := c.staging.GetLocalFileSize(filePath)
				if size < 0 {
					size = 0
				}

				if meta.IsNew {
					return &irodsclient_fs.Entry{
						Type:       irodsclient_fs.FileEntry,
						Name:       path.Base(filePath),
						Path:       filePath,
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
				return entry, nil

			case stagingfs.ActionMkdir:
				return &irodsclient_fs.Entry{
					Type:       irodsclient_fs.DirectoryEntry,
					Name:       path.Base(filePath),
					Path:       filePath,
					Size:       0,
					CreateTime: meta.CreatedAt,
					ModifyTime: meta.LastModifiedAt,
					AccessTime: meta.LastModifiedAt,
				}, nil
			}
		}

		// Check if this path was renamed away
		if c.staging.IsRenamedFrom(filePath) {
			return nil, errors.Errorf("file not found: %s", filePath)
		}
	}

	return c.client.Stat(filePath)
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
		if err := c.staging.Delete(irodsPath); err != nil {
			return err
		}
		c.invalidateFileCacheBlocks(irodsPath)
		return nil
	}
	return c.client.RemoveFile(irodsPath, force)
}

func (c *IRODSFSClientBuffered) RemoveDir(irodsPath string, recurse bool, force bool) error {
	if c.staging != nil {
		return c.staging.Rmdir(irodsPath)
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
		return c.staging.RenameDir(srcPath, destPath)
	}
	return c.client.RenameDirToDir(srcPath, destPath)
}

func (c *IRODSFSClientBuffered) RenameFileToFile(srcPath string, destPath string) error {
	if c.staging != nil {
		if err := c.staging.Rename(srcPath, destPath); err != nil {
			return err
		}
		c.invalidateFileCacheBlocks(srcPath)
		return nil
	}
	return c.client.RenameFileToFile(srcPath, destPath)
}

func (c *IRODSFSClientBuffered) CreateFile(path string, mode string) (IRODSFSFileHandle, error) {
	logger := c.logger.WithFields(log.Fields{
		"path": path,
		"mode": mode,
	})

	defer util.StackTraceFromPanic(logger)

	// Invalidate cache before creating (file may be overwritten)
	if err := c.invalidateFileCacheBlocks(path); err != nil {
		logger.Warnf("failed to invalidate cache before file creation: %v", err)
	}

	openMode := irodsclient_types.FileOpenMode(mode)

	// Use staging for write modes
	if c.staging != nil && openMode.IsWrite() {
		f, err := c.staging.OpenForWrite(path)
		if err != nil {
			return nil, err
		}

		// Truncate if mode requires it
		if openMode.Truncate() {
			if err := f.Truncate(0); err != nil {
				f.Close()
				return nil, err
			}
		}

		return newStagedHandleForNewFile(c, f, path, openMode), nil
	}

	// Fallback to direct for non-staging
	handle, err := c.client.CreateFile(path, mode)
	if err != nil {
		return nil, err
	}

	handleID := xid.New().String()
	handleLogger := logger.WithFields(log.Fields{
		"handle_id": handleID,
	})

	return &IRODSFSClientBufferedFileHandle{
		id:        handleID,
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
		entry, err := c.client.Stat(path)
		if err != nil {
			return nil, err
		}

		if openMode.IsRead() {
			// Read+Write mode (r+, a+): download first, then allow read/write
			f, err := c.staging.OpenForReadWrite(path)
			if err != nil {
				return nil, err
			}

			// For append mode, caller handles seeking; local file has full content
			return newStagedHandle(c, f, path, openMode, entry), nil
		}

		// Write-only mode (w, w+, a)
		if openMode.Truncate() {
			// w+ mode: no need to download, start fresh
			f, err := c.staging.OpenForWrite(path)
			if err != nil {
				return nil, err
			}
			if err := f.Truncate(0); err != nil {
				f.Close()
				return nil, err
			}
			return newStagedHandle(c, f, path, openMode, entry), nil
		}

		// w, a modes: need existing content to avoid data loss
		f, err := c.staging.OpenForReadWrite(path)
		if err != nil {
			return nil, err
		}
		return newStagedHandle(c, f, path, openMode, entry), nil
	}

	// Read-only mode or no staging: use cached read path
	handle, err := c.client.OpenFile(path, mode)
	if err != nil {
		return nil, err
	}

	handleID := xid.New().String()
	handleLogger := logger.WithFields(log.Fields{
		"handle_id": handleID,
	})

	return &IRODSFSClientBufferedFileHandle{
		id:        handleID,
		client:    c,
		handle:    handle,
		cache:     c.cache,
		irodsPath: path,
		helper:    c.helper,
		logger:    handleLogger,
	}, nil
}

func (c *IRODSFSClientBuffered) TruncateFile(path string, size int64) error {
	return c.client.TruncateFile(path, size)
}

// CacheFile downloads a file from iRODS into the block cache without writing to local disk
func (c *IRODSFSClientBuffered) CacheFile(irodsPath string) error {
	logger := c.logger.WithFields(log.Fields{
		"irodsPath": irodsPath,
	})

	defer util.StackTraceFromPanic(logger)

	blockReadyCallback := func(data []byte, offset int64) error {
		if len(data) > 0 {
			blockNum := c.helper.GetBlockID(offset)
			cacheKey := c.makeCacheKey(irodsPath, blockNum)
			if _, err := c.cache.PutCopy(cacheKey, data, false); err != nil {
				logger.Warnf("failed to cache block %d: %v", blockNum, err)
			}
		}
		return nil
	}

	_, err := c.client.fs.DownloadFileParallelWithCallback(irodsPath, "", c.helper.GetBlockSize(), 3, blockReadyCallback, 4, nil)
	return err
}

// DownloadFile downloads a file with block-level read-through caching
func (c *IRODSFSClientBuffered) DownloadFile(irodsPath string, localPath string) error {
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

	blockReadyCallback := func(data []byte, offset int64) error {
		if len(data) > 0 {
			blockNum := c.helper.GetBlockID(offset)
			cacheKey := c.makeCacheKey(irodsPath, blockNum)
			if _, cacheErr := c.cache.PutCopy(cacheKey, data, false); cacheErr != nil {
				logger.Warnf("failed to cache block %d: %v", blockNum, cacheErr)
			}

			if _, writeErr := f.WriteAt(data, offset); writeErr != nil {
				return errors.Wrapf(writeErr, "failed to write block at offset %d", offset)
			}
		}

		return nil
	}

	_, err = c.client.fs.DownloadFileWithCallback(irodsPath, "", c.helper.GetBlockSize(), 3, blockReadyCallback, nil)
	return err
}

// DownloadFileParallel downloads a file in parallel with block-level read-through caching
func (c *IRODSFSClientBuffered) DownloadFileParallel(irodsPath string, localPath string, taskNum int) error {
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

	blockReadyCallback := func(data []byte, offset int64) error {
		if len(data) > 0 {
			blockNum := c.helper.GetBlockID(offset)
			cacheKey := c.makeCacheKey(irodsPath, blockNum)
			if _, cacheErr := c.cache.PutCopy(cacheKey, data, false); cacheErr != nil {
				logger.Warnf("failed to cache block %d: %v", blockNum, cacheErr)
			}

			if _, writeErr := f.WriteAt(data, offset); writeErr != nil {
				return errors.Wrapf(writeErr, "failed to write block at offset %d", offset)
			}
		}

		return nil
	}

	_, err = c.client.fs.DownloadFileParallelWithCallback(irodsPath, "", c.helper.GetBlockSize(), taskNum*3, blockReadyCallback, taskNum, nil)
	return err
}

// UploadFile uploads a file and invalidates cache based on server file size
func (c *IRODSFSClientBuffered) UploadFile(localPath string, irodsPath string) error {
	logger := c.logger.WithFields(log.Fields{
		"localPath": localPath,
		"irodsPath": irodsPath,
	})

	defer util.StackTraceFromPanic(logger)

	// Upload file
	if err := c.client.UploadFile(localPath, irodsPath); err != nil {
		return err
	}

	// Invalidate cache based on server file size after upload
	if err := c.invalidateFileCacheBlocks(irodsPath); err != nil {
		logger.Warnf("failed to invalidate cache after upload: %v", err)
	}

	return nil
}

// UploadFileParallel uploads a file in parallel and invalidates cache based on server file size
func (c *IRODSFSClientBuffered) UploadFileParallel(localPath string, irodsPath string, taskNum int) error {
	logger := c.logger.WithFields(log.Fields{
		"localPath": localPath,
		"irodsPath": irodsPath,
		"taskNum":   taskNum,
	})

	defer util.StackTraceFromPanic(logger)

	// Upload file in parallel
	if err := c.client.UploadFileParallel(localPath, irodsPath, taskNum); err != nil {
		return err
	}

	// Invalidate cache based on server file size after upload
	if err := c.invalidateFileCacheBlocks(irodsPath); err != nil {
		logger.Warnf("failed to invalidate cache after upload: %v", err)
	}

	return nil
}

// makeCacheKey creates a cache key for a block
func (c *IRODSFSClientBuffered) makeCacheKey(irodsPath string, blockNum int64) string {
	return "irods:block:" + irodsPath + ":" + strconv.FormatInt(blockNum, 10)
}

// invalidateFileCacheBlocks removes all cached blocks for a file
// Used for remove, rename operations - gets size from iRODS
func (c *IRODSFSClientBuffered) invalidateFileCacheBlocks(irodsPath string) error {
	logger := c.logger.WithFields(log.Fields{
		"irodsPath": irodsPath,
	})

	defer util.StackTraceFromPanic(logger)

	var fileSize int64

	// Get file size from iRODS
	entry, err := c.client.Stat(irodsPath)
	if err == nil && entry != nil {
		fileSize = entry.Size
	}

	// If we don't have file size, use a reasonable upper bound
	if fileSize == 0 {
		fileSize = 4 * 1024 * 1024 * 1024 // 4GB default
	}

	lastBlockID := c.helper.GetLastBlockID(fileSize)

	for blockNum := int64(0); blockNum <= lastBlockID; blockNum++ {
		cacheKey := c.makeCacheKey(irodsPath, blockNum)
		c.cache.Delete(cacheKey, false)
	}

	return nil
}

// IRODSFSClientBufferedFileHandle wraps IRODSFSFileHandle with block-level caching
type IRODSFSClientBufferedFileHandle struct {
	id        string
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
	return h.handle.GetAvailable(offset)
}

// ReadAt reads from cache if available, otherwise reads full blocks from the
// underlying handle and caches them. Handles cross-block reads correctly.
func (h *IRODSFSClientBufferedFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	if !h.handle.IsReadMode() {
		return 0, errors.Errorf("file is opened with %q mode", h.handle.GetOpenMode())
	}

	defer util.StackTraceFromPanic(h.logger)

	entry := h.handle.GetEntry()
	if offset >= entry.Size {
		return 0, io.EOF
	}

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
				continue
			}
		}

		// Cache miss — read the full block from underlying handle
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
				h.logger.Warnf("failed to cache block %d: %v", blockNum, cacheErr)
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
