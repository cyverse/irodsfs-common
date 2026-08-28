package irods

import (
	"io"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
	irodsclient_metrics "github.com/cyverse/go-irodsclient/irods/metrics"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/util"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

// IRODSFSClientDirect implements IRODSClient interface with go-irodsclient
// direct access to iRODS server
// implements interfaces defined in interface.go
type IRODSFSClientDirect struct {
	id     string
	fs     *irodsclient_fs.FileSystem
	logger *log.Entry
}

// NewIRODSFSClientDirect creates IRODSFSClient using IRODSFSClientDirect
func NewIRODSFSClientDirect(fs *irodsclient_fs.FileSystem) (IRODSFSClient, error) {
	if fs == nil {
		return nil, errors.New("fs is required")
	}

	clientID := xid.New().String()

	logger := fs.GetLogger().WithFields(log.Fields{
		"fsclient_direct_id": clientID,
	})

	return &IRODSFSClientDirect{
		id:     clientID,
		fs:     fs,
		logger: logger,
	}, nil
}

// GetAccount returns iRODS Account info
func (c *IRODSFSClientDirect) GetAccount() *irodsclient_types.IRODSAccount {
	return c.fs.GetAccount()
}

// GetApplicationName returns application name
func (c *IRODSFSClientDirect) GetApplicationName() string {
	return c.fs.GetConfig().ApplicationName
}

// GetFSClient returns iRODS fs client
func (c *IRODSFSClientDirect) GetFSClient() *irodsclient_fs.FileSystem {
	return c.fs
}

// GetOpenConnections() returns total number of open connections
func (c *IRODSFSClientDirect) GetOpenConnections() int {
	return c.fs.GetOpenConnections()
}

// GetTransferMetrics() returns transfer metrics
func (c *IRODSFSClientDirect) GetMetrics() *irodsclient_metrics.IRODSMetrics {
	return c.fs.GetMetrics()
}

// Release releases resources
func (c *IRODSFSClientDirect) Release() error {
	defer util.StackTraceFromPanic(c.logger)

	if c.fs != nil {
		c.fs = nil
	}

	return nil
}

// Sync is a no-op for direct client (no staging layer)
func (c *IRODSFSClientDirect) Sync() error {
	return nil
}

// List lists directory entries
func (c *IRODSFSClientDirect) List(path string) ([]*irodsclient_fs.Entry, error) {
	logger := c.logger.WithFields(log.Fields{
		"path": path,
	})

	defer util.StackTraceFromPanic(logger)

	entries, err := c.fs.List(path)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// Stat stats fs entry
func (c *IRODSFSClientDirect) Stat(path string) (*irodsclient_fs.Entry, error) {
	logger := c.logger.WithFields(log.Fields{
		"path": path,
	})

	defer util.StackTraceFromPanic(logger)

	entry, err := c.fs.Stat(path)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

// ExistsDir checks existance of a dir
func (c *IRODSFSClientDirect) ExistsDir(path string) bool {
	logger := c.logger.WithFields(log.Fields{
		"path": path,
	})

	defer util.StackTraceFromPanic(logger)

	return c.fs.ExistsDir(path)
}

// ExistsFile checks existance of a file
func (c *IRODSFSClientDirect) ExistsFile(path string) bool {
	logger := c.logger.WithFields(log.Fields{
		"path": path,
	})

	defer util.StackTraceFromPanic(logger)

	return c.fs.ExistsFile(path)
}

// RemoveFile removes a file
func (c *IRODSFSClientDirect) RemoveFile(path string, force bool) error {
	logger := c.logger.WithFields(log.Fields{
		"path":  path,
		"force": force,
	})

	defer util.StackTraceFromPanic(logger)

	err := c.fs.RemoveFile(path, force)
	if err != nil {
		return err
	}
	return nil
}

// RemoveDir removes a directory
func (c *IRODSFSClientDirect) RemoveDir(path string, recurse bool, force bool) error {
	logger := c.logger.WithFields(log.Fields{
		"path":    path,
		"recurse": recurse,
		"force":   force,
	})

	defer util.StackTraceFromPanic(logger)

	err := c.fs.RemoveDir(path, recurse, force)
	if err != nil {
		return err
	}
	return nil
}

// MakeDir makes a new directory
func (c *IRODSFSClientDirect) MakeDir(path string, recurse bool) error {
	logger := c.logger.WithFields(log.Fields{
		"path":    path,
		"recurse": recurse,
	})

	defer util.StackTraceFromPanic(logger)

	err := c.fs.MakeDir(path, recurse)
	if err != nil {
		return err
	}
	return nil
}

// RenameDirToDir renames a directory, dest path is also a non-existing path for dir
func (c *IRODSFSClientDirect) RenameDirToDir(srcPath string, destPath string) error {
	logger := c.logger.WithFields(log.Fields{
		"srcPath":  srcPath,
		"destPath": destPath,
	})

	defer util.StackTraceFromPanic(logger)

	err := c.fs.RenameDirToDir(srcPath, destPath)
	if err != nil {
		return err
	}
	return nil
}

// RenameFileToFile renames a file, dest path is also a non-existing path for file
func (c *IRODSFSClientDirect) RenameFileToFile(srcPath string, destPath string) error {
	logger := c.logger.WithFields(log.Fields{
		"srcPath":  srcPath,
		"destPath": destPath,
	})

	defer util.StackTraceFromPanic(logger)

	err := c.fs.RenameFileToFile(srcPath, destPath)
	if err != nil {
		return err
	}
	return nil
}

// CreateFile creates a file
func (c *IRODSFSClientDirect) CreateFile(path string, mode string) (IRODSFSFileHandle, error) {
	logger := c.logger.WithFields(log.Fields{
		"path": path,
		"mode": mode,
	})

	defer util.StackTraceFromPanic(logger)

	handle, err := c.fs.CreateFile(path, "", mode)
	if err != nil {
		return nil, err
	}

	handleID := xid.New().String()
	handleLogger := logger.WithFields(log.Fields{
		"handle_id": handleID,
	})

	fileHandle := &IRODSFSClientDirectFileHandle{
		id:     handleID,
		client: c,
		handle: handle,
		logger: handleLogger,
	}

	return fileHandle, nil
}

// OpenFile opens a file
func (c *IRODSFSClientDirect) OpenFile(path string, mode string) (IRODSFSFileHandle, error) {
	logger := c.logger.WithFields(log.Fields{
		"path": path,
		"mode": mode,
	})

	defer util.StackTraceFromPanic(logger)

	handle, err := c.fs.OpenFile(path, "", mode)
	if err != nil {
		return nil, err
	}

	handleID := xid.New().String()
	handleLogger := logger.WithFields(log.Fields{
		"handle_id": handleID,
	})

	fileHandle := &IRODSFSClientDirectFileHandle{
		id:     handleID,
		client: c,
		handle: handle,
		logger: handleLogger,
	}

	return fileHandle, nil
}

// CreateFileBulk delegates to CreateFile (Direct has no staging distinction)
func (c *IRODSFSClientDirect) CreateFileBulk(path string, mode string) (IRODSFSFileHandle, error) {
	return c.CreateFile(path, mode)
}

// OpenFileBulk delegates to OpenFile (Direct has no staging distinction)
func (c *IRODSFSClientDirect) OpenFileBulk(path string, mode string) (IRODSFSFileHandle, error) {
	return c.OpenFile(path, mode)
}

// TruncateFile truncates a file
func (c *IRODSFSClientDirect) TruncateFile(path string, size int64) error {
	logger := c.logger.WithFields(log.Fields{
		"path": path,
		"size": size,
	})

	defer util.StackTraceFromPanic(logger)

	err := c.fs.TruncateFile(path, size)
	if err != nil {
		return err
	}
	return nil
}

// CacheFile is a no-op for direct client
func (c *IRODSFSClientDirect) CacheFile(irodsPath string, transferCallback irodsclient_common.TransferTrackerCallback) error {
	return nil
}

// DownloadFile downloads a file from iRODS to local filesystem
func (c *IRODSFSClientDirect) DownloadFile(irodsPath string, localPath string, transferCallback irodsclient_common.TransferTrackerCallback) error {
	logger := c.logger.WithFields(log.Fields{
		"irodsPath": irodsPath,
		"localPath": localPath,
	})

	defer util.StackTraceFromPanic(logger)

	_, err := c.fs.DownloadFile(irodsPath, "", localPath, false, transferCallback)
	return err
}

// DownloadFileParallel downloads a file from iRODS to local filesystem in parallel
func (c *IRODSFSClientDirect) DownloadFileParallel(irodsPath string, localPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	logger := c.logger.WithFields(log.Fields{
		"irodsPath": irodsPath,
		"localPath": localPath,
		"taskNum":   taskNum,
	})

	defer util.StackTraceFromPanic(logger)

	_, err := c.fs.DownloadFileParallel(irodsPath, "", localPath, taskNum, false, transferCallback)
	return err
}

func (c *IRODSFSClientDirect) DownloadFileWithCallback(irodsPath string, blockSize int, numBlocks int, blockReadyCallback irodsclient_common.DataObjectBlockCallback, transferCallback irodsclient_common.TransferTrackerCallback) error {
	logger := c.logger.WithFields(log.Fields{
		"irodsPath": irodsPath,
		"blockSize": blockSize,
		"numBlocks": numBlocks,
	})

	defer util.StackTraceFromPanic(logger)

	_, err := c.fs.DownloadFileWithCallback(irodsPath, "", blockSize, numBlocks, blockReadyCallback, transferCallback)
	return err
}

func (c *IRODSFSClientDirect) DownloadFileParallelWithCallback(irodsPath string, blockSize int, numBlocks int, blockReadyCallback irodsclient_common.DataObjectBlockCallback, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	logger := c.logger.WithFields(log.Fields{
		"irodsPath": irodsPath,
		"blockSize": blockSize,
		"numBlocks": numBlocks,
		"taskNum":   taskNum,
	})

	defer util.StackTraceFromPanic(logger)

	_, err := c.fs.DownloadFileParallelWithCallback(irodsPath, "", blockSize, numBlocks, blockReadyCallback, taskNum, transferCallback)
	return err
}

// UploadFile uploads a file from local filesystem to iRODS
func (c *IRODSFSClientDirect) UploadFile(localPath string, irodsPath string, transferCallback irodsclient_common.TransferTrackerCallback) error {
	logger := c.logger.WithFields(log.Fields{
		"localPath": localPath,
		"irodsPath": irodsPath,
	})

	defer util.StackTraceFromPanic(logger)

	_, err := c.fs.UploadFile(localPath, irodsPath, "", false, false, transferCallback)
	return err
}

// UploadFileParallel uploads a file from local filesystem to iRODS in parallel
func (c *IRODSFSClientDirect) UploadFileParallel(localPath string, irodsPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	logger := c.logger.WithFields(log.Fields{
		"localPath": localPath,
		"irodsPath": irodsPath,
		"taskNum":   taskNum,
	})

	defer util.StackTraceFromPanic(logger)

	_, err := c.fs.UploadFileParallel(localPath, irodsPath, "", taskNum, false, false, transferCallback)
	return err
}

// IRODSFSClientDirectFileHandle implements IRODSFSFileHandle
type IRODSFSClientDirectFileHandle struct {
	id     string
	client *IRODSFSClientDirect
	handle *irodsclient_fs.FileHandle
	logger *log.Entry
}

func (h *IRODSFSClientDirectFileHandle) GetID() string {
	return h.handle.GetID()
}

func (h *IRODSFSClientDirectFileHandle) GetEntry() *irodsclient_fs.Entry {
	return h.handle.GetEntry()
}

func (h *IRODSFSClientDirectFileHandle) GetOpenMode() irodsclient_types.FileOpenMode {
	return h.handle.GetOpenMode()
}

func (h *IRODSFSClientDirectFileHandle) IsReadMode() bool {
	return h.handle.IsReadMode()
}

func (h *IRODSFSClientDirectFileHandle) IsWriteMode() bool {
	return h.handle.IsWriteMode()
}

func (h *IRODSFSClientDirectFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	defer util.StackTraceFromPanic(h.logger)

	readLen, err := h.handle.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
		return readLen, err
	}
	return readLen, err
}

func (h *IRODSFSClientDirectFileHandle) GetAvailable(offset int64) int64 {
	// unknown
	return -1
}

func (h *IRODSFSClientDirectFileHandle) WriteAt(data []byte, offset int64) (int, error) {
	defer util.StackTraceFromPanic(h.logger)

	writeLen, err := h.handle.WriteAt(data, offset)
	if err != nil {
		return writeLen, err
	}
	return writeLen, nil
}

func (h *IRODSFSClientDirectFileHandle) Truncate(size int64) error {
	defer util.StackTraceFromPanic(h.logger)

	err := h.handle.Truncate(size)
	if err != nil {
		return err
	}
	return nil
}

func (h *IRODSFSClientDirectFileHandle) Flush() error {
	return nil
}

func (h *IRODSFSClientDirectFileHandle) Close() error {
	defer util.StackTraceFromPanic(h.logger)

	err := h.handle.Close()
	if err != nil {
		return err
	}
	return nil
}
