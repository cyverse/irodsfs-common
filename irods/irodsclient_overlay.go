package irods

import (
	"io"
	"os"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_metrics "github.com/cyverse/go-irodsclient/irods/metrics"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

// IRODSFSClientOverlay implements IRODSClient interface with go-irodsclient but with overlay
// file I/O is first sent to local disk then sent to iRODS server
// direct access to iRODS server
// implements interfaces defined in interface.go
type IRODSFSClientOverlay struct {
	clientDirect   *IRODSFSClientDirect
	overlayDirPath string
}

// NewIRODSFSClientOverlay creates IRODSFSClient using IRODSFSClientOverlay
func NewIRODSFSClientOverlay(account *irodsclient_types.IRODSAccount, config *irodsclient_fs.FileSystemConfig, overlayDirPath string) (IRODSFSClient, error) {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"function": "NewIRODSFSClientOverlay",
	})

	defer utils.StackTraceFromPanic(logger)

	fs, err := irodsclient_fs.NewFileSystem(account, config)
	if err != nil {
		return nil, err
	}

	clientDirect := &IRODSFSClientDirect{
		config:  config,
		account: account,
		fs:      fs,
	}

	return &IRODSFSClientOverlay{
		clientDirect:   clientDirect,
		overlayDirPath: overlayDirPath,
	}, nil
}

// GetAccount returns iRODS Account info
func (client *IRODSFSClientOverlay) GetAccount() *irodsclient_types.IRODSAccount {
	return client.clientDirect.GetAccount()
}

// GetApplicationName returns application name
func (client *IRODSFSClientOverlay) GetApplicationName() string {
	return client.clientDirect.GetApplicationName()
}

// GetConnections() returns total number of connections
func (client *IRODSFSClientOverlay) GetConnections() int {
	return client.clientDirect.GetConnections()
}

// GetTransferMetrics() returns transfer metrics
func (client *IRODSFSClientOverlay) GetMetrics() *irodsclient_metrics.IRODSMetrics {
	return client.clientDirect.GetMetrics()
}

// Release releases resources
func (client *IRODSFSClientOverlay) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "Release",
	})

	defer utils.StackTraceFromPanic(logger)

	client.clientDirect.Release()
}

// List lists directory entries
func (client *IRODSFSClientOverlay) List(path string) ([]*irodsclient_fs.Entry, error) {
	if client.clientDirect.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "List",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.List(path)
}

// Stat stats fs entry
func (client *IRODSFSClientOverlay) Stat(path string) (*irodsclient_fs.Entry, error) {
	if client.clientDirect.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "Stat",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.Stat(path)
}

// ListXattr lists xattr
func (client *IRODSFSClientOverlay) ListXattr(path string) ([]*irodsclient_types.IRODSMeta, error) {
	if client.clientDirect.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "ListXattr",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.ListXattr(path)
}

// GetXattr returns xattr value
func (client *IRODSFSClientOverlay) GetXattr(path string, name string) (*irodsclient_types.IRODSMeta, error) {
	if client.clientDirect.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "GetXattr",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.GetXattr(path, name)
}

// SetXattr sets xattr
func (client *IRODSFSClientOverlay) SetXattr(path string, name string, value string) error {
	if client.clientDirect.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "SetXattr",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.SetXattr(path, name, value)
}

// RemoveXattr removes xattr
func (client *IRODSFSClientOverlay) RemoveXattr(path string, name string) error {
	if client.clientDirect.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "RemoveXattr",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.RemoveXattr(path, name)
}

// ExistsDir checks existance of a dir
func (client *IRODSFSClientOverlay) ExistsDir(path string) bool {
	if client.clientDirect.fs == nil {
		return false
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "ExistsDir",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.ExistsDir(path)
}

// ExistsFile checks existance of a file
func (client *IRODSFSClientOverlay) ExistsFile(path string) bool {
	if client.clientDirect.fs == nil {
		return false
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "ExistsFile",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.ExistsFile(path)
}

// ListUserGroups lists user groups
func (client *IRODSFSClientOverlay) ListUserGroups(user string) ([]*irodsclient_types.IRODSUser, error) {
	if client.clientDirect.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "ListUserGroups",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.clientDirect.ListUserGroups(user)
}

// ListDirACLs lists directory ACLs
func (client *IRODSFSClientOverlay) ListDirACLs(path string) ([]*irodsclient_types.IRODSAccess, error) {
	if client.clientDirect.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "ListDirACLs",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.ListDirACLs(path)
}

// ListFileACLs lists file ACLs
func (client *IRODSFSClientOverlay) ListFileACLs(path string) ([]*irodsclient_types.IRODSAccess, error) {
	if client.clientDirect.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "ListFileACLs",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.ListFileACLs(path)
}

// ListACLsForEntries lists ACLs for entries in a collection
func (client *IRODSFSClientOverlay) ListACLsForEntries(path string) ([]*irodsclient_types.IRODSAccess, error) {
	if client.clientDirect.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "ListACLsForEntries",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.ListACLsForEntries(path)
}

// RemoveFile removes a file
func (client *IRODSFSClientOverlay) RemoveFile(path string, force bool) error {
	if client.clientDirect.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "RemoveFile",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.RemoveFile(path, force)
}

// RemoveDir removes a directory
func (client *IRODSFSClientOverlay) RemoveDir(path string, recurse bool, force bool) error {
	if client.clientDirect.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "RemoveDir",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.RemoveDir(path, recurse, force)
}

// MakeDir makes a new directory
func (client *IRODSFSClientOverlay) MakeDir(path string, recurse bool) error {
	if client.clientDirect.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "MakeDir",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.MakeDir(path, recurse)
}

// RenameDirToDir renames a directory, dest path is also a non-existing path for dir
func (client *IRODSFSClientOverlay) RenameDirToDir(srcPath string, destPath string) error {
	if client.clientDirect.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "RenameDirToDir",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.RenameDirToDir(srcPath, destPath)
}

// RenameFileToFile renames a file, dest path is also a non-existing path for file
func (client *IRODSFSClientOverlay) RenameFileToFile(srcPath string, destPath string) error {
	if client.clientDirect.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "RenameFileToFile",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.RenameFileToFile(srcPath, destPath)
}

// CreateFile creates a file
func (client *IRODSFSClientOverlay) CreateFile(path string, resource string, mode string) (IRODSFSFileHandle, error) {
	if client.clientDirect.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "CreateFile",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.CreateFile(path, resource, mode)
}

// OpenFile opens a file
func (client *IRODSFSClientOverlay) OpenFile(path string, resource string, mode string) (IRODSFSFileHandle, error) {
	if client.clientDirect.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "OpenFile",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.OpenFile(path, resource, mode)
}

// TruncateFile truncates a file
func (client *IRODSFSClientOverlay) TruncateFile(path string, size int64) error {
	if client.clientDirect.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "TruncateFile",
	})

	defer utils.StackTraceFromPanic(logger)

	// TODO: Add
	return client.clientDirect.TruncateFile(path, size)
}

func (client *IRODSFSClientOverlay) AddCacheEventHandler(handler irodsclient_fs.FilesystemCacheEventHandler) (string, error) {
	if client.clientDirect.fs == nil {
		return "", xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "AddCacheEventHandler",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.clientDirect.AddCacheEventHandler(handler)
}

func (client *IRODSFSClientOverlay) RemoveCacheEventHandler(handlerID string) error {
	if client.clientDirect.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlay",
		"function": "RemoveCacheEventHandler",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.clientDirect.RemoveCacheEventHandler(handlerID)
}

// IRODSFSClientOverlayFileHandle implements IRODSFSFileHandle
type IRODSFSClientOverlayFileHandle struct {
	localHandle   *os.File
	localHandleID string
	handle        *irodsclient_fs.FileHandle
}

func (handle *IRODSFSClientOverlayFileHandle) GetID() string {
	if handle.localHandle != nil {
		return handle.localHandleID
	}

	if handle.handle != nil {
		return handle.handle.GetID()
	}
	return ""
}

func (handle *IRODSFSClientOverlayFileHandle) GetEntry() *irodsclient_fs.Entry {
	return handle.handle.GetEntry()
}

func (handle *IRODSFSClientOverlayFileHandle) GetOpenMode() irodsclient_types.FileOpenMode {
	return handle.handle.GetOpenMode()
}

func (handle *IRODSFSClientOverlayFileHandle) GetOffset() int64 {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlayFileHandle",
		"function": "GetOffset",
	})

	defer utils.StackTraceFromPanic(logger)

	return handle.handle.GetOffset()
}

func (handle *IRODSFSClientOverlayFileHandle) IsReadMode() bool {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlayFileHandle",
		"function": "IsReadMode",
	})

	defer utils.StackTraceFromPanic(logger)

	return handle.handle.IsReadMode()
}

func (handle *IRODSFSClientOverlayFileHandle) IsWriteMode() bool {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlayFileHandle",
		"function": "IsWriteMode",
	})

	defer utils.StackTraceFromPanic(logger)

	return handle.handle.IsWriteMode()
}

func (handle *IRODSFSClientOverlayFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlayFileHandle",
		"function": "ReadAt",
	})

	defer utils.StackTraceFromPanic(logger)

	readLen, err := handle.handle.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
		return readLen, err
	}
	return readLen, err
}

func (handle *IRODSFSClientOverlayFileHandle) GetAvailable(offset int64) int64 {
	// unknown
	return -1
}

func (handle *IRODSFSClientOverlayFileHandle) WriteAt(data []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlayFileHandle",
		"function": "WriteAt",
	})

	defer utils.StackTraceFromPanic(logger)

	writeLen, err := handle.handle.WriteAt(data, offset)
	if err != nil {
		return writeLen, err
	}
	return writeLen, nil
}

func (handle *IRODSFSClientOverlayFileHandle) Lock(wait bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlayFileHandle",
		"function": "Truncate",
	})

	defer utils.StackTraceFromPanic(logger)

	return handle.handle.LockDataObject(wait)
}

func (handle *IRODSFSClientOverlayFileHandle) RLock(wait bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlayFileHandle",
		"function": "Truncate",
	})

	defer utils.StackTraceFromPanic(logger)

	return handle.handle.RLockDataObject(wait)
}

func (handle *IRODSFSClientOverlayFileHandle) Unlock() error {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlayFileHandle",
		"function": "Truncate",
	})

	defer utils.StackTraceFromPanic(logger)

	return handle.handle.UnlockDataObject()
}

func (handle *IRODSFSClientOverlayFileHandle) Truncate(size int64) error {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlayFileHandle",
		"function": "Truncate",
	})

	defer utils.StackTraceFromPanic(logger)

	err := handle.handle.Truncate(size)
	if err != nil {
		return err
	}
	return nil
}

func (handle *IRODSFSClientOverlayFileHandle) Flush() error {
	return nil
}

func (handle *IRODSFSClientOverlayFileHandle) Close() error {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientOverlayFileHandle",
		"function": "Close",
	})

	defer utils.StackTraceFromPanic(logger)

	err := handle.handle.Close()
	if err != nil {
		return err
	}
	return nil
}
