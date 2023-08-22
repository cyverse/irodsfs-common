package irods

import (
	"io"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_metrics "github.com/cyverse/go-irodsclient/irods/metrics"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

// IRODSFSClientDirect implements IRODSClient interface with go-irodsclient
// direct access to iRODS server
// implements interfaces defined in interface.go
type IRODSFSClientDirect struct {
	config  *irodsclient_fs.FileSystemConfig
	account *irodsclient_types.IRODSAccount
	fs      *irodsclient_fs.FileSystem
}

// NewIRODSFSClientDirect creates IRODSFSClient using IRODSFSClientDirect
func NewIRODSFSClientDirect(account *irodsclient_types.IRODSAccount, config *irodsclient_fs.FileSystemConfig) (IRODSFSClient, error) {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"function": "NewIRODSFSClientDirect",
	})

	defer utils.StackTraceFromPanic(logger)

	fs, err := irodsclient_fs.NewFileSystem(account, config)
	if err != nil {
		return nil, err
	}

	return &IRODSFSClientDirect{
		config:  config,
		account: account,
		fs:      fs,
	}, nil
}

// GetAccount returns iRODS Account info
func (client *IRODSFSClientDirect) GetAccount() *irodsclient_types.IRODSAccount {
	return client.account
}

// GetApplicationName returns application name
func (client *IRODSFSClientDirect) GetApplicationName() string {
	return client.config.ApplicationName
}

// GetConnections() returns total number of connections
func (client *IRODSFSClientDirect) GetConnections() int {
	return client.fs.ConnectionTotal()
}

// GetTransferMetrics() returns transfer metrics
func (client *IRODSFSClientDirect) GetMetrics() *irodsclient_metrics.IRODSMetrics {
	return client.fs.GetMetrics()
}

// Release releases resources
func (client *IRODSFSClientDirect) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "Release",
	})

	defer utils.StackTraceFromPanic(logger)

	if client.fs != nil {
		client.fs.Release()
		client.fs = nil
	}
}

// List lists directory entries
func (client *IRODSFSClientDirect) List(path string) ([]*irodsclient_fs.Entry, error) {
	if client.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "List",
	})

	defer utils.StackTraceFromPanic(logger)

	entries, err := client.fs.List(path)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// Stat stats fs entry
func (client *IRODSFSClientDirect) Stat(path string) (*irodsclient_fs.Entry, error) {
	if client.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "Stat",
	})

	defer utils.StackTraceFromPanic(logger)

	entry, err := client.fs.Stat(path)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

// ListXattr lists xattr
func (client *IRODSFSClientDirect) ListXattr(path string) ([]*irodsclient_types.IRODSMeta, error) {
	if client.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "ListXattr",
	})

	defer utils.StackTraceFromPanic(logger)

	metadatas, err := client.fs.ListMetadata(path)
	if err != nil {
		return nil, err
	}
	return metadatas, nil
}

// GetXattr returns xattr value
func (client *IRODSFSClientDirect) GetXattr(path string, name string) (*irodsclient_types.IRODSMeta, error) {
	if client.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "GetXattr",
	})

	defer utils.StackTraceFromPanic(logger)

	metas, err := client.fs.ListMetadata(path)
	if err != nil {
		return nil, err
	}

	for _, meta := range metas {
		if meta.Name == name {
			return meta, nil
		}
	}

	// if we don't find any, return nil
	return nil, nil
}

// SetXattr sets xattr
func (client *IRODSFSClientDirect) SetXattr(path string, name string, value string) error {
	if client.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "SetXattr",
	})

	defer utils.StackTraceFromPanic(logger)

	// remove first if exists, ignore error if raised
	// this is required as we can have multiple metadata with same name in iRODS
	client.fs.DeleteMetadata(path, name, "", "")

	err := client.fs.AddMetadata(path, name, value, "")
	if err != nil {
		return err
	}

	return nil
}

// RemoveXattr removes xattr
func (client *IRODSFSClientDirect) RemoveXattr(path string, name string) error {
	if client.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "RemoveXattr",
	})

	defer utils.StackTraceFromPanic(logger)

	err := client.fs.DeleteMetadata(path, name, "", "")
	if err != nil {
		return err
	}

	return nil
}

// ExistsDir checks existance of a dir
func (client *IRODSFSClientDirect) ExistsDir(path string) bool {
	if client.fs == nil {
		return false
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "ExistsDir",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.fs.ExistsDir(path)
}

// ExistsFile checks existance of a file
func (client *IRODSFSClientDirect) ExistsFile(path string) bool {
	if client.fs == nil {
		return false
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "ExistsFile",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.fs.ExistsFile(path)
}

// ListUserGroups lists user groups
func (client *IRODSFSClientDirect) ListUserGroups(user string) ([]*irodsclient_types.IRODSUser, error) {
	if client.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "ListUserGroups",
	})

	defer utils.StackTraceFromPanic(logger)

	groups, err := client.fs.ListUserGroups(user)
	if err != nil {
		return nil, err
	}
	return groups, nil
}

// ListDirACLs lists directory ACLs
func (client *IRODSFSClientDirect) ListDirACLs(path string) ([]*irodsclient_types.IRODSAccess, error) {
	if client.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "ListDirACLs",
	})

	defer utils.StackTraceFromPanic(logger)

	accesses, err := client.fs.ListDirACLs(path)
	if err != nil {
		return nil, err
	}
	return accesses, nil
}

// ListFileACLs lists file ACLs
func (client *IRODSFSClientDirect) ListFileACLs(path string) ([]*irodsclient_types.IRODSAccess, error) {
	if client.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "ListFileACLs",
	})

	defer utils.StackTraceFromPanic(logger)

	accesses, err := client.fs.ListFileACLs(path)
	if err != nil {
		return nil, err
	}
	return accesses, nil
}

// ListACLsForEntries lists ACLs for entries in a collection
func (client *IRODSFSClientDirect) ListACLsForEntries(path string) ([]*irodsclient_types.IRODSAccess, error) {
	if client.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "ListACLsForEntries",
	})

	defer utils.StackTraceFromPanic(logger)

	accesses, err := client.fs.ListACLsForEntries(path)
	if err != nil {
		return nil, err
	}
	return accesses, nil
}

// RemoveFile removes a file
func (client *IRODSFSClientDirect) RemoveFile(path string, force bool) error {
	if client.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "RemoveFile",
	})

	defer utils.StackTraceFromPanic(logger)

	err := client.fs.RemoveFile(path, force)
	if err != nil {
		return err
	}
	return nil
}

// RemoveDir removes a directory
func (client *IRODSFSClientDirect) RemoveDir(path string, recurse bool, force bool) error {
	if client.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "RemoveDir",
	})

	defer utils.StackTraceFromPanic(logger)

	err := client.fs.RemoveDir(path, recurse, force)
	if err != nil {
		return err
	}
	return nil
}

// MakeDir makes a new directory
func (client *IRODSFSClientDirect) MakeDir(path string, recurse bool) error {
	if client.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "MakeDir",
	})

	defer utils.StackTraceFromPanic(logger)

	err := client.fs.MakeDir(path, recurse)
	if err != nil {
		return err
	}
	return nil
}

// RenameDirToDir renames a directory, dest path is also a non-existing path for dir
func (client *IRODSFSClientDirect) RenameDirToDir(srcPath string, destPath string) error {
	if client.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "RenameDirToDir",
	})

	defer utils.StackTraceFromPanic(logger)

	err := client.fs.RenameDirToDir(srcPath, destPath)
	if err != nil {
		return err
	}
	return nil
}

// RenameFileToFile renames a file, dest path is also a non-existing path for file
func (client *IRODSFSClientDirect) RenameFileToFile(srcPath string, destPath string) error {
	if client.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "RenameFileToFile",
	})

	defer utils.StackTraceFromPanic(logger)

	err := client.fs.RenameFileToFile(srcPath, destPath)
	if err != nil {
		return err
	}
	return nil
}

// CreateFile creates a file
func (client *IRODSFSClientDirect) CreateFile(path string, resource string, mode string) (IRODSFSFileHandle, error) {
	if client.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "CreateFile",
	})

	defer utils.StackTraceFromPanic(logger)

	handle, err := client.fs.CreateFile(path, resource, mode)
	if err != nil {
		return nil, err
	}

	fileHandle := &IRODSFSClientDirectFileHandle{
		handle: handle,
	}

	return fileHandle, nil
}

// OpenFile opens a file
func (client *IRODSFSClientDirect) OpenFile(path string, resource string, mode string) (IRODSFSFileHandle, error) {
	if client.fs == nil {
		return nil, xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "OpenFile",
	})

	defer utils.StackTraceFromPanic(logger)

	handle, err := client.fs.OpenFile(path, resource, mode)
	if err != nil {
		return nil, err
	}

	fileHandle := &IRODSFSClientDirectFileHandle{
		handle: handle,
	}

	return fileHandle, nil
}

// TruncateFile truncates a file
func (client *IRODSFSClientDirect) TruncateFile(path string, size int64) error {
	if client.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "TruncateFile",
	})

	defer utils.StackTraceFromPanic(logger)

	err := client.fs.TruncateFile(path, size)
	if err != nil {
		return err
	}
	return nil
}

func (client *IRODSFSClientDirect) AddCacheEventHandler(handler irodsclient_fs.FilesystemCacheEventHandler) (string, error) {
	if client.fs == nil {
		return "", xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "AddCacheEventHandler",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.fs.AddCacheEventHandler(handler), nil
}

func (client *IRODSFSClientDirect) RemoveCacheEventHandler(handlerID string) error {
	if client.fs == nil {
		return xerrors.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "RemoveCacheEventHandler",
	})

	defer utils.StackTraceFromPanic(logger)

	client.fs.RemoveCacheEventHandler(handlerID)
	return nil
}

// IRODSFSClientDirectFileHandle implements IRODSFSFileHandle
type IRODSFSClientDirectFileHandle struct {
	handle *irodsclient_fs.FileHandle
}

func (handle *IRODSFSClientDirectFileHandle) GetID() string {
	return handle.handle.GetID()
}

func (handle *IRODSFSClientDirectFileHandle) GetEntry() *irodsclient_fs.Entry {
	return handle.handle.GetEntry()
}

func (handle *IRODSFSClientDirectFileHandle) GetOpenMode() irodsclient_types.FileOpenMode {
	return handle.handle.GetOpenMode()
}

func (handle *IRODSFSClientDirectFileHandle) GetOffset() int64 {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirectFileHandle",
		"function": "GetOffset",
	})

	defer utils.StackTraceFromPanic(logger)

	return handle.handle.GetOffset()
}

func (handle *IRODSFSClientDirectFileHandle) IsReadMode() bool {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirectFileHandle",
		"function": "IsReadMode",
	})

	defer utils.StackTraceFromPanic(logger)

	return handle.handle.IsReadMode()
}

func (handle *IRODSFSClientDirectFileHandle) IsWriteMode() bool {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirectFileHandle",
		"function": "IsWriteMode",
	})

	defer utils.StackTraceFromPanic(logger)

	return handle.handle.IsWriteMode()
}

func (handle *IRODSFSClientDirectFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirectFileHandle",
		"function": "ReadAt",
	})

	defer utils.StackTraceFromPanic(logger)

	readLen, err := handle.handle.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
		return readLen, err
	}
	return readLen, err
}

func (handle *IRODSFSClientDirectFileHandle) GetAvailable(offset int64) int64 {
	// unknown
	return -1
}

func (handle *IRODSFSClientDirectFileHandle) WriteAt(data []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirectFileHandle",
		"function": "WriteAt",
	})

	defer utils.StackTraceFromPanic(logger)

	writeLen, err := handle.handle.WriteAt(data, offset)
	if err != nil {
		return writeLen, err
	}
	return writeLen, nil
}

func (handle *IRODSFSClientDirectFileHandle) Lock(wait bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirectFileHandle",
		"function": "Truncate",
	})

	defer utils.StackTraceFromPanic(logger)

	return handle.handle.LockDataObject(wait)
}

func (handle *IRODSFSClientDirectFileHandle) RLock(wait bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirectFileHandle",
		"function": "Truncate",
	})

	defer utils.StackTraceFromPanic(logger)

	return handle.handle.RLockDataObject(wait)
}

func (handle *IRODSFSClientDirectFileHandle) Unlock() error {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirectFileHandle",
		"function": "Truncate",
	})

	defer utils.StackTraceFromPanic(logger)

	return handle.handle.UnlockDataObject()
}

func (handle *IRODSFSClientDirectFileHandle) Truncate(size int64) error {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirectFileHandle",
		"function": "Truncate",
	})

	defer utils.StackTraceFromPanic(logger)

	err := handle.handle.Truncate(size)
	if err != nil {
		return err
	}
	return nil
}

func (handle *IRODSFSClientDirectFileHandle) Flush() error {
	return nil
}

func (handle *IRODSFSClientDirectFileHandle) Close() error {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirectFileHandle",
		"function": "Close",
	})

	defer utils.StackTraceFromPanic(logger)

	err := handle.handle.Close()
	if err != nil {
		return err
	}
	return nil
}
