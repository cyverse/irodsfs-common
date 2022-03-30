package irods

import (
	"fmt"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
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

	goirodsfs, err := irodsclient_fs.NewFileSystem(account, config)
	if err != nil {
		return nil, err
	}

	return &IRODSFSClientDirect{
		config:  config,
		account: account,
		fs:      goirodsfs,
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
		return nil, fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "List",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.fs.List(path)
}

// Stat stats fs entry
func (client *IRODSFSClientDirect) Stat(path string) (*irodsclient_fs.Entry, error) {
	if client.fs == nil {
		return nil, fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "Stat",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.fs.Stat(path)
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

// ListUserGroups lists user groups
func (client *IRODSFSClientDirect) ListUserGroups(user string) ([]*irodsclient_types.IRODSUser, error) {
	if client.fs == nil {
		return nil, fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "ListUserGroups",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.fs.ListUserGroups(user)
}

// ListDirACLs lists directory ACLs
func (client *IRODSFSClientDirect) ListDirACLs(path string) ([]*irodsclient_types.IRODSAccess, error) {
	if client.fs == nil {
		return nil, fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "ListDirACLs",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.fs.ListDirACLs(path)
}

// ListFileACLs lists file ACLs
func (client *IRODSFSClientDirect) ListFileACLs(path string) ([]*irodsclient_types.IRODSAccess, error) {
	if client.fs == nil {
		return nil, fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "ListFileACLs",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.fs.ListFileACLs(path)
}

// RemoveFile removes a file
func (client *IRODSFSClientDirect) RemoveFile(path string, force bool) error {
	if client.fs == nil {
		return fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "RemoveFile",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.fs.RemoveFile(path, force)
}

// RemoveDir removes a directory
func (client *IRODSFSClientDirect) RemoveDir(path string, recurse bool, force bool) error {
	if client.fs == nil {
		return fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "RemoveDir",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.fs.RemoveDir(path, recurse, force)
}

// MakeDir makes a new directory
func (client *IRODSFSClientDirect) MakeDir(path string, recurse bool) error {
	if client.fs == nil {
		return fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "MakeDir",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.fs.MakeDir(path, recurse)
}

// RenameDirToDir renames a directory, dest path is also a non-existing path for dir
func (client *IRODSFSClientDirect) RenameDirToDir(srcPath string, destPath string) error {
	if client.fs == nil {
		return fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "RenameDirToDir",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.fs.RenameDirToDir(srcPath, destPath)
}

// RenameFileToFile renames a file, dest path is also a non-existing path for file
func (client *IRODSFSClientDirect) RenameFileToFile(srcPath string, destPath string) error {
	if client.fs == nil {
		return fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "RenameFileToFile",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.fs.RenameFileToFile(srcPath, destPath)
}

// CreateFile creates a file
func (client *IRODSFSClientDirect) CreateFile(path string, resource string, mode string) (IRODSFSFileHandle, error) {
	if client.fs == nil {
		return nil, fmt.Errorf("FSClient is nil")
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
		id:     handle.GetID(),
		client: client,
		handle: handle,
	}

	return fileHandle, nil
}

// OpenFile opens a file
func (client *IRODSFSClientDirect) OpenFile(path string, resource string, mode string) (IRODSFSFileHandle, error) {
	if client.fs == nil {
		return nil, fmt.Errorf("FSClient is nil")
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
		id:     handle.GetID(),
		client: client,
		handle: handle,
	}

	return fileHandle, nil
}

// TruncateFile truncates a file
func (client *IRODSFSClientDirect) TruncateFile(path string, size int64) error {
	if client.fs == nil {
		return fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirect",
		"function": "TruncateFile",
	})

	defer utils.StackTraceFromPanic(logger)

	return client.fs.TruncateFile(path, size)
}

// IRODSFSClientDirectFileHandle implements IRODSFSFileHandle
type IRODSFSClientDirectFileHandle struct {
	id     string
	client *IRODSFSClientDirect
	handle *irodsclient_fs.FileHandle
}

func (handle *IRODSFSClientDirectFileHandle) GetID() string {
	return handle.id
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

func (handle *IRODSFSClientDirectFileHandle) ReadAt(offset int64, length int) ([]byte, error) {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirectFileHandle",
		"function": "ReadAt",
	})

	defer utils.StackTraceFromPanic(logger)

	return handle.handle.ReadAt(offset, length)
}

func (handle *IRODSFSClientDirectFileHandle) WriteAt(offset int64, data []byte) error {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientDirectFileHandle",
		"function": "WriteAt",
	})

	defer utils.StackTraceFromPanic(logger)

	return handle.handle.WriteAt(offset, data)
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

	return handle.handle.Close()
}
