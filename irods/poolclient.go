package irods

import (
	"fmt"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/util"
	irodsfs_pool_client "github.com/cyverse/irodsfs-pool/client"
	log "github.com/sirupsen/logrus"
)

// IRODSFSClientPool implements IRODSClient interface with iRODS FUSE Lite Pool
// pool access
// implements interfaces defined in interface.go
type IRODSFSClientPool struct {
	config      *irodsclient_fs.FileSystemConfig
	host        string
	account     *irodsclient_types.IRODSAccount
	poolClient  *irodsfs_pool_client.PoolServiceClient
	poolSession *irodsfs_pool_client.PoolServiceSession
}

// NewIRODSFSClientPool creates IRODSFSClient using IRODSFSClientPool
func NewIRODSFSClientPool(poolHost string, poolPort int, account *irodsclient_types.IRODSAccount, config *irodsclient_fs.FileSystemConfig, clientID string) (IRODSFSClient, error) {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"function": "NewIRODSFSClientPool",
	})

	defer util.StackTraceFromPanic(logger)

	poolHostPort := fmt.Sprintf("%s:%d", poolHost, poolPort)
	poolServiceClient := irodsfs_pool_client.NewPoolServiceClient(poolHostPort, config.OperationTimeout)

	logger.Infof("Connect to pool service - %s", poolHostPort)
	err := poolServiceClient.Connect()
	if err != nil {
		return nil, err
	}

	logger.Infof("Login to pool service - user %s", account.ClientUser)
	poolServiceSession, err := poolServiceClient.Login(account, config.ApplicationName, clientID)
	if err != nil {
		return nil, err
	}

	logger.Info("Logged in to pool service")
	return &IRODSFSClientPool{
		config:      config,
		host:        poolHostPort,
		account:     account,
		poolClient:  poolServiceClient,
		poolSession: poolServiceSession,
	}, nil
}

// GetAccount returns iRODS Account info
func (client *IRODSFSClientPool) GetAccount() *irodsclient_types.IRODSAccount {
	return client.account
}

// GetApplicationName returns application name
func (client *IRODSFSClientPool) GetApplicationName() string {
	return client.config.ApplicationName
}

// Release releases resources
func (client *IRODSFSClientPool) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "Release",
	})

	defer util.StackTraceFromPanic(logger)

	client.poolClient.Logout(client.poolSession)
	client.poolClient.Disconnect()
}

// List lists directory entries
func (client *IRODSFSClientPool) List(path string) ([]*irodsclient_fs.Entry, error) {
	if client.poolClient == nil {
		return nil, fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "List",
	})

	defer util.StackTraceFromPanic(logger)

	return client.poolClient.List(client.poolSession, path)
}

// Stat stats fs entry
func (client *IRODSFSClientPool) Stat(path string) (*irodsclient_fs.Entry, error) {
	if client.poolClient == nil {
		return nil, fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "Stat",
	})

	defer util.StackTraceFromPanic(logger)

	return client.poolClient.Stat(client.poolSession, path)
}

// ExistsDir checks existance of a dir
func (client *IRODSFSClientPool) ExistsDir(path string) bool {
	if client.poolClient == nil {
		return false
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "ExistsDir",
	})

	defer util.StackTraceFromPanic(logger)

	return client.poolClient.ExistsDir(client.poolSession, path)
}

// ListUserGroups lists user groups
func (client *IRODSFSClientPool) ListUserGroups(user string) ([]*irodsclient_types.IRODSUser, error) {
	if client.poolClient == nil {
		return nil, fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "ListUserGroups",
	})

	defer util.StackTraceFromPanic(logger)

	return client.poolClient.ListUserGroups(client.poolSession, user)
}

// ListDirACLs lists directory ACLs
func (client *IRODSFSClientPool) ListDirACLs(path string) ([]*irodsclient_types.IRODSAccess, error) {
	if client.poolClient == nil {
		return nil, fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "ListDirACLs",
	})

	defer util.StackTraceFromPanic(logger)

	return client.poolClient.ListDirACLs(client.poolSession, path)
}

// ListFileACLs lists file ACLs
func (client *IRODSFSClientPool) ListFileACLs(path string) ([]*irodsclient_types.IRODSAccess, error) {
	if client.poolClient == nil {
		return nil, fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "ListFileACLs",
	})

	defer util.StackTraceFromPanic(logger)

	return client.poolClient.ListFileACLs(client.poolSession, path)
}

// RemoveFile removes a file
func (client *IRODSFSClientPool) RemoveFile(path string, force bool) error {
	if client.poolClient == nil {
		return fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "RemoveFile",
	})

	defer util.StackTraceFromPanic(logger)

	return client.poolClient.RemoveFile(client.poolSession, path, force)
}

// RemoveDir removes a directory
func (client *IRODSFSClientPool) RemoveDir(path string, recurse bool, force bool) error {
	if client.poolClient == nil {
		return fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "RemoveDir",
	})

	defer util.StackTraceFromPanic(logger)

	return client.poolClient.RemoveDir(client.poolSession, path, recurse, force)
}

// MakeDir makes a new directory
func (client *IRODSFSClientPool) MakeDir(path string, recurse bool) error {
	if client.poolClient == nil {
		return fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "MakeDir",
	})

	defer util.StackTraceFromPanic(logger)

	return client.poolClient.MakeDir(client.poolSession, path, recurse)
}

// RenameDirToDir renames a directory, dest path is also a non-existing path for dir
func (client *IRODSFSClientPool) RenameDirToDir(srcPath string, destPath string) error {
	if client.poolClient == nil {
		return fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "RenameDirToDir",
	})

	defer util.StackTraceFromPanic(logger)

	return client.poolClient.RenameDirToDir(client.poolSession, srcPath, destPath)
}

// RenameFileToFile renames a file, dest path is also a non-existing path for file
func (client *IRODSFSClientPool) RenameFileToFile(srcPath string, destPath string) error {
	if client.poolClient == nil {
		return fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "RenameFileToFile",
	})

	defer util.StackTraceFromPanic(logger)

	return client.poolClient.RenameFileToFile(client.poolSession, srcPath, destPath)
}

// CreateFile creates a file
func (client *IRODSFSClientPool) CreateFile(path string, resource string, mode string) (IRODSFSFileHandle, error) {
	if client.poolClient == nil {
		return nil, fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "CreateFile",
	})

	defer util.StackTraceFromPanic(logger)

	handle, err := client.poolClient.CreateFile(client.poolSession, path, resource, mode)
	if err != nil {
		return nil, err
	}

	fileHandle := &IRODSFSClientPoolFileHandle{
		id:     handle.GetFileHandleID(),
		client: client,
		handle: handle,
	}

	return fileHandle, nil
}

// OpenFile opens a file
func (client *IRODSFSClientPool) OpenFile(path string, resource string, mode string) (IRODSFSFileHandle, error) {
	if client.poolClient == nil {
		return nil, fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "OpenFile",
	})

	defer util.StackTraceFromPanic(logger)

	handle, err := client.poolClient.OpenFile(client.poolSession, path, resource, mode)
	if err != nil {
		return nil, err
	}

	fileHandle := &IRODSFSClientPoolFileHandle{
		id:     handle.GetFileHandleID(),
		client: client,
		handle: handle,
	}

	return fileHandle, nil
}

// TruncateFile truncates a file
func (client *IRODSFSClientPool) TruncateFile(path string, size int64) error {
	if client.poolClient == nil {
		return fmt.Errorf("FSClient is nil")
	}

	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPool",
		"function": "TruncateFile",
	})

	defer util.StackTraceFromPanic(logger)

	return client.poolClient.TruncateFile(client.poolSession, path, size)
}

// IRODSFSClientPoolFileHandle implements IRODSFileHandle
type IRODSFSClientPoolFileHandle struct {
	id     string
	client *IRODSFSClientPool
	handle *irodsfs_pool_client.PoolServiceFileHandle
}

func (handle *IRODSFSClientPoolFileHandle) GetID() string {
	return handle.id
}

func (handle *IRODSFSClientPoolFileHandle) GetEntry() *irodsclient_fs.Entry {
	return handle.handle.GetEntry()
}

func (handle *IRODSFSClientPoolFileHandle) GetOpenMode() irodsclient_types.FileOpenMode {
	return irodsclient_types.FileOpenMode(handle.handle.GetOpenMode())
}

func (handle *IRODSFSClientPoolFileHandle) GetOffset() int64 {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPoolFileHandle",
		"function": "GetOffset",
	})

	defer util.StackTraceFromPanic(logger)

	return handle.client.poolClient.GetOffset(handle.handle)
}

func (handle *IRODSFSClientPoolFileHandle) IsReadMode() bool {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPoolFileHandle",
		"function": "IsReadMode",
	})

	defer util.StackTraceFromPanic(logger)

	return handle.handle.IsReadMode()
}

func (handle *IRODSFSClientPoolFileHandle) IsWriteMode() bool {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPoolFileHandle",
		"function": "IsWriteMode",
	})

	defer util.StackTraceFromPanic(logger)

	return handle.handle.IsWriteMode()
}

func (handle *IRODSFSClientPoolFileHandle) ReadAt(offset int64, length int) ([]byte, error) {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPoolFileHandle",
		"function": "ReadAt",
	})

	defer util.StackTraceFromPanic(logger)

	return handle.client.poolClient.ReadAt(handle.handle, offset, int32(length))
}

func (handle *IRODSFSClientPoolFileHandle) WriteAt(offset int64, data []byte) error {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPoolFileHandle",
		"function": "WriteAt",
	})

	defer util.StackTraceFromPanic(logger)

	return handle.client.poolClient.WriteAt(handle.handle, offset, data)
}

func (handle *IRODSFSClientPoolFileHandle) Flush() error {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPoolFileHandle",
		"function": "Flush",
	})

	defer util.StackTraceFromPanic(logger)

	return handle.client.poolClient.Flush(handle.handle)
}

func (handle *IRODSFSClientPoolFileHandle) Close() error {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"struct":   "IRODSFSClientPoolFileHandle",
		"function": "Close",
	})

	defer util.StackTraceFromPanic(logger)

	return handle.client.poolClient.Close(handle.handle)
}
