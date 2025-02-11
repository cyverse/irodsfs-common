package irods

import (
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_metrics "github.com/cyverse/go-irodsclient/irods/metrics"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
)

type IRODSFSClient interface {
	Release()

	GetAccount() *irodsclient_types.IRODSAccount
	GetApplicationName() string

	GetConnections() int
	GetMetrics() *irodsclient_metrics.IRODSMetrics

	// API
	List(path string) ([]*irodsclient_fs.Entry, error)
	Stat(path string) (*irodsclient_fs.Entry, error)
	ListXattr(path string) ([]*irodsclient_types.IRODSMeta, error)
	GetXattr(path string, name string) (*irodsclient_types.IRODSMeta, error)
	SetXattr(path string, name string, value string) error
	RemoveXattr(path string, name string) error
	ExistsDir(path string) bool
	ExistsFile(path string) bool
	ListUserGroups(zoneName string, username string) ([]*irodsclient_types.IRODSUser, error)
	ListDirACLs(path string) ([]*irodsclient_types.IRODSAccess, error)
	ListFileACLs(path string) ([]*irodsclient_types.IRODSAccess, error)
	ListACLsForEntries(path string) ([]*irodsclient_types.IRODSAccess, error)
	RemoveFile(path string, force bool) error
	RemoveDir(path string, recurse bool, force bool) error
	MakeDir(path string, recurse bool) error
	RenameDirToDir(srcPath string, destPath string) error
	RenameFileToFile(srcPath string, destPath string) error
	CreateFile(path string, resource string, mode string) (IRODSFSFileHandle, error)
	OpenFile(path string, resource string, mode string) (IRODSFSFileHandle, error)
	TruncateFile(path string, size int64) error

	// Cache
	AddCacheEventHandler(handler irodsclient_fs.FilesystemCacheEventHandler) (string, error)
	RemoveCacheEventHandler(handlerID string) error
}

type IRODSFSFileHandle interface {
	GetID() string
	GetEntry() *irodsclient_fs.Entry
	GetOpenMode() irodsclient_types.FileOpenMode
	GetOffset() int64
	IsReadMode() bool
	IsWriteMode() bool
	ReadAt(buffer []byte, offset int64) (int, error)
	GetAvailable(offset int64) int64
	WriteAt(data []byte, offset int64) (int, error)
	Lock(wait bool) error
	RLock(wait bool) error
	Unlock() error
	Truncate(size int64) error
	Flush() error
	Close() error
}
