package irods

import (
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
)

type IRODSFSClient interface {
	Release()

	GetAccount() *irodsclient_types.IRODSAccount
	GetApplicationName() string

	GetConnections() int
	GetTransferMetrics() irodsclient_types.TransferMetrics

	// API
	List(path string) ([]*irodsclient_fs.Entry, error)
	Stat(path string) (*irodsclient_fs.Entry, error)
	ExistsDir(path string) bool
	ListUserGroups(user string) ([]*irodsclient_types.IRODSUser, error)
	ListDirACLs(path string) ([]*irodsclient_types.IRODSAccess, error)
	ListFileACLs(path string) ([]*irodsclient_types.IRODSAccess, error)
	RemoveFile(path string, force bool) error
	RemoveDir(path string, recurse bool, force bool) error
	MakeDir(path string, recurse bool) error
	RenameDirToDir(srcPath string, destPath string) error
	RenameFileToFile(srcPath string, destPath string) error
	CreateFile(path string, resource string, mode string) (IRODSFSFileHandle, error)
	OpenFile(path string, resource string, mode string) (IRODSFSFileHandle, error)
	TruncateFile(path string, size int64) error
}

type IRODSFSFileHandle interface {
	GetID() string
	GetEntry() *irodsclient_fs.Entry
	GetOpenMode() irodsclient_types.FileOpenMode
	GetOffset() int64
	IsReadMode() bool
	IsWriteMode() bool
	ReadAt(offset int64, length int) ([]byte, error)
	WriteAt(offset int64, data []byte) error
	Flush() error
	Close() error
}
