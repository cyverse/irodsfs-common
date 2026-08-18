package irods

import (
	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
	irodsclient_metrics "github.com/cyverse/go-irodsclient/irods/metrics"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
)

var (
	ErrNotReadMode  = errors.New("file is not opened in read mode")
	ErrNotWriteMode = errors.New("file is not opened in write mode")
	ErrFileStaging  = errors.New("file is in staging state and not yet synced to iRODS")
)

type IRODSFSClient interface {
	Release()

	GetAccount() *irodsclient_types.IRODSAccount
	GetApplicationName() string

	GetOpenConnections() int
	GetMetrics() *irodsclient_metrics.IRODSMetrics

	// API
	List(path string) ([]*irodsclient_fs.Entry, error)
	Stat(path string) (*irodsclient_fs.Entry, error)
	ExistsDir(path string) bool
	ExistsFile(path string) bool
	RemoveFile(path string, force bool) error
	RemoveDir(path string, recurse bool, force bool) error
	MakeDir(path string, recurse bool) error
	RenameDirToDir(srcPath string, destPath string) error
	RenameFileToFile(srcPath string, destPath string) error
	CreateFile(path string, mode string) (IRODSFSFileHandle, error)
	OpenFile(path string, mode string) (IRODSFSFileHandle, error)
	TruncateFile(path string, size int64) error

	// Sync
	Sync() error

	// File Transfer
	DownloadFile(irodsPath string, localPath string, transferCallback irodsclient_common.TransferTrackerCallback) error
	DownloadFileParallel(irodsPath string, localPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error
	UploadFile(localPath string, irodsPath string, transferCallback irodsclient_common.TransferTrackerCallback) error
	UploadFileParallel(localPath string, irodsPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error

	// Cache
	CacheFile(irodsPath string, transferCallback irodsclient_common.TransferTrackerCallback) error
}

type IRODSFSFileHandle interface {
	GetID() string
	GetEntry() *irodsclient_fs.Entry
	GetOpenMode() irodsclient_types.FileOpenMode
	IsReadMode() bool
	IsWriteMode() bool
	ReadAt(buffer []byte, offset int64) (int, error)
	GetAvailable(offset int64) int64
	WriteAt(data []byte, offset int64) (int, error)
	Truncate(size int64) error
	Flush() error
	Close() error
}
