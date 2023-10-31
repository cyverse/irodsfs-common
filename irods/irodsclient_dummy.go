package irods

import (
	"bytes"
	"fmt"
	"io"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_metrics "github.com/cyverse/go-irodsclient/irods/metrics"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/utils"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

const (
	dummyIDStart int64 = 90000000
)

// IRODSFSClientDummy implements IRODSClient interface with dummy data
// implements interfaces defined in interface.go
type IRODSFSClientDummy struct {
	account          *irodsclient_types.IRODSAccount
	dummyIDCount     int64
	dummyEntry       map[string]*irodsclient_fs.Entry
	dummyDirEntry    map[string][]*irodsclient_fs.Entry
	dummyFileContent map[string]*bytes.Buffer
}

// NewIRODSFSClientDummy creates IRODSFSClient with dummy data
func NewIRODSFSClientDummy(account *irodsclient_types.IRODSAccount) (IRODSFSClient, error) {
	logger := log.WithFields(log.Fields{
		"package":  "irods",
		"function": "NewIRODSFSClientDummy",
	})

	defer utils.StackTraceFromPanic(logger)

	client := &IRODSFSClientDummy{
		account:          account,
		dummyIDCount:     0,
		dummyEntry:       map[string]*irodsclient_fs.Entry{},
		dummyDirEntry:    map[string][]*irodsclient_fs.Entry{},
		dummyFileContent: map[string]*bytes.Buffer{},
	}

	client.fillDummy()

	return client, nil
}

// GetAccount returns iRODS Account info
func (client *IRODSFSClientDummy) GetAccount() *irodsclient_types.IRODSAccount {
	return client.account
}

// GetApplicationName returns application name
func (client *IRODSFSClientDummy) GetApplicationName() string {
	return "dummy"
}

// GetConnections() returns total number of connections
func (client *IRODSFSClientDummy) GetConnections() int {
	return 0
}

// GetTransferMetrics() returns transfer metrics
func (client *IRODSFSClientDummy) GetMetrics() *irodsclient_metrics.IRODSMetrics {
	return &irodsclient_metrics.IRODSMetrics{}
}

// Release releases resources
func (client *IRODSFSClientDummy) Release() {
}

func (client *IRODSFSClientDummy) makeDummyDir(path string) *irodsclient_fs.Entry {
	client.dummyIDCount++

	return &irodsclient_fs.Entry{
		ID:                dummyIDStart + client.dummyIDCount,
		Type:              irodsclient_fs.DirectoryEntry,
		Name:              utils.GetFileName(path),
		Path:              path,
		Owner:             client.account.ClientUser,
		Size:              0,
		DataType:          "",
		CreateTime:        time.Now(),
		ModifyTime:        time.Now(),
		CheckSumAlgorithm: "",
		CheckSum:          "",
	}
}

func (client *IRODSFSClientDummy) makeDummyFile(path string) *irodsclient_fs.Entry {
	client.dummyIDCount++

	contentLen := int64(0)
	if contentBuf, ok := client.dummyFileContent[path]; ok {
		contentLen = int64(contentBuf.Len())
	}
	return &irodsclient_fs.Entry{
		ID:                dummyIDStart + client.dummyIDCount,
		Type:              irodsclient_fs.FileEntry,
		Name:              utils.GetFileName(path),
		Path:              path,
		Owner:             client.account.ClientUser,
		Size:              contentLen,
		DataType:          "",
		CreateTime:        time.Now(),
		ModifyTime:        time.Now(),
		CheckSumAlgorithm: "",
		CheckSum:          "",
	}
}

// SyncAllDummyFileContentSize sync all dummy file content sizes
func (client *IRODSFSClientDummy) SyncAllDummyFileContentSize() {
	for _, entry := range client.dummyEntry {
		if entry.Type == irodsclient_fs.FileEntry {
			if contentBuf, ok := client.dummyFileContent[entry.Path]; ok {
				entry.Size = int64(contentBuf.Len())
			} else {
				entry.Size = 0
			}
		}
	}
}

// SyncDummyFileContentSize sync dummy file content size
func (client *IRODSFSClientDummy) SyncDummyFileContentSize(path string, contentBuf *bytes.Buffer) {
	if entry, ok := client.dummyEntry[path]; ok {
		client.dummyFileContent[path] = contentBuf
		entry.Size = int64(contentBuf.Len())
	}
}

// GetDummyFileContentBuffer returns dummy file content buffer to modify content
func (client *IRODSFSClientDummy) GetDummyFileContentBuffer(path string) (*bytes.Buffer, error) {
	if contentBuf, ok := client.dummyFileContent[path]; ok {
		return contentBuf, nil
	}
	return nil, xerrors.Errorf("failed to get dummy file content for path %s", path)
}

func (client *IRODSFSClientDummy) fillDummy() {
	rootPath := "/"
	zonePath := fmt.Sprintf("/%s", client.account.ClientZone)
	homePath := fmt.Sprintf("/%s/home", client.account.ClientZone)
	userHomePath := fmt.Sprintf("/%s/home/%s", client.account.ClientZone, client.account.ClientUser)
	errorFilePath := fmt.Sprintf("/%s/home/%s/broken_connection", client.account.ClientZone, client.account.ClientUser)

	rootEntry := client.makeDummyDir(rootPath)
	zoneEntry := client.makeDummyDir(zonePath)
	homeEntry := client.makeDummyDir(homePath)
	userHomeEntry := client.makeDummyDir(userHomePath)
	errorFileEntry := client.makeDummyFile(errorFilePath)

	client.dummyEntry[rootPath] = rootEntry
	client.dummyEntry[zonePath] = zoneEntry
	client.dummyEntry[homePath] = homeEntry
	client.dummyEntry[userHomePath] = userHomeEntry
	client.dummyEntry[errorFilePath] = errorFileEntry

	client.dummyDirEntry[rootPath] = []*irodsclient_fs.Entry{
		zoneEntry,
	}
	client.dummyDirEntry[zonePath] = []*irodsclient_fs.Entry{
		homeEntry,
	}
	client.dummyDirEntry[homePath] = []*irodsclient_fs.Entry{
		userHomeEntry,
	}
	client.dummyDirEntry[userHomePath] = []*irodsclient_fs.Entry{
		errorFileEntry,
	}

	client.dummyFileContent[errorFilePath] = &bytes.Buffer{}
}

// List lists directory entries
func (client *IRODSFSClientDummy) List(path string) ([]*irodsclient_fs.Entry, error) {
	if entries, ok := client.dummyDirEntry[path]; ok {
		return entries, nil
	}

	return nil, xerrors.Errorf("failed to find the directory for path %s: %w", path, irodsclient_types.NewFileNotFoundError(path))
}

// Stat stats fs entry
func (client *IRODSFSClientDummy) Stat(path string) (*irodsclient_fs.Entry, error) {
	if entry, ok := client.dummyEntry[path]; ok {
		return entry, nil
	}

	return nil, xerrors.Errorf("failed to find the file or directory for path %s: %w", path, irodsclient_types.NewFileNotFoundError(path))
}

// ListXattr lists xattr
func (client *IRODSFSClientDummy) ListXattr(path string) ([]*irodsclient_types.IRODSMeta, error) {
	if _, ok := client.dummyEntry[path]; ok {
		return []*irodsclient_types.IRODSMeta{}, nil
	}

	return nil, xerrors.Errorf("failed to find the file or directory for path %s: %w", path, irodsclient_types.NewFileNotFoundError(path))
}

// GetXattr returns xattr value
func (client *IRODSFSClientDummy) GetXattr(path string, name string) (*irodsclient_types.IRODSMeta, error) {
	if _, ok := client.dummyEntry[path]; ok {
		return &irodsclient_types.IRODSMeta{}, nil
	}

	return nil, xerrors.Errorf("failed to find the file or directory for path %s: %w", path, irodsclient_types.NewFileNotFoundError(path))
}

// SetXattr sets xattr
func (client *IRODSFSClientDummy) SetXattr(path string, name string, value string) error {
	if _, ok := client.dummyEntry[path]; ok {
		return nil
	}

	return xerrors.Errorf("failed to find the file or directory for path %s: %w", path, irodsclient_types.NewFileNotFoundError(path))
}

// RemoveXattr removes xattr
func (client *IRODSFSClientDummy) RemoveXattr(path string, name string) error {
	return xerrors.Errorf("failed to find the file or directory for path %s: %w", path, irodsclient_types.NewFileNotFoundError(path))
}

// ExistsDir checks existance of a dir
func (client *IRODSFSClientDummy) ExistsDir(path string) bool {
	if entry, ok := client.dummyEntry[path]; ok {
		return entry.Type == irodsclient_fs.DirectoryEntry
	}

	return false
}

// ExistsFile checks existance of a file
func (client *IRODSFSClientDummy) ExistsFile(path string) bool {
	if entry, ok := client.dummyEntry[path]; ok {
		return entry.Type == irodsclient_fs.FileEntry
	}

	return false
}

// ListUserGroups lists user groups
func (client *IRODSFSClientDummy) ListUserGroups(user string) ([]*irodsclient_types.IRODSUser, error) {
	return []*irodsclient_types.IRODSUser{}, nil
}

// ListDirACLs lists directory ACLs
func (client *IRODSFSClientDummy) ListDirACLs(path string) ([]*irodsclient_types.IRODSAccess, error) {
	if entry, ok := client.dummyEntry[path]; ok {
		if entry.Type == irodsclient_fs.DirectoryEntry {
			return []*irodsclient_types.IRODSAccess{
				{
					Path:        path,
					UserName:    client.account.ClientUser,
					UserZone:    client.account.ClientZone,
					UserType:    irodsclient_types.IRODSUserRodsUser,
					AccessLevel: irodsclient_types.IRODSAccessLevelRead,
				},
			}, nil
		}
	}

	return nil, xerrors.Errorf("failed to find the directory for path %s: %w", path, irodsclient_types.NewFileNotFoundError(path))
}

// ListFileACLs lists file ACLs
func (client *IRODSFSClientDummy) ListFileACLs(path string) ([]*irodsclient_types.IRODSAccess, error) {
	if entry, ok := client.dummyEntry[path]; ok {
		if entry.Type == irodsclient_fs.DirectoryEntry {
			return []*irodsclient_types.IRODSAccess{
				{
					Path:        path,
					UserName:    client.account.ClientUser,
					UserZone:    client.account.ClientZone,
					UserType:    irodsclient_types.IRODSUserRodsUser,
					AccessLevel: irodsclient_types.IRODSAccessLevelRead,
				},
			}, nil
		}
	}

	return nil, xerrors.Errorf("failed to find the file for path %s: %w", path, irodsclient_types.NewFileNotFoundError(path))
}

// ListACLsForEntries lists ACLs for entries in a collection
func (client *IRODSFSClientDummy) ListACLsForEntries(path string) ([]*irodsclient_types.IRODSAccess, error) {
	if entries, ok := client.dummyDirEntry[path]; ok {
		accesses := []*irodsclient_types.IRODSAccess{}
		for _, entry := range entries {
			access := &irodsclient_types.IRODSAccess{
				Path:        entry.Path,
				UserName:    client.account.ClientUser,
				UserZone:    client.account.ClientZone,
				UserType:    irodsclient_types.IRODSUserRodsUser,
				AccessLevel: irodsclient_types.IRODSAccessLevelRead,
			}

			accesses = append(accesses, access)
		}

		return accesses, nil
	}

	return nil, xerrors.Errorf("failed to find the directory for path %s: %w", path, irodsclient_types.NewFileNotFoundError(path))
}

// RemoveFile removes a file
func (client *IRODSFSClientDummy) RemoveFile(path string, force bool) error {
	return xerrors.Errorf("failed to remove the file for path %s", path)
}

// RemoveDir removes a directory
func (client *IRODSFSClientDummy) RemoveDir(path string, recurse bool, force bool) error {
	return xerrors.Errorf("failed to remove the directory for path %s", path)
}

// MakeDir makes a new directory
func (client *IRODSFSClientDummy) MakeDir(path string, recurse bool) error {
	return xerrors.Errorf("failed to make the directory for path %s", path)
}

// RenameDirToDir renames a directory, dest path is also a non-existing path for dir
func (client *IRODSFSClientDummy) RenameDirToDir(srcPath string, destPath string) error {
	return xerrors.Errorf("failed to rename the directory for path %s", srcPath)
}

// RenameFileToFile renames a file, dest path is also a non-existing path for file
func (client *IRODSFSClientDummy) RenameFileToFile(srcPath string, destPath string) error {
	return xerrors.Errorf("failed to rename the directory for path %s", srcPath)
}

// CreateFile creates a file
func (client *IRODSFSClientDummy) CreateFile(path string, resource string, mode string) (IRODSFSFileHandle, error) {
	return nil, xerrors.Errorf("failed to create the file for path %s", path)
}

// OpenFile opens a file
func (client *IRODSFSClientDummy) OpenFile(path string, resource string, mode string) (IRODSFSFileHandle, error) {
	if mode != string(irodsclient_types.FileOpenModeReadOnly) {
		// fail
		return nil, xerrors.Errorf("failed to open file %s with mode %s", path, mode)
	}

	if entry, ok := client.dummyEntry[path]; ok {
		if entry.Type == irodsclient_fs.FileEntry {
			// file
			if contentBuf, ok := client.dummyFileContent[path]; ok {
				return &IRODSFSClientDummyFileHandle{
					id:       xid.New().String(),
					entry:    entry,
					openMode: irodsclient_types.FileOpenModeReadOnly,
					offset:   0,
					content:  contentBuf,
				}, nil
			} else {
				return &IRODSFSClientDummyFileHandle{
					id:       xid.New().String(),
					entry:    entry,
					openMode: irodsclient_types.FileOpenModeReadOnly,
					offset:   0,
					content:  &bytes.Buffer{},
				}, nil
			}
		}
	}

	return nil, xerrors.Errorf("failed to open the file for path %s", path)
}

// TruncateFile truncates a file
func (client *IRODSFSClientDummy) TruncateFile(path string, size int64) error {
	if entry, ok := client.dummyEntry[path]; ok {
		if entry.Type == irodsclient_fs.FileEntry {
			// file
			return nil
		}
	}

	return xerrors.Errorf("failed to truncate the file for path %s", path)
}

func (client *IRODSFSClientDummy) AddCacheEventHandler(handler irodsclient_fs.FilesystemCacheEventHandler) (string, error) {
	return "", nil
}

func (client *IRODSFSClientDummy) RemoveCacheEventHandler(handlerID string) error {
	return nil
}

// IRODSFSClientDummyFileHandle implements IRODSFSFileHandle
type IRODSFSClientDummyFileHandle struct {
	id       string
	entry    *irodsclient_fs.Entry
	openMode irodsclient_types.FileOpenMode
	offset   int64
	content  *bytes.Buffer
}

func (handle *IRODSFSClientDummyFileHandle) GetID() string {
	return handle.id
}

func (handle *IRODSFSClientDummyFileHandle) GetEntry() *irodsclient_fs.Entry {
	return handle.entry
}

func (handle *IRODSFSClientDummyFileHandle) GetOpenMode() irodsclient_types.FileOpenMode {
	return handle.openMode
}

func (handle *IRODSFSClientDummyFileHandle) GetOffset() int64 {
	return handle.offset
}

func (handle *IRODSFSClientDummyFileHandle) IsReadMode() bool {
	return handle.openMode.IsRead()
}

func (handle *IRODSFSClientDummyFileHandle) IsWriteMode() bool {
	return handle.openMode.IsWrite()
}

func (handle *IRODSFSClientDummyFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	content := handle.content.Bytes()
	if int(offset) < len(content) {
		copied := copy(buffer, content[:offset])
		if int(offset)+copied == len(content) {
			return copied, io.EOF
		}

		return copied, nil
	}

	return 0, io.EOF
}

func (handle *IRODSFSClientDummyFileHandle) GetAvailable(offset int64) int64 {
	// unknown
	return -1
}

func (handle *IRODSFSClientDummyFileHandle) WriteAt(data []byte, offset int64) (int, error) {
	return 0, xerrors.Errorf("failed to write to the file %s", handle.entry.Path)
}

func (handle *IRODSFSClientDummyFileHandle) Lock(wait bool) error {
	return nil
}

func (handle *IRODSFSClientDummyFileHandle) RLock(wait bool) error {
	return nil
}

func (handle *IRODSFSClientDummyFileHandle) Unlock() error {
	return nil
}

func (handle *IRODSFSClientDummyFileHandle) Truncate(size int64) error {
	return xerrors.Errorf("failed to truncate the file %s", handle.entry.Path)
}

func (handle *IRODSFSClientDummyFileHandle) Flush() error {
	return nil
}

func (handle *IRODSFSClientDummyFileHandle) Close() error {
	return nil
}
