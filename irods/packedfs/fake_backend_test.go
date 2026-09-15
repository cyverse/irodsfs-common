package packedfs

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/stretchr/testify/require"
)

// fakeBackend simulates an iRODS namespace with a local directory: collections
// are directories and data objects are files. That keeps the tests honest about
// the operations the manager actually issues without needing a server.
type fakeBackend struct {
	root string

	mu sync.Mutex
	// uploadErr fails the next upload, to exercise the recovery paths.
	uploadErr error
	// renameErr fails the next rename into place.
	renameErr error
	// deleteMissingErr is returned when something that is not there is deleted.
	// CyVerse's zone answers such a request with a rule outcome
	// (CUT_ACTION_PROCESSED_ERR) rather than a file-not-found, which is what
	// aborted the very first upload of every packed directory.
	deleteMissingErr error
	uploads          int
	downloads        int
}

func newFakeBackend(t *testing.T) *fakeBackend {
	t.Helper()
	return &fakeBackend{root: t.TempDir()}
}

func (b *fakeBackend) localOf(irodsPath string) string {
	return filepath.Join(b.root, filepath.FromSlash(strings.TrimPrefix(path.Clean(irodsPath), "/")))
}

// seedFile places a data object in the simulated namespace.
func (b *fakeBackend) seedFile(t *testing.T, irodsPath string, content string) {
	t.Helper()
	localPath := b.localOf(irodsPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(localPath), 0755))
	require.NoError(t, os.WriteFile(localPath, []byte(content), 0644))
}

// seedArchive packs sourceDir and places it as the archive data object at
// irodsPath, standing in for an archive an earlier session uploaded.
func (b *fakeBackend) seedArchive(t *testing.T, irodsPath string, sourceDir string, compression Compression) {
	t.Helper()
	localPath := b.localOf(irodsPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(localPath), 0755))

	file, err := os.Create(localPath)
	require.NoError(t, err)
	_, err = Pack(sourceDir, file, compression)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func (b *fakeBackend) seedDir(t *testing.T, irodsPath string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(b.localOf(irodsPath), 0755))
}

func (b *fakeBackend) exists(irodsPath string) bool {
	_, err := os.Stat(b.localOf(irodsPath))
	return err == nil
}

// names lists the base names of everything directly inside a collection.
func (b *fakeBackend) names(t *testing.T, irodsPath string) []string {
	t.Helper()
	entries, err := os.ReadDir(b.localOf(irodsPath))
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func (b *fakeBackend) Stat(irodsPath string) (*irodsclient_fs.Entry, error) {
	info, err := os.Stat(b.localOf(irodsPath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, irodsclient_types.NewFileNotFoundError(irodsPath)
		}
		return nil, err
	}

	entryType := irodsclient_fs.FileEntry
	if info.IsDir() {
		entryType = irodsclient_fs.DirectoryEntry
	}

	return &irodsclient_fs.Entry{
		Type:       entryType,
		Name:       path.Base(irodsPath),
		Path:       irodsPath,
		Size:       info.Size(),
		ModifyTime: info.ModTime(),
	}, nil
}

func (b *fakeBackend) List(irodsPath string) ([]*irodsclient_fs.Entry, error) {
	dirEntries, err := os.ReadDir(b.localOf(irodsPath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, irodsclient_types.NewFileNotFoundError(irodsPath)
		}
		return nil, err
	}

	entries := make([]*irodsclient_fs.Entry, 0, len(dirEntries))
	for _, dirEntry := range dirEntries {
		entry, err := b.Stat(path.Join(irodsPath, dirEntry.Name()))
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (b *fakeBackend) ExistsDir(irodsPath string) bool {
	info, err := os.Stat(b.localOf(irodsPath))
	return err == nil && info.IsDir()
}

func (b *fakeBackend) MakeDir(irodsPath string, recurse bool) error {
	return os.MkdirAll(b.localOf(irodsPath), 0755)
}

func (b *fakeBackend) ExistsFile(irodsPath string) bool {
	info, err := os.Stat(b.localOf(irodsPath))
	return err == nil && !info.IsDir()
}

func (b *fakeBackend) RemoveFile(irodsPath string, force bool) error {
	err := os.Remove(b.localOf(irodsPath))
	if err != nil && os.IsNotExist(err) {
		b.mu.Lock()
		zoneErr := b.deleteMissingErr
		b.mu.Unlock()
		if zoneErr != nil {
			return zoneErr
		}
		return irodsclient_types.NewFileNotFoundError(irodsPath)
	}
	return err
}

func (b *fakeBackend) RemoveDir(irodsPath string, recurse bool, force bool) error {
	if recurse {
		return os.RemoveAll(b.localOf(irodsPath))
	}
	return os.Remove(b.localOf(irodsPath))
}

func (b *fakeBackend) RenameFileToFile(srcPath string, destPath string) error {
	b.mu.Lock()
	if b.renameErr != nil {
		err := b.renameErr
		b.renameErr = nil
		b.mu.Unlock()
		return err
	}
	b.mu.Unlock()

	// iRODS refuses to rename onto an existing data object, and the manager is
	// written to remove the destination first. Enforce it so a regression that
	// drops that step fails here.
	if _, err := os.Stat(b.localOf(destPath)); err == nil {
		return errors.Newf("destination %q already exists", destPath)
	}

	return os.Rename(b.localOf(srcPath), b.localOf(destPath))
}

func (b *fakeBackend) DownloadFileParallel(irodsPath string, localPath string, taskNum int, callback irodsclient_common.TransferTrackerCallback) error {
	b.mu.Lock()
	b.downloads++
	b.mu.Unlock()

	return copyLocal(b.localOf(irodsPath), localPath)
}

func (b *fakeBackend) UploadFileParallel(localPath string, irodsPath string, taskNum int, callback irodsclient_common.TransferTrackerCallback) error {
	b.mu.Lock()
	b.uploads++
	if b.uploadErr != nil {
		err := b.uploadErr
		b.uploadErr = nil
		b.mu.Unlock()
		return err
	}
	b.mu.Unlock()

	target := b.localOf(irodsPath)
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return err
	}
	return copyLocal(localPath, target)
}

func copyLocal(src string, dst string) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	target, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer target.Close()

	_, err = io.Copy(target, source)
	return err
}

// fakeQuota accounts for staging bytes with a hard cap, mirroring how StagingFS
// refuses a reservation it cannot satisfy.
type fakeQuota struct {
	mu    sync.Mutex
	max   int64
	used  int64
	peak  int64
	fails int
}

func (q *fakeQuota) ReserveSpace(size int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.max > 0 && q.used+size > q.max {
		q.fails++
		return errors.Newf("staging quota exceeded: used %d + requested %d > max %d", q.used, size, q.max)
	}

	q.used += size
	if q.used > q.peak {
		q.peak = q.used
	}
	return nil
}

func (q *fakeQuota) ReleaseSpace(size int64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.used -= size
	if q.used < 0 {
		q.used = 0
	}
}

func (q *fakeQuota) current() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used
}
