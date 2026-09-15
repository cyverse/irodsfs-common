package irods

import (
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	cockroach_errors "github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/irods/packedfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// emptyPackedBackend stands in for iRODS holding nothing. The dispatch tests
// below exercise mounted trees and listing rewrites, neither of which reads
// from the backend.
type emptyPackedBackend struct{}

func (b *emptyPackedBackend) Stat(irodsPath string) (*irodsclient_fs.Entry, error) {
	return nil, irodsclient_types.NewFileNotFoundError(irodsPath)
}

func (b *emptyPackedBackend) List(irodsPath string) ([]*irodsclient_fs.Entry, error) {
	return nil, irodsclient_types.NewFileNotFoundError(irodsPath)
}

func (b *emptyPackedBackend) ExistsDir(string) bool                 { return false }
func (b *emptyPackedBackend) ExistsFile(string) bool                { return false }
func (b *emptyPackedBackend) MakeDir(string, bool) error            { return nil }
func (b *emptyPackedBackend) RemoveFile(string, bool) error         { return nil }
func (b *emptyPackedBackend) RemoveDir(string, bool, bool) error    { return nil }
func (b *emptyPackedBackend) RenameFileToFile(string, string) error { return nil }

func (b *emptyPackedBackend) DownloadFileParallel(string, string, int, irodsclient_common.TransferTrackerCallback) error {
	return nil
}

func (b *emptyPackedBackend) UploadFileParallel(string, string, int, irodsclient_common.TransferTrackerCallback) error {
	return nil
}

const packedTestRoot = "/z/home/u/proj/.venv"

// newPackedTestClient builds the smallest client that can dispatch packed
// directory operations, in the style of the other tests in this package.
func newPackedTestClient(t *testing.T) *IRODSFSClientBuffered {
	t.Helper()

	config := &packedfs.Config{
		Enabled:          true,
		Names:            []string{".venv", ".git"},
		SnapshotInterval: packedfs.SnapshotDisabled,
	}
	config.ApplyDefaults()

	manager, err := packedfs.NewManager(&packedfs.ManagerConfig{
		Config:        config,
		Backend:       &emptyPackedBackend{},
		LocalRootPath: filepath.Join(t.TempDir(), "packed"),
		Owner:         "u",
	})
	require.NoError(t, err)
	t.Cleanup(func() { manager.Close() })

	return &IRODSFSClientBuffered{packed: manager, logger: newTestLogger()}
}

func TestPackedDispatchIsInertWhenDisabled(t *testing.T) {
	client := &IRODSFSClientBuffered{logger: newTestLogger()}

	assert.False(t, client.packedEnabled())
	assert.Nil(t, client.packedConfig())

	_, handled, err := client.packedList("/z/home/u/proj/.venv")
	require.NoError(t, err)
	assert.False(t, handled, "every operation falls through to the normal path")

	_, handled, err = client.packedStat("/z/home/u/proj/.venv/lib.py")
	require.NoError(t, err)
	assert.False(t, handled)

	handled, _ = client.packedExistsDir("/z/home/u/proj/.venv")
	assert.False(t, handled)

	handled, err = client.packedRemoveFile("/z/home/u/proj/.venv/lib.py", false)
	require.NoError(t, err)
	assert.False(t, handled)

	// The listing passes through untouched, archive names and all.
	entries := []*irodsclient_fs.Entry{{Name: ".venv.mount.tar", Path: "/z/home/u/proj/.venv.mount.tar"}}
	assert.Equal(t, entries, client.packedRewriteListing("/z/home/u/proj", entries))
}

func TestPackedDispatchIgnoresUnrelatedPaths(t *testing.T) {
	client := newPackedTestClient(t)

	_, handled, err := client.packedList("/z/home/u/proj/src")
	require.NoError(t, err)
	assert.False(t, handled)

	_, handled, err = client.packedStat("/z/home/u/proj/src/main.go")
	require.NoError(t, err)
	assert.False(t, handled)
}

func TestPackedRewriteListingShowsDirectoriesNotArchives(t *testing.T) {
	client := newPackedTestClient(t)

	entries := []*irodsclient_fs.Entry{
		{Name: "main.go", Path: "/z/home/u/proj/main.go", Type: irodsclient_fs.FileEntry, Size: 120},
		{Name: ".venv.mount.tar", Path: "/z/home/u/proj/.venv.mount.tar", Type: irodsclient_fs.FileEntry, Size: 900000},
		// Another archive-looking name that is not a configured directory.
		{Name: ".cargo.mount.tar", Path: "/z/home/u/proj/.cargo.mount.tar", Type: irodsclient_fs.FileEntry, Size: 42},
		// A half-uploaded archive must never be visible.
		{Name: ".git.mount.tar.uploading.abc123", Path: "/z/home/u/proj/.git.mount.tar.uploading.abc123", Type: irodsclient_fs.FileEntry},
	}

	rewritten := client.packedRewriteListing("/z/home/u/proj", entries)

	byName := map[string]*irodsclient_fs.Entry{}
	for _, entry := range rewritten {
		byName[entry.Name] = entry
	}

	require.Contains(t, byName, ".venv")
	assert.Equal(t, irodsclient_fs.DirectoryEntry, byName[".venv"].Type, "the archive is presented as the directory it holds")
	assert.Equal(t, "/z/home/u/proj/.venv", byName[".venv"].Path)
	assert.Zero(t, byName[".venv"].Size, "a directory reports no size of its own")

	assert.NotContains(t, byName, ".venv.mount.tar", "the archive itself is hidden")
	assert.Contains(t, byName, ".cargo.mount.tar", "an archive of an unconfigured name stays an ordinary file")
	assert.Contains(t, byName, "main.go")

	for name := range byName {
		assert.NotContains(t, name, ".uploading.", "a partially uploaded archive is never listed")
	}
}

func TestPackedRewriteListingIncludesDirectoryCreatedThisSession(t *testing.T) {
	client := newPackedTestClient(t)

	// Created now, so iRODS holds no archive to rewrite from.
	handled, err := client.packedMakeDir(packedTestRoot, false)
	require.True(t, handled)
	require.NoError(t, err)

	rewritten := client.packedRewriteListing("/z/home/u/proj", []*irodsclient_fs.Entry{
		{Name: "main.go", Path: "/z/home/u/proj/main.go", Type: irodsclient_fs.FileEntry},
	})

	var found *irodsclient_fs.Entry
	for _, entry := range rewritten {
		if entry.Name == ".venv" {
			found = entry
		}
	}

	require.NotNil(t, found, "a packed directory created this session is visible before its first upload")
	assert.Equal(t, irodsclient_fs.DirectoryEntry, found.Type)
}

func TestPackedDispatchServesFilesFromTheLocalTree(t *testing.T) {
	client := newPackedTestClient(t)

	handled, err := client.packedMakeDir(packedTestRoot, false)
	require.True(t, handled)
	require.NoError(t, err)

	filePath := packedTestRoot + "/lib/mod.py"
	handle, handled, err := client.packedOpen(filePath, irodsclient_types.FileOpenModeWriteTruncate, true)
	require.True(t, handled)
	require.NoError(t, err)

	written, err := handle.WriteAt([]byte("x = 1\n"), 0)
	require.NoError(t, err)
	assert.Equal(t, 6, written)
	require.NoError(t, handle.Close())

	// Stat sees it.
	entry, handled, err := client.packedStat(filePath)
	require.True(t, handled)
	require.NoError(t, err)
	assert.Equal(t, int64(6), entry.Size)
	assert.Equal(t, irodsclient_fs.FileEntry, entry.Type)

	// So do the existence checks.
	handled, exists := client.packedExistsFile(filePath)
	assert.True(t, handled)
	assert.True(t, exists)

	handled, exists = client.packedExistsDir(path.Dir(filePath))
	assert.True(t, handled)
	assert.True(t, exists)

	// The directory itself is never reported as a file.
	handled, exists = client.packedExistsFile(packedTestRoot)
	assert.True(t, handled)
	assert.False(t, exists)

	// Listing comes from the tree.
	entries, handled, err := client.packedList(path.Dir(filePath))
	require.True(t, handled)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "mod.py", entries[0].Name)

	// Reading back goes through the same local file.
	readHandle, handled, err := client.packedOpen(filePath, irodsclient_types.FileOpenModeReadOnly, false)
	require.True(t, handled)
	require.NoError(t, err)
	buffer := make([]byte, 6)
	n, err := readHandle.ReadAt(buffer, 0)
	require.NoError(t, err)
	assert.Equal(t, "x = 1\n", string(buffer[:n]))
	require.NoError(t, readHandle.Close())
}

func TestPackedLookupOfMissingDirectoryReportsNotFound(t *testing.T) {
	client := newPackedTestClient(t)

	// iRODS holds nothing here, and a lookup must not create it.
	_, handled, err := client.packedStat(packedTestRoot + "/lib.py")
	require.True(t, handled, "the path is inside a packed directory, so it does not fall through")
	require.Error(t, err)
	assert.True(t, irodsclient_types.IsFileNotFoundError(err), "callers map this to ENOENT, got %v", err)

	handled, exists := client.packedExistsDir(packedTestRoot)
	assert.True(t, handled)
	assert.False(t, exists)
}

func TestPackedRenameAcrossTheBoundaryReportsCrossMount(t *testing.T) {
	client := newPackedTestClient(t)

	handled, err := client.packedMakeDir(packedTestRoot, false)
	require.True(t, handled)
	require.NoError(t, err)

	handle, _, err := client.packedOpen(packedTestRoot+"/a.txt", irodsclient_types.FileOpenModeWriteTruncate, true)
	require.NoError(t, err)
	require.NoError(t, handle.Close())

	// Inside the mount the rename is local.
	handled, err = client.packedRenameWithin(packedTestRoot+"/a.txt", packedTestRoot+"/b.txt")
	require.True(t, handled)
	require.NoError(t, err)

	// Crossing out of it has no local equivalent.
	handled, err = client.packedRenameWithin(packedTestRoot+"/b.txt", "/z/home/u/proj/b.txt")
	require.True(t, handled)
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, packedfs.ErrCrossMountRename), "got %v", err)

	// And so does moving into one from outside.
	handled, err = client.packedRenameWithin("/z/home/u/proj/c.txt", packedTestRoot+"/c.txt")
	require.True(t, handled)
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, packedfs.ErrCrossMountRename), "got %v", err)
}

func TestPackedRemoveDirDropsTheWholeDirectory(t *testing.T) {
	client := newPackedTestClient(t)

	handled, err := client.packedMakeDir(packedTestRoot, false)
	require.True(t, handled)
	require.NoError(t, err)

	handle, _, err := client.packedOpen(packedTestRoot+"/a.txt", irodsclient_types.FileOpenModeWriteTruncate, true)
	require.NoError(t, err)
	require.NoError(t, handle.Close())

	localPath := client.packed.Mounts()[0].LocalPath

	handled, err = client.packedRemoveDir(packedTestRoot, true, true)
	require.True(t, handled)
	require.NoError(t, err)

	assert.Empty(t, client.packed.Mounts(), "the mount is gone")
	_, statErr := os.Stat(localPath)
	assert.True(t, os.IsNotExist(statErr), "the local tree is gone")
}

// StagingFS removes its whole root once its own uploads have synced. A packed
// tree inside that root would be deleted along with it even when its archive
// had failed to upload, taking the only copy of the data, so the two roots must
// be siblings.
func TestPackedRootIsNotInsideTheStagingRoot(t *testing.T) {
	const stagingRoot = "/irodsfs_pool/staging/session-1234"

	packedRoot := packedRootPathFor(stagingRoot)

	assert.Equal(t, "/irodsfs_pool/staging/session-1234-packed", packedRoot)
	assert.False(t, strings.HasPrefix(packedRoot, stagingRoot+string(filepath.Separator)),
		"%q must not sit inside the staging root that StagingFS deletes", packedRoot)
	// A trailing separator must not change where the tree lands.
	assert.Equal(t, packedRoot, packedRootPathFor(stagingRoot+"/"))
}
