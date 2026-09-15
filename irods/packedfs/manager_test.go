package packedfs

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testRoot = "/z/home/u/proj/.venv"

type testHarness struct {
	manager *Manager
	backend *fakeBackend
	quota   *fakeQuota
	config  *Config
}

func newHarness(t *testing.T, tune func(*Config, *fakeQuota)) *testHarness {
	t.Helper()

	backend := newFakeBackend(t)
	quota := &fakeQuota{}
	config := &Config{
		Enabled: true,
		Names:   []string{".venv", ".git"},
		// Snapshots are driven explicitly in tests; a timer would make them flaky.
		SnapshotInterval: SnapshotDisabled,
	}
	if tune != nil {
		tune(config, quota)
	}
	config.ApplyDefaults()

	manager, err := NewManager(&ManagerConfig{
		Config:        config,
		Backend:       backend,
		Quota:         quota,
		LocalRootPath: filepath.Join(t.TempDir(), "packed"),
		Owner:         "u",
	})
	require.NoError(t, err)
	require.NotNil(t, manager)

	t.Cleanup(func() { manager.Close() })

	return &testHarness{manager: manager, backend: backend, quota: quota, config: config}
}

func (h *testHarness) writeInMount(t *testing.T, mount *Mount, irodsPath string, content string) {
	t.Helper()
	file, _, err := h.manager.CreateFile(mount, irodsPath, irodsclient_types.FileOpenModeWriteTruncate)
	require.NoError(t, err)
	_, err = file.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func TestNewManagerDisabledReturnsNil(t *testing.T) {
	manager, err := NewManager(&ManagerConfig{Config: &Config{Enabled: false}})
	require.NoError(t, err)
	assert.Nil(t, manager, "a nil manager lets callers treat the feature as off")

	// The nil manager must stay safe to call.
	mount, ok := manager.Lookup("/z/home/u/.venv/lib")
	assert.Nil(t, mount)
	assert.False(t, ok)
	assert.NoError(t, manager.Close())
	assert.Empty(t, manager.Statuses())
}

func TestMountFreshDirectoryCreatesArchiveOnUnmount(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	assert.Equal(t, MountStateMounted, mount.State())

	// Nothing exists in iRODS yet, so the empty tree still has to be uploaded.
	assert.True(t, mount.IsDirty())

	h.writeInMount(t, mount, testRoot+"/pyvenv.cfg", "home = /usr/bin\n")
	h.writeInMount(t, mount, testRoot+"/lib/pkg/__init__.py", "x = 1\n")

	require.NoError(t, h.manager.Unmount(mount))

	assert.True(t, h.backend.exists(testRoot+".mount.tar"), "the archive is in place")
	assert.False(t, h.backend.exists(testRoot), "no collection is created for a packed directory")

	// The local tree and its quota charge are gone.
	_, statErr := os.Stat(mount.LocalPath)
	assert.True(t, os.IsNotExist(statErr))
	assert.Zero(t, h.quota.current())
}

func TestRemountRestoresContents(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/bin/activate", "export VIRTUAL_ENV\n")
	h.writeInMount(t, mount, testRoot+"/lib/pkg/mod.py", "y = 2\n")
	require.NoError(t, h.manager.Unmount(mount))

	// A fresh manager stands in for the next session.
	next, err := NewManager(&ManagerConfig{
		Config:        h.config,
		Backend:       h.backend,
		Quota:         h.quota,
		LocalRootPath: filepath.Join(t.TempDir(), "packed2"),
		Owner:         "u",
	})
	require.NoError(t, err)
	defer next.Close()

	remounted, err := next.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	assert.False(t, remounted.IsDirty(), "a tree restored from its archive matches iRODS")

	entry, err := next.Stat(remounted, testRoot+"/lib/pkg/mod.py")
	require.NoError(t, err)
	assert.Equal(t, int64(6), entry.Size)

	localPath, err := next.LocalPath(remounted, testRoot+"/bin/activate")
	require.NoError(t, err)
	content, err := os.ReadFile(localPath)
	require.NoError(t, err)
	assert.Equal(t, "export VIRTUAL_ENV\n", string(content))
}

func TestMountMigratesLegacyCollection(t *testing.T) {
	h := newHarness(t, nil)

	// A directory written before packing was enabled, or recreated by crash
	// recovery uploading staged files one at a time.
	h.backend.seedDir(t, testRoot)
	h.backend.seedFile(t, testRoot+"/pyvenv.cfg", "home = /usr/bin\n")
	h.backend.seedFile(t, testRoot+"/lib/pkg/__init__.py", "legacy\n")

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	assert.True(t, mount.IsDirty(), "a migrated collection has no archive yet")

	entries, err := h.manager.List(mount, testRoot)
	require.NoError(t, err)
	require.Len(t, entries, 2)

	require.NoError(t, h.manager.Unmount(mount))

	assert.True(t, h.backend.exists(testRoot+".mount.tar"), "the archive replaces the collection")
	assert.False(t, h.backend.exists(testRoot), "the migrated collection is removed")
}

func TestArchiveTakesPrecedenceOverStaleCollection(t *testing.T) {
	h := newHarness(t, nil)

	// Both representations exist at once. This is what a crash leaves behind:
	// the session's archive is in place, and staging recovery then replayed the
	// deferred per-file uploads, recreating the directory as a collection.
	source := t.TempDir()
	writeFile(t, filepath.Join(source, "from_archive.txt"), "authoritative\n", 0644)
	h.backend.seedArchive(t, testRoot+".mount.tar", source, CompressionNone)

	h.backend.seedDir(t, testRoot)
	h.backend.seedFile(t, testRoot+"/from_collection.txt", "stale\n")

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)

	assert.True(t, h.manager.ExistsFile(mount, testRoot+"/from_archive.txt"),
		"the archive is the authoritative representation")
	assert.False(t, h.manager.ExistsFile(mount, testRoot+"/from_collection.txt"),
		"the stale collection must not merge into the mounted tree")
	assert.False(t, mount.IsDirty(), "a tree restored from its archive is clean")
}

func TestMigratedCollectionContentReachesTheArchive(t *testing.T) {
	h := newHarness(t, nil)

	h.backend.seedDir(t, testRoot)
	h.backend.seedFile(t, testRoot+"/legacy.txt", "from the collection\n")

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/fresh.txt", "written this session\n")
	require.NoError(t, h.manager.Unmount(mount))

	remounted, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)

	assert.True(t, h.manager.ExistsFile(remounted, testRoot+"/legacy.txt"),
		"migration carries the collection's content into the archive")
	assert.True(t, h.manager.ExistsFile(remounted, testRoot+"/fresh.txt"))
}

func TestSnapshotUploadsWithoutUnmounting(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/first.txt", "one\n")

	require.NoError(t, h.manager.Pack(mount))

	assert.True(t, h.backend.exists(testRoot+".mount.tar"))
	assert.Equal(t, MountStateMounted, mount.State(), "the tree stays mounted after a snapshot")
	assert.False(t, mount.IsDirty())

	// The tree is still usable and still local.
	assert.True(t, h.manager.ExistsFile(mount, testRoot+"/first.txt"))

	h.writeInMount(t, mount, testRoot+"/second.txt", "two\n")
	assert.True(t, mount.IsDirty(), "a write after the snapshot dirties the tree again")

	require.NoError(t, h.manager.Pack(mount))

	// The second snapshot replaced the first archive rather than accumulating.
	names := h.backend.names(t, "/z/home/u/proj")
	assert.Equal(t, []string{".venv.mount.tar"}, names, "no temporary objects are left behind")
}

func TestPackKeepsTreeDirtyWhenAWriteRacesIt(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/a.txt", "one\n")
	require.NoError(t, h.manager.Pack(mount))
	require.False(t, mount.IsDirty())

	// Simulate a write landing while an archive is being built: the change
	// counter moves, so the pack that started earlier must not mark it clean.
	mark := mount.dirtyMark()
	mount.MarkDirty()
	mount.markPacked(mark, time.Now())

	assert.True(t, mount.IsDirty(), "a change during packing survives into the next snapshot")
}

func TestUnmountRefusesToLoseDataWhenUploadFails(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/important.txt", "keep me\n")

	h.backend.mu.Lock()
	h.backend.uploadErr = errors.New("network is down")
	h.backend.mu.Unlock()

	err = h.manager.Unmount(mount)
	require.Error(t, err)

	// The tree must survive a failed upload, otherwise the data is gone.
	assert.True(t, h.manager.ExistsFile(mount, testRoot+"/important.txt"))
	assert.Equal(t, MountStateMounted, mount.State())
	assert.False(t, h.backend.exists(testRoot+".mount.tar"))

	// No half-uploaded object is left in the collection.
	for _, name := range h.backend.names(t, "/z/home/u/proj") {
		assert.False(t, IsTransientArchiveName(name), "leftover temporary object %q", name)
	}

	// A retry succeeds and the data reaches iRODS.
	require.NoError(t, h.manager.Unmount(mount))
	assert.True(t, h.backend.exists(testRoot+".mount.tar"))
}

func TestPreviousArchiveSurvivesAFailedUpload(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/v1.txt", "first version\n")
	require.NoError(t, h.manager.Pack(mount))

	h.writeInMount(t, mount, testRoot+"/v2.txt", "second version\n")
	h.backend.mu.Lock()
	h.backend.uploadErr = errors.New("transfer aborted")
	h.backend.mu.Unlock()

	require.Error(t, h.manager.Pack(mount))

	// Uploading under a temporary name is what keeps the good archive intact.
	require.True(t, h.backend.exists(testRoot+".mount.tar"))

	next, err := NewManager(&ManagerConfig{
		Config:        h.config,
		Backend:       h.backend,
		Quota:         &fakeQuota{},
		LocalRootPath: filepath.Join(t.TempDir(), "packed3"),
		Owner:         "u",
	})
	require.NoError(t, err)
	defer next.Close()

	recovered, err := next.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	assert.True(t, next.ExistsFile(recovered, testRoot+"/v1.txt"), "the previous archive is still readable")
}

func TestMountRefusedWhenStagingQuotaIsFull(t *testing.T) {
	h := newHarness(t, func(config *Config, quota *fakeQuota) {
		quota.max = 64
	})

	h.backend.seedFile(t, testRoot+".mount.tar", string(make([]byte, 4096)))

	_, err := h.manager.EnsureMounted(testRoot, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "staging quota exceeded")

	// A refused mount leaves nothing charged and nothing extracted.
	assert.Zero(t, h.quota.current())
}

func TestMountRefusedWhenArchiveExceedsSizeLimit(t *testing.T) {
	h := newHarness(t, func(config *Config, quota *fakeQuota) {
		config.MaxPackedDirSize = 1024
	})

	h.backend.seedFile(t, testRoot+".mount.tar", string(make([]byte, 4096)))

	_, err := h.manager.EnsureMounted(testRoot, true)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrArchiveTooLarge), "got %v", err)
	assert.Zero(t, h.quota.current())
}

func TestQuotaChargeTracksTreeSize(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	assert.Zero(t, h.quota.current(), "an empty fresh tree charges nothing")

	h.writeInMount(t, mount, testRoot+"/data.bin", string(make([]byte, 2048)))

	// Packing reconciles the charge against what the tree now occupies.
	require.NoError(t, h.manager.Pack(mount))
	assert.Equal(t, int64(2048), h.quota.current())
	assert.Equal(t, int64(2048), mount.ReservedSize())

	require.NoError(t, h.manager.Unmount(mount))
	assert.Zero(t, h.quota.current())
}

func TestLookupDoesNotMount(t *testing.T) {
	h := newHarness(t, nil)

	mount, ok := h.manager.Lookup(testRoot + "/lib/pkg/mod.py")
	assert.Nil(t, mount)
	assert.False(t, ok, "Lookup never triggers the expensive mount")

	_, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)

	mount, ok = h.manager.Lookup(testRoot + "/lib/pkg/mod.py")
	assert.True(t, ok)
	require.NotNil(t, mount)
	assert.Equal(t, testRoot, mount.Root)
}

func TestResolveIgnoresUnrelatedPaths(t *testing.T) {
	h := newHarness(t, nil)

	mount, matched, err := h.manager.Resolve("/z/home/u/proj/src/main.go", false)
	require.NoError(t, err)
	assert.False(t, matched)
	assert.Nil(t, mount)
}

func TestConcurrentFirstAccessMountsOnce(t *testing.T) {
	h := newHarness(t, nil)

	source := t.TempDir()
	writeFile(t, filepath.Join(source, "pyvenv.cfg"), "home = /usr/bin\n", 0644)
	h.backend.seedArchive(t, testRoot+".mount.tar", source, CompressionNone)

	const callers = 8
	mounts := make(chan *Mount, callers)
	errs := make(chan error, callers)
	start := make(chan struct{})

	for i := 0; i < callers; i++ {
		go func() {
			<-start
			mount, err := h.manager.EnsureMounted(testRoot, true)
			if err != nil {
				errs <- err
				return
			}
			mounts <- mount
		}()
	}
	close(start)

	var first *Mount
	for i := 0; i < callers; i++ {
		select {
		case err := <-errs:
			t.Fatalf("concurrent mount failed: %v", err)
		case mount := <-mounts:
			if first == nil {
				first = mount
				continue
			}
			assert.Same(t, first, mount, "every caller gets the same mount")
		}
	}

	h.backend.mu.Lock()
	downloads := h.backend.downloads
	h.backend.mu.Unlock()
	assert.Equal(t, 1, downloads, "the archive is downloaded once, not once per caller")
}

func TestRenameAcrossMountBoundaryIsRefused(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/inside.txt", "content\n")

	// Within the mount the rename is a local one.
	require.NoError(t, h.manager.Rename(mount, testRoot+"/inside.txt", testRoot+"/moved.txt"))
	assert.True(t, h.manager.ExistsFile(mount, testRoot+"/moved.txt"))

	// Leaving the mount has no local equivalent, so the caller is told to copy.
	err = h.manager.Rename(mount, testRoot+"/moved.txt", "/z/home/u/proj/outside.txt")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrCrossMountRename), "got %v", err)
}

func TestStatReportsFileNotFoundForMissingPaths(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)

	_, err = h.manager.Stat(mount, testRoot+"/nope.txt")
	require.Error(t, err)
	assert.True(t, irodsclient_types.IsFileNotFoundError(err),
		"the rest of the stack maps this to ENOENT, got %v", err)
}

func TestMountClearsAStaleLocalTree(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/kept.txt", "kept\n")
	require.NoError(t, h.manager.Unmount(mount))

	// Leave debris where a crashed session's tree would be.
	stalePath := h.manager.localPathFor(testRoot)
	require.NoError(t, os.MkdirAll(stalePath, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(stalePath, "deleted.txt"), []byte("resurrected"), 0644))

	remounted, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)

	assert.True(t, h.manager.ExistsFile(remounted, testRoot+"/kept.txt"))
	assert.False(t, h.manager.ExistsFile(remounted, testRoot+"/deleted.txt"),
		"a stale tree must not resurrect files the archive does not have")
}

func TestCloseFlushesEveryMount(t *testing.T) {
	h := newHarness(t, nil)

	venv, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, venv, testRoot+"/a.txt", "a\n")

	gitRoot := "/z/home/u/proj/.git"
	git, err := h.manager.EnsureMounted(gitRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, git, gitRoot+"/HEAD", "ref: refs/heads/main\n")

	require.NoError(t, h.manager.Close())

	assert.True(t, h.backend.exists(testRoot+".mount.tar"))
	assert.True(t, h.backend.exists(gitRoot+".mount.tar"))
	assert.Zero(t, h.quota.current())
	assert.Empty(t, h.manager.Statuses())
}

func TestLookupOfMissingDirectoryDoesNotCreateIt(t *testing.T) {
	h := newHarness(t, nil)

	// iRODS holds neither an archive nor a collection. A read must report that
	// rather than conjuring an empty directory into existence.
	_, err := h.manager.EnsureMounted(testRoot, false)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNoPackedDirectory), "got %v", err)

	assert.Empty(t, h.manager.Statuses(), "a failed lookup registers no mount")
	assert.Zero(t, h.quota.current())

	// Nothing was written to iRODS either.
	assert.False(t, h.backend.exists(testRoot))
	assert.False(t, h.backend.exists(testRoot+".mount.tar"))

	// The same path with create intent does bring it into existence.
	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	assert.Equal(t, MountStateMounted, mount.State())
}

func TestRemoveDeletesBothRepresentations(t *testing.T) {
	h := newHarness(t, nil)

	// A crash can leave an archive and a recovered collection side by side.
	source := t.TempDir()
	writeFile(t, filepath.Join(source, "lib.py"), "content\n", 0644)
	h.backend.seedArchive(t, testRoot+".mount.tar", source, CompressionNone)
	h.backend.seedDir(t, testRoot)
	h.backend.seedFile(t, testRoot+"/leftover.py", "recovered\n")

	mount, err := h.manager.EnsureMounted(testRoot, false)
	require.NoError(t, err)

	require.NoError(t, h.manager.Remove(mount))

	assert.False(t, h.backend.exists(testRoot+".mount.tar"), "the archive is gone")
	assert.False(t, h.backend.exists(testRoot), "the stale collection is gone too")
	assert.Zero(t, h.quota.current())
	assert.Empty(t, h.manager.Statuses())

	// The local tree is gone, so nothing can be read back.
	_, statErr := os.Stat(mount.LocalPath)
	assert.True(t, os.IsNotExist(statErr))
}

func TestRemoveDoesNotUploadTheTreeItIsDeleting(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/doomed.txt", "about to be deleted\n")

	h.backend.mu.Lock()
	uploadsBefore := h.backend.uploads
	h.backend.mu.Unlock()

	require.NoError(t, h.manager.Remove(mount))

	h.backend.mu.Lock()
	uploadsAfter := h.backend.uploads
	h.backend.mu.Unlock()

	assert.Equal(t, uploadsBefore, uploadsAfter, "a directory being removed is never packed first")
}

func TestRenameRootMovesTheArchive(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/moved.txt", "carried across\n")

	const destRoot = "/z/home/u/other/.venv"
	// The destination collection has to exist, exactly as it would for any
	// rename a FUSE client issues.
	h.backend.seedDir(t, "/z/home/u/other")

	require.NoError(t, h.manager.RenameRoot(mount, destRoot))

	assert.False(t, h.backend.exists(testRoot+".mount.tar"))
	assert.True(t, h.backend.exists(destRoot+".mount.tar"))
	assert.Zero(t, h.quota.current())

	// The content follows the rename.
	remounted, err := h.manager.EnsureMounted(destRoot, false)
	require.NoError(t, err)
	assert.True(t, h.manager.ExistsFile(remounted, destRoot+"/moved.txt"))
}

func TestRenameRootToUnpackedNameIsRefused(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/a.txt", "a\n")

	// ".venv-backup" is not configured as packed, so it would have to become a
	// real collection; that is the caller's copy-and-unlink fallback.
	err = h.manager.RenameRoot(mount, "/z/home/u/proj/.venv-backup")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrCrossMountRename), "got %v", err)

	assert.True(t, h.manager.ExistsFile(mount, testRoot+"/a.txt"), "the tree is untouched")
}

func TestManagerWorksWithoutQuotaAccounting(t *testing.T) {
	backend := newFakeBackend(t)
	config := &Config{Enabled: true, Names: []string{".venv"}, SnapshotInterval: SnapshotDisabled}
	config.ApplyDefaults()

	// A nil Quota disables accounting rather than panicking, which is what a
	// client configured without staging hands over.
	manager, err := NewManager(&ManagerConfig{
		Config:        config,
		Backend:       backend,
		Quota:         nil,
		LocalRootPath: filepath.Join(t.TempDir(), "packed"),
		Owner:         "u",
	})
	require.NoError(t, err)
	defer manager.Close()

	mount, err := manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)

	file, _, err := manager.CreateFile(mount, testRoot+"/a.txt", irodsclient_types.FileOpenModeWriteTruncate)
	require.NoError(t, err)
	_, err = file.WriteString("content\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	require.NoError(t, manager.ReserveGrowth(mount, 4096))
	manager.ReleaseGrowth(mount, 4096)

	require.NoError(t, manager.Unmount(mount))
	assert.True(t, backend.exists(testRoot+".mount.tar"))
}

// git appends to its reflogs, so it opens .git/logs/HEAD with O_APPEND and then
// writes at an absolute offset. Passing O_APPEND through to the local file made
// Go refuse every such write, which surfaced as "Remote I/O error" and broke
// git clone into a packed .git.
func TestAppendModeHandlesWriteAt(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)

	logPath := testRoot + "/logs/HEAD"
	file, _, err := h.manager.CreateFile(mount, logPath, irodsclient_types.FileOpenModeAppend)
	require.NoError(t, err)

	first := []byte("0000 1111 checkout\n")
	n, err := file.WriteAt(first, 0)
	require.NoError(t, err, "a write to an append-mode handle must succeed")
	assert.Equal(t, len(first), n)

	second := []byte("1111 2222 commit\n")
	n, err = file.WriteAt(second, int64(len(first)))
	require.NoError(t, err)
	assert.Equal(t, len(second), n)
	require.NoError(t, file.Close())

	// Reopening for append and writing at the end works the same way.
	reopened, _, err := h.manager.OpenFile(mount, logPath, irodsclient_types.FileOpenModeAppend)
	require.NoError(t, err)
	third := []byte("2222 3333 merge\n")
	_, err = reopened.WriteAt(third, int64(len(first)+len(second)))
	require.NoError(t, err)
	require.NoError(t, reopened.Close())

	localPath, err := h.manager.LocalPath(mount, logPath)
	require.NoError(t, err)
	content, err := os.ReadFile(localPath)
	require.NoError(t, err)
	assert.Equal(t, string(first)+string(second)+string(third), string(content))
}

func TestOpenForWriteCanReadBack(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)

	filePath := testRoot + "/index"
	file, _, err := h.manager.CreateFile(mount, filePath, irodsclient_types.FileOpenModeWriteOnly)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte("payload"), 0)
	require.NoError(t, err)

	// A write-only request still gets a read-write descriptor, because callers
	// read back through the handle they created a file with.
	buffer := make([]byte, 7)
	_, err = file.ReadAt(buffer, 0)
	require.NoError(t, err, "a write-mode handle must still be readable")
	assert.Equal(t, "payload", string(buffer))
	require.NoError(t, file.Close())
}

// Truncation arrives as its own operation, so opening must never discard data.
func TestOpenForWriteDoesNotTruncate(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)

	filePath := testRoot + "/config"
	h.writeInMount(t, mount, filePath, "[core]\n")

	for _, mode := range []irodsclient_types.FileOpenMode{
		irodsclient_types.FileOpenModeWriteOnly,
		irodsclient_types.FileOpenModeWriteTruncate,
		irodsclient_types.FileOpenModeAppend,
		irodsclient_types.FileOpenModeReadWrite,
	} {
		t.Run(string(mode), func(t *testing.T) {
			file, entry, err := h.manager.OpenFile(mount, filePath, mode)
			require.NoError(t, err)
			assert.Equal(t, int64(7), entry.Size, "opening in mode %q must not discard content", mode)
			require.NoError(t, file.Close())

			localPath, err := h.manager.LocalPath(mount, filePath)
			require.NoError(t, err)
			content, err := os.ReadFile(localPath)
			require.NoError(t, err)
			assert.Equal(t, "[core]\n", string(content))
		})
	}
}

// An unrecognized local error reaches the FUSE client as a generic failure and
// shows up as "Remote I/O error", so the conditions callers act on must map to
// the iRODS error types the stack understands.
func TestLocalErrorsMapToRecognizedIRODSErrors(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)

	h.writeInMount(t, mount, testRoot+"/lib/mod.py", "x = 1\n")

	t.Run("missing path is not found", func(t *testing.T) {
		err := h.manager.RemoveFile(mount, testRoot+"/absent.py", false)
		require.Error(t, err)
		assert.True(t, irodsclient_types.IsFileNotFoundError(err), "got %v", err)
	})

	t.Run("non-empty directory", func(t *testing.T) {
		err := h.manager.RemoveDir(mount, testRoot+"/lib", false, false)
		require.Error(t, err)
		assert.True(t, irodsclient_types.IsCollectionNotEmptyError(err),
			"a non-recursive rmdir of a populated directory must be ENOTEMPTY, got %v", err)
	})

	t.Run("already existing directory", func(t *testing.T) {
		err := h.manager.MakeDir(mount, testRoot+"/lib", false)
		require.Error(t, err)
		assert.True(t, irodsclient_types.IsFileAlreadyExistError(err), "got %v", err)
	})
}

// gitCloneWorkload exercises the file patterns a git clone drives through a
// packed .git: deep directory creation, many small objects, the write-then-
// rename lock pattern, appends to reflogs, and reopening files for update.
func TestGitCloneWorkload(t *testing.T) {
	h := newHarness(t, nil)

	const gitRoot = "/z/home/u/proj/.git"
	mount, err := h.manager.EnsureMounted(gitRoot, true)
	require.NoError(t, err)

	// git init: the directory skeleton.
	for _, dir := range []string{
		"/objects/pack", "/objects/info", "/refs/heads", "/refs/tags",
		"/logs/refs/remotes/origin", "/hooks", "/info",
	} {
		require.NoError(t, h.manager.MakeDir(mount, gitRoot+dir, true), "mkdir %s", dir)
	}

	// Receiving objects: many small loose objects across fanout directories.
	for i := 0; i < 256; i++ {
		objectDir := fmt.Sprintf("%s/objects/%02x", gitRoot, i)
		require.NoError(t, h.manager.MakeDir(mount, objectDir, true))
		h.writeInMount(t, mount, fmt.Sprintf("%s/%040x", objectDir, i), fmt.Sprintf("object %d\n", i))
	}

	// The pack file is written under a temporary name and renamed into place,
	// which is how git makes its writes atomic.
	tempPack := gitRoot + "/objects/pack/tmp_pack_abc123"
	h.writeInMount(t, mount, tempPack, "PACK...binary...")
	require.NoError(t, h.manager.Rename(mount, tempPack, gitRoot+"/objects/pack/pack-deadbeef.pack"))
	assert.False(t, h.manager.ExistsFile(mount, tempPack))
	assert.True(t, h.manager.ExistsFile(mount, gitRoot+"/objects/pack/pack-deadbeef.pack"))

	// The lock-file pattern: create .lock, write, rename over the target.
	h.writeInMount(t, mount, gitRoot+"/config", "[core]\n\trepositoryformatversion = 0\n")
	h.writeInMount(t, mount, gitRoot+"/config.lock", "[core]\n\tbare = false\n")
	require.NoError(t, h.manager.Rename(mount, gitRoot+"/config.lock", gitRoot+"/config"))

	configPath, err := h.manager.LocalPath(mount, gitRoot+"/config")
	require.NoError(t, err)
	configContent, err := os.ReadFile(configPath)
	require.NoError(t, err)
	assert.Equal(t, "[core]\n\tbare = false\n", string(configContent), "the rename replaced the target")

	// update_ref: append to the reflogs. This is where the clone failed.
	for _, reflog := range []string{"/logs/HEAD", "/logs/refs/remotes/origin/HEAD"} {
		file, _, err := h.manager.CreateFile(mount, gitRoot+reflog, irodsclient_types.FileOpenModeAppend)
		require.NoError(t, err, "create %s", reflog)

		var offset int64
		for _, line := range []string{
			"0000000 1111111 clone: from https://example.invalid/repo.git\n",
			"1111111 2222222 checkout: moving from main to main\n",
		} {
			n, err := file.WriteAt([]byte(line), offset)
			require.NoError(t, err, "append to %s", reflog)
			offset += int64(n)
		}
		require.NoError(t, file.Close())
	}

	// Writing HEAD and the branch ref.
	h.writeInMount(t, mount, gitRoot+"/HEAD", "ref: refs/heads/main\n")
	h.writeInMount(t, mount, gitRoot+"/refs/heads/main", "2222222\n")

	// The whole repository survives a snapshot and a remount.
	require.NoError(t, h.manager.Pack(mount))
	require.NoError(t, h.manager.Unmount(mount))

	remounted, err := h.manager.EnsureMounted(gitRoot, false)
	require.NoError(t, err)

	assert.True(t, h.manager.ExistsFile(remounted, gitRoot+"/HEAD"))
	assert.True(t, h.manager.ExistsFile(remounted, gitRoot+"/objects/pack/pack-deadbeef.pack"))
	assert.True(t, h.manager.ExistsFile(remounted, gitRoot+"/refs/heads/main"))

	reflogPath, err := h.manager.LocalPath(remounted, gitRoot+"/logs/HEAD")
	require.NoError(t, err)
	reflog, err := os.ReadFile(reflogPath)
	require.NoError(t, err)
	assert.Contains(t, string(reflog), "clone: from")
	assert.Contains(t, string(reflog), "checkout: moving")

	objects, err := h.manager.List(remounted, gitRoot+"/objects/00")
	require.NoError(t, err)
	assert.Len(t, objects, 1)
}

// The per-directory cap has to hold while a session writes, not only at mount
// time. A tree that outgrew it in-session would upload fine and then be refused
// on the next mount, leaving the directory unreadable.
func TestGrowthBeyondTheSizeLimitIsRefused(t *testing.T) {
	h := newHarness(t, func(config *Config, quota *fakeQuota) {
		config.MaxPackedDirSize = 4096
	})

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)

	require.NoError(t, h.manager.ReserveGrowth(mount, 4000))

	err = h.manager.ReserveGrowth(mount, 500)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrArchiveTooLarge), "got %v", err)

	// The refused growth is not charged, so the tree stays exactly where it was.
	assert.Equal(t, int64(4000), mount.ReservedSize())

	// And the directory is still usable up to the cap.
	require.NoError(t, h.manager.ReserveGrowth(mount, 96))
	assert.Equal(t, int64(4096), mount.ReservedSize())
}

// The zone this first ran against answers a delete of something that is not
// there with a policy outcome, CUT_ACTION_PROCESSED_ERR, instead of a
// file-not-found. Clearing the previous archive unconditionally therefore
// aborted the very first upload of every packed directory, which by definition
// has no previous archive, and nothing ever reached iRODS.
func TestFirstUploadSucceedsWhenDeletingAMissingArchiveErrors(t *testing.T) {
	h := newHarness(t, nil)

	h.backend.mu.Lock()
	h.backend.deleteMissingErr = errors.New("failed to delete data object: CUT_ACTION_PROCESSED_ERR")
	h.backend.mu.Unlock()

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/pyvenv.cfg", "home = /usr/bin\n")

	require.NoError(t, h.manager.Unmount(mount), "the first upload must not depend on a previous archive")

	assert.True(t, h.backend.exists(testRoot+".mount.tar"))

	// No temporary object was abandoned along the way.
	for _, name := range h.backend.names(t, "/z/home/u/proj") {
		assert.False(t, IsTransientArchiveName(name), "leftover temporary object %q", name)
	}

	// And a second session, which does have a previous archive to replace,
	// still works.
	remounted, err := h.manager.EnsureMounted(testRoot, false)
	require.NoError(t, err)
	h.writeInMount(t, remounted, testRoot+"/added.cfg", "more\n")
	require.NoError(t, h.manager.Unmount(remounted))

	final, err := h.manager.EnsureMounted(testRoot, false)
	require.NoError(t, err)
	assert.True(t, h.manager.ExistsFile(final, testRoot+"/pyvenv.cfg"))
	assert.True(t, h.manager.ExistsFile(final, testRoot+"/added.cfg"))
}

// Releasing a session flushes staging and then unmounts, so an unchanged tree
// would otherwise be packed and uploaded twice back to back.
func TestUnmountDoesNotRepackAnUnchangedTree(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/lib.py", "x = 1\n")

	require.NoError(t, h.manager.Pack(mount))

	h.backend.mu.Lock()
	uploadsAfterFlush := h.backend.uploads
	h.backend.mu.Unlock()

	require.NoError(t, h.manager.Unmount(mount))

	h.backend.mu.Lock()
	uploadsAfterUnmount := h.backend.uploads
	h.backend.mu.Unlock()

	assert.Equal(t, uploadsAfterFlush, uploadsAfterUnmount,
		"a tree unchanged since its last pack is not uploaded again")
	assert.True(t, h.backend.exists(testRoot+".mount.tar"))
	assert.Empty(t, h.manager.Statuses(), "the mount is still released")
}

func TestUnmountRepacksWhenTheTreeChangedAfterTheFlush(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/lib.py", "x = 1\n")
	require.NoError(t, h.manager.Pack(mount))

	// A write lands after the flush, so the release must pick it up.
	h.writeInMount(t, mount, testRoot+"/late.py", "y = 2\n")
	require.NoError(t, h.manager.Unmount(mount))

	remounted, err := h.manager.EnsureMounted(testRoot, false)
	require.NoError(t, err)
	assert.True(t, h.manager.ExistsFile(remounted, testRoot+"/late.py"),
		"a change made after the flush still reaches iRODS")
}

// A tree that was only read is identical to its archive, so releasing it must
// not re-upload it. For a multi-gigabyte virtualenv that is the difference
// between a free unmount and a full repack.
func TestUnmountDoesNotUploadAReadOnlyTree(t *testing.T) {
	h := newHarness(t, nil)

	source := t.TempDir()
	writeFile(t, filepath.Join(source, "pyvenv.cfg"), "home = /usr/bin\n", 0644)
	h.backend.seedArchive(t, testRoot+".mount.tar", source, CompressionNone)

	mount, err := h.manager.EnsureMounted(testRoot, false)
	require.NoError(t, err)
	require.False(t, mount.IsDirty())

	// Reading does not dirty the tree.
	_, err = h.manager.List(mount, testRoot)
	require.NoError(t, err)

	h.backend.mu.Lock()
	uploadsBefore := h.backend.uploads
	h.backend.mu.Unlock()

	require.NoError(t, h.manager.Unmount(mount))

	h.backend.mu.Lock()
	uploadsAfter := h.backend.uploads
	h.backend.mu.Unlock()

	assert.Equal(t, uploadsBefore, uploadsAfter, "an untouched tree is not re-uploaded on release")
	assert.True(t, h.backend.exists(testRoot+".mount.tar"), "its archive is left in place")
}

// A tree whose archive failed to upload is the only copy of that data. Close
// must leave it on disk rather than clearing the root along with the session.
func TestCloseKeepsTreesWhoseArchiveFailedToUpload(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/irreplaceable.txt", "the only copy\n")

	h.backend.mu.Lock()
	h.backend.uploadErr = errors.New("iRODS unavailable")
	h.backend.mu.Unlock()

	require.Error(t, h.manager.Close())

	localPath, err := h.manager.LocalPath(mount, testRoot+"/irreplaceable.txt")
	require.NoError(t, err)
	content, readErr := os.ReadFile(localPath)
	require.NoError(t, readErr, "the tree must survive a failed upload")
	assert.Equal(t, "the only copy\n", string(content))
}

func TestCloseClearsTheRootWhenEverythingReachedIRODS(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/a.txt", "content\n")

	require.NoError(t, h.manager.Close())

	assert.True(t, h.backend.exists(testRoot+".mount.tar"))
	_, statErr := os.Stat(h.manager.localRootPath)
	assert.True(t, os.IsNotExist(statErr), "nothing is left behind once every archive is uploaded")
	_ = mount
}

// Two writers growing the same tree must not both see room for the same bytes.
// The limit here leaves room for exactly one chunk, so any second grant is a
// check that ran against a size another writer had already claimed.
func TestConcurrentGrowthCannotExceedTheDirectoryLimit(t *testing.T) {
	const chunk = 32 * 1024
	const limit = chunk + 1024

	h := newHarness(t, func(config *Config, quota *fakeQuota) {
		config.MaxPackedDirSize = limit
	})

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)

	const writers = 64
	var granted int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := h.manager.ReserveGrowth(mount, chunk); err == nil {
				mu.Lock()
				granted += chunk
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int64(chunk), granted, "only one writer fits under the limit")
	assert.Equal(t, granted, mount.ReservedSize(), "the charge matches what was granted")
}

// Packing is CPU and disk bound and is capped for that reason. This covers the
// ordinary path only: during shutdown the cap is deliberately bypassed so every
// tree still gets flushed.
func TestConcurrentPackingStaysWithinItsLimit(t *testing.T) {
	h := newHarness(t, func(config *Config, quota *fakeQuota) {
		config.ConcurrentPackLimit = 2
	})

	roots := []string{
		"/z/home/u/proj/.venv",
		"/z/home/u/proj/.git",
		"/z/home/u/other/.venv",
		"/z/home/u/other/.git",
	}
	h.backend.seedDir(t, "/z/home/u/proj")
	h.backend.seedDir(t, "/z/home/u/other")

	mounts := make([]*Mount, 0, len(roots))
	for _, root := range roots {
		mount, err := h.manager.EnsureMounted(root, true)
		require.NoError(t, err)
		h.writeInMount(t, mount, root+"/file.txt", "content\n")
		mounts = append(mounts, mount)
	}

	var inFlight, peak int64
	var mu sync.Mutex
	h.backend.beforeUpload = func() {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		mu.Unlock()

		time.Sleep(20 * time.Millisecond)

		mu.Lock()
		inFlight--
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for _, mount := range mounts {
		wg.Add(1)
		go func(mount *Mount) {
			defer wg.Done()
			assert.NoError(t, h.manager.Pack(mount))
		}(mount)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.LessOrEqual(t, peak, int64(2), "more packs ran at once than the configured limit")
}

// Unmounting discards the local tree, which is the only copy of anything the
// archive does not hold. A change that lands while the archive is being built
// is not in that archive, so the tree must be kept rather than deleted with it.
func TestUnmountKeepsATreeThatChangedWhileItWasPacked(t *testing.T) {
	h := newHarness(t, nil)

	mount, err := h.manager.EnsureMounted(testRoot, true)
	require.NoError(t, err)
	h.writeInMount(t, mount, testRoot+"/a.txt", "one\n")

	// A write landing mid-upload moves the change counter, so the pack that is
	// already running must not report the tree as clean.
	h.backend.beforeUpload = func() { mount.MarkDirty() }

	err = h.manager.Unmount(mount)
	require.Error(t, err, "a tree with unsaved changes must not be discarded")
	assert.Contains(t, err.Error(), "changed while it was being unmounted")

	assert.True(t, mount.IsDirty())
	assert.True(t, h.manager.ExistsFile(mount, testRoot+"/a.txt"), "the tree survives")

	// A retry with nothing further arriving completes normally.
	h.backend.beforeUpload = nil
	require.NoError(t, h.manager.Unmount(mount))
	assert.True(t, h.backend.exists(testRoot+".mount.tar"))
	_, statErr := os.Stat(mount.LocalPath)
	assert.True(t, os.IsNotExist(statErr))
}
