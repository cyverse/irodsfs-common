package packedfs

import (
	"os"
	"path/filepath"
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
