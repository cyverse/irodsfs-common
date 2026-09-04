package stagingfs

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
)

func TestStagingFSCloseRemovesDataAfterSuccessfulSync(t *testing.T) {
	rootPath := filepath.Join(t.TempDir(), "staging")
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: rootPath,
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	if err := sf.Create("/synced.txt"); err != nil {
		t.Fatalf("Failed to stage file: %v", err)
	}

	if err := sf.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if _, err := os.Stat(rootPath); !os.IsNotExist(err) {
		t.Fatalf("Staging root must be removed after successful sync, stat error: %v", err)
	}
}

func TestStagingFSClosePreservesFailedDataForRecovery(t *testing.T) {
	rootPath := filepath.Join(t.TempDir(), "staging")
	sf, err := NewStagingFSWithPersistence(&StagingFSConfig{
		LocalRootPath: rootPath,
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create persistent StagingFS: %v", err)
	}
	if err := sf.Create("/pending.txt"); err != nil {
		t.Fatalf("Failed to stage file: %v", err)
	}
	localPath := sf.getLocalDataPath("/pending.txt")
	content := []byte("pending staging data")
	if err := os.WriteFile(localPath, content, 0644); err != nil {
		t.Fatalf("Failed to write staged data: %v", err)
	}

	errSync := errors.New("iRODS unavailable")
	sf.RegisterActionHandler(func(*StagingMetadata) error { return errSync })
	if err := sf.Close(); !errors.Is(err, errSync) {
		t.Fatalf("Close error = %v, want sync error %v", err, errSync)
	}
	if data, err := os.ReadFile(localPath); err != nil {
		t.Fatalf("Failed staging data was not preserved: %v", err)
	} else if string(data) != string(content) {
		t.Fatalf("Preserved staging data = %q, want %q", data, content)
	}

	restored, err := NewStagingFSWithPersistence(&StagingFSConfig{
		LocalRootPath: rootPath,
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to reopen preserved staging data: %v", err)
	}
	if meta := restored.Get("/pending.txt"); meta == nil || meta.Action != ActionUpload {
		t.Fatalf("Restored staging metadata = %+v, want pending upload", meta)
	}
	if data, err := os.ReadFile(localPath); err != nil {
		t.Fatalf("Failed to read restored staging data: %v", err)
	} else if string(data) != string(content) {
		t.Fatalf("Restored staging data = %q, want %q", data, content)
	}
	if err := restored.Close(); err != nil {
		t.Fatalf("Failed to sync and close restored staging data: %v", err)
	}
}

func TestStagingFSCloseWaitsForBackgroundWorker(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
		SyncInterval:  time.Millisecond,
		GracePeriod:   time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	sf.RegisterActionHandler(func(meta *StagingMetadata) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	})
	if err := sf.Mkdir("/dir"); err != nil {
		t.Fatalf("Failed to stage directory: %v", err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("Background worker did not start")
	}

	closed := make(chan error, 1)
	go func() {
		closed <- sf.Close()
	}()
	select {
	case err := <-closed:
		t.Fatalf("Close returned while background worker was active: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after background worker exited")
	}
}

func TestBackgroundMkdirRemainsVisibleAsCachedDirectory(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	if err := sf.Mkdir("/repo/empty"); err != nil {
		t.Fatalf("Failed to stage directory: %v", err)
	}
	sf.syncOldItems(0)

	if meta := sf.Get("/repo/empty"); meta != nil {
		t.Fatalf("Expected pending MKDIR metadata to be synced, got %+v", meta)
	}
	if cached := sf.GetCachedDirs()["/repo/empty"]; cached == nil || cached.Action != ActionMkdir {
		t.Fatalf("Expected synced directory visibility cache, got %+v", cached)
	}
}

func TestOpenCachedForReadRefreshesAccessTime(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	path := "/cached.txt"
	localPath := sf.getLocalDataPath(path)
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		t.Fatalf("Failed to create staging data directory: %v", err)
	}
	if err := os.WriteFile(localPath, []byte("local cached data"), 0644); err != nil {
		t.Fatalf("Failed to create cached file: %v", err)
	}

	oldAccess := time.Now().Add(-time.Hour)
	sf.cacheMutex.Lock()
	sf.cachedItems[path] = &StagingMetadata{
		Path:           path,
		Action:         ActionUpload,
		FileState:      StagingFileCached,
		LastAccessedAt: oldAccess,
	}
	sf.cacheMutex.Unlock()

	f, meta, found, err := sf.OpenCachedForRead(path)
	if err != nil || !found {
		t.Fatalf("OpenCachedForRead() = (%v, found=%t), want cached file", err, found)
	}
	defer f.Close()
	data := make([]byte, len("local cached data"))
	if _, err := f.Read(data); err != nil {
		t.Fatalf("Failed to read cached file: %v", err)
	}
	if string(data) != "local cached data" {
		t.Fatalf("Cached data = %q", data)
	}
	if !meta.LastAccessedAt.After(oldAccess) {
		t.Fatalf("Returned access time = %v, want after %v", meta.LastAccessedAt, oldAccess)
	}
	if !sf.cachedItems[path].LastAccessedAt.After(oldAccess) {
		t.Fatalf("Stored access time = %v, want after %v", sf.cachedItems[path].LastAccessedAt, oldAccess)
	}
}

func TestTransitionToCachedStoresRemoteFreshness(t *testing.T) {
	remoteModified := time.Now().UTC().Truncate(time.Second)
	backend := &statMockStagingClient{entry: &irodsclient_fs.Entry{
		Size:       int64(len("synced data")),
		ModifyTime: remoteModified,
	}}
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        backend,
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	path := "/synced.txt"
	localPath := sf.getLocalDataPath(path)
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		t.Fatalf("Failed to create staging data directory: %v", err)
	}
	if err := os.WriteFile(localPath, []byte("synced data"), 0644); err != nil {
		t.Fatalf("Failed to create staged file: %v", err)
	}
	sf.setPathSize(path, int64(len("synced data")))

	sf.transitionToCached(&StagingMetadata{Path: path, Action: ActionUpload})
	cached := sf.GetCachedItems()[path]
	if cached == nil {
		t.Fatal("Expected synced file in the staging read-cache")
	}
	if !cached.RemoteFreshnessKnown || cached.RemoteSize != backend.entry.Size || !cached.RemoteModifyTime.Equal(remoteModified) {
		t.Fatalf("Cached freshness stamp = %+v, want size=%d modify=%v", cached, backend.entry.Size, remoteModified)
	}
}

// MockStagingClient implements StagingClient for testing
type MockStagingClient struct {
	removeFileForce  bool
	removeFileCalled bool
	removeDirRecurse bool
	removeDirForce   bool
	removeDirCalled  bool
}

type statMockStagingClient struct {
	MockStagingClient
	entry *irodsclient_fs.Entry
	err   error
}

func (m *statMockStagingClient) Stat(string) (*irodsclient_fs.Entry, error) {
	return m.entry, m.err
}

func (m *MockStagingClient) DownloadFileParallel(irodsPath string, localPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	return nil
}
func (m *MockStagingClient) UploadFileParallel(localPath string, irodsPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	return nil
}
func (m *MockStagingClient) RenameFileToFile(srcPath string, destPath string) error { return nil }
func (m *MockStagingClient) RenameDirToDir(srcPath string, destPath string) error   { return nil }
func (m *MockStagingClient) RemoveFile(path string, force bool) error {
	m.removeFileCalled = true
	m.removeFileForce = force
	return nil
}
func (m *MockStagingClient) MakeDir(path string, recurse bool) error { return nil }
func (m *MockStagingClient) RemoveDir(path string, recurse bool, force bool) error {
	m.removeDirCalled = true
	m.removeDirRecurse = recurse
	m.removeDirForce = force
	return nil
}

func TestStagingFSDeletePreservesForce(t *testing.T) {
	client := &MockStagingClient{}
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        client,
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	if err := sf.DeleteWithForce("/existing.txt", true); err != nil {
		t.Fatalf("Failed to stage file deletion: %v", err)
	}
	if err := sf.SyncAll(); err != nil {
		t.Fatalf("Failed to sync file deletion: %v", err)
	}
	if !client.removeFileCalled || !client.removeFileForce {
		t.Fatalf("Expected RemoveFile force=true, called=%v force=%v", client.removeFileCalled, client.removeFileForce)
	}
}

func TestStagingFSRmdirPreservesOptions(t *testing.T) {
	client := &MockStagingClient{}
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        client,
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	if err := sf.Rmdir("/existingdir", false, true); err != nil {
		t.Fatalf("Failed to remove directory: %v", err)
	}
	if client.removeDirCalled {
		t.Fatal("RemoveDir must be deferred to sync")
	}
	if err := sf.SyncAll(); err != nil {
		t.Fatalf("Failed to sync directory removal: %v", err)
	}
	if !client.removeDirCalled || client.removeDirRecurse || !client.removeDirForce {
		t.Fatalf("Expected RemoveDir recurse=false force=true, called=%v recurse=%v force=%v",
			client.removeDirCalled, client.removeDirRecurse, client.removeDirForce)
	}
}

func TestStagingFSCreate(t *testing.T) {
	tmpDir := t.TempDir()
	config := &StagingFSConfig{
		LocalRootPath: tmpDir,
		Client:        &MockStagingClient{},
	}

	sf, err := NewStagingFS(config)
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	// Create a file
	path := "/test.txt"
	err = sf.Create(path)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Verify metadata
	meta := sf.sm.Get(path)
	if meta == nil {
		t.Errorf("Expected metadata for %s", path)
	}
	if meta.Action != ActionUpload {
		t.Errorf("Expected ActionUpload, got %v", meta.Action)
	}
	if !meta.IsNew {
		t.Error("Expected IsNew=true")
	}
}

func TestStagingFSCreateAndWrite(t *testing.T) {
	tmpDir := t.TempDir()
	config := &StagingFSConfig{
		LocalRootPath: tmpDir,
		Client:        &MockStagingClient{},
	}

	sf, err := NewStagingFS(config)
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	path := "/test.txt"
	data := []byte("hello world")

	// Open for write
	f, err := sf.OpenForWrite(path, false)
	if err != nil {
		t.Fatalf("Failed to open file for writing: %v", err)
	}
	defer f.Close()

	// Write data
	_, err = f.Write(data)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Verify local file
	localPath := sf.getLocalDataPath(path)
	written, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("Failed to read local file: %v", err)
	}
	if string(written) != string(data) {
		t.Errorf("Expected %s, got %s", data, written)
	}
}

func TestStagingFSCreateDelete(t *testing.T) {
	tmpDir := t.TempDir()
	config := &StagingFSConfig{
		LocalRootPath: tmpDir,
		Client:        &MockStagingClient{},
	}

	sf, err := NewStagingFS(config)
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	path := "/test.txt"

	// Create then delete
	err = sf.Create(path)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	err = sf.Delete(path)
	if err != nil {
		t.Fatalf("Failed to delete file: %v", err)
	}

	// Verify metadata is removed (CREATE → DELETE removes metadata)
	meta := sf.sm.Get(path)
	if meta != nil {
		t.Errorf("Expected metadata to be removed, but got %v", meta)
	}
}

func TestStagingFSRenameNewFile(t *testing.T) {
	tmpDir := t.TempDir()
	config := &StagingFSConfig{
		LocalRootPath: tmpDir,
		Client:        &MockStagingClient{},
	}

	sf, err := NewStagingFS(config)
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	oldPath := "/old.txt"
	newPath := "/new.txt"

	// Create and rename
	err = sf.Create(oldPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	err = sf.Rename(oldPath, newPath)
	if err != nil {
		t.Fatalf("Failed to rename file: %v", err)
	}

	// Verify metadata path is updated
	oldMeta := sf.sm.Get(oldPath)
	if oldMeta != nil {
		t.Error("Expected old path metadata to be removed")
	}

	newMeta := sf.sm.Get(newPath)
	if newMeta == nil {
		t.Error("Expected metadata at new path")
	}
	if newMeta.Path != newPath {
		t.Errorf("Expected path=%s, got %s", newPath, newMeta.Path)
	}
}

func TestStagingFSOpenForWriteCreatesNew(t *testing.T) {
	tmpDir := t.TempDir()
	config := &StagingFSConfig{
		LocalRootPath: tmpDir,
		Client:        &MockStagingClient{},
	}

	sf, err := NewStagingFS(config)
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	path := "/newfile.txt"

	// OpenForWrite on non-existing file in staging will create it
	f, err := sf.OpenForWrite(path, false)
	if err != nil {
		t.Fatalf("Failed to open file for writing: %v", err)
	}
	f.Write([]byte("data"))
	f.Close()

	// Verify metadata
	meta := sf.sm.Get(path)
	if meta == nil {
		t.Errorf("Expected metadata for %s", path)
	}
	if meta.Action != ActionUpload {
		t.Errorf("Expected ActionUpload, got %v", meta.Action)
	}
	if !meta.IsNew {
		t.Error("Expected IsNew=true for newly written file")
	}
}

func TestStagingFSSyncAll(t *testing.T) {
	tmpDir := t.TempDir()
	config := &StagingFSConfig{
		LocalRootPath: tmpDir,
		Client:        &MockStagingClient{},
	}

	sf, err := NewStagingFS(config)
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	path := "/test.txt"
	data := []byte("test data")

	// Open for write
	f, err := sf.OpenForWrite(path, false)
	if err != nil {
		t.Fatalf("Failed to open file for writing: %v", err)
	}

	_, err = f.Write(data)
	f.Close()
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Verify file exists before sync
	localPath := sf.getLocalDataPath(path)
	if _, err := os.Stat(localPath); err != nil {
		t.Fatalf("Local file should exist before sync: %v", err)
	}

	// Register handler to verify it's called
	handlerCalled := false
	var capturedMeta *StagingMetadata

	sf.RegisterActionHandler(func(meta *StagingMetadata) error {
		handlerCalled = true
		capturedMeta = meta
		return nil
	})

	// SyncAll
	err = sf.SyncAll()
	if err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Verify handler was called
	if !handlerCalled {
		t.Error("Expected handler to be called")
	}
	if capturedMeta.Path != path {
		t.Errorf("Expected path=%s, got %s", path, capturedMeta.Path)
	}

	// Verify metadata is cleared
	all := sf.sm.GetAll()
	if len(all) > 0 {
		t.Error("Expected metadata to be cleared after sync")
	}

	// Verify local files are cleaned up
	dataDir := filepath.Join(tmpDir, "data")
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatalf("Failed to read data directory: %v", err)
	}
	if len(entries) > 0 {
		t.Error("Expected data directory to be empty after sync")
	}
}

func TestStagingFSMkdir(t *testing.T) {
	tmpDir := t.TempDir()
	config := &StagingFSConfig{
		LocalRootPath: tmpDir,
		Client:        &MockStagingClient{},
	}

	sf, err := NewStagingFS(config)
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	path := "/testdir"
	err = sf.Mkdir(path)
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Verify metadata
	meta := sf.sm.Get(path)
	if meta == nil {
		t.Errorf("Expected metadata for %s", path)
	}
	if meta.Action != ActionMkdir {
		t.Errorf("Expected ActionMkdir, got %v", meta.Action)
	}
	if !meta.IsNew {
		t.Error("Expected IsNew=true")
	}

	// Verify local directory exists
	localPath := sf.getLocalDataPath(path)
	if _, err := os.Stat(localPath); err != nil {
		t.Errorf("Expected local directory to exist: %v", err)
	}
}

func TestStagingFSMkdirThenRmdir(t *testing.T) {
	tmpDir := t.TempDir()
	config := &StagingFSConfig{
		LocalRootPath: tmpDir,
		Client:        &MockStagingClient{},
	}

	sf, err := NewStagingFS(config)
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	path := "/testdir"

	// Mkdir then Rmdir
	err = sf.Mkdir(path)
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	err = sf.Rmdir(path, true, true)
	if err != nil {
		t.Fatalf("Failed to remove directory: %v", err)
	}

	// A never-synced empty directory has no backend work and is canceled locally.
	if meta := sf.sm.Get(path); meta != nil {
		t.Fatalf("Expected MKDIR to be canceled, got %v", meta)
	}
	if err := sf.SyncAll(); err != nil {
		t.Fatalf("Failed to sync RMDIR: %v", err)
	}
	if meta := sf.sm.Get(path); meta != nil {
		t.Errorf("Expected metadata removed after sync, got %v", meta)
	}
}

func TestStagingFSRmdirExistingDir(t *testing.T) {
	tmpDir := t.TempDir()
	config := &StagingFSConfig{
		LocalRootPath: tmpDir,
		Client:        &MockStagingClient{},
	}

	sf, err := NewStagingFS(config)
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	path := "/existingdir"

	// Register handler to verify it is deferred until sync.
	handlerCalled := false
	var capturedMeta *StagingMetadata

	sf.RegisterActionHandler(func(meta *StagingMetadata) error {
		handlerCalled = true
		capturedMeta = meta
		return nil
	})

	// Rmdir on existing directory (no prior metadata)
	err = sf.Rmdir(path, true, true)
	if err != nil {
		t.Fatalf("Failed to remove directory: %v", err)
	}

	if handlerCalled {
		t.Fatal("Expected existing-directory RMDIR to be queued")
	}
	if meta := sf.sm.Get(path); meta == nil || meta.Action != ActionRmdir {
		t.Fatalf("Expected queued RMDIR metadata, got %v", meta)
	}
	if err := sf.SyncAll(); err != nil {
		t.Fatalf("Failed to sync RMDIR: %v", err)
	}
	if !handlerCalled {
		t.Fatal("Expected handler during sync")
	}
	if capturedMeta.Action != ActionRmdir {
		t.Errorf("Expected ActionRmdir, got %v", capturedMeta.Action)
	}
	if capturedMeta.IsNew {
		t.Error("Expected IsNew=false for existing directory")
	}

	if meta := sf.sm.Get(path); meta != nil {
		t.Errorf("Expected metadata to be removed after immediate sync, but got %v", meta)
	}
}

func TestStagingFSRmdirDropsPendingChildActions(t *testing.T) {
	tmpDir := t.TempDir()
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: tmpDir,
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	childPath := "/existingdir/test.txt"
	if err := sf.Delete(childPath); err != nil {
		t.Fatalf("Failed to stage child deletion: %v", err)
	}
	staleChild := *sf.sm.Get(childPath)

	var actions []ActionType
	sf.RegisterActionHandler(func(meta *StagingMetadata) error {
		actions = append(actions, meta.Action)
		return nil
	})

	if err := sf.Rmdir("/existingdir", true, true); err != nil {
		t.Fatalf("Failed to remove directory: %v", err)
	}
	if meta := sf.sm.Get(childPath); meta != nil {
		t.Fatalf("Expected child metadata to be removed, got %+v", meta)
	}

	// Simulate a background pass that captured the child before Rmdir removed
	// its metadata. It must not replay DELETE after recursive RMDIR already
	// removed the child in the backend.
	if err := sf.sm.syncOne(&staleChild); err != nil {
		t.Fatalf("Failed to discard stale child action: %v", err)
	}
	if err := sf.SyncAll(); err != nil {
		t.Fatalf("Failed to sync recursive RMDIR: %v", err)
	}
	if len(actions) != 1 || actions[0] != ActionRmdir {
		t.Fatalf("Expected only RMDIR, got actions %v", actions)
	}
}

func TestStagingFSRenameDir(t *testing.T) {
	tmpDir := t.TempDir()
	config := &StagingFSConfig{
		LocalRootPath: tmpDir,
		Client:        &MockStagingClient{},
	}

	sf, err := NewStagingFS(config)
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	oldPath := "/olddir"
	newPath := "/newdir"

	// Create and rename
	err = sf.Mkdir(oldPath)
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	err = sf.RenameDir(oldPath, newPath)
	if err != nil {
		t.Fatalf("Failed to rename directory: %v", err)
	}

	// Verify metadata path is updated
	oldMeta := sf.sm.Get(oldPath)
	if oldMeta != nil {
		t.Error("Expected old path metadata to be removed")
	}

	newMeta := sf.sm.Get(newPath)
	if newMeta == nil {
		t.Error("Expected metadata at new path")
	}
	if newMeta.Path != newPath {
		t.Errorf("Expected path=%s, got %s", newPath, newMeta.Path)
	}
}

func TestStagingFSRenameDirExisting(t *testing.T) {
	tmpDir := t.TempDir()
	config := &StagingFSConfig{
		LocalRootPath: tmpDir,
		Client:        &MockStagingClient{},
	}

	sf, err := NewStagingFS(config)
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}

	oldPath := "/existingdir"
	newPath := "/newpath"

	// Register handler to verify it is deferred until sync.
	handlerCalled := false
	var capturedMeta *StagingMetadata

	sf.RegisterActionHandler(func(meta *StagingMetadata) error {
		handlerCalled = true
		capturedMeta = meta
		return nil
	})

	// RenameDir on existing directory (no prior metadata)
	err = sf.RenameDir(oldPath, newPath)
	if err != nil {
		t.Fatalf("Failed to rename directory: %v", err)
	}

	if handlerCalled {
		t.Fatal("Expected existing-directory RENAME_DIR to be queued")
	}
	if meta := sf.sm.Get(newPath); meta == nil || meta.Action != ActionRenameDir {
		t.Fatalf("Expected queued RENAME_DIR metadata, got %v", meta)
	}
	if err := sf.SyncAll(); err != nil {
		t.Fatalf("Failed to sync RENAME_DIR: %v", err)
	}
	if !handlerCalled {
		t.Fatal("Expected handler during sync")
	}
	if capturedMeta.Action != ActionRenameDir {
		t.Errorf("Expected ActionRenameDir, got %v", capturedMeta.Action)
	}
	if capturedMeta.OldPath != oldPath {
		t.Errorf("Expected OldPath=%s, got %s", oldPath, capturedMeta.OldPath)
	}

	// Verify metadata is removed after sync
	meta := sf.sm.Get(oldPath)
	if meta != nil {
		t.Error("Expected metadata to be removed after immediate sync")
	}
}
