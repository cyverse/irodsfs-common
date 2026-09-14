package stagingfs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func TestStagingFSOpenForReadWriteFromUsesRemoteSourceAndLogicalDestination(t *testing.T) {
	client := &downloadRecordingStagingClient{content: []byte("original data")}
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        client,
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	f, err := sf.OpenForReadWriteFrom("/renamed.txt", "/original.txt", false)
	if err != nil {
		t.Fatalf("OpenForReadWriteFrom failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Failed to close staged file: %v", err)
	}

	if client.downloadedPath != "/original.txt" {
		t.Fatalf("download source = %q, want %q", client.downloadedPath, "/original.txt")
	}
	if meta := sf.Get("/renamed.txt"); meta == nil || meta.Action != ActionUpload {
		t.Fatalf("logical staging metadata = %+v, want pending upload at renamed path", meta)
	}
	data, err := os.ReadFile(sf.getLocalDataPath("/renamed.txt"))
	if err != nil {
		t.Fatalf("Failed to read logical staged file: %v", err)
	}
	if string(data) != "original data" {
		t.Fatalf("logical staged data = %q, want %q", data, "original data")
	}
}

func TestStagingFSOpenForReadWriteRetriesAfterInterruptedDownload(t *testing.T) {
	client := &interruptedDownloadStagingClient{}
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        client,
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	const path = "/interrupted.txt"
	if _, err := sf.OpenForReadWrite(path, false); err == nil {
		t.Fatal("Interrupted download unexpectedly succeeded")
	}
	if size := sf.GetLocalFileSize(path); size != -1 {
		t.Fatalf("Partial download remained at final staging path with size %d", size)
	}

	f, err := sf.OpenForReadWrite(path, false)
	if err != nil {
		t.Fatalf("Retry after interrupted download failed: %v", err)
	}
	data, err := os.ReadFile(f.Name())
	if err != nil {
		f.Close()
		t.Fatalf("Failed to read staged retry result: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Failed to close staged retry result: %v", err)
	}
	sf.ReleaseRef(path)

	if client.downloadCalls != 2 {
		t.Fatalf("download calls = %d, want 2", client.downloadCalls)
	}
	if string(data) != "complete remote contents" {
		t.Fatalf("staged retry data = %q, want complete remote contents", data)
	}
	tmpMatches, err := filepath.Glob(filepath.Join(filepath.Dir(f.Name()), ".interrupted.txt.download-*"))
	if err != nil {
		t.Fatalf("Failed to inspect temporary download files: %v", err)
	}
	if len(tmpMatches) != 0 {
		t.Fatalf("temporary download files were not cleaned up: %v", tmpMatches)
	}
}

func TestStagingFSTruncateResetsOperationGracePeriod(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	const path = "/truncate.txt"
	f, err := sf.OpenForWrite(path, false)
	if err != nil {
		t.Fatalf("OpenForWrite failed: %v", err)
	}
	if _, err := f.Write([]byte("abcdef")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	sf.ReleaseRef(path)

	oldTime := time.Now().Add(-2 * time.Hour)
	sf.sm.mu.Lock()
	operationID := sf.sm.metadata[path].OperationID
	sf.sm.metadata[path].LastModifiedAt = oldTime
	sf.sm.dag.get(operationID).Metadata.LastModifiedAt = oldTime
	sf.sm.mu.Unlock()

	if err := sf.TruncateFile(path, 3); err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	if candidates := sf.sm.getSyncCandidates(time.Hour, false); len(candidates) != 0 {
		t.Fatalf("truncate did not reset grace period; got %d sync candidates", len(candidates))
	}
	if size := sf.GetLocalFileSize(path); size != 3 {
		t.Fatalf("truncated size = %d, want 3", size)
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

type downloadRecordingStagingClient struct {
	MockStagingClient
	downloadedPath string
	content        []byte
}

type interruptedDownloadStagingClient struct {
	MockStagingClient
	downloadCalls int
}

func (m *statMockStagingClient) Stat(string) (*irodsclient_fs.Entry, error) {
	return m.entry, m.err
}

func (m *downloadRecordingStagingClient) DownloadFileParallel(irodsPath string, localPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	m.downloadedPath = irodsPath
	return os.WriteFile(localPath, m.content, 0644)
}

func (m *interruptedDownloadStagingClient) DownloadFileParallel(irodsPath string, localPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	m.downloadCalls++
	if m.downloadCalls == 1 {
		if err := os.WriteFile(localPath, []byte("partial"), 0644); err != nil {
			return err
		}
		return errors.New("simulated interrupted download")
	}
	return os.WriteFile(localPath, []byte("complete remote contents"), 0644)
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
	sf.NotifyFileClosed(path)
	sf.ReleaseRef(path)

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

func TestStagingFSSyncAllRejectsOpenWriteHandle(t *testing.T) {
	rootPath := t.TempDir()
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: rootPath,
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	const path = "/open.txt"
	f, err := sf.OpenForWrite(path, false)
	if err != nil {
		t.Fatalf("OpenForWrite failed: %v", err)
	}
	if _, err := f.Write([]byte("data still being written")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := sf.SyncAll(); !errors.Is(err, ErrOpenWriteHandles) {
		t.Fatalf("SyncAll error = %v, want ErrOpenWriteHandles", err)
	}
	if _, err := os.Stat(sf.getLocalDataPath(path)); err != nil {
		t.Fatalf("SyncAll removed an open staged file: %v", err)
	}
	if meta := sf.Get(path); meta == nil || meta.Action != ActionUpload {
		t.Fatalf("SyncAll removed pending metadata for open file: %+v", meta)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	sf.NotifyFileClosed(path)
	sf.ReleaseRef(path)

	if err := sf.SyncAll(); err != nil {
		t.Fatalf("SyncAll after closing write handle failed: %v", err)
	}
}

func TestStagingFSSyncOldPreservesBlockedUpload(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	const path = "/blocked.txt"
	if err := sf.Create(path); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	localPath := sf.getLocalDataPath(path)
	content := []byte("must survive until retry")
	if err := os.WriteFile(localPath, content, 0644); err != nil {
		t.Fatalf("Failed to write staged file: %v", err)
	}

	sf.sm.mu.Lock()
	meta := sf.sm.metadata[path]
	meta.LastModifiedAt = time.Now().Add(-2 * time.Hour)
	sf.sm.dag.get(meta.OperationID).Metadata.LastModifiedAt = meta.LastModifiedAt
	sf.sm.dag.get(meta.OperationID).State = OperationBlocked
	sf.sm.mu.Unlock()

	if err := sf.SyncOld(time.Hour); err != nil {
		t.Fatalf("SyncOld failed: %v", err)
	}
	if data, err := os.ReadFile(localPath); err != nil {
		t.Fatalf("SyncOld removed blocked staged file: %v", err)
	} else if string(data) != string(content) {
		t.Fatalf("blocked staged data = %q, want %q", data, content)
	}
	if meta := sf.Get(path); meta == nil || meta.Action != ActionUpload {
		t.Fatalf("SyncOld removed blocked metadata: %+v", meta)
	}
}

func TestStagingFSSyncOldRemovesCompletedUpload(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	const path = "/completed.txt"
	if err := sf.Create(path); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	localPath := sf.getLocalDataPath(path)
	if err := os.WriteFile(localPath, []byte("completed upload"), 0644); err != nil {
		t.Fatalf("Failed to write staged file: %v", err)
	}
	sf.setPathSize(path, int64(len("completed upload")))

	sf.sm.mu.Lock()
	meta := sf.sm.metadata[path]
	meta.LastModifiedAt = time.Now().Add(-2 * time.Hour)
	sf.sm.dag.get(meta.OperationID).Metadata.LastModifiedAt = meta.LastModifiedAt
	sf.sm.mu.Unlock()

	if err := sf.SyncOld(time.Hour); err != nil {
		t.Fatalf("SyncOld failed: %v", err)
	}
	if _, err := os.Stat(localPath); !os.IsNotExist(err) {
		t.Fatalf("SyncOld retained completed staged file, stat error: %v", err)
	}
	if meta := sf.Get(path); meta != nil {
		t.Fatalf("SyncOld retained completed metadata: %+v", meta)
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

// stagePendingUpload writes content to path and ages its pending upload so it
// becomes a sync candidate.
func stagePendingUpload(t *testing.T, sf *StagingFS, path string, content string) {
	t.Helper()

	f, err := sf.OpenForWrite(path, false)
	if err != nil {
		t.Fatalf("OpenForWrite failed: %v", err)
	}
	if _, err := f.Write([]byte(content)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	sf.ReleaseRef(path)

	oldTime := time.Now().Add(-2 * time.Hour)
	sf.sm.mu.Lock()
	operationID := sf.sm.metadata[path].OperationID
	sf.sm.metadata[path].LastModifiedAt = oldTime
	sf.sm.dag.get(operationID).Metadata.LastModifiedAt = oldTime
	sf.sm.mu.Unlock()
}

func TestStagingFSStaleCandidateSkipsTruncatedFile(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	const path = "/truncated.txt"
	stagePendingUpload(t, sf, path, "abcdef")

	// Snapshot the candidate the way a background pass does, then modify the
	// file locally before the snapshot is executed.
	candidates := sf.sm.getSyncCandidates(time.Hour, false)
	if len(candidates) != 1 {
		t.Fatalf("sync candidates = %d, want 1", len(candidates))
	}

	var uploaded []string
	sf.RegisterActionHandler(func(meta *StagingMetadata) error {
		uploaded = append(uploaded, meta.Path)
		return nil
	})

	if err := sf.TruncateFile(path, 3); err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}

	executed, err := sf.sm.syncCandidate(candidates[0], time.Hour, false)
	if err != nil {
		t.Fatalf("Stale sync candidate failed: %v", err)
	}
	if executed || len(uploaded) != 0 {
		t.Fatalf("Stale candidate uploaded %v after the file was truncated", uploaded)
	}
	if meta := sf.sm.Get(path); meta == nil || meta.Action != ActionUpload {
		t.Fatalf("staging metadata = %+v, want a pending upload for the truncated file", meta)
	}
}

func TestStagingFSStaleCandidateSkipsReopenedFile(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	const path = "/reopened.txt"
	stagePendingUpload(t, sf, path, "abcdef")

	candidates := sf.sm.getSyncCandidates(time.Hour, false)
	if len(candidates) != 1 {
		t.Fatalf("sync candidates = %d, want 1", len(candidates))
	}

	var uploaded []string
	sf.RegisterActionHandler(func(meta *StagingMetadata) error {
		uploaded = append(uploaded, meta.Path)
		return nil
	})

	// Reopening a path that already has a pending upload must restart its grace
	// period, otherwise the older candidate is synced while the new handle writes.
	f, err := sf.OpenForWrite(path, false)
	if err != nil {
		t.Fatalf("OpenForWrite failed: %v", err)
	}
	defer func() {
		f.Close()
		sf.ReleaseRef(path)
	}()

	executed, err := sf.sm.syncCandidate(candidates[0], time.Hour, false)
	if err != nil {
		t.Fatalf("Stale sync candidate failed: %v", err)
	}
	if executed || len(uploaded) != 0 {
		t.Fatalf("Stale candidate uploaded %v while a write handle was open", uploaded)
	}
	if candidates := sf.sm.getSyncCandidates(time.Hour, false); len(candidates) != 0 {
		t.Fatalf("reopen did not reset the grace period; got %d sync candidates", len(candidates))
	}
}

func TestStagingFSRenameKeepsStateWhenLocalRenameFails(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	// A staged regular file makes the destination parent directory
	// uncreatable, so the local rename cannot succeed.
	blocker, err := sf.OpenForWrite("/blocker", false)
	if err != nil {
		t.Fatalf("OpenForWrite failed: %v", err)
	}
	blocker.Close()
	sf.ReleaseRef("/blocker")

	const path = "/renamed.txt"
	stagePendingUpload(t, sf, path, "staged data")

	if err := sf.Rename(path, "/blocker/renamed.txt"); err == nil {
		t.Fatal("Rename onto an unusable local destination must fail")
	}

	if meta := sf.sm.Get(path); meta == nil || meta.Action != ActionUpload {
		t.Fatalf("staging metadata = %+v, want the pending upload to stay at the source path", meta)
	}
	if meta := sf.sm.Get("/blocker/renamed.txt"); meta != nil {
		t.Fatalf("failed rename left staging metadata at the destination: %+v", meta)
	}
	if data, err := os.ReadFile(sf.getLocalDataPath(path)); err != nil {
		t.Fatalf("Failed to read staged data after a failed rename: %v", err)
	} else if string(data) != "staged data" {
		t.Fatalf("staged data = %q, want %q", data, "staged data")
	}
}

func TestStagingFSRenameRestoresLocalFileWhenStateRenameFails(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	// A pending deletion cannot transition to RENAME, so the state change fails
	// after the local file has already been moved.
	const path = "/deleted.txt"
	if err := sf.Delete(path); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	localPath := sf.getLocalDataPath(path)
	if err := os.WriteFile(localPath, []byte("local data"), 0644); err != nil {
		t.Fatalf("Failed to write local data: %v", err)
	}

	if err := sf.Rename(path, "/deleted-renamed.txt"); err == nil {
		t.Fatal("Rename of a pending deletion must fail")
	}

	if data, err := os.ReadFile(localPath); err != nil {
		t.Fatalf("Local data was not restored after a failed rename: %v", err)
	} else if string(data) != "local data" {
		t.Fatalf("restored data = %q, want %q", data, "local data")
	}
	if _, err := os.Stat(sf.getLocalDataPath("/deleted-renamed.txt")); !os.IsNotExist(err) {
		t.Fatalf("failed rename left local data at the destination, stat error: %v", err)
	}
	if meta := sf.sm.Get(path); meta == nil || meta.Action != ActionDelete {
		t.Fatalf("staging metadata = %+v, want the pending deletion to stay at the source path", meta)
	}
}

func TestStagingFSRenameDirKeepsStateWhenLocalRenameFails(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	blocker, err := sf.OpenForWrite("/blocker", false)
	if err != nil {
		t.Fatalf("OpenForWrite failed: %v", err)
	}
	blocker.Close()
	sf.ReleaseRef("/blocker")

	const childPath = "/olddir/child.txt"
	if err := sf.Mkdir("/olddir"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	stagePendingUpload(t, sf, childPath, "child data")

	if err := sf.RenameDir("/olddir", "/blocker/newdir"); err == nil {
		t.Fatal("RenameDir onto an unusable local destination must fail")
	}

	if meta := sf.sm.Get("/olddir"); meta == nil || meta.Action != ActionMkdir {
		t.Fatalf("directory metadata = %+v, want the pending mkdir to stay at the source path", meta)
	}
	if meta := sf.sm.Get(childPath); meta == nil || meta.Action != ActionUpload {
		t.Fatalf("child metadata = %+v, want the pending upload to stay at the source path", meta)
	}
	if data, err := os.ReadFile(sf.getLocalDataPath(childPath)); err != nil {
		t.Fatalf("Failed to read staged child after a failed directory rename: %v", err)
	} else if string(data) != "child data" {
		t.Fatalf("staged child data = %q, want %q", data, "child data")
	}
}

func TestStagingFSSyncErrorHandlerMayCloseStagingFS(t *testing.T) {
	ready := make(chan struct{})
	closeResult := make(chan error, 1)
	var sf *StagingFS

	config := &StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
		SyncInterval:  time.Millisecond,
		GracePeriod:   time.Millisecond,
		OnSyncError: func(meta *StagingMetadata, err error) {
			<-ready
			select {
			case closeResult <- sf.Close():
			default:
			}
		},
	}

	sf, err := NewStagingFS(config)
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	sf.RegisterActionHandler(func(*StagingMetadata) error {
		return errors.New("simulated sync failure")
	})
	if err := sf.Create("/failing.txt"); err != nil {
		t.Fatalf("Failed to stage file: %v", err)
	}
	close(ready)

	select {
	case <-closeResult:
	case <-time.After(10 * time.Second):
		t.Fatal("Close called from the sync error handler did not return")
	}
}

// unawarePathHolder models a handle that is not tracking rename notifications,
// which is exactly the case the staging registration has to survive.
type unawarePathHolder struct{}

func (h *unawarePathHolder) UpdateStagingPath(string) {}

func TestStagingFSReleaseHandleFollowsRename(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	holder := &unawarePathHolder{}
	f, err := sf.OpenForWriteFor("/dir/old.txt", false, holder)
	if err != nil {
		t.Fatalf("OpenForWriteFor failed: %v", err)
	}
	if err := sf.Rename("/dir/old.txt", "/dir/new.txt"); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	if !sf.hasOpenRef("/dir/new.txt") {
		t.Fatal("rename did not move the open ref to the new path")
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !sf.ReleaseHandle(holder) {
		t.Fatal("ReleaseHandle did not find the registration taken by the open")
	}
	if sf.hasOpenRef("/dir/new.txt") || sf.hasOpenRef("/dir/old.txt") {
		t.Fatal("open ref leaked across the rename; background sync would skip the file forever")
	}
}

func TestStagingFSReleaseHandleFollowsDirectoryRename(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	if err := sf.Mkdir("/olddir"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	holder := &unawarePathHolder{}
	f, err := sf.OpenForWriteFor("/olddir/file.txt", false, holder)
	if err != nil {
		t.Fatalf("OpenForWriteFor failed: %v", err)
	}
	if err := sf.RenameDir("/olddir", "/newdir"); err != nil {
		t.Fatalf("RenameDir failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !sf.ReleaseHandle(holder) {
		t.Fatal("ReleaseHandle did not find the registration taken by the open")
	}
	if sf.hasOpenRef("/newdir/file.txt") || sf.hasOpenRef("/olddir/file.txt") {
		t.Fatal("open ref leaked across the directory rename")
	}
}

func TestStagingFSReleaseHandleIgnoresUnregisteredHolder(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	const path = "/shared.txt"
	writer := &unawarePathHolder{}
	f, err := sf.OpenForWriteFor(path, false, writer)
	if err != nil {
		t.Fatalf("OpenForWriteFor failed: %v", err)
	}

	// A read-only staged handle holds no ref. Closing it must not drop the ref
	// that the concurrent writer holds for the same path.
	if sf.ReleaseHandle(&unawarePathHolder{}) {
		t.Fatal("ReleaseHandle reported a registration for a holder that never took one")
	}
	if !sf.hasOpenRef(path) {
		t.Fatal("an unrelated handle released the writer's open ref")
	}

	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !sf.ReleaseHandle(writer) {
		t.Fatal("ReleaseHandle did not find the writer's registration")
	}
	if sf.hasOpenRef(path) {
		t.Fatal("the writer's open ref was not released")
	}
}

func TestStagingFSCacheEvictionDoesNotRaceWithCachedReads(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	const count = 200
	paths := make([]string, 0, count)
	now := time.Now()
	sf.cacheMutex.Lock()
	for i := 0; i < count; i++ {
		path := fmt.Sprintf("/cache/file-%d.txt", i)
		localPath := sf.getLocalDataPath(path)
		if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
			sf.cacheMutex.Unlock()
			t.Fatalf("Failed to create cache directory: %v", err)
		}
		if err := os.WriteFile(localPath, []byte("cached"), 0644); err != nil {
			sf.cacheMutex.Unlock()
			t.Fatalf("Failed to write cached file: %v", err)
		}
		sf.cachedItems[path] = &StagingMetadata{
			Path:           path,
			FileState:      StagingFileCached,
			LastAccessedAt: now.Add(-time.Duration(i) * time.Second),
		}
		paths = append(paths, path)
	}
	sf.cacheMutex.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// Free more than the cache holds so eviction walks every entry.
		sf.evictCachedOldest(1 << 40)
	}()
	go func() {
		defer wg.Done()
		// Each successful open refreshes LastAccessedAt, which eviction used to
		// read after dropping the cache lock.
		for _, path := range paths {
			file, _, found, err := sf.OpenCachedForRead(path)
			if err != nil || !found {
				continue
			}
			file.Close()
		}
	}()
	wg.Wait()
}

// blockingUploadStagingClient records what each upload actually read and can
// hold an upload open so a replacement can be attempted mid-sync.
type blockingUploadStagingClient struct {
	MockStagingClient
	started  chan struct{}
	release  chan struct{}
	contents chan string
}

func (c *blockingUploadStagingClient) UploadFileParallel(localPath string, irodsPath string, taskNum int, callback irodsclient_common.TransferTrackerCallback) error {
	if c.started != nil {
		close(c.started)
		c.started = nil
	}
	if c.release != nil {
		<-c.release
	}
	data, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	if c.contents != nil {
		c.contents <- string(data)
	}
	return nil
}

func TestStagingFSBulkUploadWaitsForInFlightUpload(t *testing.T) {
	client := &blockingUploadStagingClient{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		contents: make(chan string, 2),
	}
	started := client.started
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        client,
		SyncInterval:  time.Hour,
		GracePeriod:   time.Hour,
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	sourceDir := t.TempDir()
	first := filepath.Join(sourceDir, "first")
	second := filepath.Join(sourceDir, "second")
	if err := os.WriteFile(first, []byte("first contents"), 0644); err != nil {
		t.Fatalf("Failed to write source: %v", err)
	}
	if err := os.WriteFile(second, []byte("second contents"), 0644); err != nil {
		t.Fatalf("Failed to write source: %v", err)
	}

	const path = "/bulk.txt"
	if err := sf.StageForBulkUpload(first, path); err != nil {
		t.Fatalf("StageForBulkUpload failed: %v", err)
	}

	candidates := sf.sm.getSyncCandidates(0, true)
	if len(candidates) != 1 {
		t.Fatalf("sync candidates = %d, want 1", len(candidates))
	}
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- sf.sm.syncOne(candidates[0])
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("Upload did not start")
	}

	// The replacement must not overwrite the staging file that the in-flight
	// upload is still reading.
	stageDone := make(chan error, 1)
	go func() {
		stageDone <- sf.StageForBulkUpload(second, path)
	}()
	select {
	case err := <-stageDone:
		t.Fatalf("replacement was staged (%v) while the previous upload was still reading the file", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(client.release)
	if err := <-syncDone; err != nil {
		t.Fatalf("Upload failed: %v", err)
	}
	if uploaded := <-client.contents; uploaded != "first contents" {
		t.Fatalf("uploaded contents = %q, want %q", uploaded, "first contents")
	}
	select {
	case err := <-stageDone:
		if err != nil {
			t.Fatalf("StageForBulkUpload failed after the upload completed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("StageForBulkUpload did not finish after the upload completed")
	}

	if meta := sf.sm.Get(path); meta == nil || meta.Action != ActionBulkUpload {
		t.Fatalf("staging metadata = %+v, want a pending bulk upload for the replacement", meta)
	}
	if data, err := os.ReadFile(sf.getLocalDataPath(path)); err != nil {
		t.Fatalf("Failed to read staged replacement: %v", err)
	} else if string(data) != "second contents" {
		t.Fatalf("staged contents = %q, want %q", data, "second contents")
	}
}

func TestStagingFSBulkUploadDiscardsMetadataWhenPublishFails(t *testing.T) {
	sf, err := NewStagingFS(&StagingFSConfig{
		LocalRootPath: t.TempDir(),
		Client:        &MockStagingClient{},
		SyncInterval:  time.Hour,
		GracePeriod:   time.Hour,
	})
	if err != nil {
		t.Fatalf("Failed to create StagingFS: %v", err)
	}
	defer sf.Close()

	source := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(source, []byte("contents"), 0644); err != nil {
		t.Fatalf("Failed to write source: %v", err)
	}

	// A directory at the staging path makes publishing the copy fail.
	const path = "/blocked/bulk.txt"
	if err := os.MkdirAll(sf.getLocalDataPath(path), 0755); err != nil {
		t.Fatalf("Failed to block the staging path: %v", err)
	}

	if err := sf.StageForBulkUpload(source, path); err == nil {
		t.Fatal("StageForBulkUpload must fail when the staged copy cannot be published")
	}
	if meta := sf.sm.Get(path); meta != nil {
		t.Fatalf("failed bulk staging left pending metadata with no local data: %+v", meta)
	}

	entries, err := os.ReadDir(filepath.Dir(sf.getLocalDataPath(path)))
	if err != nil {
		t.Fatalf("Failed to inspect the staging directory: %v", err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".stage-") {
			t.Fatalf("temporary staging file was left behind: %s", entry.Name())
		}
	}
}

func TestCopyFileConcurrentToSameDestinationKeepsOneCompleteFile(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "dst")

	const writers = 8
	sources := make([]string, 0, writers)
	contents := make(map[string]struct{}, writers)
	for i := 0; i < writers; i++ {
		source := filepath.Join(dir, fmt.Sprintf("src-%d", i))
		// Large and distinct, so a copy interleaved with another is visible.
		data := strings.Repeat(fmt.Sprintf("%d", i), 1<<20)
		if err := os.WriteFile(source, []byte(data), 0644); err != nil {
			t.Fatalf("Failed to write source: %v", err)
		}
		sources = append(sources, source)
		contents[data] = struct{}{}
	}

	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for _, source := range sources {
		wg.Add(1)
		go func(source string) {
			defer wg.Done()
			if err := copyFile(source, dst); err != nil {
				errs <- err
			}
		}(source)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("copyFile failed: %v", err)
	}

	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("Failed to read destination: %v", err)
	}
	if _, ok := contents[string(data)]; !ok {
		t.Fatalf("destination holds %d bytes that match no single source; concurrent copies clobbered each other", len(data))
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to inspect directory: %v", err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".stage-") {
			t.Fatalf("temporary copy file was left behind: %s", entry.Name())
		}
	}
}
