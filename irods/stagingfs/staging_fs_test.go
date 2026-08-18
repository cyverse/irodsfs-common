package stagingfs

import (
	"os"
	"path/filepath"
	"testing"

	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
)

// MockStagingClient implements StagingClient for testing
type MockStagingClient struct{}

func (m *MockStagingClient) DownloadFileParallel(irodsPath string, localPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	return nil
}
func (m *MockStagingClient) UploadFileParallel(localPath string, irodsPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	return nil
}
func (m *MockStagingClient) RenameFileToFile(srcPath string, destPath string) error { return nil }
func (m *MockStagingClient) RenameDirToDir(srcPath string, destPath string) error   { return nil }
func (m *MockStagingClient) RemoveFile(path string, force bool) error               { return nil }
func (m *MockStagingClient) MakeDir(path string, recurse bool) error                { return nil }
func (m *MockStagingClient) RemoveDir(path string, recurse bool, force bool) error  { return nil }

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
		t.Errorf("Expected IsNew=true")
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
	f, err := sf.OpenForWrite(path)
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
		t.Errorf("Expected old path metadata to be removed")
	}

	newMeta := sf.sm.Get(newPath)
	if newMeta == nil {
		t.Errorf("Expected metadata at new path")
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
	f, err := sf.OpenForWrite(path)
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
		t.Errorf("Expected IsNew=true for newly written file")
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
	f, err := sf.OpenForWrite(path)
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
		t.Errorf("Expected handler to be called")
	}
	if capturedMeta.Path != path {
		t.Errorf("Expected path=%s, got %s", path, capturedMeta.Path)
	}

	// Verify metadata is cleared
	all := sf.sm.GetAll()
	if len(all) > 0 {
		t.Errorf("Expected metadata to be cleared after sync")
	}

	// Verify local files are cleaned up
	dataDir := filepath.Join(tmpDir, "data")
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatalf("Failed to read data directory: %v", err)
	}
	if len(entries) > 0 {
		t.Errorf("Expected data directory to be empty after sync")
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
		t.Errorf("Expected IsNew=true")
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

	err = sf.Rmdir(path)
	if err != nil {
		t.Fatalf("Failed to remove directory: %v", err)
	}

	// Verify metadata is removed (MKDIR → RMDIR removes metadata)
	meta := sf.sm.Get(path)
	if meta != nil {
		t.Errorf("Expected metadata to be removed, but got %v", meta)
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

	// Register handler to verify it's called immediately
	handlerCalled := false
	var capturedMeta *StagingMetadata

	sf.RegisterActionHandler(func(meta *StagingMetadata) error {
		handlerCalled = true
		capturedMeta = meta
		return nil
	})

	// Rmdir on existing directory (no prior metadata)
	err = sf.Rmdir(path)
	if err != nil {
		t.Fatalf("Failed to remove directory: %v", err)
	}

	// Verify handler was called immediately
	if !handlerCalled {
		t.Errorf("Expected handler to be called immediately for existing directory RMDIR")
	}
	if capturedMeta.Action != ActionRmdir {
		t.Errorf("Expected ActionRmdir, got %v", capturedMeta.Action)
	}
	if capturedMeta.IsNew {
		t.Errorf("Expected IsNew=false for existing directory")
	}

	// Verify metadata is removed after sync
	meta := sf.sm.Get(path)
	if meta != nil {
		t.Errorf("Expected metadata to be removed after immediate sync, but got %v", meta)
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
		t.Errorf("Expected old path metadata to be removed")
	}

	newMeta := sf.sm.Get(newPath)
	if newMeta == nil {
		t.Errorf("Expected metadata at new path")
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

	// Register handler to verify it's called immediately
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

	// Verify handler was called immediately
	if !handlerCalled {
		t.Errorf("Expected handler to be called immediately for existing directory RENAME")
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
		t.Errorf("Expected metadata to be removed after immediate sync")
	}
}
