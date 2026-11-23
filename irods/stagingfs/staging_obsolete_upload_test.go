package stagingfs

import (
	"os"
	"sync"
	"testing"

	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
)

type uploadRecordingStagingClient struct {
	MockStagingClient
	mu       sync.Mutex
	uploaded []string
}

func (c *uploadRecordingStagingClient) UploadFileParallel(localPath string, irodsPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.uploaded = append(c.uploaded, irodsPath)
	return nil
}

func newObsoleteUploadTestFS(t *testing.T) (*StagingFS, *uploadRecordingStagingClient) {
	t.Helper()
	client := &uploadRecordingStagingClient{}
	sf, err := NewStagingFS(&StagingFSConfig{LocalRootPath: t.TempDir(), Client: client})
	if err != nil {
		t.Fatalf("failed to create StagingFS: %v", err)
	}
	return sf, client
}

// A queued upload whose local path became a directory has nothing to send.
// Failing on it forever would keep every other operation of the session from
// being flushed, so it is dropped and the rest of the queue goes through.
func TestSyncAllDropsUploadWhoseLocalPathIsADirectory(t *testing.T) {
	sf, client := newObsoleteUploadTestFS(t)

	if err := sf.Create("/X"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := sf.Create("/other.txt"); err != nil {
		t.Fatalf("Create other: %v", err)
	}
	localPath := sf.getLocalDataPath("/X")
	if err := os.Remove(localPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(localPath, 0755); err != nil {
		t.Fatal(err)
	}

	if err := sf.SyncAll(); err != nil {
		t.Fatalf("SyncAll = %v, want the obsolete upload dropped", err)
	}

	if got := len(sf.sm.GetAll()); got != 0 {
		t.Fatalf("%d operations are left after SyncAll", got)
	}
	if len(client.uploaded) != 1 || client.uploaded[0] != "/other.txt" {
		t.Fatalf("uploaded = %v, want only /other.txt", client.uploaded)
	}
}

// The cleanup after a background sync removes the local file of an upload that
// completed. A directory staged at the path of a dropped upload is not that
// file, and stays.
func TestSyncOldKeepsDirectoryAtDroppedUploadPath(t *testing.T) {
	sf, _ := newObsoleteUploadTestFS(t)

	if err := sf.Create("/X"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	localPath := sf.getLocalDataPath("/X")
	if err := os.Remove(localPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(localPath, 0755); err != nil {
		t.Fatal(err)
	}

	if err := sf.SyncOld(0); err != nil {
		t.Fatalf("SyncOld = %v, want the obsolete upload dropped", err)
	}
	if info, err := os.Stat(localPath); err != nil || !info.IsDir() {
		t.Fatalf("the staged directory was removed: %v", err)
	}
	if got := len(sf.sm.GetAll()); got != 0 {
		t.Fatalf("%d operations are left after SyncOld", got)
	}
}

func TestSyncAllDropsUploadWhoseLocalFileIsGone(t *testing.T) {
	sf, client := newObsoleteUploadTestFS(t)

	if err := sf.Create("/gone.txt"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := os.Remove(sf.getLocalDataPath("/gone.txt")); err != nil {
		t.Fatal(err)
	}

	if err := sf.SyncAll(); err != nil {
		t.Fatalf("SyncAll = %v, want the obsolete upload dropped", err)
	}
	if len(client.uploaded) != 0 || len(sf.sm.GetAll()) != 0 {
		t.Fatalf("uploaded = %v, left = %d", client.uploaded, len(sf.sm.GetAll()))
	}
}

// A regular file is still uploaded.
func TestSyncAllStillUploadsRegularFile(t *testing.T) {
	sf, client := newObsoleteUploadTestFS(t)

	if err := sf.Create("/real.txt"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := sf.SyncAll(); err != nil {
		t.Fatalf("SyncAll: %v", err)
	}
	if len(client.uploaded) != 1 || client.uploaded[0] != "/real.txt" {
		t.Fatalf("uploaded = %v, want /real.txt", client.uploaded)
	}
}

// A directory cannot be created where a staged file still is: queueing it
// behind the file's upload would leave an upload that can never run.
func TestMkdirRefusesPathHoldingAStagedFile(t *testing.T) {
	sf, _ := newObsoleteUploadTestFS(t)

	if err := sf.Create("/X"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := sf.Mkdir("/X"); err == nil {
		t.Fatal("Mkdir over a staged file succeeded")
	}

	// Nothing was queued for the directory, and the file's upload is intact.
	operations := sf.sm.GetAll()
	if meta := operations["/X"]; meta == nil || meta.Action != ActionUpload {
		t.Fatalf("metadata at /X = %#v, want the upload", meta)
	}
	if len(sf.sm.dag.nodes) != 1 {
		t.Fatalf("%d operations queued, want 1", len(sf.sm.dag.nodes))
	}
	if info, err := os.Stat(sf.getLocalDataPath("/X")); err != nil || info.IsDir() {
		t.Fatalf("the staged file was disturbed: %v", err)
	}
}

// Removing a file and creating a directory of the same name is ordinary, and
// must keep working while the removal is still queued.
func TestMkdirAfterPendingDeleteOfFileIsAllowed(t *testing.T) {
	sf, _ := newObsoleteUploadTestFS(t)

	// A file that exists in iRODS is removed: DELETE is queued, not cancelled.
	sf.sm.mu.Lock()
	if err := sf.sm.enqueueOperation("/X", &StagingMetadata{Path: "/X", Action: ActionDelete}, nil, false); err != nil {
		sf.sm.mu.Unlock()
		t.Fatal(err)
	}
	sf.sm.mu.Unlock()

	if err := sf.Mkdir("/X"); err != nil {
		t.Fatalf("Mkdir after a queued DELETE = %v, want it allowed", err)
	}
	if meta := sf.sm.GetAll()["/X"]; meta == nil || meta.Action != ActionMkdir {
		t.Fatalf("metadata at /X = %#v, want MKDIR", meta)
	}
}
