package stagingfs

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
)

func TestModifyAfterDeleteMarksReplacementAsNew(t *testing.T) {
	manager := NewStagingStateManager()
	const path = "/test/delete-then-recreate.txt"

	if err := manager.Delete(path); err != nil {
		t.Fatalf("Delete(%q): %v", path, err)
	}
	if err := manager.Modify(path); err != nil {
		t.Fatalf("Modify(%q): %v", path, err)
	}

	metadata := manager.Get(path)
	if metadata == nil {
		t.Fatal("replacement metadata is missing")
	}
	if metadata.Action != ActionUpload {
		t.Fatalf("action = %s, want %s", metadata.Action, ActionUpload)
	}
	if !metadata.IsNew {
		t.Fatal("DELETE -> UPLOAD replacement must be marked IsNew so Stat uses the staged entry")
	}
}

func TestCreateDeleteCreateKeepsReplacementAsNew(t *testing.T) {
	manager := NewStagingStateManager()
	const path = "/test/new-delete-recreate.txt"

	if err := manager.Create(path); err != nil {
		t.Fatalf("initial Create(%q): %v", path, err)
	}
	if err := manager.Delete(path); err != nil {
		t.Fatalf("Delete(%q): %v", path, err)
	}
	if manager.Get(path) != nil {
		t.Fatal("CREATE -> DELETE must cancel the never-synced upload")
	}
	if err := manager.Create(path); err != nil {
		t.Fatalf("replacement Create(%q): %v", path, err)
	}

	metadata := manager.Get(path)
	if metadata == nil || metadata.Action != ActionUpload || !metadata.IsNew {
		t.Fatalf("replacement metadata = %#v, want new UPLOAD", metadata)
	}
}

func TestMkdirAfterRmdirMarksReplacementAsNew(t *testing.T) {
	manager := NewStagingStateManager()
	const path = "/test/dir-delete-recreate"

	if _, err := manager.Rmdir(path, true, true); err != nil {
		t.Fatalf("Rmdir(%q): %v", path, err)
	}
	if err := manager.Mkdir(path); err != nil {
		t.Fatalf("Mkdir(%q): %v", path, err)
	}

	metadata := manager.Get(path)
	if metadata == nil || metadata.Action != ActionMkdir || !metadata.IsNew {
		t.Fatalf("replacement metadata = %#v, want new MKDIR", metadata)
	}
}

func TestGetReturnsMetadataCopy(t *testing.T) {
	manager := NewStagingStateManager()
	const path = "/test/copy.txt"
	if err := manager.Create(path); err != nil {
		t.Fatalf("Create(%q): %v", path, err)
	}

	metadata := manager.Get(path)
	metadata.Action = ActionDelete
	metadata.LastModifiedAt = time.Time{}

	stored := manager.Get(path)
	if stored.Action != ActionUpload {
		t.Fatalf("stored action = %s, want %s", stored.Action, ActionUpload)
	}
	if stored.LastModifiedAt.IsZero() {
		t.Fatal("mutating Get result changed stored modification time")
	}
}

func TestTouchUpdatesMetadataAndOperationDAG(t *testing.T) {
	manager := NewStagingStateManager()
	const path = "/test/touch.txt"
	if err := manager.Create(path); err != nil {
		t.Fatalf("Create(%q): %v", path, err)
	}

	oldTime := time.Now().Add(-2 * time.Hour)
	manager.mu.Lock()
	manager.metadata[path].LastModifiedAt = oldTime
	operationID := manager.metadata[path].OperationID
	manager.dag.get(operationID).Metadata.LastModifiedAt = oldTime
	manager.mu.Unlock()

	touchedAfter := time.Now()
	if err := manager.Touch(path); err != nil {
		t.Fatalf("Touch(%q): %v", path, err)
	}

	metadata := manager.Get(path)
	if metadata.LastModifiedAt.Before(touchedAfter) {
		t.Fatalf("metadata modification time = %v, want >= %v", metadata.LastModifiedAt, touchedAfter)
	}
	manager.mu.RLock()
	dagModifiedAt := manager.dag.get(operationID).Metadata.LastModifiedAt
	manager.mu.RUnlock()
	if dagModifiedAt.Before(touchedAfter) {
		t.Fatalf("DAG modification time = %v, want >= %v", dagModifiedAt, touchedAfter)
	}
	if candidates := manager.getSyncCandidates(time.Hour, false); len(candidates) != 0 {
		t.Fatalf("Touch did not reset grace period; got %d sync candidates", len(candidates))
	}
}

func TestCompleteOperationKeepsStateWhenPersistenceFails(t *testing.T) {
	options := badger.DefaultOptions(t.TempDir())
	options.Logger = nil
	db, err := badger.Open(options)
	if err != nil {
		t.Fatalf("Failed to open Badger: %v", err)
	}

	sm := NewStagingStateManagerWithPersistence(db)
	const path = "/persisted.txt"
	if err := sm.Create(path); err != nil {
		t.Fatalf("Failed to stage file: %v", err)
	}
	candidate := *sm.Get(path)

	var calls int
	sm.RegisterActionHandler(func(*StagingMetadata) error {
		calls++
		return nil
	})

	// Every later write fails, so the completion cannot be recorded.
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close Badger: %v", err)
	}

	if err := sm.syncOne(&candidate); err == nil {
		t.Fatal("sync reported success although the completion could not be persisted")
	}
	if calls != 1 {
		t.Fatalf("backend handler calls = %d, want 1", calls)
	}

	// Memory must still describe a pending operation, or the caller is told the
	// sync failed while nothing is left to retry.
	meta := sm.Get(path)
	if meta == nil || meta.OperationID != candidate.OperationID {
		t.Fatalf("metadata = %+v, want the operation to stay pending for a retry", meta)
	}
	if candidates := sm.getSyncCandidates(0, true); len(candidates) != 1 {
		t.Fatalf("sync candidates = %d, want the operation to be runnable again", len(candidates))
	}
}

// TestSyncAllSkipsOperationCancelledDuringPass covers the candidate snapshot
// that SyncAll consumes: a pass runs every candidate it selected, so an
// operation that is cancelled while an earlier one is running must be skipped
// rather than handed to the handler.
func TestSyncAllSkipsOperationCancelledDuringPass(t *testing.T) {
	manager := NewStagingStateManager()

	const (
		firstPath     = "/mdtest/a.txt"
		cancelledPath = "/mdtest/b.txt"
		lastPath      = "/mdtest/c.txt"
	)
	for _, path := range []string{firstPath, cancelledPath, lastPath} {
		if err := manager.Create(path); err != nil {
			t.Fatalf("Create(%q): %v", path, err)
		}
	}

	synced := []string{}
	manager.RegisterActionHandler(func(metadata *StagingMetadata) error {
		synced = append(synced, metadata.Path)
		if metadata.Path == firstPath {
			// An unrelated path may be mutated from a handler, and a
			// never-synced upload is cancelled outright.
			if err := manager.Delete(cancelledPath); err != nil {
				t.Errorf("Delete(%q): %v", cancelledPath, err)
			}
		}
		return nil
	})

	if err := manager.SyncAll(); err != nil {
		t.Fatalf("SyncAll: %v", err)
	}

	for _, path := range synced {
		if path == cancelledPath {
			t.Fatalf("Expected the cancelled operation to be skipped, synced %v", synced)
		}
	}
	if len(synced) != 2 {
		t.Fatalf("Expected the two remaining operations to sync, got %v", synced)
	}
	if manager.Get(cancelledPath) != nil {
		t.Fatal("Expected no metadata for the cancelled path")
	}
}

// TestSyncAllSkipsOperationEditedDuringPass covers a queued operation that is
// edited in place, keeping its ID, after the pass took its candidates. Removing
// a directory turns a queued upload below it into a delete, so acting on the
// snapshot would upload a file the caller has just removed.
func TestSyncAllSkipsOperationEditedDuringPass(t *testing.T) {
	manager := NewStagingStateManager()

	const (
		firstPath  = "/mdtest/a/first.txt"
		editedPath = "/mdtest/b/edited.txt"
	)
	if err := manager.Create(firstPath); err != nil {
		t.Fatalf("Create(%q): %v", firstPath, err)
	}
	// Modify, not Create: the file exists in the backend, so removing its
	// directory turns the queued upload into a delete instead of cancelling it.
	if err := manager.Modify(editedPath); err != nil {
		t.Fatalf("Modify(%q): %v", editedPath, err)
	}

	synced := map[string]ActionType{}
	manager.RegisterActionHandler(func(metadata *StagingMetadata) error {
		if previous, repeated := synced[metadata.Path]; repeated {
			t.Errorf("Path %q synced twice, as %s then %s", metadata.Path, previous, metadata.Action)
		}
		synced[metadata.Path] = metadata.Action
		if metadata.Path == firstPath {
			// An unrelated subtree may be mutated from a handler.
			if _, err := manager.Rmdir("/mdtest/b", false, true); err != nil {
				t.Errorf("Rmdir: %v", err)
			}
		}
		return nil
	})

	if err := manager.SyncAll(); err != nil {
		t.Fatalf("SyncAll: %v", err)
	}

	if action, ok := synced[editedPath]; !ok || action != ActionDelete {
		t.Fatalf("Expected the edited operation to sync as %s, got %q -> %v", ActionDelete, editedPath, synced)
	}
	if action, ok := synced["/mdtest/b"]; !ok || action != ActionRmdir {
		t.Fatalf("Expected the directory removal to sync, got %v", synced)
	}
	if manager.Get(editedPath) != nil {
		t.Fatal("Expected no metadata left for the edited path")
	}
}
