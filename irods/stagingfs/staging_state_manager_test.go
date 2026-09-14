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
