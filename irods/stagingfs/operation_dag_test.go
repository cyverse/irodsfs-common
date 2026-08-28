package stagingfs

import (
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
)

func TestOperationDAGReleasesDependentNodeOnCompletion(t *testing.T) {
	dag := newOperationDAG()
	first, err := dag.add(&StagingMetadata{Path: "/old", Action: ActionRename}, nil, true)
	if err != nil {
		t.Fatalf("Failed to add first operation: %v", err)
	}
	second, err := dag.add(&StagingMetadata{Path: "/new", Action: ActionUpload}, []string{first.ID}, true)
	if err != nil {
		t.Fatalf("Failed to add dependent operation: %v", err)
	}

	ready := dag.ready(time.Hour, false)
	if len(ready) != 1 || ready[0].ID != first.ID {
		t.Fatalf("Expected only first node ready, got %+v", ready)
	}
	dag.remove(first.ID)
	ready = dag.ready(time.Hour, false)
	if len(ready) != 1 || ready[0].ID != second.ID {
		t.Fatalf("Expected dependent node after completion, got %+v", ready)
	}
}

func TestOperationDAGRejectsCycle(t *testing.T) {
	dag := newOperationDAG()
	first, err := dag.add(&StagingMetadata{Path: "/first", Action: ActionUpload}, nil, false)
	if err != nil {
		t.Fatalf("Failed to add first node: %v", err)
	}
	second, err := dag.add(&StagingMetadata{Path: "/second", Action: ActionUpload}, []string{first.ID}, false)
	if err != nil {
		t.Fatalf("Failed to add second node: %v", err)
	}
	dag.addDependency(first.ID, second.ID)
	if err := dag.validate(); err == nil {
		t.Fatal("Expected cycle validation failure")
	}
}

func TestOperationDAGPersistsRenameUploadChain(t *testing.T) {
	dbPath := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dbPath).WithLogger(nil))
	if err != nil {
		t.Fatalf("Failed to open Badger: %v", err)
	}
	sm := NewStagingStateManagerWithPersistence(db)
	if err := sm.Modify("/old.txt"); err != nil {
		t.Fatalf("Failed to queue upload: %v", err)
	}
	if _, err := sm.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatalf("Failed to queue rename: %v", err)
	}
	if len(sm.dag.nodes) != 2 {
		t.Fatalf("Expected RENAME and UPLOAD nodes, got %d", len(sm.dag.nodes))
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close Badger: %v", err)
	}

	db, err = badger.Open(badger.DefaultOptions(dbPath).WithLogger(nil))
	if err != nil {
		t.Fatalf("Failed to reopen Badger: %v", err)
	}
	defer db.Close()
	restored := NewStagingStateManagerWithPersistence(db)
	if err := restored.Restore(); err != nil {
		t.Fatalf("Failed to restore DAG: %v", err)
	}
	if len(restored.dag.nodes) != 2 {
		t.Fatalf("Expected two restored nodes, got %d", len(restored.dag.nodes))
	}

	var actions []ActionType
	restored.RegisterActionHandler(func(meta *StagingMetadata) error {
		actions = append(actions, meta.Action)
		return nil
	})
	if err := restored.SyncAll(); err != nil {
		t.Fatalf("Failed to sync restored DAG: %v", err)
	}
	want := []ActionType{ActionRename, ActionUpload}
	if len(actions) != len(want) || actions[0] != want[0] || actions[1] != want[1] {
		t.Fatalf("Expected actions %v, got %v", want, actions)
	}
}

func TestFailedChildDeleteBlocksParentRmdir(t *testing.T) {
	sm := NewStagingStateManager()
	if err := sm.Delete("/dir/file.txt"); err != nil {
		t.Fatalf("Failed to queue delete: %v", err)
	}
	if _, err := sm.Rmdir("/dir", false, false); err != nil {
		t.Fatalf("Failed to queue RMDIR: %v", err)
	}

	wantErr := errors.New("delete failed")
	var actions []ActionType
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		actions = append(actions, meta.Action)
		if meta.Action == ActionDelete {
			return wantErr
		}
		return nil
	})
	if err := sm.SyncOld(time.Hour); !errors.Is(err, wantErr) {
		t.Fatalf("Expected delete error, got %v", err)
	}
	if len(actions) != 1 || actions[0] != ActionDelete {
		t.Fatalf("Parent RMDIR ran despite failed dependency: %v", actions)
	}

	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		actions = append(actions, meta.Action)
		return nil
	})
	if err := sm.SyncAll(); err != nil {
		t.Fatalf("Failed retry: %v", err)
	}
	want := []ActionType{ActionDelete, ActionDelete, ActionRmdir}
	if len(actions) != len(want) || actions[1] != want[1] || actions[2] != want[2] {
		t.Fatalf("Expected retry then parent RMDIR %v, got %v", want, actions)
	}
}

func TestDeleteAfterQueuedRenameKeepsBothOperations(t *testing.T) {
	sm := NewStagingStateManager()
	if _, err := sm.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatalf("Failed to queue rename: %v", err)
	}
	if err := sm.Delete("/new.txt"); err != nil {
		t.Fatalf("Failed to queue delete: %v", err)
	}

	var actions []ActionType
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		actions = append(actions, meta.Action)
		return nil
	})
	if err := sm.SyncAll(); err != nil {
		t.Fatalf("Failed to sync DAG: %v", err)
	}
	want := []ActionType{ActionRename, ActionDelete}
	if len(actions) != len(want) || actions[0] != want[0] || actions[1] != want[1] {
		t.Fatalf("Expected %v, got %v", want, actions)
	}
}

func TestChainedFileRenamesRemainOrdered(t *testing.T) {
	sm := NewStagingStateManager()
	if _, err := sm.Rename("/a.txt", "/b.txt"); err != nil {
		t.Fatalf("Failed first rename: %v", err)
	}
	if _, err := sm.Rename("/b.txt", "/c.txt"); err != nil {
		t.Fatalf("Failed second rename: %v", err)
	}

	var moves [][2]string
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		moves = append(moves, [2]string{meta.OldPath, meta.Path})
		return nil
	})
	if err := sm.SyncAll(); err != nil {
		t.Fatalf("Failed to sync renames: %v", err)
	}
	want := [][2]string{{"/a.txt", "/b.txt"}, {"/b.txt", "/c.txt"}}
	if len(moves) != len(want) || moves[0] != want[0] || moves[1] != want[1] {
		t.Fatalf("Expected %v, got %v", want, moves)
	}
}

func TestChainedDirectoryRenamesRemainOrdered(t *testing.T) {
	sm := NewStagingStateManager()
	if _, err := sm.RenameDir("/a", "/b"); err != nil {
		t.Fatalf("Failed first directory rename: %v", err)
	}
	if _, err := sm.RenameDir("/b", "/c"); err != nil {
		t.Fatalf("Failed second directory rename: %v", err)
	}

	var moves [][2]string
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		moves = append(moves, [2]string{meta.OldPath, meta.Path})
		return nil
	})
	if err := sm.SyncAll(); err != nil {
		t.Fatalf("Failed to sync directory renames: %v", err)
	}
	want := [][2]string{{"/a", "/b"}, {"/b", "/c"}}
	if len(moves) != len(want) || moves[0] != want[0] || moves[1] != want[1] {
		t.Fatalf("Expected %v, got %v", want, moves)
	}
}

func TestRmdirAfterQueuedDirectoryRenameRemainsOrdered(t *testing.T) {
	sm := NewStagingStateManager()
	if _, err := sm.RenameDir("/old", "/new"); err != nil {
		t.Fatalf("Failed to queue directory rename: %v", err)
	}
	if _, err := sm.Rmdir("/new", false, true); err != nil {
		t.Fatalf("Failed to queue RMDIR: %v", err)
	}

	var actions []ActionType
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		actions = append(actions, meta.Action)
		return nil
	})
	if err := sm.SyncAll(); err != nil {
		t.Fatalf("Failed to sync DAG: %v", err)
	}
	want := []ActionType{ActionRenameDir, ActionRmdir}
	if len(actions) != len(want) || actions[0] != want[0] || actions[1] != want[1] {
		t.Fatalf("Expected %v, got %v", want, actions)
	}
}

func TestNewWorkBelowQueuedRenameDependsOnRename(t *testing.T) {
	sm := NewStagingStateManager()
	if _, err := sm.RenameDir("/old", "/new"); err != nil {
		t.Fatalf("Failed to queue directory rename: %v", err)
	}
	if err := sm.Modify("/new/file.txt"); err != nil {
		t.Fatalf("Failed to queue work below destination: %v", err)
	}

	var actions []ActionType
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		actions = append(actions, meta.Action)
		return nil
	})
	if err := sm.SyncAll(); err != nil {
		t.Fatalf("Failed to sync DAG: %v", err)
	}
	want := []ActionType{ActionRenameDir, ActionUpload}
	if len(actions) != len(want) || actions[0] != want[0] || actions[1] != want[1] {
		t.Fatalf("Expected %v, got %v", want, actions)
	}
}

func TestNeverSyncedFileRenameKeepsUploadNode(t *testing.T) {
	sm := NewStagingStateManager()
	if err := sm.Create("/old.txt"); err != nil {
		t.Fatalf("Failed to queue new file: %v", err)
	}
	if _, err := sm.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatalf("Failed to rename new file: %v", err)
	}

	var synced *StagingMetadata
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		copyMeta := *meta
		synced = &copyMeta
		return nil
	})
	if err := sm.SyncAll(); err != nil {
		t.Fatalf("Failed to sync renamed upload: %v", err)
	}
	if synced == nil || synced.Action != ActionUpload || synced.Path != "/new.txt" {
		t.Fatalf("Expected upload at new path, got %+v", synced)
	}
}

func TestLateDeleteBecomesParentRmdirDependency(t *testing.T) {
	sm := NewStagingStateManager()
	if _, err := sm.Rmdir("/dir", false, false); err != nil {
		t.Fatalf("Failed to queue parent RMDIR: %v", err)
	}
	if err := sm.Delete("/dir/file.txt"); err != nil {
		t.Fatalf("Failed to queue late child delete: %v", err)
	}

	var actions []ActionType
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		actions = append(actions, meta.Action)
		return nil
	})
	if err := sm.SyncAll(); err != nil {
		t.Fatalf("Failed to sync DAG: %v", err)
	}
	want := []ActionType{ActionDelete, ActionRmdir}
	if len(actions) != len(want) || actions[0] != want[0] || actions[1] != want[1] {
		t.Fatalf("Expected %v, got %v", want, actions)
	}
}
