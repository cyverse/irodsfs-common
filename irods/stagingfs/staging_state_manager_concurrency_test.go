package stagingfs

import (
	"errors"
	"testing"
	"time"
)

func TestNonRecursiveRmdirDrainsDeleteQueuedBehindUpload(t *testing.T) {
	sm := NewStagingStateManager()
	childPath := "/dir/file.txt"
	if err := sm.Create(childPath); err != nil {
		t.Fatalf("Failed to stage child: %v", err)
	}
	uploadMeta := *sm.Get(childPath)

	uploadStarted := make(chan struct{})
	releaseUpload := make(chan struct{})
	var actions []ActionType
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		actions = append(actions, meta.Action)
		if meta.Action == ActionUpload {
			close(uploadStarted)
			<-releaseUpload
		}
		return nil
	})

	uploadDone := make(chan error, 1)
	go func() {
		uploadDone <- sm.syncOne(&uploadMeta)
	}()
	select {
	case <-uploadStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Child upload did not start")
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- sm.DeleteWithForce(childPath, false)
	}()
	close(releaseUpload)
	if err := <-uploadDone; err != nil {
		t.Fatalf("Child upload failed: %v", err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatalf("Child delete failed: %v", err)
	}
	if meta := sm.Get(childPath); meta == nil || meta.Action != ActionDelete {
		t.Fatalf("Expected pending DELETE after upload, got %+v", meta)
	}

	if _, err := sm.Rmdir("/dir", false, false); err != nil {
		t.Fatalf("Non-recursive Rmdir failed: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("Expected RMDIR and DELETE to remain deferred, got %v", actions)
	}
	if err := sm.SyncAll(); err != nil {
		t.Fatalf("Failed to sync queued removals: %v", err)
	}
	want := []ActionType{ActionUpload, ActionDelete, ActionRmdir}
	if len(actions) != len(want) {
		t.Fatalf("Expected actions %v, got %v", want, actions)
	}
	for i := range want {
		if actions[i] != want[i] {
			t.Fatalf("Expected actions %v, got %v", want, actions)
		}
	}
	if meta := sm.Get(childPath); meta != nil {
		t.Fatalf("Expected child metadata to be removed, got %+v", meta)
	}
}

func TestNonRecursiveRmdirCancelsPendingNewChild(t *testing.T) {
	sm := NewStagingStateManager()
	if err := sm.Create("/dir/file.txt"); err != nil {
		t.Fatalf("Failed to stage child: %v", err)
	}

	handlerCalled := false
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		handlerCalled = true
		return nil
	})
	if _, err := sm.Rmdir("/dir", false, false); err != nil {
		t.Fatalf("Failed to queue RMDIR: %v", err)
	}
	if handlerCalled {
		t.Fatal("Backend handler must not run while a non-delete child remains")
	}
	if meta := sm.Get("/dir/file.txt"); meta != nil {
		t.Fatalf("Expected pending child upload to be canceled, got %+v", meta)
	}
	if meta := sm.Get("/dir"); meta == nil || meta.Action != ActionRmdir {
		t.Fatalf("Expected queued RMDIR, got %+v", meta)
	}
}

func TestRmdirExistingDirectoryQueuesBackendRemoval(t *testing.T) {
	sm := NewStagingStateManager()

	var captured *StagingMetadata
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		copy := *meta
		captured = &copy
		return nil
	})

	synced, err := sm.Rmdir("/dir", false, false)
	if err != nil {
		t.Fatalf("Rmdir failed: %v", err)
	}
	if synced {
		t.Fatal("RMDIR must not sync inline")
	}
	if captured != nil {
		t.Fatalf("Backend handler ran before sync: %+v", captured)
	}
	queued := sm.Get("/dir")
	if queued == nil || queued.Action != ActionRmdir || queued.Recurse || queued.Force {
		t.Fatalf("Expected queued RMDIR preserving options, got %+v", queued)
	}
	if err := sm.SyncAll(); err != nil {
		t.Fatalf("Failed to sync RMDIR: %v", err)
	}
	if captured == nil || captured.Action != ActionRmdir || captured.Path != "/dir" {
		t.Fatalf("Expected backend RMDIR for /dir, got %+v", captured)
	}
}

func TestRmdirFailureRemainsQueuedForRetry(t *testing.T) {
	sm := NewStagingStateManager()
	wantErr := errors.New("backend removal failed")
	sm.RegisterActionHandler(func(meta *StagingMetadata) error { return wantErr })

	if _, err := sm.Rmdir("/dir", false, false); err != nil {
		t.Fatalf("Failed to queue RMDIR: %v", err)
	}
	if err := sm.SyncAll(); !errors.Is(err, wantErr) {
		t.Fatalf("Expected backend error, got %v", err)
	}
	if meta := sm.Get("/dir"); meta == nil || meta.Action != ActionRmdir {
		t.Fatalf("Expected failed RMDIR to remain queued, got %+v", meta)
	}
}

func TestRmdirSubtreeGetsPriorityAndSyncsDeepestFirst(t *testing.T) {
	sm := NewStagingStateManager()
	if err := sm.Delete("/dir/sub/file.txt"); err != nil {
		t.Fatalf("Failed to queue child delete: %v", err)
	}
	if _, err := sm.Rmdir("/dir/sub", false, false); err != nil {
		t.Fatalf("Failed to queue child RMDIR: %v", err)
	}
	if _, err := sm.Rmdir("/dir", false, false); err != nil {
		t.Fatalf("Failed to queue parent RMDIR: %v", err)
	}
	if err := sm.Create("/unrelated.txt"); err != nil {
		t.Fatalf("Failed to queue unrelated upload: %v", err)
	}

	var actions []string
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		actions = append(actions, meta.Action.String()+":"+meta.Path)
		return nil
	})
	if err := sm.SyncOld(time.Hour); err != nil {
		t.Fatalf("Failed priority sync: %v", err)
	}

	want := []string{
		"DELETE:/dir/sub/file.txt",
		"RMDIR:/dir/sub",
		"RMDIR:/dir",
	}
	if len(actions) != len(want) {
		t.Fatalf("Expected %v, got %v", want, actions)
	}
	for i := range want {
		if actions[i] != want[i] {
			t.Fatalf("Expected %v, got %v", want, actions)
		}
	}
	if meta := sm.Get("/unrelated.txt"); meta == nil || meta.Action != ActionUpload {
		t.Fatalf("Expected unrelated upload to retain its grace period, got %+v", meta)
	}
}

func TestRenameDirRunsBeforeMovedDescendantWork(t *testing.T) {
	sm := NewStagingStateManager()
	if err := sm.Modify("/old/file.txt"); err != nil {
		t.Fatalf("Failed to queue existing-file upload: %v", err)
	}
	if _, err := sm.RenameDir("/old", "/new"); err != nil {
		t.Fatalf("Failed to queue directory rename: %v", err)
	}

	var actions []string
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		actions = append(actions, meta.Action.String()+":"+meta.Path)
		return nil
	})
	if err := sm.SyncOld(time.Hour); err != nil {
		t.Fatalf("Failed priority sync: %v", err)
	}
	want := []string{"RENAME_DIR:/new", "UPLOAD:/new/file.txt"}
	if len(actions) != len(want) || actions[0] != want[0] || actions[1] != want[1] {
		t.Fatalf("Expected %v, got %v", want, actions)
	}
}

func TestModifiedFileRenameQueuesRenameThenUpload(t *testing.T) {
	sm := NewStagingStateManager()
	if err := sm.Modify("/old.txt"); err != nil {
		t.Fatalf("Failed to queue modification: %v", err)
	}
	if _, err := sm.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatalf("Failed to queue rename: %v", err)
	}

	var actions []string
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		actions = append(actions, meta.Action.String()+":"+meta.Path)
		return nil
	})
	if err := sm.SyncAll(); err != nil {
		t.Fatalf("Failed to sync rename and upload: %v", err)
	}
	want := []string{"RENAME:/new.txt", "UPLOAD:/new.txt"}
	if len(actions) != len(want) || actions[0] != want[0] || actions[1] != want[1] {
		t.Fatalf("Expected %v, got %v", want, actions)
	}
}

func TestRecursiveRmdirWaitsForInFlightChildSync(t *testing.T) {
	sm := NewStagingStateManager()
	firstPath := "/dir/first.txt"
	secondPath := "/dir/second.txt"
	if err := sm.Create(firstPath); err != nil {
		t.Fatalf("Failed to stage first child: %v", err)
	}
	if err := sm.Create(secondPath); err != nil {
		t.Fatalf("Failed to stage second child: %v", err)
	}
	firstMeta := *sm.Get(firstPath)
	secondMeta := *sm.Get(secondPath)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	rmdirStarted := make(chan struct{})
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		switch {
		case meta.Action == ActionUpload && meta.Path == firstPath:
			close(firstStarted)
			<-releaseFirst
		case meta.Action == ActionUpload && meta.Path == secondPath:
			close(secondStarted)
		case meta.Action == ActionRmdir:
			close(rmdirStarted)
		}
		return nil
	})

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- sm.syncOne(&firstMeta)
	}()
	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("First child sync did not start")
	}

	rmdirDone := make(chan error, 1)
	go func() {
		_, err := sm.Rmdir("/dir", true, false)
		rmdirDone <- err
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		sm.mu.RLock()
		locked := sm.lockedSubtrees["/dir"]
		sm.mu.RUnlock()
		if locked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Recursive Rmdir did not lock the subtree")
		}
		time.Sleep(time.Millisecond)
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- sm.syncOne(&secondMeta)
	}()

	select {
	case <-rmdirStarted:
		t.Fatal("Rmdir reached the backend before the in-flight child completed")
	case <-secondStarted:
		t.Fatal("A new child sync started while recursive Rmdir held the subtree lock")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("First child sync failed: %v", err)
	}
	if err := <-rmdirDone; err != nil {
		t.Fatalf("Recursive Rmdir failed: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("Canceled second child sync failed: %v", err)
	}

	select {
	case <-rmdirStarted:
		t.Fatal("Rmdir backend handler ran inline")
	default:
	}
	select {
	case <-secondStarted:
		t.Fatal("Canceled second child sync reached the backend")
	default:
	}
	if meta := sm.Get(secondPath); meta != nil {
		t.Fatalf("Expected pending child metadata to be removed, got %+v", meta)
	}
	if err := sm.SyncAll(); err != nil {
		t.Fatalf("Failed to sync queued RMDIR: %v", err)
	}
	select {
	case <-rmdirStarted:
	default:
		t.Fatal("Rmdir backend handler was not called during sync")
	}
}
