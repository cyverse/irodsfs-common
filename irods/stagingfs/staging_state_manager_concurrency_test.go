package stagingfs

import (
	"testing"
	"time"

	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
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

func TestNonRecursiveRmdirRejectsPendingNonDeleteChild(t *testing.T) {
	sm := NewStagingStateManager()
	if err := sm.Create("/dir/file.txt"); err != nil {
		t.Fatalf("Failed to stage child: %v", err)
	}

	handlerCalled := false
	sm.RegisterActionHandler(func(meta *StagingMetadata) error {
		handlerCalled = true
		return nil
	})
	_, err := sm.Rmdir("/dir", false, false)
	if !irodsclient_types.IsCollectionNotEmptyError(err) {
		t.Fatalf("Expected collection-not-empty error, got %v", err)
	}
	if handlerCalled {
		t.Fatal("Backend handler must not run while a non-delete child remains")
	}
	if meta := sm.Get("/dir/file.txt"); meta == nil || meta.Action != ActionUpload {
		t.Fatalf("Expected pending child upload to remain, got %+v", meta)
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
	default:
		t.Fatal("Rmdir backend handler was not called")
	}
	select {
	case <-secondStarted:
		t.Fatal("Canceled second child sync reached the backend")
	default:
	}
	if meta := sm.Get(secondPath); meta != nil {
		t.Fatalf("Expected pending child metadata to be removed, got %+v", meta)
	}
}
