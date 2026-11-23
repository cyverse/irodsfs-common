package stagingfs

import (
	"errors"
	"sync"
	"testing"
)

type bestEffortHandler struct {
	mu     sync.Mutex
	failOn map[string]error
	ran    []string
}

func (h *bestEffortHandler) handle(meta *StagingMetadata) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.failOn[meta.Path]; err != nil {
		return err
	}
	h.ran = append(h.ran, meta.Action.String()+" "+meta.Path)
	return nil
}

func (h *bestEffortHandler) ranAction(action string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.ran {
		if r == action {
			return true
		}
	}
	return false
}

// One operation that fails must not keep the others from reaching iRODS.
func TestSyncAllRunsIndependentOperationsPastAFailure(t *testing.T) {
	sm := NewStagingStateManager()
	wantErr := errors.New("permission denied")
	h := &bestEffortHandler{failOn: map[string]error{"/bad": wantErr}}
	sm.RegisterActionHandler(h.handle)

	for _, p := range []string{"/a", "/bad", "/b", "/c"} {
		if err := sm.Delete(p); err != nil {
			t.Fatalf("Delete %s: %v", p, err)
		}
	}

	err := sm.SyncAll()

	var partial *PartialSyncError
	if !errors.As(err, &partial) {
		t.Fatalf("SyncAll = %v, want a *PartialSyncError", err)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("SyncAll = %v, want it to wrap the failed operation's error", err)
	}
	if partial.Remaining != 1 || len(partial.Failures) != 1 || partial.Failures[0].Path != "/bad" {
		t.Fatalf("partial = %+v, want one failure on /bad and nothing else left", partial)
	}
	for _, p := range []string{"/a", "/b", "/c"} {
		if !h.ranAction("DELETE " + p) {
			t.Fatalf("DELETE %s did not run; ran = %v", p, h.ran)
		}
	}

	// The failed operation is kept for a later sync, not blocked for good.
	meta := sm.Get("/bad")
	if meta == nil || meta.SyncFailCount != 1 {
		t.Fatalf("metadata at /bad = %+v, want it queued with one failure", meta)
	}
	if op := sm.dag.get(meta.OperationID); op == nil || op.State == OperationBlocked {
		t.Fatalf("the failed operation is %+v, want it retryable", op)
	}

	// Once the cause is gone, the next sync finishes it.
	h.mu.Lock()
	h.failOn = nil
	h.mu.Unlock()
	if err := sm.SyncAll(); err != nil {
		t.Fatalf("SyncAll after the failure cleared = %v", err)
	}
	if sm.Get("/bad") != nil {
		t.Fatal("the retried operation is still queued")
	}
}

// An operation that waits on a failed one must not run ahead of it.
func TestSyncAllLeavesDependentsOfAFailedOperationQueued(t *testing.T) {
	sm := NewStagingStateManager()
	wantErr := errors.New("backend unavailable")
	h := &bestEffortHandler{failOn: map[string]error{}}
	sm.RegisterActionHandler(h.handle)

	// A file is removed and a directory of the same name is created: the MKDIR
	// waits for the DELETE.
	if err := sm.Delete("/x"); err != nil {
		t.Fatal(err)
	}
	if err := sm.Mkdir("/x"); err != nil {
		t.Fatal(err)
	}
	if err := sm.Delete("/other"); err != nil {
		t.Fatal(err)
	}

	h.mu.Lock()
	h.failOn["/x"] = wantErr
	h.mu.Unlock()

	err := sm.SyncAll()

	var partial *PartialSyncError
	if !errors.As(err, &partial) {
		t.Fatalf("SyncAll = %v, want a *PartialSyncError", err)
	}
	if partial.Remaining != 2 || len(partial.Failures) != 1 {
		t.Fatalf("partial = %+v, want the failed DELETE and the MKDIR behind it left", partial)
	}
	if h.ranAction("MKDIR /x") {
		t.Fatal("the MKDIR ran ahead of the DELETE it waits for")
	}
	if !h.ranAction("DELETE /other") {
		t.Fatal("the unrelated operation did not run")
	}
}
