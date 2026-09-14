package stagingfs

import "testing"

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
