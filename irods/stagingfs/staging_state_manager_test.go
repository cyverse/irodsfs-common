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
