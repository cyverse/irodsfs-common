package inode

import (
	"testing"
)

func TestRenameStagingEntryPreservesInodeID(t *testing.T) {
	for _, testCase := range inodeManagerTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			manager := testCase.create(t)
			defer manager.Close()

			oldID := mustCreateStagingInode(t, manager, "/old/file.txt")
			destinationID := mustCreateStagingInode(t, manager, "/new/file.txt")

			if err := manager.RenameStagingEntry("/old/file.txt", "/new/file.txt"); err != nil {
				t.Fatalf("failed to rename staging entry: %v", err)
			}

			if _, ok := manager.GetInodeIDForStagingEntry("/old/file.txt"); ok {
				t.Fatal("old staging path still has an inode ID")
			}
			newID, ok := manager.GetInodeIDForStagingEntry("/new/file.txt")
			if !ok {
				t.Fatal("new staging path has no inode ID")
			}
			if newID != oldID {
				t.Fatalf("inode ID changed across rename: before %d, after %d", oldID, newID)
			}

			newPath, ok, err := manager.GetPathForStagingEntryID(oldID)
			if err != nil {
				t.Fatalf("failed to reverse lookup renamed inode ID: %v", err)
			}
			if !ok || newPath != "/new/file.txt" {
				t.Fatalf("expected renamed path %q, got %q (found: %t)", "/new/file.txt", newPath, ok)
			}
			if _, ok, err := manager.GetPathForStagingEntryID(destinationID); err != nil || ok {
				t.Fatalf("replaced destination inode ID still exists: found %t, err %v", ok, err)
			}
		})
	}
}

func TestRenameStagingEntryTreePreservesAllInodeIDs(t *testing.T) {
	for _, testCase := range inodeManagerTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			manager := testCase.create(t)
			defer manager.Close()

			paths := []string{
				"/old",
				"/old/file.txt",
				"/old/subdir",
				"/old/subdir/nested.txt",
			}
			issuedIDs := map[string]uint64{}
			for _, entryPath := range paths {
				issuedIDs[entryPath] = mustCreateStagingInode(t, manager, entryPath)
			}
			unrelatedID := mustCreateStagingInode(t, manager, "/old-sibling/file.txt")
			replacedID := mustCreateStagingInode(t, manager, "/new/replaced.txt")

			if err := manager.RenameStagingEntryTree("/old", "/new"); err != nil {
				t.Fatalf("failed to rename staging entry tree: %v", err)
			}

			for oldPath, issuedID := range issuedIDs {
				if _, ok := manager.GetInodeIDForStagingEntry(oldPath); ok {
					t.Fatalf("old staging path %q still has an inode ID", oldPath)
				}
				newPath := "/new" + oldPath[len("/old"):]
				newID, ok := manager.GetInodeIDForStagingEntry(newPath)
				if !ok {
					t.Fatalf("new staging path %q has no inode ID", newPath)
				}
				if newID != issuedID {
					t.Fatalf("inode ID for %q changed: before %d, after %d", newPath, issuedID, newID)
				}

				reversePath, ok, err := manager.GetPathForStagingEntryID(issuedID)
				if err != nil {
					t.Fatalf("failed to reverse lookup inode ID %d: %v", issuedID, err)
				}
				if !ok || reversePath != newPath {
					t.Fatalf("expected reverse path %q, got %q (found: %t)", newPath, reversePath, ok)
				}
			}

			if id, ok := manager.GetInodeIDForStagingEntry("/old-sibling/file.txt"); !ok || id != unrelatedID {
				t.Fatalf("unrelated inode mapping changed: got %d (found: %t)", id, ok)
			}
			if _, ok, err := manager.GetPathForStagingEntryID(replacedID); err != nil || ok {
				t.Fatalf("destination subtree inode ID still exists: found %t, err %v", ok, err)
			}
		})
	}
}

type inodeManagerTestCase struct {
	name   string
	create func(t *testing.T) *InodeManager
}

func inodeManagerTestCases() []inodeManagerTestCase {
	return []inodeManagerTestCase{
		{
			name: "memory",
			create: func(t *testing.T) *InodeManager {
				return NewInodeManager()
			},
		},
		{
			name: "badger",
			create: func(t *testing.T) *InodeManager {
				manager, err := NewInodeManagerWithPersistence(t.TempDir())
				if err != nil {
					t.Fatalf("failed to create persistent inode manager: %v", err)
				}
				return manager
			},
		},
	}
}

func mustCreateStagingInode(t *testing.T, manager *InodeManager, entryPath string) uint64 {
	t.Helper()
	inodeID, err := manager.CreateOrGetInodeIDForStagingEntry(entryPath)
	if err != nil {
		t.Fatalf("failed to create staging inode for %q: %v", entryPath, err)
	}
	return inodeID
}
