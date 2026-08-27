package stagingfs

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dgraph-io/badger/v3"
)

// Handler for iRODS operations
type (
	ActionHandler func(metadata *StagingMetadata) error
)

type ActionType int

const (
	ActionUpload     ActionType = iota
	ActionBulkUpload            // bulk upload via UploadFile/UploadFileParallel; deleted immediately after sync (not cached)
	ActionRename
	ActionRenameDir
	ActionDelete
	ActionMkdir
	ActionRmdir
)

func (a ActionType) String() string {
	switch a {
	case ActionUpload:
		return "UPLOAD"
	case ActionBulkUpload:
		return "BULK_UPLOAD"
	case ActionRename:
		return "RENAME"
	case ActionRenameDir:
		return "RENAME_DIR"
	case ActionDelete:
		return "DELETE"
	case ActionMkdir:
		return "MKDIR"
	case ActionRmdir:
		return "RMDIR"
	default:
		return "UNKNOWN"
	}
}

// StagingFileState represents whether a local staging file is dirty (pending sync) or cached (already synced)
type StagingFileState int

const (
	// StagingFileDirty means the local file has pending changes that need to be synced to iRODS
	StagingFileDirty StagingFileState = iota
	// StagingFileCached means the local file has been synced and is kept as a read cache
	StagingFileCached
)

func (s StagingFileState) String() string {
	switch s {
	case StagingFileDirty:
		return "DIRTY"
	case StagingFileCached:
		return "CACHED"
	default:
		return "UNKNOWN"
	}
}

// StagingMetadata represents the state of a staged file
type StagingMetadata struct {
	Path           string           // Current path
	OldPath        string           // Old path (for RENAME actions)
	Action         ActionType       // Final action
	IsNew          bool             // Is this a new file?
	CreatedAt      time.Time        // Creation time
	LastModifiedAt time.Time        // Last modification time
	SyncFailCount  int              // Number of consecutive sync failures
	FileState      StagingFileState // Whether local file is dirty or cached
	LastAccessedAt time.Time        // Last time the cached file was accessed (for eviction)
}

// StagingStateManager manages staging metadata for async uploads
type StagingStateManager struct {
	metadata      map[string]*StagingMetadata
	lockedPaths   map[string]bool       // Paths locked during sync operations
	pathConds     map[string]*sync.Cond // Per-path condition variables
	db            *badger.DB
	mu            sync.RWMutex
	ActionHandler ActionHandler
}

// NewStagingStateManager creates a new manager (memory only)
func NewStagingStateManager() *StagingStateManager {
	return &StagingStateManager{
		metadata:    make(map[string]*StagingMetadata),
		lockedPaths: make(map[string]bool),
		pathConds:   make(map[string]*sync.Cond),
		db:          nil,
	}
}

// NewStagingStateManagerWithPersistence creates a new manager with Badger persistence
func NewStagingStateManagerWithPersistence(db *badger.DB) *StagingStateManager {
	return &StagingStateManager{
		metadata:    make(map[string]*StagingMetadata),
		lockedPaths: make(map[string]bool),
		pathConds:   make(map[string]*sync.Cond),
		db:          db,
	}
}

// Create marks a path as newly created
func (sm *StagingStateManager) Create(path string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Wait for path to be unlocked if it's locked during sync
	for sm.lockedPaths[path] {
		if sm.pathConds[path] == nil {
			sm.pathConds[path] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[path].Wait()
	}

	now := time.Now()
	meta := &StagingMetadata{
		Path:           path,
		Action:         ActionUpload,
		IsNew:          true,
		CreatedAt:      now,
		LastModifiedAt: now,
	}
	return sm.persistMetadata(path, meta)
}

// CreateBulkUpload registers a path for bulk upload (will be deleted after sync, not cached)
func (sm *StagingStateManager) CreateBulkUpload(path string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for sm.lockedPaths[path] {
		if sm.pathConds[path] == nil {
			sm.pathConds[path] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[path].Wait()
	}

	now := time.Now()
	meta := &StagingMetadata{
		Path:           path,
		Action:         ActionBulkUpload,
		IsNew:          true,
		CreatedAt:      now,
		LastModifiedAt: now,
	}
	return sm.persistMetadata(path, meta)
}

// Modify marks a path as modified
func (sm *StagingStateManager) Modify(path string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Wait for path to be unlocked if it's locked during sync
	for sm.lockedPaths[path] {
		if sm.pathConds[path] == nil {
			sm.pathConds[path] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[path].Wait()
	}

	meta, exists := sm.metadata[path]

	if exists && !sm.isValidAction(meta.Action, ActionUpload) {
		return errors.Newf("cannot modify %s: invalid action transition from %s to UPLOAD", path, meta.Action)
	}

	if !exists {
		// Existing file
		now := time.Now()
		meta = &StagingMetadata{
			Path:           path,
			IsNew:          false,
			Action:         ActionUpload,
			CreatedAt:      now,
			LastModifiedAt: now,
		}
	} else {
		meta.Action = ActionUpload
		meta.LastModifiedAt = time.Now()
	}

	return sm.persistMetadata(path, meta)
}

// Rename renames a file
// Returns true if immediate sync was performed (for existing files)
func (sm *StagingStateManager) Rename(oldPath, newPath string) (bool, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Wait for both paths to be unlocked if they're locked during sync
	for sm.lockedPaths[oldPath] || sm.lockedPaths[newPath] {
		if sm.pathConds[oldPath] == nil {
			sm.pathConds[oldPath] = sync.NewCond(&sm.mu)
		}
		if sm.pathConds[newPath] == nil {
			sm.pathConds[newPath] = sync.NewCond(&sm.mu)
		}
		// Wait on oldPath condition (will be signaled when either path is unlocked)
		sm.pathConds[oldPath].Wait()
	}

	meta, exists := sm.metadata[oldPath]

	if !exists {
		// Existing file without metadata → immediate RENAME
		// Lock both paths during sync
		sm.lockedPaths[oldPath] = true
		sm.lockedPaths[newPath] = true
		// Unlock after sync completes and signal waiting goroutines
		defer func() {
			delete(sm.lockedPaths, oldPath)
			delete(sm.lockedPaths, newPath)
			if sm.pathConds[oldPath] != nil {
				sm.pathConds[oldPath].Broadcast()
			}
			if sm.pathConds[newPath] != nil {
				sm.pathConds[newPath].Broadcast()
			}
		}()

		// Call handler immediately
		if sm.ActionHandler != nil {
			err := sm.ActionHandler(&StagingMetadata{
				Path:    newPath,
				OldPath: oldPath,
				Action:  ActionRename,
				IsNew:   false,
			})
			if err != nil {
				return false, errors.Wrap(err, "handler failed for immediate RENAME sync")
			}
		}
		return true, nil
	}

	// IsNew=false and Action=ActionUpload (modified)
	if !meta.IsNew && meta.Action == ActionUpload {
		// Perform preceding action (PUT) then RENAME
		// Lock both paths during sync
		sm.lockedPaths[oldPath] = true
		sm.lockedPaths[newPath] = true
		// Unlock after sync completes and signal waiting goroutines
		defer func() {
			delete(sm.lockedPaths, oldPath)
			delete(sm.lockedPaths, newPath)
			if sm.pathConds[oldPath] != nil {
				sm.pathConds[oldPath].Broadcast()
			}
			if sm.pathConds[newPath] != nil {
				sm.pathConds[newPath].Broadcast()
			}
		}()

		// Call handler for PUT and RENAME
		if sm.ActionHandler != nil {
			// First call PUT action
			err := sm.ActionHandler(&StagingMetadata{
				Path:   oldPath,
				Action: ActionUpload,
				IsNew:  false,
			})
			if err != nil {
				return false, errors.Wrap(err, "handler failed for PUT action before RENAME")
			}

			// Then call RENAME action
			err = sm.ActionHandler(&StagingMetadata{
				Path:    newPath,
				OldPath: oldPath,
				Action:  ActionRename,
				IsNew:   false,
			})
			if err != nil {
				return false, errors.Wrap(err, "handler failed for RENAME action")
			}
		}

		// Remove metadata
		if err := sm.deleteMetadata(oldPath); err != nil {
			return false, err
		}
		return true, nil
	}

	// IsNew=true case: change path only
	meta.Path = newPath
	meta.LastModifiedAt = time.Now()

	delete(sm.metadata, oldPath)
	sm.metadata[newPath] = meta

	if err := sm.deleteMetadata(oldPath); err != nil {
		return false, err
	}
	if err := sm.persistMetadata(newPath, meta); err != nil {
		return false, err
	}

	return false, nil
}

// RenameDir renames a directory
// Returns true if immediate sync was performed (for existing directories)
func (sm *StagingStateManager) RenameDir(oldPath, newPath string) (bool, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Wait for both paths to be unlocked if they're locked during sync
	for sm.lockedPaths[oldPath] || sm.lockedPaths[newPath] {
		if sm.pathConds[oldPath] == nil {
			sm.pathConds[oldPath] = sync.NewCond(&sm.mu)
		}
		if sm.pathConds[newPath] == nil {
			sm.pathConds[newPath] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[oldPath].Wait()
	}

	meta, exists := sm.metadata[oldPath]

	if !exists {
		// Existing directory without metadata → immediate RENAME
		sm.lockedPaths[oldPath] = true
		sm.lockedPaths[newPath] = true
		defer func() {
			delete(sm.lockedPaths, oldPath)
			delete(sm.lockedPaths, newPath)
			if sm.pathConds[oldPath] != nil {
				sm.pathConds[oldPath].Broadcast()
			}
			if sm.pathConds[newPath] != nil {
				sm.pathConds[newPath].Broadcast()
			}
		}()

		// Call handler immediately
		if sm.ActionHandler != nil {
			err := sm.ActionHandler(&StagingMetadata{
				Path:    newPath,
				OldPath: oldPath,
				Action:  ActionRenameDir,
				IsNew:   false,
			})
			if err != nil {
				return false, errors.Wrap(err, "handler failed for immediate RENAME_DIR sync")
			}
		}
		return true, nil
	}

	// IsNew=true case: change path and update all children
	now := time.Now()
	meta.Path = newPath
	meta.LastModifiedAt = now

	delete(sm.metadata, oldPath)
	sm.metadata[newPath] = meta

	if err := sm.deleteMetadata(oldPath); err != nil {
		return false, err
	}
	if err := sm.persistMetadata(newPath, meta); err != nil {
		return false, err
	}

	// Update all child entries under oldPath
	oldPrefix := oldPath + "/"
	for childPath, childMeta := range sm.metadata {
		if strings.HasPrefix(childPath, oldPrefix) {
			newChildPath := newPath + "/" + childPath[len(oldPrefix):]
			childMeta.Path = newChildPath
			childMeta.LastModifiedAt = now
			if childMeta.OldPath != "" && strings.HasPrefix(childMeta.OldPath, oldPrefix) {
				childMeta.OldPath = newPath + "/" + childMeta.OldPath[len(oldPrefix):]
			}

			delete(sm.metadata, childPath)
			sm.metadata[newChildPath] = childMeta

			sm.deleteMetadata(childPath)
			sm.persistMetadata(newChildPath, childMeta)
		}
	}

	return false, nil
}

// Delete marks a path as deleted (file deletion only)
func (sm *StagingStateManager) Delete(path string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Wait for path to be unlocked if it's locked during sync
	for sm.lockedPaths[path] {
		if sm.pathConds[path] == nil {
			sm.pathConds[path] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[path].Wait()
	}

	meta, exists := sm.metadata[path]

	if exists && !sm.isValidAction(meta.Action, ActionDelete) {
		return errors.Newf("cannot delete %s: invalid action transition from %s to DELETE", path, meta.Action)
	}

	if !exists {
		// Direct deletion of existing file
		now := time.Now()
		meta = &StagingMetadata{
			Path:           path,
			Action:         ActionDelete,
			IsNew:          false,
			CreatedAt:      now,
			LastModifiedAt: now,
		}
		sm.metadata[path] = meta
	} else if meta.IsNew {
		// CREATE → DELETE: remove metadata
		if err := sm.deleteMetadata(path); err != nil {
			return err
		}
		return nil
	} else {
		meta.Action = ActionDelete
		meta.LastModifiedAt = time.Now()
	}

	return sm.persistMetadata(path, meta)
}

// Mkdir marks a directory as created
func (sm *StagingStateManager) Mkdir(path string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Wait for path to be unlocked if it's locked during sync
	for sm.lockedPaths[path] {
		if sm.pathConds[path] == nil {
			sm.pathConds[path] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[path].Wait()
	}

	now := time.Now()
	meta := &StagingMetadata{
		Path:           path,
		Action:         ActionMkdir,
		IsNew:          true,
		CreatedAt:      now,
		LastModifiedAt: now,
	}
	return sm.persistMetadata(path, meta)
}

// Rmdir marks a directory as removed
// Returns true if immediate sync was performed (for existing directories)
func (sm *StagingStateManager) Rmdir(path string) (bool, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Wait for path to be unlocked if it's locked during sync
	for sm.lockedPaths[path] {
		if sm.pathConds[path] == nil {
			sm.pathConds[path] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[path].Wait()
	}

	meta, exists := sm.metadata[path]

	if exists && !sm.isValidAction(meta.Action, ActionRmdir) {
		return false, errors.Newf("cannot remove directory %s: invalid action transition from %s to RMDIR", path, meta.Action)
	}

	if !exists {
		// Existing directory without metadata → immediate RMDIR
		sm.lockedPaths[path] = true
		defer func() {
			delete(sm.lockedPaths, path)
			if sm.pathConds[path] != nil {
				sm.pathConds[path].Broadcast()
			}
		}()

		// Call handler immediately
		if sm.ActionHandler != nil {
			err := sm.ActionHandler(&StagingMetadata{
				Path:           path,
				Action:         ActionRmdir,
				IsNew:          false,
				CreatedAt:      time.Now(),
				LastModifiedAt: time.Now(),
			})
			if err != nil {
				return false, errors.Wrap(err, "handler failed for immediate RMDIR sync")
			}
		}

		// Recursive removal of the collection also removes every data object and
		// subcollection below it. Drop their deferred staging actions so the
		// background worker does not try to delete or upload paths that no longer
		// exist in iRODS.
		if err := sm.deleteMetadataTree(path); err != nil {
			return false, err
		}
		return true, nil
	}

	if meta.IsNew {
		// MKDIR → RMDIR: remove the directory and all locally-created children.
		if err := sm.deleteMetadataTree(path); err != nil {
			return false, err
		}
		return false, nil
	}

	// Existing directory with MKDIR metadata → immediate RMDIR
	sm.lockedPaths[path] = true
	defer func() {
		delete(sm.lockedPaths, path)
		if sm.pathConds[path] != nil {
			sm.pathConds[path].Broadcast()
		}
	}()

	// Call handler immediately
	if sm.ActionHandler != nil {
		err := sm.ActionHandler(&StagingMetadata{
			Path:           path,
			Action:         ActionRmdir,
			IsNew:          false,
			CreatedAt:      meta.CreatedAt,
			LastModifiedAt: time.Now(),
		})
		if err != nil {
			return false, errors.Wrap(err, "handler failed for immediate RMDIR sync")
		}
	}

	// Recursive RMDIR already handled every descendant in iRODS.
	if err := sm.deleteMetadataTree(path); err != nil {
		return false, err
	}
	return true, nil
}

// Get retrieves metadata for a path
func (sm *StagingStateManager) Get(path string) *StagingMetadata {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.metadata[path]
}

// GetAll returns deep copies of all staged metadata.
// Deep copies prevent concurrent mutations (via Delete, Rename, etc.) from affecting
// the caller's snapshot — a necessary guarantee for the background sync worker.
func (sm *StagingStateManager) GetAll() map[string]*StagingMetadata {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make(map[string]*StagingMetadata)
	for k, v := range sm.metadata {
		copy := *v
		result[k] = &copy
	}
	return result
}

// syncOne performs handler call and removes metadata for a single path with internal locking
// Acquires and releases lock for the path
func (sm *StagingStateManager) syncOne(meta *StagingMetadata) error {
	// Wait for path to be unlocked if it's locked by another sync operation
	sm.mu.Lock()
	for sm.lockedPaths[meta.Path] {
		if sm.pathConds[meta.Path] == nil {
			sm.pathConds[meta.Path] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[meta.Path].Wait()
	}

	// GetAll/SyncOld operate on snapshots. A recursive RMDIR may have removed
	// this child while the snapshot was waiting to be processed. In that case
	// the requested backend operation was already covered by the directory
	// removal and must not be replayed.
	current := sm.metadata[meta.Path]
	if current == nil || current.LastModifiedAt.After(meta.LastModifiedAt) {
		sm.mu.Unlock()
		return nil
	}

	// Lock the path for this sync
	sm.lockedPaths[meta.Path] = true
	if sm.pathConds[meta.Path] == nil {
		sm.pathConds[meta.Path] = sync.NewCond(&sm.mu)
	}
	sm.mu.Unlock()

	// Call handler without lock (handler may take time)
	if sm.ActionHandler != nil {
		if err := sm.ActionHandler(meta); err != nil {
			// Unlock on handler error
			sm.mu.Lock()
			delete(sm.lockedPaths, meta.Path)
			if sm.pathConds[meta.Path] != nil {
				sm.pathConds[meta.Path].Broadcast()
			}
			sm.mu.Unlock()
			return errors.Wrapf(err, "handler failed for %s action on %s", meta.Action, meta.Path)
		}
	}

	// Remove from metadata with lock, but only if it hasn't been replaced by a newer
	// operation (e.g. OpenForWrite called between our snapshot and now).
	// We detect replacement by comparing LastModifiedAt: a concurrent Create/Modify
	// always sets a strictly later timestamp, so if the live entry is newer we leave it
	// for the next sync cycle rather than deleting the fresh registration.
	sm.mu.Lock()

	current = sm.metadata[meta.Path]
	var deleteErr error
	if current != nil && !current.LastModifiedAt.After(meta.LastModifiedAt) {
		deleteErr = sm.deleteMetadata(meta.Path)
	}
	// current==nil: already removed externally (e.g. DELETE on a new file) — nothing to do.
	// current.LastModifiedAt > meta.LastModifiedAt: newer entry registered during upload
	// — leave it; next sync cycle will handle it.

	// Always unlock path and signal, even on error.
	delete(sm.lockedPaths, meta.Path)
	if sm.pathConds[meta.Path] != nil {
		sm.pathConds[meta.Path].Broadcast()
	}
	sm.mu.Unlock()

	return deleteErr
}

// SyncAll performs all pending iRODS operations and clears metadata one by one (exclusive lock)
func (sm *StagingStateManager) SyncAll() error {
	for {
		sm.mu.Lock()

		// Get next unprocessed item
		var meta *StagingMetadata
		for _, m := range sm.metadata {
			meta = m
			break
		}

		// If no more items, we're done
		if meta == nil {
			sm.mu.Unlock()
			break
		}

		sm.mu.Unlock()

		// Process this item (syncOne handles locking/unlocking and waiting)
		if err := sm.syncOne(meta); err != nil {
			return err
		}
	}

	return nil
}

// SyncOld performs sync on items older than gracePeriod (10 seconds) with per-path locking
func (sm *StagingStateManager) SyncOld(gracePeriod time.Duration) error {
	sm.mu.Lock()

	// Find items older than grace period — take deep copies so concurrent mutations
	// (e.g. Rename mutating Path, Delete mutating Action) don't affect our snapshot.
	now := time.Now()
	var metasToSync []*StagingMetadata
	for _, meta := range sm.metadata {
		if now.Sub(meta.LastModifiedAt) >= gracePeriod {
			copy := *meta
			metasToSync = append(metasToSync, &copy)
		}
	}

	sm.mu.Unlock()

	// Execute handlers for old items (syncOne handles locking/waiting/unlocking)
	for _, meta := range metasToSync {
		if err := sm.syncOne(meta); err != nil {
			return err
		}
	}

	return nil
}

// Clear removes all metadata
func (sm *StagingStateManager) Clear() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.metadata = make(map[string]*StagingMetadata)

	if sm.db != nil {
		return sm.db.Update(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte("staging:")
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				if err := txn.Delete(it.Item().Key()); err != nil {
					return err
				}
			}
			return nil
		})
	}

	return nil
}

// Restore restores metadata from Badger (crash recovery)
func (sm *StagingStateManager) Restore() error {
	if sm.db == nil {
		return nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("staging:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var meta StagingMetadata

			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &meta)
			}); err != nil {
				return errors.Wrap(err, "failed to unmarshal staging metadata")
			}

			sm.metadata[meta.Path] = &meta
		}

		return nil
	})
}

// persistMetadata saves metadata to memory and Badger
func (sm *StagingStateManager) persistMetadata(path string, meta *StagingMetadata) error {
	sm.metadata[path] = meta

	if sm.db == nil {
		return nil
	}

	key := []byte(fmt.Sprintf("staging:%s", path))
	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Wrap(err, "failed to marshal staging metadata")
	}

	return sm.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

// deleteMetadata removes metadata from memory and Badger (caller must hold mu)
func (sm *StagingStateManager) deleteMetadata(path string) error {
	delete(sm.metadata, path)

	if sm.db == nil {
		return nil
	}

	key := []byte(fmt.Sprintf("staging:%s", path))
	return sm.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// deleteMetadataTree removes metadata for root and all of its descendants.
// The caller must hold sm.mu.
func (sm *StagingStateManager) deleteMetadataTree(root string) error {
	prefix := strings.TrimRight(root, "/") + "/"
	for metadataPath := range sm.metadata {
		if metadataPath == root || strings.HasPrefix(metadataPath, prefix) {
			if err := sm.deleteMetadata(metadataPath); err != nil {
				return errors.Wrapf(err, "failed to delete staging metadata for %s", metadataPath)
			}
		}
	}
	return nil
}

// deleteMetadataPublic removes metadata with locking (for external callers)
func (sm *StagingStateManager) deleteMetadataPublic(path string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.deleteMetadata(path)
}

// WaitForSync blocks until the given path is no longer being synced.
func (sm *StagingStateManager) WaitForSync(path string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for sm.lockedPaths[path] {
		if sm.pathConds[path] == nil {
			sm.pathConds[path] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[path].Wait()
	}
}

// RegisterActionHandler registers a handler for operations
func (sm *StagingStateManager) RegisterActionHandler(handler ActionHandler) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.ActionHandler = handler
}

// isValidAction checks if transition from current action to new action is valid
// ActionUpload → Upload/Delete/Rename
// ActionDelete → Upload only (DELETE → CREATE)
// ActionMkdir → Mkdir/Rmdir/RenameDir
// ActionRmdir → Mkdir only (RMDIR → MKDIR)
// ActionRename → no transitions allowed (terminal state)
// ActionRenameDir → no transitions allowed (terminal state)
func (sm *StagingStateManager) isValidAction(currentAction ActionType, newAction ActionType) bool {
	switch currentAction {
	case ActionUpload:
		// Can transition to Upload, Delete, or Rename
		return newAction == ActionUpload || newAction == ActionDelete || newAction == ActionRename
	case ActionDelete:
		// Only Upload is allowed (DELETE → CREATE)
		return newAction == ActionUpload
	case ActionMkdir:
		// Can transition to Mkdir, Rmdir, or RenameDir
		return newAction == ActionMkdir || newAction == ActionRmdir || newAction == ActionRenameDir
	case ActionRmdir:
		// Only Mkdir is allowed (RMDIR → MKDIR)
		return newAction == ActionMkdir
	case ActionRename:
		// Terminal state - no further operations allowed
		return false
	case ActionRenameDir:
		// Terminal state - no further operations allowed
		return false
	default:
		return true
	}
}
