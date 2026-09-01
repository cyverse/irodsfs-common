package stagingfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
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
	OperationID     string           // ID of the latest DAG operation for this logical path
	Path            string           // Current path
	OldPath         string           // Old path (for RENAME actions)
	Action          ActionType       // Final action
	Recurse         bool             // Recursive directory removal
	Force           bool             // Force file or directory removal
	IsNew           bool             // Is this a new file?
	CreatedAt       time.Time        // Creation time
	LastModifiedAt  time.Time        // Last modification time
	SyncFailCount   int              // Number of consecutive sync failures
	BackendMayExist bool             // A completed descendant operation may have created this directory remotely
	FileState       StagingFileState // Whether local file is dirty or cached
	LastAccessedAt  time.Time        // Last time the cached file was accessed (for eviction)
}

// StagingStateManager manages staging metadata for async uploads
type StagingStateManager struct {
	metadata       map[string]*StagingMetadata
	dag            *OperationDAG
	lockedPaths    map[string]bool       // Paths locked during sync operations
	lockedSubtrees map[string]bool       // Directory trees locked during recursive operations
	pathConds      map[string]*sync.Cond // Per-path condition variables
	db             *badger.DB
	mu             sync.RWMutex
	ActionHandler  ActionHandler
}

// NewStagingStateManager creates a new manager (memory only)
func NewStagingStateManager() *StagingStateManager {
	return &StagingStateManager{
		metadata:       make(map[string]*StagingMetadata),
		dag:            newOperationDAG(),
		lockedPaths:    make(map[string]bool),
		lockedSubtrees: make(map[string]bool),
		pathConds:      make(map[string]*sync.Cond),
		db:             nil,
	}
}

// NewStagingStateManagerWithPersistence creates a new manager with Badger persistence
func NewStagingStateManagerWithPersistence(db *badger.DB) *StagingStateManager {
	return &StagingStateManager{
		metadata:       make(map[string]*StagingMetadata),
		dag:            newOperationDAG(),
		lockedPaths:    make(map[string]bool),
		lockedSubtrees: make(map[string]bool),
		pathConds:      make(map[string]*sync.Cond),
		db:             db,
	}
}

// pathInSubtree reports whether path is root itself or one of its descendants.
func pathInSubtree(path string, root string) bool {
	cleanRoot := strings.TrimRight(root, "/")
	if cleanRoot == "" {
		return strings.HasPrefix(path, "/")
	}
	return path == cleanRoot || strings.HasPrefix(path, cleanRoot+"/")
}

// blockingLock returns the condition key for a lock preventing work on path.
// The caller must hold sm.mu.
func (sm *StagingStateManager) blockingLock(path string) string {
	if sm.lockedPaths[path] {
		return path
	}
	for root := range sm.lockedSubtrees {
		if pathInSubtree(path, root) {
			return root
		}
	}
	return ""
}

// waitForPathsUnlocked waits until no exact-path or ancestor subtree lock blocks
// any of paths. The caller must hold sm.mu.
func (sm *StagingStateManager) waitForPathsUnlocked(paths ...string) {
	for {
		blocker := ""
		for _, path := range paths {
			if blocker = sm.blockingLock(path); blocker != "" {
				break
			}
		}
		if blocker == "" {
			return
		}
		if sm.pathConds[blocker] == nil {
			sm.pathConds[blocker] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[blocker].Wait()
	}
}

// waitForSubtreeUnlocked waits until root does not overlap another recursive
// operation and its exact path is not syncing. The caller must hold sm.mu.
func (sm *StagingStateManager) waitForSubtreeUnlocked(root string) {
	for {
		blocker := ""
		if sm.lockedPaths[root] {
			blocker = root
		} else {
			for lockedRoot := range sm.lockedSubtrees {
				if pathInSubtree(root, lockedRoot) || pathInSubtree(lockedRoot, root) {
					blocker = lockedRoot
					break
				}
			}
		}
		if blocker == "" {
			return
		}
		if sm.pathConds[blocker] == nil {
			sm.pathConds[blocker] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[blocker].Wait()
	}
}

// waitForLockedDescendants waits for sync handlers already running below root.
// A subtree lock must be held before calling this so no new descendant sync can start.
func (sm *StagingStateManager) waitForLockedDescendants(root string) {
	for {
		blocker := ""
		for path := range sm.lockedPaths {
			if pathInSubtree(path, root) {
				blocker = path
				break
			}
		}
		if blocker == "" {
			return
		}
		if sm.pathConds[blocker] == nil {
			sm.pathConds[blocker] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[blocker].Wait()
	}
}

// Create marks a path as newly created
func (sm *StagingStateManager) Create(path string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Wait for exact-path sync or a recursive operation on an ancestor.
	sm.waitForPathsUnlocked(path)

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

	sm.waitForPathsUnlocked(path)

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

	// Wait for exact-path sync or a recursive operation on an ancestor.
	sm.waitForPathsUnlocked(path)

	meta, exists := sm.metadata[path]
	if exists && meta.Action == ActionRename {
		now := time.Now()
		uploadMeta := &StagingMetadata{
			Path:           path,
			OldPath:        meta.OldPath,
			Action:         ActionUpload,
			IsNew:          false,
			CreatedAt:      meta.CreatedAt,
			LastModifiedAt: now,
		}
		return sm.enqueueOperation(path, uploadMeta, []string{meta.OperationID}, true)
	}

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

// Rename queues a file rename. A never-synced file only needs its pending upload
// moved to the new logical path. Existing files retain an explicit RENAME action
// so the background worker can order it ahead of later work at the new path.
func (sm *StagingStateManager) Rename(oldPath, newPath string) (bool, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Wait for exact-path sync or a recursive operation on either ancestor.
	sm.waitForPathsUnlocked(oldPath, newPath)

	meta, exists := sm.metadata[oldPath]

	if exists && meta.IsNew {
		meta.Path = newPath
		meta.LastModifiedAt = time.Now()
		if err := sm.deleteMetadata(oldPath); err != nil {
			return false, err
		}
		meta.OperationID = ""
		if err := sm.persistMetadata(newPath, meta); err != nil {
			return false, err
		}
		return false, nil
	}

	now := time.Now()
	dependencies := make([]string, 0)
	uploadAfterRename := false
	if exists {
		switch meta.Action {
		case ActionUpload:
			uploadAfterRename = true
			if op := sm.dag.get(meta.OperationID); op != nil {
				dependencies = append(dependencies, op.Dependencies...)
			}
			if err := sm.deleteMetadata(oldPath); err != nil {
				return false, err
			}
		case ActionRename:
			dependencies = append(dependencies, meta.OperationID)
			if err := sm.detachMetadataUnlocked(oldPath); err != nil {
				return false, err
			}
		default:
			return false, errors.Newf("cannot rename %s: pending %s action", oldPath, meta.Action)
		}
	}
	renameMeta := &StagingMetadata{
		Path:           newPath,
		OldPath:        oldPath,
		Action:         ActionRename,
		IsNew:          false,
		CreatedAt:      now,
		LastModifiedAt: now,
	}
	if exists && meta.CreatedAt.Before(renameMeta.CreatedAt) {
		renameMeta.CreatedAt = meta.CreatedAt
	}
	if err := sm.enqueueOperation(newPath, renameMeta, dependencies, true); err != nil {
		return false, err
	}
	if uploadAfterRename {
		renameID := sm.metadata[newPath].OperationID
		uploadMeta := &StagingMetadata{
			Path:           newPath,
			OldPath:        oldPath,
			Action:         ActionUpload,
			IsNew:          false,
			CreatedAt:      meta.CreatedAt,
			LastModifiedAt: now,
		}
		if err := sm.enqueueOperation(newPath, uploadMeta, []string{renameID}, true); err != nil {
			return false, err
		}
	}
	return false, nil
}

// RenameDir queues a backend directory rename and moves all pending descendant
// work to the new logical subtree. For a never-synced directory no backend
// rename is necessary; only its local metadata paths are moved.
func (sm *StagingStateManager) RenameDir(oldPath, newPath string) (bool, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Stop new work from entering either tree while in-flight work drains.
	sm.waitForSubtreeUnlocked(oldPath)
	sm.waitForSubtreeUnlocked(newPath)
	sm.lockedSubtrees[oldPath] = true
	sm.lockedSubtrees[newPath] = true
	defer func() {
		delete(sm.lockedSubtrees, oldPath)
		delete(sm.lockedSubtrees, newPath)
		if sm.pathConds[oldPath] != nil {
			sm.pathConds[oldPath].Broadcast()
		}
		if sm.pathConds[newPath] != nil {
			sm.pathConds[newPath].Broadcast()
		}
	}()
	sm.waitForLockedDescendants(oldPath)
	sm.waitForLockedDescendants(newPath)

	meta, exists := sm.metadata[oldPath]
	now := time.Now()
	newDirectory := exists && meta.IsNew

	if newDirectory {
		if err := sm.rebaseSubtreeUnlocked(oldPath, newPath, "", ""); err != nil {
			return false, err
		}
		return false, nil
	}
	dependencies := make([]string, 0, 1)
	excludeOperationID := ""
	if exists && meta.Action == ActionRenameDir {
		dependencies = append(dependencies, meta.OperationID)
		excludeOperationID = meta.OperationID
		if err := sm.detachMetadataUnlocked(oldPath); err != nil {
			return false, err
		}
	} else if exists {
		return false, errors.Newf("cannot rename directory %s: pending %s action", oldPath, meta.Action)
	}

	renameMeta := &StagingMetadata{
		Path:           newPath,
		OldPath:        oldPath,
		Action:         ActionRenameDir,
		IsNew:          false,
		CreatedAt:      now,
		LastModifiedAt: now,
	}
	if err := sm.enqueueOperation(newPath, renameMeta, dependencies, true); err != nil {
		return false, err
	}
	renameID := sm.metadata[newPath].OperationID
	if err := sm.rebaseSubtreeUnlocked(oldPath, newPath, renameID, excludeOperationID); err != nil {
		return false, err
	}

	return false, nil
}

// Delete marks a path as deleted (file deletion only)
func (sm *StagingStateManager) Delete(path string) error {
	return sm.DeleteWithForce(path, false)
}

// DeleteWithForce marks a path as deleted and preserves the force option for sync.
func (sm *StagingStateManager) DeleteWithForce(path string, force bool) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Wait for exact-path sync or a recursive operation on an ancestor.
	sm.waitForPathsUnlocked(path)

	meta, exists := sm.metadata[path]
	if exists && meta.Action == ActionRename {
		now := time.Now()
		deleteMeta := &StagingMetadata{
			Path:           path,
			Action:         ActionDelete,
			Force:          force,
			IsNew:          false,
			CreatedAt:      meta.CreatedAt,
			LastModifiedAt: now,
		}
		return sm.enqueueOperation(path, deleteMeta, []string{meta.OperationID}, true)
	}

	if exists && !sm.isValidAction(meta.Action, ActionDelete) {
		return errors.Newf("cannot delete %s: invalid action transition from %s to DELETE", path, meta.Action)
	}

	if !exists {
		// Direct deletion of existing file
		now := time.Now()
		meta = &StagingMetadata{
			Path:           path,
			Action:         ActionDelete,
			Force:          force,
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
		meta.Force = force
		meta.LastModifiedAt = time.Now()
	}

	return sm.persistMetadata(path, meta)
}

// Mkdir marks a directory as created
func (sm *StagingStateManager) Mkdir(path string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Wait for exact-path sync or a recursive operation on an ancestor.
	sm.waitForPathsUnlocked(path)

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

// Rmdir queues a directory removal. It never calls the backend inline. Pending
// work in the subtree is collapsed before the RMDIR is persisted so background
// sync can process only the operations required to make the collection empty.
func (sm *StagingStateManager) Rmdir(path string, recurse bool, force bool) (bool, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Block new work anywhere below path before waiting for handlers that are
	// already running. Non-recursive RMDIR also needs this barrier because rm -rf
	// reaches FUSE as individual unlinks followed by an ordinary rmdir syscall.
	sm.waitForSubtreeUnlocked(path)
	sm.lockedSubtrees[path] = true
	defer func() {
		delete(sm.lockedSubtrees, path)
		if sm.pathConds[path] != nil {
			sm.pathConds[path].Broadcast()
		}
	}()
	sm.waitForLockedDescendants(path)

	meta, exists := sm.metadata[path]
	pendingRenameID := ""
	if exists && meta.Action == ActionRenameDir {
		pendingRenameID = meta.OperationID
	} else if exists && !sm.isValidAction(meta.Action, ActionRmdir) {
		return false, errors.Newf("cannot remove directory %s: invalid action transition from %s to RMDIR", path, meta.Action)
	}

	createdAt := time.Now()
	backendMayExist := !exists || !meta.IsNew || meta.BackendMayExist
	if exists {
		createdAt = meta.CreatedAt
	}
	for _, op := range sm.dag.nodes {
		if op.Metadata.Path != path && pathInSubtree(op.Metadata.Path, path) && !op.Metadata.IsNew {
			backendMayExist = true
			break
		}
	}

	// A true recursive removal supersedes all work below it. For the ordinary
	// non-recursive rmdir calls produced by rm -rf, keep DELETE and child RMDIR
	// actions, cancel never-uploaded objects, and turn dirty existing objects
	// into deletes.
	if recurse {
		if err := sm.cancelOperationSubtreeUnlocked(path, false); err != nil {
			return false, err
		}
	}
	for metadataPath, child := range sm.metadata {
		if metadataPath == path || !pathInSubtree(metadataPath, path) {
			continue
		}
		if recurse {
			continue
		}
		switch child.Action {
		case ActionUpload, ActionBulkUpload:
			if child.IsNew {
				if err := sm.deleteMetadata(metadataPath); err != nil {
					return false, err
				}
			} else {
				child.Action = ActionDelete
				child.Force = force
				child.LastModifiedAt = time.Now()
				if err := sm.persistMetadata(metadataPath, child); err != nil {
					return false, err
				}
			}
		case ActionMkdir:
			if child.IsNew {
				if err := sm.deleteMetadata(metadataPath); err != nil {
					return false, err
				}
			}
		}
	}
	if !backendMayExist {
		if exists && pendingRenameID == "" {
			if err := sm.deleteMetadata(path); err != nil {
				return false, err
			}
		}
		return false, nil
	}

	if exists && pendingRenameID == "" {
		if err := sm.deleteMetadata(path); err != nil {
			return false, err
		}
	}
	now := time.Now()
	rmdirMeta := &StagingMetadata{
		Path:           path,
		Action:         ActionRmdir,
		Recurse:        recurse,
		Force:          force,
		IsNew:          false,
		CreatedAt:      createdAt,
		LastModifiedAt: now,
	}
	dependencies := make([]string, 0, 1)
	if pendingRenameID != "" {
		dependencies = append(dependencies, pendingRenameID)
	}
	for metadataPath, child := range sm.metadata {
		if metadataPath != path && pathInSubtree(metadataPath, path) {
			dependencies = append(dependencies, child.OperationID)
			sm.dag.markUrgent(child.OperationID)
			if sm.db != nil {
				_ = sm.db.Update(func(txn *badger.Txn) error { return sm.persistOperationTxn(txn, child.OperationID) })
			}
		}
	}
	if err := sm.enqueueOperation(path, rmdirMeta, dependencies, true); err != nil {
		return false, err
	}
	return false, nil
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

func (sm *StagingStateManager) IsRenamedFrom(path string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, op := range sm.dag.nodes {
		if op.Metadata.OldPath == path && (op.Metadata.Action == ActionRename || op.Metadata.Action == ActionRenameDir) {
			return true
		}
	}
	return false
}

func (sm *StagingStateManager) GetPendingRenames() []*StagingMetadata {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	operations := make([]*StagingOperation, 0)
	for _, op := range sm.dag.nodes {
		if op.Metadata.Action != ActionRename && op.Metadata.Action != ActionRenameDir {
			continue
		}
		operations = append(operations, op)
	}
	sort.SliceStable(operations, func(i, j int) bool {
		return operations[i].CreatedAt.Before(operations[j].CreatedAt)
	})
	result := make([]*StagingMetadata, 0, len(operations))
	for _, op := range operations {
		copyMeta := *op.Metadata
		result = append(result, &copyMeta)
	}
	return result
}

// syncOne performs handler call and removes metadata for a single path with internal locking
// Acquires and releases lock for the path
func (sm *StagingStateManager) syncOne(meta *StagingMetadata) error {
	sm.mu.Lock()
	directoryOperation := meta.Action == ActionRmdir || meta.Action == ActionRenameDir
	if directoryOperation {
		sm.waitForSubtreeUnlocked(meta.Path)
		if meta.OldPath != "" {
			sm.waitForSubtreeUnlocked(meta.OldPath)
		}
	} else {
		sm.waitForPathsUnlocked(meta.Path, meta.OldPath)
	}
	op := sm.dag.get(meta.OperationID)
	if op == nil || len(op.Dependencies) != 0 || op.State == OperationRunning {
		sm.mu.Unlock()
		return nil
	}
	op.State = OperationRunning
	if directoryOperation {
		sm.lockedSubtrees[meta.Path] = true
		if meta.OldPath != "" {
			sm.lockedSubtrees[meta.OldPath] = true
		}
		sm.waitForLockedDescendants(meta.Path)
		if meta.OldPath != "" {
			sm.waitForLockedDescendants(meta.OldPath)
		}
	} else {
		sm.lockedPaths[meta.Path] = true
		if meta.OldPath != "" {
			sm.lockedPaths[meta.OldPath] = true
		}
	}
	if sm.pathConds[meta.Path] == nil {
		sm.pathConds[meta.Path] = sync.NewCond(&sm.mu)
	}
	if sm.db != nil {
		_ = sm.db.Update(func(txn *badger.Txn) error { return sm.persistOperationTxn(txn, op.ID) })
	}
	sm.mu.Unlock()

	if sm.ActionHandler != nil {
		if err := sm.ActionHandler(meta); err != nil {
			sm.mu.Lock()
			if live := sm.dag.get(meta.OperationID); live != nil {
				live.State = OperationFailed
				live.Metadata.SyncFailCount++
				meta.SyncFailCount = live.Metadata.SyncFailCount
				if latest := sm.metadata[live.Metadata.Path]; latest != nil && latest.OperationID == live.ID {
					latest.SyncFailCount = live.Metadata.SyncFailCount
				}
				if sm.db != nil {
					_ = sm.db.Update(func(txn *badger.Txn) error { return sm.persistOperationTxn(txn, live.ID) })
				}
			}
			sm.unlockOperationUnlocked(meta)
			sm.mu.Unlock()
			return errors.Wrapf(err, "handler failed for %q action on %q", meta.Action, meta.Path)
		}
	}

	sm.mu.Lock()
	deleteErr := sm.markAncestorDirectoriesTouchedUnlocked(meta.Path)
	if deleteErr == nil {
		deleteErr = sm.completeOperationUnlocked(meta.OperationID, meta.Path)
	}
	sm.unlockOperationUnlocked(meta)
	sm.mu.Unlock()

	return deleteErr
}

func (sm *StagingStateManager) markAncestorDirectoriesTouchedUnlocked(path string) error {
	for directoryPath, meta := range sm.metadata {
		if directoryPath == path || !pathInSubtree(path, directoryPath) || meta.Action != ActionMkdir || !meta.IsNew || meta.BackendMayExist {
			continue
		}
		meta.BackendMayExist = true
		if err := sm.persistMetadata(directoryPath, meta); err != nil {
			return err
		}
	}
	return nil
}

func (sm *StagingStateManager) unlockOperationUnlocked(meta *StagingMetadata) {
	if meta.Action == ActionRmdir || meta.Action == ActionRenameDir {
		delete(sm.lockedSubtrees, meta.Path)
		if meta.OldPath != "" {
			delete(sm.lockedSubtrees, meta.OldPath)
		}
	} else {
		delete(sm.lockedPaths, meta.Path)
		if meta.OldPath != "" {
			delete(sm.lockedPaths, meta.OldPath)
		}
	}
	if sm.pathConds[meta.Path] != nil {
		sm.pathConds[meta.Path].Broadcast()
	}
	if meta.OldPath != "" && sm.pathConds[meta.OldPath] != nil {
		sm.pathConds[meta.OldPath].Broadcast()
	}
}

func (sm *StagingStateManager) completeOperationUnlocked(operationID string, path string) error {
	if sm.dag.get(operationID) == nil {
		return nil
	}
	sm.dag.remove(operationID)
	latest := sm.metadata[path]
	removeLatest := latest != nil && latest.OperationID == operationID
	if removeLatest {
		delete(sm.metadata, path)
	}
	if sm.db == nil {
		return nil
	}
	return sm.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete([]byte(fmt.Sprintf("operation:%s", operationID))); err != nil {
			return err
		}
		if removeLatest {
			if err := txn.Delete([]byte(fmt.Sprintf("staging:%s", path))); err != nil {
				return err
			}
		}
		for id := range sm.dag.nodes {
			if err := sm.persistOperationTxn(txn, id); err != nil {
				return err
			}
		}
		return nil
	})
}

// SyncAll performs all pending iRODS operations and clears metadata one by one (exclusive lock)
func (sm *StagingStateManager) SyncAll() error {
	for {
		metas := sm.getSyncCandidates(0, true)
		if len(metas) == 0 {
			sm.mu.RLock()
			remaining := len(sm.dag.nodes)
			sm.mu.RUnlock()
			if remaining != 0 {
				return errors.Newf("operation DAG has %d blocked or cyclic nodes", remaining)
			}
			break
		}
		if err := sm.syncOne(metas[0]); err != nil {
			return err
		}
	}

	return nil
}

// SyncOld performs sync on items older than gracePeriod (10 seconds) with per-path locking
func (sm *StagingStateManager) SyncOld(gracePeriod time.Duration) error {
	for {
		metas := sm.getSyncCandidates(gracePeriod, false)
		if len(metas) == 0 {
			return nil
		}
		for _, meta := range metas {
			if err := sm.syncOne(meta); err != nil {
				return err
			}
		}
	}
}

// getSyncCandidates returns a deterministic dependency order. Directory
// operations and everything they block bypass the normal grace period.
func (sm *StagingStateManager) getSyncCandidates(gracePeriod time.Duration, includeAll bool) []*StagingMetadata {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	operations := sm.dag.ready(gracePeriod, includeAll)
	result := make([]*StagingMetadata, 0, len(operations))
	for _, op := range operations {
		result = append(result, op.Metadata)
	}
	return result
}

// Clear removes all metadata
func (sm *StagingStateManager) Clear() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.metadata = make(map[string]*StagingMetadata)
	sm.dag = newOperationDAG()

	if sm.db != nil {
		return sm.db.Update(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				key := it.Item().KeyCopy(nil)
				if !bytes.HasPrefix(key, []byte("staging:")) && !bytes.HasPrefix(key, []byte("operation:")) {
					continue
				}
				if err := txn.Delete(key); err != nil {
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

	sm.metadata = make(map[string]*StagingMetadata)
	sm.dag = newOperationDAG()
	migrated := false
	err := sm.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("operation:")
		it := txn.NewIterator(opts)
		for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
			item := it.Item()
			var op StagingOperation
			if err := item.Value(func(val []byte) error { return json.Unmarshal(val, &op) }); err != nil {
				it.Close()
				return errors.Wrap(err, "failed to unmarshal staging operation")
			}
			sm.dag.restore(&op)
		}
		it.Close()
		if err := sm.dag.validate(); err != nil {
			return errors.Wrap(err, "failed to restore staging operation DAG")
		}

		opts = badger.DefaultIteratorOptions
		opts.Prefix = []byte("staging:")
		it = txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
			item := it.Item()
			var meta StagingMetadata

			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &meta)
			}); err != nil {
				return errors.Wrap(err, "failed to unmarshal staging metadata")
			}

			if meta.OperationID != "" && sm.dag.get(meta.OperationID) != nil {
				sm.metadata[meta.Path] = &meta
			} else {
				op, err := sm.dag.add(&meta, nil, false)
				if err != nil {
					return err
				}
				copyMeta := *op.Metadata
				sm.metadata[copyMeta.Path] = &copyMeta
				migrated = true
			}
		}

		return nil
	})
	if err != nil || !migrated {
		return err
	}
	return sm.db.Update(func(txn *badger.Txn) error {
		for path, meta := range sm.metadata {
			data, err := json.Marshal(meta)
			if err != nil {
				return err
			}
			if err := txn.Set([]byte(fmt.Sprintf("staging:%s", path)), data); err != nil {
				return err
			}
		}
		for id := range sm.dag.nodes {
			if err := sm.persistOperationTxn(txn, id); err != nil {
				return err
			}
		}
		return nil
	})
}

// persistMetadata saves metadata to memory and Badger
func (sm *StagingStateManager) persistMetadata(path string, meta *StagingMetadata) error {
	if meta.OperationID == "" || sm.dag.get(meta.OperationID) == nil {
		meta.OperationID = ""
		return sm.enqueueOperation(path, meta, nil, false)
	}
	sm.metadata[path] = meta
	if op := sm.dag.get(meta.OperationID); op != nil {
		copyMeta := *meta
		op.Metadata = &copyMeta
	}

	if sm.db == nil {
		return nil
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Wrap(err, "failed to marshal staging metadata")
	}
	return sm.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte(fmt.Sprintf("staging:%s", path)), data); err != nil {
			return err
		}
		return sm.persistOperationTxn(txn, meta.OperationID)
	})
}

func (sm *StagingStateManager) enqueueOperation(path string, meta *StagingMetadata, dependencies []string, urgent bool) error {
	rmdirParents := make([]string, 0)
	for id, op := range sm.dag.nodes {
		if op.Metadata.Action != ActionRenameDir && op.Metadata.Action != ActionRmdir {
			continue
		}
		if pathInSubtree(path, op.Metadata.Path) {
			if op.Metadata.Action == ActionRmdir && path != op.Metadata.Path &&
				(meta.Action == ActionDelete || meta.Action == ActionRmdir) {
				rmdirParents = append(rmdirParents, id)
			} else {
				dependencies = append(dependencies, id)
			}
			urgent = urgent || op.Urgent
		}
	}
	op, err := sm.dag.add(meta, dependencies, urgent)
	if err != nil {
		return err
	}
	for _, parentID := range rmdirParents {
		sm.dag.addDependency(parentID, op.ID)
		sm.dag.markUrgent(op.ID)
	}
	copyMeta := *op.Metadata
	sm.metadata[path] = &copyMeta
	if sm.db == nil {
		return nil
	}
	data, err := json.Marshal(&copyMeta)
	if err != nil {
		return errors.Wrap(err, "failed to marshal staging metadata")
	}
	return sm.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte(fmt.Sprintf("staging:%s", path)), data); err != nil {
			return err
		}
		if err := sm.persistOperationTxn(txn, op.ID); err != nil {
			return err
		}
		for _, parentID := range rmdirParents {
			if err := sm.persistOperationTxn(txn, parentID); err != nil {
				return err
			}
		}
		return nil
	})
}

func (sm *StagingStateManager) detachMetadataUnlocked(path string) error {
	delete(sm.metadata, path)
	if sm.db == nil {
		return nil
	}
	return sm.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(fmt.Sprintf("staging:%s", path)))
	})
}

func (sm *StagingStateManager) rebaseSubtreeUnlocked(oldRoot string, newRoot string, dependencyID string, excludeOperationID string) error {
	moved := make(map[string]*StagingMetadata)
	oldKeys := make([]string, 0)
	for path, meta := range sm.metadata {
		if !pathInSubtree(path, oldRoot) {
			continue
		}
		newPath := newRoot + path[len(strings.TrimRight(oldRoot, "/")):]
		copyMeta := *meta
		copyMeta.Path = newPath
		if copyMeta.OldPath != "" && pathInSubtree(copyMeta.OldPath, oldRoot) {
			copyMeta.OldPath = newRoot + copyMeta.OldPath[len(strings.TrimRight(oldRoot, "/")):]
		}
		copyMeta.LastModifiedAt = time.Now()
		moved[newPath] = &copyMeta
		oldKeys = append(oldKeys, path)
	}
	for _, path := range oldKeys {
		delete(sm.metadata, path)
	}
	for path, meta := range moved {
		sm.metadata[path] = meta
	}
	for id, op := range sm.dag.nodes {
		if id == excludeOperationID {
			continue
		}
		if pathInSubtree(op.Metadata.Path, oldRoot) {
			sm.dag.rebasePath(id, oldRoot, newRoot)
			if dependencyID != "" && id != dependencyID {
				sm.dag.addDependency(id, dependencyID)
				sm.dag.markUrgent(id)
			}
		}
	}
	if sm.db == nil {
		return nil
	}
	return sm.db.Update(func(txn *badger.Txn) error {
		for _, path := range oldKeys {
			if err := txn.Delete([]byte(fmt.Sprintf("staging:%s", path))); err != nil {
				return err
			}
		}
		for path, meta := range moved {
			data, err := json.Marshal(meta)
			if err != nil {
				return err
			}
			if err := txn.Set([]byte(fmt.Sprintf("staging:%s", path)), data); err != nil {
				return err
			}
		}
		for id := range sm.dag.nodes {
			if err := sm.persistOperationTxn(txn, id); err != nil {
				return err
			}
		}
		return nil
	})
}

func (sm *StagingStateManager) cancelOperationSubtreeUnlocked(root string, includeRoot bool) error {
	removedIDs := make([]string, 0)
	for id, op := range sm.dag.nodes {
		if pathInSubtree(op.Metadata.Path, root) && (includeRoot || op.Metadata.Path != root) {
			removedIDs = append(removedIDs, id)
		}
	}
	for _, id := range removedIDs {
		sm.dag.remove(id)
	}
	removedPaths := make([]string, 0)
	for path := range sm.metadata {
		if pathInSubtree(path, root) && (includeRoot || path != root) {
			removedPaths = append(removedPaths, path)
			delete(sm.metadata, path)
		}
	}
	if sm.db == nil {
		return nil
	}
	return sm.db.Update(func(txn *badger.Txn) error {
		for _, id := range removedIDs {
			if err := txn.Delete([]byte(fmt.Sprintf("operation:%s", id))); err != nil {
				return err
			}
		}
		for _, path := range removedPaths {
			if err := txn.Delete([]byte(fmt.Sprintf("staging:%s", path))); err != nil {
				return err
			}
		}
		for id := range sm.dag.nodes {
			if err := sm.persistOperationTxn(txn, id); err != nil {
				return err
			}
		}
		return nil
	})
}

func (sm *StagingStateManager) persistOperationTxn(txn *badger.Txn, operationID string) error {
	op := sm.dag.get(operationID)
	if op == nil {
		return nil
	}
	data, err := json.Marshal(op)
	if err != nil {
		return errors.Wrap(err, "failed to marshal staging operation")
	}
	return txn.Set([]byte(fmt.Sprintf("operation:%s", operationID)), data)
}

// deleteMetadata removes metadata from memory and Badger (caller must hold mu)
func (sm *StagingStateManager) deleteMetadata(path string) error {
	meta := sm.metadata[path]
	delete(sm.metadata, path)
	if meta != nil {
		sm.dag.remove(meta.OperationID)
	}

	if sm.db == nil {
		return nil
	}

	return sm.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete([]byte(fmt.Sprintf("staging:%s", path))); err != nil {
			return err
		}
		if meta != nil {
			if err := txn.Delete([]byte(fmt.Sprintf("operation:%s", meta.OperationID))); err != nil {
				return err
			}
		}
		for id := range sm.dag.nodes {
			if err := sm.persistOperationTxn(txn, id); err != nil {
				return err
			}
		}
		return nil
	})
}

func (sm *StagingStateManager) markOperationBlockedPublic(operationID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if op := sm.dag.get(operationID); op != nil {
		op.State = OperationBlocked
		if sm.db != nil {
			_ = sm.db.Update(func(txn *badger.Txn) error { return sm.persistOperationTxn(txn, operationID) })
		}
	}
}

func (sm *StagingStateManager) retryBlockedOperations() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for id, op := range sm.dag.nodes {
		if op.State != OperationBlocked {
			continue
		}
		op.State = OperationQueued
		op.Metadata.SyncFailCount = 0
		if latest := sm.metadata[op.Metadata.Path]; latest != nil && latest.OperationID == id {
			latest.SyncFailCount = 0
		}
		if sm.db != nil {
			_ = sm.db.Update(func(txn *badger.Txn) error { return sm.persistOperationTxn(txn, id) })
		}
	}
}

// WaitForSync blocks until the given path is no longer being synced.
func (sm *StagingStateManager) WaitForSync(path string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.waitForPathsUnlocked(path)
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
