package stagingfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	stdpath "path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dgraph-io/badger/v3"
	log "github.com/sirupsen/logrus"
)

// Handler for iRODS operations
type (
	// ActionHandler performs one backend operation for a staged path.
	//
	// It runs while the state manager holds the logical lock for that path, or
	// for the whole subtree when the operation is recursive. A handler must
	// therefore not re-enter a mutating staging operation (Create, Modify,
	// Touch, Delete, Rename, Rmdir, ...) for that path or for any path inside
	// that subtree: the re-entrant call waits for a lock that is released only
	// once the handler returns, so it deadlocks. Read-only calls such as Get and
	// GetAll, and mutations of unrelated paths, are safe.
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
	OperationID          string           // ID of the latest DAG operation for this logical path
	Path                 string           // Current path
	OldPath              string           // Old path (for RENAME actions)
	Action               ActionType       // Final action
	Recurse              bool             // Recursive directory removal
	Force                bool             // Force file or directory removal
	IsNew                bool             // Is this a new file?
	CreatedAt            time.Time        // Creation time
	LastModifiedAt       time.Time        // Last modification time
	SyncFailCount        int              // Number of consecutive sync failures
	BackendMayExist      bool             // A completed descendant operation may have created this directory remotely
	FileState            StagingFileState // Whether local file is dirty or cached
	LastAccessedAt       time.Time        // Last time the cached file was accessed (for eviction)
	RemoteSize           int64            // iRODS size recorded immediately after the upload that created a cached file
	RemoteModifyTime     time.Time        // iRODS modification time recorded with RemoteSize
	RemoteFreshnessKnown bool             // True only when RemoteSize and RemoteModifyTime were recorded after sync
}

// StagingStateManager manages staging metadata for async uploads
type StagingStateManager struct {
	metadata       map[string]*StagingMetadata
	dag            *OperationDAG
	lockedPaths    map[string]bool       // Paths locked during sync operations
	lockedSubtrees map[string]bool       // Directory trees locked during recursive operations
	writeLeases    map[string]int        // Paths reserved by a local writer; sync defers while held
	leasedRoots    map[string]int        // Subtrees reserved by a local recursive operation
	pendingRoots   map[string]int        // Subtrees a local directory operation is waiting to lock; overlapping syncs wait
	pathConds      map[string]*sync.Cond // Per-path condition variables
	progressCond   *sync.Cond            // Signals that pending operations may have become runnable
	db             *badger.DB
	logger         *log.Entry
	mu             sync.RWMutex
	// ActionHandler is read under mu by every sync. Change it with
	// RegisterActionHandler; assigning to it directly races with a sync that is
	// selecting the handler to call.
	ActionHandler ActionHandler
}

// NewStagingStateManager creates a new manager (memory only)
func NewStagingStateManager() *StagingStateManager {
	return newStagingStateManager(nil)
}

// NewStagingStateManagerWithPersistence creates a new manager with Badger persistence
func NewStagingStateManagerWithPersistence(db *badger.DB) *StagingStateManager {
	return newStagingStateManager(db)
}

func newStagingStateManager(db *badger.DB) *StagingStateManager {
	sm := &StagingStateManager{
		metadata:       make(map[string]*StagingMetadata),
		dag:            newOperationDAG(),
		lockedPaths:    make(map[string]bool),
		lockedSubtrees: make(map[string]bool),
		writeLeases:    make(map[string]int),
		leasedRoots:    make(map[string]int),
		pendingRoots:   make(map[string]int),
		pathConds:      make(map[string]*sync.Cond),
		db:             db,
		logger:         log.NewEntry(log.StandardLogger()),
	}
	sm.progressCond = sync.NewCond(&sm.mu)
	return sm
}

// cleanPath returns the canonical form of a logical staging path: rooted, slash
// separated, and free of ".", ".." and repeated slashes. Canonicalizing at the
// API boundary keeps two spellings of one path from being tracked as separate
// operations under separate locks, and keeps a path from reaching outside the
// staging root once it is joined with the local data directory.
func cleanPath(path string) string {
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return stdpath.Clean(path)
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

// slowLockWaitThreshold is how long a directory operation may wait for running
// syncs before the wait is reported as a warning rather than a debug message.
const slowLockWaitThreshold = 10 * time.Second

// lockWait counts the waits a caller made on locks held by running syncs.
type lockWait struct {
	waits        int
	firstBlocker string
	lastBlocker  string
}

func (w *lockWait) record(blocker string) {
	if w.waits == 0 {
		w.firstBlocker = blocker
	}
	w.waits++
	w.lastBlocker = blocker
}

func (w *lockWait) add(other lockWait) {
	if other.waits == 0 {
		return
	}
	if w.waits == 0 {
		w.firstBlocker = other.firstBlocker
	}
	w.waits += other.waits
	w.lastBlocker = other.lastBlocker
}

// logLockWait reports a directory operation that had to wait for running syncs
// before it could be queued, which the FUSE caller sees as latency.
func (sm *StagingStateManager) logLockWait(action ActionType, path string, start time.Time, wait lockWait) {
	elapsed := time.Since(start)
	if wait.waits == 0 && elapsed < slowLockWaitThreshold {
		return
	}
	entry := sm.logger.WithFields(log.Fields{
		"action":       action.String(),
		"path":         path,
		"waits":        wait.waits,
		"firstBlocker": wait.firstBlocker,
		"lastBlocker":  wait.lastBlocker,
		"elapsed":      elapsed.String(),
	})
	if elapsed >= slowLockWaitThreshold {
		entry.Warn("staging operation waited long for running syncs")
		return
	}
	entry.Debug("staging operation waited for running syncs")
}

// reserveSubtreeUnlocked keeps syncs overlapping root from starting while the
// caller waits for the ones already running. Without it the background worker
// retakes sm.mu and starts the next overlapping sync before the woken caller
// runs, so the caller waits for the whole backlog below root instead of the
// syncs that were running when it arrived. Release the reservation with
// releaseSubtreeReservationUnlocked once root is locked. The caller must hold
// sm.mu.
func (sm *StagingStateManager) reserveSubtreeUnlocked(root string) {
	sm.pendingRoots[root]++
}

// releaseSubtreeReservationUnlocked drops a reservation taken by
// reserveSubtreeUnlocked and wakes the syncs it held back. The caller must hold
// sm.mu.
func (sm *StagingStateManager) releaseSubtreeReservationUnlocked(root string) {
	sm.pendingRoots[root]--
	if sm.pendingRoots[root] <= 0 {
		delete(sm.pendingRoots, root)
	}
	if cond := sm.pathConds[root]; cond != nil {
		cond.Broadcast()
	}
	sm.notifyProgressUnlocked()
}

// reservedRootUnlocked returns a reserved subtree that must keep the operation
// described by meta from starting, or "" when none does. Directory operations
// overlap a reservation in either direction, other operations only when they
// fall inside it. The caller must hold sm.mu.
func (sm *StagingStateManager) reservedRootUnlocked(meta *StagingMetadata) string {
	directoryOperation := meta.Action == ActionRmdir || meta.Action == ActionRenameDir
	for root := range sm.pendingRoots {
		for _, path := range []string{meta.Path, meta.OldPath} {
			if path == "" {
				continue
			}
			if pathInSubtree(path, root) || (directoryOperation && pathInSubtree(root, path)) {
				return root
			}
		}
	}
	return ""
}

// waitForSubtreeUnlocked waits until root does not overlap another recursive
// operation and its exact path is not syncing. The caller must hold sm.mu.
func (sm *StagingStateManager) waitForSubtreeUnlocked(root string) lockWait {
	var wait lockWait
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
			return wait
		}
		wait.record(blocker)
		if sm.pathConds[blocker] == nil {
			sm.pathConds[blocker] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[blocker].Wait()
	}
}

// waitForLockedDescendants waits for sync handlers already running below root.
// A subtree lock must be held before calling this so no new descendant sync can start.
func (sm *StagingStateManager) waitForLockedDescendants(root string) lockWait {
	var wait lockWait
	for {
		blocker := ""
		for path := range sm.lockedPaths {
			if pathInSubtree(path, root) {
				blocker = path
				break
			}
		}
		if blocker == "" {
			return wait
		}
		wait.record(blocker)
		if sm.pathConds[blocker] == nil {
			sm.pathConds[blocker] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[blocker].Wait()
	}
}

// AcquireWriteLease reserves path while its caller mutates the local staging
// file. It first waits for an in-flight sync of the same path (or of an
// enclosing recursive operation) and then keeps sync from starting on that path
// until ReleaseWriteLease is called, so a local modification can never be
// interleaved with the upload of a candidate that was selected just before it.
//
// A lease deliberately does not block metadata mutations such as Create,
// Modify, or Touch: the lease holder itself registers those, and blocking them
// would deadlock.
func (sm *StagingStateManager) AcquireWriteLease(path string) {
	path = cleanPath(path)
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.waitForPathsUnlocked(path)
	sm.writeLeases[path]++
}

// ReleaseWriteLease drops one reservation taken by AcquireWriteLease.
func (sm *StagingStateManager) ReleaseWriteLease(path string) {
	path = cleanPath(path)
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.writeLeases[path]--
	if sm.writeLeases[path] <= 0 {
		delete(sm.writeLeases, path)
	}
	if cond := sm.pathConds[path]; cond != nil {
		cond.Broadcast()
	}
	sm.notifyProgressUnlocked()
}

// AcquireWriteLeaseSubtree reserves an entire subtree while its caller performs
// a local recursive operation such as a directory rename. In addition to the
// guarantees of AcquireWriteLease it keeps descendant operations from syncing,
// and returns only once syncs already running below root have finished.
func (sm *StagingStateManager) AcquireWriteLeaseSubtree(root string) {
	root = cleanPath(root)
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.reserveSubtreeUnlocked(root)
	sm.waitForSubtreeUnlocked(root)
	// Registering the lease first stops any new descendant sync, so the drain
	// below cannot be outrun by a sync that starts while it waits.
	sm.leasedRoots[root]++
	sm.releaseSubtreeReservationUnlocked(root)
	sm.waitForLockedDescendants(root)
}

// ReleaseWriteLeaseSubtree drops one reservation taken by AcquireWriteLeaseSubtree.
func (sm *StagingStateManager) ReleaseWriteLeaseSubtree(root string) {
	root = cleanPath(root)
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.leasedRoots[root]--
	if sm.leasedRoots[root] <= 0 {
		delete(sm.leasedRoots, root)
	}
	if cond := sm.pathConds[root]; cond != nil {
		cond.Broadcast()
	}
	sm.notifyProgressUnlocked()
}

// leasedPathUnlocked returns a write-leased path that must keep the operation
// described by meta from running, or "" when none does. Directory operations
// are held back by a lease anywhere in the subtrees they touch, and a leased
// subtree holds back anything overlapping it. The caller must hold sm.mu.
func (sm *StagingStateManager) leasedPathUnlocked(meta *StagingMetadata) string {
	paths := []string{meta.Path}
	if meta.OldPath != "" {
		paths = append(paths, meta.OldPath)
	}

	if meta.Action == ActionRmdir || meta.Action == ActionRenameDir {
		for leased := range sm.writeLeases {
			for _, path := range paths {
				if pathInSubtree(leased, path) {
					return leased
				}
			}
		}
	} else {
		for _, path := range paths {
			if sm.writeLeases[path] > 0 {
				return path
			}
		}
	}

	for root := range sm.leasedRoots {
		for _, path := range paths {
			if pathInSubtree(path, root) || pathInSubtree(root, path) {
				return root
			}
		}
	}
	return ""
}

// notifyProgressUnlocked wakes SyncAll waiters after a change that can make a
// pending operation runnable: an operation finishing or being removed, a write
// lease being released, or a blocked operation being requeued. The caller must
// hold sm.mu.
func (sm *StagingStateManager) notifyProgressUnlocked() {
	sm.progressCond.Broadcast()
}

// Create marks a path as newly created
func (sm *StagingStateManager) Create(path string) error {
	path = cleanPath(path)
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
	path = cleanPath(path)
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
	path = cleanPath(path)
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Wait for exact-path sync or a recursive operation on an ancestor.
	sm.waitForPathsUnlocked(path)

	meta, exists := sm.metadata[path]
	if exists && (meta.Action == ActionRename || meta.Action == ActionRmdir) {
		// The pending operation has to reach the backend before this upload:
		// the rename that moved the object here, or the removal of the
		// directory that used to occupy the path. After a removal the object
		// is new, so it must not be looked up in the backend either.
		now := time.Now()
		uploadMeta := &StagingMetadata{
			Path:           path,
			OldPath:        meta.OldPath,
			Action:         ActionUpload,
			IsNew:          meta.Action == ActionRmdir,
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
		// DELETE -> UPLOAD represents a new local file at this path. The
		// delete worker may already have removed the old backend object, so
		// subsequent Stat calls must use the staged entry rather than looking
		// up the now-deleted backend object.
		if meta.Action == ActionDelete {
			meta.IsNew = true
		}
		meta.Action = ActionUpload
		meta.LastModifiedAt = time.Now()
	}

	return sm.persistMetadata(path, meta)
}

// Rename queues a file rename. A never-synced file only needs its pending upload
// moved to the new logical path. Existing files retain an explicit RENAME action
// so the background worker can order it ahead of later work at the new path.
func (sm *StagingStateManager) Rename(oldPath, newPath string) (bool, error) {
	oldPath = cleanPath(oldPath)
	newPath = cleanPath(newPath)
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
	oldPath = cleanPath(oldPath)
	newPath = cleanPath(newPath)
	waitStart := time.Now()
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Stop new work from entering either tree while in-flight work drains.
	sm.reserveSubtreeUnlocked(oldPath)
	sm.reserveSubtreeUnlocked(newPath)
	var wait lockWait
	wait.add(sm.waitForSubtreeUnlocked(oldPath))
	wait.add(sm.waitForSubtreeUnlocked(newPath))
	sm.lockedSubtrees[oldPath] = true
	sm.lockedSubtrees[newPath] = true
	sm.releaseSubtreeReservationUnlocked(oldPath)
	sm.releaseSubtreeReservationUnlocked(newPath)
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
	wait.add(sm.waitForLockedDescendants(oldPath))
	wait.add(sm.waitForLockedDescendants(newPath))
	sm.logLockWait(ActionRenameDir, oldPath, waitStart, wait)

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
	path = cleanPath(path)
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
	path = cleanPath(path)
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
	path = cleanPath(path)
	waitStart := time.Now()
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Block new work anywhere below path before waiting for handlers that are
	// already running. Non-recursive RMDIR also needs this barrier because rm -rf
	// reaches FUSE as individual unlinks followed by an ordinary rmdir syscall.
	sm.reserveSubtreeUnlocked(path)
	var wait lockWait
	wait.add(sm.waitForSubtreeUnlocked(path))
	sm.lockedSubtrees[path] = true
	sm.releaseSubtreeReservationUnlocked(path)
	defer func() {
		delete(sm.lockedSubtrees, path)
		if sm.pathConds[path] != nil {
			sm.pathConds[path].Broadcast()
		}
	}()
	wait.add(sm.waitForLockedDescendants(path))
	sm.logLockWait(ActionRmdir, path, waitStart, wait)

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
			sm.persistOperationStateUnlocked(child.OperationID)
		}
	}
	if err := sm.enqueueOperation(path, rmdirMeta, dependencies, true); err != nil {
		return false, err
	}
	return false, nil
}

// Touch updates the modification time for a pending path in memory, in the
// operation DAG, and in persistent storage. Callers must use this instead of
// mutating metadata returned by Get.
func (sm *StagingStateManager) Touch(path string) error {
	path = cleanPath(path)
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.waitForPathsUnlocked(path)
	meta := sm.metadata[path]
	if meta == nil {
		return nil
	}
	meta.LastModifiedAt = time.Now()
	return sm.persistMetadata(path, meta)
}

// DiscardPendingOperation removes the pending operation identified by
// operationID, and the metadata pointing at it, without queuing any backend
// work. It undoes a registration whose local data could not be staged; nothing
// happens when the operation has already been replaced or completed.
func (sm *StagingStateManager) DiscardPendingOperation(path string, operationID string) error {
	path = cleanPath(path)
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.waitForPathsUnlocked(path)
	meta := sm.metadata[path]
	if meta == nil || meta.OperationID != operationID {
		return nil
	}
	return sm.deleteMetadata(path)
}

// Get retrieves a copy of metadata for a path.
func (sm *StagingStateManager) Get(path string) *StagingMetadata {
	path = cleanPath(path)
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	meta := sm.metadata[path]
	if meta == nil {
		return nil
	}
	copyMeta := *meta
	return &copyMeta
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
	path = cleanPath(path)
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

// syncOne performs the operation described by meta regardless of its age. It is
// used by SyncAll, which must drain every pending operation.
func (sm *StagingStateManager) syncOne(meta *StagingMetadata) error {
	_, _, err := sm.syncCandidate(meta, 0, true)
	return err
}

// syncCandidate performs handler call and removes metadata for a single path
// with internal locking. Acquires and releases lock for the path.
//
// meta is a snapshot taken by getSyncCandidates, so the live operation is
// re-validated against the same readiness rule once the path lock is held:
// anything modified, leased by a local writer, or already picked up by another
// worker in the meantime is left for a later pass. executed reports whether the
// handler actually ran, and stale reports a candidate that no longer describes
// the operation it was taken from, which together let callers tell a pass that
// made no progress from one that has to take a fresh snapshot.
func (sm *StagingStateManager) syncCandidate(meta *StagingMetadata, gracePeriod time.Duration, includeAll bool) (executed bool, stale bool, err error) {
	sm.mu.Lock()
	directoryOperation := meta.Action == ActionRmdir || meta.Action == ActionRenameDir
	for {
		if directoryOperation {
			sm.waitForSubtreeUnlocked(meta.Path)
			if meta.OldPath != "" {
				sm.waitForSubtreeUnlocked(meta.OldPath)
			}
		} else {
			sm.waitForPathsUnlocked(meta.Path, meta.OldPath)
		}
		// A reservation is held only while its owner waits for syncs that are
		// already running, so waiting here is brief and cannot deadlock: this
		// sync holds no path lock yet.
		reserved := sm.reservedRootUnlocked(meta)
		if reserved == "" {
			break
		}
		if sm.pathConds[reserved] == nil {
			sm.pathConds[reserved] = sync.NewCond(&sm.mu)
		}
		sm.pathConds[reserved].Wait()
	}
	op := sm.dag.get(meta.OperationID)
	if op == nil || len(op.Dependencies) != 0 || op.State == OperationRunning {
		sm.mu.Unlock()
		return false, false, nil
	}
	if !operationMatchesCandidate(op.Metadata, meta) {
		// A queued operation is edited in place, keeping its ID: removing a
		// directory turns a queued upload below it into a delete, and renaming
		// one rebases the paths below it. The snapshot then describes work the
		// caller no longer asked for, so it is dropped in favour of a fresh one
		// rather than handed to the handler.
		sm.mu.Unlock()
		return false, true, nil
	}
	if !includeAll && !op.Urgent && time.Since(op.Metadata.LastModifiedAt) < gracePeriod {
		// A local modification landed after this candidate was selected.
		sm.mu.Unlock()
		return false, false, nil
	}
	if sm.leasedPathUnlocked(op.Metadata) != "" {
		sm.mu.Unlock()
		return false, false, nil
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
	sm.persistOperationStateUnlocked(op.ID)
	// Read the handler while the lock is held: a concurrent
	// RegisterActionHandler writes it under the same lock.
	handler := sm.ActionHandler
	pendingOperations := len(sm.dag.nodes)
	urgent := op.Urgent
	sm.mu.Unlock()

	if handler != nil {
		syncLogger := sm.logger.WithFields(log.Fields{
			"action":            meta.Action.String(),
			"path":              meta.Path,
			"oldPath":           meta.OldPath,
			"operationID":       meta.OperationID,
			"urgent":            urgent,
			"pendingOperations": pendingOperations,
		})
		syncLogger.Debug("staging sync started")
		syncStart := time.Now()
		err := handler(meta)
		syncLogger = syncLogger.WithField("elapsed", time.Since(syncStart).String())
		if err != nil {
			syncLogger.WithError(err).Debug("staging sync failed")
			sm.mu.Lock()
			if live := sm.dag.get(meta.OperationID); live != nil {
				live.State = OperationFailed
				live.Metadata.SyncFailCount++
				meta.SyncFailCount = live.Metadata.SyncFailCount
				if latest := sm.metadata[live.Metadata.Path]; latest != nil && latest.OperationID == live.ID {
					latest.SyncFailCount = live.Metadata.SyncFailCount
				}
				sm.persistOperationStateUnlocked(live.ID)
			}
			sm.unlockOperationUnlocked(meta)
			sm.mu.Unlock()
			return true, false, errors.Wrapf(err, "handler failed for %q action on %q", meta.Action, meta.Path)
		}
		syncLogger.Debug("staging sync finished")
	}

	sm.mu.Lock()
	deleteErr := sm.markAncestorDirectoriesTouchedUnlocked(meta.Path)
	if deleteErr == nil {
		deleteErr = sm.completeOperationUnlocked(meta.OperationID, meta.Path)
	}
	if deleteErr != nil {
		// The backend work is done but could not be recorded. Leave the
		// operation runnable instead of stranding it in RUNNING, so a later
		// pass retries it rather than never touching it again.
		if live := sm.dag.get(meta.OperationID); live != nil && live.State == OperationRunning {
			live.State = OperationQueued
		}
	}
	sm.unlockOperationUnlocked(meta)
	sm.mu.Unlock()

	return true, false, deleteErr
}

// operationMatchesCandidate reports whether a snapshot still describes the
// queued operation it was taken from. Only the fields the path locks and the
// handler act on are compared; bookkeeping such as the failure count or the
// modification time is expected to move under a running sync.
func operationMatchesCandidate(live *StagingMetadata, candidate *StagingMetadata) bool {
	return live.Action == candidate.Action &&
		live.Path == candidate.Path &&
		live.OldPath == candidate.OldPath &&
		live.Force == candidate.Force &&
		live.Recurse == candidate.Recurse
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
	sm.notifyProgressUnlocked()
}

func (sm *StagingStateManager) completeOperationUnlocked(operationID string, path string) error {
	op := sm.dag.get(operationID)
	if op == nil {
		return nil
	}

	latest := sm.metadata[path]
	removeLatest := latest != nil && latest.OperationID == operationID

	// Apply the removal in memory first: the transaction has to persist the
	// dependency lists that the removal leaves behind. When the write fails,
	// memory is put back the way it was, so the error the caller sees describes
	// an operation that is still pending in both places and can be retried.
	dependents := sm.dag.dependentsOf(operationID)
	sm.dag.remove(operationID)
	if removeLatest {
		delete(sm.metadata, path)
	}

	if sm.db == nil {
		return nil
	}

	err := sm.db.Update(func(txn *badger.Txn) error {
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
	if err != nil {
		sm.dag.reinsert(op, dependents)
		if removeLatest {
			sm.metadata[path] = latest
		}
		return err
	}

	return nil
}

// SyncAll performs all pending iRODS operations and clears metadata one by one (exclusive lock)
func (sm *StagingStateManager) SyncAll() error {
	for {
		// A candidate may be skipped because another worker took it or a local
		// writer holds a lease, so a pass can execute nothing at all.
		//
		// The whole snapshot is consumed before a new one is taken. Rebuilding
		// it after every executed operation would walk, copy and sort the
		// entire backlog once per operation, which is what dominates the flush
		// of a large staging area. syncCandidate re-reads every candidate
		// under the lock, so one that no longer describes runnable work by the
		// time the pass reaches it is skipped instead of acted on, and work
		// queued during the pass is picked up by the next one.
		progressed := false
		for _, meta := range sm.getSyncCandidates(0, true) {
			executed, stale, err := sm.syncCandidate(meta, 0, true)
			if err != nil {
				return err
			}
			if executed || stale {
				progressed = true
			}
		}
		if progressed {
			continue
		}

		sm.mu.Lock()
		remaining := len(sm.dag.nodes)
		if remaining == 0 {
			sm.mu.Unlock()
			return nil
		}
		if sm.runnableCandidateExistsUnlocked() {
			// Work queued or edited while the pass ran is not in the snapshot
			// the pass consumed. Take a fresh one instead of waiting for a
			// completion that nothing is going to signal, or reporting a DAG
			// that merely changed as blocked.
			sm.mu.Unlock()
			continue
		}
		if !sm.progressPossibleUnlocked() {
			sm.mu.Unlock()
			return errors.Newf("operation DAG has %d blocked or cyclic nodes", remaining)
		}
		// Work owned by someone else is still in flight. Wait for it rather than
		// reporting a DAG that is merely busy as blocked or cyclic.
		sm.progressCond.Wait()
		sm.mu.Unlock()
	}
}

// progressPossibleUnlocked reports whether a pending operation is expected to
// become runnable once work owned by someone else finishes: an operation that
// another worker is running, or one held back by a local writer's lease. The
// caller must hold sm.mu.
func (sm *StagingStateManager) progressPossibleUnlocked() bool {
	for _, op := range sm.dag.nodes {
		if op.State == OperationRunning {
			return true
		}
		if op.State == OperationBlocked || len(op.Dependencies) != 0 {
			continue
		}
		if sm.leasedPathUnlocked(op.Metadata) != "" {
			return true
		}
	}
	return false
}

// runnableCandidateExistsUnlocked reports whether an operation could run right
// now: nothing blocks it, it waits on nothing, it is not already running, and
// no local writer holds its data. The caller must hold sm.mu.
func (sm *StagingStateManager) runnableCandidateExistsUnlocked() bool {
	for _, op := range sm.dag.nodes {
		if op.State == OperationRunning || op.State == OperationBlocked || len(op.Dependencies) != 0 {
			continue
		}
		if sm.leasedPathUnlocked(op.Metadata) != "" {
			continue
		}
		return true
	}
	return false
}

// SyncOld performs sync on items older than gracePeriod (10 seconds) with per-path locking
func (sm *StagingStateManager) SyncOld(gracePeriod time.Duration) error {
	for {
		metas := sm.getSyncCandidates(gracePeriod, false)
		if len(metas) == 0 {
			return nil
		}
		progressed := false
		for _, meta := range metas {
			executed, stale, err := sm.syncCandidate(meta, gracePeriod, false)
			if err != nil {
				return err
			}
			progressed = progressed || executed || stale
		}
		// Candidates that are still leased or were touched while syncing are
		// left for the next pass instead of being retried in a busy loop.
		if !progressed {
			return nil
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
	// A pending operation already registered for this path keeps its place in
	// the queue: the new operation replaces it logically but must reach the
	// backend after it. Without this, a delete of the path could run after the
	// rename that later moved another object onto it, removing the new object.
	if pending := sm.metadata[path]; pending != nil && pending.OperationID != "" {
		if op := sm.dag.get(pending.OperationID); op != nil {
			dependencies = append(dependencies, op.ID)
		}
	}

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
	defer sm.notifyProgressUnlocked()

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

// persistOperationStateUnlocked writes one operation's current state outside any
// transaction the caller owns. A failure is not fatal — a restore normalizes
// RUNNING back to QUEUED and the work is retried — but it must not pass
// silently, because a failing database is how staging loses its durability.
// The caller must hold sm.mu.
func (sm *StagingStateManager) persistOperationStateUnlocked(operationID string) {
	if sm.db == nil {
		return
	}
	if err := sm.db.Update(func(txn *badger.Txn) error {
		return sm.persistOperationTxn(txn, operationID)
	}); err != nil {
		sm.logger.WithError(err).Warnf("failed to persist staging operation %s", operationID)
	}
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
	defer sm.notifyProgressUnlocked()

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
		sm.persistOperationStateUnlocked(operationID)
	}
}

func (sm *StagingStateManager) retryBlockedOperations() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	defer sm.notifyProgressUnlocked()
	for id, op := range sm.dag.nodes {
		if op.State != OperationBlocked {
			continue
		}
		op.State = OperationQueued
		op.Metadata.SyncFailCount = 0
		if latest := sm.metadata[op.Metadata.Path]; latest != nil && latest.OperationID == id {
			latest.SyncFailCount = 0
		}
		sm.persistOperationStateUnlocked(id)
	}
}

// WaitForSync blocks until the given path is no longer being synced.
func (sm *StagingStateManager) WaitForSync(path string) {
	path = cleanPath(path)
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.waitForPathsUnlocked(path)
}

// RegisterActionHandler registers a handler for operations.
// See ActionHandler for the re-entrancy rules a handler must follow.
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
