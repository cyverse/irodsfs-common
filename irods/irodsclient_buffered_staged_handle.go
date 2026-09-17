package irods

import (
	"context"
	"io"
	"os"
	"path"
	"sync"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/util"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

var _ IRODSFSFileHandle = (*IRODSFSClientBufferedStagedHandle)(nil)

// IRODSFSClientBufferedStagedHandle implements IRODSFSFileHandle using local staging files.
// Write operations go to a local file; background sync uploads to iRODS.
type IRODSFSClientBufferedStagedHandle struct {
	id        string
	client    *IRODSFSClientBuffered
	file      *os.File
	irodsPath string
	openMode  irodsclient_types.FileOpenMode
	entry     *irodsclient_fs.Entry
	logger    *log.Entry
	mu        sync.Mutex
}

func newStagedHandle(client *IRODSFSClientBuffered, file *os.File, irodsPath string, mode irodsclient_types.FileOpenMode, entry *irodsclient_fs.Entry) *IRODSFSClientBufferedStagedHandle {
	handleID := xid.New().String()

	var handleLogger *log.Entry
	if client != nil && client.logger != nil {
		handleLogger = client.logger.WithFields(log.Fields{
			"handle_id": handleID,
			"path":      irodsPath,
			"mode":      string(mode),
		})
	} else {
		handleLogger = log.WithFields(log.Fields{
			"handle_id": handleID,
			"path":      irodsPath,
			"mode":      string(mode),
		})
	}

	return &IRODSFSClientBufferedStagedHandle{
		id:        handleID,
		client:    client,
		file:      file,
		irodsPath: irodsPath,
		openMode:  mode,
		entry:     entry,
		logger:    handleLogger,
	}
}

func newStagedHandleForNewFile(client *IRODSFSClientBuffered, file *os.File, irodsPath string, mode irodsclient_types.FileOpenMode) *IRODSFSClientBufferedStagedHandle {
	now := time.Now()
	entry := &irodsclient_fs.Entry{
		Type:       irodsclient_fs.FileEntry,
		Name:       path.Base(irodsPath),
		Path:       irodsPath,
		Size:       0,
		CreateTime: now,
		ModifyTime: now,
		AccessTime: now,
	}

	return newStagedHandle(client, file, irodsPath, mode, entry)
}

// setFile attaches the staging file to a handle that was created before the
// file was opened. Handles are built first so the staging open can take the
// open ref and the rename registration for them in one step.
func (h *IRODSFSClientBufferedStagedHandle) setFile(file *os.File) {
	h.mu.Lock()
	h.file = file
	h.mu.Unlock()
}

func (h *IRODSFSClientBufferedStagedHandle) GetID() string {
	return h.id
}

func (h *IRODSFSClientBufferedStagedHandle) GetEntry() *irodsclient_fs.Entry {
	return h.entry
}

func (h *IRODSFSClientBufferedStagedHandle) GetOpenMode() irodsclient_types.FileOpenMode {
	return h.openMode
}

func (h *IRODSFSClientBufferedStagedHandle) IsReadMode() bool {
	return h.openMode.IsRead()
}

func (h *IRODSFSClientBufferedStagedHandle) IsWriteMode() bool {
	return h.openMode.IsWrite()
}

func (h *IRODSFSClientBufferedStagedHandle) GetAvailable(offset int64) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	info, err := h.file.Stat()
	if err != nil {
		return -1
	}

	available := info.Size() - offset
	if available < 0 {
		return 0
	}
	return available
}

func (h *IRODSFSClientBufferedStagedHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	if !h.openMode.IsRead() {
		return 0, ErrNotReadMode
	}

	defer util.StackTraceFromPanic(h.logger)

	h.mu.Lock()
	defer h.mu.Unlock()

	n, err := h.file.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
		return n, err
	}
	return n, err
}

func (h *IRODSFSClientBufferedStagedHandle) WriteAt(data []byte, offset int64) (int, error) {
	if !h.openMode.IsWrite() {
		return 0, ErrNotWriteMode
	}

	defer util.StackTraceFromPanic(h.logger)

	h.mu.Lock()
	defer h.mu.Unlock()

	// Charge the growth against the staging quota before writing: the file size
	// is otherwise only counted when the handle is closed, so a single handle
	// could write far past the quota.
	if h.client != nil && h.client.staging != nil {
		if err := h.client.staging.ReserveFileGrowth(h.irodsPath, offset+int64(len(data))); err != nil {
			return 0, err
		}
	}

	n, err := h.file.WriteAt(data, offset)
	if err != nil {
		return n, err
	}

	// Update entry size if file grew
	newEnd := offset + int64(n)
	if newEnd > h.entry.Size {
		h.entry.Size = newEnd
	}

	return n, nil
}

func (h *IRODSFSClientBufferedStagedHandle) Truncate(size int64) error {
	defer util.StackTraceFromPanic(h.logger)

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.client != nil && h.client.staging != nil {
		if err := h.client.staging.ReserveFileGrowth(h.irodsPath, size); err != nil {
			return err
		}
	}

	if err := h.file.Truncate(size); err != nil {
		return err
	}

	h.entry.Size = size
	return nil
}

func (h *IRODSFSClientBufferedStagedHandle) Flush() error {
	defer util.StackTraceFromPanic(h.logger)

	h.mu.Lock()
	defer h.mu.Unlock()

	return h.file.Sync()
}

// UpdateStagingPath updates the iRODS path held by this handle. Called by StagingFS.Rename
// to keep the handle's path in sync after a rename while the handle is open.
func (h *IRODSFSClientBufferedStagedHandle) UpdateStagingPath(newPath string) {
	h.mu.Lock()
	oldPath := h.irodsPath
	h.irodsPath = newPath
	h.mu.Unlock()

	// locks are keyed by path, so they have to follow the file
	moveFileLocks(h.fileLockManager(), oldPath, newPath)
}

// Getlk returns a lock that conflicts with the given lock, or nil if the lock
// can be acquired
func (h *IRODSFSClientBufferedStagedHandle) Getlk(lock *FileLock) (*FileLock, error) {
	defer util.StackTraceFromPanic(h.logger)

	return testFileLock(h.fileLockManager(), h.currentPath(), h.id, lock)
}

// Setlk acquires or releases a lock without waiting. It returns
// ErrFileLockConflict if the lock is held by another owner.
func (h *IRODSFSClientBufferedStagedHandle) Setlk(lock *FileLock) error {
	defer util.StackTraceFromPanic(h.logger)

	return setFileLock(h.fileLockManager(), h.currentPath(), h.id, lock)
}

// Setlkw acquires a lock, waiting until it becomes available or the context is
// canceled
func (h *IRODSFSClientBufferedStagedHandle) Setlkw(ctx context.Context, lock *FileLock) error {
	defer util.StackTraceFromPanic(h.logger)

	return setFileLockWait(ctx, h.fileLockManager(), h.currentPath(), h.id, lock)
}

// fileLockManager returns the lock manager shared by every handle of the client
func (h *IRODSFSClientBufferedStagedHandle) fileLockManager() *FileLockManager {
	if h.client == nil {
		return nil
	}

	return h.client.GetFileLockManager()
}

// currentPath returns the iRODS path the handle currently points at, which
// changes when the file is renamed while the handle is open
func (h *IRODSFSClientBufferedStagedHandle) currentPath() string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.irodsPath
}

func (h *IRODSFSClientBufferedStagedHandle) Close() error {
	defer util.StackTraceFromPanic(h.logger)

	h.mu.Lock()
	err := h.file.Close()
	currentPath := h.irodsPath
	h.mu.Unlock() // release before calling staging methods to avoid deadlock with Rename

	// locks are released on close, like the kernel does for flock() and OFD locks
	releaseFileLocks(h.fileLockManager(), h.id)

	if err != nil {
		return err
	}

	if h.client != nil {
		if h.client.staging != nil {
			// Update size tracking to reflect all bytes written via WriteAt.
			h.client.staging.NotifyFileClosed(currentPath)
			// Releases whatever this handle actually holds, including after a
			// rename, and does nothing for a read-only handle that holds no ref.
			h.client.staging.ReleaseHandle(h)
		}
		// Pass the known entry size to skip the iRODS Stat call inside
		// invalidateFileCacheBlocks. For bulk-uploaded files the file does not
		// exist on iRODS yet (sync is async), so the stat would fail and still
		// incur a blocking network round-trip.
		h.client.invalidateFileCacheBlocksHint(currentPath, h.entry.Size)
	}

	return nil
}
