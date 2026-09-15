package irods

import (
	"io"
	"os"
	"sync"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/irods/packedfs"
	"github.com/cyverse/irodsfs-common/util"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

var _ IRODSFSFileHandle = (*packedFileHandle)(nil)

// packedFileHandle serves a file inside a mounted packed directory straight from
// the extracted tree on staging disk.
//
// It deliberately does not share the staged handle's bookkeeping: a packed file
// has no staging metadata, is never synced on its own, and is not in the block
// cache, so per-file sync notifications and cache invalidation would all be
// wrong here. What it does keep is quota accounting, so a runaway write inside
// a packed directory fails instead of filling the staging disk.
type packedFileHandle struct {
	id        string
	manager   *packedfs.Manager
	mount     *packedfs.Mount
	file      *os.File
	irodsPath string
	openMode  irodsclient_types.FileOpenMode
	entry     *irodsclient_fs.Entry
	logger    *log.Entry
	mu        sync.Mutex
}

func newPackedFileHandle(manager *packedfs.Manager, mount *packedfs.Mount, file *os.File, irodsPath string, mode irodsclient_types.FileOpenMode, entry *irodsclient_fs.Entry, logger *log.Entry) *packedFileHandle {
	handleID := xid.New().String()

	if logger == nil {
		logger = log.WithField("package", "irods")
	}

	return &packedFileHandle{
		id:        handleID,
		manager:   manager,
		mount:     mount,
		file:      file,
		irodsPath: irodsPath,
		openMode:  mode,
		entry:     entry,
		logger: logger.WithFields(log.Fields{
			"handle_id": handleID,
			"path":      irodsPath,
			"mode":      string(mode),
			"packed":    true,
		}),
	}
}

func (h *packedFileHandle) GetID() string {
	return h.id
}

func (h *packedFileHandle) GetEntry() *irodsclient_fs.Entry {
	return h.entry
}

func (h *packedFileHandle) GetOpenMode() irodsclient_types.FileOpenMode {
	return h.openMode
}

func (h *packedFileHandle) IsReadMode() bool {
	return h.openMode.IsRead()
}

func (h *packedFileHandle) IsWriteMode() bool {
	return h.openMode.IsWrite()
}

func (h *packedFileHandle) GetAvailable(offset int64) int64 {
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

func (h *packedFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
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

func (h *packedFileHandle) WriteAt(data []byte, offset int64) (int, error) {
	if !h.openMode.IsWrite() {
		return 0, ErrNotWriteMode
	}

	defer util.StackTraceFromPanic(h.logger)

	h.mu.Lock()
	defer h.mu.Unlock()

	// Charge the growth before writing, so the quota is checked while the write
	// can still be refused.
	newEnd := offset + int64(len(data))
	if growth := newEnd - h.entry.Size; growth > 0 {
		if err := h.manager.ReserveGrowth(h.mount, growth); err != nil {
			return 0, err
		}
	}

	n, err := h.file.WriteAt(data, offset)
	if err != nil {
		return n, err
	}

	if written := offset + int64(n); written > h.entry.Size {
		h.entry.Size = written
	}

	h.mount.MarkDirty()
	return n, nil
}

func (h *packedFileHandle) Truncate(size int64) error {
	defer util.StackTraceFromPanic(h.logger)

	h.mu.Lock()
	defer h.mu.Unlock()

	if growth := size - h.entry.Size; growth > 0 {
		if err := h.manager.ReserveGrowth(h.mount, growth); err != nil {
			return err
		}
	}

	if err := h.file.Truncate(size); err != nil {
		return err
	}

	if shrink := h.entry.Size - size; shrink > 0 {
		h.manager.ReleaseGrowth(h.mount, shrink)
	}

	h.entry.Size = size
	h.mount.MarkDirty()
	return nil
}

func (h *packedFileHandle) Flush() error {
	defer util.StackTraceFromPanic(h.logger)

	h.mu.Lock()
	defer h.mu.Unlock()

	return h.file.Sync()
}

func (h *packedFileHandle) Close() error {
	defer util.StackTraceFromPanic(h.logger)

	h.mu.Lock()
	defer h.mu.Unlock()

	// Closing is all there is to do: the bytes are already on staging disk, and
	// they reach iRODS with the next snapshot or at session release.
	return h.file.Close()
}
