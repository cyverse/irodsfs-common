package irods

import (
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
		handleLogger = log.NewEntry(log.StandardLogger()).WithFields(log.Fields{
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

func (h *IRODSFSClientBufferedStagedHandle) Close() error {
	defer util.StackTraceFromPanic(h.logger)

	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.file.Close(); err != nil {
		return err
	}

	// Invalidate read cache for this path since local writes may differ
	if h.client != nil {
		h.client.invalidateFileCacheBlocks(h.irodsPath)
	}

	return nil
}
