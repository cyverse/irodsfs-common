package packedfs

import (
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

// ErrNotMounted is returned when an operation names a packed directory that is
// not currently mounted.
var ErrNotMounted = errors.New("packed directory is not mounted")

// ErrNoPackedDirectory is returned when a read resolves a packed directory that
// iRODS does not hold, in either representation. A lookup must not bring such a
// directory into existence, so the caller reports it as not found.
var ErrNoPackedDirectory = errors.New("packed directory does not exist")

// uploadTempSuffix marks the data object a pack uploads before it takes the
// archive's real name. Nothing outside a pack in progress should see it.
const uploadTempSuffix = ".uploading"

// Backend is the part of the iRODS client a Manager needs. It is satisfied by
// IRODSFSClientDirect and kept narrow so the manager can be tested without a
// server.
type Backend interface {
	Stat(irodsPath string) (*irodsclient_fs.Entry, error)
	List(irodsPath string) ([]*irodsclient_fs.Entry, error)
	ExistsDir(irodsPath string) bool
	ExistsFile(irodsPath string) bool
	MakeDir(irodsPath string, recurse bool) error
	RemoveFile(irodsPath string, force bool) error
	RemoveDir(irodsPath string, recurse bool, force bool) error
	RenameFileToFile(srcPath string, destPath string) error
	DownloadFileParallel(irodsPath string, localPath string, taskNum int, callback irodsclient_common.TransferTrackerCallback) error
	UploadFileParallel(localPath string, irodsPath string, taskNum int, callback irodsclient_common.TransferTrackerCallback) error
}

// Quota accounts for the staging disk a mounted tree occupies. StagingFS
// satisfies it; a nil Quota disables accounting.
type Quota interface {
	// ReserveSpace charges size bytes against the staging quota, returning an
	// error when the quota cannot cover it. A mounted tree is never evictable,
	// so a full staging area makes writes fail rather than silently uploading
	// the deferred files one by one.
	ReserveSpace(size int64) error
	// ReleaseSpace returns bytes charged by ReserveSpace.
	ReleaseSpace(size int64)
}

// ManagerConfig wires a Manager to its session.
type ManagerConfig struct {
	Config *Config
	// Backend talks to iRODS.
	Backend Backend
	// Quota accounts for staging disk usage. May be nil.
	Quota Quota
	// LocalRootPath is the directory holding extracted trees, normally
	// "{staging}/{sessionID}/packed".
	LocalRootPath string
	// Owner is the iRODS user that synthesized entries are attributed to.
	Owner string
	// InodeResolver assigns stable inode IDs to synthesized entries. May be nil.
	InodeResolver func(irodsPath string) (uint64, error)
	// TransferTaskNum is the parallelism used for archive transfers.
	TransferTaskNum int
	Logger          *log.Entry
}

// Manager owns every packed directory mounted for one session.
type Manager struct {
	config          *Config
	backend         Backend
	quota           Quota
	localRootPath   string
	owner           string
	inodeResolver   func(string) (uint64, error)
	transferTaskNum int
	logger          *log.Entry

	mu     sync.RWMutex
	mounts map[string]*Mount
	// mountLocks serializes mounting per root so concurrent first accesses
	// extract the archive once.
	mountLocks map[string]*sync.Mutex

	// packSem caps concurrent packs, which are CPU and disk bound.
	packSem chan struct{}

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewManager creates a Manager. It returns nil when packed directories are
// disabled, so callers can treat a nil Manager as "feature off".
func NewManager(managerConfig *ManagerConfig) (*Manager, error) {
	if managerConfig == nil || managerConfig.Config == nil || !managerConfig.Config.Enabled {
		return nil, nil
	}

	config := managerConfig.Config
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if managerConfig.Backend == nil {
		return nil, errors.New("packed directory manager requires a backend")
	}
	if managerConfig.LocalRootPath == "" {
		return nil, errors.New("packed directory manager requires a local root path")
	}

	if err := os.MkdirAll(managerConfig.LocalRootPath, 0755); err != nil {
		return nil, errors.Wrapf(err, "failed to create packed directory root %q", managerConfig.LocalRootPath)
	}

	taskNum := managerConfig.TransferTaskNum
	if taskNum <= 0 {
		taskNum = 4
	}

	logger := managerConfig.Logger
	if logger == nil {
		logger = log.WithField("package", "packedfs")
	}

	manager := &Manager{
		config:          config,
		backend:         managerConfig.Backend,
		quota:           managerConfig.Quota,
		localRootPath:   managerConfig.LocalRootPath,
		owner:           managerConfig.Owner,
		inodeResolver:   managerConfig.InodeResolver,
		transferTaskNum: taskNum,
		logger:          logger,
		mounts:          map[string]*Mount{},
		mountLocks:      map[string]*sync.Mutex{},
		packSem:         make(chan struct{}, config.ConcurrentPackLimit),
		stopCh:          make(chan struct{}),
	}

	manager.startSnapshotWorker()

	return manager, nil
}

// Config exposes the configuration, so callers can match paths without holding
// a second copy.
func (m *Manager) Config() *Config {
	return m.config
}

// localPathFor maps an iRODS path to its place under the local root, mirroring
// the layout StagingFS uses so the tree stays recognizable on disk.
func (m *Manager) localPathFor(irodsPath string) string {
	return filepath.Join(m.localRootPath, filepath.FromSlash(strings.TrimPrefix(path.Clean(irodsPath), "/")))
}

// LocalPath returns the local file backing an iRODS path inside a mount.
func (m *Manager) LocalPath(mount *Mount, irodsPath string) (string, error) {
	relPath, err := relativeTo(mount.Root, irodsPath)
	if err != nil {
		return "", err
	}
	if relPath == "" {
		return mount.LocalPath, nil
	}
	return filepath.Join(mount.LocalPath, filepath.FromSlash(relPath)), nil
}

// relativeTo returns the portion of irodsPath below root.
func relativeTo(root string, irodsPath string) (string, error) {
	root = path.Clean(root)
	irodsPath = path.Clean(irodsPath)

	if irodsPath == root {
		return "", nil
	}
	if !strings.HasPrefix(irodsPath, root+"/") {
		return "", errors.Newf("path %q is not inside packed directory %q", irodsPath, root)
	}

	return strings.TrimPrefix(irodsPath, root+"/"), nil
}

// Lookup returns the mount serving irodsPath when one is already mounted. It
// never triggers a mount, so it is safe on hot paths.
func (m *Manager) Lookup(irodsPath string) (*Mount, bool) {
	if m == nil {
		return nil, false
	}

	root, ok := m.config.MatchRoot(irodsPath)
	if !ok {
		return nil, false
	}

	m.mu.RLock()
	mount, found := m.mounts[root]
	m.mu.RUnlock()

	if !found || !mount.Usable() {
		return nil, false
	}
	return mount, true
}

// Resolve returns the mount serving irodsPath, mounting it on first access.
// The bool reports whether irodsPath is inside a packed directory at all, so a
// caller can fall through to its normal path when it is not.
//
// Mounting downloads and extracts the whole archive. That cost is paid once per
// session and per directory, and it is what lets every later operation run
// against local disk with no iRODS round trip at all.
//
// create must be true only for an operation that would itself bring the
// directory into existence, such as a mkdir or a file creation. A read passes
// false and gets ErrNoPackedDirectory when iRODS holds nothing, because a
// lookup of a path that does not exist must not create it.
func (m *Manager) Resolve(irodsPath string, create bool) (*Mount, bool, error) {
	if m == nil {
		return nil, false, nil
	}

	root, ok := m.config.MatchRoot(irodsPath)
	if !ok {
		return nil, false, nil
	}

	mount, err := m.EnsureMounted(root, create)
	if err != nil {
		return nil, true, err
	}
	return mount, true, nil
}

// EnsureMounted mounts the packed directory at root if it is not mounted yet.
// See Resolve for what create means.
func (m *Manager) EnsureMounted(root string, create bool) (*Mount, error) {
	root = path.Clean(root)

	m.mu.RLock()
	mount, found := m.mounts[root]
	m.mu.RUnlock()
	if found && mount.Usable() {
		return mount, nil
	}

	// Serialize per root so concurrent first accesses extract once.
	lock := m.mountLockFor(root)
	lock.Lock()
	defer lock.Unlock()

	m.mu.RLock()
	mount, found = m.mounts[root]
	m.mu.RUnlock()
	if found && mount.Usable() {
		return mount, nil
	}

	mount, err := m.mount(root, create)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.mounts[root] = mount
	m.mu.Unlock()

	return mount, nil
}

func (m *Manager) mountLockFor(root string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()

	lock, ok := m.mountLocks[root]
	if !ok {
		lock = &sync.Mutex{}
		m.mountLocks[root] = lock
	}
	return lock
}

// mount materializes the directory at root as a local tree. The caller holds
// the per-root mount lock.
func (m *Manager) mount(root string, create bool) (*Mount, error) {
	archivePath := m.config.ArchivePath(root)
	localPath := m.localPathFor(root)

	mount := &Mount{
		Root:        root,
		LocalPath:   localPath,
		ArchivePath: archivePath,
		state:       MountStateMounting,
	}

	logger := m.logger.WithFields(log.Fields{"root": root, "archive": archivePath})

	// A leftover tree from a crashed session would merge into the fresh
	// extraction and resurrect deleted files, so start from nothing.
	if err := os.RemoveAll(localPath); err != nil {
		return nil, errors.Wrapf(err, "failed to clear stale local tree %q", localPath)
	}

	size, legacy, err := m.materialize(mount, create, logger)
	if err != nil {
		// A directory that simply does not exist is an ordinary lookup miss, not
		// a mount failure worth recording or cleaning up after.
		if errors.Is(err, ErrNoPackedDirectory) {
			return nil, err
		}
		mount.mountErr = err
		mount.setState(MountStateFailed)
		// Leave nothing half-extracted behind for the next attempt.
		if removeErr := os.RemoveAll(localPath); removeErr != nil {
			logger.WithError(removeErr).Warn("failed to clean up after a failed mount")
		}
		return nil, err
	}

	mount.mu.Lock()
	mount.state = MountStateMounted
	mount.reservedSize = size
	mount.mountedAt = time.Now()
	mount.legacyCollection = legacy
	// A directory restored from an archive matches what iRODS holds, so it is
	// clean. One created fresh, or migrated from a collection, has nothing in
	// the archive yet and must be uploaded.
	mount.dirty = legacy || size == 0
	mount.mu.Unlock()

	logger.Infof("Mounted packed directory (%d bytes on staging disk)", size)
	return mount, nil
}

// materialize fills the local tree from whichever representation iRODS holds
// and returns the staging bytes charged plus whether a legacy collection was
// migrated.
func (m *Manager) materialize(mount *Mount, create bool, logger *log.Entry) (int64, bool, error) {
	entry, err := m.backend.Stat(mount.ArchivePath)
	if err != nil && !irodsclient_types.IsFileNotFoundError(err) {
		return 0, false, errors.Wrapf(err, "failed to stat archive %q", mount.ArchivePath)
	}

	if err == nil && entry.Type == irodsclient_fs.FileEntry {
		size, unpackErr := m.unpackArchive(mount, entry.Size, logger)
		if unpackErr != nil {
			return 0, false, unpackErr
		}
		return size, false, nil
	}

	// No archive. A collection at the same path is a directory written before
	// this feature was enabled, or one recreated by crash recovery uploading
	// staged files one by one. Migrate it: the next pack replaces it.
	if m.backend.ExistsDir(mount.Root) {
		logger.Info("No archive found but a collection exists; migrating it to a packed directory")
		size, downloadErr := m.downloadCollection(mount, logger)
		if downloadErr != nil {
			return 0, false, downloadErr
		}
		return size, true, nil
	}

	// Nothing in iRODS holds this directory. Only an operation that creates it
	// may go on; a lookup reports it missing.
	if !create {
		return 0, false, errors.Wrapf(ErrNoPackedDirectory, "no archive or collection at %q", mount.Root)
	}

	if err := os.MkdirAll(mount.LocalPath, 0755); err != nil {
		return 0, false, errors.Wrapf(err, "failed to create local tree %q", mount.LocalPath)
	}
	return 0, false, nil
}

// unpackArchive downloads the archive and extracts it into the local tree.
func (m *Manager) unpackArchive(mount *Mount, archiveSize int64, logger *log.Entry) (int64, error) {
	if archiveSize > m.config.MaxPackedDirSize {
		return 0, errors.Wrapf(ErrArchiveTooLarge,
			"archive %q is %d bytes, over the %d byte limit",
			mount.ArchivePath, archiveSize, m.config.MaxPackedDirSize)
	}

	tempPath := mount.LocalPath + ".archive." + xid.New().String()
	if err := os.MkdirAll(filepath.Dir(tempPath), 0755); err != nil {
		return 0, errors.Wrapf(err, "failed to create local tree parent for %q", mount.LocalPath)
	}

	// Charge for the archive and for the tree it expands into. For an
	// uncompressed archive, the default, the extracted size is within a few
	// percent of the archive size; a codec is given headroom and the exact
	// figure is reconciled once the extraction has finished.
	estimate := archiveSize + archiveSize*expansionFactor(m.config.Compression)
	if estimate > m.config.MaxPackedDirSize {
		estimate = m.config.MaxPackedDirSize
	}
	if err := m.reserve(estimate); err != nil {
		return 0, err
	}
	charged := estimate

	release := func() {
		m.release(charged)
		os.Remove(tempPath)
	}

	if err := m.backend.DownloadFileParallel(mount.ArchivePath, tempPath, m.transferTaskNum, nil); err != nil {
		release()
		return 0, errors.Wrapf(err, "failed to download archive %q", mount.ArchivePath)
	}

	archiveFile, err := os.Open(tempPath)
	if err != nil {
		release()
		return 0, errors.Wrapf(err, "failed to open downloaded archive %q", tempPath)
	}

	_, unpackErr := Unpack(archiveFile, mount.LocalPath, m.config.Compression, m.config.MaxPackedDirSize)
	archiveFile.Close()
	os.Remove(tempPath)

	if unpackErr != nil {
		m.release(charged)
		return 0, errors.Wrapf(unpackErr, "failed to unpack archive %q", mount.ArchivePath)
	}

	// Reconcile the estimate against what the tree actually occupies. The
	// archive's own bytes are gone now that the temporary file is removed.
	actual := treeSize(mount.LocalPath)
	if err := m.rebalance(charged, actual); err != nil {
		os.RemoveAll(mount.LocalPath)
		return 0, err
	}

	logger.Infof("Unpacked archive of %d bytes into %d bytes of local tree", archiveSize, actual)
	return actual, nil
}

// expansionFactor estimates how much larger an extracted tree is than its
// archive, used only to size the initial quota reservation.
func expansionFactor(compression Compression) int64 {
	if compression == CompressionNone {
		return 1
	}
	return 3
}

// downloadCollection copies an iRODS collection into the local tree, used once
// when migrating a directory that predates packing.
func (m *Manager) downloadCollection(mount *Mount, logger *log.Entry) (int64, error) {
	if err := os.MkdirAll(mount.LocalPath, 0755); err != nil {
		return 0, errors.Wrapf(err, "failed to create local tree %q", mount.LocalPath)
	}

	var charged int64
	cleanup := func() {
		m.release(charged)
		os.RemoveAll(mount.LocalPath)
	}

	// Iterative walk: these trees are deep and wide, and recursion over an
	// untrusted depth is a needless risk.
	queue := []string{mount.Root}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		entries, err := m.backend.List(current)
		if err != nil {
			cleanup()
			return 0, errors.Wrapf(err, "failed to list collection %q", current)
		}

		for _, entry := range entries {
			localPath, err := m.LocalPath(mount, entry.Path)
			if err != nil {
				cleanup()
				return 0, err
			}

			if entry.Type == irodsclient_fs.DirectoryEntry {
				if err := os.MkdirAll(localPath, 0755); err != nil {
					cleanup()
					return 0, errors.Wrapf(err, "failed to create %q", localPath)
				}
				queue = append(queue, entry.Path)
				continue
			}

			if charged+entry.Size > m.config.MaxPackedDirSize {
				cleanup()
				return 0, errors.Wrapf(ErrArchiveTooLarge,
					"collection %q exceeds the %d byte limit", mount.Root, m.config.MaxPackedDirSize)
			}
			if err := m.reserve(entry.Size); err != nil {
				cleanup()
				return 0, err
			}
			charged += entry.Size

			if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
				cleanup()
				return 0, errors.Wrapf(err, "failed to create parent of %q", localPath)
			}
			if err := m.backend.DownloadFileParallel(entry.Path, localPath, m.transferTaskNum, nil); err != nil {
				cleanup()
				return 0, errors.Wrapf(err, "failed to download %q", entry.Path)
			}
		}
	}

	logger.Infof("Migrated collection into a local tree of %d bytes", charged)
	return charged, nil
}

func (m *Manager) reserve(size int64) error {
	if m.quota == nil || size <= 0 {
		return nil
	}
	return m.quota.ReserveSpace(size)
}

func (m *Manager) release(size int64) {
	if m.quota == nil || size <= 0 {
		return
	}
	m.quota.ReleaseSpace(size)
}

// rebalance moves a charge from one size to another without briefly releasing
// the whole reservation, which another mount could take in between.
func (m *Manager) rebalance(from int64, to int64) error {
	switch {
	case to > from:
		if err := m.reserve(to - from); err != nil {
			m.release(from)
			return err
		}
	case to < from:
		m.release(from - to)
	}
	return nil
}

// treeSize sums the regular file bytes under root. Failures are ignored: the
// figure feeds quota accounting, and a partially readable tree should not fail
// a mount that otherwise succeeded.
func treeSize(root string) int64 {
	var total int64
	filepath.WalkDir(root, func(_ string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		info, statErr := entry.Info()
		if statErr != nil || !info.Mode().IsRegular() {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

// ReserveGrowth charges additional staging bytes to a mounted tree, refusing
// the growth when the staging quota cannot cover it.
//
// A mounted tree is not evictable, so this is where a full staging area turns
// into a failed write rather than into a silent per-file upload of the very
// directory that packing exists to keep local.
func (m *Manager) ReserveGrowth(mount *Mount, delta int64) error {
	if m == nil || mount == nil || delta <= 0 {
		return nil
	}

	// Enforce the per-directory cap here as well as at mount time. A tree that
	// grew past it during a session would pack and upload fine and then be
	// refused on the next mount, leaving the directory unreadable; failing the
	// write that crosses the line keeps the directory usable.
	//
	// The check and the charge are one step: concurrent writers that each only
	// read the current size would every one of them see room and every one of
	// them take it. The staging reservation is left outside the lock, since it
	// can block on eviction, and is rolled back if it fails.
	mount.mu.Lock()
	if mount.reservedSize+delta > m.config.MaxPackedDirSize {
		projected := mount.reservedSize + delta
		mount.mu.Unlock()
		return errors.Wrapf(ErrArchiveTooLarge,
			"packed directory %q would reach %d bytes, over the %d byte limit",
			mount.Root, projected, m.config.MaxPackedDirSize)
	}
	mount.reservedSize += delta
	mount.mu.Unlock()

	if err := m.reserve(delta); err != nil {
		mount.mu.Lock()
		mount.reservedSize -= delta
		if mount.reservedSize < 0 {
			mount.reservedSize = 0
		}
		mount.mu.Unlock()
		return err
	}

	return nil
}

// ReleaseGrowth returns staging bytes charged by ReserveGrowth, for a file that
// shrank.
func (m *Manager) ReleaseGrowth(mount *Mount, delta int64) {
	if m == nil || mount == nil || delta <= 0 {
		return
	}

	mount.mu.Lock()
	if delta > mount.reservedSize {
		delta = mount.reservedSize
	}
	mount.reservedSize -= delta
	mount.mu.Unlock()

	m.release(delta)
}

// RootEntry describes a packed directory without mounting it.
//
// Mounting extracts the whole archive, which is far more work than a stat
// needs, and an `ls -l` of a parent directory stats every child. Answering from
// the archive's own metadata is what keeps listing a project cheap when it
// holds a multi-gigabyte virtualenv.
func (m *Manager) RootEntry(root string) (*irodsclient_fs.Entry, bool) {
	if m == nil {
		return nil, false
	}

	root = path.Clean(root)

	// A mounted tree may already differ from the archive, so it wins.
	if mount, ok := m.Lookup(root); ok {
		if entry, err := m.Stat(mount, root); err == nil {
			return entry, true
		}
	}

	if archiveEntry, err := m.backend.Stat(m.config.ArchivePath(root)); err == nil &&
		archiveEntry.Type == irodsclient_fs.FileEntry {
		return &irodsclient_fs.Entry{
			ID:   archiveEntry.ID,
			Type: irodsclient_fs.DirectoryEntry,
			Name: path.Base(root),
			Path: root,
			// A directory has no size of its own, and the archive's size is not
			// the size of the tree inside it either.
			Size:       0,
			Owner:      archiveEntry.Owner,
			CreateTime: archiveEntry.CreateTime,
			ModifyTime: archiveEntry.ModifyTime,
			AccessTime: archiveEntry.AccessTime,
		}, true
	}

	// No archive yet, but a collection written before packing was enabled, or
	// recreated by crash recovery, still makes the directory exist.
	if collectionEntry, err := m.backend.Stat(root); err == nil &&
		collectionEntry.Type == irodsclient_fs.DirectoryEntry {
		return collectionEntry, true
	}

	return nil, false
}
