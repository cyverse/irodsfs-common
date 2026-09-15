package packedfs

import (
	"os"
	"path"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
)

// ErrCrossMountRename is returned when a rename crosses the boundary of a
// packed directory. The two sides live in different namespaces - one on local
// staging disk, one in iRODS - so there is no rename that can move between
// them. Callers map this to EXDEV, which makes the caller copy and unlink, the
// same fallback a rename across filesystems gets.
var ErrCrossMountRename = errors.New("rename crosses a packed directory boundary")

// List returns the entries of a directory inside a mounted packed directory.
func (m *Manager) List(mount *Mount, irodsPath string) ([]*irodsclient_fs.Entry, error) {
	localPath, err := m.LocalPath(mount, irodsPath)
	if err != nil {
		return nil, err
	}

	dirEntries, err := os.ReadDir(localPath)
	if err != nil {
		return nil, translateLocalError(err, irodsPath)
	}

	entries := make([]*irodsclient_fs.Entry, 0, len(dirEntries))
	for _, dirEntry := range dirEntries {
		childPath := path.Join(irodsPath, dirEntry.Name())
		entry, err := m.Stat(mount, childPath)
		if err != nil {
			// A file removed between the listing and the stat is simply gone.
			if irodsclient_types.IsFileNotFoundError(err) {
				continue
			}
			return nil, err
		}
		entries = append(entries, entry)
	}

	// os.ReadDir already sorts by name, but Stat may have dropped entries, so
	// sort again to keep the order a caller sees stable.
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })

	return entries, nil
}

// Stat describes one path inside a mounted packed directory.
func (m *Manager) Stat(mount *Mount, irodsPath string) (*irodsclient_fs.Entry, error) {
	localPath, err := m.LocalPath(mount, irodsPath)
	if err != nil {
		return nil, err
	}

	// Follow symlinks so a link reports the size and kind of its target, which
	// is what a caller reading through it will find. A broken link falls back
	// to the link itself rather than disappearing from its directory.
	info, err := os.Stat(localPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, translateLocalError(err, irodsPath)
		}
		info, err = os.Lstat(localPath)
		if err != nil {
			return nil, translateLocalError(err, irodsPath)
		}
	}

	return m.entryFor(irodsPath, info), nil
}

func (m *Manager) entryFor(irodsPath string, info os.FileInfo) *irodsclient_fs.Entry {
	entryType := irodsclient_fs.FileEntry
	size := info.Size()
	if info.IsDir() {
		entryType = irodsclient_fs.DirectoryEntry
		size = 0
	}

	var inodeID int64
	if m.inodeResolver != nil {
		if id, err := m.inodeResolver(irodsPath); err == nil {
			inodeID = int64(id)
		}
	}

	modTime := info.ModTime()
	return &irodsclient_fs.Entry{
		ID:         inodeID,
		Type:       entryType,
		Name:       path.Base(irodsPath),
		Path:       irodsPath,
		Owner:      m.owner,
		Size:       size,
		CreateTime: modTime,
		ModifyTime: modTime,
		AccessTime: modTime,
	}
}

// ExistsDir reports whether a directory exists inside the mount.
func (m *Manager) ExistsDir(mount *Mount, irodsPath string) bool {
	localPath, err := m.LocalPath(mount, irodsPath)
	if err != nil {
		return false
	}
	info, err := os.Stat(localPath)
	return err == nil && info.IsDir()
}

// ExistsFile reports whether a regular file exists inside the mount.
func (m *Manager) ExistsFile(mount *Mount, irodsPath string) bool {
	localPath, err := m.LocalPath(mount, irodsPath)
	if err != nil {
		return false
	}
	info, err := os.Stat(localPath)
	return err == nil && !info.IsDir()
}

// MakeDir creates a directory inside the mount.
func (m *Manager) MakeDir(mount *Mount, irodsPath string, recurse bool) error {
	localPath, err := m.LocalPath(mount, irodsPath)
	if err != nil {
		return err
	}

	if recurse {
		err = os.MkdirAll(localPath, 0755)
	} else {
		err = os.Mkdir(localPath, 0755)
	}
	if err != nil {
		return translateLocalError(err, irodsPath)
	}

	mount.MarkDirty()
	return nil
}

// RemoveDir removes a directory inside the mount.
func (m *Manager) RemoveDir(mount *Mount, irodsPath string, recurse bool, force bool) error {
	localPath, err := m.LocalPath(mount, irodsPath)
	if err != nil {
		return err
	}

	// Removing the mount root itself would leave the mount pointing at nothing.
	// It is handled by the caller, which unmounts and deletes the archive.
	if localPath == mount.LocalPath {
		return errors.Newf("cannot remove the root of packed directory %q through this path", mount.Root)
	}

	if recurse {
		err = os.RemoveAll(localPath)
	} else {
		err = os.Remove(localPath)
	}
	if err != nil {
		if force && os.IsNotExist(err) {
			return nil
		}
		return translateLocalError(err, irodsPath)
	}

	mount.MarkDirty()
	return nil
}

// RemoveFile removes a file inside the mount.
func (m *Manager) RemoveFile(mount *Mount, irodsPath string, force bool) error {
	localPath, err := m.LocalPath(mount, irodsPath)
	if err != nil {
		return err
	}

	if err := os.Remove(localPath); err != nil {
		if force && os.IsNotExist(err) {
			return nil
		}
		return translateLocalError(err, irodsPath)
	}

	mount.MarkDirty()
	return nil
}

// Rename moves a path within one mount. Both sides must be in the same mount;
// otherwise the caller gets ErrCrossMountRename and falls back to copy-unlink.
func (m *Manager) Rename(mount *Mount, srcPath string, destPath string) error {
	srcLocal, err := m.LocalPath(mount, srcPath)
	if err != nil {
		return err
	}
	destLocal, err := m.LocalPath(mount, destPath)
	if err != nil {
		return errors.Wrapf(ErrCrossMountRename, "%q -> %q", srcPath, destPath)
	}

	if err := os.MkdirAll(filepath.Dir(destLocal), 0755); err != nil {
		return translateLocalError(err, destPath)
	}
	if err := os.Rename(srcLocal, destLocal); err != nil {
		return translateLocalError(err, srcPath)
	}

	mount.MarkDirty()
	return nil
}

// TruncateFile sets the length of a file inside the mount.
func (m *Manager) TruncateFile(mount *Mount, irodsPath string, size int64) error {
	localPath, err := m.LocalPath(mount, irodsPath)
	if err != nil {
		return err
	}

	if err := os.Truncate(localPath, size); err != nil {
		return translateLocalError(err, irodsPath)
	}

	mount.MarkDirty()
	return nil
}

// OpenFile opens a file inside the mount and returns the local file together
// with the entry describing it.
//
// The returned *os.File is the real backing file: reads and writes go straight
// to staging disk with no iRODS round trip, which is the whole point of holding
// these directories locally.
func (m *Manager) OpenFile(mount *Mount, irodsPath string, mode irodsclient_types.FileOpenMode) (*os.File, *irodsclient_fs.Entry, error) {
	localPath, err := m.LocalPath(mount, irodsPath)
	if err != nil {
		return nil, nil, err
	}

	flag, writable, err := openFlagFor(mode)
	if err != nil {
		return nil, nil, err
	}

	if writable {
		if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
			return nil, nil, translateLocalError(err, irodsPath)
		}
	}

	file, err := os.OpenFile(localPath, flag, 0644)
	if err != nil {
		return nil, nil, translateLocalError(err, irodsPath)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, translateLocalError(err, irodsPath)
	}

	if writable {
		// Mark on open rather than on write: a handle opened for writing is
		// reason enough to include the tree in the next snapshot, and it keeps
		// the hot write path free of bookkeeping.
		mount.MarkDirty()
	}

	return file, m.entryFor(irodsPath, info), nil
}

// CreateFile creates a file inside the mount and opens it.
func (m *Manager) CreateFile(mount *Mount, irodsPath string, mode irodsclient_types.FileOpenMode) (*os.File, *irodsclient_fs.Entry, error) {
	localPath, err := m.LocalPath(mount, irodsPath)
	if err != nil {
		return nil, nil, err
	}

	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return nil, nil, translateLocalError(err, irodsPath)
	}

	flag, _, err := openFlagFor(mode)
	if err != nil {
		return nil, nil, err
	}

	file, err := os.OpenFile(localPath, flag|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil, translateLocalError(err, irodsPath)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, translateLocalError(err, irodsPath)
	}

	mount.MarkDirty()
	return file, m.entryFor(irodsPath, info), nil
}

// openFlagFor maps an iRODS open mode to local open flags and reports whether
// the handle can write.
func openFlagFor(mode irodsclient_types.FileOpenMode) (int, bool, error) {
	switch mode {
	case irodsclient_types.FileOpenModeReadOnly:
		return os.O_RDONLY, false, nil
	case irodsclient_types.FileOpenModeReadWrite:
		return os.O_RDWR, true, nil
	case irodsclient_types.FileOpenModeWriteOnly:
		return os.O_WRONLY | os.O_CREATE, true, nil
	case irodsclient_types.FileOpenModeWriteTruncate:
		return os.O_WRONLY | os.O_CREATE | os.O_TRUNC, true, nil
	case irodsclient_types.FileOpenModeAppend:
		return os.O_WRONLY | os.O_CREATE | os.O_APPEND, true, nil
	case irodsclient_types.FileOpenModeReadAppend:
		return os.O_RDWR | os.O_CREATE | os.O_APPEND, true, nil
	default:
		return 0, false, errors.Newf("unknown file open mode %q", string(mode))
	}
}

// translateLocalError turns a local filesystem error into the iRODS error the
// rest of the stack already knows how to map to an errno.
func translateLocalError(err error, irodsPath string) error {
	if err == nil {
		return nil
	}

	if os.IsNotExist(err) {
		return irodsclient_types.NewFileNotFoundError(irodsPath)
	}

	return errors.Wrapf(err, "packed directory operation failed for %q", irodsPath)
}

// TouchMount records that a mount changed, for callers that mutate the local
// tree through a handle rather than through this package.
func (m *Manager) TouchMount(mount *Mount) {
	if mount != nil {
		mount.MarkDirty()
	}
}
