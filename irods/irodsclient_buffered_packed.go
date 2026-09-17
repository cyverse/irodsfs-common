package irods

import (
	"os"
	"path"
	"time"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/irods/packedfs"
)

// This file holds everything the buffered client does for packed directories:
// directories configured to live in iRODS as one archive data object instead of
// as a collection of very many small files.
//
// While a session uses such a directory it is extracted on staging disk, and
// every operation below is served from there. The archive crosses the wire only
// on the first access, on a snapshot, and at session release.

// packedEnabled reports whether packed directory handling is active.
func (c *IRODSFSClientBuffered) packedEnabled() bool {
	return c.packed != nil
}

// packedConfig returns the packed directory configuration, or nil.
func (c *IRODSFSClientBuffered) packedConfig() *packedfs.Config {
	if !c.packedEnabled() {
		return nil
	}
	return c.packed.Config()
}

// packedResolve returns the mount serving irodsPath, mounting it if needed.
//
// matched reports that irodsPath lies inside a packed directory, so the caller
// must not fall through to its normal iRODS path even when an error came back.
// A directory that iRODS does not hold is reported as a plain not-found, since
// a lookup must not bring one into existence; create is set only by operations
// that legitimately do.
func (c *IRODSFSClientBuffered) packedResolve(irodsPath string, create bool) (mount *packedfs.Mount, matched bool, err error) {
	if !c.packedEnabled() {
		return nil, false, nil
	}

	mount, matched, err = c.packed.Resolve(irodsPath, create)
	if err != nil {
		if errors.Is(err, packedfs.ErrNoPackedDirectory) {
			return nil, matched, irodsclient_types.NewFileNotFoundError(irodsPath)
		}
		return nil, matched, err
	}

	return mount, matched, nil
}

// packedIsRoot reports whether irodsPath is a packed directory itself, rather
// than something inside one.
func (c *IRODSFSClientBuffered) packedIsRoot(irodsPath string) bool {
	config := c.packedConfig()
	if config == nil {
		return false
	}

	root, ok := config.MatchRoot(irodsPath)
	return ok && root == path.Clean(irodsPath)
}

// packedRootEntry describes a packed directory without mounting it, adding the
// stable inode ID this client hands out for staged entries.
func (c *IRODSFSClientBuffered) packedRootEntry(root string) (*irodsclient_fs.Entry, bool) {
	entry, ok := c.packed.RootEntry(root)
	if !ok {
		return nil, false
	}

	c.packedApplyInodeID(entry)
	return entry, true
}

// packedDirEntryFrom turns an archive data object found in a listing into the
// directory entry that callers should see in its place.
func (c *IRODSFSClientBuffered) packedDirEntryFrom(root string, archiveEntry *irodsclient_fs.Entry) *irodsclient_fs.Entry {
	entry := &irodsclient_fs.Entry{
		ID:   archiveEntry.ID,
		Type: irodsclient_fs.DirectoryEntry,
		Name: path.Base(root),
		Path: root,
		// A directory reports no size of its own; the archive's size is not the
		// size of the tree inside it either way.
		Size:       0,
		Owner:      archiveEntry.Owner,
		CreateTime: archiveEntry.CreateTime,
		ModifyTime: archiveEntry.ModifyTime,
		AccessTime: archiveEntry.AccessTime,
	}

	c.packedApplyInodeID(entry)

	if entry.ModifyTime.IsZero() {
		now := time.Now()
		entry.CreateTime = now
		entry.ModifyTime = now
		entry.AccessTime = now
	}

	return entry
}

// packedApplyInodeID gives a synthesized entry the same stable inode ID the
// rest of the client issues, so a packed directory keeps its identity across
// listings.
func (c *IRODSFSClientBuffered) packedApplyInodeID(entry *irodsclient_fs.Entry) {
	if c.inodeManager == nil {
		return
	}

	if inodeID, err := c.inodeManager.CreateOrGetInodeIDForStagingEntry(entry.Path); err == nil {
		entry.ID = int64(inodeID)
	}
}

// packedRewriteListing replaces the archive data objects in a listing with the
// directories they hold, so a caller sees ".venv" where iRODS stores
// ".venv.mount.tar".
func (c *IRODSFSClientBuffered) packedRewriteListing(dirPath string, entries []*irodsclient_fs.Entry) []*irodsclient_fs.Entry {
	config := c.packedConfig()
	if config == nil {
		return entries
	}

	rewritten := make([]*irodsclient_fs.Entry, 0, len(entries))
	for _, entry := range entries {
		// An archive half-way through an upload is not a user-visible file.
		if packedfs.IsTransientArchiveName(entry.Name) {
			continue
		}

		if !config.IsArchiveName(entry.Name) {
			rewritten = append(rewritten, entry)
			continue
		}

		root := path.Join(dirPath, entry.Name[:len(entry.Name)-len(config.ArchiveSuffix())])
		rewritten = append(rewritten, c.packedDirEntryFrom(root, entry))
	}

	// A packed directory that is mounted but whose archive does not exist yet -
	// one created during this session - has nothing in the backend listing to
	// rewrite, so add it here.
	for _, mount := range c.packed.Mounts() {
		if path.Dir(mount.Root) != path.Clean(dirPath) {
			continue
		}
		if containsEntryPath(rewritten, mount.Root) {
			continue
		}
		if entry, err := c.packed.Stat(mount, mount.Root); err == nil {
			rewritten = append(rewritten, entry)
		}
	}

	return rewritten
}

func containsEntryPath(entries []*irodsclient_fs.Entry, irodsPath string) bool {
	for _, entry := range entries {
		if entry.Path == irodsPath {
			return true
		}
	}
	return false
}

// packedRenameDir handles a rename where either side is a packed directory.
func (c *IRODSFSClientBuffered) packedRenameDir(srcPath string, destPath string) (handled bool, err error) {
	config := c.packedConfig()
	if config == nil {
		return false, nil
	}

	srcRoot, srcMatched := config.MatchRoot(srcPath)
	_, destMatched := config.MatchRoot(destPath)
	if !srcMatched && !destMatched {
		return false, nil
	}

	// Renaming a packed directory itself moves its archive.
	if srcMatched && srcRoot == path.Clean(srcPath) {
		mount, _, err := c.packedResolve(srcPath, false)
		if err != nil {
			return true, err
		}
		return true, c.packed.RenameRoot(mount, destPath)
	}

	return c.packedRenameWithin(srcPath, destPath)
}

// packedRenameWithin handles a rename of a path inside a packed directory.
func (c *IRODSFSClientBuffered) packedRenameWithin(srcPath string, destPath string) (handled bool, err error) {
	config := c.packedConfig()
	if config == nil {
		return false, nil
	}

	srcRoot, srcMatched := config.MatchRoot(srcPath)
	destRoot, destMatched := config.MatchRoot(destPath)
	if !srcMatched && !destMatched {
		return false, nil
	}

	// One side inside a packed directory and the other outside means the two
	// live in different namespaces, one on staging disk and one in iRODS. There
	// is no rename that spans them, so the caller copies and unlinks instead.
	if srcRoot != destRoot {
		return true, errors.Wrapf(packedfs.ErrCrossMountRename, "%q -> %q", srcPath, destPath)
	}

	mount, _, err := c.packedResolve(srcPath, false)
	if err != nil {
		return true, err
	}

	return true, c.packed.Rename(mount, srcPath, destPath)
}

// packedOpen opens a file inside a packed directory.
func (c *IRODSFSClientBuffered) packedOpen(irodsPath string, mode irodsclient_types.FileOpenMode, create bool) (IRODSFSFileHandle, bool, error) {
	mount, matched, err := c.packedResolve(irodsPath, create)
	if !matched {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}

	var file *os.File
	var entry *irodsclient_fs.Entry
	if create {
		file, entry, err = c.packed.CreateFile(mount, irodsPath, mode)
	} else {
		file, entry, err = c.packed.OpenFile(mount, irodsPath, mode)
	}
	if err != nil {
		return nil, true, err
	}

	return newPackedFileHandle(c.packed, mount, file, irodsPath, mode, entry, c.GetFileLockManager(), c.logger), true, nil
}

// packedList serves a directory listing from inside a packed directory.
func (c *IRODSFSClientBuffered) packedList(dirPath string) ([]*irodsclient_fs.Entry, bool, error) {
	mount, matched, err := c.packedResolve(dirPath, false)
	if !matched {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}

	entries, err := c.packed.List(mount, dirPath)
	return entries, true, err
}

// packedStat describes a path in or at a packed directory.
func (c *IRODSFSClientBuffered) packedStat(filePath string) (*irodsclient_fs.Entry, bool, error) {
	config := c.packedConfig()
	if config == nil {
		return nil, false, nil
	}

	if _, matched := config.MatchRoot(filePath); !matched {
		return nil, false, nil
	}

	// The directory itself is answered from the archive's metadata, without
	// paying for an extraction that a stat does not need.
	if c.packedIsRoot(filePath) {
		if entry, ok := c.packedRootEntry(filePath); ok {
			return entry, true, nil
		}
		return nil, true, irodsclient_types.NewFileNotFoundError(filePath)
	}

	mount, _, err := c.packedResolve(filePath, false)
	if err != nil {
		return nil, true, err
	}

	entry, err := c.packed.Stat(mount, filePath)
	return entry, true, err
}

// packedExistsDir answers a directory existence check for a packed path.
func (c *IRODSFSClientBuffered) packedExistsDir(dirPath string) (handled bool, exists bool) {
	config := c.packedConfig()
	if config == nil {
		return false, false
	}

	if _, matched := config.MatchRoot(dirPath); !matched {
		return false, false
	}

	if c.packedIsRoot(dirPath) {
		_, ok := c.packedRootEntry(dirPath)
		return true, ok
	}

	mount, _, err := c.packedResolve(dirPath, false)
	if err != nil {
		return true, false
	}
	return true, c.packed.ExistsDir(mount, dirPath)
}

// packedExistsFile answers a file existence check for a packed path.
func (c *IRODSFSClientBuffered) packedExistsFile(filePath string) (handled bool, exists bool) {
	config := c.packedConfig()
	if config == nil {
		return false, false
	}

	if _, matched := config.MatchRoot(filePath); !matched {
		return false, false
	}

	// The directory itself is never a file.
	if c.packedIsRoot(filePath) {
		return true, false
	}

	mount, _, err := c.packedResolve(filePath, false)
	if err != nil {
		return true, false
	}
	return true, c.packed.ExistsFile(mount, filePath)
}

// packedRemoveFile removes a file inside a packed directory.
func (c *IRODSFSClientBuffered) packedRemoveFile(irodsPath string, force bool) (handled bool, err error) {
	mount, matched, err := c.packedResolve(irodsPath, false)
	if !matched {
		return false, nil
	}
	if err != nil {
		if force && irodsclient_types.IsFileNotFoundError(err) {
			return true, nil
		}
		return true, err
	}

	return true, c.packed.RemoveFile(mount, irodsPath, force)
}

// packedRemoveDir removes a directory in or at a packed directory.
func (c *IRODSFSClientBuffered) packedRemoveDir(irodsPath string, recurse bool, force bool) (handled bool, err error) {
	config := c.packedConfig()
	if config == nil {
		return false, nil
	}

	if _, matched := config.MatchRoot(irodsPath); !matched {
		return false, nil
	}

	// Removing the packed directory itself drops the tree and both of its
	// representations in iRODS, without packing a tree that is being deleted.
	if c.packedIsRoot(irodsPath) {
		mount, _, err := c.packedResolve(irodsPath, false)
		if err != nil {
			if force && irodsclient_types.IsFileNotFoundError(err) {
				return true, nil
			}
			return true, err
		}
		return true, c.packed.Remove(mount)
	}

	mount, _, err := c.packedResolve(irodsPath, false)
	if err != nil {
		if force && irodsclient_types.IsFileNotFoundError(err) {
			return true, nil
		}
		return true, err
	}

	return true, c.packed.RemoveDir(mount, irodsPath, recurse, force)
}

// packedMakeDir creates a directory in or at a packed directory.
func (c *IRODSFSClientBuffered) packedMakeDir(irodsPath string, recurse bool) (handled bool, err error) {
	config := c.packedConfig()
	if config == nil {
		return false, nil
	}

	if _, matched := config.MatchRoot(irodsPath); !matched {
		return false, nil
	}

	// Creating the packed directory itself is exactly a mount with create
	// intent: an empty local tree that the first pack turns into an archive.
	mount, _, err := c.packedResolve(irodsPath, true)
	if err != nil {
		return true, err
	}

	if c.packedIsRoot(irodsPath) {
		return true, nil
	}

	return true, c.packed.MakeDir(mount, irodsPath, recurse)
}

// packedTruncateFile truncates a file inside a packed directory.
func (c *IRODSFSClientBuffered) packedTruncateFile(irodsPath string, size int64) (handled bool, err error) {
	mount, matched, err := c.packedResolve(irodsPath, false)
	if !matched {
		return false, nil
	}
	if err != nil {
		return true, err
	}

	return true, c.packed.TruncateFile(mount, irodsPath, size)
}
