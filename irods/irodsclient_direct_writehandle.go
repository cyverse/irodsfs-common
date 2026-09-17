package irods

import (
	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
)

// sharedWriteHandle is one iRODS write handle that several file handles use.
//
// iRODS refuses a second write open of a data object with HIERARCHY_ERROR, but
// POSIX callers open a file for writing as many times as they like - a build
// tool taking a lock on a file it also writes, for one. Those opens share the
// one handle iRODS allows, and it is closed when the last of them is.
//
// Sharing is safe because every read and write carries its own offset and
// go-irodsclient serializes the calls on the handle. What the handles do not
// get is an offset of their own, which they never had here: the FUSE layer
// above always passes an explicit offset.
type sharedWriteHandle struct {
	handle   *irodsclient_fs.FileHandle
	openMode irodsclient_types.FileOpenMode
	refCount int
}

// satisfies returns true if this handle can serve an open in the given mode
func (s *sharedWriteHandle) satisfies(mode irodsclient_types.FileOpenMode) bool {
	if mode.IsRead() && !s.openMode.IsRead() {
		return false
	}

	return !mode.IsWrite() || s.openMode.IsWrite()
}

// acquireWriteHandle returns the write handle of a path, opening it with open
// when no other file handle holds it. The returned handle is shared, so it must
// be given back with releaseWriteHandle rather than closed.
func (c *IRODSFSClientDirect) acquireWriteHandle(path string, mode irodsclient_types.FileOpenMode, open func() (*irodsclient_fs.FileHandle, error)) (*irodsclient_fs.FileHandle, error) {
	c.writeHandleMutex.Lock()
	defer c.writeHandleMutex.Unlock()

	if shared, ok := c.writeHandles[path]; ok {
		if !shared.satisfies(mode) {
			return nil, errors.Errorf("file %q is already open for %q, which does not serve a %q open", path, shared.openMode, mode)
		}

		if mode.Truncate() {
			if err := shared.handle.Truncate(0); err != nil {
				return nil, errors.Wrapf(err, "failed to truncate file %q that is already open", path)
			}
		}

		shared.refCount++
		return shared.handle, nil
	}

	handle, err := open()
	if err != nil {
		return nil, err
	}

	c.writeHandles[path] = &sharedWriteHandle{
		handle:   handle,
		openMode: mode,
		refCount: 1,
	}

	return handle, nil
}

// releaseWriteHandle gives back a shared write handle, closing it once no file
// handle holds it any more. It reports whether the handle was shared at all.
func (c *IRODSFSClientDirect) releaseWriteHandle(path string, handle *irodsclient_fs.FileHandle) (bool, error) {
	c.writeHandleMutex.Lock()

	shared, ok := c.writeHandles[path]
	if !ok || shared.handle != handle {
		// not a shared handle, or the path now holds a different one, which
		// happens when the file was reopened after a rename
		c.writeHandleMutex.Unlock()
		return false, nil
	}

	shared.refCount--
	if shared.refCount > 0 {
		c.writeHandleMutex.Unlock()
		return true, nil
	}

	delete(c.writeHandles, path)
	c.writeHandleMutex.Unlock()

	return true, handle.Close()
}

// renameWriteHandle follows an open write handle to the path the file was
// renamed to, so that a later open of that path shares it
func (c *IRODSFSClientDirect) renameWriteHandle(oldPath string, newPath string) {
	if oldPath == newPath {
		return
	}

	c.writeHandleMutex.Lock()
	defer c.writeHandleMutex.Unlock()

	shared, ok := c.writeHandles[oldPath]
	if !ok {
		return
	}

	delete(c.writeHandles, oldPath)
	c.writeHandles[newPath] = shared
}
