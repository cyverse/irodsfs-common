package packedfs

import (
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

// IsTransientArchiveName reports whether a base name is a partially uploaded
// archive. A pack uploads under this name and renames it into place, so such an
// object is only ever visible for the moment between the two, and it must not
// be shown as a user file.
func IsTransientArchiveName(name string) bool {
	return strings.Contains(name, uploadTempSuffix+".")
}

// Pack writes the mount's local tree to iRODS as a single archive, leaving the
// tree mounted and usable.
//
// This is the snapshot path: it bounds how much work a crash can lose without
// giving up the local-only speed that packing exists for.
func (m *Manager) Pack(mount *Mount) error {
	return m.packLocked(mount, false)
}

// Unmount packs the tree, uploads it, then removes it from staging disk and
// from the registry. After this the directory exists in iRODS as an archive
// only.
func (m *Manager) Unmount(mount *Mount) error {
	return m.packLocked(mount, true)
}

func (m *Manager) packLocked(mount *Mount, unmount bool) error {
	mount.packMu.Lock()
	defer mount.packMu.Unlock()

	if !mount.Usable() {
		if unmount && mount.State() == MountStateUnmounted {
			return nil
		}
		return errors.Wrapf(ErrNotMounted, "packed directory %q is %s", mount.Root, mount.State())
	}

	logger := m.logger.WithFields(log.Fields{"root": mount.Root, "archive": mount.ArchivePath})

	// Packing is CPU and disk bound, so only a few run at once. A release that
	// waits here still finishes; it just does not compete with every other
	// session closing at the same moment.
	select {
	case m.packSem <- struct{}{}:
	case <-m.stopCh:
		// Shutdown still has to flush this tree, so proceed without a slot
		// rather than dropping data on the floor.
		logger.Debug("Packing without a concurrency slot because the manager is stopping")
	}
	defer func() {
		select {
		case <-m.packSem:
		default:
		}
	}()

	previousState := mount.State()
	mount.setState(MountStatePacking)

	mark := mount.dirtyMark()

	// Releasing a session flushes it and then unmounts it, so without this the
	// same tree would be packed and uploaded twice in a row. A clean tree is
	// identical to its archive either way, whether it was just flushed or was
	// only ever read, so there is nothing to send.
	if unmount && !mount.IsDirty() {
		logger.Debug("Skipping the pack of an unchanged packed directory")
	} else {
		if uploadErr := m.uploadArchive(mount, logger); uploadErr != nil {
			mount.setState(previousState)
			return uploadErr
		}
		mount.markPacked(mark, time.Now())
	}

	if !unmount {
		mount.setState(previousState)
		// The tree stays on disk, but it has grown or shrunk since the last
		// accounting, so bring the quota charge back in line.
		m.reconcileSize(mount, logger)
		return nil
	}

	m.discardLocalTree(mount, logger)
	mount.setState(MountStateUnmounted)

	m.mu.Lock()
	delete(m.mounts, mount.Root)
	m.mu.Unlock()

	logger.Info("Unmounted packed directory")
	return nil
}

// removeArchiveIfPresent clears a data object so a rename can take its name.
//
// The check before the delete is not an optimization. A zone that runs policy
// on delete can answer a request to remove something that is not there with a
// rule outcome such as CUT_ACTION_PROCESSED_ERR rather than a recognizable
// file-not-found, and treating that as a failure aborted the very first upload
// of every packed directory, which by definition has no previous archive.
//
// The delete is likewise confirmed by looking rather than by reading the error,
// for the same reason: what matters is whether the name is free.
func (m *Manager) removeArchiveIfPresent(archivePath string) error {
	if !m.backend.ExistsFile(archivePath) {
		return nil
	}

	err := m.backend.RemoveFile(archivePath, true)
	if err == nil || irodsclient_types.IsFileNotFoundError(err) {
		return nil
	}

	if !m.backend.ExistsFile(archivePath) {
		return nil
	}

	return errors.Wrapf(err, "failed to remove the archive %q", archivePath)
}

// uploadArchive builds the archive and puts it in place.
//
// The archive is uploaded under a temporary name and then renamed, so a crash
// or a failed transfer never leaves a truncated archive under the real name:
// the worst case is a stray temporary object, while overwriting in place would
// destroy the only copy of the directory for as long as the upload runs.
func (m *Manager) uploadArchive(mount *Mount, logger *log.Entry) error {
	localArchivePath := mount.LocalPath + ".pack." + xid.New().String()
	if err := os.MkdirAll(filepath.Dir(localArchivePath), 0755); err != nil {
		return errors.Wrapf(err, "failed to create parent for %q", localArchivePath)
	}
	defer os.Remove(localArchivePath)

	archiveFile, err := os.Create(localArchivePath)
	if err != nil {
		return errors.Wrapf(err, "failed to create local archive %q", localArchivePath)
	}

	packedBytes, packErr := Pack(mount.LocalPath, archiveFile, m.config.Compression)
	closeErr := archiveFile.Close()
	if packErr != nil {
		return errors.Wrapf(packErr, "failed to pack %q", mount.Root)
	}
	if closeErr != nil {
		return errors.Wrapf(closeErr, "failed to finalize local archive %q", localArchivePath)
	}

	// The archive is a data object inside the packed directory's parent
	// collection, and that collection may still be a pending staging operation:
	// a session short enough to close before the background sync creates it
	// would have nowhere to put the archive. MakeDir with recurse returns
	// cleanly when the collection is already there, so this is safe to repeat
	// and does not depend on when staging catches up.
	parentCollection := path.Dir(mount.ArchivePath)
	if err := m.backend.MakeDir(parentCollection, true); err != nil {
		return errors.Wrapf(err, "failed to create the collection %q holding the archive", parentCollection)
	}

	remoteTempPath := mount.ArchivePath + uploadTempSuffix + "." + xid.New().String()
	if err := m.backend.UploadFileParallel(localArchivePath, remoteTempPath, m.transferTaskNum, nil); err != nil {
		// The partial object would otherwise linger in the collection.
		if removeErr := m.backend.RemoveFile(remoteTempPath, true); removeErr != nil {
			logger.WithError(removeErr).Debug("failed to remove a partially uploaded archive")
		}
		return errors.Wrapf(err, "failed to upload archive for %q", mount.Root)
	}

	// iRODS will not rename onto an existing data object, so the previous
	// archive goes first. The gap between the two is the only window in which
	// the directory has no archive, and it is a metadata operation wide.
	if err := m.removeArchiveIfPresent(mount.ArchivePath); err != nil {
		m.backend.RemoveFile(remoteTempPath, true)
		return errors.Wrapf(err, "failed to clear the previous archive of %q", mount.Root)
	}

	if err := m.backend.RenameFileToFile(remoteTempPath, mount.ArchivePath); err != nil {
		return errors.Wrapf(err, "failed to move the uploaded archive into place at %q", mount.ArchivePath)
	}

	// A directory migrated from a collection still has that collection in
	// iRODS. Remove it only now that the archive is safely in place, so a
	// failure anywhere above leaves the original data intact.
	mount.mu.RLock()
	legacy := mount.legacyCollection
	mount.mu.RUnlock()
	if legacy {
		if err := m.backend.RemoveDir(mount.Root, true, true); err != nil && !irodsclient_types.IsFileNotFoundError(err) {
			// The archive is authoritative from here on; a surviving collection
			// is shadowed by it, so this is not worth failing the pack for.
			logger.WithError(err).Warn("failed to remove the migrated collection; the archive is in place and takes precedence")
		}
	}

	logger.Infof("Uploaded packed directory archive (%d bytes of file data)", packedBytes)
	return nil
}

// reconcileSize brings the quota charge in line with what the tree occupies.
func (m *Manager) reconcileSize(mount *Mount, logger *log.Entry) {
	actual := treeSize(mount.LocalPath)

	mount.mu.Lock()
	previous := mount.reservedSize
	mount.mu.Unlock()

	if actual == previous {
		return
	}

	if err := m.rebalance(previous, actual); err != nil {
		// The tree grew past what staging can cover. It stays mounted and
		// charged at the old figure; the next write hits the quota instead.
		logger.WithError(err).Warn("failed to grow the staging reservation for a packed directory")
		return
	}

	mount.mu.Lock()
	mount.reservedSize = actual
	mount.mu.Unlock()
}

// discardLocalTree removes the extracted tree and gives its quota back.
func (m *Manager) discardLocalTree(mount *Mount, logger *log.Entry) {
	if err := os.RemoveAll(mount.LocalPath); err != nil {
		logger.WithError(err).Warn("failed to remove the local tree of a packed directory")
	}

	mount.mu.Lock()
	charged := mount.reservedSize
	mount.reservedSize = 0
	mount.mu.Unlock()

	m.release(charged)
}

// startSnapshotWorker packs dirty mounts periodically. Without it a long-lived
// session would keep every change local until release, so a crash would lose
// the whole session's work on these directories.
func (m *Manager) startSnapshotWorker() {
	if !m.config.SnapshotsEnabled() {
		m.logger.Info("Packed directory snapshots are disabled; directories upload at session release only")
		return
	}

	interval := m.config.SnapshotInterval
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-m.stopCh:
				return
			case <-ticker.C:
				m.snapshotDirty()
			}
		}
	}()

	m.logger.Infof("Packed directory snapshots run every %s", interval)
}

// snapshotDirty packs every mount with unsaved changes.
func (m *Manager) snapshotDirty() {
	m.mu.RLock()
	mounts := make([]*Mount, 0, len(m.mounts))
	for _, mount := range m.mounts {
		mounts = append(mounts, mount)
	}
	m.mu.RUnlock()

	var wg sync.WaitGroup
	for _, mount := range mounts {
		if !mount.IsDirty() || !mount.Usable() {
			continue
		}

		wg.Add(1)
		go func(mount *Mount) {
			defer wg.Done()
			if err := m.Pack(mount); err != nil {
				m.logger.WithError(err).Warnf("snapshot of packed directory %q failed", mount.Root)
			}
		}(mount)
	}
	wg.Wait()
}

// Mounts returns the currently mounted directories.
func (m *Manager) Mounts() []*Mount {
	if m == nil {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	mounts := make([]*Mount, 0, len(m.mounts))
	for _, mount := range m.mounts {
		mounts = append(mounts, mount)
	}
	return mounts
}

// Statuses reports every mount for the monitoring API.
func (m *Manager) Statuses() []Status {
	mounts := m.Mounts()
	statuses := make([]Status, 0, len(mounts))
	for _, mount := range mounts {
		statuses = append(statuses, mount.Status())
	}
	return statuses
}

// UnmountAll packs and uploads every mounted directory. It is the session
// release path, and it reports the combined failure rather than stopping at the
// first one, so a single bad directory cannot strand the others.
func (m *Manager) UnmountAll() error {
	if m == nil {
		return nil
	}

	var combined error
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, mount := range m.Mounts() {
		wg.Add(1)
		go func(mount *Mount) {
			defer wg.Done()
			if err := m.Unmount(mount); err != nil {
				mu.Lock()
				combined = errors.CombineErrors(combined, err)
				mu.Unlock()
			}
		}(mount)
	}
	wg.Wait()

	return combined
}

// Close stops the snapshot worker and flushes every mounted directory to iRODS.
//
// The local root is removed only when every directory reached iRODS. A tree
// whose archive failed to upload is the only copy of that data, so it is left
// on disk for an operator to recover rather than deleted along with the
// session.
func (m *Manager) Close() error {
	if m == nil {
		return nil
	}

	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
	m.wg.Wait()

	if err := m.UnmountAll(); err != nil {
		m.logger.WithError(err).Warnf(
			"keeping the packed directory trees under %q: their archives did not reach iRODS",
			m.localRootPath)
		return err
	}

	if err := os.RemoveAll(m.localRootPath); err != nil {
		return errors.Wrapf(err, "failed to remove the packed directory root %q", m.localRootPath)
	}

	return nil
}

// Remove deletes a packed directory outright: the local tree is discarded
// without being packed, and both representations are removed from iRODS.
//
// This is "rm -rf .venv". Packing first would upload a tree that is about to be
// deleted, so the tree is dropped rather than flushed.
func (m *Manager) Remove(mount *Mount) error {
	mount.packMu.Lock()
	defer mount.packMu.Unlock()

	logger := m.logger.WithFields(log.Fields{"root": mount.Root, "archive": mount.ArchivePath})

	m.discardLocalTree(mount, logger)
	mount.setState(MountStateUnmounted)

	m.mu.Lock()
	delete(m.mounts, mount.Root)
	m.mu.Unlock()

	var combined error
	if err := m.removeArchiveIfPresent(mount.ArchivePath); err != nil {
		combined = err
	}

	// A directory that was still a collection, or one a crash recovery
	// recreated, has to go too.
	if m.backend.ExistsDir(mount.Root) {
		if err := m.backend.RemoveDir(mount.Root, true, true); err != nil && !irodsclient_types.IsFileNotFoundError(err) {
			combined = errors.CombineErrors(combined, errors.Wrapf(err, "failed to remove collection %q", mount.Root))
		}
	}

	if combined == nil {
		logger.Info("Removed packed directory")
	}
	return combined
}

// Rename moves a whole packed directory to another packed path.
//
// Both sides must be configured packed names: the archive is a single data
// object, so the move is a rename of that object, and a destination that is not
// packed would have to be expanded into a collection instead. That case is
// reported as a cross-boundary rename so the caller copies.
func (m *Manager) RenameRoot(mount *Mount, destRoot string) error {
	destRoot = path.Clean(destRoot)
	if !m.config.IsPackedName(path.Base(destRoot)) {
		return errors.Wrapf(ErrCrossMountRename, "%q -> %q", mount.Root, destRoot)
	}

	// Flush first so the archive about to be renamed holds the current tree.
	if err := m.Pack(mount); err != nil {
		return err
	}

	mount.packMu.Lock()
	defer mount.packMu.Unlock()

	destArchivePath := m.config.ArchivePath(destRoot)
	logger := m.logger.WithFields(log.Fields{"root": mount.Root, "dest": destRoot})

	if err := m.removeArchiveIfPresent(destArchivePath); err != nil {
		return err
	}
	if err := m.backend.RenameFileToFile(mount.ArchivePath, destArchivePath); err != nil {
		return errors.Wrapf(err, "failed to rename archive %q to %q", mount.ArchivePath, destArchivePath)
	}

	// Drop the local tree rather than moving it. The archive is current as of
	// the flush above, so the next access to the new path mounts it there.
	m.discardLocalTree(mount, logger)
	mount.setState(MountStateUnmounted)

	m.mu.Lock()
	delete(m.mounts, mount.Root)
	m.mu.Unlock()

	logger.Info("Renamed packed directory")
	return nil
}
