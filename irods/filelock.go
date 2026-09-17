package irods

import (
	"context"
	"math"
	"sync"

	"github.com/cockroachdb/errors"
)

// FileLockEndOfFile marks a lock range that extends to the end of the file.
// A whole-file lock (flock) is expressed as [0, FileLockEndOfFile].
const FileLockEndOfFile uint64 = math.MaxUint64

var (
	// ErrFileLockConflict is returned when a lock request conflicts with a lock held by another owner
	ErrFileLockConflict = errors.New("file lock conflict")

	// ErrFileLockManagerUnavailable is returned when a file handle has no lock manager to serve locks
	ErrFileLockManagerUnavailable = errors.New("file lock manager is unavailable")
)

// FileLockType is a type of a file lock
type FileLockType uint32

const (
	// FileLockTypeRead is a shared lock
	FileLockTypeRead FileLockType = iota
	// FileLockTypeWrite is an exclusive lock
	FileLockTypeWrite
	// FileLockTypeUnlock releases locks in the given range
	FileLockTypeUnlock
)

func (t FileLockType) String() string {
	switch t {
	case FileLockTypeRead:
		return "read"
	case FileLockTypeWrite:
		return "write"
	case FileLockTypeUnlock:
		return "unlock"
	default:
		return "unknown"
	}
}

// FileLockOwner identifies who holds a lock.
//
// The kernel hands FUSE a lock owner that already carries the right identity for
// each locking mechanism, so the two are keyed differently:
//
//   - flock() and OFD locks are owned by an open file description. Owner is
//     unique per open, and Handle is compared as well so that two opens never
//     look like one owner.
//   - fcntl() POSIX locks are owned by the process. The kernel sends the same
//     Owner for every file descriptor the process has on the file, so Handle is
//     not compared - otherwise a process that opens the same file twice would
//     conflict with itself.
//
// Scope separates owners that belong to different clients of one lock manager.
// It is empty for a lock manager serving a single process, and holds the
// session id when a pool server serves many mounts from one table.
type FileLockOwner struct {
	Scope  string // client/session the owner belongs to
	Handle string // file handle that requested the lock
	Owner  uint64 // lock owner id given by the caller (FUSE lock_owner)
	Flock  bool   // the request came from flock(), not from fcntl()
}

// sameAs returns true if both locks are owned by the same owner.
// Locks taken through different mechanisms never share an owner.
func (o *FileLockOwner) sameAs(other *FileLockOwner) bool {
	if o.Flock != other.Flock {
		return false
	}

	if o.Scope != other.Scope || o.Owner != other.Owner {
		return false
	}

	if o.Flock {
		// an open file description, not a process
		return o.Handle == other.Handle
	}

	return true
}

// FileLock is a byte-range lock request or a lock held on a file.
// Start and End are both inclusive.
type FileLock struct {
	Type  FileLockType
	Owner FileLockOwner
	Pid   uint32 // pid of the process that requested the lock, reported back by Test
	Start uint64
	End   uint64
}

func overlaps(start1 uint64, end1 uint64, start2 uint64, end2 uint64) bool {
	return start1 <= end2 && start2 <= end1
}

// FileLockManager manages byte-range file locks in memory.
//
// Locks are tracked per file path, not per file handle, so that two handles on
// the same file see each other's locks. See FileLockOwner for how a holder is
// identified.
//
// flock() locks and fcntl() locks are kept in one table but never conflict with
// each other, which is how the kernel treats them: a file can be locked through
// both mechanisms at once.
//
// Known deviations from POSIX:
//
//   - A process that closes one of several file descriptors on a file keeps the
//     locks it took through the others. POSIX drops them all. The locks are
//     released when the handle that took them is closed, as flock() and OFD
//     locks are. (The FUSE library this serves does not report the lock owner of
//     a FLUSH, so owner-wide release on close cannot be implemented here.)
//   - Deadlocks between waiters are not detected, so no waiter fails with
//     EDEADLK; a blocking request waits until the conflicting lock is released
//     or the given context is canceled.
//   - The locks live in this process only. A client that reaches iRODS without
//     going through this process does not see them.
type FileLockManager struct {
	mutex   sync.Mutex
	locks   map[string][]*FileLock // key is a file path
	waiters map[chan struct{}]struct{}
}

// NewFileLockManager creates a new FileLockManager
func NewFileLockManager() *FileLockManager {
	return &FileLockManager{
		locks:   map[string][]*FileLock{},
		waiters: map[chan struct{}]struct{}{},
	}
}

// Test returns a lock that conflicts with the given request, or nil if the
// request can be granted
func (m *FileLockManager) Test(path string, lock *FileLock) *FileLock {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	conflict := m.findConflict(path, lock)
	if conflict == nil {
		return nil
	}

	conflictCopy := *conflict
	return &conflictCopy
}

// Lock acquires, downgrades, upgrades or releases a lock without waiting.
// It returns ErrFileLockConflict if another owner holds a conflicting lock.
func (m *FileLockManager) Lock(path string, lock *FileLock) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if conflict := m.findConflict(path, lock); conflict != nil {
		return errors.Wrapf(ErrFileLockConflict, "%s lock on %q [%d, %d] conflicts with a %s lock held by pid %d", lock.Type.String(), path, lock.Start, lock.End, conflict.Type.String(), conflict.Pid)
	}

	m.apply(path, lock)
	return nil
}

// LockWait acquires a lock, waiting until it becomes available or the context
// is canceled. Releasing a lock never waits.
func (m *FileLockManager) LockWait(ctx context.Context, path string, lock *FileLock) error {
	for {
		m.mutex.Lock()

		conflict := m.findConflict(path, lock)
		if conflict == nil {
			m.apply(path, lock)
			m.mutex.Unlock()
			return nil
		}

		// register before releasing the mutex so that a release happening right
		// after the conflict check still wakes this waiter up
		waiter := make(chan struct{})
		m.waiters[waiter] = struct{}{}
		m.mutex.Unlock()

		select {
		case <-ctx.Done():
			m.removeWaiter(waiter)
			return errors.Wrapf(ctx.Err(), "failed to acquire a %s lock on %q [%d, %d]", lock.Type.String(), path, lock.Start, lock.End)
		case <-waiter:
			// a lock was released, check again
		}
	}
}

// Release releases every lock taken by the given file handle.
// It scans all files rather than taking a path, so that a handle whose file was
// renamed while it was open does not leak its locks.
func (m *FileLockManager) Release(handleID string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	released := false

	for path, held := range m.locks {
		remaining := make([]*FileLock, 0, len(held))
		for _, l := range held {
			if l.Owner.Handle != handleID {
				remaining = append(remaining, l)
			}
		}

		if len(remaining) == len(held) {
			continue
		}

		m.store(path, remaining)
		released = true
	}

	if released {
		m.notifyWaiters()
	}
}

// Move makes the locks held on oldPath follow the file to newPath, so that a
// rename of an open file does not hide the locks it holds.
//
// Locks already held on newPath are kept. A rename that replaces an existing
// file leaves that file's holders with locks that are no longer reachable by
// name, which mirrors what the kernel does with the replaced inode.
func (m *FileLockManager) Move(oldPath string, newPath string) {
	if oldPath == newPath {
		return
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	moved, ok := m.locks[oldPath]
	if !ok {
		return
	}

	delete(m.locks, oldPath)
	m.store(newPath, append(m.locks[newPath], moved...))
}

// findConflict returns a lock held by another owner that conflicts with the
// given request. The caller must hold the mutex.
func (m *FileLockManager) findConflict(path string, lock *FileLock) *FileLock {
	if lock.Type == FileLockTypeUnlock {
		// releasing never conflicts
		return nil
	}

	for _, held := range m.locks[path] {
		if held.Owner.Flock != lock.Owner.Flock {
			// flock() locks and fcntl() locks are independent
			continue
		}

		if held.Owner.sameAs(&lock.Owner) {
			// an owner never conflicts with itself
			continue
		}

		if !overlaps(held.Start, held.End, lock.Start, lock.End) {
			continue
		}

		if held.Type == FileLockTypeWrite || lock.Type == FileLockTypeWrite {
			return held
		}
		// two read locks are compatible
	}

	return nil
}

// apply replaces the owner's locks in the requested range with the requested
// lock. The caller must hold the mutex and must have checked for conflicts.
func (m *FileLockManager) apply(path string, lock *FileLock) {
	held := m.locks[path]
	updated := make([]*FileLock, 0, len(held)+2)
	replaced := false

	for _, l := range held {
		if !l.Owner.sameAs(&lock.Owner) || !overlaps(l.Start, l.End, lock.Start, lock.End) {
			updated = append(updated, l)
			continue
		}

		// this lock is replaced by the request, in whole or in part
		replaced = true

		// the request covers a part of this lock - keep the parts it does not cover
		if l.Start < lock.Start {
			left := *l
			left.End = lock.Start - 1
			updated = append(updated, &left)
		}

		if l.End > lock.End {
			right := *l
			right.Start = lock.End + 1 // lock.End < l.End, so this does not overflow
			updated = append(updated, &right)
		}
	}

	// a range the owner held is released when it is unlocked, and it is opened
	// up for readers when a write lock is downgraded to a read lock
	released := replaced && lock.Type != FileLockTypeWrite

	if lock.Type != FileLockTypeUnlock {
		newLock := *lock
		updated = append(updated, &newLock)
	}

	m.store(path, updated)

	if released {
		// a range this owner held is gone or shareable now, waiters may proceed
		m.notifyWaiters()
	}
}

// store saves the lock list of a file, dropping the entry when it is empty.
// The caller must hold the mutex.
func (m *FileLockManager) store(path string, locks []*FileLock) {
	if len(locks) == 0 {
		delete(m.locks, path)
		return
	}

	m.locks[path] = locks
}

// notifyWaiters wakes up every waiter. The caller must hold the mutex.
func (m *FileLockManager) notifyWaiters() {
	for waiter := range m.waiters {
		close(waiter)
		delete(m.waiters, waiter)
	}
}

func (m *FileLockManager) removeWaiter(waiter chan struct{}) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.waiters, waiter)
}

// forHandle returns a copy of the lock owned by the given file handle. Callers
// of a file handle do not know the handle id it uses to identify its owner, so
// the handle fills it in.
func forHandle(handleID string, lock *FileLock) *FileLock {
	lockCopy := *lock
	lockCopy.Owner.Handle = handleID
	return &lockCopy
}

// testFileLock is a helper for IRODSFSFileHandle.Getlk implementations
func testFileLock(manager *FileLockManager, path string, handleID string, lock *FileLock) (*FileLock, error) {
	if manager == nil {
		return nil, ErrFileLockManagerUnavailable
	}

	return manager.Test(path, forHandle(handleID, lock)), nil
}

// setFileLock is a helper for IRODSFSFileHandle.Setlk implementations
func setFileLock(manager *FileLockManager, path string, handleID string, lock *FileLock) error {
	if manager == nil {
		return ErrFileLockManagerUnavailable
	}

	return manager.Lock(path, forHandle(handleID, lock))
}

// setFileLockWait is a helper for IRODSFSFileHandle.Setlkw implementations
func setFileLockWait(ctx context.Context, manager *FileLockManager, path string, handleID string, lock *FileLock) error {
	if manager == nil {
		return ErrFileLockManagerUnavailable
	}

	return manager.LockWait(ctx, path, forHandle(handleID, lock))
}

// releaseFileLocks is a helper for IRODSFSFileHandle.Close implementations
func releaseFileLocks(manager *FileLockManager, handleID string) {
	if manager == nil {
		return
	}

	manager.Release(handleID)
}

// moveFileLocks is a helper for renaming a file that may hold locks
func moveFileLocks(manager *FileLockManager, oldPath string, newPath string) {
	if manager == nil {
		return
	}

	manager.Move(oldPath, newPath)
}
