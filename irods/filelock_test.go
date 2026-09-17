package irods

import (
	"context"
	"os"
	"testing"
	"time"

	cockroach_errors "github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fcntlLock builds a whole-file POSIX lock owned by a process
func fcntlLock(lockType FileLockType, handleID string, owner uint64, pid uint32) *FileLock {
	return &FileLock{
		Type: lockType,
		Owner: FileLockOwner{
			Handle: handleID,
			Owner:  owner,
		},
		Pid:   pid,
		Start: 0,
		End:   FileLockEndOfFile,
	}
}

// flockLock builds a whole-file flock() lock owned by an open file description
func flockLock(lockType FileLockType, handleID string, owner uint64, pid uint32) *FileLock {
	lock := fcntlLock(lockType, handleID, owner, pid)
	lock.Owner.Flock = true
	return lock
}

func TestFileLockManagerWriteLockConflictsWithOtherOwner(t *testing.T) {
	manager := NewFileLockManager()

	err := manager.Lock("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle1", 1, 100))
	require.NoError(t, err)

	err = manager.Lock("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle2", 2, 200))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockConflict))

	// the same owner may relock
	err = manager.Lock("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle1", 1, 100))
	require.NoError(t, err)
}

func TestFileLockManagerPosixLockOwnerIsTheProcess(t *testing.T) {
	manager := NewFileLockManager()

	// a process that opens the same file twice gets one lock owner from the
	// kernel, so its second handle must not conflict with its first
	require.NoError(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle1", 1, 100)))
	require.NoError(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle2", 1, 100)))

	assert.Nil(t, manager.Test("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle3", 1, 100)))

	// another process still conflicts
	assert.NotNil(t, manager.Test("/zone/home/file", fcntlLock(FileLockTypeRead, "handle1", 2, 200)))
}

func TestFileLockManagerFlockOwnerIsTheOpenFile(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", flockLock(FileLockTypeWrite, "handle1", 1, 100)))

	// flock() owners are per open file description, so another handle conflicts
	// even when the caller reports the same lock owner id
	err := manager.Lock("/zone/home/file", flockLock(FileLockTypeWrite, "handle2", 1, 100))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockConflict))

	// the same open file description may relock
	require.NoError(t, manager.Lock("/zone/home/file", flockLock(FileLockTypeWrite, "handle1", 1, 100)))
}

func TestFileLockManagerFlockAndPosixLocksAreIndependent(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", flockLock(FileLockTypeWrite, "handle1", 1, 100)))

	// a POSIX lock is kept in its own list, as the kernel does
	require.NoError(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle2", 2, 200)))

	assert.Nil(t, manager.Test("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle2", 2, 200)))
	assert.NotNil(t, manager.Test("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle3", 3, 300)))
	assert.NotNil(t, manager.Test("/zone/home/file", flockLock(FileLockTypeWrite, "handle3", 3, 300)))
}

func TestFileLockManagerScopeSeparatesOwners(t *testing.T) {
	manager := NewFileLockManager()

	sessionA := fcntlLock(FileLockTypeWrite, "handle1", 1, 100)
	sessionA.Owner.Scope = "session-a"
	require.NoError(t, manager.Lock("/zone/home/file", sessionA))

	// the same lock owner id coming from another session is another owner
	sessionB := fcntlLock(FileLockTypeWrite, "handle2", 1, 100)
	sessionB.Owner.Scope = "session-b"

	err := manager.Lock("/zone/home/file", sessionB)
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockConflict))

	// and the same session does not conflict with itself
	sessionAAgain := fcntlLock(FileLockTypeWrite, "handle3", 1, 100)
	sessionAAgain.Owner.Scope = "session-a"
	assert.Nil(t, manager.Test("/zone/home/file", sessionAAgain))
}

func TestFileLockManagerReadLocksAreShared(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeRead, "handle1", 1, 100)))
	require.NoError(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeRead, "handle2", 2, 200)))

	err := manager.Lock("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle3", 3, 300))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockConflict))
}

func TestFileLockManagerLocksAreScopedPerFile(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file1", fcntlLock(FileLockTypeWrite, "handle1", 1, 100)))
	require.NoError(t, manager.Lock("/zone/home/file2", fcntlLock(FileLockTypeWrite, "handle2", 2, 200)))
}

func TestFileLockManagerTestReportsConflictingLock(t *testing.T) {
	manager := NewFileLockManager()

	held := fcntlLock(FileLockTypeWrite, "handle1", 1, 100)
	held.Start, held.End = 10, 20
	require.NoError(t, manager.Lock("/zone/home/file", held))

	request := fcntlLock(FileLockTypeRead, "handle2", 2, 200)
	request.Start, request.End = 15, 30

	conflict := manager.Test("/zone/home/file", request)
	require.NotNil(t, conflict)
	assert.Equal(t, FileLockTypeWrite, conflict.Type)
	assert.Equal(t, uint32(100), conflict.Pid)
	assert.Equal(t, uint64(10), conflict.Start)
	assert.Equal(t, uint64(20), conflict.End)

	// a range that does not overlap is free
	free := fcntlLock(FileLockTypeWrite, "handle2", 2, 200)
	free.Start, free.End = 21, 30
	assert.Nil(t, manager.Test("/zone/home/file", free))

	// the holder itself sees no conflict
	own := fcntlLock(FileLockTypeWrite, "handle1", 1, 100)
	own.Start, own.End = 15, 30
	assert.Nil(t, manager.Test("/zone/home/file", own))
}

func TestFileLockManagerByteRangeLocks(t *testing.T) {
	manager := NewFileLockManager()

	first := fcntlLock(FileLockTypeWrite, "handle1", 1, 100)
	first.Start, first.End = 0, 99
	require.NoError(t, manager.Lock("/zone/home/file", first))

	// a non-overlapping range is grantable to another owner
	second := fcntlLock(FileLockTypeWrite, "handle2", 2, 200)
	second.Start, second.End = 100, 199
	require.NoError(t, manager.Lock("/zone/home/file", second))

	// an overlapping range is not
	third := fcntlLock(FileLockTypeWrite, "handle2", 2, 200)
	third.Start, third.End = 99, 150
	err := manager.Lock("/zone/home/file", third)
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockConflict))
}

func TestFileLockManagerPartialUnlockSplitsRange(t *testing.T) {
	manager := NewFileLockManager()

	held := fcntlLock(FileLockTypeWrite, "handle1", 1, 100)
	held.Start, held.End = 0, 99
	require.NoError(t, manager.Lock("/zone/home/file", held))

	// punch a hole in the middle
	hole := fcntlLock(FileLockTypeUnlock, "handle1", 1, 100)
	hole.Start, hole.End = 40, 59
	require.NoError(t, manager.Lock("/zone/home/file", hole))

	// the hole is grantable, the rest is not
	inHole := fcntlLock(FileLockTypeWrite, "handle2", 2, 200)
	inHole.Start, inHole.End = 40, 59
	require.NoError(t, manager.Lock("/zone/home/file", inHole))

	beforeHole := fcntlLock(FileLockTypeWrite, "handle2", 2, 200)
	beforeHole.Start, beforeHole.End = 39, 39
	assert.NotNil(t, manager.Test("/zone/home/file", beforeHole))

	afterHole := fcntlLock(FileLockTypeWrite, "handle2", 2, 200)
	afterHole.Start, afterHole.End = 60, 60
	assert.NotNil(t, manager.Test("/zone/home/file", afterHole))
}

func TestFileLockManagerUnlockOfUnlockedRangeSucceeds(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeUnlock, "handle1", 1, 100)))
}

func TestFileLockManagerReleaseFreesHandleLocks(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle1", 1, 100)))
	require.Error(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle2", 2, 200)))

	manager.Release("handle1")

	require.NoError(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle2", 2, 200)))
}

func TestFileLockManagerReleaseKeepsLocksOfOtherHandlesOfTheSameOwner(t *testing.T) {
	manager := NewFileLockManager()

	// one process, two open files, one lock each
	first := fcntlLock(FileLockTypeWrite, "handle1", 1, 100)
	first.Start, first.End = 0, 9
	require.NoError(t, manager.Lock("/zone/home/file", first))

	second := fcntlLock(FileLockTypeWrite, "handle2", 1, 100)
	second.Start, second.End = 10, 19
	require.NoError(t, manager.Lock("/zone/home/file", second))

	manager.Release("handle1")

	// POSIX would drop both here, this manager keeps what the open handle took
	other := fcntlLock(FileLockTypeWrite, "handle3", 2, 200)
	other.Start, other.End = 0, 9
	assert.Nil(t, manager.Test("/zone/home/file", other))

	other.Start, other.End = 10, 19
	assert.NotNil(t, manager.Test("/zone/home/file", other))
}

func TestFileLockManagerLockWaitBlocksUntilReleased(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle1", 1, 100)))

	acquired := make(chan error, 1)
	go func() {
		acquired <- manager.LockWait(context.Background(), "/zone/home/file", fcntlLock(FileLockTypeWrite, "handle2", 2, 200))
	}()

	select {
	case <-acquired:
		t.Fatal("LockWait returned while a conflicting lock was held")
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeUnlock, "handle1", 1, 100)))

	select {
	case err := <-acquired:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("LockWait did not return after the conflicting lock was released")
	}
}

func TestFileLockManagerLockWaitWakesOnHandleRelease(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle1", 1, 100)))

	acquired := make(chan error, 1)
	go func() {
		acquired <- manager.LockWait(context.Background(), "/zone/home/file", fcntlLock(FileLockTypeWrite, "handle2", 2, 200))
	}()

	time.Sleep(50 * time.Millisecond)
	manager.Release("handle1")

	select {
	case err := <-acquired:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("LockWait did not return after the holder was released")
	}
}

func TestFileLockManagerLockWaitWakesOnWriteLockDowngrade(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle1", 1, 100)))

	acquired := make(chan error, 1)
	go func() {
		acquired <- manager.LockWait(context.Background(), "/zone/home/file", fcntlLock(FileLockTypeRead, "handle2", 2, 200))
	}()

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeRead, "handle1", 1, 100)))

	select {
	case err := <-acquired:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("LockWait did not return after the write lock was downgraded")
	}
}

func TestFileLockManagerLockWaitHonorsContextCancel(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle1", 1, 100)))

	ctx, cancel := context.WithCancel(context.Background())
	acquired := make(chan error, 1)
	go func() {
		acquired <- manager.LockWait(ctx, "/zone/home/file", fcntlLock(FileLockTypeWrite, "handle2", 2, 200))
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-acquired:
		require.Error(t, err)
		assert.True(t, cockroach_errors.Is(err, context.Canceled))
	case <-time.After(5 * time.Second):
		t.Fatal("LockWait did not return after the context was canceled")
	}

	// the canceled waiter left no lock behind
	assert.Nil(t, manager.Test("/zone/home/file", fcntlLock(FileLockTypeWrite, "handle1", 1, 100)))
}

func TestFileLockManagerLockWaitSerializesWaiters(t *testing.T) {
	manager := NewFileLockManager()

	const waiterNum = 8
	counter := 0
	done := make(chan error, waiterNum)

	for i := range waiterNum {
		owner := uint64(i + 1)
		go func() {
			lock := flockLock(FileLockTypeWrite, "handle", owner, uint32(owner))
			err := manager.LockWait(context.Background(), "/zone/home/file", lock)
			if err != nil {
				done <- err
				return
			}

			counter++

			done <- manager.Lock("/zone/home/file", flockLock(FileLockTypeUnlock, "handle", owner, uint32(owner)))
		}()
	}

	for range waiterNum {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("waiters did not finish")
		}
	}

	assert.Equal(t, waiterNum, counter)
}

func TestFileLockManagerReleaseFreesLocksAfterMove(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/old", fcntlLock(FileLockTypeWrite, "handle1", 1, 100)))
	manager.Move("/zone/home/old", "/zone/home/new")

	// the lock follows the file
	assert.Nil(t, manager.Test("/zone/home/old", fcntlLock(FileLockTypeWrite, "handle2", 2, 200)))
	assert.NotNil(t, manager.Test("/zone/home/new", fcntlLock(FileLockTypeWrite, "handle2", 2, 200)))

	// and releasing the handle still finds it under the new path
	manager.Release("handle1")
	assert.Nil(t, manager.Test("/zone/home/new", fcntlLock(FileLockTypeWrite, "handle2", 2, 200)))
}

func TestFileLockManagerMoveKeepsLocksOfDestination(t *testing.T) {
	manager := NewFileLockManager()

	source := fcntlLock(FileLockTypeWrite, "handle1", 1, 100)
	source.Start, source.End = 0, 9
	require.NoError(t, manager.Lock("/zone/home/old", source))

	destination := fcntlLock(FileLockTypeWrite, "handle2", 2, 200)
	destination.Start, destination.End = 10, 19
	require.NoError(t, manager.Lock("/zone/home/new", destination))

	manager.Move("/zone/home/old", "/zone/home/new")

	probe := fcntlLock(FileLockTypeRead, "handle3", 3, 300)
	probe.Start, probe.End = 0, 9
	assert.NotNil(t, manager.Test("/zone/home/new", probe))

	probe.Start, probe.End = 10, 19
	assert.NotNil(t, manager.Test("/zone/home/new", probe))
}

func TestFileLockManagerMoveIsNoOpWhenNothingHeld(t *testing.T) {
	manager := NewFileLockManager()

	manager.Move("/zone/home/old", "/zone/home/new")
	manager.Move("/zone/home/same", "/zone/home/same")

	assert.Nil(t, manager.Test("/zone/home/new", fcntlLock(FileLockTypeWrite, "handle1", 1, 100)))
}

func TestFileLockHelpersFillInTheHandle(t *testing.T) {
	manager := NewFileLockManager()

	// a caller does not know the handle id, the handle fills it in
	lock := flockLock(FileLockTypeWrite, "", 1, 100)
	require.NoError(t, setFileLock(manager, "/zone/home/file", "handle1", lock))
	assert.Empty(t, lock.Owner.Handle, "the caller's lock must not be modified")

	// the lock is owned by handle1, so handle2 conflicts with it
	err := setFileLock(manager, "/zone/home/file", "handle2", flockLock(FileLockTypeWrite, "", 1, 100))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockConflict))

	conflict, err := testFileLock(manager, "/zone/home/file", "handle2", flockLock(FileLockTypeRead, "", 1, 100))
	require.NoError(t, err)
	require.NotNil(t, conflict)
	assert.Equal(t, "handle1", conflict.Owner.Handle)

	releaseFileLocks(manager, "handle1")
	require.NoError(t, setFileLock(manager, "/zone/home/file", "handle2", flockLock(FileLockTypeWrite, "", 1, 100)))
}

func TestFileLockHelpersReportMissingManager(t *testing.T) {
	_, err := testFileLock(nil, "/zone/home/file", "handle1", fcntlLock(FileLockTypeWrite, "", 1, 100))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockManagerUnavailable))

	err = setFileLock(nil, "/zone/home/file", "handle1", fcntlLock(FileLockTypeWrite, "", 1, 100))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockManagerUnavailable))

	err = setFileLockWait(context.Background(), nil, "/zone/home/file", "handle1", fcntlLock(FileLockTypeWrite, "", 1, 100))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockManagerUnavailable))

	// these must not panic
	releaseFileLocks(nil, "handle1")
	moveFileLocks(nil, "/zone/home/old", "/zone/home/new")
}

func TestBufferedHandlesShareOneFileLockTable(t *testing.T) {
	const path = "/zone/home/shared.txt"

	manager := NewFileLockManager()
	client := &IRODSFSClientBuffered{
		client: &IRODSFSClientDirect{fileLockManager: manager},
	}

	stagedFile, err := os.CreateTemp(t.TempDir(), "staged-*")
	require.NoError(t, err)
	staged := newStagedHandle(client, stagedFile, path, irodsclient_types.FileOpenModeWriteOnly, &irodsclient_fs.Entry{
		Type: irodsclient_fs.FileEntry,
		Name: "shared.txt",
		Path: path,
	})

	buffered := &IRODSFSClientBufferedFileHandle{
		id:        "buffered-handle",
		client:    client,
		handle:    newMockFileHandle(path, nil, irodsclient_types.FileOpenModeReadOnly),
		irodsPath: path,
	}

	packedFile, err := os.CreateTemp(t.TempDir(), "packed-*")
	require.NoError(t, err)
	packed := newPackedFileHandle(nil, nil, packedFile, path, irodsclient_types.FileOpenModeReadOnly, nil, manager, nil)

	// a lock taken on the staged handle is seen by the other handle types
	require.NoError(t, staged.Setlk(flockLock(FileLockTypeWrite, "", 1, 100)))

	err = buffered.Setlk(flockLock(FileLockTypeWrite, "", 2, 200))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockConflict))

	conflict, err := packed.Getlk(flockLock(FileLockTypeRead, "", 3, 300))
	require.NoError(t, err)
	require.NotNil(t, conflict)
	assert.Equal(t, uint32(100), conflict.Pid)

	// renaming the staged file carries the lock over to the new path
	const newPath = "/zone/home/renamed.txt"
	staged.UpdateStagingPath(newPath)

	probe := flockLock(FileLockTypeWrite, "other-handle", 4, 400)
	assert.Nil(t, manager.Test(path, probe))
	assert.NotNil(t, manager.Test(newPath, probe))

	// closing a handle releases the locks it holds
	packedLock := flockLock(FileLockTypeWrite, "", 5, 500)
	packedLock.Start, packedLock.End = 0, 9
	require.NoError(t, packed.Setlk(packedLock))

	probe.Start, probe.End = 0, 9
	assert.NotNil(t, manager.Test(path, probe))

	require.NoError(t, packed.Close())
	assert.Nil(t, manager.Test(path, probe))
}

func TestHandlesWithoutLockManagerReportUnavailable(t *testing.T) {
	stagedFile, err := os.CreateTemp(t.TempDir(), "staged-*")
	require.NoError(t, err)

	// a staged handle with no client has no lock manager to serve locks
	staged := newStagedHandle(nil, stagedFile, "/zone/home/orphan.txt", irodsclient_types.FileOpenModeWriteOnly, nil)

	err = staged.Setlk(fcntlLock(FileLockTypeWrite, "", 1, 100))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockManagerUnavailable))
}
