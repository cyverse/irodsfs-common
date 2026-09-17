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

func wholeFileLock(lockType FileLockType, owner uint64, pid uint32) *FileLock {
	return &FileLock{
		Type:  lockType,
		Owner: owner,
		Pid:   pid,
		Start: 0,
		End:   FileLockEndOfFile,
	}
}

func TestFileLockManagerWriteLockConflictsWithOtherHandle(t *testing.T) {
	manager := NewFileLockManager()

	err := manager.Lock("/zone/home/file", "handle1", wholeFileLock(FileLockTypeWrite, 1, 100))
	require.NoError(t, err)

	err = manager.Lock("/zone/home/file", "handle2", wholeFileLock(FileLockTypeWrite, 2, 200))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockConflict))

	// the same handle and owner may relock
	err = manager.Lock("/zone/home/file", "handle1", wholeFileLock(FileLockTypeWrite, 1, 100))
	require.NoError(t, err)

	// a different lock owner on the same handle is a different owner
	err = manager.Lock("/zone/home/file", "handle1", wholeFileLock(FileLockTypeWrite, 2, 100))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockConflict))
}

func TestFileLockManagerReadLocksAreShared(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", "handle1", wholeFileLock(FileLockTypeRead, 1, 100)))
	require.NoError(t, manager.Lock("/zone/home/file", "handle2", wholeFileLock(FileLockTypeRead, 2, 200)))

	err := manager.Lock("/zone/home/file", "handle3", wholeFileLock(FileLockTypeWrite, 3, 300))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockConflict))
}

func TestFileLockManagerLocksAreScopedPerFile(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file1", "handle1", wholeFileLock(FileLockTypeWrite, 1, 100)))
	require.NoError(t, manager.Lock("/zone/home/file2", "handle2", wholeFileLock(FileLockTypeWrite, 2, 200)))
}

func TestFileLockManagerTestReportsConflictingLock(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", "handle1", &FileLock{
		Type: FileLockTypeWrite, Owner: 1, Pid: 100, Start: 10, End: 20,
	}))

	conflict := manager.Test("/zone/home/file", "handle2", &FileLock{
		Type: FileLockTypeRead, Owner: 2, Pid: 200, Start: 15, End: 30,
	})
	require.NotNil(t, conflict)
	assert.Equal(t, FileLockTypeWrite, conflict.Type)
	assert.Equal(t, uint32(100), conflict.Pid)
	assert.Equal(t, uint64(10), conflict.Start)
	assert.Equal(t, uint64(20), conflict.End)

	// a range that does not overlap is free
	assert.Nil(t, manager.Test("/zone/home/file", "handle2", &FileLock{
		Type: FileLockTypeWrite, Owner: 2, Pid: 200, Start: 21, End: 30,
	}))

	// the holder itself sees no conflict
	assert.Nil(t, manager.Test("/zone/home/file", "handle1", &FileLock{
		Type: FileLockTypeWrite, Owner: 1, Pid: 100, Start: 15, End: 30,
	}))
}

func TestFileLockManagerByteRangeLocks(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", "handle1", &FileLock{
		Type: FileLockTypeWrite, Owner: 1, Pid: 100, Start: 0, End: 99,
	}))

	// a non-overlapping range is grantable to another owner
	require.NoError(t, manager.Lock("/zone/home/file", "handle2", &FileLock{
		Type: FileLockTypeWrite, Owner: 2, Pid: 200, Start: 100, End: 199,
	}))

	// an overlapping range is not
	err := manager.Lock("/zone/home/file", "handle2", &FileLock{
		Type: FileLockTypeWrite, Owner: 2, Pid: 200, Start: 99, End: 150,
	})
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockConflict))
}

func TestFileLockManagerPartialUnlockSplitsRange(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", "handle1", &FileLock{
		Type: FileLockTypeWrite, Owner: 1, Pid: 100, Start: 0, End: 99,
	}))

	// punch a hole in the middle
	require.NoError(t, manager.Lock("/zone/home/file", "handle1", &FileLock{
		Type: FileLockTypeUnlock, Owner: 1, Pid: 100, Start: 40, End: 59,
	}))

	// the hole is grantable, the rest is not
	require.NoError(t, manager.Lock("/zone/home/file", "handle2", &FileLock{
		Type: FileLockTypeWrite, Owner: 2, Pid: 200, Start: 40, End: 59,
	}))
	assert.NotNil(t, manager.Test("/zone/home/file", "handle2", &FileLock{
		Type: FileLockTypeWrite, Owner: 2, Pid: 200, Start: 39, End: 39,
	}))
	assert.NotNil(t, manager.Test("/zone/home/file", "handle2", &FileLock{
		Type: FileLockTypeWrite, Owner: 2, Pid: 200, Start: 60, End: 60,
	}))
}

func TestFileLockManagerUnlockOfUnlockedRangeSucceeds(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", "handle1", wholeFileLock(FileLockTypeUnlock, 1, 100)))
}

func TestFileLockManagerReleaseFreesHandleLocks(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", "handle1", wholeFileLock(FileLockTypeWrite, 1, 100)))
	require.Error(t, manager.Lock("/zone/home/file", "handle2", wholeFileLock(FileLockTypeWrite, 2, 200)))

	manager.Release("handle1")

	require.NoError(t, manager.Lock("/zone/home/file", "handle2", wholeFileLock(FileLockTypeWrite, 2, 200)))
}

func TestFileLockManagerLockWaitBlocksUntilReleased(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", "handle1", wholeFileLock(FileLockTypeWrite, 1, 100)))

	acquired := make(chan error, 1)
	go func() {
		acquired <- manager.LockWait(context.Background(), "/zone/home/file", "handle2", wholeFileLock(FileLockTypeWrite, 2, 200))
	}()

	select {
	case <-acquired:
		t.Fatal("LockWait returned while a conflicting lock was held")
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, manager.Lock("/zone/home/file", "handle1", wholeFileLock(FileLockTypeUnlock, 1, 100)))

	select {
	case err := <-acquired:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("LockWait did not return after the conflicting lock was released")
	}
}

func TestFileLockManagerLockWaitWakesOnHandleRelease(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", "handle1", wholeFileLock(FileLockTypeWrite, 1, 100)))

	acquired := make(chan error, 1)
	go func() {
		acquired <- manager.LockWait(context.Background(), "/zone/home/file", "handle2", wholeFileLock(FileLockTypeWrite, 2, 200))
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

	require.NoError(t, manager.Lock("/zone/home/file", "handle1", wholeFileLock(FileLockTypeWrite, 1, 100)))

	acquired := make(chan error, 1)
	go func() {
		acquired <- manager.LockWait(context.Background(), "/zone/home/file", "handle2", wholeFileLock(FileLockTypeRead, 2, 200))
	}()

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, manager.Lock("/zone/home/file", "handle1", wholeFileLock(FileLockTypeRead, 1, 100)))

	select {
	case err := <-acquired:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("LockWait did not return after the write lock was downgraded")
	}
}

func TestFileLockManagerLockWaitHonorsContextCancel(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/file", "handle1", wholeFileLock(FileLockTypeWrite, 1, 100)))

	ctx, cancel := context.WithCancel(context.Background())
	acquired := make(chan error, 1)
	go func() {
		acquired <- manager.LockWait(ctx, "/zone/home/file", "handle2", wholeFileLock(FileLockTypeWrite, 2, 200))
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
	assert.Nil(t, manager.Test("/zone/home/file", "handle1", wholeFileLock(FileLockTypeWrite, 1, 100)))
}

func TestFileLockManagerLockWaitSerializesWaiters(t *testing.T) {
	manager := NewFileLockManager()

	const waiterNum = 8
	counter := 0
	done := make(chan error, waiterNum)

	for i := range waiterNum {
		owner := uint64(i + 1)
		go func() {
			lock := wholeFileLock(FileLockTypeWrite, owner, uint32(owner))
			err := manager.LockWait(context.Background(), "/zone/home/file", "handle", lock)
			if err != nil {
				done <- err
				return
			}

			counter++

			unlock := wholeFileLock(FileLockTypeUnlock, owner, uint32(owner))
			done <- manager.Lock("/zone/home/file", "handle", unlock)
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

	require.NoError(t, manager.Lock("/zone/home/old", "handle1", wholeFileLock(FileLockTypeWrite, 1, 100)))
	manager.Move("/zone/home/old", "/zone/home/new")

	// the lock follows the file
	assert.Nil(t, manager.Test("/zone/home/old", "handle2", wholeFileLock(FileLockTypeWrite, 2, 200)))
	assert.NotNil(t, manager.Test("/zone/home/new", "handle2", wholeFileLock(FileLockTypeWrite, 2, 200)))

	// and releasing the handle still finds it under the new path
	manager.Release("handle1")
	assert.Nil(t, manager.Test("/zone/home/new", "handle2", wholeFileLock(FileLockTypeWrite, 2, 200)))
}

func TestFileLockManagerMoveKeepsLocksOfDestination(t *testing.T) {
	manager := NewFileLockManager()

	require.NoError(t, manager.Lock("/zone/home/old", "handle1", &FileLock{
		Type: FileLockTypeWrite, Owner: 1, Pid: 100, Start: 0, End: 9,
	}))
	require.NoError(t, manager.Lock("/zone/home/new", "handle2", &FileLock{
		Type: FileLockTypeWrite, Owner: 2, Pid: 200, Start: 10, End: 19,
	}))

	manager.Move("/zone/home/old", "/zone/home/new")

	assert.NotNil(t, manager.Test("/zone/home/new", "handle3", &FileLock{
		Type: FileLockTypeRead, Owner: 3, Pid: 300, Start: 0, End: 9,
	}))
	assert.NotNil(t, manager.Test("/zone/home/new", "handle3", &FileLock{
		Type: FileLockTypeRead, Owner: 3, Pid: 300, Start: 10, End: 19,
	}))
}

func TestFileLockManagerMoveIsNoOpWhenNothingHeld(t *testing.T) {
	manager := NewFileLockManager()

	manager.Move("/zone/home/old", "/zone/home/new")
	manager.Move("/zone/home/same", "/zone/home/same")

	assert.Nil(t, manager.Test("/zone/home/new", "handle1", wholeFileLock(FileLockTypeWrite, 1, 100)))
}

func TestFileLockHelpersReportMissingManager(t *testing.T) {
	_, err := testFileLock(nil, "/zone/home/file", "handle1", wholeFileLock(FileLockTypeWrite, 1, 100))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockManagerUnavailable))

	err = setFileLock(nil, "/zone/home/file", "handle1", wholeFileLock(FileLockTypeWrite, 1, 100))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockManagerUnavailable))

	err = setFileLockWait(context.Background(), nil, "/zone/home/file", "handle1", wholeFileLock(FileLockTypeWrite, 1, 100))
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
	require.NoError(t, staged.Setlk(wholeFileLock(FileLockTypeWrite, 1, 100)))

	err = buffered.Setlk(wholeFileLock(FileLockTypeWrite, 2, 200))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockConflict))

	conflict, err := packed.Getlk(wholeFileLock(FileLockTypeRead, 3, 300))
	require.NoError(t, err)
	require.NotNil(t, conflict)
	assert.Equal(t, uint32(100), conflict.Pid)

	// renaming the staged file carries the lock over to the new path
	const newPath = "/zone/home/renamed.txt"
	staged.UpdateStagingPath(newPath)

	assert.Nil(t, manager.Test(path, "other-handle", wholeFileLock(FileLockTypeWrite, 4, 400)))
	assert.NotNil(t, manager.Test(newPath, "other-handle", wholeFileLock(FileLockTypeWrite, 4, 400)))

	// closing a handle releases the locks it holds
	require.NoError(t, packed.Setlk(&FileLock{Type: FileLockTypeWrite, Owner: 5, Pid: 500, Start: 0, End: 9}))
	assert.NotNil(t, manager.Test(path, "other-handle", &FileLock{
		Type: FileLockTypeWrite, Owner: 4, Pid: 400, Start: 0, End: 9,
	}))

	require.NoError(t, packed.Close())
	assert.Nil(t, manager.Test(path, "other-handle", &FileLock{
		Type: FileLockTypeWrite, Owner: 4, Pid: 400, Start: 0, End: 9,
	}))
}

func TestHandlesWithoutLockManagerReportUnavailable(t *testing.T) {
	stagedFile, err := os.CreateTemp(t.TempDir(), "staged-*")
	require.NoError(t, err)

	// a staged handle with no client has no lock manager to serve locks
	staged := newStagedHandle(nil, stagedFile, "/zone/home/orphan.txt", irodsclient_types.FileOpenModeWriteOnly, nil)

	err = staged.Setlk(wholeFileLock(FileLockTypeWrite, 1, 100))
	require.Error(t, err)
	assert.True(t, cockroach_errors.Is(err, ErrFileLockManagerUnavailable))
}
