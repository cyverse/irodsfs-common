package irods

import (
	"testing"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSharedWriteHandleSatisfies(t *testing.T) {
	tests := []struct {
		name     string
		held     irodsclient_types.FileOpenMode
		asked    irodsclient_types.FileOpenMode
		expected bool
	}{
		{"read-write serves read-write", irodsclient_types.FileOpenModeReadWrite, irodsclient_types.FileOpenModeReadWrite, true},
		{"read-write serves write-only", irodsclient_types.FileOpenModeReadWrite, irodsclient_types.FileOpenModeWriteOnly, true},
		{"read-write serves append", irodsclient_types.FileOpenModeReadWrite, irodsclient_types.FileOpenModeAppend, true},
		{"write-only serves write-only", irodsclient_types.FileOpenModeWriteOnly, irodsclient_types.FileOpenModeWriteOnly, true},
		{"write-only does not serve read-write", irodsclient_types.FileOpenModeWriteOnly, irodsclient_types.FileOpenModeReadWrite, false},
		{"write-only does not serve read-append", irodsclient_types.FileOpenModeWriteOnly, irodsclient_types.FileOpenModeReadAppend, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			shared := &sharedWriteHandle{openMode: test.held}
			assert.Equal(t, test.expected, shared.satisfies(test.asked))
		})
	}
}

func TestAcquireWriteHandleSharesOneHandle(t *testing.T) {
	client := &IRODSFSClientDirect{writeHandles: map[string]*sharedWriteHandle{}}
	backing := &irodsclient_fs.FileHandle{}

	opens := 0
	open := func() (*irodsclient_fs.FileHandle, error) {
		opens++
		return backing, nil
	}

	first, err := client.acquireWriteHandle("/zone/home/file", irodsclient_types.FileOpenModeReadWrite, open)
	require.NoError(t, err)

	second, err := client.acquireWriteHandle("/zone/home/file", irodsclient_types.FileOpenModeReadWrite, open)
	require.NoError(t, err)

	// iRODS refuses a second write open, so the two handles must share one
	assert.Equal(t, 1, opens, "the file must be opened once")
	assert.Same(t, first, second)
	assert.Equal(t, 2, client.writeHandles["/zone/home/file"].refCount)

	// giving one back keeps the handle for the other
	shared, err := client.releaseWriteHandle("/zone/home/file", backing)
	require.NoError(t, err)
	assert.True(t, shared)
	assert.Equal(t, 1, client.writeHandles["/zone/home/file"].refCount)
}

func TestAcquireWriteHandleRejectsAnUnservableMode(t *testing.T) {
	client := &IRODSFSClientDirect{writeHandles: map[string]*sharedWriteHandle{}}
	open := func() (*irodsclient_fs.FileHandle, error) { return &irodsclient_fs.FileHandle{}, nil }

	_, err := client.acquireWriteHandle("/zone/home/file", irodsclient_types.FileOpenModeWriteOnly, open)
	require.NoError(t, err)

	// the open handle cannot read, so it cannot serve a read-write open
	_, err = client.acquireWriteHandle("/zone/home/file", irodsclient_types.FileOpenModeReadWrite, open)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already open")
}

func TestReleaseWriteHandleIgnoresAnotherHandle(t *testing.T) {
	client := &IRODSFSClientDirect{writeHandles: map[string]*sharedWriteHandle{}}
	backing := &irodsclient_fs.FileHandle{}
	open := func() (*irodsclient_fs.FileHandle, error) { return backing, nil }

	_, err := client.acquireWriteHandle("/zone/home/file", irodsclient_types.FileOpenModeReadWrite, open)
	require.NoError(t, err)

	// a read-only handle, or one left over from before a rename, is not the
	// shared handle of this path and is closed by its owner instead
	shared, err := client.releaseWriteHandle("/zone/home/file", &irodsclient_fs.FileHandle{})
	require.NoError(t, err)
	assert.False(t, shared)
	assert.Equal(t, 1, client.writeHandles["/zone/home/file"].refCount)

	shared, err = client.releaseWriteHandle("/zone/home/other", backing)
	require.NoError(t, err)
	assert.False(t, shared)
}

func TestRenameWriteHandleFollowsTheFile(t *testing.T) {
	client := &IRODSFSClientDirect{writeHandles: map[string]*sharedWriteHandle{}}
	backing := &irodsclient_fs.FileHandle{}
	open := func() (*irodsclient_fs.FileHandle, error) { return backing, nil }

	_, err := client.acquireWriteHandle("/zone/home/old", irodsclient_types.FileOpenModeReadWrite, open)
	require.NoError(t, err)

	client.renameWriteHandle("/zone/home/old", "/zone/home/new")

	assert.NotContains(t, client.writeHandles, "/zone/home/old")
	require.Contains(t, client.writeHandles, "/zone/home/new")

	// an open of the new path shares the handle that followed it
	again, err := client.acquireWriteHandle("/zone/home/new", irodsclient_types.FileOpenModeReadWrite, func() (*irodsclient_fs.FileHandle, error) {
		t.Fatal("the file must not be opened again")
		return nil, nil
	})
	require.NoError(t, err)
	assert.Same(t, backing, again)
	assert.Equal(t, 2, client.writeHandles["/zone/home/new"].refCount)
}
