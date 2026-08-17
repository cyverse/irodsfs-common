package irods

import (
	"io"
	"os"
	"testing"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/irods/cache"
	"github.com/cyverse/irodsfs-common/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	log "github.com/sirupsen/logrus"
)

// mockFileHandle implements IRODSFSFileHandle for testing
type mockFileHandle struct {
	id       string
	entry    *irodsclient_fs.Entry
	openMode irodsclient_types.FileOpenMode
	data     []byte
}

func newMockFileHandle(path string, data []byte, mode irodsclient_types.FileOpenMode) *mockFileHandle {
	return &mockFileHandle{
		id: "mock-handle-1",
		entry: &irodsclient_fs.Entry{
			Type:       irodsclient_fs.FileEntry,
			Name:       "test.dat",
			Path:       path,
			Size:       int64(len(data)),
			CreateTime: time.Now(),
			ModifyTime: time.Now(),
		},
		openMode: mode,
		data:     data,
	}
}

func (h *mockFileHandle) GetID() string                                    { return h.id }
func (h *mockFileHandle) GetEntry() *irodsclient_fs.Entry                  { return h.entry }
func (h *mockFileHandle) GetOpenMode() irodsclient_types.FileOpenMode      { return h.openMode }
func (h *mockFileHandle) IsReadMode() bool                                 { return h.openMode.IsRead() }
func (h *mockFileHandle) IsWriteMode() bool                                { return h.openMode.IsWrite() }
func (h *mockFileHandle) GetAvailable(offset int64) int64                  { return int64(len(h.data)) - offset }
func (h *mockFileHandle) Flush() error                                     { return nil }
func (h *mockFileHandle) Close() error                                     { return nil }
func (h *mockFileHandle) Truncate(size int64) error {
	if size < int64(len(h.data)) {
		h.data = h.data[:size]
	} else {
		h.data = append(h.data, make([]byte, size-int64(len(h.data)))...)
	}
	h.entry.Size = size
	return nil
}

func (h *mockFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	if offset >= int64(len(h.data)) {
		return 0, io.EOF
	}

	n := copy(buffer, h.data[offset:])
	if offset+int64(n) >= int64(len(h.data)) {
		return n, io.EOF
	}
	return n, nil
}

func (h *mockFileHandle) WriteAt(data []byte, offset int64) (int, error) {
	end := offset + int64(len(data))
	if end > int64(len(h.data)) {
		h.data = append(h.data, make([]byte, end-int64(len(h.data)))...)
		h.entry.Size = end
	}
	copy(h.data[offset:], data)
	return len(data), nil
}

// helper to create a test cache manager
func newTestCacheManager(t *testing.T) *cache.MemoryCacheManager {
	config := &cache.MemoryCacheConfig{
		NumCounters: 1000,
		MaxCost:     1024 * 1024,
		BufferItems: 64,
		TTL:         1 * time.Hour,
		Name:        "test",
	}
	mgr, err := cache.NewMemoryCacheManager(config)
	require.NoError(t, err)
	return mgr
}

func newTestLogger() *log.Entry {
	return log.NewEntry(log.StandardLogger())
}

// --- IRODSFSClientBufferedFileHandle Tests ---

func TestBufferedFileHandleReadAtCacheHit(t *testing.T) {
	cacheMgr := newTestCacheManager(t)
	defer cacheMgr.Release()

	blockSize := 16
	helper := util.NewFileBlockHelper(blockSize)
	data := []byte("0123456789abcdef") // exactly 1 block (16 bytes)
	mock := newMockFileHandle("/test/file.dat", data, irodsclient_types.FileOpenModeReadOnly)

	handle := &IRODSFSClientBufferedFileHandle{
		id:        "test-handle",
		client:    &IRODSFSClientBuffered{logger: newTestLogger()},
		handle:    mock,
		cache:     cacheMgr,
		irodsPath: "/test/file.dat",
		helper:    helper,
		logger:    newTestLogger(),
	}

	// Pre-populate cache for block 0 (simulating a previous read)
	cacheKey := handle.makeCacheKey(0)
	cacheMgr.PutCopy(cacheKey, data, true)

	// Modify the underlying data — if cache works, we should still get original
	mock.data = []byte("XXXXXXXXXXXXXXXX")

	buf := make([]byte, 16)
	n, err := handle.ReadAt(buf, 0)
	assert.Equal(t, 16, n)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, []byte("0123456789abcdef"), buf) // cached data, not modified
}

func TestBufferedFileHandleReadAtCrossBock(t *testing.T) {
	cacheMgr := newTestCacheManager(t)
	defer cacheMgr.Release()

	blockSize := 8
	helper := util.NewFileBlockHelper(blockSize)
	data := []byte("AAAAAAAABBBBBBBBCCCCCCCC") // 3 blocks of 8 bytes
	mock := newMockFileHandle("/test/cross.dat", data, irodsclient_types.FileOpenModeReadOnly)

	handle := &IRODSFSClientBufferedFileHandle{
		id:        "test-handle",
		client:    &IRODSFSClientBuffered{logger: newTestLogger()},
		handle:    mock,
		cache:     cacheMgr,
		irodsPath: "/test/cross.dat",
		helper:    helper,
		logger:    newTestLogger(),
	}

	// Read across block boundary: offset 4, len 8 (spans block 0 and block 1)
	buf := make([]byte, 8)
	n, err := handle.ReadAt(buf, 4)
	assert.Equal(t, 8, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte("AAAABBBB"), buf)
}

func TestBufferedFileHandleReadAtPartialLastBlock(t *testing.T) {
	cacheMgr := newTestCacheManager(t)
	defer cacheMgr.Release()

	blockSize := 8
	helper := util.NewFileBlockHelper(blockSize)
	data := []byte("AAAAAAAABBB") // block 0: 8 bytes, block 1: 3 bytes
	mock := newMockFileHandle("/test/partial.dat", data, irodsclient_types.FileOpenModeReadOnly)

	handle := &IRODSFSClientBufferedFileHandle{
		id:        "test-handle",
		client:    &IRODSFSClientBuffered{logger: newTestLogger()},
		handle:    mock,
		cache:     cacheMgr,
		irodsPath: "/test/partial.dat",
		helper:    helper,
		logger:    newTestLogger(),
	}

	// Read from block 1 (partial block, only 3 bytes)
	buf := make([]byte, 8)
	n, err := handle.ReadAt(buf, 8)
	assert.Equal(t, 3, n)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, []byte("BBB"), buf[:3])
}

func TestBufferedFileHandleReadAtBeyondEOF(t *testing.T) {
	cacheMgr := newTestCacheManager(t)
	defer cacheMgr.Release()

	blockSize := 16
	helper := util.NewFileBlockHelper(blockSize)
	data := []byte("short")
	mock := newMockFileHandle("/test/short.dat", data, irodsclient_types.FileOpenModeReadOnly)

	handle := &IRODSFSClientBufferedFileHandle{
		id:        "test-handle",
		client:    &IRODSFSClientBuffered{logger: newTestLogger()},
		handle:    mock,
		cache:     cacheMgr,
		irodsPath: "/test/short.dat",
		helper:    helper,
		logger:    newTestLogger(),
	}

	// Read beyond file size
	buf := make([]byte, 10)
	n, err := handle.ReadAt(buf, 100)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}

func TestBufferedFileHandleReadAtClampToFileSize(t *testing.T) {
	cacheMgr := newTestCacheManager(t)
	defer cacheMgr.Release()

	blockSize := 16
	helper := util.NewFileBlockHelper(blockSize)
	data := []byte("hello")
	mock := newMockFileHandle("/test/clamp.dat", data, irodsclient_types.FileOpenModeReadOnly)

	handle := &IRODSFSClientBufferedFileHandle{
		id:        "test-handle",
		client:    &IRODSFSClientBuffered{logger: newTestLogger()},
		handle:    mock,
		cache:     cacheMgr,
		irodsPath: "/test/clamp.dat",
		helper:    helper,
		logger:    newTestLogger(),
	}

	// Request more bytes than file has
	buf := make([]byte, 100)
	n, err := handle.ReadAt(buf, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, []byte("hello"), buf[:5])
}

func TestBufferedFileHandleWriteAtInvalidatesCache(t *testing.T) {
	cacheMgr := newTestCacheManager(t)
	defer cacheMgr.Release()

	blockSize := 8
	helper := util.NewFileBlockHelper(blockSize)
	data := []byte("AAAAAAAABBBBBBBB") // 2 blocks
	mock := newMockFileHandle("/test/write.dat", data, irodsclient_types.FileOpenModeReadWrite)

	handle := &IRODSFSClientBufferedFileHandle{
		id:        "test-handle",
		client:    &IRODSFSClientBuffered{logger: newTestLogger()},
		handle:    mock,
		cache:     cacheMgr,
		irodsPath: "/test/write.dat",
		helper:    helper,
		logger:    newTestLogger(),
	}

	// Pre-populate cache for block 0
	cacheKey0 := handle.makeCacheKey(0)
	cacheMgr.Put(cacheKey0, []byte("AAAAAAAA"), true)
	assert.True(t, cacheMgr.Has(cacheKey0))

	// Write to block 0
	n, err := handle.WriteAt([]byte("XXXX"), 0)
	assert.NoError(t, err)
	assert.Equal(t, 4, n)

	// Cache for block 0 should be invalidated
	assert.False(t, cacheMgr.Has(cacheKey0))

	// Block 1 cache should still be valid
	cacheKey1 := handle.makeCacheKey(1)
	cacheMgr.Put(cacheKey1, []byte("BBBBBBBB"), true)
	assert.True(t, cacheMgr.Has(cacheKey1))
}

func TestBufferedFileHandleTruncateInvalidatesCache(t *testing.T) {
	cacheMgr := newTestCacheManager(t)
	defer cacheMgr.Release()

	blockSize := 8
	helper := util.NewFileBlockHelper(blockSize)
	data := []byte("AAAAAAAABBBBBBBB") // 2 blocks
	mock := newMockFileHandle("/test/trunc.dat", data, irodsclient_types.FileOpenModeReadWrite)

	handle := &IRODSFSClientBufferedFileHandle{
		id:        "test-handle",
		client:    &IRODSFSClientBuffered{logger: newTestLogger()},
		handle:    mock,
		cache:     cacheMgr,
		irodsPath: "/test/trunc.dat",
		helper:    helper,
		logger:    newTestLogger(),
	}

	// Populate both block caches
	cacheMgr.Put(handle.makeCacheKey(0), []byte("AAAAAAAA"), true)
	cacheMgr.Put(handle.makeCacheKey(1), []byte("BBBBBBBB"), true)

	// Truncate to 4 bytes (within block 0)
	err := handle.Truncate(4)
	assert.NoError(t, err)

	// Block 0 should be invalidated (it's <= lastBlockID)
	assert.False(t, cacheMgr.Has(handle.makeCacheKey(0)))
}

func TestBufferedFileHandleReadNotWriteMode(t *testing.T) {
	cacheMgr := newTestCacheManager(t)
	defer cacheMgr.Release()

	blockSize := 16
	helper := util.NewFileBlockHelper(blockSize)
	data := []byte("data")
	mock := newMockFileHandle("/test/wo.dat", data, irodsclient_types.FileOpenModeWriteOnly)

	handle := &IRODSFSClientBufferedFileHandle{
		id:        "test-handle",
		client:    &IRODSFSClientBuffered{logger: newTestLogger()},
		handle:    mock,
		cache:     cacheMgr,
		irodsPath: "/test/wo.dat",
		helper:    helper,
		logger:    newTestLogger(),
	}

	buf := make([]byte, 4)
	_, err := handle.ReadAt(buf, 0)
	assert.Error(t, err)
}

// --- IRODSFSClientBufferedStagedHandle Tests ---

func TestStagedHandleWriteAndRead(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "staged-test-*")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	handle := newStagedHandle(nil, tmpFile, "/test/staged.dat",
		irodsclient_types.FileOpenModeReadWrite,
		&irodsclient_fs.Entry{
			Type: irodsclient_fs.FileEntry,
			Name: "staged.dat",
			Path: "/test/staged.dat",
			Size: 0,
		})

	// Write data
	n, err := handle.WriteAt([]byte("hello world"), 0)
	assert.NoError(t, err)
	assert.Equal(t, 11, n)

	// Entry size should update
	assert.Equal(t, int64(11), handle.GetEntry().Size)

	// Read back
	buf := make([]byte, 11)
	n, err = handle.ReadAt(buf, 0)
	assert.Equal(t, 11, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello world"), buf)
}

func TestStagedHandleWriteOnly(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "staged-wo-*")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	handle := newStagedHandle(nil, tmpFile, "/test/wo.dat",
		irodsclient_types.FileOpenModeWriteOnly,
		&irodsclient_fs.Entry{
			Type: irodsclient_fs.FileEntry,
			Name: "wo.dat",
			Path: "/test/wo.dat",
			Size: 0,
		})

	// Write should succeed
	n, err := handle.WriteAt([]byte("data"), 0)
	assert.NoError(t, err)
	assert.Equal(t, 4, n)

	// Read should fail (write-only mode)
	buf := make([]byte, 4)
	_, err = handle.ReadAt(buf, 0)
	assert.ErrorIs(t, err, ErrNotReadMode)
}

func TestStagedHandleReadOnly(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "staged-ro-*")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Pre-write data to the file
	tmpFile.Write([]byte("existing data"))
	tmpFile.Sync()

	handle := newStagedHandle(nil, tmpFile, "/test/ro.dat",
		irodsclient_types.FileOpenModeReadOnly,
		&irodsclient_fs.Entry{
			Type: irodsclient_fs.FileEntry,
			Name: "ro.dat",
			Path: "/test/ro.dat",
			Size: 13,
		})

	// Read should succeed
	buf := make([]byte, 13)
	n, err := handle.ReadAt(buf, 0)
	assert.Equal(t, 13, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte("existing data"), buf)

	// Write should fail (read-only mode)
	_, err = handle.WriteAt([]byte("x"), 0)
	assert.ErrorIs(t, err, ErrNotWriteMode)
}

func TestStagedHandleTruncate(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "staged-trunc-*")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	handle := newStagedHandle(nil, tmpFile, "/test/trunc.dat",
		irodsclient_types.FileOpenModeReadWrite,
		&irodsclient_fs.Entry{
			Type: irodsclient_fs.FileEntry,
			Name: "trunc.dat",
			Path: "/test/trunc.dat",
			Size: 0,
		})

	// Write data
	handle.WriteAt([]byte("hello world"), 0)
	assert.Equal(t, int64(11), handle.GetEntry().Size)

	// Truncate
	err = handle.Truncate(5)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), handle.GetEntry().Size)

	// Read should return truncated data
	buf := make([]byte, 10)
	n, err := handle.ReadAt(buf, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, []byte("hello"), buf[:5])
}

func TestStagedHandleFlush(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "staged-flush-*")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	handle := newStagedHandle(nil, tmpFile, "/test/flush.dat",
		irodsclient_types.FileOpenModeWriteOnly,
		&irodsclient_fs.Entry{
			Type: irodsclient_fs.FileEntry,
			Name: "flush.dat",
			Path: "/test/flush.dat",
			Size: 0,
		})

	handle.WriteAt([]byte("flush me"), 0)

	err = handle.Flush()
	assert.NoError(t, err)

	// Verify data persisted to disk
	content, err := os.ReadFile(tmpFile.Name())
	assert.NoError(t, err)
	assert.Equal(t, []byte("flush me"), content)
}

func TestStagedHandleClose(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "staged-close-*")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Use nil client to skip cache invalidation in unit test
	handle := newStagedHandle(nil, tmpFile, "/test/close.dat",
		irodsclient_types.FileOpenModeWriteOnly,
		&irodsclient_fs.Entry{
			Type: irodsclient_fs.FileEntry,
			Name: "close.dat",
			Path: "/test/close.dat",
			Size: 0,
		})

	handle.WriteAt([]byte("data"), 0)

	err = handle.Close()
	assert.NoError(t, err)

	// File should be closed — further operations should fail
	_, err = tmpFile.Write([]byte("x"))
	assert.Error(t, err)
}

func TestStagedHandleGetAvailable(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "staged-avail-*")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	handle := newStagedHandle(nil, tmpFile, "/test/avail.dat",
		irodsclient_types.FileOpenModeReadWrite,
		&irodsclient_fs.Entry{
			Type: irodsclient_fs.FileEntry,
			Name: "avail.dat",
			Path: "/test/avail.dat",
			Size: 0,
		})

	handle.WriteAt([]byte("0123456789"), 0)

	assert.Equal(t, int64(10), handle.GetAvailable(0))
	assert.Equal(t, int64(5), handle.GetAvailable(5))
	assert.Equal(t, int64(0), handle.GetAvailable(10))
	assert.Equal(t, int64(0), handle.GetAvailable(15))
}

func TestStagedHandleRandomWrite(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "staged-random-*")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	handle := newStagedHandle(nil, tmpFile, "/test/random.dat",
		irodsclient_types.FileOpenModeReadWrite,
		&irodsclient_fs.Entry{
			Type: irodsclient_fs.FileEntry,
			Name: "random.dat",
			Path: "/test/random.dat",
			Size: 0,
		})

	// Write at different offsets (simulating random I/O)
	handle.WriteAt([]byte("AAAA"), 0)
	handle.WriteAt([]byte("BBBB"), 100)
	handle.WriteAt([]byte("CCCC"), 50)

	assert.Equal(t, int64(104), handle.GetEntry().Size)

	// Read back at specific offsets
	buf := make([]byte, 4)
	n, err := handle.ReadAt(buf, 0)
	assert.Equal(t, 4, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte("AAAA"), buf)

	n, err = handle.ReadAt(buf, 50)
	assert.Equal(t, 4, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte("CCCC"), buf)

	n, err = handle.ReadAt(buf, 100)
	assert.Equal(t, 4, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte("BBBB"), buf)
}

func TestStagedHandleForNewFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "staged-new-*")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	handle := newStagedHandleForNewFile(nil, tmpFile, "/zone/home/user/newfile.txt",
		irodsclient_types.FileOpenModeWriteTruncate)

	assert.Equal(t, "newfile.txt", handle.GetEntry().Name)
	assert.Equal(t, "/zone/home/user/newfile.txt", handle.GetEntry().Path)
	assert.Equal(t, int64(0), handle.GetEntry().Size)
	assert.Equal(t, irodsclient_types.FileOpenModeWriteTruncate, handle.GetOpenMode())
	assert.True(t, handle.IsWriteMode())
	assert.False(t, handle.IsReadMode())
}

func TestStagedHandleModes(t *testing.T) {
	tests := []struct {
		mode    irodsclient_types.FileOpenMode
		isRead  bool
		isWrite bool
	}{
		{irodsclient_types.FileOpenModeReadOnly, true, false},
		{irodsclient_types.FileOpenModeReadWrite, true, true},
		{irodsclient_types.FileOpenModeWriteOnly, false, true},
		{irodsclient_types.FileOpenModeWriteTruncate, false, true},
		{irodsclient_types.FileOpenModeAppend, false, true},
		{irodsclient_types.FileOpenModeReadAppend, true, true},
	}

	for _, tc := range tests {
		t.Run(string(tc.mode), func(t *testing.T) {
			tmpFile, err := os.CreateTemp("", "staged-mode-*")
			require.NoError(t, err)
			defer os.Remove(tmpFile.Name())

			handle := newStagedHandle(nil, tmpFile, "/test/mode.dat", tc.mode,
				&irodsclient_fs.Entry{
					Type: irodsclient_fs.FileEntry,
					Name: "mode.dat",
					Path: "/test/mode.dat",
					Size: 0,
				})

			assert.Equal(t, tc.isRead, handle.IsReadMode())
			assert.Equal(t, tc.isWrite, handle.IsWriteMode())

			handle.Close()
		})
	}
}
