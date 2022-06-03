package testcases

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"testing"

	"github.com/cyverse/go-irodsclient/fs"
	common_io "github.com/cyverse/irodsfs-common/io"
	common_cache "github.com/cyverse/irodsfs-common/io/cache"
	"github.com/cyverse/irodsfs-common/irods"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
)

const (
	kb int64 = int64(1024)
	mb int64 = int64(1024 * 1024)
)

var (
	ioTestID = xid.New().String()
)

func TestIO(t *testing.T) {
	setup()
	defer shutdown()

	makeHomeDir(t, ioTestID)

	t.Run("test VerySmallSyncWriteRead", testVerySmallSyncWriteRead)
	t.Run("test SmallSyncWriteRead", testSmallSyncWriteRead)
	t.Run("test LargeSyncWriteRead", testLargeSyncWriteRead)

	t.Run("test VerySmallSyncBufferedWriteRead", testVerySmallSyncBufferedWriteRead)
	t.Run("test SmallAsyncBufferedWriteRead", testSmallSyncBufferedWriteRead)
	t.Run("test LargeAsyncBufferedWriteRead", testLargeSyncBufferedWriteRead)

	t.Run("test VerySmallAsyncWriteRead", testVerySmallAsyncWriteRead)
	t.Run("test SmallAsyncWriteRead", testSmallAsyncWriteRead)
	t.Run("test LargeAsyncWriteRead", testLargeAsyncWriteRead)

	t.Run("test VerySmallAsyncWriteReadWithCache", testVerySmallAsyncWriteReadWithCache)
	t.Run("test SmallAsyncWriteReadWithCache", testSmallAsyncWriteReadWithCache)
	t.Run("test LargeAsyncWriteReadWithCache", testLargeAsyncWriteReadWithCache)
}

func testVerySmallSyncWriteRead(t *testing.T) {
	syncWriteRead(t, 1*kb)
	syncWriteRead(t, 16*kb)
	syncWriteRead(t, 16*kb+1)
	syncWriteRead(t, 17*kb)
	syncWriteRead(t, 32*kb)
	syncWriteRead(t, 32*kb+1)
	syncWriteRead(t, 33*kb)
}

func testSmallSyncWriteRead(t *testing.T) {
	syncWriteRead(t, 1*mb)
	syncWriteRead(t, 1*mb+1)
	syncWriteRead(t, 1*mb+100)
	syncWriteRead(t, 2*mb)
	syncWriteRead(t, 2*mb+100)
}

func testLargeSyncWriteRead(t *testing.T) {
	syncWriteRead(t, 10*mb)
	syncWriteRead(t, 10*mb+1)
	syncWriteRead(t, 10*mb+100)
	syncWriteRead(t, 20*mb)
	syncWriteRead(t, 20*mb+100)
}

func testVerySmallSyncBufferedWriteRead(t *testing.T) {
	syncBufferedWriteRead(t, 1*kb)
	syncBufferedWriteRead(t, 16*kb)
	syncBufferedWriteRead(t, 16*kb+1)
	syncBufferedWriteRead(t, 17*kb)
	syncBufferedWriteRead(t, 32*kb)
	syncBufferedWriteRead(t, 32*kb+1)
	syncBufferedWriteRead(t, 33*kb)
}

func testSmallSyncBufferedWriteRead(t *testing.T) {
	syncBufferedWriteRead(t, 1*mb)
	syncBufferedWriteRead(t, 1*mb+1)
	syncBufferedWriteRead(t, 1*mb+100)
	syncBufferedWriteRead(t, 2*mb)
	syncBufferedWriteRead(t, 2*mb+100)
}

func testLargeSyncBufferedWriteRead(t *testing.T) {
	syncBufferedWriteRead(t, 10*mb)
	syncBufferedWriteRead(t, 10*mb+1)
	syncBufferedWriteRead(t, 10*mb+100)
	syncBufferedWriteRead(t, 20*mb)
	syncBufferedWriteRead(t, 20*mb+100)
}

func testVerySmallAsyncWriteRead(t *testing.T) {
	asyncWriteRead(t, 1*kb)
	asyncWriteRead(t, 16*kb)
	asyncWriteRead(t, 16*kb+1)
	asyncWriteRead(t, 17*kb)
	asyncWriteRead(t, 32*kb)
	asyncWriteRead(t, 32*kb+1)
	asyncWriteRead(t, 33*kb)
}

func testSmallAsyncWriteRead(t *testing.T) {
	asyncWriteRead(t, 1*mb)
	asyncWriteRead(t, 1*mb+1)
	asyncWriteRead(t, 1*mb+100)
	asyncWriteRead(t, 2*mb)
	asyncWriteRead(t, 2*mb+100)
}

func testLargeAsyncWriteRead(t *testing.T) {
	asyncWriteRead(t, 10*mb)
	asyncWriteRead(t, 10*mb+1)
	asyncWriteRead(t, 10*mb+100)
	asyncWriteRead(t, 20*mb)
	asyncWriteRead(t, 20*mb+100)
}

func testVerySmallAsyncWriteReadWithCache(t *testing.T) {
	asyncWriteReadWithCache(t, 1*kb)
	asyncWriteReadWithCache(t, 16*kb)
	asyncWriteReadWithCache(t, 16*kb+1)
	asyncWriteReadWithCache(t, 17*kb)
	asyncWriteReadWithCache(t, 32*kb)
	asyncWriteReadWithCache(t, 32*kb+1)
	asyncWriteReadWithCache(t, 33*kb)
}

func testSmallAsyncWriteReadWithCache(t *testing.T) {
	asyncWriteReadWithCache(t, 1*mb)
	asyncWriteReadWithCache(t, 1*mb+1)
	asyncWriteReadWithCache(t, 1*mb+100)
	asyncWriteReadWithCache(t, 2*mb)
	asyncWriteReadWithCache(t, 2*mb+100)
}

func testLargeAsyncWriteReadWithCache(t *testing.T) {
	asyncWriteReadWithCache(t, 10*mb)
	asyncWriteReadWithCache(t, 10*mb+1)
	asyncWriteReadWithCache(t, 10*mb+100)
	asyncWriteReadWithCache(t, 20*mb)
	asyncWriteReadWithCache(t, 20*mb+100)
}

func syncWriteRead(t *testing.T, size int64) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("irodsfs-common-test")

	filesystem, err := irods.NewIRODSFSClientDirect(account, fsConfig)
	assert.NoError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(ioTestID)

	newDataObjectFilename := "testobj_sync_123"
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	// write
	writeHandle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	assert.NoError(t, err)

	writer := common_io.NewSyncWriter(writeHandle, nil)

	buf := makeTestDataBuf(16 * 1024)

	toWrite := size
	totalWrittenBytes := int64(0)

	writeHasher := sha1.New()
	for totalWrittenBytes < toWrite {
		written, writeErr := writer.WriteAt(buf, totalWrittenBytes)
		assert.NoError(t, writeErr)

		_, hashErr := writeHasher.Write(buf[:written])
		assert.NoError(t, hashErr)

		totalWrittenBytes += int64(written)
	}

	err = writer.Flush()
	assert.NoError(t, err)

	writer.Release()

	err = writeHandle.Close()
	assert.NoError(t, err)

	writeHashBytes := writeHasher.Sum(nil)
	writeHashString := hex.EncodeToString(writeHashBytes)

	// read
	readHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	assert.NoError(t, err)

	reader := common_io.NewSyncReader(readHandle, nil)
	totalReadBytes := int64(0)

	readHasher := sha1.New()
	readBuffer := make([]byte, 16*1024)
	for totalReadBytes < totalWrittenBytes {
		toRead := totalWrittenBytes - totalReadBytes
		if toRead > int64(len(readBuffer)) {
			toRead = int64(len(readBuffer))
		}

		read, readErr := reader.ReadAt(readBuffer[:int32(toRead)], totalReadBytes)
		_, hashErr := readHasher.Write(readBuffer[:read])
		assert.NoError(t, hashErr)

		totalReadBytes += int64(read)

		if readErr != nil && readErr == io.EOF {
			break
		}
		assert.NoError(t, readErr)
	}

	reader.Release()

	err = readHandle.Close()
	assert.NoError(t, err)

	readHashBytes := readHasher.Sum(nil)
	readHashString := hex.EncodeToString(readHashBytes)

	// compare
	assert.Equal(t, totalWrittenBytes, totalReadBytes)
	assert.Equal(t, writeHashString, readHashString)

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	assert.NoError(t, err)

	assert.False(t, filesystem.ExistsFile(newDataObjectPath))
}

func syncBufferedWriteRead(t *testing.T, size int64) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("irodsfs-common-test")

	filesystem, err := irods.NewIRODSFSClientDirect(account, fsConfig)
	assert.NoError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(ioTestID)

	newDataObjectFilename := "testobj_sync_123"
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	// write
	writeHandle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	assert.NoError(t, err)

	syncWriter := common_io.NewSyncWriter(writeHandle, nil)
	writer := common_io.NewSyncBufferedWriter(syncWriter, int(64*kb))

	buf := makeTestDataBuf(16 * 1024)

	toWrite := size
	totalWrittenBytes := int64(0)

	writeHasher := sha1.New()
	for totalWrittenBytes < toWrite {
		written, writeErr := writer.WriteAt(buf, totalWrittenBytes)
		assert.NoError(t, writeErr)

		_, hashErr := writeHasher.Write(buf[:written])
		assert.NoError(t, hashErr)

		totalWrittenBytes += int64(written)
	}

	err = writer.Flush()
	assert.NoError(t, err)

	writer.Release()

	err = writeHandle.Close()
	assert.NoError(t, err)

	writeHashBytes := writeHasher.Sum(nil)
	writeHashString := hex.EncodeToString(writeHashBytes)

	// read
	readHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	assert.NoError(t, err)

	reader := common_io.NewSyncReader(readHandle, nil)
	totalReadBytes := int64(0)

	readHasher := sha1.New()
	readBuffer := make([]byte, 16*1024)
	for totalReadBytes < totalWrittenBytes {
		toRead := totalWrittenBytes - totalReadBytes
		if toRead > int64(len(readBuffer)) {
			toRead = int64(len(readBuffer))
		}

		read, readErr := reader.ReadAt(readBuffer[:int32(toRead)], totalReadBytes)
		_, hashErr := readHasher.Write(readBuffer[:read])
		assert.NoError(t, hashErr)

		totalReadBytes += int64(read)

		if readErr != nil && readErr == io.EOF {
			break
		}
		assert.NoError(t, readErr)
	}

	reader.Release()

	err = readHandle.Close()
	assert.NoError(t, err)

	readHashBytes := readHasher.Sum(nil)
	readHashString := hex.EncodeToString(readHashBytes)

	// compare
	assert.Equal(t, totalWrittenBytes, totalReadBytes)
	assert.Equal(t, writeHashString, readHashString)

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	assert.NoError(t, err)

	assert.False(t, filesystem.ExistsFile(newDataObjectPath))
}

func asyncWriteRead(t *testing.T, size int64) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("irodsfs-common-test")

	filesystem, err := irods.NewIRODSFSClientDirect(account, fsConfig)
	assert.NoError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(ioTestID)

	newDataObjectFilename := "testobj_async_123"
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	// write
	writeHandle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	assert.NoError(t, err)

	syncWriter := common_io.NewSyncWriter(writeHandle, nil)
	writer := common_io.NewAsyncWriter(syncWriter, 16*1024, "/tmp")

	buf := makeTestDataBuf(16 * 1024)

	toWrite := size
	totalWrittenBytes := int64(0)

	writeHasher := sha1.New()
	for totalWrittenBytes < toWrite {
		written, writeErr := writer.WriteAt(buf, totalWrittenBytes)
		assert.NoError(t, writeErr)

		_, hashErr := writeHasher.Write(buf[:written])
		assert.NoError(t, hashErr)

		totalWrittenBytes += int64(written)
	}

	err = writer.Flush()
	assert.NoError(t, err)

	writer.Release()

	err = writeHandle.Close()
	assert.NoError(t, err)

	writeHashBytes := writeHasher.Sum(nil)
	writeHashString := hex.EncodeToString(writeHashBytes)

	// read
	readHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	assert.NoError(t, err)

	syncReader := common_io.NewSyncReader(readHandle, nil)
	reader := common_io.NewAsyncBlockReader(syncReader, 16*1024, 16*1024, "/tmp")
	totalReadBytes := int64(0)

	readHasher := sha1.New()
	readBuffer := make([]byte, 16*1024)
	for totalReadBytes < totalWrittenBytes {
		toRead := totalWrittenBytes - totalReadBytes
		if toRead > int64(len(readBuffer)) {
			toRead = int64(len(readBuffer))
		}

		read, readErr := reader.ReadAt(readBuffer[:int32(toRead)], totalReadBytes)
		_, hashErr := readHasher.Write(readBuffer[:read])
		assert.NoError(t, hashErr)

		totalReadBytes += int64(read)

		if readErr != nil && readErr == io.EOF {
			break
		}
		assert.NoError(t, readErr)
	}

	reader.Release()

	err = readHandle.Close()
	assert.NoError(t, err)

	readHashBytes := readHasher.Sum(nil)
	readHashString := hex.EncodeToString(readHashBytes)

	// compare
	assert.Equal(t, totalWrittenBytes, totalReadBytes)
	assert.Equal(t, writeHashString, readHashString)

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	assert.NoError(t, err)

	assert.False(t, filesystem.ExistsFile(newDataObjectPath))
}

func asyncWriteReadWithCache(t *testing.T, size int64) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("irodsfs-common-test")

	filesystem, err := irods.NewIRODSFSClientDirect(account, fsConfig)
	assert.NoError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(ioTestID)

	newDataObjectFilename := "testobj_async_123"
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	// write
	writeHandle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	assert.NoError(t, err)

	syncWriter := common_io.NewSyncWriter(writeHandle, nil)
	writer := common_io.NewAsyncWriter(syncWriter, 16*1024, "/tmp")

	buf := makeTestDataBuf(16 * 1024)

	toWrite := size
	totalWrittenBytes := int64(0)

	writeHasher := sha1.New()
	for totalWrittenBytes < toWrite {
		written, writeErr := writer.WriteAt(buf, totalWrittenBytes)
		assert.NoError(t, writeErr)

		_, hashErr := writeHasher.Write(buf[:written])
		assert.NoError(t, hashErr)

		totalWrittenBytes += int64(written)
	}

	err = writer.Flush()
	assert.NoError(t, err)

	writer.Release()

	err = writeHandle.Close()
	assert.NoError(t, err)

	writeHashBytes := writeHasher.Sum(nil)
	writeHashString := hex.EncodeToString(writeHashBytes)

	// read
	readHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	assert.NoError(t, err)

	cacheStore, err := common_cache.NewDiskCacheStore(100*mb, int(mb), "/tmp")
	assert.NoError(t, err)

	syncReader := common_io.NewSyncReader(readHandle, nil)
	reader := common_io.NewAsyncBlockReaderWithCache(syncReader, 16*1024, 16*1024, "fake_checksum", cacheStore, "/tmp")
	totalReadBytes := int64(0)

	readHasher := sha1.New()
	readBuffer := make([]byte, 16*1024)
	for totalReadBytes < totalWrittenBytes {
		toRead := totalWrittenBytes - totalReadBytes
		if toRead > int64(len(readBuffer)) {
			toRead = int64(len(readBuffer))
		}

		read, readErr := reader.ReadAt(readBuffer[:int32(toRead)], totalReadBytes)
		_, hashErr := readHasher.Write(readBuffer[:read])
		assert.NoError(t, hashErr)

		totalReadBytes += int64(read)

		if readErr != nil && readErr == io.EOF {
			break
		}
		assert.NoError(t, readErr)
	}

	reader.Release()

	readHashBytes := readHasher.Sum(nil)
	readHashString := hex.EncodeToString(readHashBytes)

	// compare
	assert.Equal(t, totalWrittenBytes, totalReadBytes)
	assert.Equal(t, writeHashString, readHashString)

	// read again. must hit cache
	totalReadBytes = int64(0)

	readHasher = sha1.New()
	for totalReadBytes < totalWrittenBytes {
		toRead := totalWrittenBytes - totalReadBytes
		if toRead > int64(len(readBuffer)) {
			toRead = int64(len(readBuffer))
		}

		read, readErr := reader.ReadAt(readBuffer[:int32(toRead)], totalReadBytes)
		_, hashErr := readHasher.Write(readBuffer[:read])
		assert.NoError(t, hashErr)

		totalReadBytes += int64(read)

		if readErr != nil && readErr == io.EOF {
			break
		}
		assert.NoError(t, readErr)
	}

	err = readHandle.Close()
	assert.NoError(t, err)

	readHashBytes = readHasher.Sum(nil)
	readHashString = hex.EncodeToString(readHashBytes)

	// compare
	assert.Equal(t, totalWrittenBytes, totalReadBytes)
	assert.Equal(t, writeHashString, readHashString)

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	assert.NoError(t, err)

	assert.False(t, filesystem.ExistsFile(newDataObjectPath))
}
