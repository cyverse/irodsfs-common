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
	"github.com/stretchr/testify/assert"
)

const (
	kb int64 = int64(1024)
	mb int64 = int64(1024 * 1024)
	gb int64 = int64(1024 * 1024 * 1024)

	iRODSIOBlockSize   int = 16 * 1024 * 1024 // 16MB
	iRODSReadWriteSize int = 128 * 1024       // 128KB
)

func getIOTest() Test {
	return Test{
		Name: "I/O",
		Func: ioTest,
	}
}

func ioTest(t *testing.T, test *Test) {
	t.Run("VerySmallSyncWriteRead", testVerySmallSyncWriteRead)
	t.Run("SmallSyncWriteRead", testSmallSyncWriteRead)
	t.Run("LargeSyncWriteRead", testLargeSyncWriteRead)

	t.Run("VerySmallSyncBufferedWriteRead", testVerySmallSyncBufferedWriteRead)
	t.Run("SmallSyncBufferedWriteRead", testSmallSyncBufferedWriteRead)
	t.Run("LargeSyncBufferedWriteRead", testLargeSyncBufferedWriteRead)

	t.Run("VerySmallAsyncWriteRead", testVerySmallAsyncWriteRead)
	t.Run("SmallAsyncWriteRead", testSmallAsyncWriteRead)
	t.Run("LargeAsyncWriteRead", testLargeAsyncWriteRead)

	t.Run("VerySmallAsyncWriteReadWithCache", testVerySmallAsyncWriteReadWithCache)
	t.Run("SmallAsyncWriteReadWithCache", testSmallAsyncWriteReadWithCache)
	t.Run("LargeAsyncWriteReadWithCache", testLargeAsyncWriteReadWithCache)

	t.Run("VerySmallAsyncWriteReadWithPrefetch", testVerySmallAsyncWriteReadWithPrefetch)
	t.Run("SmallAsyncWriteReadWithPrefetch", testSmallAsyncWriteReadWithPrefetch)
	t.Run("LargeAsyncWriteReadWithPrefetch", testLargeAsyncWriteReadWithPrefetch)
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
	syncBufferedWriteRead(t, 20*mb)
	syncBufferedWriteRead(t, 20*mb+1)
	syncBufferedWriteRead(t, 20*mb+100)
	syncBufferedWriteRead(t, 50*mb)
	syncBufferedWriteRead(t, 50*mb+100)
	syncBufferedWriteRead(t, 100*mb)
	syncBufferedWriteRead(t, 100*mb+100)
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
	asyncWriteRead(t, 20*mb)
	asyncWriteRead(t, 20*mb+1)
	asyncWriteRead(t, 20*mb+100)
	asyncWriteRead(t, 50*mb)
	asyncWriteRead(t, 50*mb+100)
	asyncWriteRead(t, 100*mb)
	asyncWriteRead(t, 100*mb+100)
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
	asyncWriteReadWithCache(t, 20*mb)
	asyncWriteReadWithCache(t, 20*mb+1)
	asyncWriteReadWithCache(t, 20*mb+100)
	asyncWriteReadWithCache(t, 50*mb)
	asyncWriteReadWithCache(t, 50*mb+100)
	asyncWriteReadWithCache(t, 100*mb)
	asyncWriteReadWithCache(t, 100*mb+100)
}

func testVerySmallAsyncWriteReadWithPrefetch(t *testing.T) {
	asyncWriteReadWithPrefetch(t, 1*kb)
	asyncWriteReadWithPrefetch(t, 16*kb)
	asyncWriteReadWithPrefetch(t, 16*kb+1)
	asyncWriteReadWithPrefetch(t, 17*kb)
	asyncWriteReadWithPrefetch(t, 32*kb)
	asyncWriteReadWithPrefetch(t, 32*kb+1)
	asyncWriteReadWithPrefetch(t, 33*kb)
}

func testSmallAsyncWriteReadWithPrefetch(t *testing.T) {
	asyncWriteReadWithPrefetch(t, 1*mb)
	asyncWriteReadWithPrefetch(t, 1*mb+1)
	asyncWriteReadWithPrefetch(t, 1*mb+100)
	asyncWriteReadWithPrefetch(t, 2*mb)
	asyncWriteReadWithPrefetch(t, 2*mb+100)
}

func testLargeAsyncWriteReadWithPrefetch(t *testing.T) {
	asyncWriteReadWithPrefetch(t, 20*mb)
	asyncWriteReadWithPrefetch(t, 20*mb+1)
	asyncWriteReadWithPrefetch(t, 20*mb+100)
	asyncWriteReadWithPrefetch(t, 50*mb)
	asyncWriteReadWithPrefetch(t, 50*mb+100)
	asyncWriteReadWithPrefetch(t, 100*mb)
	asyncWriteReadWithPrefetch(t, 100*mb+100)
}

func syncWriteRead(t *testing.T, size int64) {
	t.Logf("Testing size %d", size)

	test := GetCurrentTest()
	server := test.GetCurrentServer()
	serverInfo := server.GetInfo()
	account, err := serverInfo.GetAccount()
	FailError(t, err)

	fsConfig := fs.NewFileSystemConfig("irodsfs-common-test")

	filesystem, err := irods.NewIRODSFSClientDirect(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	newDataObjectFilename := "testobj_sync_123"
	newDataObjectPath := homeDir + "/" + newDataObjectFilename
	// write
	writeHandle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	FailError(t, err)

	writer := common_io.NewSyncWriter(filesystem, writeHandle)

	toWrite := size
	totalWrittenBytes := int64(0)

	writeHasher := sha1.New()
	for totalWrittenBytes < toWrite {
		buf := MakeRandomContentDataBuf(16 * 1024)
		writeLen := toWrite - totalWrittenBytes
		if writeLen > int64(len(buf)) {
			writeLen = int64(len(buf))
		}

		written, writeErr := writer.WriteAt(buf[:writeLen], totalWrittenBytes)
		FailError(t, writeErr)
		if writeErr != nil {
			break
		}

		_, hashErr := writeHasher.Write(buf[:written])
		FailError(t, hashErr)
		if hashErr != nil {
			break
		}

		totalWrittenBytes += int64(written)
	}

	err = writer.Flush()
	FailError(t, err)

	writer.Release()

	err = writeHandle.Close()
	FailError(t, err)

	writeHashBytes := writeHasher.Sum(nil)
	writeHashString := hex.EncodeToString(writeHashBytes)

	// read
	readHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	FailError(t, err)

	reader := common_io.NewSyncReader(filesystem, readHandle)
	totalReadBytes := int64(0)

	readHasher := sha1.New()
	readBuffer := make([]byte, iRODSReadWriteSize)
	for totalReadBytes < totalWrittenBytes {
		read, readErr := reader.ReadAt(readBuffer, totalReadBytes)
		if readErr != nil && readErr != io.EOF {
			FailError(t, readErr)
			break
		}

		_, hashErr := readHasher.Write(readBuffer[:read])
		FailError(t, hashErr)
		if hashErr != nil {
			break
		}

		totalReadBytes += int64(read)

		if readErr == io.EOF {
			break
		}
	}

	reader.Release()

	err = readHandle.Close()
	FailError(t, err)

	readHashBytes := readHasher.Sum(nil)
	readHashString := hex.EncodeToString(readHashBytes)

	// compare
	assert.Equal(t, totalWrittenBytes, totalReadBytes)
	assert.Equal(t, writeHashString, readHashString)

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	FailError(t, err)

	assert.False(t, filesystem.ExistsFile(newDataObjectPath))
}

func syncBufferedWriteRead(t *testing.T, size int64) {
	t.Logf("Testing size %d", size)

	test := GetCurrentTest()
	server := test.GetCurrentServer()
	serverInfo := server.GetInfo()
	account, err := serverInfo.GetAccount()
	FailError(t, err)

	fsConfig := fs.NewFileSystemConfig("irodsfs-common-test")

	filesystem, err := irods.NewIRODSFSClientDirect(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	newDataObjectFilename := "testobj_sync_123"
	newDataObjectPath := homeDir + "/" + newDataObjectFilename

	// write
	writeHandle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	FailError(t, err)

	syncWriter := common_io.NewSyncWriter(filesystem, writeHandle)
	writer := common_io.NewSyncBufferedWriter(syncWriter, int(64*kb))

	toWrite := size
	totalWrittenBytes := int64(0)

	writeHasher := sha1.New()
	for totalWrittenBytes < toWrite {
		buf := MakeRandomContentDataBuf(16 * 1024)
		writeLen := toWrite - totalWrittenBytes
		if writeLen > int64(len(buf)) {
			writeLen = int64(len(buf))
		}

		written, writeErr := writer.WriteAt(buf[:writeLen], totalWrittenBytes)
		FailError(t, writeErr)
		if writeErr != nil {
			break
		}

		_, hashErr := writeHasher.Write(buf[:written])
		FailError(t, hashErr)
		if hashErr != nil {
			break
		}

		totalWrittenBytes += int64(written)
	}

	err = writer.Flush()
	FailError(t, err)

	writer.Release()

	err = writeHandle.Close()
	FailError(t, err)

	writeHashBytes := writeHasher.Sum(nil)
	writeHashString := hex.EncodeToString(writeHashBytes)

	// read
	readHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	FailError(t, err)

	reader := common_io.NewSyncReader(filesystem, readHandle)
	totalReadBytes := int64(0)

	readHasher := sha1.New()
	readBuffer := make([]byte, iRODSReadWriteSize)
	for totalReadBytes < totalWrittenBytes {
		read, readErr := reader.ReadAt(readBuffer, totalReadBytes)
		if readErr != nil && readErr != io.EOF {
			FailError(t, readErr)
			break
		}

		_, hashErr := readHasher.Write(readBuffer[:read])
		FailError(t, hashErr)
		if hashErr != nil {
			break
		}

		totalReadBytes += int64(read)

		if readErr == io.EOF {
			break
		}
	}

	reader.Release()

	err = readHandle.Close()
	FailError(t, err)

	readHashBytes := readHasher.Sum(nil)
	readHashString := hex.EncodeToString(readHashBytes)

	// compare
	assert.Equal(t, totalWrittenBytes, totalReadBytes)
	assert.Equal(t, writeHashString, readHashString)

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	FailError(t, err)

	assert.False(t, filesystem.ExistsFile(newDataObjectPath))
}

func asyncWriteRead(t *testing.T, size int64) {
	t.Logf("Testing size %d", size)

	test := GetCurrentTest()
	server := test.GetCurrentServer()
	serverInfo := server.GetInfo()
	account, err := serverInfo.GetAccount()
	FailError(t, err)

	fsConfig := fs.NewFileSystemConfig("irodsfs-common-test")

	filesystem, err := irods.NewIRODSFSClientDirect(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	newDataObjectFilename := "testobj_async_123"
	newDataObjectPath := homeDir + "/" + newDataObjectFilename

	// write
	writeHandle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	FailError(t, err)

	syncWriter := common_io.NewSyncWriter(filesystem, writeHandle)
	writer := common_io.NewAsyncWriter(syncWriter)

	toWrite := size
	totalWrittenBytes := int64(0)

	writeHasher := sha1.New()
	for totalWrittenBytes < toWrite {
		buf := MakeRandomContentDataBuf(16 * 1024)
		writeLen := toWrite - totalWrittenBytes
		if writeLen > int64(len(buf)) {
			writeLen = int64(len(buf))
		}

		written, writeErr := writer.WriteAt(buf[:writeLen], totalWrittenBytes)
		FailError(t, writeErr)
		if writeErr != nil {
			break
		}

		_, hashErr := writeHasher.Write(buf[:written])
		FailError(t, hashErr)
		if hashErr != nil {
			break
		}

		totalWrittenBytes += int64(written)
	}

	err = writer.Flush()
	FailError(t, err)

	writer.Release()

	err = writeHandle.Close()
	FailError(t, err)

	writeHashBytes := writeHasher.Sum(nil)
	writeHashString := hex.EncodeToString(writeHashBytes)

	// read
	readHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	FailError(t, err)

	syncReader := common_io.NewSyncReader(filesystem, readHandle)
	reader, err := common_io.NewAsyncReader([]common_io.Reader{syncReader}, iRODSIOBlockSize)
	FailError(t, err)

	totalReadBytes := int64(0)

	readHasher := sha1.New()
	readBuffer := make([]byte, iRODSReadWriteSize)
	for totalReadBytes < totalWrittenBytes {
		read, readErr := reader.ReadAt(readBuffer, totalReadBytes)
		if readErr != nil && readErr != io.EOF {
			FailError(t, readErr)
			break
		}

		_, hashErr := readHasher.Write(readBuffer[:read])
		FailError(t, hashErr)
		if hashErr != nil {
			break
		}

		totalReadBytes += int64(read)

		if readErr == io.EOF {
			break
		}
	}

	reader.Release()

	err = readHandle.Close()
	FailError(t, err)

	readHashBytes := readHasher.Sum(nil)
	readHashString := hex.EncodeToString(readHashBytes)

	// compare
	assert.Equal(t, totalWrittenBytes, totalReadBytes)
	assert.Equal(t, writeHashString, readHashString)

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	FailError(t, err)

	assert.False(t, filesystem.ExistsFile(newDataObjectPath))
}

func asyncWriteReadWithCache(t *testing.T, size int64) {
	t.Logf("Testing size %d", size)

	test := GetCurrentTest()
	server := test.GetCurrentServer()
	serverInfo := server.GetInfo()
	account, err := serverInfo.GetAccount()
	FailError(t, err)

	fsConfig := fs.NewFileSystemConfig("irodsfs-common-test")

	filesystem, err := irods.NewIRODSFSClientDirect(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	newDataObjectFilename := "testobj_async_123"
	newDataObjectPath := homeDir + "/" + newDataObjectFilename

	// write
	writeHandle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	FailError(t, err)

	syncWriter := common_io.NewSyncWriter(filesystem, writeHandle)
	writer := common_io.NewAsyncWriter(syncWriter)

	toWrite := size
	totalWrittenBytes := int64(0)

	writeHasher := sha1.New()
	for totalWrittenBytes < toWrite {
		buf := MakeRandomContentDataBuf(16 * 1024)
		writeLen := toWrite - totalWrittenBytes
		if writeLen > int64(len(buf)) {
			writeLen = int64(len(buf))
		}

		written, writeErr := writer.WriteAt(buf[:writeLen], totalWrittenBytes)
		FailError(t, writeErr)
		if writeErr != nil {
			break
		}

		_, hashErr := writeHasher.Write(buf[:written])
		FailError(t, hashErr)
		if hashErr != nil {
			break
		}

		totalWrittenBytes += int64(written)
	}

	err = writer.Flush()
	FailError(t, err)

	writer.Release()

	err = writeHandle.Close()
	FailError(t, err)

	writeHashBytes := writeHasher.Sum(nil)
	writeHashString := hex.EncodeToString(writeHashBytes)

	cacheStore, err := common_cache.NewDiskCacheStore(100*mb, int(mb), "/tmp/irodsfs-common")
	FailError(t, err)

	// read #1
	readHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	FailError(t, err)

	syncReader := common_io.NewSyncReader(filesystem, readHandle)
	reader, err := common_io.NewAsyncCacheThroughReader([]common_io.Reader{syncReader}, iRODSIOBlockSize, cacheStore)
	FailError(t, err)

	totalReadBytes := int64(0)

	readHasher := sha1.New()
	readBuffer := make([]byte, iRODSReadWriteSize)
	for totalReadBytes < totalWrittenBytes {
		//t.Logf("read at %d", totalReadBytes)
		read, readErr := reader.ReadAt(readBuffer, totalReadBytes)
		if readErr != nil && readErr != io.EOF {
			FailError(t, readErr)
			break
		}

		_, hashErr := readHasher.Write(readBuffer[:read])
		FailError(t, hashErr)
		if hashErr != nil {
			break
		}

		totalReadBytes += int64(read)

		if readErr == io.EOF {
			break
		}
	}

	reader.Release()

	err = readHandle.Close()
	FailError(t, err)

	readHashBytes := readHasher.Sum(nil)
	readHashString := hex.EncodeToString(readHashBytes)

	// compare
	assert.Equal(t, totalWrittenBytes, totalReadBytes)
	assert.Equal(t, writeHashString, readHashString)

	// read #2
	// read again. must hit cache
	readHandle, err = filesystem.OpenFile(newDataObjectPath, "", "r")
	FailError(t, err)

	syncReader = common_io.NewSyncReader(filesystem, readHandle)
	reader, err = common_io.NewAsyncCacheThroughReader([]common_io.Reader{syncReader}, iRODSIOBlockSize, cacheStore)
	FailError(t, err)

	totalReadBytes = int64(0)

	readHasher = sha1.New()
	for totalReadBytes < totalWrittenBytes {
		//t.Logf("read at %d", totalReadBytes)
		read, readErr := reader.ReadAt(readBuffer, totalReadBytes)
		if readErr != nil && readErr != io.EOF {
			FailError(t, readErr)
			break
		}

		_, hashErr := readHasher.Write(readBuffer[:read])
		FailError(t, hashErr)
		if hashErr != nil {
			break
		}

		totalReadBytes += int64(read)

		if readErr == io.EOF {
			break
		}
	}

	reader.Release()

	err = readHandle.Close()
	FailError(t, err)

	readHashBytes = readHasher.Sum(nil)
	readHashString = hex.EncodeToString(readHashBytes)

	// compare
	assert.Equal(t, totalWrittenBytes, totalReadBytes)
	assert.Equal(t, writeHashString, readHashString)

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	FailError(t, err)

	assert.False(t, filesystem.ExistsFile(newDataObjectPath))
}

func asyncWriteReadWithPrefetch(t *testing.T, size int64) {
	t.Logf("Testing size %d", size)

	test := GetCurrentTest()
	server := test.GetCurrentServer()
	serverInfo := server.GetInfo()
	account, err := serverInfo.GetAccount()
	FailError(t, err)

	fsConfig := fs.NewFileSystemConfig("irodsfs-common-test")

	filesystem, err := irods.NewIRODSFSClientDirect(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	newDataObjectFilename := "testobj_async_123"
	newDataObjectPath := homeDir + "/" + newDataObjectFilename

	// write
	writeHandle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	FailError(t, err)

	syncWriter := common_io.NewSyncWriter(filesystem, writeHandle)
	writer := common_io.NewAsyncWriter(syncWriter)

	toWrite := size
	totalWrittenBytes := int64(0)

	writeHasher := sha1.New()
	for totalWrittenBytes < toWrite {
		buf := MakeRandomContentDataBuf(16 * 1024)
		writeLen := toWrite - totalWrittenBytes
		if writeLen > int64(len(buf)) {
			writeLen = int64(len(buf))
		}

		written, writeErr := writer.WriteAt(buf[:writeLen], totalWrittenBytes)
		FailError(t, writeErr)
		if writeErr != nil {
			break
		}

		_, hashErr := writeHasher.Write(buf[:written])
		FailError(t, hashErr)
		if hashErr != nil {
			break
		}

		totalWrittenBytes += int64(written)
	}

	err = writer.Flush()
	FailError(t, err)

	writer.Release()

	err = writeHandle.Close()
	FailError(t, err)

	writeHashBytes := writeHasher.Sum(nil)
	writeHashString := hex.EncodeToString(writeHashBytes)

	cacheStore, err := common_cache.NewDiskCacheStore(100*mb, int(mb), "/tmp/irodsfs-common")
	FailError(t, err)

	// read #1
	readHandle1, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	FailError(t, err)

	readHandle2, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	FailError(t, err)

	syncReader1 := common_io.NewSyncReader(filesystem, readHandle1)
	syncReader2 := common_io.NewSyncReader(filesystem, readHandle2)
	reader, err := common_io.NewAsyncCacheThroughReader([]common_io.Reader{syncReader1, syncReader2}, iRODSIOBlockSize, cacheStore)
	FailError(t, err)
	totalReadBytes := int64(0)

	readHasher := sha1.New()
	readBuffer := make([]byte, iRODSReadWriteSize)
	for totalReadBytes < totalWrittenBytes {
		read, readErr := reader.ReadAt(readBuffer, totalReadBytes)
		if readErr != nil && readErr != io.EOF {
			FailError(t, readErr)
			break
		}

		_, hashErr := readHasher.Write(readBuffer[:read])
		FailError(t, hashErr)
		if hashErr != nil {
			break
		}

		totalReadBytes += int64(read)

		if readErr == io.EOF {
			break
		}
	}

	reader.Release()

	err = readHandle1.Close()
	FailError(t, err)
	err = readHandle2.Close()
	FailError(t, err)

	readHashBytes := readHasher.Sum(nil)
	readHashString := hex.EncodeToString(readHashBytes)

	// compare
	assert.Equal(t, totalWrittenBytes, totalReadBytes)
	assert.Equal(t, writeHashString, readHashString)

	// read #2
	// again. must hit cache
	readHandle1, err = filesystem.OpenFile(newDataObjectPath, "", "r")
	FailError(t, err)

	readHandle2, err = filesystem.OpenFile(newDataObjectPath, "", "r")
	FailError(t, err)

	syncReader1 = common_io.NewSyncReader(filesystem, readHandle1)
	syncReader2 = common_io.NewSyncReader(filesystem, readHandle2)
	reader, err = common_io.NewAsyncCacheThroughReader([]common_io.Reader{syncReader1, syncReader2}, iRODSIOBlockSize, cacheStore)
	FailError(t, err)
	totalReadBytes = int64(0)

	readHasher = sha1.New()
	for totalReadBytes < totalWrittenBytes {
		read, readErr := reader.ReadAt(readBuffer, totalReadBytes)
		if readErr != nil && readErr != io.EOF {
			FailError(t, readErr)
			break
		}

		_, hashErr := readHasher.Write(readBuffer[:read])
		FailError(t, hashErr)
		if hashErr != nil {
			break
		}

		totalReadBytes += int64(read)

		if readErr == io.EOF {
			break
		}
	}

	reader.Release()

	err = readHandle1.Close()
	FailError(t, err)
	err = readHandle2.Close()
	FailError(t, err)

	readHashBytes = readHasher.Sum(nil)
	readHashString = hex.EncodeToString(readHashBytes)

	// compare
	assert.Equal(t, totalWrittenBytes, totalReadBytes)
	assert.Equal(t, writeHashString, readHashString)

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	FailError(t, err)

	assert.False(t, filesystem.ExistsFile(newDataObjectPath))
}
