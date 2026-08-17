package writebuffer

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteBufferBasicWrite(t *testing.T) {
	mgr := NewWriteBufferManager(nil)

	var flushed []struct {
		data   []byte
		offset int64
	}

	wb := mgr.CreateBuffer(func(data []byte, offset int64) error {
		flushed = append(flushed, struct {
			data   []byte
			offset int64
		}{append([]byte(nil), data...), offset})
		return nil
	})
	defer wb.Close()

	n, err := wb.WriteAt([]byte("hello"), 0)
	assert.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, int64(5), wb.GetBufferedSize())
	assert.True(t, wb.HasBufferedData())
	assert.Empty(t, flushed)
}

func TestWriteBufferFlush(t *testing.T) {
	mgr := NewWriteBufferManager(nil)

	var flushed []struct {
		data   []byte
		offset int64
	}

	wb := mgr.CreateBuffer(func(data []byte, offset int64) error {
		flushed = append(flushed, struct {
			data   []byte
			offset int64
		}{append([]byte(nil), data...), offset})
		return nil
	})
	defer wb.Close()

	wb.WriteAt([]byte("aaa"), 0)
	wb.WriteAt([]byte("bbb"), 100)
	wb.WriteAt([]byte("ccc"), 50)

	err := wb.Flush()
	assert.NoError(t, err)
	assert.False(t, wb.HasBufferedData())
	assert.Equal(t, int64(0), wb.GetBufferedSize())

	// Verify flush order is sorted by offset
	assert.Len(t, flushed, 3)
	assert.Equal(t, int64(0), flushed[0].offset)
	assert.Equal(t, []byte("aaa"), flushed[0].data)
	assert.Equal(t, int64(50), flushed[1].offset)
	assert.Equal(t, []byte("ccc"), flushed[1].data)
	assert.Equal(t, int64(100), flushed[2].offset)
	assert.Equal(t, []byte("bbb"), flushed[2].data)
}

func TestWriteBufferClose(t *testing.T) {
	mgr := NewWriteBufferManager(nil)

	var flushed int
	wb := mgr.CreateBuffer(func(data []byte, offset int64) error {
		flushed++
		return nil
	})

	wb.WriteAt([]byte("data"), 0)
	err := wb.Close()
	assert.NoError(t, err)
	assert.Equal(t, 1, flushed)
}

func TestWriteBufferPerBufferLimit(t *testing.T) {
	config := &WriteBufferConfig{
		MaxTotalSize:  1024 * 1024,
		MaxBufferSize: 100,
	}
	mgr := NewWriteBufferManager(config)

	var flushCount int
	wb := mgr.CreateBuffer(func(data []byte, offset int64) error {
		flushCount++
		return nil
	})
	defer wb.Close()

	// Write 60 bytes
	wb.WriteAt(make([]byte, 60), 0)
	assert.Equal(t, 0, flushCount)

	// Write 60 more — exceeds per-buffer limit (100), triggers flush
	wb.WriteAt(make([]byte, 60), 60)
	assert.Equal(t, 1, flushCount)
}

func TestWriteBufferLargeWriteBypassesBuffer(t *testing.T) {
	config := &WriteBufferConfig{
		MaxTotalSize:  1024 * 1024,
		MaxBufferSize: 100,
	}
	mgr := NewWriteBufferManager(config)

	var flushed []struct {
		size   int
		offset int64
	}
	wb := mgr.CreateBuffer(func(data []byte, offset int64) error {
		flushed = append(flushed, struct {
			size   int
			offset int64
		}{len(data), offset})
		return nil
	})
	defer wb.Close()

	// Buffer some small data first
	wb.WriteAt([]byte("small"), 0)

	// Write >= maxSize: flushes existing + writes directly
	bigData := make([]byte, 100)
	n, err := wb.WriteAt(bigData, 200)
	assert.NoError(t, err)
	assert.Equal(t, 100, n)

	// First flush is the buffered "small", second is the big direct write
	assert.Len(t, flushed, 2)
	assert.Equal(t, 5, flushed[0].size)
	assert.Equal(t, int64(0), flushed[0].offset)
	assert.Equal(t, 100, flushed[1].size)
	assert.Equal(t, int64(200), flushed[1].offset)
}

func TestWriteBufferGlobalLimit(t *testing.T) {
	config := &WriteBufferConfig{
		MaxTotalSize:  100,
		MaxBufferSize: 200,
	}
	mgr := NewWriteBufferManager(config)

	var flushCount int
	wb := mgr.CreateBuffer(func(data []byte, offset int64) error {
		flushCount++
		return nil
	})
	defer wb.Close()

	// Fill global capacity
	wb.WriteAt(make([]byte, 80), 0)
	assert.Equal(t, 0, flushCount)

	// Exceeds global limit — triggers flush
	wb.WriteAt(make([]byte, 30), 80)
	assert.Equal(t, 1, flushCount)
}

func TestWriteBufferOverwriteSameOffset(t *testing.T) {
	mgr := NewWriteBufferManager(nil)

	var flushed [][]byte
	wb := mgr.CreateBuffer(func(data []byte, offset int64) error {
		flushed = append(flushed, append([]byte(nil), data...))
		return nil
	})
	defer wb.Close()

	wb.WriteAt([]byte("first"), 0)
	wb.WriteAt([]byte("second"), 0)

	assert.Equal(t, int64(6), wb.GetBufferedSize())

	wb.Flush()
	assert.Len(t, flushed, 1)
	assert.Equal(t, []byte("second"), flushed[0])
}

func TestWriteBufferDataIsolation(t *testing.T) {
	mgr := NewWriteBufferManager(nil)

	var flushed []byte
	wb := mgr.CreateBuffer(func(data []byte, offset int64) error {
		flushed = append([]byte(nil), data...)
		return nil
	})
	defer wb.Close()

	data := []byte("original")
	wb.WriteAt(data, 0)

	// Mutate source data — should not affect buffered data
	data[0] = 'X'

	wb.Flush()
	assert.Equal(t, []byte("original"), flushed)
}

func TestWriteBufferMultipleBuffers(t *testing.T) {
	config := &WriteBufferConfig{
		MaxTotalSize:  100,
		MaxBufferSize: 100,
	}
	mgr := NewWriteBufferManager(config)

	wb1 := mgr.CreateBuffer(func(data []byte, offset int64) error { return nil })
	wb2 := mgr.CreateBuffer(func(data []byte, offset int64) error { return nil })

	wb1.WriteAt(make([]byte, 40), 0)
	wb2.WriteAt(make([]byte, 40), 0)

	assert.Equal(t, int64(80), mgr.GetTotalSize())
	assert.Equal(t, int64(20), mgr.GetAvailableSize())

	wb1.Close()
	assert.Equal(t, int64(40), mgr.GetTotalSize())

	wb2.Close()
	assert.Equal(t, int64(0), mgr.GetTotalSize())
}

func TestWriteBufferFlushError(t *testing.T) {
	mgr := NewWriteBufferManager(nil)

	expectedErr := fmt.Errorf("write failed")
	wb := mgr.CreateBuffer(func(data []byte, offset int64) error {
		return expectedErr
	})
	defer wb.Close()

	wb.WriteAt([]byte("data"), 0)

	err := wb.Flush()
	assert.ErrorIs(t, err, expectedErr)
}

func TestWriteBufferConcurrentWrites(t *testing.T) {
	config := &WriteBufferConfig{
		MaxTotalSize:  1024 * 1024,
		MaxBufferSize: 1024 * 1024,
	}
	mgr := NewWriteBufferManager(config)

	var mu sync.Mutex
	var flushCount int
	wb := mgr.CreateBuffer(func(data []byte, offset int64) error {
		mu.Lock()
		flushCount++
		mu.Unlock()
		return nil
	})
	defer wb.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wb.WriteAt([]byte("data"), int64(i*10))
		}(i)
	}
	wg.Wait()

	assert.True(t, wb.HasBufferedData())

	err := wb.Flush()
	assert.NoError(t, err)
	assert.False(t, wb.HasBufferedData())
}

func TestWriteBufferEmptyFlush(t *testing.T) {
	mgr := NewWriteBufferManager(nil)

	var flushCount int
	wb := mgr.CreateBuffer(func(data []byte, offset int64) error {
		flushCount++
		return nil
	})
	defer wb.Close()

	err := wb.Flush()
	assert.NoError(t, err)
	assert.Equal(t, 0, flushCount)
}
