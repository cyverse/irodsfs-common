package writebuffer

import (
	"sort"
	"sync"
)

// WriteBufferConfig holds configuration for WriteBufferManager
type WriteBufferConfig struct {
	MaxTotalSize  int64 // Global max size across all buffers
	MaxBufferSize int64 // Max size per individual buffer
}

func NewDefaultWriteBufferConfig() *WriteBufferConfig {
	return &WriteBufferConfig{
		MaxTotalSize:  256 * 1024 * 1024, // 256MB global
		MaxBufferSize: 16 * 1024 * 1024,  // 16MB per buffer
	}
}

// WriteBufferFlushFunc is called when buffered data needs to be written out
type WriteBufferFlushFunc func(data []byte, offset int64) error

// WriteBufferManager manages multiple WriteBuffers with a shared global size limit
type WriteBufferManager struct {
	mutex     sync.Mutex
	config    *WriteBufferConfig
	buffers   []*WriteBuffer
	totalSize int64
}

// NewWriteBufferManager creates a new WriteBufferManager
func NewWriteBufferManager(config *WriteBufferConfig) *WriteBufferManager {
	if config == nil {
		config = NewDefaultWriteBufferConfig()
	}
	return &WriteBufferManager{
		config: config,
	}
}

// GetConfig returns the config
func (m *WriteBufferManager) GetConfig() *WriteBufferConfig {
	return m.config
}

// GetTotalSize returns the total buffered size across all buffers
func (m *WriteBufferManager) GetTotalSize() int64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.totalSize
}

// GetAvailableSize returns remaining global capacity
func (m *WriteBufferManager) GetAvailableSize() int64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.config.MaxTotalSize - m.totalSize
}

// CreateBuffer creates a new WriteBuffer managed by this manager
func (m *WriteBufferManager) CreateBuffer(flushFunc WriteBufferFlushFunc) *WriteBuffer {
	wb := &WriteBuffer{
		manager:   m,
		writes:    make(map[int64][]byte),
		maxSize:   m.config.MaxBufferSize,
		flushFunc: flushFunc,
	}

	m.mutex.Lock()
	m.buffers = append(m.buffers, wb)
	m.mutex.Unlock()

	return wb
}

// removeBuffer removes a buffer from the manager
func (m *WriteBufferManager) removeBuffer(wb *WriteBuffer) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for i, b := range m.buffers {
		if b == wb {
			m.buffers = append(m.buffers[:i], m.buffers[i+1:]...)
			break
		}
	}
}

// addSize adds to the global total. Returns false if it would exceed the limit.
func (m *WriteBufferManager) addSize(size int64) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.totalSize+size > m.config.MaxTotalSize {
		return false
	}
	m.totalSize += size
	return true
}

// subtractSize subtracts from the global total
func (m *WriteBufferManager) subtractSize(size int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.totalSize -= size
	if m.totalSize < 0 {
		m.totalSize = 0
	}
}

// WriteBuffer accumulates small writes in memory and spills when the per-buffer
// or global size limit is exceeded.
type WriteBuffer struct {
	mutex       sync.Mutex
	manager     *WriteBufferManager
	writes      map[int64][]byte // offset -> data
	currentSize int64
	maxSize     int64
	flushFunc   WriteBufferFlushFunc
}

// WriteAt buffers data at the given offset. Spills if per-buffer or global limit is exceeded.
func (wb *WriteBuffer) WriteAt(data []byte, offset int64) (int, error) {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	dataSize := int64(len(data))

	// If this single write exceeds per-buffer max, flush existing + write directly
	if dataSize >= wb.maxSize {
		if err := wb.flushLocked(); err != nil {
			return 0, err
		}
		if err := wb.flushFunc(data, offset); err != nil {
			return 0, err
		}
		return len(data), nil
	}

	// If adding this write would exceed per-buffer limit, flush first
	if wb.currentSize+dataSize > wb.maxSize {
		if err := wb.flushLocked(); err != nil {
			return 0, err
		}
	}

	// If adding this write would exceed global limit, flush first
	if !wb.manager.addSize(dataSize) {
		if err := wb.flushLocked(); err != nil {
			return 0, err
		}
		// Try again after flush
		if !wb.manager.addSize(dataSize) {
			// Still no room - write directly
			if err := wb.flushFunc(data, offset); err != nil {
				return 0, err
			}
			return len(data), nil
		}
	}

	// Buffer the write (copy data to avoid external mutation)
	existing, exists := wb.writes[offset]
	if exists {
		wb.currentSize -= int64(len(existing))
		wb.manager.subtractSize(int64(len(existing)))
	}

	buf := make([]byte, len(data))
	copy(buf, data)
	wb.writes[offset] = buf
	wb.currentSize += dataSize

	return len(data), nil
}

// Flush writes all buffered data to the underlying writer, ordered by offset
func (wb *WriteBuffer) Flush() error {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()
	return wb.flushLocked()
}

// Close flushes remaining data and removes this buffer from the manager
func (wb *WriteBuffer) Close() error {
	wb.mutex.Lock()
	err := wb.flushLocked()
	wb.mutex.Unlock()

	wb.manager.removeBuffer(wb)
	return err
}

// flushLocked flushes all buffered writes (caller must hold mutex)
func (wb *WriteBuffer) flushLocked() error {
	if len(wb.writes) == 0 {
		return nil
	}

	// Sort offsets for sequential write order
	offsets := make([]int64, 0, len(wb.writes))
	for off := range wb.writes {
		offsets = append(offsets, off)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })

	for _, off := range offsets {
		if err := wb.flushFunc(wb.writes[off], off); err != nil {
			return err
		}
	}

	wb.manager.subtractSize(wb.currentSize)
	wb.writes = make(map[int64][]byte)
	wb.currentSize = 0
	return nil
}

// GetBufferedSize returns the current buffered size for this buffer
func (wb *WriteBuffer) GetBufferedSize() int64 {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()
	return wb.currentSize
}

// HasBufferedData returns true if there is unflushed data
func (wb *WriteBuffer) HasBufferedData() bool {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()
	return len(wb.writes) > 0
}
