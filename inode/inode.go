package inode

import "sync"

const (
	vpathEntryIDStart   = uint64(9000000000000000000)
	overlayEntryIDStart = uint64(9000100000000000000)
	irodsEntryIDStart   = uint64(1000000000000000000)

	// ID range limits for safety checks
	overlayEntryIDEnd = uint64(18446744073709551615) // Max uint64
)

// InodeManager is a struct that manages inode.
type InodeManager struct {
	currentVPathEntryIDInc   uint64
	currentOverlayEntryIDInc uint64
	vpathEntryIDMap          map[string]uint64
	overlayEntryIDMap        map[string]uint64
	mutex                    sync.RWMutex
}

// NewInodeManager creates a new InodeManager
func NewInodeManager() *InodeManager {
	return &InodeManager{
		currentVPathEntryIDInc:   0,
		currentOverlayEntryIDInc: 0,
		vpathEntryIDMap:          map[string]uint64{},
		overlayEntryIDMap:        map[string]uint64{},
		mutex:                    sync.RWMutex{},
	}
}

// GetInodeIDForIRODSEntryID returns inode id for iRODS entry id
func (manager *InodeManager) GetInodeIDForIRODSEntryID(entryID int64) uint64 {
	return irodsEntryIDStart + uint64(entryID)
}

// GetInodeIDForVPathEntryID returns inode id for vpath entry id
func (manager *InodeManager) GetInodeIDForVPathEntryID(entryID uint64) uint64 {
	// the same
	return entryID
}

// GetInodeIDForVPathEntry returns inode id for vpath entry path
func (manager *InodeManager) GetInodeIDForVPathEntry(vpath string) uint64 {
	// First try with read lock
	manager.mutex.RLock()
	if id, ok := manager.vpathEntryIDMap[vpath]; ok {
		manager.mutex.RUnlock()
		return id
	}
	manager.mutex.RUnlock()

	// Need to create new entry, use write lock
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	// Double-check after acquiring write lock (another goroutine might have created it)
	if id, ok := manager.vpathEntryIDMap[vpath]; ok {
		return id
	}

	// Create a new and save for reuse
	id := vpathEntryIDStart + manager.currentVPathEntryIDInc
	manager.currentVPathEntryIDInc++
	manager.vpathEntryIDMap[vpath] = id
	return id
}

// GetInodeIDForOverlayEntry returns inode id for overlay entry path
func (manager *InodeManager) GetInodeIDForOverlayEntry(irodsPath string) uint64 {
	// First try with read lock
	manager.mutex.RLock()
	if id, ok := manager.overlayEntryIDMap[irodsPath]; ok {
		manager.mutex.RUnlock()
		return id
	}
	manager.mutex.RUnlock()

	// Need to create new entry, use write lock
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	// Double-check after acquiring write lock (another goroutine might have created it)
	if id, ok := manager.overlayEntryIDMap[irodsPath]; ok {
		return id
	}

	// Create a new and save for reuse
	id := overlayEntryIDStart + manager.currentOverlayEntryIDInc
	manager.currentOverlayEntryIDInc++
	manager.overlayEntryIDMap[irodsPath] = id
	return id
}

// IsVPathEntryID checks if the given inode ID belongs to vpath entries
func IsVPathEntryID(inodeID uint64) bool {
	return inodeID >= vpathEntryIDStart && inodeID < overlayEntryIDStart
}

// IsOverlayEntryID checks if the given inode ID belongs to overlay entries
func IsOverlayEntryID(inodeID uint64) bool {
	return inodeID >= overlayEntryIDStart && inodeID <= overlayEntryIDEnd
}

// IsIRODSEntryID checks if the given inode ID belongs to iRODS entries
func IsIRODSEntryID(inodeID uint64) bool {
	return inodeID >= irodsEntryIDStart && inodeID < vpathEntryIDStart
}
