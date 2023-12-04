package inode

import "sync"

const (
	vpathEntryIDStart   = uint64(9000000000000000000)
	overlayEntryIDStart = uint64(9000100000000000000)
	irodsEntryIDStart   = uint64(1000000000000000000)
)

// InodeManager is a struct that manages inode.
type InodeManager struct {
	currentVPathEntryIDInc   uint64
	currentOverlayEntryIDInc uint64
	vpathEntryIDMap          map[string]uint64
	overlayEntryIDMap        map[string]uint64
	mutex                    sync.Mutex
}

// NewInodeManager creates a new InodeManager
func NewInodeManager() *InodeManager {
	return &InodeManager{
		currentVPathEntryIDInc: 0,
		vpathEntryIDMap:        map[string]uint64{},
		mutex:                  sync.Mutex{},
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
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if id, ok := manager.vpathEntryIDMap[vpath]; ok {
		return id
	}

	// not exist
	// create a new and save for reuse
	// need to return the same id later
	id := vpathEntryIDStart + manager.currentVPathEntryIDInc
	manager.currentVPathEntryIDInc++
	manager.vpathEntryIDMap[vpath] = id
	return id
}

// GetInodeIDForOverlayEntry returns inode id for overlay entry path
func (manager *InodeManager) GetInodeIDForOverlayEntry(irodsPath string) uint64 {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if id, ok := manager.overlayEntryIDMap[irodsPath]; ok {
		return id
	}

	// not exist
	// create a new and save for reuse
	// need to return the same id later
	id := overlayEntryIDStart + manager.currentOverlayEntryIDInc
	manager.currentOverlayEntryIDInc++
	manager.overlayEntryIDMap[irodsPath] = id
	return id
}
