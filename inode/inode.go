package inode

import (
	"math"
	"sync"

	"github.com/cockroachdb/errors"
)

const (
	// InodeIDInvalid represents an invalid inode ID (inode 0 is never valid in FUSE)
	InodeIDInvalid = uint64(0)
	// InodeIDRoot represents the root inode (inode 1 is reserved for the root directory in FUSE)
	InodeIDRoot = uint64(1)

	irodsEntryIDStart   = uint64(1000000000000000000)
	virtualEntryIDStart = uint64(7000000000000000000)

	// Capped at max int64 to ensure safe uint64 -> int64 conversion
	virtualEntryIDEnd = uint64(math.MaxInt64)
)

// InodeManager is a struct that manages inode.
type InodeManager struct {
	currentVirtualEntryIDInc uint64
	virtualEntryIDMap        map[string]uint64
	reverseVirtualEntryIDMap map[uint64]string
	mutex                    sync.RWMutex
}

// NewInodeManager creates a new InodeManager
func NewInodeManager() *InodeManager {
	return &InodeManager{
		currentVirtualEntryIDInc: 0,
		virtualEntryIDMap:        map[string]uint64{},
		reverseVirtualEntryIDMap: map[uint64]string{},
		mutex:                    sync.RWMutex{},
	}
}

// GetInodeIDForIRODSEntryID returns inode ID for iRODS entry ID
func (manager *InodeManager) GetInodeIDForIRODSEntryID(entryID uint64) (uint64, error) {
	id := irodsEntryIDStart + entryID
	if id < irodsEntryIDStart || id >= virtualEntryIDStart {
		return 0, errors.Newf("iRODS inode ID overflow: entryID %d exceeds available range", entryID)
	}
	return id, nil
}

// GetIRODSEntryIDForInodeID returns the original iRODS entry ID from an inode ID
func GetIRODSEntryIDForInodeID(inodeID uint64) (uint64, bool) {
	if !IsIRODSEntryID(inodeID) {
		return 0, false
	}
	return inodeID - irodsEntryIDStart, true
}

// GetInodeIDForVirtualEntry returns inode id for a virtual entry (vpath or locally buffered).
func (manager *InodeManager) GetInodeIDForVirtualEntry(path string) (uint64, error) {
	// First try with read lock
	manager.mutex.RLock()
	if id, ok := manager.virtualEntryIDMap[path]; ok {
		manager.mutex.RUnlock()
		return id, nil
	}
	manager.mutex.RUnlock()

	// Need to create new entry, use write lock
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	// Double-check after acquiring write lock (another goroutine might have created it)
	if id, ok := manager.virtualEntryIDMap[path]; ok {
		return id, nil
	}

	// Create a new and save for reuse
	id := virtualEntryIDStart + manager.currentVirtualEntryIDInc
	if id < virtualEntryIDStart || id > virtualEntryIDEnd {
		return 0, errors.Newf("virtual inode ID overflow: no more IDs available for path %q", path)
	}
	manager.currentVirtualEntryIDInc++
	manager.virtualEntryIDMap[path] = id
	manager.reverseVirtualEntryIDMap[id] = path
	return id, nil
}

// GetPathForVirtualEntryID returns the path for a given virtual inode ID.
func (manager *InodeManager) GetPathForVirtualEntryID(inodeID uint64) (string, bool) {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()
	path, ok := manager.reverseVirtualEntryIDMap[inodeID]
	return path, ok
}

// RemoveVirtualEntry removes a virtual entry from the map after it receives an iRODS ID.
func (manager *InodeManager) RemoveVirtualEntry(path string) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if id, ok := manager.virtualEntryIDMap[path]; ok {
		delete(manager.reverseVirtualEntryIDMap, id)
		delete(manager.virtualEntryIDMap, path)
	}
}

// IsValidInodeID checks if the given inode ID falls within a known range
func IsValidInodeID(inodeID uint64) bool {
	if inodeID == InodeIDRoot {
		return true
	}
	return IsIRODSEntryID(inodeID) || IsVirtualEntryID(inodeID)
}

// IsVirtualEntryID checks if the given inode ID belongs to virtual entries
func IsVirtualEntryID(inodeID uint64) bool {
	return inodeID >= virtualEntryIDStart && inodeID <= virtualEntryIDEnd
}

// IsIRODSEntryID checks if the given inode ID belongs to iRODS entries
func IsIRODSEntryID(inodeID uint64) bool {
	return inodeID >= irodsEntryIDStart && inodeID < virtualEntryIDStart
}
