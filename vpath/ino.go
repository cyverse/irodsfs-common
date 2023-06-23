package vpath

// EntryIDManager is a struct that manages entry id.
type EntryIDManager struct {
	entries   map[int64]string // id to path
	currentID int64
}

// NewEntryIDManager creates a new INodeManager
func NewINodeManager(freeInodeStart int64) *EntryIDManager {
	return &EntryIDManager{
		entries:   map[int64]string{},
		currentID: freeInodeStart,
	}
}

// GetNextINode returns next id
func (manager *EntryIDManager) GetNextINode(path string) int64 {
	myID := manager.currentID
	manager.currentID--

	manager.entries[myID] = path
	return myID
}
