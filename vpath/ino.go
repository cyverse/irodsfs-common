package vpath

// INodeManager is a struct that manages inodes.
type INodeManager struct {
	entries      map[int64]string // id to path
	currentInode int64
}

// NewINodeManager creates a new INodeManager
func NewINodeManager(freeInodeStart int64) *INodeManager {
	return &INodeManager{
		entries:      map[int64]string{},
		currentInode: freeInodeStart,
	}
}

// GetNextINode returns next inode
func (manager *INodeManager) GetNextINode(path string) int64 {
	inodeID := manager.currentInode
	manager.currentInode++

	manager.entries[inodeID] = path
	return inodeID
}
