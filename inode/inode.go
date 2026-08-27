package inode

import (
	"math"

	"github.com/cockroachdb/errors"
)

const (
	// InodeIDInvalid represents an invalid inode ID (inode 0 is never valid in FUSE)
	InodeIDInvalid = uint64(0)

	IrodsEntryIDStart   = uint64(1000000000000000000)
	VirtualEntryIDStart = uint64(7000000000000000000)
	StagingEntryIDStart = uint64(8000000000000000000)

	// Capped at max int64 to ensure safe uint64 -> int64 conversion
	StagingEntryIDEnd = uint64(math.MaxInt64)
)

// GetInodeIDForIRODSEntryID returns inode ID for iRODS entry ID
func GetInodeIDForIRODSEntryID(entryID uint64) (uint64, error) {
	id := IrodsEntryIDStart + entryID
	if !IsIRODSEntryID(id) {
		return 0, errors.Newf("iRODS inode ID overflow: entryID %d exceeds available range", entryID)
	}
	return id, nil
}

// IsValidInodeID checks if the given inode ID falls within a known range
func IsValidInodeID(inodeID uint64) bool {
	return inodeID >= IrodsEntryIDStart && inodeID <= StagingEntryIDEnd
}

// IsIRODSEntryID checks if the given inode ID belongs to iRODS entries
func IsIRODSEntryID(inodeID uint64) bool {
	return inodeID >= IrodsEntryIDStart && inodeID < VirtualEntryIDStart
}

// IsVirtualEntryID checks if the given inode ID belongs to virtual entries
func IsVirtualEntryID(inodeID uint64) bool {
	return inodeID >= VirtualEntryIDStart && inodeID < StagingEntryIDStart
}

// IsVirtualEntryID checks if the given inode ID belongs to virtual entries
func IsStagingEntryID(inodeID uint64) bool {
	return inodeID >= StagingEntryIDStart && inodeID <= StagingEntryIDEnd
}
