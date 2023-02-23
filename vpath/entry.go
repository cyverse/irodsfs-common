package vpath

import (
	"fmt"
	"strings"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/irodsfs-common/utils"
	"golang.org/x/xerrors"
)

// VPathEntryType determins if the vpath entry is an actual iRODS entry (irods) or a virtual directory entry (virtual).
// The virtual directory entries are read-only directories containing irods or other virtual entries in it.
type VPathEntryType string

const (
	// VPathVirtualDir is an entry type for virtual directory entry
	VPathVirtualDir VPathEntryType = "virtual"
	// VPathIRODS is an entry type for irods entry
	VPathIRODS VPathEntryType = "irods"
)

// VPathVirtualDirEntry is a virtual directory entry struct
type VPathVirtualDirEntry struct {
	ID         int64
	Name       string
	Path       string
	Owner      string
	Size       int64
	CreateTime time.Time
	ModifyTime time.Time
	DirEntries []*VPathEntry
}

// VPathEntry is a virtual path entry struct
type VPathEntry struct {
	Type     VPathEntryType
	Path     string
	ReadOnly bool

	// Only one of fields below is filled according to the Type
	VirtualDirEntry *VPathVirtualDirEntry
	IRODSEntry      *irodsclient_fs.Entry
}

// NewVPathEntryFromIRODSFSEntry creates a new VPathEntry from IRODSEntry
func NewVPathEntryFromIRODSFSEntry(path string, irodsEntry *irodsclient_fs.Entry, readonly bool) *VPathEntry {
	return &VPathEntry{
		Type:            VPathIRODS,
		Path:            path,
		ReadOnly:        readonly,
		VirtualDirEntry: nil,
		IRODSEntry:      irodsEntry,
	}
}

// ToString stringifies the object
func (entry *VPathEntry) ToString() string {
	return fmt.Sprintf("<VPathEntry %s %s %t %p %p>", entry.Type, entry.Path, entry.ReadOnly, entry.VirtualDirEntry, entry.IRODSEntry)
}

// GetIRODSPath returns an iRODS path for the given vpath
func (entry *VPathEntry) GetIRODSPath(vpath string) (string, error) {
	if entry.Type != VPathIRODS {
		err := xerrors.Errorf("failed to compute IRODS Path because entry type is not iRODS")
		return "", err
	}

	relPath, err := utils.GetRelativePath(entry.Path, vpath)
	if err != nil {
		return "", xerrors.Errorf("failed to compute relative path: %w", err)
	}

	if strings.HasPrefix(relPath, "../") {
		return "", xerrors.Errorf("failed to compute relative path from %s to %s", entry.Path, vpath)
	}

	if relPath == "." {
		return entry.IRODSEntry.Path, nil
	}

	return utils.JoinPath(entry.IRODSEntry.Path, relPath), nil
}
