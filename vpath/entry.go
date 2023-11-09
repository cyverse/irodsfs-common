package vpath

import (
	"fmt"
	"strings"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/irods"
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
	Type      VPathEntryType
	Path      string
	IRODSPath string // maybe empty if type is VPathVirtualDir
	ReadOnly  bool

	// Only one of fields below is filled according to the Type
	// both fields may have nil when iRODS entry is not retrieved successfully due to iRODS fail
	VirtualDirEntry *VPathVirtualDirEntry
	IRODSEntry      *irodsclient_fs.Entry
}

// NewVPathEntryFromIRODSFSEntry creates a new VPathEntry from IRODSEntry
func NewVPathEntryFromIRODSFSEntry(path string, irodsPath string, irodsEntry *irodsclient_fs.Entry, readonly bool) *VPathEntry {
	return &VPathEntry{
		Type:            VPathIRODS,
		Path:            path,
		IRODSPath:       irodsPath,
		ReadOnly:        readonly,
		VirtualDirEntry: nil,
		IRODSEntry:      irodsEntry,
	}
}

// ToString stringifies the object
func (entry *VPathEntry) ToString() string {
	return fmt.Sprintf("<VPathEntry %s %s %s %t %p %p>", entry.Type, entry.Path, entry.IRODSPath, entry.ReadOnly, entry.VirtualDirEntry, entry.IRODSEntry)
}

// IsIRODSEntry returns true if the entry is for iRODS entry
func (entry *VPathEntry) IsIRODSEntry() bool {
	return entry.Type == VPathIRODS
}

// IsVirtualDirEntry returns true if the entry is for virtual dir
func (entry *VPathEntry) IsVirtualDirEntry() bool {
	return entry.Type == VPathVirtualDir
}

// RequireIRODSEntryUpdate returns true if it requires to update IRODSEntry field
func (entry *VPathEntry) RequireIRODSEntryUpdate() bool {
	if entry.Type == VPathIRODS {
		if entry.IRODSEntry == nil {
			return true
		}
	}

	return false
}

// UpdateIRODSEntry updates IRODSEntry field
func (entry *VPathEntry) UpdateIRODSEntry(fsClient irods.IRODSFSClient) error {
	if entry.Type == VPathIRODS {
		irodsEntry, err := fsClient.Stat(entry.IRODSPath)
		if err != nil {
			if irodsclient_types.IsFileNotFoundError(err) {
				return xerrors.Errorf("failed to find path %s: %w", entry.IRODSPath, err)
			}

			return xerrors.Errorf("failed to update IRODSEntry for path %s: %w", entry.IRODSPath, err)
		}

		entry.IRODSEntry = irodsEntry
		return nil
	}

	// do nothing
	return nil
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
		return entry.IRODSPath, nil
	}

	return utils.JoinPath(entry.IRODSPath, relPath), nil
}

// StatIRODSEntry returns an iRODS stat for the given vpath
func (entry *VPathEntry) StatIRODSEntry(fsClient irods.IRODSFSClient, vpath string) (string, *irodsclient_fs.Entry, error) {
	irodsPath, err := entry.GetIRODSPath(vpath)
	if err != nil {
		return "", nil, xerrors.Errorf("failed to stat iRODS entry for vpath %s: %w", vpath, err)
	}

	irodsEntry, err := fsClient.Stat(irodsPath)
	return irodsPath, irodsEntry, err
}
