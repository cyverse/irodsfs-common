package vpath

import (
	"time"

	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-common/inode"
	"github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

// VPathManager is a struct that manages virtual paths.
type VPathManager struct {
	inodeManager *inode.InodeManager
	// path mappings given by user
	pathMappings []VPathMapping
	// entries is a map holding vpath entries.
	// Key is a vpath, value is an entry
	entries  map[string]*VPathEntry
	fsClient irods.IRODSFSClient
}

// NewVPathManager creates a new VPathManager
func NewVPathManager(fsClient irods.IRODSFSClient, inodeManager *inode.InodeManager, pathMappings []VPathMapping) (*VPathManager, error) {
	logger := log.WithFields(log.Fields{
		"package":  "vpath",
		"function": "NewVPathManager",
	})

	manager := &VPathManager{
		inodeManager: inodeManager,
		pathMappings: pathMappings,
		entries:      map[string]*VPathEntry{},
		fsClient:     fsClient,
	}

	logger.Info("Building a hierarchy")
	err := manager.build()
	if err != nil {
		buildErr := xerrors.Errorf("failed to build a hierarchy: %w", err)
		logger.Errorf("%+v", buildErr)
		return nil, buildErr
	}

	return manager, nil
}

// build builds VPaths from mappings
func (manager *VPathManager) build() error {
	manager.entries = map[string]*VPathEntry{}

	// build
	for _, mapping := range manager.pathMappings {
		err := manager.buildOne(&mapping)
		if err != nil {
			return xerrors.Errorf("failed to build vpath mapping: %w", err)
		}
	}
	return nil
}

// HasEntry returns true if it has VFS Entry for the path
func (manager *VPathManager) HasEntry(vpath string) bool {
	_, ok := manager.entries[vpath]
	return ok
}

// GetEntry returns VFS Entry for the Path
func (manager *VPathManager) GetEntry(vpath string) *VPathEntry {
	if entry, ok := manager.entries[vpath]; ok {
		return entry
	}

	return nil
}

// GetClosestEntry returns the closest VFS Entry for the path
// if an entry for the given vpath exists, returns it
// if not exists, finds a parent dir entry that exists
func (manager *VPathManager) GetClosestEntry(vpath string) *VPathEntry {
	// if there's an exact match
	// returns it
	entry := manager.GetEntry(vpath)
	if entry != nil {
		return entry
	}

	// get all parent dirs of the given vpath and check if it exists
	parentDirs := utils.GetParentDirs(vpath)
	var closestEntry *VPathEntry
	for _, parentDir := range parentDirs {
		if entry, ok := manager.entries[parentDir]; ok {
			closestEntry = entry
		} else {
			// not exists?
			// stop - it is clear that subdirs of the parentDir do not exist
			break
		}
	}

	return closestEntry
}

// buildOne builds one VFS mapping
func (manager *VPathManager) buildOne(mapping *VPathMapping) error {
	logger := log.WithFields(log.Fields{
		"package":  "vpath",
		"struct":   "VPathManager",
		"function": "buildOne",
	})

	logger.Infof("Building a VPath Entry - %s", mapping.IRODSPath)

	now := time.Now()

	parentDirs := utils.GetParentDirs(mapping.MappingPath)
	for idx, parentDir := range parentDirs {
		// add parentDir if not exists
		if parentDirEntry, ok := manager.entries[parentDir]; ok {
			// exists, check if it is VPathVirtualDir
			if parentDirEntry.Type != VPathVirtualDir {
				// already exists
				// can't create a virtual dir entry under an irods entry
				return xerrors.Errorf("failed to create a virtual dir entry %s, entry already exists", parentDir)
			}
		} else {
			inodeID := manager.inodeManager.GetInodeIDForVPathEntry(parentDir)
			dirEntry := &VPathEntry{
				Type:     VPathVirtualDir,
				Path:     parentDir,
				ReadOnly: true,
				VirtualDirEntry: &VPathVirtualDirEntry{
					ID:         inodeID,
					Name:       utils.GetFileName(parentDir),
					Path:       parentDir,
					Owner:      manager.fsClient.GetAccount().ClientUser,
					Size:       0,
					CreateTime: now,
					ModifyTime: now,
					DirEntries: []*VPathEntry{}, // emptry directory for now
				},
				IRODSEntry: nil,
			}
			manager.entries[parentDir] = dirEntry

			// add entry to its parent dir's dir entry list
			if idx != 0 {
				parentPath := parentDirs[idx-1]
				if parentEntry, ok := manager.entries[parentPath]; ok {
					parentEntry.VirtualDirEntry.DirEntries = append(parentEntry.VirtualDirEntry.DirEntries, dirEntry)
				}
			}
		}
	}

	pathExist := false
	errored := false
	makeDir := false

	logger.Debugf("Checking path - %s", mapping.IRODSPath)
	irodsEntry, err := manager.fsClient.Stat(mapping.IRODSPath)
	if err != nil {
		if irodsclient_types.IsFileNotFoundError(err) {
			if mapping.ResourceType == VPathMappingDirectory {
				// dir not found
				if mapping.CreateDir {
					// create dir
					makeDir = true
					// fall below
				} else {
					if mapping.IgnoreNotExistError {
						// skip
						logger.WithError(err).Debugf("ignoring non-existing dir %s for mounting", mapping.IRODSPath)
						return nil
					}

					logger.WithError(err).Errorf("failed to find dir %s for mounting", mapping.IRODSPath)
					return xerrors.Errorf("failed to find dir %s for mounting: %w", mapping.IRODSPath, err)
				}
			} else {
				// file not found
				if mapping.IgnoreNotExistError {
					// skip
					logger.WithError(err).Debugf("ignoring non-existing file %s for mounting", mapping.IRODSPath)
					return nil
				}

				logger.WithError(err).Errorf("failed to find file %s for mounting", mapping.IRODSPath)
				return xerrors.Errorf("failed to find file %s for mounting: %w", mapping.IRODSPath, err)
			}
		} else {
			// server error
			logger.WithError(err).Errorf("failed to check path - %s", mapping.IRODSPath)
			errored = true
		}
	} else {
		pathExist = true
	}

	// make dir
	if makeDir {
		err := manager.fsClient.MakeDir(mapping.IRODSPath, true)
		if err != nil {
			logger.WithError(err).Errorf("failed to make a dir %s for mounting", mapping.IRODSPath)

			if mapping.IgnoreNotExistError {
				// skip
				logger.WithError(err).Debugf("ignoring non-existing dir %s for mounting", mapping.IRODSPath)
				return nil
			}

			return xerrors.Errorf("failed to make dir %s for mounting: %w", mapping.IRODSPath, err)
		} else {
			// make dir ok
			irodsEntry, err = manager.fsClient.Stat(mapping.IRODSPath)
			if err != nil {
				logger.WithError(err).Errorf("failed to find dir %s for mounting", mapping.IRODSPath)
				errored = true
			} else {
				pathExist = true
			}
		}
	}

	if pathExist {
		// add entry
		logger.Debugf("Creating VFS entry mapping - irods path %s => vpath %s (%t)", irodsEntry.Path, mapping.MappingPath, mapping.ReadOnly)
		entry := NewVPathEntryFromIRODSFSEntry(mapping.MappingPath, mapping.IRODSPath, irodsEntry, mapping.ReadOnly)
		manager.entries[mapping.MappingPath] = entry

		// add to parent
		if len(parentDirs) > 0 {
			parentPath := parentDirs[len(parentDirs)-1]
			if parentEntry, ok := manager.entries[parentPath]; ok {
				parentEntry.VirtualDirEntry.DirEntries = append(parentEntry.VirtualDirEntry.DirEntries, entry)
			}
		}
	} else if errored {
		// add empty entry
		logger.Debugf("Creating VFS entry mapping - irods path %s => vpath %s (%t), empty entry", mapping.IRODSPath, mapping.MappingPath, mapping.ReadOnly)
		entry := NewVPathEntryFromIRODSFSEntry(mapping.MappingPath, mapping.IRODSPath, nil, mapping.ReadOnly)
		manager.entries[mapping.MappingPath] = entry

		// add to parent
		if len(parentDirs) > 0 {
			parentPath := parentDirs[len(parentDirs)-1]
			if parentEntry, ok := manager.entries[parentPath]; ok {
				parentEntry.VirtualDirEntry.DirEntries = append(parentEntry.VirtualDirEntry.DirEntries, entry)
			}
		}
	} else {
		logger.Errorf("failed to build a mapping for path - %s", mapping.IRODSPath)
		return xerrors.Errorf("failed to build a mapping for path - %s", mapping.IRODSPath)
	}

	return nil
}
