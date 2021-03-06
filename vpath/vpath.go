package vpath

import (
	"fmt"
	"time"

	"github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
)

// VPathManager is a struct that manages virtual paths.
type VPathManager struct {
	// path mappings given by user
	pathMappings []VPathMapping
	// entries is a map holding vpath entries.
	// Key is a vpath, value is an entry
	entries  map[string]*VPathEntry
	fsClient irods.IRODSFSClient
}

// NewVPathManager creates a new VPathManager
func NewVPathManager(fsClient irods.IRODSFSClient, pathMappings []VPathMapping) (*VPathManager, error) {
	logger := log.WithFields(log.Fields{
		"package":  "vpath",
		"function": "NewVPathManager",
	})

	manager := &VPathManager{
		pathMappings: pathMappings,
		entries:      map[string]*VPathEntry{},
		fsClient:     fsClient,
	}

	logger.Info("Building a hierarchy")
	err := manager.build()
	if err != nil {
		logger.WithError(err).Error("failed to build a hierarchy")
		return nil, err
	}

	return manager, nil
}

// build builds VPaths from mappings
func (manager *VPathManager) build() error {
	logger := log.WithFields(log.Fields{
		"package":  "vpath",
		"struct":   "VPathManager",
		"function": "build",
	})

	manager.entries = map[string]*VPathEntry{}

	// build
	for _, mapping := range manager.pathMappings {
		err := manager.buildOne(&mapping)
		if err != nil {
			logger.Error(err)
			return err
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
				err := fmt.Errorf("failed to create a virtual dir entry %s, iRODS entry already exists", parentDir)
				logger.Error(err)
				return err
			}
		} else {
			dirEntry := &VPathEntry{
				Type:     VPathVirtualDir,
				Path:     parentDir,
				ReadOnly: true,
				VirtualDirEntry: &VPathVirtualDirEntry{
					ID:         0,
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

	// if it is an iRODS dir (collection) resource, CreateDir flag is on
	if mapping.ResourceType == VPathMappingDirectory && mapping.CreateDir {
		logger.Debugf("Checking if path exists - %s", mapping.IRODSPath)
		if !manager.fsClient.ExistsDir(mapping.IRODSPath) {
			logger.Debugf("Creating path - %s", mapping.IRODSPath)
			err := manager.fsClient.MakeDir(mapping.IRODSPath, true)
			if err != nil {
				logger.WithError(err).Errorf("failed to make a dir - %s", mapping.IRODSPath)
				// fall below
			}
		}
	}

	// add leaf
	logger.Debugf("Checking path - %s", mapping.IRODSPath)
	irodsEntry, err := manager.fsClient.Stat(mapping.IRODSPath)
	if err != nil {
		if mapping.IgnoreNotExistError {
			// ignore
			return nil
		}

		logger.WithError(err).Errorf("failed to stat - %s", mapping.IRODSPath)
		return err
	}

	logger.Debugf("Creating VFS entry mapping - irods path %s => vpath %s (%t)", irodsEntry.Path, mapping.MappingPath, mapping.ReadOnly)
	entry := NewVPathEntryFromIRODSFSEntry(mapping.MappingPath, irodsEntry, mapping.ReadOnly)
	manager.entries[mapping.MappingPath] = entry

	// add to parent
	if len(parentDirs) > 0 {
		parentPath := parentDirs[len(parentDirs)-1]
		if parentEntry, ok := manager.entries[parentPath]; ok {
			parentEntry.VirtualDirEntry.DirEntries = append(parentEntry.VirtualDirEntry.DirEntries, entry)
		}
	}

	return nil
}
