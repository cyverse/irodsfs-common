package vpath

import (
	"path"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsclient_util "github.com/cyverse/go-irodsclient/irods/util"
	"github.com/cyverse/irodsfs-common/inode"
	"github.com/cyverse/irodsfs-common/irods"
	log "github.com/sirupsen/logrus"
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
	mutex    sync.RWMutex
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
		buildErr := errors.Wrapf(err, "failed to build a hierarchy")
		logger.Errorf("%+v", buildErr)
		return nil, buildErr
	}

	return manager, nil
}

// HasEntry returns true if it has VFS Entry for the path
func (manager *VPathManager) HasEntry(vpath string) bool {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	_, ok := manager.entries[vpath]
	return ok
}

// GetEntry returns VFS Entry for the Path
func (manager *VPathManager) GetEntry(vpath string) *VPathEntry {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	if entry, ok := manager.entries[vpath]; ok {
		return entry
	}

	return nil
}

// GetClosestEntry returns the closest VFS Entry for the path
// if an entry for the given vpath exists, returns it
// if not exists, finds a parent dir entry that exists
func (manager *VPathManager) GetClosestEntry(vpath string) *VPathEntry {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	// if there's an exact match
	// returns it
	if entry, ok := manager.entries[vpath]; ok {
		return entry
	}

	// get all parent dirs of the given vpath and check if it exists
	parentDirs := irodsclient_util.GetIRODSParentDirs(vpath)
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

// build builds VPaths from mappings
func (manager *VPathManager) build() error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	manager.entries = map[string]*VPathEntry{}

	// build
	for _, mapping := range manager.pathMappings {
		err := manager.buildMapping(&mapping)
		if err != nil {
			return errors.Wrapf(err, "failed to build vpath mapping")
		}
	}
	return nil
}

// buildMapping builds a single virtual path mapping
func (manager *VPathManager) buildMapping(mapping *VPathMapping) error {
	logger := log.WithFields(log.Fields{
		"package":  "vpath",
		"struct":   "VPathManager",
		"function": "buildMapping",
	})

	logger.Infof("Building a VPath Entry %q", mapping.IRODSPath)

	now := time.Now()

	parentDirs := irodsclient_util.GetIRODSParentDirs(mapping.MappingPath)
	for idx, parentDir := range parentDirs {
		// add parentDir if not exists
		if parentDirEntry, ok := manager.entries[parentDir]; ok {
			// exists, check if it is VPathVirtualDir
			if parentDirEntry.Type != VPathVirtualDir {
				// already exists
				// can't create a virtual dir entry under an irods entry
				return errors.Newf("failed to create a virtual dir entry %q, entry already exists", parentDir)
			}
		} else {
			inodeID, err := manager.inodeManager.GetInodeIDForVirtualEntry(parentDir)
			if err != nil {
				return err
			}
			dirEntry := &VPathEntry{
				Type:     VPathVirtualDir,
				Path:     parentDir,
				ReadOnly: true,
				VirtualDirEntry: &VPathVirtualDirEntry{
					ID:         inodeID,
					Name:       path.Base(parentDir),
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

	logger.Debugf("Checking path - %q", mapping.IRODSPath)
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
						logger.WithError(err).Debugf("ignoring non-existing dir %q for mounting", mapping.IRODSPath)
						return nil
					}

					logger.WithError(err).Errorf("failed to find dir %q for mounting", mapping.IRODSPath)
					return errors.Wrapf(err, "failed to find dir %q for mounting", mapping.IRODSPath)
				}
			} else {
				// file not found
				if mapping.IgnoreNotExistError {
					// skip
					logger.WithError(err).Debugf("ignoring non-existing file %q for mounting", mapping.IRODSPath)
					return nil
				}

				logger.WithError(err).Errorf("failed to find file %q for mounting", mapping.IRODSPath)
				return errors.Wrapf(err, "failed to find file %q for mounting", mapping.IRODSPath)
			}
		} else {
			// server error
			logger.WithError(err).Errorf("failed to check path - %q", mapping.IRODSPath)
			errored = true
		}
	} else {
		pathExist = true
	}

	// make dir
	if makeDir {
		err := manager.fsClient.MakeDir(mapping.IRODSPath, true)
		if err != nil {
			logger.WithError(err).Errorf("failed to make a dir %q for mounting", mapping.IRODSPath)

			if mapping.IgnoreNotExistError {
				// skip
				logger.WithError(err).Debugf("ignoring non-existing dir %q for mounting", mapping.IRODSPath)
				return nil
			}

			return errors.Wrapf(err, "failed to make dir %q for mounting", mapping.IRODSPath)
		} else {
			// make dir ok
			irodsEntry, err = manager.fsClient.Stat(mapping.IRODSPath)
			if err != nil {
				logger.WithError(err).Errorf("failed to find dir %q for mounting", mapping.IRODSPath)
				errored = true
			} else {
				pathExist = true
			}
		}
	}

	if pathExist {
		// add entry
		logger.Debugf("Creating VFS entry mapping - irods path %q => vpath %q (%t)", irodsEntry.Path, mapping.MappingPath, mapping.ReadOnly)
		entry := NewVPathEntryFromIRODSFSEntry(mapping.MappingPath, mapping.IRODSPath, irodsEntry, mapping.ReadOnly)
		manager.entries[mapping.MappingPath] = entry

		// add to parent
		if len(parentDirs) > 0 {
			manager.addToParentDir(entry, parentDirs[len(parentDirs)-1])
		}
	} else if errored {
		// add empty entry
		logger.Debugf("Creating VFS entry mapping - irods path %q => vpath %q (%t), empty entry", mapping.IRODSPath, mapping.MappingPath, mapping.ReadOnly)
		entry := NewVPathEntryFromIRODSFSEntry(mapping.MappingPath, mapping.IRODSPath, nil, mapping.ReadOnly)
		manager.entries[mapping.MappingPath] = entry

		// add to parent
		if len(parentDirs) > 0 {
			manager.addToParentDir(entry, parentDirs[len(parentDirs)-1])
		}
	} else {
		logger.Errorf("failed to build a mapping for path %q", mapping.IRODSPath)
		return errors.Newf("failed to build a mapping for path %q", mapping.IRODSPath)
	}

	return nil
}

// addToParentDir adds an entry to its parent directory's entry list
func (manager *VPathManager) addToParentDir(entry *VPathEntry, parentPath string) {
	if parentEntry, ok := manager.entries[parentPath]; ok {
		parentEntry.VirtualDirEntry.DirEntries = append(parentEntry.VirtualDirEntry.DirEntries, entry)
	}
}
