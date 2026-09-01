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
	// path mappings given by user
	pathMappings []VPathMapping
	// entries is a map holding vpath entries.
	// Key is a vpath, value is an entry
	entries                  map[string]*VPathEntry
	currentVirtualEntryIDInc uint64
	virtualEntryIDMap        map[string]uint64
	reverseVirtualEntryIDMap map[uint64]string
	fsClient                 irods.IRODSFSClient
	mutex                    sync.RWMutex
}

// NewVPathManager creates a new VPathManager
func NewVPathManager(fsClient irods.IRODSFSClient, pathMappings []VPathMapping) (*VPathManager, error) {
	logger := log.WithFields(log.Fields{})

	manager := &VPathManager{
		pathMappings:             pathMappings,
		entries:                  map[string]*VPathEntry{},
		currentVirtualEntryIDInc: 0,
		virtualEntryIDMap:        map[string]uint64{},
		reverseVirtualEntryIDMap: map[uint64]string{},
		fsClient:                 fsClient,
	}

	logger.Info("Building a hierarchy")
	err := manager.build()
	if err != nil {
		buildErr := errors.Wrap(err, "failed to build a hierarchy")
		logger.Error(buildErr)
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
			return errors.Wrap(err, "failed to build vpath mapping")
		}
	}
	return nil
}

// CreateOrGetInodeIDForVirtualEntry returns inode id for a virtual entry.
func (manager *VPathManager) CreateOrGetInodeIDForVirtualEntry(path string) (uint64, error) {
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
	id := inode.VirtualEntryIDStart + manager.currentVirtualEntryIDInc
	if !inode.IsVirtualEntryID(id) {
		return 0, errors.Newf("virtual inode ID overflow: no more IDs available for path %q", path)
	}
	manager.currentVirtualEntryIDInc++
	manager.virtualEntryIDMap[path] = id
	manager.reverseVirtualEntryIDMap[id] = path
	return id, nil
}

// internal function
func (manager *VPathManager) createOrGetInodeIDForVirtualEntry(path string) (uint64, error) {
	// First try
	if id, ok := manager.virtualEntryIDMap[path]; ok {
		return id, nil
	}

	// Double-check after acquiring write lock (another goroutine might have created it)
	if id, ok := manager.virtualEntryIDMap[path]; ok {
		return id, nil
	}

	// Create a new and save for reuse
	id := inode.VirtualEntryIDStart + manager.currentVirtualEntryIDInc
	if !inode.IsVirtualEntryID(id) {
		return 0, errors.Newf("virtual inode ID overflow: no more IDs available for path %q", path)
	}
	manager.currentVirtualEntryIDInc++
	manager.virtualEntryIDMap[path] = id
	manager.reverseVirtualEntryIDMap[id] = path
	return id, nil
}

// buildMapping builds a single virtual path mapping
func (manager *VPathManager) buildMapping(mapping *VPathMapping) error {
	logger := log.WithFields(log.Fields{})

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
			inodeID, err := manager.createOrGetInodeIDForVirtualEntry(parentDir)
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

					werr := errors.Wrapf(err, "failed to find dir %q for mounting", mapping.IRODSPath)
					logger.Error(werr)
					return werr
				}
			} else {
				// file not found
				if mapping.IgnoreNotExistError {
					// skip
					logger.WithError(err).Debugf("ignoring non-existing file %q for mounting", mapping.IRODSPath)
					return nil
				}

				werr := errors.Wrapf(err, "failed to find file %q for mounting", mapping.IRODSPath)
				logger.Error(werr)
				return werr
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
		werr := errors.Newf("failed to build a mapping for path %q", mapping.IRODSPath)
		logger.Error(werr)
		return werr
	}

	return nil
}

// addToParentDir adds an entry to its parent directory's entry list
func (manager *VPathManager) addToParentDir(entry *VPathEntry, parentPath string) {
	if parentEntry, ok := manager.entries[parentPath]; ok {
		parentEntry.VirtualDirEntry.DirEntries = append(parentEntry.VirtualDirEntry.DirEntries, entry)
	}
}
