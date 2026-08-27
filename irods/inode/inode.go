package inode

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/irodsfs-common/inode"
	"github.com/dgraph-io/badger/v3"
)

const (
	stagingEntryPathKeyPrefix = "inode:staging:path:"
	stagingEntryIDKeyPrefix   = "inode:staging:id:"
)

// InodeManager is a struct that manages inode.
type InodeManager struct {
	currentStagingEntryIDInc uint64
	stagingEntryIDMap        map[string]uint64
	reverseStagingEntryIDMap map[uint64]string
	db                       *badger.DB
	metaPath                 string
	persistent               bool
	closed                   bool
	mutex                    sync.RWMutex
}

// NewInodeManager creates a new InodeManager
func NewInodeManager() *InodeManager {
	return &InodeManager{
		currentStagingEntryIDInc: 0,
		stagingEntryIDMap:        map[string]uint64{},
		reverseStagingEntryIDMap: map[uint64]string{},
		mutex:                    sync.RWMutex{},
	}
}

// NewInodeManagerWithPersistence creates a new InodeManager backed by Badger.
// The database is runtime-only: metadata left by a previous process is removed
// instead of being restored.
func NewInodeManagerWithPersistence(localRootPath string) (*InodeManager, error) {
	if localRootPath == "" {
		return nil, errors.New("LocalRootPath is required")
	}

	metaPath := filepath.Join(localRootPath, "inode")
	if err := os.MkdirAll(metaPath, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create inode metadata directory")
	}

	opts := badger.DefaultOptions(metaPath)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open Badger database")
	}
	if err := db.DropAll(); err != nil {
		db.Close()
		return nil, errors.Wrap(err, "failed to clear stale inode metadata")
	}

	return &InodeManager{
		currentStagingEntryIDInc: 0,
		db:                       db,
		metaPath:                 metaPath,
		persistent:               true,
		mutex:                    sync.RWMutex{},
	}, nil
}

// GetInodeIDForStagingEntry returns inode id for a staging entry (locally buffered).
func (manager *InodeManager) GetInodeIDForStagingEntry(path string) (uint64, bool) {
	if manager.persistent {
		return manager.getPersistentInodeIDForStagingEntry(path)
	}

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()
	if id, ok := manager.stagingEntryIDMap[path]; ok {
		return id, true
	} else {
		return 0, false
	}
}

// CreateOrGetInodeIDForStagingEntry returns inode id for a staging entry (locally buffered), if not exists, generate one.
func (manager *InodeManager) CreateOrGetInodeIDForStagingEntry(path string) (uint64, error) {
	if manager.persistent {
		return manager.createOrGetPersistentInodeIDForStagingEntry(path)
	}

	// First try with read lock
	manager.mutex.RLock()
	if id, ok := manager.stagingEntryIDMap[path]; ok {
		manager.mutex.RUnlock()
		return id, nil
	}
	manager.mutex.RUnlock()

	// Need to create new entry, use write lock
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	// Double-check after acquiring write lock (another goroutine might have created it)
	if id, ok := manager.stagingEntryIDMap[path]; ok {
		return id, nil
	}

	// Create a new and save for reuse
	id := inode.StagingEntryIDStart + manager.currentStagingEntryIDInc
	if !inode.IsStagingEntryID(id) {
		return 0, errors.Newf("staging inode ID overflow: no more IDs available for path %q", path)
	}
	manager.currentStagingEntryIDInc++
	manager.stagingEntryIDMap[path] = id
	manager.reverseStagingEntryIDMap[id] = path
	return id, nil
}

// GetPathForStagingEntryID returns the path for a given staging inode ID.
func (manager *InodeManager) GetPathForStagingEntryID(inodeID uint64) (string, bool, error) {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	if manager.persistent {
		if manager.closed {
			return "", false, errors.New("inode manager is closed")
		}

		var path string
		err := manager.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(getStagingEntryIDKey(inodeID))
			if err != nil {
				return err
			}

			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			path = string(value)
			return nil
		})
		if errors.Is(err, badger.ErrKeyNotFound) {
			return "", false, nil
		}
		if err != nil {
			return "", false, errors.Wrap(err, "failed to get staging entry path")
		}
		return path, true, nil
	}

	path, ok := manager.reverseStagingEntryIDMap[inodeID]
	return path, ok, nil
}

// RemoveStagingEntry removes a staging entry
func (manager *InodeManager) RemoveStagingEntry(path string) error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if manager.persistent {
		if manager.closed {
			return errors.New("inode manager is closed")
		}

		err := manager.db.Update(func(txn *badger.Txn) error {
			item, err := txn.Get(getStagingEntryPathKey(path))
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}

			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			inodeID, err := decodeStagingEntryID(value)
			if err != nil {
				return err
			}

			if err := txn.Delete(getStagingEntryPathKey(path)); err != nil {
				return err
			}
			return txn.Delete(getStagingEntryIDKey(inodeID))
		})
		if err != nil {
			return errors.Wrapf(err, "failed to remove staging entry %q", path)
		}
		return nil
	}

	if id, ok := manager.stagingEntryIDMap[path]; ok {
		delete(manager.reverseStagingEntryIDMap, id)
		delete(manager.stagingEntryIDMap, path)
	}
	return nil
}

// RenameStagingEntry moves a staging entry to a new path while preserving its inode ID.
func (manager *InodeManager) RenameStagingEntry(oldPath string, newPath string) error {
	return manager.renameStagingEntries(oldPath, newPath, false)
}

// RenameStagingEntryTree moves a directory and all staging entries below it while
// preserving every inode ID.
func (manager *InodeManager) RenameStagingEntryTree(oldPath string, newPath string) error {
	return manager.renameStagingEntries(oldPath, newPath, true)
}

// Close closes the Badger database and removes its runtime-only metadata.
func (manager *InodeManager) Close() error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if !manager.persistent || manager.closed {
		return nil
	}

	if err := manager.db.Close(); err != nil {
		return err
	}
	manager.closed = true
	manager.db = nil

	if manager.metaPath != "" {
		if err := os.RemoveAll(manager.metaPath); err != nil {
			return errors.Wrap(err, "failed to remove inode metadata directory")
		}
	}

	return nil
}

func (manager *InodeManager) getPersistentInodeIDForStagingEntry(path string) (uint64, bool) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if manager.closed {
		return 0, false
	}

	inodeID := uint64(0)
	exist := false
	err := manager.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(getStagingEntryPathKey(path))
		if err == nil {
			value, valueErr := item.ValueCopy(nil)
			if valueErr != nil {
				return valueErr
			}
			inodeID, valueErr = decodeStagingEntryID(value)
			exist = true
			return valueErr
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		return nil
	})
	if err != nil {
		return 0, false
	}
	return inodeID, exist
}

func (manager *InodeManager) createOrGetPersistentInodeIDForStagingEntry(path string) (uint64, error) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if manager.closed {
		return 0, errors.New("inode manager is closed")
	}

	var inodeID uint64
	created := false
	err := manager.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(getStagingEntryPathKey(path))
		if err == nil {
			value, valueErr := item.ValueCopy(nil)
			if valueErr != nil {
				return valueErr
			}
			inodeID, valueErr = decodeStagingEntryID(value)
			return valueErr
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		inodeID = inode.StagingEntryIDStart + manager.currentStagingEntryIDInc
		if !inode.IsStagingEntryID(inodeID) {
			return errors.Newf("staging inode ID overflow: no more IDs available for path %q", path)
		}

		encodedID := make([]byte, 8)
		binary.BigEndian.PutUint64(encodedID, inodeID)
		if err := txn.Set(getStagingEntryPathKey(path), encodedID); err != nil {
			return err
		}
		if err := txn.Set(getStagingEntryIDKey(inodeID), []byte(path)); err != nil {
			return err
		}
		created = true
		return nil
	})
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get staging inode ID for path %q", path)
	}
	if created {
		manager.currentStagingEntryIDInc++
	}
	return inodeID, nil
}

func getStagingEntryPathKey(path string) []byte {
	return []byte(stagingEntryPathKeyPrefix + path)
}

func getStagingEntryIDKey(inodeID uint64) []byte {
	key := make([]byte, len(stagingEntryIDKeyPrefix)+8)
	copy(key, stagingEntryIDKeyPrefix)
	binary.BigEndian.PutUint64(key[len(stagingEntryIDKeyPrefix):], inodeID)
	return key
}

func decodeStagingEntryID(value []byte) (uint64, error) {
	if len(value) != 8 {
		return 0, errors.Newf("invalid staging inode ID length: %d", len(value))
	}
	return binary.BigEndian.Uint64(value), nil
}

type stagingEntryMapping struct {
	path    string
	inodeID uint64
}

func (manager *InodeManager) renameStagingEntries(oldPath string, newPath string, recursive bool) error {
	if oldPath == newPath {
		return nil
	}

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if manager.persistent {
		if manager.closed {
			return errors.New("inode manager is closed")
		}
		return manager.renamePersistentStagingEntries(oldPath, newPath, recursive)
	}

	sourceMappings := []stagingEntryMapping{}
	for entryPath, inodeID := range manager.stagingEntryIDMap {
		if isStagingPathMatch(entryPath, oldPath, recursive) {
			sourceMappings = append(sourceMappings, stagingEntryMapping{
				path:    entryPath,
				inodeID: inodeID,
			})
		}
	}
	if len(sourceMappings) == 0 {
		return nil
	}

	for entryPath, inodeID := range manager.stagingEntryIDMap {
		if isStagingPathMatch(entryPath, newPath, recursive) {
			delete(manager.stagingEntryIDMap, entryPath)
			delete(manager.reverseStagingEntryIDMap, inodeID)
		}
	}

	for _, mapping := range sourceMappings {
		delete(manager.stagingEntryIDMap, mapping.path)
		newEntryPath := getRenamedStagingPath(mapping.path, oldPath, newPath)
		manager.stagingEntryIDMap[newEntryPath] = mapping.inodeID
		manager.reverseStagingEntryIDMap[mapping.inodeID] = newEntryPath
	}

	return nil
}

func (manager *InodeManager) renamePersistentStagingEntries(oldPath string, newPath string, recursive bool) error {
	sourceMappings, err := manager.getPersistentStagingEntryMappings(oldPath, recursive)
	if err != nil {
		return errors.Wrapf(err, "failed to find staging entries under %q", oldPath)
	}
	if len(sourceMappings) == 0 {
		return nil
	}

	destinationMappings, err := manager.getPersistentStagingEntryMappings(newPath, recursive)
	if err != nil {
		return errors.Wrapf(err, "failed to find staging entries under %q", newPath)
	}

	batch := manager.db.NewWriteBatch()
	defer batch.Cancel()

	for _, mapping := range destinationMappings {
		if err := batch.Delete(getStagingEntryPathKey(mapping.path)); err != nil {
			return errors.Wrap(err, "failed to remove destination staging path")
		}
		if err := batch.Delete(getStagingEntryIDKey(mapping.inodeID)); err != nil {
			return errors.Wrap(err, "failed to remove destination staging inode ID")
		}
	}

	for _, mapping := range sourceMappings {
		if err := batch.Delete(getStagingEntryPathKey(mapping.path)); err != nil {
			return errors.Wrap(err, "failed to remove old staging path")
		}

		newEntryPath := getRenamedStagingPath(mapping.path, oldPath, newPath)
		encodedID := make([]byte, 8)
		binary.BigEndian.PutUint64(encodedID, mapping.inodeID)
		if err := batch.Set(getStagingEntryPathKey(newEntryPath), encodedID); err != nil {
			return errors.Wrap(err, "failed to save new staging path")
		}
		if err := batch.Set(getStagingEntryIDKey(mapping.inodeID), []byte(newEntryPath)); err != nil {
			return errors.Wrap(err, "failed to update staging inode path")
		}
	}

	if err := batch.Flush(); err != nil {
		return errors.Wrap(err, "failed to rename staging entries")
	}
	return nil
}

func (manager *InodeManager) getPersistentStagingEntryMappings(rootPath string, recursive bool) ([]stagingEntryMapping, error) {
	mappings := []stagingEntryMapping{}
	err := manager.db.View(func(txn *badger.Txn) error {
		if !recursive {
			item, err := txn.Get(getStagingEntryPathKey(rootPath))
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}

			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			inodeID, err := decodeStagingEntryID(value)
			if err != nil {
				return err
			}
			mappings = append(mappings, stagingEntryMapping{path: rootPath, inodeID: inodeID})
			return nil
		}

		opts := badger.DefaultIteratorOptions
		opts.Prefix = getStagingEntryPathKey(cleanStagingRootPath(rootPath))
		iterator := txn.NewIterator(opts)
		defer iterator.Close()

		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			item := iterator.Item()
			entryPath := string(item.KeyCopy(nil)[len(stagingEntryPathKeyPrefix):])
			if !isStagingPathMatch(entryPath, rootPath, true) {
				continue
			}

			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			inodeID, err := decodeStagingEntryID(value)
			if err != nil {
				return err
			}
			mappings = append(mappings, stagingEntryMapping{path: entryPath, inodeID: inodeID})
		}
		return nil
	})
	return mappings, err
}

func cleanStagingRootPath(rootPath string) string {
	if rootPath == "/" {
		return rootPath
	}
	return strings.TrimRight(rootPath, "/")
}

func isStagingPathMatch(entryPath string, rootPath string, recursive bool) bool {
	rootPath = cleanStagingRootPath(rootPath)
	if entryPath == rootPath {
		return true
	}
	if !recursive {
		return false
	}
	if rootPath == "/" {
		return strings.HasPrefix(entryPath, "/")
	}
	return strings.HasPrefix(entryPath, rootPath+"/")
}

func getRenamedStagingPath(entryPath string, oldPath string, newPath string) string {
	oldPath = cleanStagingRootPath(oldPath)
	newPath = cleanStagingRootPath(newPath)
	if entryPath == oldPath {
		return newPath
	}
	if newPath == "/" {
		return newPath + strings.TrimPrefix(entryPath[len(oldPath):], "/")
	}
	return newPath + entryPath[len(oldPath):]
}
