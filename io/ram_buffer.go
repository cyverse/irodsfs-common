package io

import (
	"fmt"
	"sync"
	"time"
)

// RAMBufferEntry defines an entry, implements BufferEntry
type RAMBufferEntry struct {
	key          string
	size         int
	accessCount  int
	creationTime time.Time
	data         []byte
	mutex        sync.Mutex
}

// NewRAMBufferEntry creates a new RAMBufferEntry
func NewRAMBufferEntry(key string, data []byte) *RAMBufferEntry {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	return &RAMBufferEntry{
		key:          key,
		size:         len(data),
		accessCount:  0,
		creationTime: time.Now(),
		data:         dataCopy,
	}
}

// GetKey returns key of the entry
func (entry *RAMBufferEntry) GetKey() string {
	return entry.key
}

// GetSize returns size of the entry
func (entry *RAMBufferEntry) GetSize() int {
	return entry.size
}

// GetAccessCount returns access count of the entry
func (entry *RAMBufferEntry) GetAccessCount() int {
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	return entry.accessCount
}

// GetCreationTime returns creation time of the entry
func (entry *RAMBufferEntry) GetCreationTime() time.Time {
	return entry.creationTime
}

// GetCreationTime returns data of the entry
func (entry *RAMBufferEntry) GetData() []byte {
	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	entry.accessCount++
	return entry.data
}

// RAMBufferEntryGroup defines a group, implements BufferEntryGroup
type RAMBufferEntryGroup struct {
	buffer *RAMBuffer

	name     string
	size     int64
	entryMap map[string]*RAMBufferEntry

	mutex sync.Mutex
}

// NewRAMBufferEntryGroup creates a new RAMBufferEntryGroup
func NewRAMBufferEntryGroup(buffer *RAMBuffer, name string) *RAMBufferEntryGroup {
	return &RAMBufferEntryGroup{
		buffer: buffer,

		name:     name,
		size:     0,
		entryMap: map[string]*RAMBufferEntry{},
	}
}

// GetBuffer returns buffer
func (group *RAMBufferEntryGroup) GetBuffer() Buffer {
	return group.buffer
}

// GetName returns group name
func (group *RAMBufferEntryGroup) GetName() string {
	return group.name
}

// GetEntryCount returns the number of entries in the group
func (group *RAMBufferEntryGroup) GetEntryCount() int {
	group.buffer.mutex.Lock()
	defer group.buffer.mutex.Unlock()

	group.mutex.Lock()
	defer group.mutex.Unlock()

	return len(group.entryMap)
}

func (group *RAMBufferEntryGroup) getEntryCountWithoutBufferLock() int {
	group.mutex.Lock()
	defer group.mutex.Unlock()

	return len(group.entryMap)
}

// GetSize returns total size of entries for the group
func (group *RAMBufferEntryGroup) GetSize() int64 {
	group.buffer.mutex.Lock()
	defer group.buffer.mutex.Unlock()

	group.mutex.Lock()
	defer group.mutex.Unlock()

	return group.size
}

func (group *RAMBufferEntryGroup) getSizeWithoutBufferLock() int64 {
	group.mutex.Lock()
	defer group.mutex.Unlock()

	return group.size
}

// GetEntryKeys returns keys of entries in the group
func (group *RAMBufferEntryGroup) GetEntryKeys() []string {
	group.buffer.mutex.Lock()
	defer group.buffer.mutex.Unlock()

	group.mutex.Lock()
	defer group.mutex.Unlock()

	keys := []string{}

	for key := range group.entryMap {
		keys = append(keys, key)
	}
	return keys
}

// DeleteAllEntries deletes all entries in the group
func (group *RAMBufferEntryGroup) DeleteAllEntries() {
	group.buffer.mutex.Lock()
	group.mutex.Lock()

	for _, entry := range group.entryMap {
		group.size -= int64(entry.GetSize())
	}

	group.entryMap = map[string]*RAMBufferEntry{}

	group.mutex.Unlock()
	group.buffer.condition.Broadcast()
	group.buffer.mutex.Unlock()
}

func (group *RAMBufferEntryGroup) deleteAllEntriesWithoutBufferLock() {
	group.mutex.Lock()
	defer group.mutex.Unlock()

	for _, entry := range group.entryMap {
		group.size -= int64(entry.GetSize())
	}

	group.entryMap = map[string]*RAMBufferEntry{}
}

// CreateEntry creates an entry in the group
func (group *RAMBufferEntryGroup) CreateEntry(key string, data []byte) (BufferEntry, error) {
	group.buffer.mutex.Lock()
	if group.buffer.sizeCap < int64(len(data)) {
		group.buffer.mutex.Unlock()
		return nil, fmt.Errorf("requested data %d is larger than size cap %d", len(data), group.buffer.sizeCap)
	}
	group.buffer.mutex.Unlock()

	for {
		group.buffer.mutex.Lock()

		var size int64 = 0
		for _, group := range group.buffer.entryGroupMap {
			size += group.getSizeWithoutBufferLock()
		}
		avail := group.buffer.sizeCap - size

		if avail >= int64(len(data)) {
			group.mutex.Lock()

			entry := NewRAMBufferEntry(key, data)
			group.entryMap[key] = entry
			group.size += int64(len(data))

			group.mutex.Unlock()
			group.buffer.mutex.Unlock()
			return entry, nil
		}

		// wait for availability
		group.buffer.condition.Wait()
		group.buffer.mutex.Unlock()
	}
}

// GetEntry returns an entry
func (group *RAMBufferEntryGroup) GetEntry(key string) BufferEntry {
	group.buffer.mutex.Lock()
	defer group.buffer.mutex.Unlock()

	group.mutex.Lock()
	defer group.mutex.Unlock()

	if entry, ok := group.entryMap[key]; ok {
		return entry
	}

	return nil
}

// DeleteEntry deletes an entry
func (group *RAMBufferEntryGroup) DeleteEntry(key string) {
	group.buffer.mutex.Lock()
	group.mutex.Lock()

	if entry, ok := group.entryMap[key]; ok {
		group.size -= int64(entry.GetSize())
	}

	delete(group.entryMap, key)

	group.mutex.Unlock()
	group.buffer.condition.Broadcast()
	group.buffer.mutex.Unlock()
}

// PopEntry returns and deletes an entry
func (group *RAMBufferEntryGroup) PopEntry(key string) BufferEntry {
	group.buffer.mutex.Lock()
	group.mutex.Lock()

	var returnEntry BufferEntry = nil
	if entry, ok := group.entryMap[key]; ok {
		group.size -= int64(entry.GetSize())
		returnEntry = entry
	}

	delete(group.entryMap, key)

	group.mutex.Unlock()
	group.buffer.condition.Broadcast()
	group.buffer.mutex.Unlock()

	return returnEntry
}

// RAMBuffer implements Buffer
type RAMBuffer struct {
	sizeCap       int64
	entryGroupMap map[string]*RAMBufferEntryGroup

	mutex     *sync.Mutex
	condition *sync.Cond
}

// NewRAMBuffer creates a new RAMBuffer
func NewRAMBuffer(sizeCap int64) *RAMBuffer {
	mutex := sync.Mutex{}
	return &RAMBuffer{
		sizeCap:       sizeCap,
		entryGroupMap: map[string]*RAMBufferEntryGroup{},
		mutex:         &mutex,
		condition:     sync.NewCond(&mutex),
	}
}

// Release releases all resources for buffer
func (buffer *RAMBuffer) Release() {
	buffer.DeleteAllEntryGroups()
}

// GetSizeCap returns size cap
func (buffer *RAMBuffer) GetSizeCap() int64 {
	return buffer.sizeCap
}

// GetTotalEntries returns total number of entries
func (buffer *RAMBuffer) GetTotalEntries() int {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()

	entries := 0

	for _, group := range buffer.entryGroupMap {
		entries += group.getEntryCountWithoutBufferLock()
	}

	return entries
}

// GetTotalEntrySize returns total size of entries
func (buffer *RAMBuffer) GetTotalEntrySize() int64 {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()

	var size int64 = 0
	for _, group := range buffer.entryGroupMap {
		size += group.getSizeWithoutBufferLock()
	}

	return size
}

// GetAvailableSize returns available size
func (buffer *RAMBuffer) GetAvailableSize() int64 {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()

	var size int64 = 0
	for _, group := range buffer.entryGroupMap {
		size += group.getSizeWithoutBufferLock()
	}

	return buffer.sizeCap - size
}

// WaitForSpace waits until the given size of space is available
func (buffer *RAMBuffer) WaitForSpace(spaceRequired int64) bool {
	buffer.mutex.Lock()
	if buffer.sizeCap < spaceRequired {
		buffer.mutex.Unlock()
		return false
	}
	buffer.mutex.Unlock()

	for {
		buffer.mutex.Lock()

		var size int64 = 0
		for _, group := range buffer.entryGroupMap {
			size += group.getSizeWithoutBufferLock()
		}
		avail := buffer.sizeCap - size

		if avail >= spaceRequired {
			buffer.mutex.Unlock()
			return true
		}

		// wait for availability
		buffer.condition.Wait()
		buffer.mutex.Unlock()
	}
}

// CreateEntryGroup creates a new BufferEntryGroup
func (buffer *RAMBuffer) CreateEntryGroup(name string) BufferEntryGroup {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()

	group := NewRAMBufferEntryGroup(buffer, name)
	buffer.entryGroupMap[name] = group

	return group
}

// GetEntryGroup returns an entry group
func (buffer *RAMBuffer) GetEntryGroup(name string) BufferEntryGroup {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()

	if group, ok := buffer.entryGroupMap[name]; ok {
		return group
	}

	return nil
}

// GetEntryGroups returns all entry groups
func (buffer *RAMBuffer) GetEntryGroups() []BufferEntryGroup {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()

	groups := []BufferEntryGroup{}

	for _, group := range buffer.entryGroupMap {
		groups = append(groups, group)
	}

	return groups
}

// DeleteEntryGroup deletes an entry group
func (buffer *RAMBuffer) DeleteEntryGroup(name string) {
	buffer.mutex.Lock()

	if group, ok := buffer.entryGroupMap[name]; ok {
		group.deleteAllEntriesWithoutBufferLock()
	}

	delete(buffer.entryGroupMap, name)

	buffer.condition.Broadcast()
	buffer.mutex.Unlock()
}

// DeleteAllEntryGroups deletes all entry groups
func (buffer *RAMBuffer) DeleteAllEntryGroups() {
	buffer.mutex.Lock()

	for _, group := range buffer.entryGroupMap {
		group.deleteAllEntriesWithoutBufferLock()
	}

	buffer.entryGroupMap = map[string]*RAMBufferEntryGroup{}

	buffer.condition.Broadcast()
	buffer.mutex.Unlock()
}
