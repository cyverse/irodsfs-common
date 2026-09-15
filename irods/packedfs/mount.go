package packedfs

import (
	"sync"
	"time"
)

// MountState is the lifecycle position of one packed directory.
type MountState int

const (
	// MountStateMounting means the archive is being downloaded and extracted.
	MountStateMounting MountState = iota
	// MountStateMounted means the local tree is authoritative and every
	// operation on the directory is served from local disk.
	MountStateMounted
	// MountStatePacking means a snapshot or a release is packing the tree.
	// The tree stays readable and writable while this runs.
	MountStatePacking
	// MountStateUnmounted means the tree has been packed, uploaded and removed.
	MountStateUnmounted
	// MountStateFailed means mounting failed; the directory is not usable.
	MountStateFailed
)

func (s MountState) String() string {
	switch s {
	case MountStateMounting:
		return "MOUNTING"
	case MountStateMounted:
		return "MOUNTED"
	case MountStatePacking:
		return "PACKING"
	case MountStateUnmounted:
		return "UNMOUNTED"
	case MountStateFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// Mount is one packed directory held as a local tree for the life of a session.
type Mount struct {
	// Root is the iRODS path of the directory, such as "/z/home/u/p/.venv".
	Root string
	// LocalPath is the extracted tree on staging disk.
	LocalPath string
	// ArchivePath is the iRODS data object holding the packed form.
	ArchivePath string

	// packMu serializes packing this mount, so a snapshot and a release cannot
	// build two archives from the same tree at once.
	packMu sync.Mutex

	mu sync.RWMutex

	state MountState
	// dirty records that the local tree changed since the last successful
	// upload, so a snapshot that has nothing to do can skip the work.
	dirty bool
	// dirtySeq counts local changes. A pack records it before reading the tree
	// and clears dirty only if it has not moved, so a write that lands while
	// the archive is being built is not lost from the next snapshot.
	dirtySeq uint64
	// reservedSize is the staging quota currently charged for this tree. It is
	// reconciled after every pack, since the tree grows and shrinks in use.
	reservedSize int64
	mountedAt    time.Time
	lastPackedAt time.Time
	// legacyCollection records that iRODS still holds this directory as a
	// collection, which the first successful pack replaces with the archive.
	legacyCollection bool
	mountErr         error
}

// State returns the current lifecycle state.
func (m *Mount) State() MountState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

func (m *Mount) setState(state MountState) {
	m.mu.Lock()
	m.state = state
	m.mu.Unlock()
}

// Usable reports whether operations may be served from this mount. Packing runs
// alongside normal use, so it does not make a mount unusable.
func (m *Mount) Usable() bool {
	state := m.State()
	return state == MountStateMounted || state == MountStatePacking
}

// MarkDirty records a local change that a later snapshot or release must upload.
func (m *Mount) MarkDirty() {
	m.mu.Lock()
	m.dirty = true
	m.dirtySeq++
	m.mu.Unlock()
}

// dirtyMark reads the change counter, to be passed back to markPacked.
func (m *Mount) dirtyMark() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.dirtySeq
}

// markPacked records a successful upload. The tree counts as clean only when no
// change landed since mark was taken.
func (m *Mount) markPacked(mark uint64, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastPackedAt = at
	if m.dirtySeq == mark {
		m.dirty = false
	}
	m.legacyCollection = false
}

// IsDirty reports whether the tree changed since the last successful upload.
func (m *Mount) IsDirty() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.dirty
}

// ReservedSize is the staging quota currently charged for this tree.
func (m *Mount) ReservedSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.reservedSize
}

// LastPackedAt is the time of the last successful upload, zero if never.
func (m *Mount) LastPackedAt() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastPackedAt
}

// MountedAt is the time the tree became usable.
func (m *Mount) MountedAt() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.mountedAt
}

// Status is a point-in-time view of a mount, for monitoring.
type Status struct {
	Root         string    `json:"root"`
	ArchivePath  string    `json:"archive_path"`
	LocalPath    string    `json:"local_path"`
	State        string    `json:"state"`
	Dirty        bool      `json:"dirty"`
	SizeBytes    int64     `json:"size_bytes"`
	MountedAt    time.Time `json:"mounted_at"`
	LastPackedAt time.Time `json:"last_packed_at,omitempty"`
	Error        string    `json:"error,omitempty"`
}

// Status snapshots the mount for the monitoring API.
func (m *Mount) Status() Status {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := Status{
		Root:         m.Root,
		ArchivePath:  m.ArchivePath,
		LocalPath:    m.LocalPath,
		State:        m.state.String(),
		Dirty:        m.dirty,
		SizeBytes:    m.reservedSize,
		MountedAt:    m.mountedAt,
		LastPackedAt: m.lastPackedAt,
	}
	if m.mountErr != nil {
		status.Error = m.mountErr.Error()
	}
	return status
}
