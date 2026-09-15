// Package packedfs stores directories that hold very many small files as a
// single tar data object in iRODS instead of as a collection.
//
// Uploading such a directory file-by-file costs one round trip per file, which
// dominates the transfer for trees like .venv or .git. A packed directory is
// therefore kept entirely on local staging disk while a session uses it, and
// crosses the wire only as one archive: on the first access to the directory
// (unpack), every SnapshotInterval while it is dirty, and when the session is
// released (pack).
package packedfs

import (
	"path"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

// Compression selects the codec wrapped around the tar stream.
type Compression string

const (
	// CompressionNone stores a plain tar. It is the default: the contents of a
	// packed directory are usually already-compressed data (wheels, shared
	// objects, git objects), so a codec costs CPU without saving much, and an
	// uncompressed archive stays seekable.
	CompressionNone Compression = "none"
	CompressionGzip Compression = "gzip"
	CompressionZstd Compression = "zstd"
)

// SnapshotDisabled is the normalized SnapshotInterval that turns periodic
// snapshots off. Any negative configured value becomes this.
const SnapshotDisabled time.Duration = -1

// Default configuration values.
const (
	DefaultSuffix              = ".mount.tar"
	DefaultMaxPackedDirSize    = 5 * 1024 * 1024 * 1024 // 5GB
	DefaultSnapshotInterval    = 30 * time.Minute
	DefaultConcurrentPackLimit = 2
)

// Extension returns the suffix the codec appends to the tar name.
func (c Compression) Extension() string {
	switch c {
	case CompressionGzip:
		return ".gz"
	case CompressionZstd:
		return ".zst"
	default:
		return ""
	}
}

func (c Compression) validate() error {
	switch c {
	case CompressionNone, CompressionGzip, CompressionZstd:
		return nil
	default:
		return errors.Newf("unknown compression %q, expected one of none, gzip, zstd", string(c))
	}
}

// Config describes which directories are packed and how.
type Config struct {
	// Enabled turns the whole feature off when false. A zero Config is disabled.
	Enabled bool

	// Names lists directory base names that are packed, for example ".venv" or
	// ".git". Matching is on the base name at any depth.
	Names []string

	// Suffix is appended to the directory name to form the data object name, so
	// ".venv" is stored as ".venv.mount.tar". Compression adds its own
	// extension on top of this.
	Suffix string

	Compression Compression

	// MaxPackedDirSize caps how much local staging disk one packed directory may
	// occupy. Mounting a larger archive is refused rather than filling staging.
	MaxPackedDirSize int64

	// SnapshotInterval bounds how much work a crash can lose: a dirty mount is
	// packed and uploaded this often even while the session stays open.
	//
	// Zero selects DefaultSnapshotInterval, matching how the surrounding
	// staging configuration treats unset durations. A negative value disables
	// snapshots, leaving session release as the only upload point.
	SnapshotInterval time.Duration

	// ConcurrentPackLimit caps how many directories are packed at once, since
	// packing is CPU and disk bound.
	ConcurrentPackLimit int

	// nameSet is built by ApplyDefaults for lookups.
	nameSet map[string]struct{}
}

// ApplyDefaults fills unset fields with their defaults and builds the lookup
// set. It must be called before the Config is used.
func (c *Config) ApplyDefaults() {
	if c.Suffix == "" {
		c.Suffix = DefaultSuffix
	}
	if c.Compression == "" {
		c.Compression = CompressionNone
	}
	if c.MaxPackedDirSize <= 0 {
		c.MaxPackedDirSize = DefaultMaxPackedDirSize
	}
	if c.SnapshotInterval == 0 {
		c.SnapshotInterval = DefaultSnapshotInterval
	} else if c.SnapshotInterval < 0 {
		// Normalize every negative value to the single "disabled" sentinel.
		c.SnapshotInterval = SnapshotDisabled
	}
	if c.ConcurrentPackLimit <= 0 {
		c.ConcurrentPackLimit = DefaultConcurrentPackLimit
	}

	c.nameSet = make(map[string]struct{}, len(c.Names))
	for _, name := range c.Names {
		name = strings.TrimSpace(name)
		if name != "" {
			c.nameSet[name] = struct{}{}
		}
	}
}

// Validate reports configuration that cannot work, so the service fails at
// startup instead of at the first access to a packed directory.
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if err := c.Compression.validate(); err != nil {
		return err
	}

	if !strings.HasPrefix(c.Suffix, ".") {
		return errors.Newf("packed directory suffix %q must start with a dot", c.Suffix)
	}
	if strings.ContainsRune(c.Suffix, '/') {
		return errors.Newf("packed directory suffix %q must not contain a path separator", c.Suffix)
	}

	if len(c.nameSet) == 0 {
		return errors.New("packed directories are enabled but no directory names are configured")
	}

	for name := range c.nameSet {
		if strings.ContainsRune(name, '/') {
			return errors.Newf("packed directory name %q must be a base name, not a path", name)
		}
		if name == "." || name == ".." {
			return errors.Newf("packed directory name %q is not a valid directory name", name)
		}
		// A name carrying the suffix would make the archive of ".venv" itself
		// look like a packed directory, so the two namespaces must stay apart.
		if strings.HasSuffix(name, c.Suffix) {
			return errors.Newf("packed directory name %q must not end with the archive suffix %q", name, c.Suffix)
		}
	}

	return nil
}

// SnapshotsEnabled reports whether periodic snapshot packing is on.
func (c *Config) SnapshotsEnabled() bool {
	return c.Enabled && c.SnapshotInterval > 0
}

// ArchiveSuffix is the full suffix of an archive data object, including the
// extension the codec adds.
func (c *Config) ArchiveSuffix() string {
	return c.Suffix + c.Compression.Extension()
}

// ArchivePath returns the data object path holding the packed form of the
// directory at root, for example "/z/home/u/p/.venv" -> "/z/home/u/p/.venv.mount.tar".
func (c *Config) ArchivePath(root string) string {
	return root + c.ArchiveSuffix()
}

// IsArchiveName reports whether a base name is the archive of a packed
// directory. Such entries are hidden from listings, because the directory they
// hold is what callers are meant to see.
func (c *Config) IsArchiveName(name string) bool {
	if !c.Enabled {
		return false
	}

	suffix := c.ArchiveSuffix()
	if !strings.HasSuffix(name, suffix) {
		return false
	}

	// Only an archive of a configured name counts, so an unrelated file that
	// happens to end in ".mount.tar" stays visible.
	return c.IsPackedName(strings.TrimSuffix(name, suffix))
}

// IsPackedName reports whether a directory base name is configured as packed.
func (c *Config) IsPackedName(name string) bool {
	if !c.Enabled {
		return false
	}
	_, ok := c.nameSet[name]
	return ok
}

// MatchRoot returns the outermost ancestor of p (p itself included) whose base
// name is a configured packed directory.
//
// The outermost match wins so that a nested packed directory, such as a .git
// inside a .venv, is carried inside its parent's archive rather than being
// packed separately. Packing it separately would require writing a data object
// into a collection that does not exist in iRODS.
func (c *Config) MatchRoot(p string) (string, bool) {
	if !c.Enabled || len(c.nameSet) == 0 {
		return "", false
	}

	p = path.Clean(p)
	if p == "/" || p == "." || p == "" {
		return "", false
	}

	segments := strings.Split(strings.TrimPrefix(p, "/"), "/")
	for i, segment := range segments {
		if _, ok := c.nameSet[segment]; ok {
			return "/" + strings.Join(segments[:i+1], "/"), true
		}
	}

	return "", false
}

// MatchArchiveRoot maps an archive path back to the directory it holds, so a
// listing that returns ".venv.mount.tar" can present ".venv".
func (c *Config) MatchArchiveRoot(archivePath string) (string, bool) {
	if !c.Enabled {
		return "", false
	}

	suffix := c.ArchiveSuffix()
	if !strings.HasSuffix(archivePath, suffix) {
		return "", false
	}

	root := strings.TrimSuffix(archivePath, suffix)
	if !c.IsPackedName(path.Base(root)) {
		return "", false
	}

	return root, true
}
