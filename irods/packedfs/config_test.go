package packedfs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestConfig(names ...string) *Config {
	if len(names) == 0 {
		names = []string{".venv", ".git"}
	}
	config := &Config{Enabled: true, Names: names}
	config.ApplyDefaults()
	return config
}

func TestApplyDefaults(t *testing.T) {
	config := &Config{Enabled: true, Names: []string{".venv", "  ", ".git"}}
	config.ApplyDefaults()

	assert.Equal(t, DefaultSuffix, config.Suffix)
	assert.Equal(t, CompressionNone, config.Compression)
	assert.Equal(t, int64(DefaultMaxPackedDirSize), config.MaxPackedDirSize)
	assert.Equal(t, DefaultSnapshotInterval, config.SnapshotInterval)
	assert.Equal(t, DefaultConcurrentPackLimit, config.ConcurrentPackLimit)
	assert.Len(t, config.nameSet, 2, "blank names are dropped")
}

func TestSnapshotIntervalModes(t *testing.T) {
	// Zero means "unset", matching how the staging config treats durations.
	unset := &Config{Enabled: true, Names: []string{".venv"}}
	unset.ApplyDefaults()
	assert.Equal(t, DefaultSnapshotInterval, unset.SnapshotInterval)
	assert.True(t, unset.SnapshotsEnabled())

	explicit := &Config{Enabled: true, Names: []string{".venv"}, SnapshotInterval: time.Minute}
	explicit.ApplyDefaults()
	assert.Equal(t, time.Minute, explicit.SnapshotInterval)
	assert.True(t, explicit.SnapshotsEnabled())

	// A negative value is the only way to turn snapshots off.
	disabled := &Config{Enabled: true, Names: []string{".venv"}, SnapshotInterval: -time.Second}
	disabled.ApplyDefaults()
	assert.Equal(t, SnapshotDisabled, disabled.SnapshotInterval)
	assert.False(t, disabled.SnapshotsEnabled())
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr string
	}{
		{
			name:   "disabled config is always valid",
			config: &Config{Enabled: false},
		},
		{
			name:   "valid",
			config: &Config{Enabled: true, Names: []string{".venv"}},
		},
		{
			name:    "no names",
			config:  &Config{Enabled: true},
			wantErr: "no directory names are configured",
		},
		{
			name:    "name with separator",
			config:  &Config{Enabled: true, Names: []string{"a/b"}},
			wantErr: "must be a base name",
		},
		{
			name:    "dot name",
			config:  &Config{Enabled: true, Names: []string{".."}},
			wantErr: "not a valid directory name",
		},
		{
			name:    "name carrying the suffix",
			config:  &Config{Enabled: true, Names: []string{".venv.packedfs.tar"}},
			wantErr: "must not end with the archive suffix",
		},
		{
			name:    "suffix without dot",
			config:  &Config{Enabled: true, Names: []string{".venv"}, Suffix: "mounttar"},
			wantErr: "must start with a dot",
		},
		{
			name:    "suffix with separator",
			config:  &Config{Enabled: true, Names: []string{".venv"}, Suffix: ".mount/tar"},
			wantErr: "must not contain a path separator",
		},
		{
			name:    "unknown compression",
			config:  &Config{Enabled: true, Names: []string{".venv"}, Compression: "lz4"},
			wantErr: "unknown compression",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.config.ApplyDefaults()
			err := test.config.Validate()
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantErr)
		})
	}
}

func TestMatchRoot(t *testing.T) {
	config := newTestConfig()

	tests := []struct {
		path     string
		wantRoot string
		wantOK   bool
	}{
		{"/z/home/u/proj/.venv", "/z/home/u/proj/.venv", true},
		{"/z/home/u/proj/.venv/lib/python3.12/site-packages/x.py", "/z/home/u/proj/.venv", true},
		{"/z/home/u/proj/src/main.go", "", false},
		{"/z/home/u/proj", "", false},
		{"/", "", false},
		{"", "", false},
		// A packed directory nested inside another belongs to the outer
		// archive, so the outer root wins.
		{"/z/home/u/.venv/src/.git/config", "/z/home/u/.venv", true},
		// A name that merely contains a configured name is not a match.
		{"/z/home/u/my.venv/lib", "", false},
		{"/z/home/u/.venvs/lib", "", false},
	}

	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			root, ok := config.MatchRoot(test.path)
			assert.Equal(t, test.wantOK, ok)
			assert.Equal(t, test.wantRoot, root)
		})
	}
}

func TestMatchRootDisabled(t *testing.T) {
	config := &Config{Enabled: false, Names: []string{".venv"}}
	config.ApplyDefaults()

	_, ok := config.MatchRoot("/z/home/u/.venv/lib")
	assert.False(t, ok)
}

func TestArchivePathAndName(t *testing.T) {
	config := newTestConfig()

	assert.Equal(t, "/z/u/p/.venv.packedfs.tar", config.ArchivePath("/z/u/p/.venv"))
	assert.True(t, config.IsArchiveName(".venv.packedfs.tar"))
	assert.True(t, config.IsArchiveName(".git.packedfs.tar"))
	// Not a configured directory name, so it is an ordinary user file.
	assert.False(t, config.IsArchiveName(".cargo.packedfs.tar"))
	assert.False(t, config.IsArchiveName("notes.tar"))
	assert.False(t, config.IsArchiveName(".venv"))

	root, ok := config.MatchArchiveRoot("/z/u/p/.venv.packedfs.tar")
	require.True(t, ok)
	assert.Equal(t, "/z/u/p/.venv", root)

	_, ok = config.MatchArchiveRoot("/z/u/p/data.tar")
	assert.False(t, ok)
}

func TestArchiveSuffixFollowsCompression(t *testing.T) {
	config := &Config{Enabled: true, Names: []string{".venv"}, Compression: CompressionGzip}
	config.ApplyDefaults()

	assert.Equal(t, ".packedfs.tar.gz", config.ArchiveSuffix())
	assert.Equal(t, "/p/.venv.packedfs.tar.gz", config.ArchivePath("/p/.venv"))
	assert.True(t, config.IsArchiveName(".venv.packedfs.tar.gz"))
	// With gzip configured, a plain tar is not this config's archive.
	assert.False(t, config.IsArchiveName(".venv.packedfs.tar"))
}
