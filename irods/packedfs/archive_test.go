package packedfs

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, path string, content string, mode os.FileMode) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
	require.NoError(t, os.WriteFile(path, []byte(content), mode))
}

// buildSampleTree lays out a tree shaped like a virtualenv: nested directories,
// an executable, a symlink to an interpreter outside the tree, and an empty
// directory.
func buildSampleTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writeFile(t, filepath.Join(root, "pyvenv.cfg"), "home = /usr/bin\n", 0644)
	writeFile(t, filepath.Join(root, "bin", "activate"), "export VIRTUAL_ENV\n", 0755)
	writeFile(t, filepath.Join(root, "lib", "python3.12", "site-packages", "pkg", "__init__.py"), "x = 1\n", 0644)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "share", "empty"), 0755))
	require.NoError(t, os.Symlink("/usr/bin/python3.12", filepath.Join(root, "bin", "python")))

	return root
}

func TestPackUnpackRoundTrip(t *testing.T) {
	for _, compression := range []Compression{CompressionNone, CompressionGzip, CompressionZstd} {
		t.Run(string(compression), func(t *testing.T) {
			source := buildSampleTree(t)

			var buffer bytes.Buffer
			written, err := Pack(source, &buffer, compression)
			require.NoError(t, err)
			assert.Positive(t, written)

			target := filepath.Join(t.TempDir(), "extracted")
			restored, err := Unpack(&buffer, target, compression, 0)
			require.NoError(t, err)
			assert.Equal(t, written, restored)

			// Regular files keep content and mode.
			content, err := os.ReadFile(filepath.Join(target, "lib", "python3.12", "site-packages", "pkg", "__init__.py"))
			require.NoError(t, err)
			assert.Equal(t, "x = 1\n", string(content))

			info, err := os.Stat(filepath.Join(target, "bin", "activate"))
			require.NoError(t, err)
			assert.Equal(t, os.FileMode(0755), info.Mode().Perm(), "executable bit survives the round trip")

			// The symlink is preserved as a link, not followed. A per-file
			// upload to iRODS would lose this.
			linkInfo, err := os.Lstat(filepath.Join(target, "bin", "python"))
			require.NoError(t, err)
			require.NotZero(t, linkInfo.Mode()&os.ModeSymlink, "symlink stays a symlink")
			target2, err := os.Readlink(filepath.Join(target, "bin", "python"))
			require.NoError(t, err)
			assert.Equal(t, "/usr/bin/python3.12", target2)

			// Empty directories survive.
			emptyInfo, err := os.Stat(filepath.Join(target, "share", "empty"))
			require.NoError(t, err)
			assert.True(t, emptyInfo.IsDir())
		})
	}
}

func TestPackPreservesModTime(t *testing.T) {
	source := t.TempDir()
	filePath := filepath.Join(source, "data.bin")
	writeFile(t, filePath, "payload", 0644)

	stamp := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
	require.NoError(t, os.Chtimes(filePath, stamp, stamp))

	var buffer bytes.Buffer
	_, err := Pack(source, &buffer, CompressionNone)
	require.NoError(t, err)

	target := filepath.Join(t.TempDir(), "extracted")
	_, err = Unpack(&buffer, target, CompressionNone, 0)
	require.NoError(t, err)

	info, err := os.Stat(filepath.Join(target, "data.bin"))
	require.NoError(t, err)
	assert.Equal(t, stamp.Unix(), info.ModTime().Unix())
}

func TestPackEmptyDirectory(t *testing.T) {
	source := t.TempDir()

	var buffer bytes.Buffer
	written, err := Pack(source, &buffer, CompressionNone)
	require.NoError(t, err)
	assert.Zero(t, written)

	target := filepath.Join(t.TempDir(), "extracted")
	_, err = Unpack(&buffer, target, CompressionNone, 0)
	require.NoError(t, err)

	entries, err := os.ReadDir(target)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestUnpackEnforcesSizeLimit(t *testing.T) {
	source := t.TempDir()
	writeFile(t, filepath.Join(source, "big.bin"), string(bytes.Repeat([]byte("a"), 4096)), 0644)

	var buffer bytes.Buffer
	_, err := Pack(source, &buffer, CompressionNone)
	require.NoError(t, err)

	target := filepath.Join(t.TempDir(), "extracted")
	_, err = Unpack(&buffer, target, CompressionNone, 1024)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrArchiveTooLarge), "got %v", err)
}

// tarball builds an archive from raw headers so the extractor can be tested
// against entries Pack would never produce.
func tarball(t *testing.T, build func(w *tar.Writer)) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	build(writer)
	require.NoError(t, writer.Close())
	return &buffer
}

func TestUnpackRejectsAbsolutePath(t *testing.T) {
	buffer := tarball(t, func(w *tar.Writer) {
		body := "pwned"
		require.NoError(t, w.WriteHeader(&tar.Header{
			Name: "/etc/passwd", Mode: 0644, Size: int64(len(body)), Typeflag: tar.TypeReg,
		}))
		_, err := w.Write([]byte(body))
		require.NoError(t, err)
	})

	_, err := Unpack(buffer, filepath.Join(t.TempDir(), "extracted"), CompressionNone, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnsafeArchiveEntry), "got %v", err)
}

func TestUnpackRejectsParentTraversal(t *testing.T) {
	buffer := tarball(t, func(w *tar.Writer) {
		body := "pwned"
		require.NoError(t, w.WriteHeader(&tar.Header{
			Name: "../../escaped.txt", Mode: 0644, Size: int64(len(body)), Typeflag: tar.TypeReg,
		}))
		_, err := w.Write([]byte(body))
		require.NoError(t, err)
	})

	root := t.TempDir()
	_, err := Unpack(buffer, filepath.Join(root, "extracted"), CompressionNone, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnsafeArchiveEntry), "got %v", err)

	_, statErr := os.Stat(filepath.Join(root, "escaped.txt"))
	assert.True(t, os.IsNotExist(statErr), "nothing was written outside the root")
}

// An archive can create a symlink pointing anywhere and then write through it.
// Only the component check in resolveUnder stops the second entry.
func TestUnpackRejectsWriteThroughSymlink(t *testing.T) {
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "victim.txt"), []byte("original"), 0644))

	buffer := tarball(t, func(w *tar.Writer) {
		require.NoError(t, w.WriteHeader(&tar.Header{
			Name: "escape", Linkname: outside, Mode: 0777, Typeflag: tar.TypeSymlink,
		}))
		body := "pwned"
		require.NoError(t, w.WriteHeader(&tar.Header{
			Name: "escape/victim.txt", Mode: 0644, Size: int64(len(body)), Typeflag: tar.TypeReg,
		}))
		_, err := w.Write([]byte(body))
		require.NoError(t, err)
	})

	_, err := Unpack(buffer, filepath.Join(t.TempDir(), "extracted"), CompressionNone, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnsafeArchiveEntry), "got %v", err)

	content, readErr := os.ReadFile(filepath.Join(outside, "victim.txt"))
	require.NoError(t, readErr)
	assert.Equal(t, "original", string(content), "the file outside the root is untouched")
}

func TestUnpackSkipsUnsupportedEntryTypes(t *testing.T) {
	buffer := tarball(t, func(w *tar.Writer) {
		require.NoError(t, w.WriteHeader(&tar.Header{
			Name: "dev/null", Mode: 0666, Typeflag: tar.TypeChar, Devmajor: 1, Devminor: 3,
		}))
		body := "keep"
		require.NoError(t, w.WriteHeader(&tar.Header{
			Name: "ok.txt", Mode: 0644, Size: int64(len(body)), Typeflag: tar.TypeReg,
		}))
		_, err := w.Write([]byte(body))
		require.NoError(t, err)
	})

	target := filepath.Join(t.TempDir(), "extracted")
	_, err := Unpack(buffer, target, CompressionNone, 0)
	require.NoError(t, err, "an unsupported entry is skipped, not fatal")

	content, err := os.ReadFile(filepath.Join(target, "ok.txt"))
	require.NoError(t, err)
	assert.Equal(t, "keep", string(content))

	_, statErr := os.Stat(filepath.Join(target, "dev", "null"))
	assert.True(t, os.IsNotExist(statErr))
}

func TestUnpackOverwritesExistingTree(t *testing.T) {
	source := t.TempDir()
	writeFile(t, filepath.Join(source, "a.txt"), "new", 0644)

	var buffer bytes.Buffer
	_, err := Pack(source, &buffer, CompressionNone)
	require.NoError(t, err)

	target := t.TempDir()
	writeFile(t, filepath.Join(target, "a.txt"), "stale", 0644)

	_, err = Unpack(&buffer, target, CompressionNone, 0)
	require.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(target, "a.txt"))
	require.NoError(t, err)
	assert.Equal(t, "new", string(content))
}
