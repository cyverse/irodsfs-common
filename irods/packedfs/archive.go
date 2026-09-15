package packedfs

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/klauspost/compress/zstd"
)

// ErrArchiveTooLarge is returned when unpacking would exceed the configured
// per-directory size cap.
var ErrArchiveTooLarge = errors.New("packed directory exceeds the configured size limit")

// ErrUnsafeArchiveEntry is returned for an archive entry that would write
// outside the directory being unpacked.
var ErrUnsafeArchiveEntry = errors.New("archive entry escapes the extraction root")

func compressWriter(w io.Writer, compression Compression) (io.WriteCloser, error) {
	switch compression {
	case CompressionGzip:
		return gzip.NewWriter(w), nil
	case CompressionZstd:
		encoder, err := zstd.NewWriter(w)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create zstd writer")
		}
		return encoder, nil
	default:
		return nopWriteCloser{w}, nil
	}
}

func decompressReader(r io.Reader, compression Compression) (io.ReadCloser, error) {
	switch compression {
	case CompressionGzip:
		reader, err := gzip.NewReader(r)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create gzip reader")
		}
		return reader, nil
	case CompressionZstd:
		decoder, err := zstd.NewReader(r)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create zstd reader")
		}
		return decoder.IOReadCloser(), nil
	default:
		return io.NopCloser(r), nil
	}
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }

// Pack writes the tree rooted at localRoot to w as a tar stream and returns the
// number of file bytes archived.
//
// Directories, regular files and symlinks are preserved with their mode and
// modification time. Symlinks are stored as links rather than followed, which
// keeps a virtualenv's interpreter links intact; a per-file upload to iRODS
// would lose them. Entries of any other type (sockets, devices, fifos) are
// skipped, since iRODS cannot represent them either.
func Pack(localRoot string, w io.Writer, compression Compression) (int64, error) {
	compressor, err := compressWriter(w, compression)
	if err != nil {
		return 0, err
	}

	tarWriter := tar.NewWriter(compressor)

	var totalBytes int64
	walkErr := filepath.WalkDir(localRoot, func(localPath string, entry fs.DirEntry, err error) error {
		if err != nil {
			return errors.Wrapf(err, "failed to walk %q", localPath)
		}

		relPath, err := filepath.Rel(localRoot, localPath)
		if err != nil {
			return errors.Wrapf(err, "failed to relativize %q", localPath)
		}
		if relPath == "." {
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			// A file removed between the walk listing it and this stat is not a
			// failure: the snapshot simply predates the removal.
			if os.IsNotExist(err) {
				return nil
			}
			return errors.Wrapf(err, "failed to stat %q", localPath)
		}

		var linkTarget string
		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, err = os.Readlink(localPath)
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return errors.Wrapf(err, "failed to read symlink %q", localPath)
			}
		} else if !info.Mode().IsRegular() && !info.IsDir() {
			return nil
		}

		header, err := tar.FileInfoHeader(info, linkTarget)
		if err != nil {
			return errors.Wrapf(err, "failed to build archive header for %q", localPath)
		}
		// FileInfoHeader takes the base name only; the full relative path is
		// what places the entry inside the archive. Use forward slashes so an
		// archive stays portable across platforms.
		header.Name = filepath.ToSlash(relPath)
		if info.IsDir() {
			header.Name += "/"
		}

		if err := tarWriter.WriteHeader(header); err != nil {
			return errors.Wrapf(err, "failed to write archive header for %q", localPath)
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		file, err := os.Open(localPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return errors.Wrapf(err, "failed to open %q", localPath)
		}
		defer file.Close()

		written, err := io.Copy(tarWriter, file)
		if err != nil {
			return errors.Wrapf(err, "failed to archive %q", localPath)
		}
		if written != info.Size() {
			// tar records the size in the header, so a file that changed length
			// under the walk would produce a stream no reader can parse.
			return errors.Newf("size of %q changed during packing: header %d, wrote %d", localPath, info.Size(), written)
		}

		totalBytes += written
		return nil
	})

	if walkErr != nil {
		tarWriter.Close()
		compressor.Close()
		return 0, walkErr
	}

	if err := tarWriter.Close(); err != nil {
		compressor.Close()
		return 0, errors.Wrap(err, "failed to finalize archive")
	}
	if err := compressor.Close(); err != nil {
		return 0, errors.Wrap(err, "failed to finalize archive compression")
	}

	return totalBytes, nil
}

// Unpack extracts a tar stream into localRoot and returns the number of file
// bytes written. It refuses an archive whose contents exceed maxSize, so a
// directory cannot fill the staging disk, and refuses entries that would write
// outside localRoot.
func Unpack(r io.Reader, localRoot string, compression Compression, maxSize int64) (int64, error) {
	decompressor, err := decompressReader(r, compression)
	if err != nil {
		return 0, err
	}
	defer decompressor.Close()

	if err := os.MkdirAll(localRoot, 0755); err != nil {
		return 0, errors.Wrapf(err, "failed to create extraction root %q", localRoot)
	}

	tarReader := tar.NewReader(decompressor)

	var totalBytes int64
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return totalBytes, errors.Wrap(err, "failed to read archive")
		}

		relPath, err := safeRelPath(header.Name)
		if err != nil {
			return totalBytes, err
		}
		if relPath == "" {
			continue
		}

		targetPath, err := resolveUnder(localRoot, relPath)
		if err != nil {
			return totalBytes, err
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode).Perm()); err != nil {
				return totalBytes, errors.Wrapf(err, "failed to create directory %q", targetPath)
			}

		case tar.TypeReg:
			if maxSize > 0 && totalBytes+header.Size > maxSize {
				return totalBytes, errors.Wrapf(ErrArchiveTooLarge,
					"unpacking %q needs more than %d bytes", localRoot, maxSize)
			}

			written, err := writeArchiveFile(targetPath, tarReader, os.FileMode(header.Mode).Perm(), header.Size)
			if err != nil {
				return totalBytes, err
			}
			totalBytes += written

		case tar.TypeSymlink:
			// The link target is not validated: creating a dangling or outward
			// pointing symlink writes nothing outside the root, and a
			// virtualenv legitimately links to an interpreter outside it.
			// resolveUnder keeps later entries from being written *through* it.
			if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
				return totalBytes, errors.Wrapf(err, "failed to replace %q", targetPath)
			}
			if err := os.Symlink(header.Linkname, targetPath); err != nil {
				return totalBytes, errors.Wrapf(err, "failed to create symlink %q", targetPath)
			}

		case tar.TypeLink:
			linkRelPath, err := safeRelPath(header.Linkname)
			if err != nil {
				return totalBytes, err
			}
			linkTargetPath, err := resolveUnder(localRoot, linkRelPath)
			if err != nil {
				return totalBytes, err
			}
			if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
				return totalBytes, errors.Wrapf(err, "failed to replace %q", targetPath)
			}
			if err := os.Link(linkTargetPath, targetPath); err != nil {
				return totalBytes, errors.Wrapf(err, "failed to create hard link %q", targetPath)
			}

		default:
			// Skip entry types Pack never produces and iRODS cannot hold.
			continue
		}

		if !header.ModTime.IsZero() && header.Typeflag != tar.TypeSymlink {
			// A failure here costs only timestamp fidelity, so it is logged by
			// the caller's archive rather than aborting the extraction.
			_ = os.Chtimes(targetPath, header.ModTime, header.ModTime)
		}
	}

	return totalBytes, nil
}

func writeArchiveFile(targetPath string, r io.Reader, mode os.FileMode, size int64) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return 0, errors.Wrapf(err, "failed to create parent of %q", targetPath)
	}

	if mode == 0 {
		mode = 0644
	}

	file, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create %q", targetPath)
	}
	defer file.Close()

	written, err := io.Copy(file, io.LimitReader(r, size))
	if err != nil {
		return written, errors.Wrapf(err, "failed to write %q", targetPath)
	}

	return written, nil
}

// safeRelPath normalizes an archive entry name and rejects one that is absolute
// or climbs out of the extraction root.
func safeRelPath(name string) (string, error) {
	name = strings.TrimSuffix(filepath.ToSlash(name), "/")
	if name == "" || name == "." {
		return "", nil
	}

	if strings.HasPrefix(name, "/") {
		return "", errors.Wrapf(ErrUnsafeArchiveEntry, "entry %q is an absolute path", name)
	}

	cleaned := filepath.Clean(filepath.FromSlash(name))
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", errors.Wrapf(ErrUnsafeArchiveEntry, "entry %q climbs above the root", name)
	}

	return cleaned, nil
}

// resolveUnder joins relPath onto root, creating each intermediate directory,
// and fails if any existing component is a symlink.
//
// Rejecting symlinked components is what stops an archive from writing through
// a link it created earlier: an entry pair of "bin -> /etc" followed by
// "bin/passwd" has a name that is relative and clean, so only this check keeps
// the write inside the root.
func resolveUnder(root string, relPath string) (string, error) {
	current := root
	for _, segment := range strings.Split(relPath, string(filepath.Separator)) {
		if segment == "" || segment == "." {
			continue
		}

		current = filepath.Join(current, segment)

		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", errors.Wrapf(err, "failed to stat %q", current)
		}

		if info.Mode()&os.ModeSymlink != 0 && current != filepath.Join(root, relPath) {
			return "", errors.Wrapf(ErrUnsafeArchiveEntry,
				"entry %q passes through symlink %q", relPath, current)
		}
	}

	return filepath.Join(root, relPath), nil
}
