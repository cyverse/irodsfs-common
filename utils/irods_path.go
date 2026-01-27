package utils

import (
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

// GetIRODSZone returns the zone of the iRODS path
func GetIRODSZone(p string) (string, error) {
	if len(p) < 1 {
		return "", errors.Newf("failed to extract Zone from path %q", p)
	}

	if p[0] != '/' {
		return "", errors.Newf("failed to extract Zone from path %q", p)
	}

	parts := strings.Split(p[1:], "/")
	if len(parts) >= 1 {
		if len(parts[0]) > 0 {
			return parts[0], nil
		}
	}
	return "", errors.Newf("failed to extract Zone from path %q", p)
}

// GetPathDepth returns depth of the path
// "/" returns 0
// "abc" returns -1
// "/abc" returns 0
// "/a/b" returns 1
// "/a/b/c" returns 2
func GetPathDepth(p string) int {
	if !strings.HasPrefix(p, "/") {
		return -1
	}

	cleanPath := path.Clean(p)

	if cleanPath == "/" {
		return 0
	}

	pArr := strings.Split(p[1:], "/")
	return len(pArr) - 1
}

// GetParentDirs returns all parent dirs
func GetParentDirs(p string) []string {
	parents := []string{}

	if p == "/" {
		return parents
	}

	curPath := p
	for len(curPath) > 0 && curPath != "/" {
		curDir := path.Dir(curPath)
		if len(curDir) > 0 {
			parents = append(parents, curDir)
		}

		curPath = curDir
	}

	// sort
	sort.Slice(parents, func(i int, j int) bool {
		return len(parents[i]) < len(parents[j])
	})

	return parents
}

// GetRelativePath returns relative path
func GetRelativePath(p1 string, p2 string) (string, error) {
	p1s := filepath.FromSlash(p1)
	p2s := filepath.FromSlash(p2)

	rel, err := filepath.Rel(p1s, p2s)
	if err != nil {
		return "", errors.Errorf("failed to get relative path from %q to %q: %w", p1, p2, err)
	}

	rels := filepath.ToSlash(rel)

	return rels, nil
}
