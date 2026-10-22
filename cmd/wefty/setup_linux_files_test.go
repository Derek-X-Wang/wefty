//go:build unix

package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/systemduser"
)

type linuxRootInfo struct{ fs.FileInfo }

func (i linuxRootInfo) Sys() any { s := *(i.FileInfo.Sys().(*syscall.Stat_t)); s.Uid = 0; return &s }

// All standard Linux paths are mirrored under a temporary root. Nothing reads
// this Mac's tools or systemd configuration, and fake tools are never executed.
func linuxTestFiles(t *testing.T) (systemduser.FileSystem, string) {
	t.Helper()
	mirror, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	virtual := func(path string) bool {
		for _, prefix := range []string{"/usr", "/bin", "/lib", "/etc", "/run"} {
			if path == prefix || strings.HasPrefix(path, prefix+"/") {
				return true
			}
		}
		return path == "/"
	}
	mapped := func(path string) string { return filepath.Join(mirror, strings.TrimPrefix(path, "/")) }
	for _, tool := range []string{"systemctl", "loginctl", "xdg-open"} {
		path := mapped("/usr/bin/" + tool)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("fake tool; never executed"), 0755); err != nil {
			t.Fatal(err)
		}
	}
	metadata := func(path string, stat func(string) (fs.FileInfo, error)) (fs.FileInfo, error) {
		if virtual(path) {
			info, err := stat(mapped(path))
			if err != nil {
				return nil, err
			}
			return linuxRootInfo{info}, nil
		}
		return stat(path)
	}
	files := systemduser.FileSystem{
		Lstat: func(path string) (fs.FileInfo, error) { return metadata(path, os.Lstat) },
		Stat:  func(path string) (fs.FileInfo, error) { return metadata(path, os.Stat) },
		ReadDir: func(path string) ([]fs.DirEntry, error) {
			if virtual(path) {
				return os.ReadDir(mapped(path))
			}
			return os.ReadDir(path)
		},
		EvalSymlinks: func(path string) (string, error) {
			if !virtual(path) {
				return filepath.EvalSymlinks(path)
			}
			real, err := filepath.EvalSymlinks(mapped(path))
			if err != nil {
				return "", err
			}
			rel, err := filepath.Rel(mirror, real)
			if err != nil || !filepath.IsLocal(rel) {
				return "", os.ErrPermission
			}
			return filepath.Clean("/" + filepath.ToSlash(rel)), nil
		},
	}
	return files, mirror
}
