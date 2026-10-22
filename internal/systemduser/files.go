package systemduser

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// FileSystem injects read-only metadata and directory inspection. Zero uses the
// operating system; tests can model root-owned Linux files without root access.
type FileSystem struct {
	Lstat        func(string) (fs.FileInfo, error)
	Stat         func(string) (fs.FileInfo, error)
	ReadDir      func(string) ([]fs.DirEntry, error)
	EvalSymlinks func(string) (string, error)
}

func (f FileSystem) lstat(path string) (fs.FileInfo, error) {
	if f.Lstat != nil {
		return f.Lstat(path)
	}
	return os.Lstat(path)
}
func (f FileSystem) stat(path string) (fs.FileInfo, error) {
	if f.Stat != nil {
		return f.Stat(path)
	}
	return os.Stat(path)
}
func (f FileSystem) readDir(path string) ([]fs.DirEntry, error) {
	if f.ReadDir != nil {
		return f.ReadDir(path)
	}
	return os.ReadDir(path)
}
func (f FileSystem) resolve(path string) (string, error) {
	if f.EvalSymlinks != nil {
		return f.EvalSymlinks(path)
	}
	return filepath.EvalSymlinks(path)
}

// ResolveTool never searches PATH. Symlinks have no meaningful permission
// bits; both their owner and their resolved regular target must be trusted.
func ResolveTool(name string, files FileSystem) (string, error) {
	switch name {
	case "systemctl", "loginctl", "xdg-open":
	default:
		return "", errors.New("unknown Linux user-service tool")
	}
	for _, folder := range []string{"/usr/bin", "/bin"} {
		path := filepath.Join(folder, name)
		if !files.protectedFile(path) {
			continue
		}
		info, err := files.stat(path)
		if err == nil && info.Mode().Perm()&0111 != 0 {
			return path, nil
		}
	}
	return "", fmt.Errorf("%s not found at /usr/bin or /bin (missing or unsafe executable)", name)
}
func (f FileSystem) protectedFile(path string) bool {
	info, err := f.stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	resolved, err := f.resolve(path)
	if err != nil || !filepath.IsAbs(resolved) {
		return false
	}
	for _, spelling := range []string{path, resolved} {
		for p := spelling; ; p = filepath.Dir(p) {
			link, err := f.lstat(p)
			if err != nil || !rootOwned(link) {
				return false
			}
			if link.Mode()&os.ModeSymlink == 0 && link.Mode().Perm()&0022 != 0 {
				return false
			}
			target, err := f.stat(p)
			if err != nil || !rootOwned(target) || target.Mode().Perm()&0022 != 0 {
				return false
			}
			if p != spelling && !target.IsDir() {
				return false
			}
			if p == filepath.Dir(p) {
				break
			}
		}
	}
	return true
}
