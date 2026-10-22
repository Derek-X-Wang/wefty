//go:build unix

package systemduser

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

type rootMetadata struct {
	fs.FileInfo
	mode fs.FileMode
	uid  uint32
}

func (i rootMetadata) Mode() fs.FileMode { return i.mode }
func (i rootMetadata) Sys() any          { s := *(i.FileInfo.Sys().(*syscall.Stat_t)); s.Uid = i.uid; return &s }

func TestManagerAllowsTrustedGlobalDropInsAndReruns(t *testing.T) {
	for _, drop := range []string{"/usr/lib/systemd/user/service.d/10-timeout-abort.conf", "/etc/systemd/user/service.d/local.conf", "/run/systemd/user/dev.wefty-.service.d/prefix.conf", `/usr/lib/systemd/user/service.d/a b.conf`} {
		t.Run(drop, func(t *testing.T) {
			root, path := managerFixture(t)
			if err := root.WriteFile(filepath.Base(path), []byte("current"), 0644); err != nil {
				t.Fatal(err)
			}
			file, _ := os.Stat(path)
			directory, _ := os.Stat(filepath.Dir(path))
			metadata := func(p string) (fs.FileInfo, error) {
				if p == drop {
					return rootMetadata{file, 0644, 0}, nil
				}
				if strings.HasPrefix(p, "/usr") || strings.HasPrefix(p, "/etc") || strings.HasPrefix(p, "/run") || p == "/" {
					return rootMetadata{directory, fs.ModeDir | 0755, 0}, nil
				}
				return os.Lstat(p)
			}
			files := FileSystem{Lstat: metadata, Stat: metadata, EvalSymlinks: func(p string) (string, error) { return p, nil }}
			f := &fakeManager{loaded: true, running: true, enabled: true, fragment: path}
			m := Manager{UnitDirectory: filepath.Dir(path), Home: filepath.Dir(path), Files: files, Run: func(ctx context.Context, name string, args ...string) ([]byte, error) {
				b, err := f.run(ctx, name, args...)
				if args[1] == "show" {
					value := drop
					if strings.Contains(drop, " ") {
						value = `"` + drop + `"`
					}
					b = []byte(strings.ReplaceAll(string(b), "DropInPaths=", "DropInPaths="+value))
				}
				return b, err
			}}
			for attempt := 0; attempt < 2; attempt++ {
				state, err := m.State(t.Context(), filepath.Base(path))
				if err != nil || len(state.DropIns) != 1 || state.DropIns[0] != drop {
					t.Fatalf("trusted drop-in rejected: %+v %v", state, err)
				}
				action, err := m.Converge(t.Context(), root, path, filepath.Base(path), []byte("current"))
				if err != nil || action != Unchanged {
					t.Fatalf("trusted rerun: %s %v", action, err)
				}
			}
			if len(f.mutations()) != 0 {
				t.Fatal(f.calls)
			}
		})
	}
}

func TestManagerRefusesUnsafeDropIns(t *testing.T) {
	for _, kind := range []string{"unit-specific-root", "home", "runtime", "user-owner", "group-writable", "world-writable", "writable-parent", "symlink-home", "malformed-list"} {
		t.Run(kind, func(t *testing.T) {
			root, path := managerFixture(t)
			if err := root.WriteFile(filepath.Base(path), []byte("current"), 0644); err != nil {
				t.Fatal(err)
			}
			file, _ := os.Stat(path)
			directory, _ := os.Stat(filepath.Dir(path))
			drop := "/usr/lib/systemd/user/service.d/policy.conf"
			switch kind {
			case "unit-specific-root":
				drop = "/etc/systemd/user/dev.wefty.l1.service.d/override.conf"
			case "home":
				drop = filepath.Join(filepath.Dir(path), "service.d", "policy.conf")
			case "runtime":
				drop = "/run/user/1001/systemd/user/service.d/policy.conf"
			}
			metadata := func(p string) (fs.FileInfo, error) {
				if p == drop {
					mode, uid := fs.FileMode(0644), uint32(0)
					if kind == "user-owner" {
						uid = 1001
					}
					if kind == "group-writable" {
						mode = 0664
					}
					if kind == "world-writable" {
						mode = 0666
					}
					return rootMetadata{file, mode, uid}, nil
				}
				if strings.HasPrefix(p, "/usr") || strings.HasPrefix(p, "/etc") || strings.HasPrefix(p, "/run") || p == "/" {
					mode := fs.ModeDir | 0755
					if kind == "writable-parent" && p == filepath.Dir(drop) {
						mode = fs.ModeDir | 0777
					}
					return rootMetadata{directory, mode, 0}, nil
				}
				return os.Lstat(p)
			}
			files := FileSystem{Lstat: metadata, Stat: metadata, EvalSymlinks: func(p string) (string, error) {
				if kind == "symlink-home" {
					return filepath.Join(filepath.Dir(path), "policy.conf"), nil
				}
				return p, nil
			}}
			f := &fakeManager{loaded: true, running: true, enabled: true, fragment: path}
			m := Manager{Files: files, Home: filepath.Dir(path), RuntimeDir: "/run/user/1001", Run: func(ctx context.Context, name string, args ...string) ([]byte, error) {
				b, err := f.run(ctx, name, args...)
				if args[1] == "show" {
					value := drop
					if kind == "malformed-list" {
						value = `"unterminated`
					}
					b = []byte(strings.ReplaceAll(string(b), "DropInPaths=", "DropInPaths="+value))
				}
				return b, err
			}}
			if _, err := m.Converge(t.Context(), root, path, filepath.Base(path), []byte("changed")); err == nil {
				t.Fatal("unsafe drop-in accepted")
			}
			if len(f.mutations()) != 0 {
				t.Fatal("unsafe definition was mutated", f.calls)
			}
		})
	}
}
