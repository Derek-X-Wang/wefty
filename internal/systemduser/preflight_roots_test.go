package systemduser

import (
	"io/fs"
	"path/filepath"
	"testing"
)

// Overrides in the XDG user-unit folders must be found before enrollment, not
// only after the unit loads and the role is committed.
func TestPreflightScansXDGUserUnitFolders(t *testing.T) {
	read := map[string]bool{}
	m := Manager{UnitDirectory: "/home/owner/.config/systemd/user", Home: "/home/owner", Files: FileSystem{
		ReadDir: func(path string) ([]fs.DirEntry, error) {
			read[path] = true
			return nil, fs.ErrNotExist
		},
	}}
	if err := m.Preflight("dev.wefty.l1.service"); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{"/etc/xdg/systemd/user", "/usr/local/share/systemd/user", "/usr/share/systemd/user"} {
		if !read[filepath.Join(root, "dev.wefty.l1.service.d")] {
			t.Errorf("preflight did not inspect %s", root)
		}
	}
}
