package agent

import (
	"path/filepath"
	"testing"
)

func TestManagedRootDirectoryUsesPlatformStateLocations(t *testing.T) {
	home := filepath.Join(string(filepath.Separator), "home", "wefty")
	tests := []struct {
		name, goos, xdgStateHome, want string
	}{
		{
			name: "darwin application support", goos: "darwin",
			want: filepath.Join(home, "Library", "Application Support", "wefty"),
		},
		{
			name: "linux xdg state", goos: "linux", xdgStateHome: filepath.Join(home, "state"),
			want: filepath.Join(home, "state", "wefty"),
		},
		{
			name: "linux fallback", goos: "linux",
			want: filepath.Join(home, ".local", "state", "wefty"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := managedRootDirectory(test.goos, home, func(name string) string {
				if name == "XDG_STATE_HOME" {
					return test.xdgStateHome
				}
				return ""
			})
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("managed root = %q, want %q", got, test.want)
			}
		})
	}
}

func TestManagedRootDirectoryRejectsRelativeXDGStateHome(t *testing.T) {
	if _, err := managedRootDirectory("linux", "/home/wefty", func(string) string { return "relative" }); err == nil {
		t.Fatal("relative XDG_STATE_HOME was accepted")
	}
}
