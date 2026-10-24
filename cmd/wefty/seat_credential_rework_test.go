package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestSeatCredentialStateRoots(t *testing.T) {
	for _, mode := range []string{"xdg", "home", "override"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			t.Setenv("HOME", root)
			t.Setenv("TMPDIR", t.TempDir())
			t.Setenv("XDG_STATE_HOME", "")
			t.Setenv("WEFTY_SEAT_STATE_ROOT", "")
			want := filepath.Join(root, ".local", "state", "wefty", "seats")
			if mode == "xdg" {
				t.Setenv("XDG_STATE_HOME", root)
				want = filepath.Join(root, "wefty", "seats")
			}
			if mode == "override" {
				want = filepath.Join(root, "override")
				t.Setenv("WEFTY_SEAT_STATE_ROOT", want)
			}
			id := "seat_00000000000000000000000000000901"
			path, err := storeSeatCredential(id, "secret")
			if err != nil {
				t.Fatal(err)
			}
			if filepath.Dir(path) != want {
				t.Fatalf("credential root=%s want=%s", filepath.Dir(path), want)
			}
			input := l1.SeatCallInput{SeatID: id, Folder: "/workspace", Network: "none", Form: "any"}
			if err := storeSeatCallInput(input); err != nil {
				t.Fatal(err)
			}
			got, err := loadSeatCallInput(id)
			if err != nil || got != input {
				t.Fatalf("receipt: %+v %v", got, err)
			}
			info, err := os.Stat(want)
			if err != nil || info.Mode().Perm() != 0700 {
				t.Fatalf("root: %v %v", info, err)
			}
		})
	}
}

func TestSeatCredentialRejectsForeignOwner(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WEFTY_SEAT_STATE_ROOT", root)
	old := seatCredentialUID
	seatCredentialUID = func() int { return os.Getuid() + 1 }
	t.Cleanup(func() { seatCredentialUID = old })
	if _, err := storeSeatCredential("seat_00000000000000000000000000000902", "secret"); err == nil {
		t.Fatal("accepted a directory owned by another uid")
	}
}

func TestSeatCredentialMacFallbackAndReadSearch(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS fallback only")
	}
	state, tmp := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	t.Setenv("TMPDIR", tmp)
	t.Setenv("WEFTY_SEAT_STATE_ROOT", "")
	if err := os.Chmod(state, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(state, 0700) })
	id := "seat_00000000000000000000000000000904"
	input := l1.SeatCallInput{SeatID: id, Folder: "/workspace", Form: "any", Network: "none"}
	if err := storeSeatCallInput(input); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(tmp, "wefty-seats", id+".request.json")); err != nil {
		t.Fatalf("fallback receipt: %v", err)
	}
	// A later harness can write the default root. Readers must still search the
	// older fallback even when the default directory is now accessible.
	if err := os.Chmod(state, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(state, "wefty", "seats"), 0700); err != nil {
		t.Fatal(err)
	}
	got, err := loadSeatCallInput(id)
	if err != nil || got != input {
		t.Fatalf("cross-harness read: %+v %v", got, err)
	}
	blocked := filepath.Join(state, "blocked")
	if err := os.Mkdir(blocked, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(blocked, 0700) })
	t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(blocked, "explicit"))
	if _, err := storeSeatCredential(id, "secret"); !os.IsPermission(err) {
		t.Fatalf("override fell back: %v", err)
	}
}
