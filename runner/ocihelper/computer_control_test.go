package ocihelper

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
)

func TestComputerControlStateIsFreshExactReadableAndAtomic(t *testing.T) {
	logDirectory := filepath.Join(t.TempDir(), "wefty-log-segments-attempt")
	mounted := false
	controlDirectory, err := prepareComputerControlDirectory(logDirectory, func(string) error {
		mounted = true
		return nil
	}, func(string) error {
		mounted = false
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !mounted {
		t.Fatal("Computer control directory was not mounted before publication")
	}
	statePath := filepath.Join(controlDirectory, computerControlFilename)
	assertComputerControlFile(t, statePath, computerControlFalse)

	stop := make(chan struct{})
	readerErr := make(chan error, 1)
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			payload, readErr := os.ReadFile(statePath)
			if readErr != nil {
				readerErr <- readErr
				return
			}
			if string(payload) != string(computerControlFalse) && string(payload) != string(computerControlTrue) {
				readerErr <- errors.New("reader observed a partial Computer control state")
				return
			}
		}
	}()
	for index := 0; index < 100; index++ {
		if err := atomicWriteComputerControlState(controlDirectory, index%2 == 0); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	readers.Wait()
	select {
	case err := <-readerErr:
		t.Fatal(err)
	default:
	}
	assertComputerControlFile(t, statePath, computerControlFalse)
	entries, err := os.ReadDir(controlDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != computerControlFilename {
		t.Fatalf("Computer control directory = %+v", entries)
	}
	if _, err := prepareComputerControlDirectory(logDirectory, func(string) error { return nil }, func(string) error { return nil }); err == nil {
		t.Fatal("fresh attempt preparation reused prior Computer control state")
	}
}

func TestComputerTokenFileIsAtomicTenantOwnedAndRemovedOnDisable(t *testing.T) {
	controlDirectory := t.TempDir()
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	if err := atomicWriteComputerToken(controlDirectory, "first-secret", uid, gid); err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(controlDirectory, computerTokenFilename)
	assertComputerTokenFile(t, tokenPath, "first-secret", uid, gid)
	if err := atomicWriteComputerToken(controlDirectory, "replacement-secret", uid, gid); err != nil {
		t.Fatal(err)
	}
	assertComputerTokenFile(t, tokenPath, "replacement-secret", uid, gid)
	if err := atomicWriteComputerToken(controlDirectory, "", uid, gid); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tokenPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disabled Computer token file stat error = %v, want not exist", err)
	}
	entries, err := os.ReadDir(controlDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("Computer token update left residue: %+v", entries)
	}
}

func assertComputerTokenFile(t *testing.T, path, want string, uid, gid uint32) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != want {
		t.Fatalf("computer-token = %q, want %q", payload, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o400 {
		t.Fatalf("computer-token mode = %o, want 0400", info.Mode().Perm())
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uid || stat.Gid != gid {
		t.Fatalf("computer-token owner = %#v, want uid=%d gid=%d", info.Sys(), uid, gid)
	}
}

func assertComputerControlFile(t *testing.T, path string, want []byte) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != string(want) {
		t.Fatalf("driver.json = %q, want exact %q", payload, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o444 {
		t.Fatalf("driver.json mode = %o, want 0444", info.Mode().Perm())
	}
}
