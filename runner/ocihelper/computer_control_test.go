package ocihelper

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
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
