package ocihelper

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type vanishedHandoffDirectory struct {
	file     *os.File
	names    []string
	err      error
	closeErr error
	closed   bool
}

func (directory *vanishedHandoffDirectory) Readdirnames(int) ([]string, error) {
	return directory.names, directory.err
}
func (directory *vanishedHandoffDirectory) Close() error {
	directory.closed = true
	if directory.file != nil {
		return errors.Join(directory.file.Close(), directory.closeErr)
	}
	return directory.closeErr
}

func TestDetachedHandoffReadToleratesConcurrentDeletion(t *testing.T) {
	root := filepath.Join(t.TempDir(), ".removing-wefty-handoff-volume-test")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "result.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	backing, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backing.Close() })
	names, err := backing.Readdirnames(handoffReadChunk)
	if err != nil || len(names) != 1 {
		t.Fatalf("buffer directory names: %v err=%v", names, err)
	}
	// A collector removes the directory while another reader still has names
	// buffered. Linux getdents can return those names together with ENOENT.
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	directory := &vanishedHandoffDirectory{
		file:  backing,
		names: names,
		err:   &os.PathError{Op: "readdirent", Path: root, Err: os.ErrNotExist},
	}
	var removed []string
	err = freeDetachedHandoffContents(t.Context(), filepath.Base(root), directory, func(child string) error {
		removed = append(removed, child)
		return os.RemoveAll(filepath.Join(root, child))
	})
	if err != nil {
		t.Fatalf("free concurrently deleted tree: %v", err)
	}
	if !directory.closed || !reflect.DeepEqual(removed, directory.names) {
		t.Fatalf("closed=%t removed=%v", directory.closed, removed)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("detached tree remains: %v", err)
	}
}

func TestDetachedHandoffReadPreservesOtherFailures(t *testing.T) {
	for _, names := range [][]string{nil, {"result.json"}} {
		directory := &vanishedHandoffDirectory{names: names, err: os.ErrPermission}
		err := freeDetachedHandoffContents(t.Context(), "detached", directory, func(string) error { return nil })
		if !errors.Is(err, os.ErrPermission) || !directory.closed {
			t.Fatalf("names=%v err=%v closed=%t", names, err, directory.closed)
		}
	}
}

func TestDetachedHandoffReadPreservesCloseFailureAfterVanish(t *testing.T) {
	directory := &vanishedHandoffDirectory{names: []string{"result.json"}, err: os.ErrNotExist, closeErr: io.ErrClosedPipe}
	err := freeDetachedHandoffContents(t.Context(), "detached", directory, func(string) error { return nil })
	if !errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrNotExist) || !directory.closed {
		t.Fatalf("close failure: %v", err)
	}
}
