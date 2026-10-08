package ocihelper

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDetachedHandoffCancelledFreeResumesOnNextPass(t *testing.T) {
	for _, midTree := range []bool{false, true} {
		name := "before_walk"
		if midTree {
			name = "mid_tree"
		}
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), ".removing-wefty-handoff-volume-test")
			if err := os.Mkdir(root, 0o700); err != nil {
				t.Fatal(err)
			}
			for _, child := range []string{"first", "second", "third"} {
				if err := os.WriteFile(filepath.Join(root, child), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if !midTree {
				cancel()
			}
			freed := 0
			err := freeDetachedHandoffRoot(ctx, root, func(child string) error {
				freed++
				cancel()
				return nil
			})
			if err != nil {
				t.Fatalf("cancelled free must leave its detached root for the next pass: %v", err)
			}
			wantFreed := 0
			if midTree {
				wantFreed = 1
			}
			remaining, err := os.ReadDir(root)
			if err != nil || freed != wantFreed || len(remaining) != 3-wantFreed {
				t.Fatalf("cancelled free: freed=%d remaining=%d err=%v", freed, len(remaining), err)
			}
			if err := freeDetachedHandoffRoot(t.Context(), root, func(child string) error {
				return nil
			}); err != nil {
				t.Fatalf("next pass: %v", err)
			}
			if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("next pass retained detached root: %v", err)
			}
		})
	}
}

func TestDetachedHandoffFreeRemovesNonDirectory(t *testing.T) {
	root := filepath.Join(t.TempDir(), ".removing-wefty-handoff-volume-test")
	if err := os.WriteFile(root, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := freeDetachedHandoffRoot(t.Context(), root, func(string) error {
		t.Fatal("non-directory root has no children")
		return nil
	}); err != nil {
		t.Fatalf("free non-directory detached root: %v", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("non-directory detached root remains: %v", err)
	}
}

func TestDetachedHandoffFreeUnlinksSymlinkWithoutTouchingTarget(t *testing.T) {
	for _, targetKind := range []string{"directory", "file", "missing"} {
		t.Run(targetKind, func(t *testing.T) {
			base := t.TempDir()
			root := filepath.Join(base, ".removing-wefty-handoff-volume-test")
			target := filepath.Join(base, "outside-handoffs")
			payload := target
			if targetKind == "directory" {
				if err := os.Mkdir(target, 0o700); err != nil {
					t.Fatal(err)
				}
				payload = filepath.Join(target, "result.json")
			}
			if targetKind != "missing" {
				if err := os.WriteFile(payload, []byte("keep"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Symlink(target, root); err != nil {
				t.Fatal(err)
			}
			walked := false
			err := freeDetachedHandoffRoot(t.Context(), root, func(child string) error {
				walked = true
				return nil
			})
			if err != nil || walked {
				t.Errorf("free symlink: walked=%t err=%v", walked, err)
			}
			if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("detached symlink remains: %v", err)
			}
			if targetKind != "missing" {
				if content, err := os.ReadFile(payload); err != nil || string(content) != "keep" {
					t.Errorf("symlink target changed: content=%q err=%v", content, err)
				}
			}
		})
	}
}

// RemoveAll must unlink a child symlink rather than walking its target. This
// exercises removal through the retained root used by the helper.
func TestDetachedHandoffFreeDoesNotFollowChildSymlink(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, ".removing-wefty-handoff-volume-test")
	target := filepath.Join(base, "outside-handoffs")
	for _, directory := range []string{root, target} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	payload := filepath.Join(target, "result.json")
	if err := os.WriteFile(payload, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "child")); err != nil {
		t.Fatal(err)
	}
	if err := freeDetachedHandoffRoot(t.Context(), root, func(child string) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("detached root remains: %v", err)
	}
	if content, err := os.ReadFile(payload); err != nil || string(content) != "keep" {
		t.Errorf("child symlink target changed: content=%q err=%v", content, err)
	}
}

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
	complete, err := freeDetachedHandoffContents(t.Context(), filepath.Base(root), directory, func(child string) error {
		removed = append(removed, child)
		return os.RemoveAll(filepath.Join(root, child))
	})
	if err != nil || !complete {
		t.Fatalf("free concurrently deleted tree: complete=%t err=%v", complete, err)
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
		complete, err := freeDetachedHandoffContents(t.Context(), "detached", directory, func(string) error { return nil })
		if complete || !errors.Is(err, os.ErrPermission) || !directory.closed {
			t.Fatalf("names=%v complete=%t err=%v closed=%t", names, complete, err, directory.closed)
		}
	}
}

func TestDetachedHandoffReadPreservesCloseFailureAfterVanish(t *testing.T) {
	directory := &vanishedHandoffDirectory{names: []string{"result.json"}, err: os.ErrNotExist, closeErr: io.ErrClosedPipe}
	complete, err := freeDetachedHandoffContents(t.Context(), "detached", directory, func(string) error { return nil })
	if complete || !errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrNotExist) || !directory.closed {
		t.Fatalf("close failure: complete=%t err=%v", complete, err)
	}
}

func TestDetachedHandoffRootSwapDuringChildRemovalStaysConfined(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "detached")
	moved := filepath.Join(base, "moved")
	outside := filepath.Join(base, "outside")
	for _, directory := range []string{root, outside} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, child := range []string{"first", "second"} {
			if err := os.WriteFile(filepath.Join(directory, child), []byte("keep"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	freed := 0
	err := freeDetachedHandoffRoot(t.Context(), root, func(child string) error {
		if freed == 0 {
			if err := os.Rename(root, moved); err != nil {
				return err
			}
			if err := os.Symlink(outside, root); err != nil {
				return err
			}
		}
		freed++
		return nil
	})
	if err != nil || freed != 2 {
		t.Fatalf("free swapped root: children=%d err=%v", freed, err)
	}
	for _, child := range []string{"first", "second"} {
		if content, err := os.ReadFile(filepath.Join(outside, child)); err != nil || string(content) != "keep" {
			t.Errorf("outside child %s changed: content=%q err=%v", child, content, err)
		}
	}
	if children, err := os.ReadDir(moved); err != nil || len(children) != 0 {
		t.Errorf("original detached tree was not freed: children=%v err=%v", children, err)
	}
}

func TestDetachedHandoffRootSwapBeforeOpenFailsIdentityCheck(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "detached")
	moved := filepath.Join(base, "moved")
	outside := filepath.Join(base, "outside")
	for _, directory := range []string{root, outside} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "child"), []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	walked := false
	err := freeDetachedHandoffRootWithOpenHook(t.Context(), root, func(child string) error {
		walked = true
		return nil
	}, func() {
		if err := os.Rename(root, moved); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, root); err != nil {
			t.Fatal(err)
		}
	})
	if err == nil || walked {
		t.Errorf("root replacement must fail before walking children: walked=%t err=%v", walked, err)
	}
	for _, directory := range []string{moved, outside} {
		if content, err := os.ReadFile(filepath.Join(directory, "child")); err != nil || string(content) != "keep" {
			t.Errorf("child in %s changed: content=%q err=%v", directory, content, err)
		}
	}
}
