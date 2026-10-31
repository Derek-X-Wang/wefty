//go:build darwin || linux

package agentinstall

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/Derek-X-Wang/wefty/internal/testwait"
)

func TestRollingLoadedPlistValidatesTypeBeforeRead(t *testing.T) {
	for _, kind := range []string{"directory", "dangling-symlink", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			g := f.prepare(t, true)
			must(t, os.Remove(g.Plist))
			switch kind {
			case "directory":
				must(t, os.Mkdir(g.Plist, 0644))
			case "dangling-symlink":
				must(t, os.Symlink(filepath.Join(f.s.LaunchAgents, "absent-target"), g.Plist))
			case "fifo":
				must(t, unix.Mkfifo(g.Plist, 0644))
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := f.s.LoadCandidate(ctx, g.ID, f.base); done <- err }()
			var err error
			select {
			case err = <-done:
			case <-ctx.Done():
				// A regression can block opening a FIFO despite context cancellation.
				// A nonblocking writer releases that reader so this test cannot hang
				// while holding the catalog lock, even against the broken implementation.
				if kind == "fifo" {
					fd, openErr := unix.Open(g.Plist, unix.O_WRONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
					if openErr == nil {
						_ = unix.Close(fd)
					} else {
						t.Error("unblock FIFO reader", openErr)
					}
				}
				select {
				case <-done:
				case <-time.After(testwait.Bound):
					t.Error("plist read did not stop after unblocking")
				}
				t.Fatal("LoadCandidate blocked reading a nonregular plist")
			}
			var refusal *VerificationRefusal
			if !errors.As(err, &refusal) {
				t.Fatalf("%s plist must refuse before read, got %v", kind, err)
			}
			// Refusing the read must leave catalog ownership unchanged.
			c, readErr := f.s.Read()
			must(t, readErr)
			if c.Primary != g.ID || c.Generations[g.ID].PlistDigest != g.PlistDigest {
				t.Fatal("refusal changed catalog ownership")
			}
		})
	}
}

func TestRollingLoadedPlistReplacementCannotFollowOrBlock(t *testing.T) {
	for _, kind := range []string{"fifo", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			g := f.prepare(t, true)
			original, err := os.ReadFile(g.Plist)
			must(t, err)
			// An in-root symlink to identical, correctly owned bytes would pass the
			// old validation/read split. The replacement must be refused regardless.
			target := filepath.Join(f.s.LaunchAgents, "replacement-target")
			must(t, os.WriteFile(target, original, 0644))
			replaced := make(chan struct{}, 1)
			f.s.beforePlistOpen = func() error {
				if err := os.Remove(g.Plist); err != nil {
					return err
				}
				var err error
				if kind == "fifo" {
					err = unix.Mkfifo(g.Plist, 0644)
				} else {
					err = os.Symlink(filepath.Base(target), g.Plist)
				}
				if err == nil {
					replaced <- struct{}{}
				}
				return err
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := f.s.LoadCandidate(ctx, g.ID, f.base); done <- err }()
			select {
			case err = <-done:
			case <-ctx.Done():
				if kind == "fifo" {
					fd, openErr := unix.Open(g.Plist, unix.O_WRONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
					if openErr == nil {
						_ = unix.Close(fd)
					} else {
						t.Error("unblock replaced FIFO", openErr)
					}
				}
				select {
				case <-done:
				case <-time.After(testwait.Bound):
					t.Error("replaced plist read did not stop after unblocking")
				}
				t.Fatal("replacement blocked LoadCandidate while holding the catalog lock")
			}
			select {
			case <-replaced:
			default:
				t.Fatal("replacement hook did not run")
			}
			var refusal *VerificationRefusal
			if !errors.As(err, &refusal) {
				t.Fatalf("replacement %s must refuse, got %v", kind, err)
			}
			c, readErr := f.s.Read()
			must(t, readErr)
			if c.Primary != g.ID || c.Generations[g.ID].PlistDigest != g.PlistDigest {
				t.Fatal("replacement refusal changed catalog ownership")
			}
		})
	}
}
