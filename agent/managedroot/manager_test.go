//go:build darwin || linux

package managedroot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

var errInjectedCrash = errors.New("injected crash")

func TestManagedRootDeletionGuardrailMatrix(t *testing.T) {
	runDeletionGuardrailMatrix(t)
}

func runDeletionGuardrailMatrix(t *testing.T) {
	t.Helper()

	t.Run("unsafe roots", func(t *testing.T) {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		for _, root := range []string{"", ".", string(filepath.Separator), home} {
			t.Run(strings.ReplaceAll(root, string(filepath.Separator), "_"), func(t *testing.T) {
				_, err := Initialize(Config{Root: root, NodeID: "node", BootSessionID: "boot"})
				if !errors.Is(err, ErrUnsafeRoot) {
					t.Fatalf("Initialize(%q) error = %v, want %v", root, err, ErrUnsafeRoot)
				}
			})
		}
	})

	t.Run("malicious identifiers remain immediate children", func(t *testing.T) {
		root := syntheticRoot(t)
		manager, err := Initialize(Config{
			Root: root, NodeID: "../../node/../../../home", BootSessionID: "boot/../old",
		})
		if err != nil {
			t.Fatal(err)
		}
		for index, jobID := range []string{"..", "../../home", "job/with/separators", "job\\with\\separators", ".", strings.Repeat("x", 1024)} {
			paths, err := manager.CreateService(jobID, uint64(index+1))
			if err != nil {
				t.Fatalf("CreateService(%q): %v", jobID, err)
			}
			if filepath.Dir(paths.Root) != manager.ServicesPath() {
				t.Fatalf("service root %q is not an immediate child of %q", paths.Root, manager.ServicesPath())
			}
			if filepath.Base(paths.Root) != EncodeID(jobID) {
				t.Fatalf("service component = %q, want encoded ID %q", filepath.Base(paths.Root), EncodeID(jobID))
			}
			request := removalFor(manager, jobID, uint64(index+1))
			if err := manager.Remove(context.Background(), request); err != nil {
				t.Fatalf("Remove(%q): %v", jobID, err)
			}
		}
	})

	t.Run("process tree reap is a hard precondition", func(t *testing.T) {
		fixture := newFixture(t, "boot-a", "job", 1)
		request := fixture.removal
		request.ProcessTreeReaped = false
		if err := fixture.manager.Remove(context.Background(), request); !errors.Is(err, ErrProcessTreeNotReaped) {
			t.Fatalf("Remove() error = %v, want %v", err, ErrProcessTreeNotReaped)
		}
		assertExists(t, fixture.paths.Root)
	})

	t.Run("root instance mismatch fails closed", func(t *testing.T) {
		fixture := newFixture(t, "boot-a", "job", 1)
		request := fixture.removal
		request.RootInstanceID = "wrong-root-instance"
		if err := fixture.manager.Remove(context.Background(), request); !errors.Is(err, ErrRootManifestMismatch) {
			t.Fatalf("Remove() error = %v, want %v", err, ErrRootManifestMismatch)
		}
		assertExists(t, fixture.paths.Root)
	})

	t.Run("root instance identifiers are random per synthetic root", func(t *testing.T) {
		first, err := Initialize(Config{Root: syntheticRoot(t), NodeID: "node", BootSessionID: "boot"})
		if err != nil {
			t.Fatal(err)
		}
		second, err := Initialize(Config{Root: syntheticRoot(t), NodeID: "node", BootSessionID: "boot"})
		if err != nil {
			t.Fatal(err)
		}
		if first.Manifest().RootInstanceID == second.Manifest().RootInstanceID {
			t.Fatal("independent managed roots received the same root instance ID")
		}
	})

	t.Run("missing and corrupt ownership metadata fail closed", func(t *testing.T) {
		for _, test := range []struct {
			name    string
			payload []byte
		}{
			{name: "missing"},
			{name: "corrupt", payload: []byte("not-json")},
		} {
			t.Run(test.name, func(t *testing.T) {
				fixture := newFixture(t, "boot-a", "job", 1)
				manifestPath := filepath.Join(fixture.paths.Root, OwnershipManifestName)
				if test.payload == nil {
					if err := os.Remove(manifestPath); err != nil {
						t.Fatal(err)
					}
				} else if err := os.WriteFile(manifestPath, test.payload, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := fixture.manager.Remove(context.Background(), fixture.removal); !errors.Is(err, ErrOwnershipManifest) {
					t.Fatalf("Remove() error = %v, want %v", err, ErrOwnershipManifest)
				}
				assertExists(t, fixture.paths.Root)
			})
		}
	})

	t.Run("existing root with corrupt root manifest fails closed", func(t *testing.T) {
		fixture := newFixture(t, "boot-a", "job", 1)
		if err := os.WriteFile(filepath.Join(fixture.manager.NodePath(), RootManifestName), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Open(Config{Root: fixture.config.Root, NodeID: fixture.config.NodeID, BootSessionID: "boot-b"})
		if !errors.Is(err, ErrRootManifest) {
			t.Fatalf("Open() error = %v, want %v", err, ErrRootManifest)
		}
		assertExists(t, fixture.paths.Root)
	})

	t.Run("symlink target to home is refused", func(t *testing.T) {
		fixture := newFixture(t, "boot-a", "job", 1)
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		before, err := os.Stat(home)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(fixture.paths.Root); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(home, fixture.paths.Root); err != nil {
			t.Fatal(err)
		}
		if err := fixture.manager.Remove(context.Background(), fixture.removal); !errors.Is(err, ErrSymlink) {
			t.Fatalf("Remove() error = %v, want %v", err, ErrSymlink)
		}
		after, err := os.Stat(home)
		if err != nil || !os.SameFile(before, after) {
			t.Fatalf("user home changed through a managed-root symlink: %v", err)
		}
	})

	t.Run("symlink swap after validation is refused", func(t *testing.T) {
		fixture := newFixture(t, "boot-a", "job", 1)
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		before, err := os.Stat(home)
		if err != nil {
			t.Fatal(err)
		}
		nested := filepath.Join(fixture.paths.Data, "nested")
		if err := os.Mkdir(nested, 0o700); err != nil {
			t.Fatal(err)
		}
		fixture.manager.checkpoint = func(checkpoint Checkpoint) error {
			if checkpoint != CheckpointAfterValidate {
				return nil
			}
			if err := os.Rename(nested, nested+"-original"); err != nil {
				return err
			}
			return os.Symlink(home, nested)
		}
		if err := fixture.manager.Remove(context.Background(), fixture.removal); !errors.Is(err, ErrSymlink) {
			t.Fatalf("Remove() error = %v, want %v", err, ErrSymlink)
		}
		after, err := os.Stat(home)
		if err != nil || !os.SameFile(before, after) {
			t.Fatalf("user home changed through a post-validation symlink swap: %v", err)
		}
	})

	t.Run("nested mount identity makes cleanup incomplete", func(t *testing.T) {
		fixture := newFixture(t, "boot-a", "job", 1)
		nested := filepath.Join(fixture.paths.Data, "mounted")
		if err := os.Mkdir(nested, 0o700); err != nil {
			t.Fatal(err)
		}
		realIdentityAt := fixture.manager.mountIdentityAt
		fixture.manager.mountIdentityAt = func(parentFD int, name string) (mountIdentity, error) {
			identity, err := realIdentityAt(parentFD, name)
			if err == nil && name == "mounted" {
				identity.key += ":other-mount"
			}
			return identity, err
		}
		if err := fixture.manager.Remove(context.Background(), fixture.removal); !errors.Is(err, ErrMountBoundary) {
			t.Fatalf("Remove() error = %v, want %v", err, ErrMountBoundary)
		}
		assertExists(t, fixture.paths.Root)
	})

	t.Run("concurrent removal generations cannot race ownership", func(t *testing.T) {
		fixture := newFixture(t, "boot-a", "job", 2)
		stale := fixture.removal
		stale.Generation = 1
		current := fixture.removal
		var wait sync.WaitGroup
		errorsByGeneration := make([]error, 2)
		for index, request := range []Removal{stale, current} {
			wait.Add(1)
			go func(index int, request Removal) {
				defer wait.Done()
				errorsByGeneration[index] = fixture.manager.Remove(context.Background(), request)
			}(index, request)
		}
		wait.Wait()
		if !errors.Is(errorsByGeneration[0], ErrRemovalGeneration) {
			t.Fatalf("stale generation error = %v, want %v", errorsByGeneration[0], ErrRemovalGeneration)
		}
		if errorsByGeneration[1] != nil {
			t.Fatalf("current generation error = %v", errorsByGeneration[1])
		}
		assertMissing(t, fixture.paths.Root)
	})

	t.Run("stale boot is rejected and a new boot resumes offline cleanup", func(t *testing.T) {
		fixture := newFixture(t, "boot-a", "job", 1)
		fixture.manager.checkpoint = crashOnceAt(CheckpointAfterQuarantineRename)
		if err := fixture.manager.Remove(context.Background(), fixture.removal); !errors.Is(err, errInjectedCrash) {
			t.Fatalf("Remove() error = %v, want injected crash", err)
		}
		fixture.manager.checkpoint = nil
		newBoot, err := Open(Config{Root: fixture.config.Root, NodeID: fixture.config.NodeID, BootSessionID: "boot-b"})
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.manager.Remove(context.Background(), fixture.removal); !errors.Is(err, ErrStaleBootSession) {
			t.Fatalf("old boot Remove() error = %v, want %v", err, ErrStaleBootSession)
		}
		completed, err := newBoot.Resume(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(completed) != 1 || completed[0].JobID != fixture.removal.JobID || completed[0].Generation != fixture.removal.Generation {
			t.Fatalf("Resume() = %#v, want completed removal", completed)
		}
		assertMissing(t, fixture.paths.Root)
	})

	t.Run("missing target after recorded quarantine is idempotent success", func(t *testing.T) {
		fixture := newFixture(t, "boot-a", "job", 1)
		fixture.manager.checkpoint = crashOnceAt(CheckpointAfterDelete)
		if err := fixture.manager.Remove(context.Background(), fixture.removal); !errors.Is(err, errInjectedCrash) {
			t.Fatalf("Remove() error = %v, want injected crash", err)
		}
		fixture.manager.checkpoint = nil
		if err := fixture.manager.Remove(context.Background(), fixture.removal); err != nil {
			t.Fatalf("idempotent Remove(): %v", err)
		}
		assertMissing(t, fixture.paths.Root)
	})

	t.Run("missing target without a quarantine record fails closed", func(t *testing.T) {
		fixture := newFixture(t, "boot-a", "job", 1)
		if err := os.RemoveAll(fixture.paths.Root); err != nil {
			t.Fatal(err)
		}
		if err := fixture.manager.Remove(context.Background(), fixture.removal); !errors.Is(err, ErrOwnershipManifest) {
			t.Fatalf("Remove() error = %v, want %v", err, ErrOwnershipManifest)
		}
	})

	t.Run("orphaned quarantine without a removal record fails closed", func(t *testing.T) {
		fixture := newFixture(t, "boot-a", "job", 1)
		orphan := filepath.Join(fixture.manager.NodePath(), quarantineDirectory, "orphan")
		if err := os.Mkdir(orphan, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.manager.Resume(context.Background()); !errors.Is(err, ErrRemovalRecord) {
			t.Fatalf("Resume() error = %v, want %v", err, ErrRemovalRecord)
		}
		assertExists(t, orphan)
	})

	t.Run("crash recovery before and after every phase", func(t *testing.T) {
		for _, checkpoint := range deletionCheckpoints {
			t.Run(string(checkpoint), func(t *testing.T) {
				fixture := newFixture(t, "boot-a", "job", 1)
				fixture.manager.checkpoint = crashOnceAt(checkpoint)
				if err := fixture.manager.Remove(context.Background(), fixture.removal); !errors.Is(err, errInjectedCrash) {
					t.Fatalf("Remove() error = %v, want injected crash at %s", err, checkpoint)
				}
				fixture.manager.checkpoint = nil
				if err := fixture.manager.Remove(context.Background(), fixture.removal); err != nil {
					t.Fatalf("Remove() after crash at %s: %v", checkpoint, err)
				}
				assertMissing(t, fixture.paths.Root)
			})
		}
	})
}

type fixture struct {
	manager *Manager
	config  Config
	paths   ServicePaths
	removal Removal
}

func newFixture(t *testing.T, bootSessionID, jobID string, generation uint64) fixture {
	t.Helper()
	config := Config{Root: syntheticRoot(t), NodeID: "stable-node", BootSessionID: bootSessionID}
	manager, err := Initialize(config)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := manager.CreateService(jobID, generation)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(paths.Data, "payload.db"), []byte("owned"), 0o600); err != nil {
		t.Fatal(err)
	}
	return fixture{
		manager: manager,
		config:  config,
		paths:   paths,
		removal: removalFor(manager, jobID, generation),
	}
}

func removalFor(manager *Manager, jobID string, generation uint64) Removal {
	return Removal{
		JobID:             jobID,
		Generation:        generation,
		RootInstanceID:    manager.Manifest().RootInstanceID,
		CleanupFence:      "cleanup-fence",
		BootSessionID:     manager.BootSessionID(),
		ProcessTreeReaped: true,
	}
}

func syntheticRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, "state")
}

func crashOnceAt(want Checkpoint) func(Checkpoint) error {
	crashed := false
	return func(got Checkpoint) error {
		if got == want && !crashed {
			crashed = true
			return errInjectedCrash
		}
		return nil
	}
}

func assertExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("expected %q to exist: %v", path, err)
	}
}

func assertMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected %q to be absent, got %v", path, err)
	}
}
