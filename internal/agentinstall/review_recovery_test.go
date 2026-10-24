package agentinstall

import (
	"errors"
	"fmt"
	"github.com/Derek-X-Wang/wefty/l1"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRemovalDirectorySwapCannotDeletePrimary(t *testing.T) {
	for _, kind := range []string{"candidate", "retirement"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			old := f.prepare(t, true)
			var primary Generation
			var remove func() error
			if kind == "candidate" {
				primary = old
				g, err := f.s.PrepareCandidate(f.source, "next", testBoot, []int{1}, "op")
				must(t, err)
				_, err = f.s.LoadCandidate(t.Context(), g.ID, f.base)
				must(t, err)
				state := f.f.states[g.Label]
				state.Running = false
				state.PID = 0
				f.f.states[g.Label] = state
				remove = func() error { return f.s.RemoveCandidate(t.Context(), g.ID, "op") }
			} else {
				primary = f.prepare(t, false)
				f.promote(t, primary)
				f.lifetimes[old.ID]()
				delete(f.lifetimes, old.ID)
				state := f.f.states[old.Label]
				state.Running = false
				state.PID = 0
				f.f.states[old.Label] = state
				c, err := f.s.Read()
				must(t, err)
				g := c.Generations[old.ID]
				remote := l1.NodeGeneration{AgentGenerationID: old.ID, BootSessionID: g.Process.BootSessionID, Role: l1.NodeGenerationClosed, PromotionOperationID: "promotion"}
				must(t, f.s.PublishClosedGeneration(remote))
				remove = func() error {
					return f.s.CleanupClosed(t.Context(), l1.NodeGenerationInventory{NodeID: "node", Generations: []l1.NodeGeneration{remote}})
				}
			}
			expected, err := os.ReadFile(primary.Executable)
			must(t, err)
			f.s.beforeGenerationRemoval = func(id string) error {
				must(t, os.Rename(filepath.Join(f.s.Root(), id), filepath.Join(f.s.Root(), id+"-moved")))
				return os.Symlink(primary.ID, filepath.Join(f.s.Root(), id))
			}
			err = remove()
			actual, readErr := os.ReadFile(primary.Executable)
			if readErr != nil || string(actual) != string(expected) {
				t.Fatalf("primary files deleted after directory swap: %v", readErr)
			}
			var refused *VerificationRefusal
			if !errors.As(err, &refused) {
				t.Fatalf("directory swap was not refused: %v", err)
			}
		})
	}
}

func TestCandidateRollbackAfterPlistIntentBeforePublication(t *testing.T) {
	f := newFixture(t)
	primary := f.prepare(t, true)
	candidate, err := f.s.PrepareCandidate(f.source, "next", testBoot, []int{1}, "operation")
	must(t, err)
	hit := false
	f.s.afterCandidateWrite = func(phase string) error { hit = true; return errors.New("interrupt after digest") }
	if _, err = f.s.LoadCandidate(t.Context(), candidate.ID, f.base); err == nil || !hit {
		t.Fatal("missing interrupted publication", err)
	}
	if _, err = os.Lstat(candidate.Plist); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("plist unexpectedly published", err)
	}
	must(t, f.s.RemoveCandidate(t.Context(), candidate.ID, "operation"))
	must(t, f.s.RemoveCandidate(t.Context(), candidate.ID, "operation"))
	if _, err = os.Stat(primary.Executable); err != nil {
		t.Fatal("primary removed", err)
	}
}

func TestInitializationRecoversOperationBoundPartialRoot(t *testing.T) {
	for _, phase := range []string{"intent_recorded", "root_created", "generation_directory_created", "executable_created", "catalog_published"} {
		t.Run(phase, func(t *testing.T) {
			f := newFixture(t)
			hit := false
			f.s.InitializationStep = func(at string) error {
				if at == phase {
					hit = true
					return errors.New("interrupt initialization")
				}
				return nil
			}
			_, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			if err == nil || !hit {
				t.Fatal("missing initialization interruption", err)
			}
			// Even a private-looking unrelated entry must not be silently adopted.
			must(t, os.MkdirAll(filepath.Join(f.s.Root(), "unowned"), 0700))
			f.s.InitializationStep = nil
			var refused *VerificationRefusal
			if _, err = f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation"); !errors.As(err, &refused) {
				t.Fatal("arbitrary retained storage was adopted", err)
			}
			must(t, os.Remove(filepath.Join(f.s.Root(), "unowned")))
			first, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			must(t, err)
			replay, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			must(t, err)
			if first.ID != replay.ID {
				t.Fatal("initialization repeated")
			}
			if _, err = f.s.Initialize("node", f.source, "changed", testBoot, []int{1}, "operation"); !errors.As(err, &refused) {
				t.Fatal("changed initialization payload accepted", err)
			}
		})
	}
}

func TestRemovalFinalIdentitySwapPreservesReplacement(t *testing.T) {
	for _, kind := range []string{"candidate", "retirement"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			old := f.prepare(t, true)
			var primary Generation
			var remove func() error
			if kind == "candidate" {
				primary = old
				g, err := f.s.PrepareCandidate(f.source, "next", testBoot, []int{1}, "op")
				must(t, err)
				_, err = f.s.LoadCandidate(t.Context(), g.ID, f.base)
				must(t, err)
				state := f.f.states[g.Label]
				state.Running = false
				state.PID = 0
				f.f.states[g.Label] = state
				remove = func() error { return f.s.RemoveCandidate(t.Context(), g.ID, "op") }
			} else {
				primary = f.prepare(t, false)
				f.promote(t, primary)
				f.lifetimes[old.ID]()
				delete(f.lifetimes, old.ID)
				state := f.f.states[old.Label]
				state.Running = false
				state.PID = 0
				f.f.states[old.Label] = state
				c, err := f.s.Read()
				must(t, err)
				g := c.Generations[old.ID]
				remote := l1.NodeGeneration{AgentGenerationID: old.ID, BootSessionID: g.Process.BootSessionID, Role: l1.NodeGenerationClosed, PromotionOperationID: "promotion"}
				must(t, f.s.PublishClosedGeneration(remote))
				remove = func() error {
					return f.s.CleanupClosed(t.Context(), l1.NodeGenerationInventory{NodeID: "node", Generations: []l1.NodeGeneration{remote}})
				}
			}
			expected, err := os.ReadFile(primary.Executable)
			must(t, err)
			var swappedID string
			f.s.afterRemovalIdentityCheck = func(id string) error {
				swappedID = id
				must(t, os.Rename(filepath.Join(f.s.Root(), id), filepath.Join(f.s.Root(), id+"-moved")))
				return os.Mkdir(filepath.Join(f.s.Root(), id), 0700)
			}
			err = remove()
			actual, readErr := os.ReadFile(primary.Executable)
			if readErr != nil || string(actual) != string(expected) {
				t.Fatalf("primary files deleted after directory swap: %v", readErr)
			}
			if _, statErr := os.Stat(filepath.Join(f.s.Root(), swappedID)); statErr != nil {
				t.Fatalf("replacement directory removed: %v", statErr)
			}
			must(t, err)
			for _, id := range []string{swappedID, swappedID + "-moved"} {
				entries, err := os.ReadDir(filepath.Join(f.s.Root(), id))
				if err != nil || len(entries) != 0 {
					t.Fatalf("empty original/replacement not retained: %s %v %v", id, entries, err)
				}
			}
		})
	}
}

// These retained files model death within the publisher, before its rename.
func TestInitializationRetainedPublicationStaging(t *testing.T) {
	for _, kind := range []string{"executable", "catalog"} {
		for _, synced := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/synced=%t", kind, synced), func(t *testing.T) {
				f := newFixture(t)
				phase := "generation_directory_created"
				if kind == "catalog" {
					phase = "executable_created"
				}
				f.s.InitializationStep = func(at string) error {
					if at == phase {
						return errors.New("interrupt")
					}
					return nil
				}
				g, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
				if err == nil {
					t.Fatal("missing interrupt")
				}
				name := filepath.Join(f.s.Root(), g.ID, "wefty-agent.init-"+g.ID+".tmp")
				mode := os.FileMode(0500)
				if kind == "catalog" {
					name = filepath.Join(f.s.Root(), "catalog.json.init-"+g.ID+".tmp")
					mode = 0600
				}
				stage, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
				must(t, err)
				_, err = stage.Write([]byte("partial"))
				must(t, err)
				if synced {
					must(t, stage.Sync())
				}
				must(t, stage.Close())
				f.s.InitializationStep = nil
				replay, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
				must(t, err)
				if replay.ID != g.ID {
					t.Fatal("repeated initialization")
				}
				if _, err = os.Lstat(name); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("retained staging survived", err)
				}
			})
		}
	}
}

func TestInitializationInterruptedInsidePublication(t *testing.T) {
	for _, phase := range []string{"executable_staging_created", "executable_staging_synced", "executable_published_before_sync", "executable_staging_removed", "executable_directory_synced", "catalog_staging_created", "catalog_staging_synced", "catalog_published_before_sync", "catalog_staging_removed"} {
		t.Run(phase, func(t *testing.T) {
			f := newFixture(t)
			hit := false
			f.s.InitializationStep = func(at string) error {
				if at == phase {
					hit = true
					return errors.New("death inside publisher")
				}
				return nil
			}
			g, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			if err == nil || !hit {
				t.Fatal("publication window not reached", err)
			}
			f.s.InitializationStep = nil
			replay, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			must(t, err)
			if replay.ID != g.ID {
				t.Fatal("duplicate generation")
			}
			data, err := os.ReadFile(replay.Executable)
			must(t, err)
			source, err := os.ReadFile(f.source)
			must(t, err)
			if string(data) != string(source) {
				t.Fatal("partial executable published")
			}
		})
	}
}

// Exit inside the publisher without defers, so the child drops its descriptors
// and locks exactly as a dead upgrader does. All paths belong to this fixture.
func TestInitializationProcessDeathInsidePublication(t *testing.T) {
	if phase := os.Getenv("WEFTY_TEST_INITIALIZATION_CRASH_PHASE"); phase != "" {
		s := Store{Data: os.Getenv("WEFTY_TEST_INITIALIZATION_DATA"), LaunchAgents: os.Getenv("WEFTY_TEST_INITIALIZATION_AGENTS")}
		s.InitializationStep = func(at string) error {
			if at == phase {
				os.Exit(83)
			}
			return nil
		}
		_, err := s.Initialize("node", os.Getenv("WEFTY_TEST_INITIALIZATION_SOURCE"), "next", testBoot, []int{1}, "operation")
		t.Fatal("crash phase not reached", err)
	}
	for _, phase := range []string{"executable_staging_created", "executable_staging_synced", "executable_published_before_sync", "executable_staging_removed", "executable_directory_synced", "catalog_staging_created", "catalog_staging_synced", "catalog_published_before_sync", "catalog_staging_removed"} {
		t.Run(phase, func(t *testing.T) {
			f := newFixture(t)
			child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestInitializationProcessDeathInsidePublication$")
			child.Env = append(os.Environ(), "WEFTY_TEST_INITIALIZATION_CRASH_PHASE="+phase, "WEFTY_TEST_INITIALIZATION_DATA="+f.s.Data, "WEFTY_TEST_INITIALIZATION_AGENTS="+f.s.LaunchAgents, "WEFTY_TEST_INITIALIZATION_SOURCE="+f.source)
			out, err := child.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 83 {
				t.Fatalf("fixture did not die inside publisher: %v %s", err, out)
			}
			g, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			must(t, err)
			replay, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			must(t, err)
			if g.ID != replay.ID {
				t.Fatal("duplicate initialization")
			}
			for _, dir := range []string{f.s.Root(), filepath.Dir(g.Executable)} {
				entries, err := os.ReadDir(dir)
				must(t, err)
				for _, entry := range entries {
					if strings.HasSuffix(entry.Name(), ".tmp") {
						t.Fatal("publication left staging", entry.Name())
					}
				}
			}
		})
	}
}

func TestInitializationRefusesUnownedPublicationArtifacts(t *testing.T) {
	for _, kind := range []string{"foreign_executable", "foreign_catalog", "old_random_temp"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			f.s.InitializationStep = func(at string) error {
				if at == "generation_directory_created" {
					return errors.New("interrupt")
				}
				return nil
			}
			g, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			if err == nil {
				t.Fatal("missing interruption")
			}
			name := filepath.Join(f.s.Root(), g.ID, "wefty-agent.init-"+g.ID+".tmp")
			switch kind {
			case "foreign_executable":
				name = filepath.Join(f.s.Root(), g.ID, "wefty-agent.init-foreign.tmp")
			case "foreign_catalog":
				name = filepath.Join(f.s.Root(), "catalog.json.tmp")
			case "old_random_temp":
				name = filepath.Join(f.s.Root(), g.ID, "wefty-agent.tmp-unbound")
			}
			must(t, os.WriteFile(name, []byte("unowned"), 0600))
			f.s.InitializationStep = nil
			_, err = f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			var refused *VerificationRefusal
			if !errors.As(err, &refused) {
				t.Fatal("unowned staging accepted", err)
			}
			if _, err = os.Lstat(name); err != nil {
				t.Fatal("unowned staging removed", err)
			}
		})
	}
}
