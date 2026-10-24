package agentinstall

import (
	"errors"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestInitializationAmbiguousStagePreserved(t *testing.T) {
	for _, kind := range []string{"executable", "catalog"} {
		for _, swap := range []bool{false, true} {
			t.Run(kind+map[bool]string{true: "/swap", false: "/foreign"}[swap], func(t *testing.T) {
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
					t.Fatal("missing interruption")
				}
				name := filepath.Join(f.s.Root(), g.ID, "wefty-agent.init-"+g.ID+".tmp")
				mode := os.FileMode(0500)
				if kind == "catalog" {
					name = filepath.Join(f.s.Root(), "catalog.json.init-"+g.ID+".tmp")
					mode = 0600
				}
				must(t, os.WriteFile(name, []byte("foreign original"), mode))
				hit := false
				moved := filepath.Join(t.TempDir(), "original")
				f.s.InitializationStep = func(at string) error {
					if swap && at == "stage_validated_before_custody" && !hit {
						hit = true
						must(t, os.Rename(name, moved))
						return os.WriteFile(name, []byte("foreign replacement"), mode)
					}
					return nil
				}
				_, err = f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
				must(t, err)
				expected := "foreign original"
				if swap {
					if !hit {
						t.Fatal("missing swap window")
					}
					data, err := os.ReadFile(moved)
					must(t, err)
					if string(data) != "foreign original" {
						t.Fatal("original lost")
					}
					expected = "foreign replacement"
				}
				found := false
				must(t, filepath.WalkDir(f.s.Data, func(path string, entry os.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if entry.IsDir() || !strings.Contains(path, "quarantine") {
						return nil
					}
					data, err := os.ReadFile(path)
					if err != nil {
						return err
					}
					if string(data) == expected {
						found = true
					}
					return nil
				}))
				if !found {
					t.Fatal("ambiguous stage deleted rather than retained in quarantine")
				}
				f.s.InitializationStep = nil
				_, err = f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
				must(t, err)
			})
		}
	}
}
func TestInitializationSyncsExecutableBeforeCatalog(t *testing.T) {
	for _, accept := range []bool{false, true} {
		t.Run(map[bool]string{true: "accept", false: "create"}[accept], func(t *testing.T) {
			f := newFixture(t)
			if accept {
				hit := false
				f.s.InitializationStep = func(at string) error {
					if at == "executable_staging_removed" {
						hit = true
						return errors.New("death after staging removal")
					}
					return nil
				}
				_, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
				if err == nil || !hit {
					t.Fatal("missing exact interruption", err)
				}
			}
			synced := false
			f.s.initializationSyncDir = func(kind string, dir *os.Root) error {
				if kind == "generation_executable" {
					synced = true
				}
				return durable.SyncDir(dir)
			}
			f.s.InitializationStep = func(at string) error {
				if at == "catalog_staging_created" && !synced {
					t.Fatal("catalog publication preceded generation-directory sync")
				}
				return nil
			}
			_, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			must(t, err)
			if !synced {
				t.Fatal("executable directory not synced")
			}
		})
	}
}

func TestInitializationQuarantineDurableTransitions(t *testing.T) {
	for _, phase := range []string{"quarantine_intent_recorded", "quarantine_directory_created", "stage_validated_before_custody", "quarantine_moved", "quarantine_recorded"} {
		t.Run(phase, func(t *testing.T) {
			f := newFixture(t)
			f.s.InitializationStep = func(at string) error {
				if at == "generation_directory_created" {
					return errors.New("prepare retained stage")
				}
				return nil
			}
			g, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			if err == nil {
				t.Fatal("missing prepare interruption")
			}
			name := filepath.Join(f.s.Root(), g.ID, initializationStage(g.ID, "executable"))
			must(t, os.WriteFile(name, []byte("foreign preserved evidence"), 0500))
			hit := false
			f.s.InitializationStep = func(at string) error {
				if at == phase {
					hit = true
					return errors.New("interrupt custody")
				}
				return nil
			}
			_, err = f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			if err == nil || !hit {
				t.Fatal("custody boundary not reached", err)
			}
			f.s.InitializationStep = nil
			replay, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			must(t, err)
			if replay.ID != g.ID {
				t.Fatal("duplicate initialization")
			}
			parent, err := os.OpenRoot(f.s.Data)
			must(t, err)
			defer parent.Close()
			var in initializationIntent
			must(t, readPrivate(parent, initializationFile, &in))
			count := 0
			for _, q := range in.Quarantines {
				if q.State != "retained" {
					t.Fatal("unfinished quarantine", q)
				}
				data, err := os.ReadFile(filepath.Join(f.s.Data, initializationQuarantinePrefix+q.ID, "artifact"))
				must(t, err)
				if string(data) == "foreign preserved evidence" {
					count++
				}
			}
			if count != 1 {
				t.Fatal("quarantine lost or duplicated original bytes", count)
			}
		})
	}
}
func TestInitializationExecutableSyncFaultFencesCatalog(t *testing.T) {
	f := newFixture(t)
	fault := errors.New("generation directory fsync fault")
	f.s.initializationSyncDir = func(kind string, dir *os.Root) error {
		if kind == "generation_executable" {
			return fault
		}
		return durable.SyncDir(dir)
	}
	_, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
	var refusal *VerificationRefusal
	if !errors.Is(err, fault) || errors.As(err, &refusal) {
		t.Fatal("sync fault lost or misclassified", err)
	}
	if _, err = os.Stat(filepath.Join(f.s.Root(), catalogName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("catalog published after failed executable sync", err)
	}
	f.s.initializationSyncDir = nil
	_, err = f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
	must(t, err)
}
func TestInitializationCommittedReplayKeepsLiveLifetime(t *testing.T) {
	f := newFixture(t)
	g, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
	must(t, err)
	lifetime, err := f.s.StartupWithLifetime(g.ID, "node", g.Executable, testBoot)
	must(t, err)
	defer lifetime.Close()
	replay, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
	must(t, err)
	if replay.ID != g.ID {
		t.Fatal("committed replay changed primary")
	}
}

func TestInitializationFreshStageReplacementCannotPublish(t *testing.T) {
	for _, kind := range []string{"executable", "catalog"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			synced, swapped := false, false
			original := filepath.Join(t.TempDir(), "created-stage")
			var expected []byte
			f.s.InitializationStep = func(at string) error {
				if at == kind+"_staging_synced" {
					synced = true
				}
				if at == "stage_validated_before_custody" && synced && !swapped {
					swapped = true
					parent, err := os.OpenRoot(f.s.Data)
					must(t, err)
					var in initializationIntent
					must(t, readPrivate(parent, initializationFile, &in))
					parent.Close()
					name := filepath.Join(f.s.Root(), in.Generation.ID, initializationStage(in.Generation.ID, kind))
					mode := os.FileMode(0500)
					if kind == "catalog" {
						name = filepath.Join(f.s.Root(), initializationStage(in.Generation.ID, kind))
						mode = 0600
					}
					expected, err = os.ReadFile(name)
					must(t, err)
					must(t, os.Rename(name, original))
					return os.WriteFile(name, []byte("foreign swapped publication"), mode)
				}
				return nil
			}
			_, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			var refused *VerificationRefusal
			if !swapped || !errors.As(err, &refused) {
				t.Fatal("replaced stage was published or misclassified", err)
			}
			if _, err = os.Stat(filepath.Join(f.s.Root(), catalogName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("foreign bytes reached catalog ownership", err)
			}
			actual, err := os.ReadFile(original)
			must(t, err)
			if string(actual) != string(expected) {
				t.Fatal("original stage was destroyed")
			}
			f.s.InitializationStep = nil
			_, err = f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
			must(t, err)
			parent, err := os.OpenRoot(f.s.Data)
			must(t, err)
			defer parent.Close()
			var in initializationIntent
			must(t, readPrivate(parent, initializationFile, &in))
			found := false
			for _, q := range in.Quarantines {
				data, err := os.ReadFile(filepath.Join(f.s.Data, initializationQuarantinePrefix+q.ID, "artifact"))
				must(t, err)
				if string(data) == "foreign swapped publication" {
					found = true
				}
			}
			if !found {
				t.Fatal("publication replacement was not retained in recorded custody")
			}
		})
	}
}

func TestInitializationQuarantineDestinationCollisionPreservesBoth(t *testing.T) {
	f := newFixture(t)
	f.s.InitializationStep = func(at string) error {
		if at == "generation_directory_created" {
			return errors.New("prepare foreign stage")
		}
		return nil
	}
	g, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
	if err == nil {
		t.Fatal("missing prepare interruption")
	}
	name := filepath.Join(f.s.Root(), g.ID, initializationStage(g.ID, "executable"))
	must(t, os.WriteFile(name, []byte("foreign source"), 0500))
	hit := false
	f.s.InitializationStep = func(at string) error {
		if at == "stage_validated_before_custody" && !hit {
			hit = true
			parent, err := os.OpenRoot(f.s.Data)
			must(t, err)
			var in initializationIntent
			must(t, readPrivate(parent, initializationFile, &in))
			parent.Close()
			q := in.Quarantines[len(in.Quarantines)-1]
			return os.WriteFile(filepath.Join(f.s.Data, initializationQuarantinePrefix+q.ID, "artifact"), []byte("foreign destination"), 0600)
		}
		return nil
	}
	_, err = f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
	must(t, err)
	found := map[string]int{}
	parent, err := os.OpenRoot(f.s.Data)
	must(t, err)
	defer parent.Close()
	var in initializationIntent
	must(t, readPrivate(parent, initializationFile, &in))
	for _, q := range in.Quarantines {
		data, err := os.ReadFile(filepath.Join(f.s.Data, initializationQuarantinePrefix+q.ID, "artifact"))
		must(t, err)
		found[string(data)]++
	}
	if !hit || found["foreign source"] != 1 || found["foreign destination"] != 1 {
		t.Fatal("quarantine collision overwrote unrelated bytes", found)
	}
}

func TestInitializationConcurrentRetriesKeepCustody(t *testing.T) {
	f := newFixture(t)
	f.s.InitializationStep = func(at string) error {
		if at == "generation_directory_created" {
			return errors.New("prepare foreign stage")
		}
		return nil
	}
	g, err := f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
	if err == nil {
		t.Fatal("missing prepare interruption")
	}
	must(t, os.WriteFile(filepath.Join(f.s.Root(), g.ID, initializationStage(g.ID, "executable")), []byte("first foreign stage"), 0500))
	f.s.InitializationStep = nil
	stale := f.s
	read := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	stale.InitializationStep = func(at string) error {
		if at == "intent_recorded" {
			close(read)
			<-release
		}
		return nil
	}
	done := make(chan error, 1)
	go func() {
		_, err := stale.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
		done <- err
	}()
	<-read
	_, err = f.s.Initialize("node", f.source, "next", testBoot, []int{1}, "operation")
	must(t, err)
	parent, err := os.OpenRoot(f.s.Data)
	must(t, err)
	defer parent.Close()
	var before initializationIntent
	must(t, readPrivate(parent, initializationFile, &before))
	must(t, os.WriteFile(filepath.Join(f.s.Root(), initializationStage(g.ID, "catalog")), []byte("later foreign stage"), 0600))
	unblock()
	must(t, <-done)
	var after initializationIntent
	must(t, readPrivate(parent, initializationFile, &after))
	if len(after.Quarantines) != len(before.Quarantines)+1 {
		t.Fatalf("stale retry overwrote durable custody: before=%d after=%d", len(before.Quarantines), len(after.Quarantines))
	}
	for _, old := range before.Quarantines {
		found := false
		for _, q := range after.Quarantines {
			if q == old {
				found = true
			}
		}
		if !found {
			t.Fatal("lost prior custody", old)
		}
	}
}
