package agentinstall

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
)

const testBoot = "darwin:11111111-1111-1111-1111-111111111111"
const nextBoot = "darwin:22222222-2222-2222-2222-222222222222"

type fakeManager struct {
	states      map[string]launchagent.State
	disabled    map[string]bool
	mutations   []string
	nextPID     int
	failDisable bool
}

func (f *fakeManager) run(_ context.Context, _ string, args ...string) ([]byte, error) {
	label := filepath.Base(args[len(args)-1])
	label = strings.TrimSuffix(label, ".plist")
	switch args[0] {
	case "print":
		s, ok := f.states[label]
		if !ok || !s.Loaded {
			return nil, launchagent.ErrServiceNotFound
		}
		state := "waiting"
		if s.Running {
			state = "running"
		}
		return fmt.Appendf(nil, "state = %s\npid = %d\n", state, s.PID), nil
	case "enable":
		delete(f.disabled, label)
	case "disable":
		if f.failDisable {
			return nil, errors.New("disable failed")
		}
		f.disabled[label] = true
	case "bootstrap":
		if f.states[label].Loaded {
			return nil, errors.New("duplicate label")
		}
		f.nextPID++
		f.states[label] = launchagent.State{Loaded: true, Running: true, PID: f.nextPID}
	case "bootout":
		delete(f.states, label)
	case "kickstart":
		return nil, errors.New("unexpected kickstart")
	default:
		return nil, fmt.Errorf("unexpected launchctl %v", args)
	}
	f.mutations = append(f.mutations, args[0]+" "+label)
	return nil, nil
}

type fixture struct {
	starts    map[int]string
	lifetimes map[string]func()
	s         Store
	f         *fakeManager
	source    string
	base      launchagent.Agent
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(home, "data")
	plists := filepath.Join(home, "LaunchAgents")
	for _, p := range []string{data, plists} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	source := filepath.Join(home, "candidate")
	if err := os.WriteFile(source, []byte("immutable executable"), 0700); err != nil {
		t.Fatal(err)
	}
	f := &fakeManager{states: map[string]launchagent.State{}, disabled: map[string]bool{}, nextPID: 100}
	starts := map[int]string{}
	return &fixture{starts: starts, lifetimes: map[string]func(){}, s: Store{Data: data, LaunchAgents: plists, Manager: launchagent.Manager{Run: f.run, UID: 501}, ProcessStartIdentity: func(pid int) (string, error) { return starts[pid], nil }}, f: f, source: source, base: launchagent.Agent{Label: "dev.wefty.node-agent", Program: source, WorkingDirectory: data, LogPath: filepath.Join(home, "agent.log"), Arguments: []string{"--profile=" + filepath.Join(home, "profile.json")}, ExitTimeOut: 95, ProcessType: "Interactive"}}
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func (f *fixture) prepare(t *testing.T, first bool) Generation {
	t.Helper()
	var g Generation
	var err error
	if first {
		g, err = f.s.Initialize("node", f.source, "v1", testBoot, []int{1})
	} else {
		g, err = f.s.PrepareCandidate(f.source, "v2", testBoot, []int{1})
	}
	must(t, err)
	_, err = f.s.LoadCandidate(t.Context(), g.ID, f.base)
	must(t, err)
	f.starts[f.f.states[g.Label].PID] = fmt.Sprintf("process-start-%d", f.f.states[g.Label].PID)
	f.holdLifetime(t, g.ID)
	now := time.Now().UTC()
	binding := Binding{ProcessStartID: f.starts[f.f.states[g.Label].PID], PID: f.f.states[g.Label].PID, BootSessionID: g.CandidateBootSessionID, SystemBootID: testBoot, ReadyAt: now}
	root, close, err := f.s.open(false)
	must(t, err)
	phase := "registered"
	if g.Role == "" {
		phase = "candidate_waiting"
	}
	must(t, writePrivate(root, filepath.Join(g.ID, "ready.json"), Ready{Phase: phase, GenerationID: g.ID, NodeID: "node", Version: g.Version, Digest: g.Digest, Binding: binding}))
	close()
	_, err = f.s.BindReady(t.Context(), g.ID, binding, g.CreatedAt)
	must(t, err)
	c, err := f.s.Read()
	must(t, err)
	return c.Generations[g.ID]
}
func (f *fixture) promote(t *testing.T, g Generation) {
	t.Helper()
	c, err := f.s.Read()
	must(t, err)
	r, err := f.s.BeginPromotion(g.ID, testBoot, "promote-"+g.ID, c.Revision, 1, emptyResidents(c))
	must(t, err)
	must(t, f.s.CommitPromotion(t.Context(), r.ID, r.L1OperationID, testBoot, 1))
}
func TestThreeGenerationsKeepLabelsExecutablesAndRebootOnlyPrimary(t *testing.T) {
	f := newFixture(t)
	g1 := f.prepare(t, true)
	g2 := f.prepare(t, false)
	f.promote(t, g2)
	g3 := f.prepare(t, false)
	// Candidate and two predecessors are loaded simultaneously, with original PIDs.
	for _, g := range []Generation{g1, g2, g3} {
		if !f.f.states[g.Label].Running {
			t.Fatalf("generation not running: %s", g.ID)
		}
		b, err := os.ReadFile(g.Executable)
		must(t, err)
		if string(b) != "immutable executable" {
			t.Fatal("binary changed")
		}
	}
	f.promote(t, g3)
	c, err := f.s.Read()
	must(t, err)
	if c.Primary != g3.ID || len(c.Generations) != 3 {
		t.Fatalf("bad catalog: %+v", c)
	}
	folder, err := os.OpenRoot(f.s.LaunchAgents)
	must(t, err)
	defer folder.Close()
	for _, g := range []Generation{g1, g2, g3} {
		payload, err := os.ReadFile(g.Plist)
		must(t, err)
		if !strings.Contains(string(payload), "--agent-generation-id="+g.ID) || strings.Contains(string(payload), "AbandonProcessGroup") {
			t.Fatal("wrong plist arguments or keys")
		}
		before := len(f.f.mutations)
		action, err := f.s.Manager.Converge(t.Context(), folder, g.Plist, g.Label, payload)
		must(t, err)
		if action != launchagent.Unchanged || len(f.f.mutations) != before {
			t.Fatal("matching convergence disturbed live generation")
		}
		_, err = f.s.Manager.Converge(t.Context(), folder, g.Plist, g.Label, append(payload, ' '))
		if err == nil || len(f.f.mutations) != before {
			t.Fatal("changed convergence disturbed generation")
		}
		if g.ID == g3.ID {
			if f.f.disabled[g.Label] || !strings.Contains(string(payload), "<key>RunAtLoad</key>\n  <true/>") {
				t.Fatal("primary not bootable")
			}
			f.releaseLifetime(g.ID)
			release, err := f.s.Startup(g.ID, "node", g.Executable, nextBoot)
			must(t, err)
			release()
		} else {
			if !f.f.disabled[g.Label] || !strings.Contains(string(payload), "<key>KeepAlive</key>\n  <false/>") {
				t.Fatal("retiree not persistently suppressed")
			}
			_, err := f.s.Startup(g.ID, "node", g.Executable, nextBoot)
			if !errors.Is(err, ErrStartupSuppressed) {
				t.Fatalf("retiree started after reboot: %v", err)
			}
			_, err = f.s.Startup(g.ID, "node", g.Executable, testBoot)
			if !errors.Is(err, ErrStartupSuppressed) {
				t.Fatal("stale loaded definition restarted retiree")
			}
		}
	}
	for _, mutation := range f.f.mutations {
		if strings.HasPrefix(mutation, "bootout ") || strings.HasPrefix(mutation, "kickstart ") {
			t.Fatalf("overlap ended a generation: %s", mutation)
		}
	}
}
func TestReadinessRejectsStaleGenerationProcessBootAndTime(t *testing.T) {
	for _, field := range []string{"generation", "digest", "version", "pid", "boot_session", "system_boot", "time", "manager_pid", "mode", "symlink"} {
		t.Run(field, func(t *testing.T) {
			f := newFixture(t)
			g := f.prepare(t, true)
			c, err := f.s.Read()
			must(t, err)
			var r Ready
			root, close, err := f.s.open(false)
			must(t, err)
			must(t, readPrivate(root, filepath.Join(g.ID, "ready.json"), &r))
			switch field {
			case "generation":
				r.GenerationID = strings.Repeat("0", 32)
			case "digest":
				r.Digest = "bad"
			case "version":
				r.Version = "old"
			case "pid":
				r.PID++
			case "boot_session":
				r.BootSessionID = "stale"
			case "system_boot":
				r.SystemBootID = nextBoot
			case "time":
				r.ReadyAt = g.CreatedAt.Add(-time.Second)
			case "manager_pid":
				s := f.f.states[g.Label]
				s.PID++
				f.f.states[g.Label] = s
			}
			must(t, writePrivate(root, filepath.Join(g.ID, "ready.json"), r))
			close()
			if field == "mode" {
				must(t, os.Chmod(f.s.ReadyPath(g.ID), 0644))
			}
			if field == "symlink" {
				must(t, os.Rename(f.s.ReadyPath(g.ID), f.s.ReadyPath(g.ID)+"-target"))
				must(t, os.Symlink(f.s.ReadyPath(g.ID)+"-target", f.s.ReadyPath(g.ID)))
			}
			_, err = f.s.BindReady(t.Context(), g.ID, *c.Generations[g.ID].Process, g.CreatedAt)
			if err == nil {
				t.Fatal("accepted stale readiness")
			}
		})
	}
}
func TestCandidateStartupIsSingleUseBootBoundAndPendingPromotionBlocksPrimary(t *testing.T) {
	f := newFixture(t)
	primary := f.prepare(t, true)
	candidate := f.prepare(t, false)
	_, err := f.s.Startup(candidate.ID, "node", candidate.Executable, nextBoot)
	if !errors.Is(err, ErrStartupSuppressed) {
		t.Fatal(err)
	}
	f.releaseLifetime(candidate.ID)
	release, err := f.s.Startup(candidate.ID, "node", candidate.Executable, testBoot)
	must(t, err)
	_, err = f.s.Startup(candidate.ID, "node", candidate.Executable, testBoot)
	if err == nil {
		t.Fatal("duplicate process lock allowed")
	}
	release()
	_, err = f.s.Startup(candidate.ID, "node", candidate.Executable, testBoot)
	if !errors.Is(err, ErrStartupSuppressed) {
		t.Fatalf("candidate restarted: %v", err)
	}
	c, err := f.s.Read()
	must(t, err)
	f.holdLifetime(t, candidate.ID)
	r, err := f.s.BeginPromotion(candidate.ID, testBoot, "operation", c.Revision, 1, emptyResidents(c))
	must(t, err)
	_, err = f.s.Startup(primary.ID, "node", primary.Executable, nextBoot)
	if err == nil {
		t.Fatal("unresolved cutover started stale primary")
	}
	f.f.failDisable = true
	before := len(f.f.mutations)
	if f.s.CommitPromotion(t.Context(), r.ID, r.L1OperationID, testBoot, 1) == nil {
		t.Fatal("ignored suppression failure")
	}
	c, err = f.s.Read()
	must(t, err)
	if c.PendingReceipt != r.ID || c.Primary != primary.ID || len(f.f.mutations) != before {
		t.Fatal("failed promotion lost its fence")
	}
}
func TestFinishedGenerationCleanupNeedsProofProcessAbsenceAndKeepsOtherGenerations(t *testing.T) {
	f := newFixture(t)
	old := f.prepare(t, true)
	primary := f.prepare(t, false)
	f.promote(t, primary)
	c, err := f.s.Read()
	must(t, err)
	proof := CompletionProof{GenerationID: old.ID, CatalogRevision: c.Revision, Binding: *old.Process, PayloadsAbsent: true, L1SessionClosed: true, EvidenceSafe: true, EvidenceDigest: strings.Repeat("a", 64)}
	held := proof
	held.L1SessionClosed = false
	if f.s.RemoveFinished(t.Context(), held) == nil {
		t.Fatal("removed live resident work")
	}
	if f.s.RemoveFinished(t.Context(), proof) == nil {
		t.Fatal("removed running process")
	}
	s := f.f.states[old.Label]
	f.releaseLifetime(old.ID)
	s.Running = false
	s.PID = 0
	f.f.states[old.Label] = s
	release, err := lockDirectory(filepath.Join(f.s.Root(), old.ID))
	must(t, err)
	if f.s.RemoveFinished(t.Context(), proof) == nil {
		t.Fatal("ignored live generation process lock")
	}
	release()
	must(t, f.s.RemoveFinished(t.Context(), proof))
	must(t, f.s.RemoveFinished(t.Context(), proof))
	for _, p := range []string{old.Plist, old.Executable, filepath.Dir(old.Executable)} {
		if _, err := os.Lstat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("left retired file %s: %v", p, err)
		}
	}
	if !f.f.states[primary.Label].Running {
		t.Fatal("cleanup stopped another generation")
	}
	_, err = os.Stat(primary.Executable)
	must(t, err)
	c, err = f.s.Read()
	must(t, err)
	if len(c.Generations) != 1 {
		t.Fatal("catalog kept deleted generation")
	}
}
func TestPrivateRecordsOwnershipAndImmutableBinary(t *testing.T) {
	f := newFixture(t)
	g := f.prepare(t, true)
	info, err := os.Stat(filepath.Join(f.s.Root(), catalogName))
	must(t, err)
	if info.Mode().Perm() != 0600 {
		t.Fatal("public catalog")
	}
	must(t, os.Chmod(g.Executable, 0700))
	must(t, os.WriteFile(g.Executable, []byte("changed"), 0500))
	must(t, os.Chmod(g.Executable, 0500))
	_, err = f.s.Startup(g.ID, "node", g.Executable, testBoot)
	if err == nil {
		t.Fatal("changed executable started")
	}
	must(t, os.Chmod(filepath.Join(f.s.Root(), catalogName), 0644))
	if _, err := f.s.Read(); err == nil {
		t.Fatal("public catalog accepted")
	}
	if !hostboot.ValidIdentity(testBoot) {
		t.Fatal("test boot invalid")
	}
}

func emptyResidents(c Catalog) PromotionEvidence {
	e := PromotionEvidence{IntentRevision: 1, Residents: map[string][]ResidentAttempt{}, ServiceRecoveryKeys: map[string]string{}}
	for id, g := range c.Generations {
		e.Residents[id] = append([]ResidentAttempt{}, g.ResidentAttempts...)
	}
	return e
}

func TestCleanupResumesFromReceiptAndRefusesChangedOwnedFiles(t *testing.T) {
	for _, scenario := range []string{"unknown-evidence", "changed-plist", "interrupted"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			old := f.prepare(t, true)
			next := f.prepare(t, false)
			f.promote(t, next)
			c, err := f.s.Read()
			must(t, err)
			proof := CompletionProof{GenerationID: old.ID, CatalogRevision: c.Revision, Binding: *old.Process, PayloadsAbsent: true, L1SessionClosed: true, EvidenceSafe: true, EvidenceDigest: strings.Repeat("b", 64)}
			state := f.f.states[old.Label]
			f.releaseLifetime(old.ID)
			state.Running = false
			state.PID = 0
			f.f.states[old.Label] = state
			before := len(f.f.mutations)
			switch scenario {
			case "unknown-evidence":
				must(t, os.WriteFile(filepath.Join(filepath.Dir(old.Executable), "pending-result"), []byte("keep"), 0600))
			case "changed-plist":
				must(t, os.WriteFile(old.Plist, []byte("owner changed"), 0644))
			case "interrupted":
				original := f.s.Manager.Run
				f.s.Manager.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
					if args[0] == "bootout" {
						return nil, errors.New("interrupted unload")
					}
					return original(ctx, name, args...)
				}
				if f.s.RemoveFinished(t.Context(), proof) == nil {
					t.Fatal("expected interrupted cleanup")
				}
				// Simulate interruption after one owned file deletion. The durable receipt
				// permits retry without demanding that a deleted executable reappear.
				must(t, os.Remove(old.Executable))
				f.s.Manager.Run = original
				must(t, f.s.RemoveFinished(t.Context(), proof))
				return
			}
			if f.s.RemoveFinished(t.Context(), proof) == nil {
				t.Fatal("cleanup accepted unknown or changed ownership")
			}
			if len(f.f.mutations) != before {
				t.Fatal("cleanup stopped label before verifying owned files")
			}
			_, err = os.Stat(old.Executable)
			must(t, err)
		})
	}
}

func TestLoadedGenerationSelectionCannotRewriteItsRecord(t *testing.T) {
	f := newFixture(t)
	f.base.Environment = map[string]string{"WEFTY_LAUNCH_UNIT": "dev.wefty.node-agent"}
	g := f.prepare(t, true)
	c, err := f.s.Read()
	must(t, err)
	selected, err := f.s.Agent(f.base, g)
	must(t, err)
	if selected.Environment["WEFTY_LAUNCH_UNIT"] != g.Label || f.base.Environment["WEFTY_LAUNCH_UNIT"] != "dev.wefty.node-agent" {
		t.Fatal("generation environment changed legacy input or kept wrong label")
	}
	changed := f.base
	changed.Arguments = append(append([]string{}, changed.Arguments...), "--changed-input")
	before := len(f.f.mutations)
	if _, err := f.s.LoadCandidate(t.Context(), g.ID, changed); err == nil {
		t.Fatal("replaced loaded generation")
	}
	after, err := f.s.Read()
	must(t, err)
	if !sameGeneration(c.Generations[g.ID], after.Generations[g.ID]) || c.Revision != after.Revision || len(f.f.mutations) != before {
		t.Fatal("refused load rewrote retained generation")
	}
	state := f.f.states[g.Label]
	state.Running = false
	state.PID = 0
	f.f.states[g.Label] = state
	folder, err := os.OpenRoot(f.s.LaunchAgents)
	must(t, err)
	defer folder.Close()
	payload, err := os.ReadFile(g.Plist)
	must(t, err)
	if _, err := f.s.Manager.Converge(t.Context(), folder, g.Plist, g.Label, payload); err == nil {
		t.Fatal("restarted stopped generation")
	}
}

func TestCandidateWaitsWithoutRegistrationUntilCommittedPrimary(t *testing.T) {
	f := newFixture(t)
	f.prepare(t, true)
	g := f.prepare(t, false)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if !errors.Is(f.s.AwaitActivation(ctx, g.ID), context.Canceled) {
		t.Fatal("candidate escaped waiting before promotion")
	}
	c, err := f.s.Read()
	must(t, err)
	r, err := f.s.BindReady(t.Context(), g.ID, *g.Process, g.CreatedAt)
	must(t, err)
	if r.Phase != "candidate_waiting" || r.BootSessionID != g.CandidateBootSessionID {
		t.Fatal("candidate readiness did not bind prepared process")
	}
	c, err = f.s.Read()
	must(t, err)
	receipt, err := f.s.BeginPromotion(g.ID, testBoot, "promote-waiter", c.Revision, 1, emptyResidents(c))
	must(t, err)
	if !errors.Is(f.s.AwaitActivation(ctx, g.ID), context.Canceled) {
		t.Fatal("pending candidate escaped waiting")
	}
	must(t, f.s.CommitPromotion(t.Context(), receipt.ID, receipt.L1OperationID, testBoot, 1))
	must(t, f.s.AwaitActivation(t.Context(), g.ID))
}

func TestFurtherUpgradeRetainsExactResidentsOfEveryRetiree(t *testing.T) {
	f := newFixture(t)
	first := f.prepare(t, true)
	second := f.prepare(t, false)
	c, err := f.s.Read()
	must(t, err)
	e := emptyResidents(c)
	resident := ResidentAttempt{JobID: "job-1", AttemptID: "attempt-1", BootSessionID: first.Process.BootSessionID, AuthorityGeneration: 7, FencingToken: "fence-1"}
	secondResident := ResidentAttempt{JobID: "job-0", AttemptID: "attempt-0", BootSessionID: first.Process.BootSessionID, AuthorityGeneration: 7, FencingToken: "fence-0"}
	e.Residents[first.ID] = []ResidentAttempt{resident, secondResident}
	r, err := f.s.BeginPromotion(second.ID, testBoot, "first-operation", c.Revision, 1, e)
	must(t, err)
	must(t, f.s.CommitPromotion(t.Context(), r.ID, r.L1OperationID, testBoot, 1))
	third := f.prepare(t, false)
	c, err = f.s.Read()
	must(t, err)
	e = emptyResidents(c)
	e.Residents[first.ID] = []ResidentAttempt{}
	if _, err := f.s.BeginPromotion(third.ID, testBoot, "bad-operation", c.Revision, 1, e); err == nil {
		t.Fatal("accepted omitted retiring resident")
	}
	c, err = f.s.Read()
	must(t, err)
	if c.PendingReceipt != "" {
		t.Fatal("refusal published activation intent")
	}
	e = emptyResidents(c)
	e.L1CatalogRevision = 1
	r, err = f.s.BeginPromotion(third.ID, testBoot, "second-operation", c.Revision, 1, e)
	must(t, err)
	must(t, f.s.CommitPromotion(t.Context(), r.ID, r.L1OperationID, testBoot, 2))
	c, err = f.s.Read()
	must(t, err)
	if len(c.Generations[first.ID].ResidentAttempts) != 2 || c.Generations[first.ID].ResidentAttempts[0] != secondResident || c.Generations[first.ID].ResidentAttempts[1] != resident {
		t.Fatal("further upgrade changed resident authority identity")
	}
	proof := CompletionProof{GenerationID: first.ID, CatalogRevision: c.Revision, Binding: *first.Process, PayloadsAbsent: false, L1SessionClosed: false, EvidenceSafe: true, EvidenceDigest: strings.Repeat("c", 64)}
	if err := f.s.RemoveFinished(t.Context(), proof); err == nil {
		t.Fatal("removed retiree holding a resident Attempt")
	}
	if !f.f.states[first.Label].Running {
		t.Fatal("refusal stopped the resident owner")
	}
}

func TestCompletionReceiptAloneCannotAuthorizePrimaryOrIncompleteEvidence(t *testing.T) {
	for _, scenario := range []string{"primary", "incomplete-evidence"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			g := f.prepare(t, true)
			if scenario == "incomplete-evidence" {
				next := f.prepare(t, false)
				f.promote(t, next)
			}
			c, err := f.s.Read()
			must(t, err)
			g = c.Generations[g.ID]
			proof := CompletionProof{GenerationID: g.ID, CatalogRevision: c.Revision, Binding: *g.Process, PayloadsAbsent: true, L1SessionClosed: true, EvidenceSafe: scenario != "incomplete-evidence", EvidenceDigest: strings.Repeat("d", 64)}
			root, close, err := f.s.open(false)
			must(t, err)
			must(t, writePrivate(root, "finished-"+g.ID+".json", completionReceipt{Version: 1, Generation: g, Proof: proof, PlistDigest: g.PlistDigest}))
			close()
			state := f.f.states[g.Label]
			state.Running = false
			state.PID = 0
			f.f.states[g.Label] = state
			before := len(f.f.mutations)
			if err := f.s.RemoveFinished(t.Context(), proof); err == nil {
				t.Fatal("receipt authorized primary deletion or incomplete evidence")
			}
			if len(f.f.mutations) != before {
				t.Fatal("invalid receipt reached service-manager mutation")
			}
			_, err = os.Stat(g.Executable)
			must(t, err)
		})
	}
}

func (f *fixture) holdLifetime(t *testing.T, id string) {
	t.Helper()
	release, err := lockDirectory(filepath.Join(f.s.Root(), id))
	must(t, err)
	f.lifetimes[id] = release
	t.Cleanup(func() { f.releaseLifetime(id) })
}
func (f *fixture) releaseLifetime(id string) {
	if release := f.lifetimes[id]; release != nil {
		delete(f.lifetimes, id)
		release()
	}
}

func TestBindReadyRejectsReusedPID(t *testing.T) {
	f := newFixture(t)
	g := f.prepare(t, true)
	before, err := f.s.Read()
	must(t, err)
	// The label, PID, boot session and saved ready file are unchanged. Only the
	// independent kernel observation now describes a replacement process.
	f.starts[g.Process.PID] = "replacement-process-start"
	if _, err := f.s.BindReady(t.Context(), g.ID, *g.Process, g.CreatedAt); err == nil {
		t.Fatal("accepted reused PID with a different process incarnation")
	}
	after, err := f.s.Read()
	must(t, err)
	if before.Revision != after.Revision || !sameGeneration(before.Generations[g.ID], after.Generations[g.ID]) {
		t.Fatal("rejected readiness changed the catalog")
	}
}

func TestCommitPromotionRejectsReusedPID(t *testing.T) {
	f := newFixture(t)
	primary := f.prepare(t, true)
	candidate := f.prepare(t, false)
	c, err := f.s.Read()
	must(t, err)
	r, err := f.s.BeginPromotion(candidate.ID, testBoot, "promotion-reused-pid", c.Revision, 1, emptyResidents(c))
	must(t, err)
	f.starts[candidate.Process.PID] = "replacement-process-start"
	mutations := len(f.f.mutations)
	if err := f.s.CommitPromotion(t.Context(), r.ID, r.L1OperationID, testBoot, 1); err == nil {
		t.Fatal("committed promotion using a dead process's readiness with a reused PID")
	}
	after, err := f.s.Read()
	must(t, err)
	if after.Primary != primary.ID || after.PendingReceipt != r.ID || len(f.f.mutations) != mutations {
		t.Fatal("rejected promotion changed primary or touched labels")
	}
}

func TestReadinessRejectsFreeLifetimeLock(t *testing.T) {
	for _, at := range []string{"BindReady", "CommitPromotion"} {
		t.Run(at, func(t *testing.T) {
			f := newFixture(t)
			f.prepare(t, true)
			candidate := f.prepare(t, false)
			c, err := f.s.Read()
			must(t, err)
			var receipt ActivationReceipt
			if at == "CommitPromotion" {
				receipt, err = f.s.BeginPromotion(candidate.ID, testBoot, "promotion-free-lock", c.Revision, 1, emptyResidents(c))
				must(t, err)
			}
			f.releaseLifetime(candidate.ID)
			if at == "BindReady" {
				_, err = f.s.BindReady(t.Context(), candidate.ID, *candidate.Process, candidate.CreatedAt)
			} else {
				err = f.s.CommitPromotion(t.Context(), receipt.ID, receipt.L1OperationID, testBoot, 1)
			}
			if err == nil {
				t.Fatal("accepted readiness with no generation lifetime lock holder")
			}
		})
	}
}

func TestReadinessFailsClosedWhenProcessStartIsUnavailable(t *testing.T) {
	for _, at := range []string{"BindReady", "CommitPromotion"} {
		for _, failure := range []string{"error", "empty"} {
			t.Run(at+"/"+failure, func(t *testing.T) {
				f := newFixture(t)
				primary := f.prepare(t, true)
				candidate := f.prepare(t, false)
				c, err := f.s.Read()
				must(t, err)
				var receipt ActivationReceipt
				if at == "CommitPromotion" {
					receipt, err = f.s.BeginPromotion(candidate.ID, testBoot, "promotion-unverified-process", c.Revision, 1, emptyResidents(c))
					must(t, err)
				}
				f.s.ProcessStartIdentity = func(int) (string, error) {
					if failure == "error" {
						return "", errors.New("kernel query denied")
					}
					return "", nil
				}
				before := len(f.f.mutations)
				if at == "BindReady" {
					_, err = f.s.BindReady(t.Context(), candidate.ID, *candidate.Process, candidate.CreatedAt)
				} else {
					err = f.s.CommitPromotion(t.Context(), receipt.ID, receipt.L1OperationID, testBoot, 1)
				}
				if err == nil {
					t.Fatal("accepted unavailable process incarnation evidence")
				}
				after, err := f.s.Read()
				must(t, err)
				if after.Primary != primary.ID || len(f.f.mutations) != before {
					t.Fatal("unverified incarnation changed primary or labels")
				}
			})
		}
	}
}

func TestPrimaryFenceFollowsEveryPromotionAndRejectsPendingCutover(t *testing.T) {
	f := newFixture(t)
	first := f.prepare(t, true)
	writes := 0
	write := func() error { writes++; return nil }
	must(t, f.s.WithPrimary(first.ID, write))
	second := f.prepare(t, false)
	if err := f.s.WithPrimary(second.ID, write); !errors.Is(err, ErrStartupSuppressed) {
		t.Fatalf("candidate write: %v", err)
	}
	f.promote(t, second)
	if err := f.s.WithPrimary(first.ID, write); !errors.Is(err, ErrStartupSuppressed) {
		t.Fatalf("retiree write: %v", err)
	}
	must(t, f.s.WithPrimary(second.ID, write))
	third := f.prepare(t, false)
	c, err := f.s.Read()
	must(t, err)
	r, err := f.s.BeginPromotion(third.ID, testBoot, "third-cutover", c.Revision, 1, emptyResidents(c))
	must(t, err)
	for _, id := range []string{first.ID, second.ID, third.ID} {
		if err := f.s.WithPrimary(id, write); !errors.Is(err, ErrStartupSuppressed) {
			t.Fatalf("pending cutover write: %v", err)
		}
	}
	must(t, f.s.CommitPromotion(t.Context(), r.ID, r.L1OperationID, testBoot, 1))
	must(t, f.s.WithPrimary(third.ID, write))
	if writes != 3 {
		t.Fatalf("Node-wide writes=%d", writes)
	}
}

func TestGenerationLifetimeAllowsOneRuntimeClaimAndPreservesReadinessLock(t *testing.T) {
	f := newFixture(t)
	g := f.prepare(t, true)
	f.releaseLifetime(g.ID)
	lifetime, err := f.s.StartupWithLifetime(g.ID, "node", g.Executable, testBoot)
	must(t, err)
	defer lifetime.Close()
	must(t, requireLifetimeLock(filepath.Join(f.s.Root(), g.ID)))
	if err := lifetime.ClaimRuntime("other-node", g.ID); err == nil {
		t.Fatal("accepted another Node's lifetime proof")
	}
	if err := lifetime.ClaimRuntime("node", "other-generation"); err == nil {
		t.Fatal("accepted another generation's lifetime proof")
	}
	var claims atomic.Int32
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if lifetime.ClaimRuntime("node", g.ID) == nil {
				claims.Add(1)
			}
		}()
	}
	wg.Wait()
	if claims.Load() != 1 {
		t.Fatalf("runtime claims=%d", claims.Load())
	}
	// Borrowing never re-flocks or releases the descriptor that readiness probes.
	_, err = f.s.BindReady(t.Context(), g.ID, *g.Process, g.CreatedAt)
	must(t, err)
	lifetime.Close()
	lifetime.Close()
	if err := lifetime.ClaimRuntime("node", g.ID); err == nil {
		t.Fatal("released lifetime proof admitted runtime")
	}
	if _, err := f.s.BindReady(t.Context(), g.ID, *g.Process, g.CreatedAt); err == nil {
		t.Fatal("readiness accepted released borrowed lifetime lock")
	}
}
