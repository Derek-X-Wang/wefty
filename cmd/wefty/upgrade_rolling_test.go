package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/upgradecompat"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

const rollingTestBoot = "darwin:11111111-1111-1111-1111-111111111111"

type rollingFixture struct {
	*upgradeFixture
	h                  *nodeCLIHarness
	paths              setupPaths
	install            agentinstall.Store
	primary            agentinstall.Generation
	claims             []*l1.Claim
	lifetimes          map[string]*agentinstall.GenerationLifetime
	promoted           bool
	heartbeatOnly      bool
	late               bool
	stopPrimaryControl func()
	beforeRecover      func(string)
	failPhase          string
}

func newRollingFixture(t *testing.T) *rollingFixture { return newRollingFixtureWithLegacy(t, false) }
func newRollingFixtureWithLegacy(t *testing.T, legacy bool) *rollingFixture {
	t.Helper()
	f := &rollingFixture{upgradeFixture: newUpgradeFixture(t), h: newNodeCLIHarness(t), lifetimes: map[string]*agentinstall.GenerationLifetime{}}
	f.paths, _ = resolveSetupPaths(f.home)
	f.h.clock.Advance(f.now.Sub(f.h.clock.Now()))
	f.operations.requestRolling = true
	f.operations.bootIdentity = func() (string, error) { return rollingTestBoot, nil }
	f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		label := strings.TrimSuffix(filepath.Base(args[len(args)-1]), ".plist")
		if args[0] == "disable" {
			f.events = append(f.events, "disable:"+label)
			return nil, nil
		}
		if args[0] == "bootout" && strings.HasPrefix(label, "dev.wefty.node-agent.g-") {
			t.Fatal("rolling booted out a surviving generation")
		}
		if args[0] == "bootstrap" && label == setupL3Label {
			f.restarted = true
			f.now = f.now.Add(50 * time.Millisecond)
			f.h.clock.Advance(50 * time.Millisecond)
		}
		output, err := f.launchd.run(ctx, name, args...)
		if args[0] == "print" && err == nil && strings.HasPrefix(label, "dev.wefty.node-agent.g-") {
			return []byte(fmt.Sprintf("state = running\npid = %d\n", os.Getpid())), nil
		}
		return output, err
	}
	f.install = agentinstall.Store{Data: f.paths.data, LaunchAgents: f.paths.launchAgents, Manager: launchagent.Manager{Run: f.host.launchctl, UID: 501}, ProcessStartIdentity: func(int) (string, error) { return "test-process-start", nil }}
	f.operations.generationStore = func(setupPaths) agentinstall.Store { return f.install }
	var err error
	if !legacy {
		f.primary, err = f.install.Initialize(f.node.NodeID, filepath.Join(f.paths.bin, "wefty-agent"), "v1", rollingTestBoot, []int{1})
		mustRolling(t, err)
	}
	profile, err := connprofile.Read(f.paths.profile)
	mustRolling(t, err)
	digests := map[string]string{}
	for _, name := range setupBinaries {
		digests[name], err = executableDigest(filepath.Join(f.paths.bin, name))
		mustRolling(t, err)
	}
	services, err := setupServices(f.paths, profile, digests, f.loginPATH+":"+f.paths.bin)
	mustRolling(t, err)
	if !legacy {
		_, err = f.install.LoadCandidate(t.Context(), f.primary.ID, services[2])
		mustRolling(t, err)
		f.holdReady(t, f.primary, "old-boot")
	}
	f.node, err = f.h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: f.node.NodeID}, contract.NodeRegistration{NodeID: f.node.NodeID, BootSessionID: "old-boot", AgentGenerationID: f.primary.ID, OS: "darwin", Architecture: "arm64", AgentVersion: "v1", RootInstanceID: "root", Capabilities: map[string]bool{"kind:process": true, "oneshot:restartable": true, "upgrade:retirement:v1": true}, CapabilityRevision: 1, CapabilityObservedAt: f.now}, l1.NodePolicy{Tags: []string{"rolling"}, MaxOneshotSlots: 8, MaxServiceSlots: 4}, true)
	mustRolling(t, err)
	originalCommand := f.operations.command
	f.operations.command = func(ctx context.Context, dir string, env []string, name string, args ...string) ([]byte, error) {
		if slices.Contains(args, "--profile-preflight") {
			return []byte("profile-readable plain\n"), nil
		}
		if slices.Contains(args, "--retirement-protocols") {
			return []byte(`{"version":1,"retirement_protocol_versions":[1]}`), nil
		}
		return originalCommand(ctx, dir, env, name, args...)
	}
	f.operations.interruptAttempt = func(context.Context, connprofile.Profile, string, l1.InterruptRestartableAttemptRequest) (l1.Job, error) {
		t.Fatal("rolling interrupted a one-shot")
		return l1.Job{}, nil
	}
	f.operations.wait = func(ctx context.Context, d time.Duration) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		f.now = f.now.Add(d)
		f.h.clock.Advance(d)
		c, err := f.install.Read()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		for _, g := range c.Generations {
			if g.Role == "" && f.launchd.loaded[g.Label] && f.lifetimes[g.ID] == nil {
				f.holdReady(t, g, g.CandidateBootSessionID)
			}
		}
		return nil
	}
	f.operations.generations = func(ctx context.Context, _ connprofile.Profile) (l1.NodeGenerationInventory, error) {
		return f.h.store.NodeGenerations(ctx, f.node.NodeID, f.node.NodeID)
	}
	f.operations.stageGeneration = func(ctx context.Context, _ connprofile.Profile, r contract.NodeRegistration) (l1.NodeGenerationInventory, error) {
		return f.h.store.StageNodeGeneration(ctx, f.node.NodeID, r)
	}
	f.operations.promoteGeneration = func(ctx context.Context, _ connprofile.Profile, r l1.PromoteNodeGenerationRequest) (l1.NodeGenerationInventory, error) {

		in, err := f.h.store.PromoteNodeGeneration(ctx, f.node.NodeID, f.node.NodeID, r)
		if err != nil {
			return in, err
		}
		f.promoted = true
		if f.failPhase == "promotion_requested" {
			return l1.NodeGenerationInventory{}, errors.New("injected lost accepted promotion reply")
		}
		for _, g := range in.Generations {
			if g.Role == l1.NodeGenerationRetiring {
				mustRolling(t, f.install.PublishRetirement(g))
			}
		}
		return in, nil
	}
	registeredReady := map[string]bool{f.primary.ID: true}
	f.operations.observe = func(ctx context.Context, _ connprofile.Profile) (l1.Node, error) {
		c, err := f.install.Read()
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return l1.Node{}, err
		}
		in, err := f.h.store.NodeGenerations(ctx, f.node.NodeID, f.node.NodeID)
		if err != nil {
			return l1.Node{}, err
		}
		current, err := f.h.store.GetNode(ctx, f.node.NodeID)
		if err != nil {
			return current, err
		}
		if len(in.Generations) == 0 && c.Primary != "" {
			g := c.Generations[c.Primary]
			if f.launchd.loaded[g.Label] {
				f.holdReady(t, g, g.CandidateBootSessionID)
				registration := current.NodeRegistration
				registration.AgentGenerationID = g.ID
				registration.BootSessionID = g.CandidateBootSessionID
				registration.AgentVersion = g.Version
				current, err = f.h.store.RegisterNode(ctx, fabric.Identity{NodeID: f.node.NodeID}, registration, l1.NodePolicy{Tags: []string{"rolling"}, MaxOneshotSlots: 8, MaxServiceSlots: 4}, true)
				if err != nil {
					return current, err
				}
			}
		}
		for _, g := range in.Generations {
			if g.Role == l1.NodeGenerationPrimary && c.Primary == g.AgentGenerationID && current.AgentGenerationID != g.AgentGenerationID {
				current, err = f.h.store.RegisterNode(ctx, fabric.Identity{NodeID: f.node.NodeID}, g.Registration, l1.NodePolicy{Tags: []string{"rolling"}, MaxOneshotSlots: 8, MaxServiceSlots: 4}, true)
				if err != nil {
					return current, err
				}
			}
		}
		if f.restarted {
			_, err = f.h.store.HeartbeatNode(ctx, f.node.NodeID, f.node.NodeID, current.BootSessionID)
			if err != nil {
				return current, err
			}
			for _, claim := range f.claims {
				if f.heartbeatOnly && f.promoted {
					continue
				}
				job, err := f.job(ctx, claim.Job.JobID)
				if err != nil {
					return current, err
				}
				for _, a := range job.Attempts {
					if a.AttemptID == claim.Lease.AttemptID && (a.State == contract.AttemptClaimed || a.State == contract.AttemptRunning) {
						_, err = f.h.store.RenewLease(ctx, f.node.NodeID, claim.Job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken)
						if err != nil {
							return current, err
						}
					}
				}
			}
		}
		if c.PendingReceipt == "" && c.Primary != "" && current.AgentGenerationID == c.Primary && f.launchd.loaded[c.Generations[c.Primary].Label] && f.lifetimes[c.Primary] != nil && !registeredReady[c.Primary] {
			g := c.Generations[c.Primary]
			mustRolling(t, f.install.PublishReady(g.ID, c.NodeID, g.Version, g.Executable, current.BootSessionID, rollingTestBoot))
			registeredReady[g.ID] = true
		}
		return f.h.store.GetNode(ctx, f.node.NodeID)
	}
	f.operations.readNode = f.operations.observe
	f.operations.readJob = func(ctx context.Context, _ connprofile.Profile, id string) (l1.Job, error) {
		return f.job(ctx, id)
	}
	f.operations.setClaims = func(ctx context.Context, _ connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
		if !r.ClaimsEnabled && f.late {
			f.late = false
			f.claim(t, "late", contract.JobClassOneShot, false)
		}
		return f.h.store.SetNodeClaimsByOperator(ctx, f.node.NodeID, "operator", r)
	}
	f.operations.generationControl = func(ctx context.Context, _ agentinstall.Store, r agentinstall.ControlRequest) (json.RawMessage, error) {
		if r.Action != "quiesce_services" {
			t.Fatal("unexpected control", r.Action)
		}
		var proof agent.UpgradeServiceQuiescence
		mustRolling(t, json.Unmarshal(r.Payload, &proof))
		persisted, err := readUpgradeDrain(f.paths.config)
		mustRolling(t, err)
		if !slices.Equal(persisted.Services, proof.Services) {
			t.Fatal("service inventory not persisted before stop")
		}
		for _, a := range proof.Services {
			for _, claim := range f.claims {
				if claim.Lease.AttemptID == a.AttemptID {
					_, err = f.h.store.CompleteAttempt(ctx, f.node.NodeID, a.JobID, a.AttemptID, l1.CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "quiesce-" + a.AttemptID, Result: l1.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt})
					mustRolling(t, err)
				}
			}
		}
		return json.Marshal(proof)
	}
	f.operations.recoverService = func(ctx context.Context, _ connprofile.Profile, id string, r l1.ServiceRestartRequest) (l1.Job, error) {
		if f.beforeRecover != nil {
			f.beforeRecover(id)
		}
		job, _, err := f.h.store.RestartService(ctx, id, r)
		return job, err
	}
	// Make a second invocation build distinct bytes, even at the same source
	// version: generation identity never relies on a version comparison.
	build := f.operations.build
	count := 0
	f.operations.build = func(ctx context.Context, source, dest, commit, path string) error {
		if err := build(ctx, source, dest, commit, path); err != nil {
			return err
		}
		count++
		file, err := os.OpenFile(filepath.Join(dest, "wefty-agent"), os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(file, "# build %d\n", count)
		return errors.Join(err, file.Close())
	}
	t.Cleanup(func() {
		for _, l := range f.lifetimes {
			l.Close()
		}
	})
	return f
}
func (f *rollingFixture) job(ctx context.Context, id string) (l1.Job, error) {
	job, err := f.h.store.GetJob(ctx, id)
	if err == nil {
		job.Attempts, err = f.h.store.ListJobAttempts(ctx, id)
	}
	return job, err
}
func mustRolling(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func (f *rollingFixture) holdReady(t *testing.T, g agentinstall.Generation, boot string) {
	t.Helper()
	c, err := f.install.Read()
	mustRolling(t, err)
	lifetime, err := f.install.StartupWithLifetime(g.ID, c.NodeID, g.Executable, rollingTestBoot)
	mustRolling(t, err)
	f.lifetimes[g.ID] = lifetime
	mustRolling(t, f.install.PublishReady(g.ID, c.NodeID, g.Version, g.Executable, boot, rollingTestBoot))
	if g.Role == "primary" {
		_, err = f.install.BindReady(t.Context(), g.ID, agentinstall.Binding{PID: os.Getpid(), BootSessionID: boot, SystemBootID: rollingTestBoot}, time.Now().Add(-time.Second))
		mustRolling(t, err)
	}
}
func (f *rollingFixture) claim(t *testing.T, key, class string, restartable bool) *l1.Claim {
	t.Helper()
	spec := nodeCLIJobSpec(key, class, "rolling")
	spec.Restartable = restartable
	if class == contract.JobClassService {
		spec.Restart = contract.RestartNever
	}
	_, _, err := f.h.store.CreateJob(t.Context(), spec)
	mustRolling(t, err)
	node, err := f.h.store.GetNode(t.Context(), f.node.NodeID)
	mustRolling(t, err)
	claim, err := f.h.store.ClaimJob(t.Context(), f.node.NodeID, f.node.NodeID, node.BootSessionID, class)
	mustRolling(t, err)
	if claim == nil {
		t.Fatal("missing claim")
	}
	_, err = f.h.store.StartAttempt(t.Context(), f.node.NodeID, claim.Job.JobID, claim.Lease.AttemptID, l1.StartedRequest{FencingToken: claim.Lease.FencingToken})
	mustRolling(t, err)
	f.claims = append(f.claims, claim)
	return claim
}
func TestUpgradeRollingRealL1MixedLateClaimsAndFurtherOverlap(t *testing.T) {
	f := newRollingFixture(t)
	a := f.claim(t, "ordinary", contract.JobClassOneShot, false)
	b := f.claim(t, "restartable", contract.JobClassOneShot, true)
	f.late = true
	report, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	if report.StoppingRestartable != 0 || report.RestartPending != 0 || len(report.RunningWork) != 3 {
		t.Fatalf("rolling lost work or interrupted: %+v", report)
	}
	receipt, err := readUpgradeRolling(f.paths)
	mustRolling(t, err)
	if receipt.Phase != "complete" || len(receipt.RenewalProof) != 3 || receipt.Promotion == nil || len(receipt.Promotion.ResidentAttempts) != 3 {
		t.Fatalf("incomplete receipt: %+v", receipt)
	}
	for _, claim := range []*l1.Claim{a, b} {
		job, err := f.job(t.Context(), claim.Job.JobID)
		mustRolling(t, err)
		if len(job.Attempts) != 1 || job.CurrentAttemptID != claim.Lease.AttemptID {
			t.Fatal("one-shot identity changed", job)
		}
	}
	f.claim(t, "new-primary", contract.JobClassOneShot, false)
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	in, err := f.h.store.NodeGenerations(t.Context(), f.node.NodeID, f.node.NodeID)
	mustRolling(t, err)
	primary, retiring := 0, 0
	for _, g := range in.Generations {
		if g.Role == l1.NodeGenerationPrimary {
			primary++
		}
		if g.Role == l1.NodeGenerationRetiring {
			retiring++
		}
	}
	if primary != 1 || retiring != 2 {
		t.Fatalf("cohort primary=%d retiring=%d", primary, retiring)
	}
}
func TestUpgradeRollingHeartbeatOnlyAndInterruptedReceipts(t *testing.T) {
	for _, mode := range []string{"heartbeat-only", "promotion_requested"} {
		t.Run(mode, func(t *testing.T) {
			f := newRollingFixture(t)
			f.claim(t, "resident", contract.JobClassOneShot, false)
			f.heartbeatOnly = mode == "heartbeat-only"
			if !f.heartbeatOnly {
				f.failPhase = mode
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil {
				t.Fatal("unproved activation passed")
			}
			receipt, err := readUpgradeRolling(f.paths)
			mustRolling(t, err)
			if receipt.Promotion == nil || receipt.Phase == "complete" {
				t.Fatalf("missing resumable exact promotion receipt: %+v", receipt)
			}
			_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil || commandExitCode(err) != 1 {
				t.Fatal("unverified recovery did not keep its operational cause", err)
			}
			node, err := f.h.store.GetNode(t.Context(), f.node.NodeID)
			mustRolling(t, err)
			if node.ClaimsEnabled {
				t.Fatal("unproved cutover restored claims")
			}
		})
	}
}
func TestUpgradeRollingServiceIntentRacesStayStopped(t *testing.T) {
	f := newRollingFixture(t)
	f.claim(t, "oneshot", contract.JobClassOneShot, false)
	running := f.claim(t, "running-service", contract.JobClassService, false)
	stopped := f.claim(t, "stopped-service", contract.JobClassService, false)
	removed := f.claim(t, "removed-service", contract.JobClassService, false)
	f.beforeRecover = func(id string) {
		var err error
		if id == stopped.Job.JobID {
			_, err = f.h.store.SetServiceDesiredState(t.Context(), id, contract.ServiceDesiredStopped)
		}
		if id == removed.Job.JobID {
			_, err = f.h.store.RemoveService(t.Context(), id)
		}
		mustRolling(t, err)
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	for _, c := range []*l1.Claim{running, stopped, removed} {
		job, err := f.job(t.Context(), c.Job.JobID)
		mustRolling(t, err)
		if c == running && job.State != contract.JobQueued {
			t.Fatal("desired restart=never service not recovered", job)
		}
		if c == stopped && job.DesiredState != contract.ServiceDesiredStopped {
			t.Fatal("stopped service restarted")
		}
		if c == removed && job.Removal == nil {
			t.Fatal("removed service restarted")
		}
	}
}
func TestUpgradeRollingGateRefusesIncompatibleCohort(t *testing.T) {
	f := newRollingFixture(t)
	command := f.operations.command
	f.operations.command = func(ctx context.Context, dir string, env []string, name string, args ...string) ([]byte, error) {
		if slices.Contains(args, "--retirement-protocols") {
			return []byte(`{"version":1,"retirement_protocol_versions":[2]}`), nil
		}
		return command(ctx, dir, env, name, args...)
	}
	f.launchd.commands = nil
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	var refused *upgradeRollingRefusedError
	if !errors.As(err, &refused) || refused.Reason != upgradecompat.IncompatibleProtocols || commandExitCode(err) != exitConflict {
		t.Fatal("wrong gate refusal", err)
	}
	if len(f.launchd.mutating()) != 0 {
		t.Fatal("refused gate changed services")
	}
	if _, err = os.Stat(filepath.Join(f.paths.config, upgradeDrainFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refused gate closed admission")
	}
}

func TestUpgradeDefaultNeverCallsRollingGate(t *testing.T) {
	f := newUpgradeFixture(t)
	f.operations.generations = func(context.Context, connprofile.Profile) (l1.NodeGenerationInventory, error) {
		t.Fatal("default upgrade consulted rolling gate")
		return l1.NodeGenerationInventory{}, nil
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	paths, _ := resolveSetupPaths(f.home)
	if _, err := readUpgradeRolling(paths); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("default upgrade wrote rolling receipt", err)
	}
}

func TestUpgradeRollingLegacyDrainEnrollsFirstPrimary(t *testing.T) {
	f := newRollingFixtureWithLegacy(t, true)
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	c, err := f.install.Read()
	mustRolling(t, err)
	if len(c.Generations) != 1 || c.Generations[c.Primary].Role != "primary" || c.Generations[c.Primary].Process == nil {
		t.Fatal("legacy drain did not enroll a ready first primary", c)
	}
	if _, err = os.Stat(filepath.Join(f.paths.launchAgents, setupNodeAgentLabel+".plist")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("legacy bootstrap can restart beside primary", err)
	}
	r, err := readUpgradeRolling(f.paths)
	mustRolling(t, err)
	if r.Phase != "complete" {
		t.Fatal("enrollment not complete", r.Phase)
	}
}

func TestUpgradeRollingProofKeepsDisappearedAttempts(t *testing.T) {
	for _, state := range []string{"renewed", "terminal", "missing", "lost", "interrupted", "heartbeat-only"} {
		t.Run(state, func(t *testing.T) {
			after := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
			now := after.Add(time.Second)
			a := l1.Attempt{AttemptID: "original", State: contract.AttemptRunning, UpdatedAt: after, LeaseExpiresAt: after.Add(l1.DefaultLeaseDuration)}
			switch state {
			case "renewed":
				a.UpdatedAt = now
				a.LeaseExpiresAt = now.Add(l1.DefaultLeaseDuration)
			case "terminal":
				exit := 0
				a.State = contract.AttemptSucceeded
				a.Result = &l1.ProcessResult{ExitCode: &exit}
			case "lost":
				a.State = contract.AttemptLost
			case "interrupted":
				a.State = contract.AttemptInterrupted
			}
			r := rollingUpgrade{receipt: upgradeRollingReceipt{Candidate: agentinstall.Generation{Version: "next", CandidateBootSessionID: "next-boot"}, Preserved: []l1.NodeActiveAttempt{{JobID: "job", AttemptID: "original", Kind: contract.JobKindProcess, Class: contract.JobClassOneShot}}}, op: upgradeHost{now: func() time.Time { return now }, wait: func(context.Context, time.Duration) error { now = now.Add(time.Second); return nil }, readJob: func(context.Context, connprofile.Profile, string) (l1.Job, error) {
				attempts := []l1.Attempt{a}
				if state == "missing" {
					attempts = nil
				}
				return l1.Job{Attempts: attempts}, nil
			}, observe: func(context.Context, connprofile.Profile) (l1.Node, error) {
				return l1.Node{NodeRegistration: contract.NodeRegistration{BootSessionID: "next-boot", AgentVersion: "next"}, State: contract.NodeAlive, LastHeartbeatAt: now}, nil
			}}}
			err := r.waitSurvivors(t.Context(), l1.NodeGenerationInventory{}, after)
			if (err == nil) != (state == "renewed" || state == "terminal") {
				t.Fatalf("disappeared Attempt proof %s: %v", state, err)
			}
		})
	}
}

func TestUpgradeRollingEveryInterruptedStepIsReadable(t *testing.T) {
	for _, phase := range []string{"prepared", "admission_closed", "services_quiescing", "services_quiesced", "control_plane_renewed", "candidate_preparing", "candidate_loading", "candidate_ready", "candidate_staging", "promotion_requested", "promotion_committed", "catalog_reconciled", "retirement_acknowledged", "survivor_renewal_proved", "installation_completing", "installation_complete"} {
		t.Run(phase, func(t *testing.T) {
			f := newRollingFixture(t)
			f.claim(t, "resident", contract.JobClassOneShot, false)
			f.operations.rollingStep = func(got string) error {
				if got == phase {
					return errors.New("interrupted at " + phase)
				}
				return nil
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil {
				t.Fatal("injected interruption passed")
			}
			r, err := readUpgradeRolling(f.paths)
			mustRolling(t, err)
			if r.Phase != phase || r.NodeID != f.node.NodeID || len(r.BinaryDigests) != 4 || len(r.Preserved) != 1 {
				t.Fatalf("interruption lost phase or inputs: %+v", r)
			}
			if r.ActivationReceiptID != "" {
				_, err = f.install.ReadActivationReceipt(r.ActivationReceiptID)
				mustRolling(t, err)
			}
			if err = refuseInterruptedRolling(f.paths); err == nil {
				t.Fatal("incomplete activation allowed another upgrade")
			}
		})
	}
}

func TestUpgradeRollingProfilePreflightRequiresPositiveResponse(t *testing.T) {
	f := newRollingFixture(t)
	command := f.operations.command
	f.operations.command = func(ctx context.Context, dir string, env []string, name string, args ...string) ([]byte, error) {
		if slices.Contains(args, "--profile-preflight") {
			return []byte(f.commit + "\n"), nil
		}
		return command(ctx, dir, env, name, args...)
	}
	f.launchd.commands = nil
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	assertUpgradeControlOperational(t, err, "malformed plain profile preflight reply")
	if len(f.launchd.mutating()) != 0 {
		t.Fatal("ignored profile preflight changed installation", err)
	}
}

func TestUpgradeRollingGenerationOperationsUseInstalledAgent(t *testing.T) {
	f := newRollingFixture(t)
	promotionCommitted, releaseReply := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseReply) }) }
	runtime := exposeRollingL1WithPromotionReply(t, f, func() { close(promotionCommitted); <-releaseReply })
	t.Cleanup(release)
	p, err := connprofile.Read(f.paths.profile)
	mustRolling(t, err)
	op := upgradeOperations(f.host)
	op.generations = nil
	op.stageGeneration = nil
	op.promoteGeneration = nil
	op.generationControl = nil
	host := f.host
	host.upgrade = &op
	op = upgradeOperations(host)
	in, err := op.generations(t.Context(), p)
	mustRolling(t, err)
	if len(in.Generations) != 1 || in.Generations[0].AgentGenerationID != f.primary.ID {
		t.Fatal("agent inventory omitted primary", in)
	}
	node, err := f.h.store.GetNode(t.Context(), f.node.NodeID)
	mustRolling(t, err)
	node, err = f.h.store.SetNodeClaimsByOperator(t.Context(), f.node.NodeID, "operator", l1.NodeIntentRequest{ClaimsEnabled: false, IntentRevision: node.IntentRevision, Reason: "upgrade"})
	mustRolling(t, err)
	operation := "control-promotion"
	proof := agent.UpgradeServiceQuiescence{OperationID: operation, NodeID: node.NodeID, AgentGenerationID: f.primary.ID, BootSessionID: node.BootSessionID, IntentRevision: node.IntentRevision, IntentReason: node.IntentReason, Services: []l1.NodeActiveAttempt{}}
	mutation := agentinstall.ControlRequest{ID: operation, Action: "stage_generation", NodeID: node.NodeID, GenerationID: f.primary.ID, BootSessionID: node.BootSessionID, Payload: []byte(`{}`)}
	if _, err := op.generationControl(t.Context(), f.install, mutation); err == nil || !strings.Contains(err.Error(), "service barrier") {
		t.Fatal("mutation without service barrier accepted", err)
	}
	body, _ := json.Marshal(proof)
	_, err = op.generationControl(t.Context(), f.install, agentinstall.ControlRequest{ID: operation, Action: "quiesce_services", NodeID: node.NodeID, GenerationID: f.primary.ID, BootSessionID: node.BootSessionID, Payload: body})
	mustRolling(t, err)
	candidate, err := f.install.PrepareCandidate(filepath.Join(f.paths.bin, "wefty-agent"), "candidate", rollingTestBoot, []int{1})
	mustRolling(t, err)
	// The agent requires the same local readiness evidence used by activation.
	profile, err := connprofile.Read(f.paths.profile)
	mustRolling(t, err)
	services, err := setupServices(f.paths, profile, map[string]string{}, f.loginPATH)
	mustRolling(t, err)
	_, err = f.install.LoadCandidate(t.Context(), candidate.ID, services[2])
	mustRolling(t, err)
	f.holdReady(t, candidate, candidate.CandidateBootSessionID)
	_, err = f.install.BindReady(t.Context(), candidate.ID, agentinstall.Binding{PID: os.Getpid(), BootSessionID: candidate.CandidateBootSessionID, SystemBootID: rollingTestBoot}, candidate.CreatedAt)
	mustRolling(t, err)
	receipt := upgradeRollingReceipt{Version: 1, NodeID: node.NodeID, OperationID: operation}
	mustRolling(t, writeUpgradeRolling(f.paths, &receipt, "candidate_staging"))
	registration := node.NodeRegistration
	registration.AgentGenerationID, registration.BootSessionID, registration.AgentVersion = candidate.ID, candidate.CandidateBootSessionID, candidate.Version
	foreign := registration
	foreign.NodeID = "other-node"
	if _, err := op.stageGeneration(t.Context(), p, foreign); err == nil {
		t.Fatal("staged a foreign node")
	}
	in, err = op.stageGeneration(t.Context(), p, registration)
	mustRolling(t, err)
	catalog, err := f.install.Read()
	mustRolling(t, err)
	evidence := agentinstall.PromotionEvidence{IntentRevision: node.IntentRevision, L1CatalogRevision: in.CatalogRevision, Residents: map[string][]agentinstall.ResidentAttempt{f.primary.ID: {}, candidate.ID: {}}}
	_, err = f.install.BeginPromotion(candidate.ID, rollingTestBoot, operation, catalog.Revision, 1, evidence)
	mustRolling(t, err)
	request := l1.PromoteNodeGenerationRequest{OperationID: operation, ExpectedPrimaryGenerationID: f.primary.ID, ExpectedPrimaryBootSessionID: node.BootSessionID, ExpectedAuthorityGeneration: node.AuthorityGeneration, CandidateGenerationID: candidate.ID, CandidateBootSessionID: candidate.CandidateBootSessionID, ExpectedCatalogRevision: in.CatalogRevision, CatalogRevision: in.CatalogRevision + 1, IntentRevision: node.IntentRevision, SelectedProtocol: 1, RetiringGenerationIDs: []string{}, ResidentAttempts: []l1.ResidentAttempt{}}
	invalid := request
	invalid.CatalogRevision++
	_, err = op.promoteGeneration(t.Context(), p, invalid)
	var protocol *apiResponseError
	if !errors.As(err, &protocol) || protocol.APIError.Code != contract.ErrorInvalidRequest || protocol.StatusCode != http.StatusBadRequest {
		t.Fatal("agent hop lost the typed L1 error", err)
	}
	changed := request
	changed.OperationID = "another-operation"
	if _, err := op.promoteGeneration(t.Context(), p, changed); err == nil {
		t.Fatal("promotion operation did not match receipt")
	}
	// This is the same production routing used by the coordinator; no node
	// participant exists in the CLI request path.
	type promotionReply struct {
		inventory l1.NodeGenerationInventory
		err       error
	}
	reply := make(chan promotionReply, 1)
	go func() { in, err := op.promoteGeneration(t.Context(), p, request); reply <- promotionReply{in, err} }()
	select {
	case <-promotionCommitted:
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not commit promotion")
	}
	retired := make(chan error, 1)
	go func() { _, err := runtime.Retire(t.Context()); retired <- err }()
	retiredEarly := false
	select {
	case err := <-retired:
		retiredEarly = true
		t.Errorf("retired before promotion reply became durable: %v", err)
	case <-time.After(time.Second):
	}
	release()
	var promoted l1.NodeGenerationInventory
	select {
	case result := <-reply:
		mustRolling(t, result.err)
		promoted = result.inventory
	case <-time.After(5 * time.Second):
		t.Fatal("missing durable promotion reply")
	}
	if !retiredEarly {
		select {
		case err := <-retired:
			mustRolling(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("retirement did not join promotion reply")
		}
	}
	result, err := f.install.ReadControlResult(f.primary.ID)
	mustRolling(t, err)
	if result.Error != "" || result.Request.Action != "promote_generation" {
		t.Fatal("retirement preceded durable promotion proof", result)
	}
	mustRolling(t, validateRollingPromotion(promoted, request))
}

func TestUpgradeRollingExistingCatalogNeverUsesLegacyEnrollment(t *testing.T) {
	f := newRollingFixture(t)
	read := f.operations.generations
	f.operations.generations = func(ctx context.Context, p connprofile.Profile) (l1.NodeGenerationInventory, error) {
		in, err := read(ctx, p)
		for i := range in.Generations {
			in.Generations[i].RetirementProtocolVersions = nil
		}
		return in, err
	}
	f.launchd.commands = nil
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	var refused *upgradeRollingRefusedError
	if !errors.As(err, &refused) || refused.Reason != upgradecompat.LegacyPrimary || len(f.launchd.mutating()) != 0 {
		t.Fatal("existing generation catalog entered legacy enrollment", err)
	}
	if _, err = os.Stat(filepath.Join(f.paths.config, upgradeDrainFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unsupported catalog closed admission", err)
	}
}

func (f *rollingFixture) closeRetiree(t *testing.T, id string) {
	t.Helper()
	in, err := f.h.store.NodeGenerations(t.Context(), f.node.NodeID, f.node.NodeID)
	mustRolling(t, err)
	for _, g := range in.Generations {
		if g.AgentGenerationID == id {
			for _, resident := range g.ResidentAttempts {
				zero := 0
				_, err = f.h.store.CompleteAttempt(t.Context(), f.node.NodeID, resident.JobID, resident.AttemptID, l1.CompletionRequest{FencingToken: resident.FencingToken, IdempotencyKey: "finished-" + resident.AttemptID, Result: l1.ProcessResult{ExitCode: &zero}, RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt})
				mustRolling(t, err)
			}
			_, err = f.h.store.CloseNodeGeneration(t.Context(), f.node.NodeID, f.node.NodeID, id, l1.CloseNodeGenerationRequest{BootSessionID: g.BootSessionID, PromotionOperationID: g.PromotionOperationID, ResidentAttempts: g.ResidentAttempts, RuntimesReaped: true, EvidenceSettled: true})
			mustRolling(t, err)
			// Closed L1 history is retained while #1180 owns local file cleanup.
			f.lifetimes[id].Close()
			delete(f.lifetimes, id)
			return
		}
	}
	t.Fatal("retiree absent", id)
}
func TestUpgradeRollingClosedHistoryAndUnboundedLiveCohort(t *testing.T) {
	for _, scenario := range []string{"idle-close-then-second", "finished-and-live-then-third", "two-live-then-third", "closes-during-second"} {
		t.Run(scenario, func(t *testing.T) {
			f := newRollingFixture(t)
			first := f.primary.ID
			if scenario != "idle-close-then-second" && scenario != "closes-during-second" {
				f.claim(t, "first", contract.JobClassOneShot, false)
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			mustRolling(t, err)
			if scenario == "idle-close-then-second" {
				f.closeRetiree(t, first)
			}
			if scenario == "closes-during-second" {
				setClaims := f.operations.setClaims
				f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
					if !r.ClaimsEnabled {
						f.closeRetiree(t, first)
					}
					return setClaims(ctx, p, r)
				}
			}
			f.claim(t, "second", contract.JobClassOneShot, false)
			_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
			mustRolling(t, err)
			expectedRetirees := 1
			if scenario != "idle-close-then-second" && scenario != "closes-during-second" {
				if scenario == "finished-and-live-then-third" {
					f.closeRetiree(t, first)
				}
				f.claim(t, "third", contract.JobClassOneShot, false)
				_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
				mustRolling(t, err)
				expectedRetirees = 2
				if scenario == "two-live-then-third" {
					expectedRetirees = 3
				}
			}
			in, err := f.h.store.NodeGenerations(t.Context(), f.node.NodeID, f.node.NodeID)
			mustRolling(t, err)
			primary, retiring := 0, 0
			for _, g := range in.Generations {
				if g.Role == l1.NodeGenerationPrimary {
					primary++
				}
				if g.Role == l1.NodeGenerationRetiring {
					retiring++
				}
			}
			rolling, err := readUpgradeRolling(f.paths)
			mustRolling(t, err)
			activation, err := f.install.ReadActivationReceipt(rolling.ActivationReceiptID)
			mustRolling(t, err)
			_, historical := activation.ClosedGenerations[first]
			if historical != (scenario != "two-live-then-third") {
				t.Fatal("activation receipt did not separate closed ownership", activation.ClosedGenerations)
			}
			if primary != 1 || retiring != expectedRetirees {
				t.Fatalf("primary=%d retiring=%d want 1,%d", primary, retiring, expectedRetirees)
			}
		})
	}
}

func exposeRollingL1(t *testing.T, f *rollingFixture) {
	t.Helper()
	exposeRollingL1WithPromotionReply(t, f, nil)
}
func exposeRollingL1WithPromotionReply(t *testing.T, f *rollingFixture, beforeReply func()) *agent.Agent {
	t.Helper()
	fabricID := "plain-" + rand.Text()
	network, err := plain.NewNetworkWithID(fabricID)
	mustRolling(t, err)
	control := network.NewFabric(fabric.Identity{NodeID: "control"})
	listener, err := control.Listen("tcp", "127.0.0.1:0")
	mustRolling(t, err)
	server, err := l1.NewServer(control, f.h.store, l1.ServerConfig{})
	mustRolling(t, err)
	handler := server.Handler()
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if beforeReply != nil && strings.HasSuffix(r.URL.Path, "/generations/promote") {
			result := httptest.NewRecorder()
			handler.ServeHTTP(result, r)
			if result.Code == http.StatusOK {
				beforeReply()
			}
			for key, values := range result.Header() {
				w.Header()[key] = values
			}
			w.WriteHeader(result.Code)
			_, _ = w.Write(result.Body.Bytes())
			return
		}
		handler.ServeHTTP(w, r)
	})}
	ctx, cancel := context.WithCancel(t.Context())
	done := serveTestServer(ctx, func() error {
		err := httpServer.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	t.Cleanup(func() {
		cancel()
		httpServer.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	p, err := connprofile.Read(f.paths.profile)
	mustRolling(t, err)
	p.PlainFabricID, p.L1Address = fabricID, listener.Addr().String()
	mustRolling(t, connprofile.Write(f.paths.profile, p))
	catalog, err := f.install.Read()
	mustRolling(t, err)
	primary := catalog.Generations[catalog.Primary]
	node, err := f.h.store.GetNode(t.Context(), f.node.NodeID)
	mustRolling(t, err)
	participant := network.NewFabric(fabric.Identity{NodeID: node.NodeID, Tags: []string{l1.DefaultAgentPrincipalTag, contract.StableNodeTagPrefix + node.NodeID}})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	mustRolling(t, err)
	runtime, err := agent.New(agent.Config{Fabric: participant, ControlPlaneAddress: p.L1Address, NodeID: node.NodeID, AgentGenerationID: primary.ID, BootSessionID: primary.Process.BootSessionID, Version: node.AgentVersion, OS: "darwin", Architecture: "arm64", RetirementAcknowledged: f.install.PublishRetirement, Capabilities: node.Capabilities, ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir(), PrimaryGeneration: func(write func() error) error { return f.install.WithPrimary(primary.ID, write) }})
	mustRolling(t, err)
	// Model the control-only agent's own capability publication before serving
	// requests. All managed state is disposable; no payload loop runs here.
	snapshot := runtime.CapabilitySnapshot().CapabilityObservation
	snapshot.Revision = node.CapabilityRevision + 1
	_, err = f.h.store.HeartbeatNodeWithCapabilityObservation(t.Context(), node.NodeID, node.NodeID, node.BootSessionID, snapshot, l1.NodePolicy{Tags: []string{"rolling"}, MaxOneshotSlots: 8, MaxServiceSlots: 4})
	mustRolling(t, err)
	controlCtx, stopControl := context.WithCancel(t.Context())
	controlDone := make(chan error, 1)
	controlStore := f.install
	go func() { controlDone <- runtime.ServeUpgradeControl(controlCtx, controlStore) }()
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			stopControl()
			if err := <-controlDone; err != nil {
				t.Error(err)
			}
			runtime.Close()
		})
	}
	f.stopPrimaryControl = stop
	t.Cleanup(stop)
	return runtime
}
func TestUpgradeRollingRealBinaryOCIAndOverlapRefusals(t *testing.T) {
	binary := buildWefty(t)
	for _, overlap := range []bool{false, true} {
		t.Run(fmt.Sprintf("overlap=%t", overlap), func(t *testing.T) {
			f := newRollingFixture(t)
			expected := "unsupported_installation"
			if overlap {
				f.claim(t, "live", contract.JobClassOneShot, false)
				_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
				mustRolling(t, err)
				if runtime.GOOS == "darwin" {
					expected = "unsafe_drain_during_overlap"
				}
				// The Linux binary uses its own data root, without this Mac
				// fixture's generation catalog. Its legacy scope refusal is
				// unsupported_installation, before observing the Mac overlap.
			}
			exposeRollingL1(t, f)
			config, err := ocicontrol.ReadInstalledConfig(f.paths.nodeConfig)
			mustRolling(t, err)
			config.ControlSocket = filepath.Join(f.paths.data, "oci-control.sock")
			mustRolling(t, ocicontrol.WriteInstalledConfig(f.paths.nodeConfig, config))
			code, output := runWeftyWithEnv(t, binary, 15*time.Second, []string{"HOME=" + f.home, "WEFTY_PROFILE=" + f.paths.profile}, "--json", "upgrade", "--rolling")
			if code != exitConflict || !strings.Contains(output, expected) || !strings.Contains(output, `"code": "conflict"`) {
				t.Fatalf("exit=%d expected=%s output=%s", code, expected, output)
			}
		})
	}
}

func TestUpgradeRollingUnsupportedGateUsesLiveAuthority(t *testing.T) {
	for _, installation := range []upgradecompat.Installation{upgradecompat.Linux, upgradecompat.FleetMac, upgradecompat.OCIMac} {
		t.Run(string(installation), func(t *testing.T) {
			f := newRollingFixture(t)
			p, err := connprofile.Read(f.paths.profile)
			mustRolling(t, err)
			check := func(expected upgradecompat.Reason) {
				t.Helper()
				err := refuseUnsupportedRolling(t.Context(), f.host, f.paths, *f.operations, p, installation)
				var refusal *upgradeRollingRefusedError
				if !errors.As(err, &refusal) || refusal.Reason != expected || commandExitCode(err) != exitConflict {
					t.Fatal("gate reason differs", err)
				}
			}
			check(upgradecompat.UnsupportedInstallation)
			_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
			mustRolling(t, err)
			check(upgradecompat.UnsafeDrainDuringOverlap)
			f.closeRetiree(t, f.primary.ID)
			check(upgradecompat.UnsupportedInstallation)
		})
	}
}

func TestUpgradeRollingLegacyInventoryUsesClientObservation(t *testing.T) {
	f := newUpgradeFixture(t)
	p := f.profile(t)
	op := upgradeOperations(f.host)
	op.generations = nil
	op.generationControl = func(context.Context, agentinstall.Store, agentinstall.ControlRequest) (json.RawMessage, error) {
		t.Fatal("legacy inventory tried installed generation control")
		return nil, nil
	}
	host := f.host
	host.upgrade = &op
	op = upgradeOperations(host)
	in, err := op.generations(t.Context(), p)
	mustRolling(t, err)
	if in.NodeID != f.node.NodeID || len(in.Generations) != 0 {
		t.Fatal("legacy client observation differs", in)
	}
	f.node.AgentGenerationID = "unowned-generation"
	_, err = op.generations(t.Context(), p)
	assertUpgradeControlCLIConflict(t, err)
	f.node.AgentGenerationID = ""
	paths, _ := resolveSetupPaths(f.home)
	mustRolling(t, os.Mkdir(filepath.Join(paths.data, agentinstall.Directory), 0700))
	_, err = op.generations(t.Context(), p)
	assertUpgradeControlOperational(t, err)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing catalog cause discarded", err)
	}
}
