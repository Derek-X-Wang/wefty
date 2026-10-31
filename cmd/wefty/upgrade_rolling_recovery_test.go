package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

func enableRollingRecoveryFixture(t *testing.T, f *rollingFixture) {
	t.Helper()
	control := f.operations.generationControl
	f.operations.generationControl = func(ctx context.Context, s agentinstall.Store, r agentinstall.ControlRequest) (json.RawMessage, error) {
		if r.Action == "abort_staged_generation" {
			var payload agentinstall.StagedAbortControl
			mustRolling(t, json.Unmarshal(r.Payload, &payload))
			inventory, err := f.h.store.CloseNodeGeneration(ctx, f.node.NodeID, f.node.NodeID, payload.CandidateGenerationID, payload.Request)
			if err != nil {
				return nil, err
			}
			return json.Marshal(inventory)
		}
		if r.Action == "resume_services" {
			return nil, nil
		}
		return control(ctx, s, r)
	}
	waiting := f.operations.wait
	stopped := map[string]bool{}
	run := f.install.Manager.Run
	f.install.Manager.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		label := strings.TrimSuffix(filepath.Base(args[len(args)-1]), ".plist")
		if stopped[label] && args[0] == "print" {
			return []byte("state = waiting\npid = 0\n"), nil
		}
		if stopped[label] && args[0] == "bootout" {
			delete(stopped, label)
			return f.launchd.run(ctx, name, args...)
		}
		return run(ctx, name, args...)
	}
	f.operations.wait = func(ctx context.Context, d time.Duration) error {
		c, err := f.install.Read()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				if _, statErr := os.Lstat(f.install.Root()); errors.Is(statErr, os.ErrNotExist) {
					return waiting(ctx, d)
				}
			}
			return err
		}
		for _, g := range c.Generations {
			if g.AbortOperation != "" {
				if lifetime := f.lifetimes[g.ID]; lifetime != nil {
					lifetime.Close()
					delete(f.lifetimes, g.ID)
				}
				stopped[g.Label] = true
			}
		}
		return waiting(ctx, d)
	}
	f.host.upgrade = f.operations
}

func TestRollingRecoveryEveryDurableTransitionConverges(t *testing.T) {
	phases := []string{"prepared", "admission_closing", "admission_closed", "services_quiescing", "services_quiesced", "installation_writing", "control_plane_renewed", "candidate_preparing", "candidate_loading", "candidate_ready", "candidate_staging", "promotion_requested", "promotion_committed", "catalog_reconciled", "retirement_acknowledged", "survivor_renewal_proved", "installation_completing", "installation_complete", "admission_settling"}
	for _, phase := range phases {
		t.Run(phase, func(t *testing.T) {
			f := newRollingFixture(t)
			enableRollingRecoveryFixture(t, f)
			resident := f.claim(t, "resident", contract.JobClassOneShot, false)
			service := f.claim(t, "service", contract.JobClassService, false)
			promotions := 0
			promote := f.operations.promoteGeneration
			f.operations.promoteGeneration = func(ctx context.Context, p connprofile.Profile, r l1.PromoteNodeGenerationRequest) (l1.NodeGenerationInventory, error) {
				before, err := f.h.store.NodeGenerations(ctx, f.node.NodeID, f.node.NodeID)
				if err != nil {
					return before, err
				}
				result, err := promote(ctx, p, r)
				if result.CatalogRevision > before.CatalogRevision {
					promotions++
				}
				return result, err
			}
			hit := false
			f.operations.rollingStep = func(got string) error {
				if got == phase {
					hit = true
					return errors.New("interrupt " + phase)
				}
				return nil
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil {
				t.Fatal("missing interruption")
			}
			if !hit {
				t.Fatalf("failed before requested transition %s: %v", phase, err)
			}
			beforeRestarts := slices.Clone(f.launchd.commands)
			f.operations.rollingStep = nil
			// The next ordinary invocation must reconcile before its legacy drain path.
			f.operations.requestRolling = false
			_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
			mustRolling(t, err)
			receipt, err := readUpgradeRolling(f.paths)
			mustRolling(t, err)
			if receipt.Phase != "complete" && receipt.Phase != "rolled_back" {
				t.Fatalf("not settled: %+v", receipt)
			}
			job, err := f.job(t.Context(), resident.Job.JobID)
			mustRolling(t, err)
			if len(job.Attempts) != 1 || job.CurrentAttemptID != resident.Lease.AttemptID || job.Attempts[0].State != contract.AttemptRunning {
				t.Fatalf("ordinary work changed: %+v", job)
			}
			sj, err := f.job(t.Context(), service.Job.JobID)
			mustRolling(t, err)
			if len(sj.Attempts) != 1 || sj.LifetimeRestartCount > 1 {
				t.Fatalf("service repeated: %+v", sj)
			}
			expected := 0
			if slices.Index(phases, phase) >= slices.Index(phases, "candidate_ready") {
				expected = 1
			}
			if promotions != expected {
				t.Fatalf("promotions=%d want=%d", promotions, expected)
			}
			for _, command := range f.launchd.commands[len(beforeRestarts):] {
				if strings.Contains(strings.Join(command, " "), "bootstrap") && (strings.Contains(strings.Join(command, " "), setupL1Label) || strings.Contains(strings.Join(command, " "), setupL3Label)) {
					t.Fatalf("recovery restarted controls: %s", command)
				}
			}
		})
	}
}

func TestRollingRecoveryCandidateRemovalKeepsAnyNumberOfRetirees(t *testing.T) {
	for _, count := range []int{0, 1, 2} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			f := newRollingFixture(t)
			enableRollingRecoveryFixture(t, f)
			claims := []*l1.Claim{f.claim(t, "oldest", contract.JobClassOneShot, false)}
			for i := 0; i < count; i++ {
				_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
				mustRolling(t, err)
				claims = append(claims, f.claim(t, fmt.Sprintf("next-%d", i), contract.JobClassOneShot, false))
			}
			before, err := f.install.Read()
			mustRolling(t, err)
			f.operations.rollingStep = func(phase string) error {
				if phase == "candidate_loading" {
					return errors.New("interrupt")
				}
				return nil
			}
			_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil {
				t.Fatal("missing interrupt")
			}
			f.operations.rollingStep = nil
			_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
			mustRolling(t, err)
			after, err := f.install.Read()
			mustRolling(t, err)
			if after.Primary != before.Primary || len(after.Generations) != len(before.Generations) {
				t.Fatalf("cohort changed: before=%+v after=%+v", before, after)
			}
			for _, claim := range claims {
				job, err := f.job(t.Context(), claim.Job.JobID)
				mustRolling(t, err)
				if len(job.Attempts) != 1 || job.CurrentAttemptID != claim.Lease.AttemptID {
					t.Fatal("older work changed")
				}
			}
		})
	}
}

func TestRollingRecoveryLostPromotionReplyConverges(t *testing.T) {
	f := newRollingFixture(t)
	enableRollingRecoveryFixture(t, f)
	f.claim(t, "resident", contract.JobClassOneShot, false)
	f.failPhase = "promotion_requested"
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil {
		t.Fatal("missing lost reply")
	}
	f.failPhase = ""
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	in, err := f.h.store.NodeGenerations(t.Context(), f.node.NodeID, f.node.NodeID)
	mustRolling(t, err)
	if in.CatalogRevision != 1 {
		t.Fatal("duplicate promotion", in.CatalogRevision)
	}
}

func TestRollingRecoveryRefusesDestructiveRollbackWithAcceptedWork(t *testing.T) {
	for _, retirees := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(retirees), func(t *testing.T) {
			f := newRollingFixture(t)
			enableRollingRecoveryFixture(t, f)
			for i := 0; i < retirees; i++ {
				f.claim(t, fmt.Sprintf("older-%d", i), contract.JobClassOneShot, false)
				_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
				mustRolling(t, err)
			}
			f.operations.rollingStep = func(phase string) error {
				if phase == "retirement_acknowledged" {
					return errors.New("interrupt")
				}
				return nil
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil {
				t.Fatal("missing interrupt")
			}
			f.operations.rollingStep = nil
			node, err := f.operations.observe(t.Context(), connprofile.Profile{})
			mustRolling(t, err)
			_, err = f.h.store.SetNodeClaimsByOperator(t.Context(), f.node.NodeID, "operator", l1.NodeIntentRequest{ClaimsEnabled: true, IntentRevision: node.IntentRevision, Reason: "operator resumed new work"})
			mustRolling(t, err)
			accepted := f.claim(t, "new-primary-ordinary", contract.JobClassOneShot, false)
			before, err := f.install.Read()
			mustRolling(t, err)
			run := f.install.Manager.Run
			f.install.Manager.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if args[0] == "print" && strings.HasSuffix(args[len(args)-1], before.Generations[before.Primary].Label) {
					return []byte("state = waiting\npid = 0\n"), nil
				}
				return run(ctx, name, args...)
			}
			mutations := len(f.launchd.mutating())
			_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
			var refused *destructiveRollingRollbackError
			if !errors.As(err, &refused) || commandExitCode(err) != exitConflict {
				t.Fatal("destructive rollback not typed", err)
			}
			details := refused.APIError.Details["accepted_attempts"].([]l1.NodeActiveAttempt)
			if !slices.ContainsFunc(details, func(a l1.NodeActiveAttempt) bool { return a.AttemptID == accepted.Lease.AttemptID }) {
				t.Fatal("new primary work not accounted for")
			}
			after, err := f.install.Read()
			mustRolling(t, err)
			if after.Primary != before.Primary || len(after.Generations) != len(before.Generations) || len(f.launchd.mutating()) != mutations {
				t.Fatal("rollback changed live ownership")
			}
			job, err := f.job(t.Context(), accepted.Job.JobID)
			mustRolling(t, err)
			if len(job.Attempts) != 1 || job.CurrentAttemptID != accepted.Lease.AttemptID {
				t.Fatal("ordinary work repeated")
			}
		})
	}
}

func TestRollingRecoveryRetainsOwnerEditedCompletionFile(t *testing.T) {
	f := newRollingFixture(t)
	enableRollingRecoveryFixture(t, f)
	f.operations.rollingStep = func(phase string) error {
		if phase == "installation_completing" {
			return errors.New("interrupt")
		}
		return nil
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil {
		t.Fatal("missing interrupt")
	}
	receipt, err := readUpgradeRolling(f.paths)
	mustRolling(t, err)
	selected := ""
	for _, file := range receipt.CompletionFiles {
		if file.Path != filepath.Join(f.paths.configRel, setupManifestName) {
			selected = file.Path
			break
		}
	}
	if selected == "" {
		t.Fatal("fixture lacks a prepared skill file")
	}
	path := filepath.Join(f.paths.home, selected)
	mustRolling(t, os.MkdirAll(filepath.Dir(path), 0700))
	edited := []byte("owner edit during recovery\n")
	mustRolling(t, os.WriteFile(path, edited, 0644))
	f.operations.rollingStep = nil
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	var api *apiResponseError
	if !errors.As(err, &api) || commandExitCode(err) != exitConflict {
		t.Fatal("edited completion file not refused", err)
	}
	actual, readErr := os.ReadFile(path)
	mustRolling(t, readErr)
	if !bytes.Equal(actual, edited) {
		t.Fatal("owner edit overwritten")
	}
}

func TestRollingRecoveryLegacyEnrollmentTransitions(t *testing.T) {
	for _, phase := range []string{"legacy_admission_closing", "legacy_draining", "legacy_stopping", "legacy_generation_loading", "legacy_generation_ready", "legacy_installation_complete", "legacy_admission_settling"} {
		t.Run(phase, func(t *testing.T) {
			f := newRollingFixtureWithLegacy(t, true)
			enableRollingRecoveryFixture(t, f)
			initial := phase
			if phase == "legacy_installation_complete" || phase == "legacy_admission_settling" {
				initial = "legacy_generation_loading"
			}
			hit := false
			f.operations.rollingStep = func(got string) error {
				if got == initial {
					hit = true
					return errors.New("interrupt " + got)
				}
				return nil
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil {
				t.Fatal("missing legacy interruption")
			}
			if !hit {
				t.Fatalf("failed before initial legacy transition %s: %v", initial, err)
			}
			if initial != phase {
				hit = false
				f.operations.rollingStep = func(got string) error {
					if got == phase {
						hit = true
						return errors.New("interrupt recovery " + got)
					}
					return nil
				}
				_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
				if err == nil || !hit {
					t.Fatalf("missing recovery interruption at %s: %v", phase, err)
				}
			}
			f.operations.rollingStep = nil
			_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
			mustRolling(t, err)
			receipt, err := readUpgradeRolling(f.paths)
			mustRolling(t, err)
			if receipt.Phase != "complete" && receipt.Phase != "rolled_back" {
				t.Fatalf("legacy recovery not settled: %+v", receipt)
			}
			if receipt.Phase == "complete" {
				c, err := f.install.Read()
				mustRolling(t, err)
				if len(c.Generations) != 1 || c.Primary == "" {
					t.Fatal("legacy retry created multiple generations")
				}
			}
		})
	}
}

func TestRollingRecoveryReceiptDoesNotProveRetireeSurvival(t *testing.T) {
	f := newRollingFixture(t)
	enableRollingRecoveryFixture(t, f)
	original := f.claim(t, "ordinary", contract.JobClassOneShot, false)
	f.operations.rollingStep = func(phase string) error {
		if phase == "installation_complete" {
			return errors.New("interrupt")
		}
		return nil
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil {
		t.Fatal("missing interruption")
	}
	receipt, err := readUpgradeRolling(f.paths)
	mustRolling(t, err)
	if receipt.RenewalProof[original.Lease.AttemptID].AttemptID == "" {
		t.Fatal("missing durable original proof")
	}
	f.operations.rollingStep = nil
	run := f.install.Manager.Run
	f.install.Manager.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "print" && strings.HasSuffix(args[len(args)-1], f.primary.Label) {
			return []byte("state = waiting\npid = 0\n"), nil
		}
		return run(ctx, name, args...)
	}
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil || commandExitCode(err) != exitConflict {
		t.Fatal("receipt substituted for fresh process proof", err)
	}
	after, err := readUpgradeRolling(f.paths)
	mustRolling(t, err)
	if after.Phase == "complete" {
		t.Fatal("lost retiree reported as recovered")
	}
	job, err := f.job(t.Context(), original.Job.JobID)
	mustRolling(t, err)
	if len(job.Attempts) != 1 || job.CurrentAttemptID != original.Lease.AttemptID {
		t.Fatal("ordinary Attempt repeated")
	}
}

func TestRollingRecoveryLostStageReplyDoesNotRestage(t *testing.T) {
	f := newRollingFixture(t)
	enableRollingRecoveryFixture(t, f)
	f.claim(t, "resident", contract.JobClassOneShot, false)
	stage := f.operations.stageGeneration
	calls := 0
	f.operations.stageGeneration = func(ctx context.Context, p connprofile.Profile, request contract.NodeRegistration) (l1.NodeGenerationInventory, error) {
		calls++
		in, err := stage(ctx, p, request)
		if err == nil && calls == 1 {
			return l1.NodeGenerationInventory{}, errors.New("lost accepted stage reply")
		}
		return in, err
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil {
		t.Fatal("missing lost stage reply")
	}
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	if calls != 1 {
		t.Fatal("recovery repeated accepted staging", calls)
	}
	in, err := f.h.store.NodeGenerations(t.Context(), f.node.NodeID, f.node.NodeID)
	mustRolling(t, err)
	if in.CatalogRevision != 1 {
		t.Fatal("recovery repeated promotion")
	}
}

func TestRollingRecoveryFreshTransportReadsRecordedPromotionAfterActorExit(t *testing.T) {
	f := newRollingFixture(t)
	request := agentinstall.ControlRequest{ID: "operation", RequestID: "original", NodeID: f.node.NodeID, GenerationID: f.primary.ID, BootSessionID: "old-boot", Action: "promote_generation"}
	request.Payload, _ = json.Marshal(l1.PromoteNodeGenerationRequest{OperationID: request.ID, ExpectedPrimaryGenerationID: f.primary.ID, ExpectedPrimaryBootSessionID: "old-boot", CandidateGenerationID: "candidate", CandidateBootSessionID: "candidate-boot", ResidentAttempts: []l1.ResidentAttempt{}, RetiringGenerationIDs: []string{}})
	payload := json.RawMessage(`{"node_id":"recorded-node"}`)
	mustRolling(t, f.install.PublishControl(agentinstall.ControlResult{Request: request, Payload: payload}))
	request.RequestID = "fresh-after-actor-exit"
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	result, err := requestGenerationControl(ctx, f.install, request)
	mustRolling(t, err)
	if !bytes.Equal(result, payload) {
		t.Fatal("recorded promotion was lost after transport request changed")
	}
}

func TestRollingRecoveryMissingDrainUsesExactAdmissionSnapshot(t *testing.T) {
	for _, phase := range []string{"admission_closing", "services_quiesced", "admission_settling"} {
		t.Run(phase, func(t *testing.T) {
			f := newRollingFixture(t)
			enableRollingRecoveryFixture(t, f)
			hit := false
			f.operations.rollingStep = func(got string) error {
				if got == phase {
					hit = true
					return errors.New("interrupt")
				}
				return nil
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil || !hit {
				t.Fatalf("did not interrupt %s: %v", phase, err)
			}
			receipt, err := readUpgradeRolling(f.paths)
			mustRolling(t, err)
			if receipt.RecoveryDrain == nil {
				t.Fatal("missing pre-mutation admission snapshot")
			}
			err = os.Remove(filepath.Join(f.paths.config, upgradeDrainFile))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			f.operations.rollingStep = nil
			_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
			mustRolling(t, err)
			node, err := f.h.store.GetNode(t.Context(), f.node.NodeID)
			mustRolling(t, err)
			if !node.ClaimsEnabled {
				t.Fatal("exact interrupted admission was not restored")
			}
		})
	}
}

func TestRollingRecoveryMissingAdmissionEvidenceKeepsFault(t *testing.T) {
	f := newRollingFixture(t)
	enableRollingRecoveryFixture(t, f)
	f.operations.rollingStep = func(got string) error {
		if got == "services_quiesced" {
			return errors.New("interrupt")
		}
		return nil
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil {
		t.Fatal("did not interrupt")
	}
	receipt, err := readUpgradeRolling(f.paths)
	mustRolling(t, err)
	receipt.RecoveryDrain = nil
	mustRolling(t, writeUpgradeRolling(f.paths, receipt, receipt.Phase))
	mustRolling(t, os.Remove(filepath.Join(f.paths.config, upgradeDrainFile)))
	f.operations.rollingStep = nil
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing storage cause: %v", err)
	}
	var refusal *apiResponseError
	if errors.As(err, &refusal) {
		t.Fatalf("filesystem loss converted into refusal: %v", err)
	}
	node, err := f.h.store.GetNode(t.Context(), f.node.NodeID)
	mustRolling(t, err)
	if node.ClaimsEnabled {
		t.Fatal("admission changed without evidence")
	}
}

func TestRollingRecoveryStoppedStagedCandidateKeepsRetirees(t *testing.T) {
	for _, count := range []int{0, 1, 2} {
		for _, phase := range []string{"candidate_staging", "promotion_requested"} {
			t.Run(fmt.Sprintf("%d/%s", count, phase), func(t *testing.T) {
				f := newRollingFixture(t)
				enableRollingRecoveryFixture(t, f)
				claims := []*l1.Claim{f.claim(t, "oldest", contract.JobClassOneShot, false)}
				for i := 0; i < count; i++ {
					_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
					mustRolling(t, err)
					claims = append(claims, f.claim(t, fmt.Sprintf("resident-%d", i), contract.JobClassOneShot, false))
				}
				before, err := f.install.Read()
				mustRolling(t, err)
				stage := f.operations.stageGeneration
				if phase == "candidate_staging" {
					f.operations.stageGeneration = func(ctx context.Context, p connprofile.Profile, registration contract.NodeRegistration) (l1.NodeGenerationInventory, error) {
						in, err := stage(ctx, p, registration)
						if err != nil {
							return in, err
						}
						return in, errors.New("lost stage reply")
					}
				} else {
					f.operations.rollingStep = func(got string) error {
						if got == phase {
							return errors.New("interrupt")
						}
						return nil
					}
				}
				_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
				if err == nil {
					t.Fatal("missing interruption")
				}
				receipt, err := readUpgradeRolling(f.paths)
				mustRolling(t, err)
				candidate := receipt.Candidate
				if lifetime := f.lifetimes[candidate.ID]; lifetime != nil {
					lifetime.Close()
					delete(f.lifetimes, candidate.ID)
				}
				delete(f.launchd.loaded, candidate.Label)
				f.operations.stageGeneration = stage
				f.failPhase = ""
				f.operations.rollingStep = nil
				_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
				mustRolling(t, err)
				after, err := f.install.Read()
				mustRolling(t, err)
				if after.Primary != before.Primary || len(after.Generations) != len(before.Generations) || after.PendingReceipt != "" {
					t.Fatalf("cohort changed: %+v", after)
				}
				for _, claim := range claims {
					job, err := f.job(t.Context(), claim.Job.JobID)
					mustRolling(t, err)
					if len(job.Attempts) != 1 || job.CurrentAttemptID != claim.Lease.AttemptID {
						t.Fatal("older work changed")
					}
				}
			})
		}
	}
}

func TestRollingRecoveryStagedAbortRequestSurvivesInterruption(t *testing.T) {
	f := newRollingFixture(t)
	enableRollingRecoveryFixture(t, f)
	f.claim(t, "resident", contract.JobClassOneShot, false)
	f.operations.rollingStep = func(phase string) error {
		if phase == "promotion_requested" {
			return errors.New("interrupt")
		}
		return nil
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil {
		t.Fatal("missing interruption")
	}
	receipt, err := readUpgradeRolling(f.paths)
	mustRolling(t, err)
	if lifetime := f.lifetimes[receipt.Candidate.ID]; lifetime != nil {
		lifetime.Close()
		delete(f.lifetimes, receipt.Candidate.ID)
	}
	delete(f.launchd.loaded, receipt.Candidate.Label)
	hit := false
	f.operations.rollingStep = func(phase string) error {
		if phase == "rolling_back" {
			hit = true
			return errors.New("interrupt abort")
		}
		return nil
	}
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil || !hit {
		t.Fatal("missing abort interruption", err)
	}
	receipt, err = readUpgradeRolling(f.paths)
	mustRolling(t, err)
	if receipt.StagedAbort == nil {
		t.Fatal("abort request not durable")
	}
	original, _ := json.Marshal(receipt.StagedAbort)
	f.operations.rollingStep = nil
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	receipt, err = readUpgradeRolling(f.paths)
	mustRolling(t, err)
	replay, _ := json.Marshal(receipt.StagedAbort)
	if !bytes.Equal(original, replay) || receipt.Phase != "rolled_back" {
		t.Fatal("abort replay changed")
	}
	in, err := f.h.store.NodeGenerations(t.Context(), f.node.NodeID, f.node.NodeID)
	mustRolling(t, err)
	if in.CatalogRevision != 0 {
		t.Fatal("rollback promoted")
	}
}
