//go:build rolling_upgrade_acceptance && (darwin || linux)

package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Fake launchd, fake readiness/process binding and fixture-driven renewals.
// This exercises the real coordinator and SQLite authority, never survival.
func newB1FakeLaunchdFixture(t *testing.T) *rollingFixture {
	t.Helper()
	f := newRollingFixture(t)
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	address := strings.TrimPrefix(server.URL, "http://")
	// Reuse #1181's effective-endpoint guard before any coordinator operation.
	mustRolling(t, diagnosticFixtureEndpointError(address))
	f.host.l1Address, f.host.l3Address = address, address
	p, err := connprofile.Read(f.paths.profile)
	mustRolling(t, err)
	p.L1Address, p.L3Address = address, address
	mustRolling(t, connprofile.Write(f.paths.profile, p))
	checked := globalOptions{profilePath: f.paths.profile}
	mustRolling(t, applyConnectionProfile(&checked))
	for _, endpoint := range []string{checked.l1Address, checked.l3Address} {
		mustRolling(t, diagnosticFixtureEndpointError(endpoint))
	}
	return f
}

func TestB1FakeLaunchdThreeCutovers(t *testing.T) {
	f := newB1FakeLaunchdFixture(t)
	var originals []*l1.Claim
	for i := 0; i < 3; i++ {
		originals = append(originals, f.claim(t, string(rune('a'+i)), contract.JobClassOneShot, i == 1))
		_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
		mustRolling(t, err)
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
	if primary != 1 || retiring != 3 {
		t.Fatalf("cohort primary=%d retiring=%d", primary, retiring)
	}
	for _, claim := range originals {
		attempts, err := f.h.store.ListJobAttempts(t.Context(), claim.Job.JobID)
		mustRolling(t, err)
		if len(attempts) != 1 || attempts[0].AttemptID != claim.Lease.AttemptID {
			t.Fatalf("successor is not survival: %+v", attempts)
		}
	}
	t.Log("FAKE-LAUNCHD: process bindings, readiness and renewals are fixture-driven; no installed survival claim")
}

func TestB1FakeLaunchdExitDuringCutover(t *testing.T) {
	for _, code := range []int{0, 17} {
		t.Run(string(rune('a'+code)), func(t *testing.T) {
			f := newB1FakeLaunchdFixture(t)
			original := f.claim(t, "exit-at-cutover", contract.JobClassOneShot, false)
			completed := false
			f.operations.rollingStep = func(phase string) error {
				if phase == "promotion_committed" && !completed {
					completed = true
					_, err := f.h.store.CompleteAttempt(t.Context(), f.node.NodeID, original.Job.JobID, original.Lease.AttemptID, l1.CompletionRequest{FencingToken: original.Lease.FencingToken, IdempotencyKey: "cutover-exit", Result: l1.ProcessResult{ExitCode: &code}, RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt})
					return err
				}
				return nil
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			mustRolling(t, err)
			if !completed {
				t.Fatal("cutover injection not reached")
			}
			attempts, err := f.h.store.ListJobAttempts(t.Context(), original.Job.JobID)
			mustRolling(t, err)
			if len(attempts) != 1 || attempts[0].AttemptID != original.Lease.AttemptID || attempts[0].Result == nil || attempts[0].Result.ExitCode == nil || *attempts[0].Result.ExitCode != code {
				t.Fatalf("terminal evidence changed: %+v", attempts)
			}
		})
	}
}

func TestB1FakeLaunchdCandidateFailure(t *testing.T) {
	f := newB1FakeLaunchdFixture(t)
	enableRollingRecoveryFixture(t, f)
	original := f.claim(t, "resident", contract.JobClassOneShot, false)
	injected := false
	f.operations.rollingStep = func(phase string) error {
		if phase == "candidate_loading" {
			injected = true
			return errors.New("acceptance candidate start failure")
		}
		return nil
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil || !injected {
		t.Fatal("candidate failure not exercised", err)
	}
	f.operations.rollingStep = nil
	_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
	mustRolling(t, err)
	attempts, err := f.h.store.ListJobAttempts(t.Context(), original.Job.JobID)
	mustRolling(t, err)
	if len(attempts) != 1 || attempts[0].AttemptID != original.Lease.AttemptID || attempts[0].Result != nil {
		t.Fatalf("candidate failure replaced resident: %+v", attempts)
	}
	catalog, err := f.install.Read()
	mustRolling(t, err)
	if catalog.Primary != f.primary.ID {
		t.Fatal("failed candidate became primary")
	}
}

func TestB1SuccessorCannotProveSurvival(t *testing.T) {
	// Exercise the production-called acceptance predicate with a healthy new
	// primary and a renewed successor, while the preserved Attempt is absent.
	f := newB1FakeLaunchdFixture(t)
	original := f.claim(t, "original", contract.JobClassOneShot, false)
	after := f.now
	r := rollingUpgrade{receipt: upgradeRollingReceipt{Candidate: f.primary, Preserved: []l1.NodeActiveAttempt{{JobID: original.Job.JobID, AttemptID: original.Lease.AttemptID, Kind: contract.JobKindProcess, Class: contract.JobClassOneShot}}}, op: *f.operations}
	r.receipt.Candidate.Version = "next"
	r.receipt.Candidate.CandidateBootSessionID = "next-boot"
	r.op.observe = func(context.Context, connprofile.Profile) (l1.Node, error) {
		return l1.Node{NodeRegistration: contract.NodeRegistration{BootSessionID: "next-boot", AgentVersion: "next"}, State: contract.NodeAlive, LastHeartbeatAt: f.now}, nil
	}
	r.op.readJob = func(context.Context, connprofile.Profile, string) (l1.Job, error) {
		return l1.Job{CurrentAttemptID: "replacement", Attempts: []l1.Attempt{{AttemptID: "replacement", State: contract.AttemptRunning, UpdatedAt: f.now, LeaseExpiresAt: f.now.Add(l1.DefaultLeaseDuration)}}}, nil
	}
	if err := r.waitSurvivors(t.Context(), l1.NodeGenerationInventory{}, after); err == nil {
		t.Fatal("renewed replacement Attempt was accepted as survival")
	}
}

func TestB1FakeLaunchdStatusDoctorStates(t *testing.T) {
	// Shared #1181 cases cover healthy N-generation overlap, stale/unknown
	// renewals, interrupted/reboot/lost Attempts, cutover and interrupted receipts.
	t.Run("text-json", TestRollingStatusTextAndJSONFixtures)
	t.Run("receipt-phases", TestRollingStatusReceiptPhasesAndReadOnly)
	t.Run("diagnostic-read-only", TestRollingStatusDoctorReadsWithoutMutation)
	t.Run("effective-endpoint-guard", TestRound2EffectiveEndpointGuard)
}
