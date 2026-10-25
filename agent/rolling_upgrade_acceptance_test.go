//go:build rolling_upgrade_acceptance && (darwin || linux)

package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

// This is real-agent/real-child/SQLite evidence at compressed timings. There
// is no installation, launchd, real L3 Run creation or control-plane restart.
// Run IDs below are ledger-origin fixture labels, not proof of an L3 Run.
func TestB1NumberedProcessesAcrossThreePromotions(t *testing.T) {
	network := plain.NewNetwork()
	store, stop := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{"node": {Tags: []string{"linux"}, MaxOneshotSlots: 12}}, 2*time.Second)
	t.Cleanup(stop)
	spool, managed, handoffs := t.TempDir(), temporaryManagedRoot(t), t.TempDir()
	var primaryMu sync.Mutex
	primaryID := "g0"
	type payload struct {
		originalPID  string
		job          l1.Job
		attempt      l1.Attempt
		pid, release string
		code         int
		restartCount int
	}
	var payloads []payload
	var agents []*Agent
	var exits []chan error
	for generation := 0; generation < 4; generation++ {
		id, boot := fmt.Sprintf("g%d", generation), fmt.Sprintf("boot%d", generation)
		if generation > 0 {
			node, err := store.GetNode(t.Context(), "node")
			b1Must(t, err)
			node, err = store.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{ClaimsEnabled: false, IntentRevision: node.IntentRevision, Reason: "upgrade"})
			b1Must(t, err)
			inventory, err := store.NodeGenerations(t.Context(), "b1-agent", "node")
			b1Must(t, err)
			previous := agents[generation-1].registration
			candidate := previous
			candidate.AgentGenerationID, candidate.BootSessionID = id, boot
			inventory, err = store.StageNodeGeneration(t.Context(), "b1-agent", candidate)
			b1Must(t, err)
			var retirees []string
			var residents []l1.ResidentAttempt
			for _, g := range inventory.Generations {
				if g.Role == l1.NodeGenerationRetiring {
					retirees = append(retirees, g.AgentGenerationID)
				}
				if g.Role == l1.NodeGenerationPrimary {
					residents = append(residents, g.ResidentAttempts...)
				}
			}
			_, err = store.PromoteNodeGeneration(t.Context(), "b1-agent", "node", l1.PromoteNodeGenerationRequest{OperationID: "promote-" + id, ExpectedPrimaryGenerationID: previous.AgentGenerationID, ExpectedPrimaryBootSessionID: previous.BootSessionID, ExpectedAuthorityGeneration: node.AuthorityGeneration, CandidateGenerationID: id, CandidateBootSessionID: boot, ExpectedCatalogRevision: inventory.CatalogRevision, CatalogRevision: inventory.CatalogRevision + 1, IntentRevision: node.IntentRevision, SelectedProtocol: 1, RetiringGenerationIDs: retirees, ResidentAttempts: residents})
			b1Must(t, err)
			primaryMu.Lock()
			primaryID = id
			primaryMu.Unlock()
			_, err = agents[generation-1].Retire(t.Context())
			b1Must(t, err)
		}
		zero := 0.0
		a, err := New(Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "b1-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane", NodeID: "node", AgentGenerationID: id, BootSessionID: boot, Version: id, OS: "darwin", Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true, contract.CapabilityExecCaptureBundle: true}, GuardianExecutable: agentBinaryPath, ManagedRootDirectory: managed, LogSpoolDirectory: spool, HandoffRoot: handoffs, MaxOneshotSlots: 12, OneshotLoadLimit: &zero, HeartbeatInterval: 100 * time.Millisecond, ClaimInterval: 20 * time.Millisecond, RenewalInterval: 100 * time.Millisecond, Logf: t.Logf, PrimaryGeneration: func(write func() error) error {
			primaryMu.Lock()
			defer primaryMu.Unlock()
			if primaryID != id {
				return errors.New("not primary")
			}
			return write()
		}})
		b1Must(t, err)
		// Inject the existing host-boot reader before any agent goroutine starts.
		resource := a.managedResource.(*processManagedResource)
		resource.root, err = managedroot.Open(managedroot.Config{Root: managed, NodeID: "node", BootSessionID: boot, HostBootIdentity: rootHostIdentity})
		b1Must(t, err)
		agents = append(agents, a)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		exits = append(exits, done)
		go func() { done <- a.Run(ctx); close(done) }()
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				t.Error("agent did not join")
			}
			a.Close()
		})
		select {
		case <-a.Registered():
		case <-time.After(10 * time.Second):
			t.Fatal("registration timeout")
		}
		if generation > 0 {
			node, err := store.GetNode(t.Context(), "node")
			b1Must(t, err)
			_, err = store.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{ClaimsEnabled: true, IntentRevision: node.IntentRevision, Reason: "upgrade complete"})
			b1Must(t, err)
		}
		// At least two resident generations remain live at the third cutover.
		// A fresh Job must actually run on each replacement primary meanwhile.
		for _, workload := range []struct {
			code        int
			restartable bool
		}{{0, false}, {17, false}, {17, true}} {
			code := workload.code
			dir := t.TempDir()
			pid, release := filepath.Join(dir, "pid"), filepath.Join(dir, "release")
			t.Cleanup(func() { _ = os.WriteFile(release, nil, 0600) })
			runID := fmt.Sprintf("fixture-run-%d-%d-%t", generation, code, workload.restartable)
			script := `i=0; while [ ! -e "$2" ]; do i=$((i+1)); printf 'out:%04d\n' "$i"; printf 'err:%04d\n' "$i" >&2; printf '%s' "$i" > "$2.count"; if [ "$i" -eq 1 ]; then printf '%s' "$$" > "$1"; fi; sleep 0.05; done; exit "$3"`
			job, _, err := store.CreateJobAs(t.Context(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: runID, Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, Restartable: workload.restartable, Labels: map[string]string{"run_id": runID}, RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", script, "sh", pid, release, strconv.Itoa(code)}, WorkingDirectory: dir}}, l1.JobOrigin{OriginatingSubmitter: "ledger", SubmittedByRunLedger: true})
			b1Must(t, err)
			retirementWait(t, "numbered child running", func() bool { _, e := os.Stat(pid); return e == nil })
			attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
			b1Must(t, err)
			if len(attempts) != 1 {
				t.Fatalf("expected original Attempt: %+v", attempts)
			}
			current, err := store.GetJob(t.Context(), job.JobID)
			b1Must(t, err)
			count := 0
			if current.RestartRecovery != nil {
				count = current.RestartRecovery.Count
			}
			originalPID, err := os.ReadFile(pid)
			b1Must(t, err)
			payloads = append(payloads, payload{originalPID: string(originalPID), job: job, attempt: attempts[0], pid: pid, release: release, code: code, restartCount: count})
		}
	}
	// Exceed the original Lease. Real renewal loops must extend every exact
	// Attempt, including the oldest generation; a heartbeat cannot do this.
	time.Sleep(2400 * time.Millisecond)
	for _, p := range payloads {
		attempts, err := store.ListJobAttempts(t.Context(), p.job.JobID)
		b1Must(t, err)
		current, err := store.GetJob(t.Context(), p.job.JobID)
		b1Must(t, err)
		if current.State != contract.JobRunning || len(attempts) != 1 || attempts[0].AttemptID != p.attempt.AttemptID || !attempts[0].UpdatedAt.After(p.attempt.UpdatedAt) || !attempts[0].LeaseExpiresAt.After(p.attempt.LeaseExpiresAt) {
			t.Fatalf("replacement or expired Attempt is not survival: %+v %+v", current, attempts)
		}
		pid, err := os.ReadFile(p.pid)
		b1Must(t, err)
		// The PID was captured before promotion. A fresh Attempt or process
		// must never be reported as survival.
		if string(pid) != p.originalPID {
			t.Fatal("child PID changed across promotion")
		}
		n, err := strconv.Atoi(string(pid))
		b1Must(t, err)
		b1Must(t, syscall.Kill(n, 0))
		if current.Spec.Labels["run_id"] != p.job.Spec.Labels["run_id"] {
			t.Fatal("Run label changed")
		}
		b1Must(t, os.WriteFile(p.release, nil, 0600))
		retirementWait(t, "real completion", func() bool {
			j, e := store.GetJob(t.Context(), p.job.JobID)
			return e == nil && (j.State == contract.JobSucceeded || j.State == contract.JobFailed)
		})
		afterPID, err := os.ReadFile(p.pid)
		b1Must(t, err)
		if string(pid) != string(afterPID) {
			t.Fatal("replacement child overwrote PID")
		}
		attempts, err = store.ListJobAttempts(t.Context(), p.job.JobID)
		b1Must(t, err)
		if len(attempts) != 1 || attempts[0].AttemptID != p.attempt.AttemptID || attempts[0].Result == nil || attempts[0].Result.ExitCode == nil || *attempts[0].Result.ExitCode != p.code {
			t.Fatalf("real exit lost: %+v", attempts)
		}
		current, err = store.GetJob(t.Context(), p.job.JobID)
		b1Must(t, err)
		if current.RestartRecovery != nil && current.RestartRecovery.Count != p.restartCount {
			t.Fatal("restart count rose")
		}
		var events []contract.LogEvent
		cursor := ""
		for {
			page, err := store.GetJobLogs(t.Context(), p.job.JobID, cursor, l1.MaxLogPageLimit)
			b1Must(t, err)
			events = append(events, page.Events...)
			if len(page.Events) == 0 || page.NextCursor == "" {
				break
			}
			if page.NextCursor == cursor {
				t.Fatal("log cursor made no progress")
			}
			cursor = page.NextCursor
		}
		output := map[contract.LogStream]*strings.Builder{}
		seq := map[contract.LogStream]uint64{}
		for _, e := range events {
			if e.AttemptID != p.attempt.AttemptID || e.Gap != nil || (output[e.Stream] != nil && e.Sequence != seq[e.Stream]+1) {
				t.Fatalf("log identity/gap/order: %+v", e)
			}
			seq[e.Stream] = e.Sequence
			if output[e.Stream] == nil {
				output[e.Stream] = new(strings.Builder)
			}
			output[e.Stream].Write(e.Bytes)
		}
		counter, err := os.ReadFile(p.release + ".count")
		b1Must(t, err)
		records, err := strconv.Atoi(string(counter))
		b1Must(t, err)
		if records < 2 {
			t.Fatal("payload did not keep emitting through overlap")
		}
		for _, stream := range []contract.LogStream{contract.LogStdout, contract.LogStderr} {
			prefix := "out"
			if stream == contract.LogStderr {
				prefix = "err"
			}
			var want strings.Builder
			for i := 1; i <= records; i++ {
				fmt.Fprintf(&want, "%s:%04d\n", prefix, i)
			}
			if output[stream] == nil || output[stream].String() != want.String() {
				t.Fatalf("incomplete/unordered %s: %+v", stream, events)
			}
		}
		t.Logf("Run fixture=%s Job=%s Attempt=%s PID=%s exit=%d restart_count=%d renewed=%s", p.job.Spec.Labels["run_id"], p.job.JobID, p.attempt.AttemptID, pid, p.code, p.restartCount, attempts[0].UpdatedAt)
	}
	for i := 0; i < 3; i++ {
		reviewRetirementExit(t, agents[i], exits[i])
	}
	inventory, err := store.NodeGenerations(t.Context(), "b1-agent", "node")
	b1Must(t, err)
	for _, g := range inventory.Generations {
		if g.AgentGenerationID != "g3" && g.Role != l1.NodeGenerationClosed {
			t.Fatalf("retiree not independently closed: %+v", g)
		}
	}
}
func b1Must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// These run real children against SQLite; only time and failed log transport
// are injected. A settled Lease cannot erase undelivered evidence.
func TestB1CancellationLeaseExpiryAndDelayedEvidence(t *testing.T) {
	t.Run("cancel-pending-evidence", TestRetirementCancellationKeepsEvidencePastSettlementDeadline)
	t.Run("lease-loss-pending-evidence", TestRetirementLeaseLossClosesAfterEvidenceRecovery)
	t.Run("late-durable-append", TestRetirementWaitsForAsynchronousDurableAppend)
}
