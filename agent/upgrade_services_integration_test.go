//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestUpgradeServicesMixedWorkloadAcrossTwoRetirees(t *testing.T) {
	// Node capacity includes retirees: each of two old primaries keeps both
	// an ordinary and a Restartable one-shot alive through the final promotion.
	const oneShotSlots = 4
	network := plain.NewNetwork()
	store, stop := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{"node": {Tags: []string{"linux"}, MaxOneshotSlots: oneShotSlots, MaxServiceSlots: 2}}, l1.DefaultLeaseDuration)
	t.Cleanup(stop)
	spool, handoffs, managed := t.TempDir(), t.TempDir(), temporaryManagedRoot(t)
	var primaryMu sync.Mutex
	primary := "g0"
	var agents []*Agent
	var finished []chan error
	var oneshots, services []l1.Job
	var releases []string
	var previous []l1.NodeActiveAttempt
	for i := 0; i < 3; i++ {
		id, boot := fmt.Sprintf("g%d", i), fmt.Sprintf("boot%d", i)
		if i > 0 {
			node, err := store.GetNode(t.Context(), "node")
			if err != nil {
				t.Fatal(err)
			}
			node, err = store.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{ClaimsEnabled: false, IntentRevision: node.IntentRevision, Reason: "upgrade"})
			if err != nil {
				t.Fatal(err)
			}
			previous = nil
			for _, job := range services {
				j, err := store.GetJob(t.Context(), job.JobID)
				if err != nil {
					t.Fatal(err)
				}
				previous = append(previous, l1.NodeActiveAttempt{JobID: job.JobID, AttemptID: j.CurrentAttemptID, BootSessionID: agents[i-1].registration.BootSessionID, Kind: contract.JobKindProcess, Class: contract.JobClassService})
			}
			request := UpgradeServiceQuiescence{OperationID: "upgrade-" + id, NodeID: "node", AgentGenerationID: agents[i-1].registration.AgentGenerationID, BootSessionID: agents[i-1].registration.BootSessionID, Services: previous}
			barrierContext, cancelBarrier := context.WithTimeout(t.Context(), 15*time.Second)
			_, err = agents[i-1].QuiesceUpgradeServices(barrierContext, request)
			cancelBarrier()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = agents[i-1].QuiesceUpgradeServices(t.Context(), request); err != nil {
				t.Fatal("barrier replay:", err)
			}
			for _, old := range previous {
				attempts, err := store.ListJobAttempts(t.Context(), old.JobID)
				if err != nil || len(attempts) != i || attempts[len(attempts)-1].Result == nil {
					t.Fatalf("predecessor not finalized: %+v %v", attempts, err)
				}
			}
			candidate := agents[i-1].registration
			candidate.AgentGenerationID, candidate.BootSessionID = id, boot
			inventory, err := store.StageNodeGeneration(t.Context(), "upgrade-agent", candidate)
			if err != nil {
				t.Fatal(err)
			}
			var old l1.NodeGeneration
			retirees := []string{}
			for _, g := range inventory.Generations {
				if g.Role == l1.NodeGenerationPrimary {
					old = g
				}
				if g.Role == l1.NodeGenerationRetiring {
					retirees = append(retirees, g.AgentGenerationID)
				}
			}
			_, err = store.PromoteNodeGeneration(t.Context(), "upgrade-agent", "node", l1.PromoteNodeGenerationRequest{OperationID: "promote-" + id, ExpectedPrimaryGenerationID: old.AgentGenerationID, ExpectedPrimaryBootSessionID: old.BootSessionID, ExpectedAuthorityGeneration: old.AuthorityGeneration, CandidateGenerationID: id, CandidateBootSessionID: boot, ExpectedCatalogRevision: inventory.CatalogRevision, CatalogRevision: inventory.CatalogRevision + 1, IntentRevision: node.IntentRevision, SelectedProtocol: 1, RetiringGenerationIDs: retirees, ResidentAttempts: old.ResidentAttempts})
			if err != nil {
				t.Fatal(err)
			}
			primaryMu.Lock()
			primary = id
			primaryMu.Unlock()
			if _, err = agents[i-1].Retire(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err = agents[i-1].ResumeUpgradeServiceClaims(t.Context(), request.OperationID); err == nil {
				t.Fatal("retiree reopened services")
			}
			for _, old := range previous {
				recovery := l1.ServiceRestartRequest{IdempotencyKey: request.OperationID + ":" + old.BootSessionID + ":" + old.AttemptID, Upgrade: &l1.ServiceUpgradeRecovery{NodeID: "node", AttemptID: old.AttemptID, BootSessionID: old.BootSessionID}}
				for replay := 0; replay < 2; replay++ {
					job, repeated, err := store.RestartService(t.Context(), old.JobID, recovery)
					if err != nil || repeated != (replay == 1) || job.State != contract.JobQueued {
						t.Fatalf("recovery=%+v replay=%v error=%v", job, repeated, err)
					}
				}
			}
		}
		zero := 0.0
		a, err := New(Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "upgrade-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane", NodeID: "node", BootSessionID: boot, AgentGenerationID: id, OS: "darwin", Version: id, Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true, contract.CapabilityExecCaptureBundle: true}, GuardianExecutable: agentBinaryPath, LogSpoolDirectory: spool, HandoffRoot: handoffs, ManagedRootDirectory: managed, MaxOneshotSlots: oneShotSlots, MaxServiceSlots: 2, OneshotLoadLimit: &zero, HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 10 * time.Millisecond, RenewalInterval: 100 * time.Millisecond, Logf: t.Logf,
			PrimaryGeneration: func(write func() error) error {
				primaryMu.Lock()
				defer primaryMu.Unlock()
				if primary != id {
					return errors.New("not primary")
				}
				return write()
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		// Keep the real managed-root/process lifecycle, but supply the existing
		// kernel-reader seam before any agent goroutine can use this manager.
		// The fixture's generations share one host boot; only agent boots change.
		resource := a.managedResource.(*processManagedResource)
		resource.root, err = managedroot.Open(managedroot.Config{Root: managed, NodeID: "node", BootSessionID: boot, HostBootIdentity: rootHostIdentity})
		if err != nil {
			a.Close()
			t.Fatal(err)
		}
		agents = append(agents, a)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		finished = append(finished, done)
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
		case <-time.After(testwait.Bound):
			t.Fatal("registration timeout")
		}
		if i > 0 {
			node, err := store.GetNode(t.Context(), "node")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{ClaimsEnabled: true, IntentRevision: node.IntentRevision, Reason: "upgrade complete"}); err != nil {
				t.Fatal(err)
			}
		} else {
			for _, policy := range []string{contract.RestartAlways, contract.RestartNever} {
				dir := t.TempDir()
				job, _, err := store.CreateJob(t.Context(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "service-" + policy, Class: contract.JobClassService, Kind: contract.JobKindProcess, Restart: policy, RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", `trap 'exit 0' TERM; while :; do sleep 0.1; done`}, WorkingDirectory: dir}})
				if err != nil {
					t.Fatal(err)
				}
				services = append(services, job)
			}
		}
		for _, service := range services {
			retirementWait(t, "service fresh Attempt running", func() bool {
				job, err := store.GetJob(t.Context(), service.JobID)
				if err == nil && job.State == contract.JobFailed {
					t.Fatalf("service failed before upgrade: last_failure=%s policy_stop=%+v", job.LastFailure, job.PolicyStop)
				}
				return err == nil && job.State == contract.JobRunning
			})
			marker := filepath.Join(managed, "agent", "nodes", managedroot.EncodeID("node"), "services", managedroot.EncodeID(service.JobID), "data", "upgrade-marker")
			if i == 0 {
				if err := os.WriteFile(marker, []byte(service.JobID), 0600); err != nil {
					t.Fatal(err)
				}
			} else if content, err := os.ReadFile(marker); err != nil || string(content) != service.JobID {
				t.Fatalf("service data changed: %q %v", content, err)
			}
			attempts, err := store.ListJobAttempts(t.Context(), service.JobID)
			if err != nil || len(attempts) != i+1 {
				t.Fatalf("service attempts=%+v %v", attempts, err)
			}
		}
		// Keep both ordinary and Restartable one-shots under each old primary.
		if i < 2 {
			for _, restartable := range []bool{false, true} {
				job, ready, release := retirementReviewJob(t, store, fmt.Sprintf("work-%s-%v", id, restartable), restartable, nil)
				oneshots = append(oneshots, job)
				releases = append(releases, release)
				// Every earlier payload intentionally remains resident. Report a
				// fixture capacity error directly instead of timing out on its file.
				node, err := store.GetNode(t.Context(), "node")
				if err != nil {
					t.Fatal(err)
				}
				current, err := store.GetJob(t.Context(), job.JobID)
				if err != nil {
					t.Fatal(err)
				}
				if current.State == contract.JobQueued && node.OneshotOccupancy >= node.MaxOneshotSlots {
					t.Fatalf("one-shot cannot start: generation=%s restartable=%t state=%s Node occupancy=%d/%d; earlier payloads remain live", id, restartable, current.State, node.OneshotOccupancy, node.MaxOneshotSlots)
				}
				retirementWait(t, fmt.Sprintf("one-shot running generation=%s restartable=%t", id, restartable), func() bool { _, err := os.Stat(ready); return err == nil })
			}
		}
	}
	inventory, err := store.NodeGenerations(t.Context(), "upgrade-agent", "node")
	if err != nil {
		t.Fatal(err)
	}
	retirees := 0
	for _, g := range inventory.Generations {
		if g.Role == l1.NodeGenerationRetiring {
			retirees++
			if len(g.ResidentAttempts) != 2 {
				t.Fatalf("services remained resident: %+v", g)
			}
		}
	}
	if retirees != 2 {
		t.Fatalf("retirees=%d", retirees)
	}
	for _, job := range oneshots {
		j, err := store.GetJob(t.Context(), job.JobID)
		if err != nil || j.State != contract.JobRunning {
			t.Fatalf("kept one-shot ended: %+v %v", j, err)
		}
		attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
		if err != nil || len(attempts) != 1 {
			t.Fatalf("one-shot replaced: %+v %v", attempts, err)
		}
	}
	for _, release := range releases {
		if err := os.WriteFile(release, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, job := range oneshots {
		retirementWait(t, "one-shot finalized under retiree", func() bool {
			j, err := store.GetJob(t.Context(), job.JobID)
			return err == nil && j.State == contract.JobFailed
		})
		attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
		if err != nil || len(attempts) != 1 || attempts[0].Result == nil || attempts[0].Result.ExitCode == nil || *attempts[0].Result.ExitCode != 17 {
			t.Fatalf("one-shot evidence=%+v %v", attempts, err)
		}
	}
	for i := range 2 {
		reviewRetirementExit(t, agents[i], finished[i])
	}
	for _, service := range services {
		j, err := store.GetJob(t.Context(), service.JobID)
		if err != nil || j.State != contract.JobRunning {
			t.Fatalf("retiree service cleanup touched successor: %+v %v", j, err)
		}
	}
	// The shared managed-root owner survives; service quiescence never removes it.
	if _, err := os.Stat(filepath.Join(managed, "agent", "nodes", managedroot.EncodeID("node"), "services")); err != nil {
		t.Fatal(err)
	}
}
