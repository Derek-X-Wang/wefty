//go:build darwin || linux

package main

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Exercise the production signal handler with a real process resident. The
// signal channel is injected; existing binary E2E tests cover OS delivery.
func TestTERMDuringRetirementKeepsOrdinaryShutdownSemantics(t *testing.T) {
	testRetirementTERM(t, true)
}

func TestTERMDemotionBeforeLocalObservation(t *testing.T) { testRetirementTERM(t, false) }

func testRetirementTERM(t *testing.T, latch bool) {
	t.Helper()
	heartbeat := 20 * time.Millisecond
	if !latch {
		heartbeat = time.Hour
	}
	for _, forced := range []bool{false, true} {
		t.Run(map[bool]string{false: "one TERM waits", true: "second TERM forces"}[forced], func(t *testing.T) {
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			ready, release := filepath.Join(dir, "ready"), filepath.Join(dir, "release")
			network := plain.NewNetwork()
			control := network.NewFabric(fabric.Identity{NodeID: "control"})
			store, err := l1.OpenStore(filepath.Join(dir, "l1.sqlite"), l1.StoreOptions{LeaseDuration: 60 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			server, err := l1.NewServer(control, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"node": {MaxOneshotSlots: 1}}})
			if err != nil {
				t.Fatal(err)
			}
			listener, err := control.Listen("tcp", "wefty://control-plane")
			if err != nil {
				t.Fatal(err)
			}
			serverContext, stopServer := context.WithCancel(t.Context())
			serverDone := make(chan error, 1)
			go func() { serverDone <- server.Serve(serverContext, listener) }()
			defer func() {
				stopServer()
				if err := <-serverDone; err != nil {
					t.Error(err)
				}
			}()
			zero := 0.0
			node, err := agent.New(agent.Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane", NodeID: "node", BootSessionID: "boot", AgentGenerationID: "generation", OS: "darwin", Version: "test", Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true}, LogSpoolDirectory: filepath.Join(dir, "spool"), ManagedRootDirectory: filepath.Join(dir, "managed"), HandoffRoot: filepath.Join(dir, "handoffs"), OneshotLoadLimit: &zero, MaxOneshotSlots: 1, HeartbeatInterval: heartbeat, ClaimInterval: 10 * time.Millisecond, RenewalInterval: 100 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer node.Close()
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- node.Run(ctx); close(done) }()
			defer func() { cancel(); <-done }()
			select {
			case <-node.Registered():
			case <-time.After(testwait.Bound):
				t.Fatal("agent did not register")
			}
			job, _, err := store.CreateJob(t.Context(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "term-retirement", Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", `touch "$1"; while [ ! -e "$2" ]; do sleep 0.1; done; exit 17`, "sh", ready, release}, WorkingDirectory: dir, HandoffDirectory: dir}})
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err := os.Stat(ready); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("real payload did not start")
				}
				time.Sleep(10 * time.Millisecond)
			}
			current, err := store.GetNode(t.Context(), "node")
			if err != nil {
				t.Fatal(err)
			}
			current, err = store.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{IntentRevision: current.IntentRevision, ClaimsEnabled: false, Reason: "upgrade"})
			if err != nil {
				t.Fatal(err)
			}
			inventory, err := store.NodeGenerations(t.Context(), "agent", "node")
			if err != nil {
				t.Fatal(err)
			}
			primary := inventory.Generations[0]
			candidate := primary.Registration
			candidate.AgentGenerationID = "next"
			candidate.BootSessionID = "next-boot"
			inventory, err = store.StageNodeGeneration(t.Context(), "agent", candidate)
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.PromoteNodeGeneration(t.Context(), "agent", "node", l1.PromoteNodeGenerationRequest{OperationID: "promote", ExpectedPrimaryGenerationID: primary.AgentGenerationID, ExpectedPrimaryBootSessionID: primary.BootSessionID, ExpectedAuthorityGeneration: primary.AuthorityGeneration, CandidateGenerationID: candidate.AgentGenerationID, CandidateBootSessionID: candidate.BootSessionID, ExpectedCatalogRevision: inventory.CatalogRevision, CatalogRevision: inventory.CatalogRevision + 1, IntentRevision: current.IntentRevision, SelectedProtocol: 1, RetiringGenerationIDs: []string{}, ResidentAttempts: primary.ResidentAttempts})
			if err != nil {
				t.Fatal(err)
			}
			if latch {
				if _, err := node.Retire(t.Context()); err != nil {
					t.Fatal(err)
				}
			} else if node.Status().State == agent.LifecycleRetiring {
				t.Fatal("demotion was already observed before TERM")
			}
			signals := make(chan os.Signal, 2)
			drainStarted := make(chan struct{})
			handlerDone := make(chan struct{})
			go func() {
				defer close(handlerDone)
				handleShutdownSignals(ctx, signals, 0, func(ctx context.Context) error {
					node.InterruptRestartableForShutdown()
					_, err := node.Drain(ctx)
					close(drainStarted)
					return err
				}, cancel, t.Logf)
			}()
			signals <- syscall.SIGTERM
			select {
			case <-drainStarted:
			case <-time.After(testwait.Bound):
				t.Fatal("TERM did not start normal drain")
			}
			select {
			case err := <-done:
				t.Fatalf("first TERM ended ordinary resident: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			if node.Status().State != agent.LifecycleDraining {
				t.Fatalf("TERM lifecycle=%s", node.Status().State)
			}
			if forced {
				signals <- syscall.SIGTERM
			} else {
				if err := os.WriteFile(release, nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("shutdown did not join resident")
			}
			cancel()
			<-handlerDone
			attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
			if err != nil || len(attempts) != 1 || attempts[0].Result == nil {
				t.Fatalf("missing result: %+v %v", attempts, err)
			}
			result := attempts[0].Result
			if forced {
				if result.TerminationCause != contract.TerminationCauseAgent {
					t.Fatalf("forced result=%+v", result)
				}
			} else if result.ExitCode == nil || *result.ExitCode != 17 {
				t.Fatalf("ordinary TERM lost real exit: %+v", result)
			}
		})
	}
}
