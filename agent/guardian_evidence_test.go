//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
	wr "github.com/Derek-X-Wang/wefty/runner"
	p "github.com/Derek-X-Wang/wefty/runner/process"
)

// Use the real redactor -> batching sink -> SQLite append path, holding its
// only append connection until AFTER the runner returns. Output cancellation
// must release the sink; releasing the SQL connection cannot rescue the run.
func TestGuardianCrashCancelsBlockedProductionSpool(t *testing.T) {
	spool, err := openLogSpool(t.TempDir(), "guardian-node", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	claim := l1.Claim{Job: l1.Job{JobID: "job", CreatedAt: time.Now(), Spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassService}}, Lease: l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence"}}
	if err := spool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	connection, err := spool.appendDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	entered := make(chan struct{})
	var once sync.Once
	spool.appendCheckpoint = func(context.Context) { once.Do(func() { close(entered) }) }
	sink := newRedactingOutputSink(&batchingLogSink{spool: spool, done: make(chan struct{}), batchSize: 100}, map[string]string{"secret": "REDACT-ME"})
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "payload.pid")
	release := filepath.Join(dir, "release")
	script := `echo $$ > "$1"; echo output; echo error >&2; while [ ! -f "$2" ]; do sleep 0.01; done; kill -KILL "$PPID"; exec /bin/sleep 30`
	adapter := p.NewAdapter(p.New(p.Config{GuardianExecutable: agentBinaryPath, TerminationGraceTime: 20 * time.Millisecond, ProcessReapTimeout: time.Second}))
	authority := wr.AttemptAuthority{AttemptID: "attempt", JobID: "job", BootSessionID: "boot", NodeID: "node", FencingToken: "fence"}
	done := make(chan runOutcome, 1)
	t.Cleanup(func() {
		data, _ := os.ReadFile(pidPath)
		pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
		if pid > 0 {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			waitAgentGroupGone(t, pid)
		}
	})
	go func() {
		result, err := adapter.Run(context.Background(), wr.Request{Authority: authority, IdlePolicy: wr.IgnoreIdle, LifetimeBoundary: wr.AgentBootLifetime, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", script, "guardian-spool", pidPath, release}, WorkingDirectory: dir}}, sink)
		done <- runOutcome{result: result.Outcome, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("production spool did not reach blocked append")
	}
	testwait.Until(t, "SQL-blocked spool append", func() (bool, error) {
		waitCount := spool.appendDB.Stats().WaitCount
		if waitCount > 0 {
			return true, nil
		}
		return false, fmt.Errorf("SQL wait count = %d", waitCount)
	})
	if spool.appendDB.Stats().WaitCount == 0 {
		t.Fatal("spool append was not actually SQL-blocked")
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case outcome := <-done:
		if !errors.Is(outcome.err, p.ErrGuardianLost) || !outcome.result.LogEvidenceIncomplete {
			t.Fatalf("run result=%+v err=%v", outcome.result, outcome.err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Guardian crash did not cancel SQL-blocked production sink")
	}
	receipt, err := adapter.ReapAndVerify(context.Background(), wr.ReapRequest{Authority: authority})
	if !errors.Is(err, p.ErrGuardianLost) || receipt.RuntimeQuiesced {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
}

func waitAgentGroupGone(t *testing.T, pgid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if errors.Is(syscall.Kill(-pgid, 0), syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("created payload group %d still exists", pgid)
}

func TestGuardedStartedHasDurableProcessInventory(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root, err := managedroot.Initialize(managedroot.Config{Root: dir, NodeID: "node", BootSessionID: "boot", HostBootIdentity: func() (string, error) { return "darwin:1:0:00000000-0000-0000-0000-000000000001", nil }})
	if err != nil {
		t.Fatal(err)
	}
	resource := &processManagedResource{root: root, bootSessionID: "boot"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := &inventoryObservingExecutor{inner: p.New(p.Config{GuardianExecutable: agentBinaryPath, TerminationGraceTime: 20 * time.Millisecond, ProcessReapTimeout: time.Second}), root: root, cancel: cancel}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		managedResource: resource, runtimes: testRuntimeSet(executor), nodeID: "node", bootSessionID: "boot", clock: systemClock{}, observer: newLifecycleObserver(systemClock{}),
	})
	claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassService, Restart: contract.RestartAlways, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sleep"}, Argv: []string{"sleep", "30"}, WorkingDirectory: dir}}}, Lease: l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence"}}
	result, err := lifecycle.runWorkload(ctx, claim)
	if executor.err != nil || !executor.observed {
		t.Fatalf("durable Started observed=%t err=%v result=%+v runErr=%v", executor.observed, executor.err, result, err)
	}
	if !errors.Is(err, context.Canceled) || result.SpawnError != nil || result.OutputError != "" {
		t.Fatalf("guarded stop result=%+v err=%v", result, err)
	}
	groups, err := resource.processGroups("job")
	if err != nil || len(groups) != 1 || groups[0].ProcessGroupID <= 0 {
		t.Fatalf("retained inventory=%+v err=%v", groups, err)
	}
	waitAgentGroupGone(t, groups[0].ProcessGroupID)
	paths, err := root.ServicePaths("job")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(paths.Attempts)
	if err != nil || len(entries) != 0 {
		t.Fatalf("scratch cleanup entries=%v err=%v", entries, err)
	}
}

type inventoryObservingExecutor struct {
	inner    *p.Runner
	root     *managedroot.Manager
	cancel   context.CancelFunc
	observed bool
	err      error
}

func (executor *inventoryObservingExecutor) Run(ctx context.Context, request p.Request, sink p.OutputSink) (contract.ProcessResult, error) {
	original := request.Started
	request.Started = func() {
		groups, err := executor.root.ProcessAttempts("job")
		if err != nil || len(groups) != 1 || groups[0].ProcessGroupID <= 0 || groups[0].AttemptID != "attempt" || groups[0].BootSessionID != "boot" {
			executor.err = errors.New("Started preceded durable payload group")
		}
		executor.observed = true
		if original != nil {
			original()
		}
		executor.cancel()
	}
	return executor.inner.Run(ctx, request, sink)
}

func TestGuardianSpawnFailureResolvesPendingInventory(t *testing.T) {
	for _, failure := range []string{"payload", "guardian", "reservation", "endpoint"} {
		t.Run(failure, func(t *testing.T) {
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			root, err := managedroot.Initialize(managedroot.Config{Root: dir, NodeID: "node", BootSessionID: "boot", HostBootIdentity: func() (string, error) { return "linux:00000000-0000-0000-0000-000000000001", nil }})
			if err != nil {
				t.Fatal(err)
			}
			resource := &processManagedResource{root: root, bootSessionID: "boot"}
			guardian := agentBinaryPath
			if failure == "guardian" {
				guardian = filepath.Join(dir, "missing-guardian")
			}
			dependencies := attemptLifecycleDependencies{managedResource: resource, runtimes: testRuntimeSet(p.New(p.Config{GuardianExecutable: guardian})), nodeID: "node", bootSessionID: "boot", clock: systemClock{}, observer: newLifecycleObserver(systemClock{})}
			claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassService, Restart: contract.RestartAlways, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: filepath.Join(dir, "missing-payload")}, Argv: []string{"missing"}, WorkingDirectory: dir}}}, Lease: l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence"}}
			if failure == "reservation" || failure == "endpoint" {
				port := 12345
				claim.Job.Spec.PublishedPort = &port
				dependencies.reservePublishedPort = func(l1.Claim) (net.Listener, *contract.SpawnFailure) {
					if failure == "reservation" {
						return nil, &contract.SpawnFailure{Code: contract.SpawnFailureProcessRequest, Message: "reservation refused"}
					}
					listener, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					return listener, nil
				}
				dependencies.prepareServiceEndpoint = func(context.Context) (serviceRuntimeEndpoint, error) {
					return serviceRuntimeEndpoint{}, errors.New("endpoint failed")
				}
			}
			result, _ := newAttemptLifecycle(dependencies).runWorkload(context.Background(), claim)
			if result.SpawnError == nil {
				t.Fatalf("expected known spawn failure: %+v", result)
			}
			paths, err := root.ServicePaths("job")
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(paths.Root, "process-attempts", managedroot.EncodeID("attempt")+".json"))
			if err != nil {
				t.Fatal(err)
			}
			var record map[string]any
			if err := json.Unmarshal(data, &record); err != nil {
				t.Fatal(err)
			}
			if record["not_spawned"] != true || record["process_group_id"] != float64(0) {
				t.Fatalf("known failure left unresolved inventory: %s", data)
			}
		})
	}
}
