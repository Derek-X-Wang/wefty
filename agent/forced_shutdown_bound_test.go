//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// slowL1Call is one agent call the slow L1 held until the agent gave up.
type slowL1Call struct {
	kind string
	held time.Duration
}

// TestForcedShutdownEndsWithinItsBoundWhenL1IsSlow holds ForcedShutdownBound
// to what a forced shutdown really takes. The resident payload ignores
// SIGTERM, so the runner spends its whole termination grace, and it writes a
// result into its managed handoff, so the agent uploads one. Once shutdown
// starts, L1 holds the chosen calls until the agent gives up on each. Every
// case checks that each held phase really ran to its own timeout and that the
// shutdown ended within that route's phases, which the bound must cover. A
// new serial phase, or a phase that stops honoring its timeout, fails here.
func TestForcedShutdownEndsWithinItsBoundWhenL1IsSlow(t *testing.T) {
	timeouts := ShutdownTimeouts{
		TerminationGrace: 100 * time.Millisecond, ProcessReap: 100 * time.Millisecond,
		Finalization: time.Second, Operation: 500 * time.Millisecond,
	}
	for _, test := range []struct {
		name string
		// slow are the call kinds L1 holds once shutdown starts.
		slow []string
		// timedOut are the kinds that must each reach their timeout path.
		timedOut []string
		// route is the most this route may spend after cancellation.
		route time.Duration
	}{
		{
			// Undelivered logs: the final flush runs to the finalization
			// timeout plus the request in flight, the completion is left to
			// evidence recovery, and the result upload runs to its timeout.
			name: "stuck logs", slow: []string{"logs", "result"}, timedOut: []string{"logs", "result"},
			route: timeouts.TerminationGrace + timeouts.ProcessReap + timeouts.Finalization + 2*timeouts.Operation,
		},
		{
			// Delivered logs: the live completion runs to its timeout, and a
			// completion that failed uploads no result.
			name: "slow completion", slow: []string{"complete"}, timedOut: []string{"complete"},
			route: timeouts.TerminationGrace + timeouts.ProcessReap + timeouts.Operation,
		},
		{
			// The completion lands at once, and the result upload runs to its
			// timeout.
			name: "slow result upload", slow: []string{"result"}, timedOut: []string{"result"},
			route: timeouts.TerminationGrace + timeouts.ProcessReap + timeouts.Operation,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bound := ForcedShutdownBound(timeouts)
			if test.route > bound {
				t.Fatalf("ForcedShutdownBound %s does not cover this route's phases %s", bound, test.route)
			}
			elapsed, calls := forcedShutdownAgainstSlowL1(t, timeouts, test.slow)
			for _, kind := range test.timedOut {
				reached := false
				for _, call := range calls {
					// The agent, not the test, ended the call: it was held for
					// that call's own timeout.
					reached = reached || call.kind == kind && call.held >= timeouts.Operation*9/10
				}
				if !reached {
					t.Fatalf("the %s call never reached its timeout path: %+v", kind, calls)
				}
			}
			// Scheduling and process exit get a little slack, never a phase.
			if slack := 300 * time.Millisecond; elapsed > test.route+slack {
				t.Fatalf("forced shutdown took %s, more than this route's phases %s plus %s (bound %s); held calls %+v",
					elapsed, test.route, slack, bound, calls)
			}
			t.Logf("forced shutdown took %s; route phases %s; bound %s; held calls %+v", elapsed, test.route, bound, calls)
		})
	}
}

// forcedShutdownAgainstSlowL1 runs one resident attempt, makes L1 hold the
// slow call kinds, cancels the agent, and reports how long Run took to
// return and which calls L1 held.
func forcedShutdownAgainstSlowL1(t *testing.T, timeouts ShutdownTimeouts, slowKinds []string) (time.Duration, []slowL1Call) {
	t.Helper()
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "bound.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	l1Server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		"node-1": l1.DefaultNodePolicy("linux"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	var slow atomic.Bool
	var mu sync.Mutex
	var calls []slowL1Call
	var holding sync.WaitGroup
	heldLogs := make(chan struct{}, 1)
	handler := l1Server.Handler()
	slowed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kind := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		if slow.Load() && strings.HasPrefix(r.URL.Path, "/v1/agent/jobs/") && slices.Contains(slowKinds, kind) {
			if kind == "logs" {
				select {
				case heldLogs <- struct{}{}:
				default:
				}
			}
			holding.Add(1)
			defer holding.Done()
			started := time.Now()
			// Reading the body lets the server notice when the agent hangs
			// up, which is how a held call ends.
			_, _ = io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
			mu.Lock()
			calls = append(calls, slowL1Call{kind: kind, held: time.Since(started)})
			mu.Unlock()
			return
		}
		handler.ServeHTTP(w, r)
	})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: slowed}
	go func() { _ = httpServer.Serve(listener) }()
	defer httpServer.Close()

	runtime := processrunner.NewAdapterForBoot(processrunner.New(processrunner.Config{
		TerminationGraceTime: timeouts.TerminationGrace, ProcessReapTimeout: timeouts.ProcessReap,
	}), "boot-1")
	nodeAgent, err := New(Config{
		Fabric:              network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}}),
		ControlPlaneAddress: "wefty://control-plane", NodeID: "node-1", BootSessionID: "boot-1", Version: "test",
		Capabilities:      map[string]bool{"kind:process": true},
		WorkloadRuntimes:  map[string]WorkloadRuntime{contract.JobKindProcess: runtime},
		HeartbeatInterval: time.Second, ClaimInterval: 10 * time.Millisecond, RenewalInterval: 200 * time.Millisecond,
		LogFlushInterval: 20 * time.Millisecond, LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir(),
		FinalizationTimeout: timeouts.Finalization, OperationTimeout: timeouts.Operation,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()

	client := newHTTPClient(network.NewFabric(fabric.Identity{NodeID: "client", Tags: []string{l1.DefaultClientPrincipalTag}}), "wefty://control-plane")
	defer client.CloseIdleConnections()
	directory := t.TempDir()
	ready := filepath.Join(directory, "ready")
	submitE2EJob(t, client, contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "bound-" + fmt.Sprint(time.Now().UnixNano()),
		Kind: "process", Class: contract.JobClassOneShot, RoutingTags: []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: "/bin/sh"},
			// No handoff directory: the node manages one, owned by the job,
			// and uploads the result written there. The payload ignores
			// SIGTERM and keeps writing, so log batches are always pending.
			Argv: []string{"sh", "-c", `trap '' TERM; printf '{"passed":false,"gates":[]}' > "$WEFTY_HANDOFF_DIR/result.json"; touch ` +
				ready + `; while :; do echo tick; sleep 0.02; done`},
			WorkingDirectory: directory,
		},
	})
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the attempt never started")
		}
	}
	time.Sleep(100 * time.Millisecond)

	slow.Store(true)
	// With slow logs, shutdown starts only once a log batch is stuck, so the
	// final flush has something it cannot deliver.
	if slices.Contains(slowKinds, "logs") {
		select {
		case <-heldLogs:
		case <-time.After(5 * time.Second):
			t.Fatal("no log batch reached the slow L1")
		}
	}
	started := time.Now()
	cancel()
	bound := ForcedShutdownBound(timeouts)
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run after forced shutdown = %v", err)
		}
	case <-time.After(bound + 10*time.Second):
		t.Fatalf("forced shutdown still running %s after cancellation; its bound is %s", bound+10*time.Second, bound)
	}
	elapsed := time.Since(started)
	// Every held call ended when the agent gave up on it; let the handlers
	// record that before reading.
	released := make(chan struct{})
	go func() { holding.Wait(); close(released) }()
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("a held L1 call was never abandoned by the agent")
	}
	mu.Lock()
	defer mu.Unlock()
	return elapsed, append([]slowL1Call(nil), calls...)
}
