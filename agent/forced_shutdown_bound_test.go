//go:build darwin || linux

package agent

import (
	"cmp"
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
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// finalFlushDeadline is the event of the agent reporting that the final log
// flush ran to its finalization timeout. Every other shutdown event is an
// agent call to a slow kind, named by its kind.
const finalFlushDeadline = "final log flush deadline"

// shutdownEvent is one recorded step of a forced shutdown, seen from the
// agent's side: a call it made to a slow kind, or its report that the final
// log flush hit its deadline.
type shutdownEvent struct {
	kind string
	// at is when the agent sent the call (or reported the flush deadline),
	// and end is when the call returned to it; both are measured from the
	// cancellation.
	at, end time.Duration
	// ended is why the call returned: the error of its request context, or
	// nil if that context was still live.
	ended error
}

func (event shutdownEvent) String() string {
	if event.kind == finalFlushDeadline {
		return fmt.Sprintf("(%s at %s)", event.kind, event.at)
	}
	return fmt.Sprintf("(%s call from %s to %s, ended by %v)", event.kind, event.at, event.end, event.ended)
}

// reachedTimeoutPath reports whether the phase behind this event ended on its
// own timeout. The agent reports the flush deadline only on that path. L1
// never answers a held call, so the call returns only when the agent stops
// waiting for it, and only an expired deadline on its own request context
// counts: an explicit cancellation does not, however late it comes.
func (event shutdownEvent) reachedTimeoutPath() bool {
	return event.kind == finalFlushDeadline || errors.Is(event.ended, context.DeadlineExceeded)
}

// l1Hold is how long the slow L1 held one call before the agent hung up. It
// is only diagnostic: L1 sees a call late when a busy runner delays it, so
// its view of the hold is not the agent's.
type l1Hold struct {
	kind     string
	at, held time.Duration
}

func (hold l1Hold) String() string {
	return fmt.Sprintf("(%s at %s for %s)", hold.kind, hold.at, hold.held)
}

// shutdownCallObserver is the agent client's transport in this test. It
// records each call to a slow kind once shutdown is under way, as the agent
// saw it.
type shutdownCallObserver struct {
	base    http.RoundTripper
	observe func(*http.Request) (kind string, observed bool)
	record  func(event shutdownEvent, at, end time.Time)
}

func (observer shutdownCallObserver) RoundTrip(request *http.Request) (*http.Response, error) {
	kind, observed := observer.observe(request)
	if !observed {
		return observer.base.RoundTrip(request)
	}
	at := time.Now()
	response, err := observer.base.RoundTrip(request)
	end := time.Now()
	observer.record(shutdownEvent{kind: kind, ended: requestEnd(request.Context(), end)}, at, end)
	return response, err
}

// requestEnd is the error of a request context once its round trip has
// returned. The HTTP client's own timeout can end the round trip a moment
// before the context's deadline timer runs, so a context whose deadline has
// passed counts as expired even before its error is set.
func requestEnd(ctx context.Context, end time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !end.Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// TestForcedShutdownEndsWithinItsBoundWhenL1IsSlow holds ForcedShutdownBound
// to what a forced shutdown really takes. The resident payload ignores
// SIGTERM, so the runner spends its whole termination grace, and it writes a
// result into its managed handoff, so the agent uploads one. Once shutdown
// starts, L1 holds the chosen calls until the agent gives up on each. Every
// case checks, from the recorded events and their order, that each phase of
// its route reached its own timeout path after the phase before it, and that
// the shutdown ended within ForcedShutdownBound. A phase that never times out
// keeps Run from returning, and one that overruns its timeout far enough to
// matter pushes the shutdown past the bound; both fail here.
func TestForcedShutdownEndsWithinItsBoundWhenL1IsSlow(t *testing.T) {
	// The bound allows three operation timeouts and the slowest route here
	// spends two, so one operation timeout is the room the bound check leaves
	// for scheduling stalls; a hosted macOS runner once stalled one held call
	// by 400 ms. Finalization stays twice the operation timeout, so a final
	// flush that ran to twice its own timeout would still overrun the bound.
	timeouts := ShutdownTimeouts{
		TerminationGrace: 100 * time.Millisecond, ProcessReap: 100 * time.Millisecond,
		Finalization: 2 * time.Second, Operation: time.Second,
	}
	for _, test := range []struct {
		name string
		// slow are the call kinds L1 holds once shutdown starts.
		slow []string
		// phases are the events that show each phase of this route reaching
		// its timeout path, in the order the phases run.
		phases []string
		// route is what this route's phases add up to. The bound must cover
		// it.
		route time.Duration
	}{
		{
			// Undelivered logs: the final flush runs to the finalization
			// timeout, the completion falls to inline evidence recovery,
			// whose log replay runs to its timeout, and the result upload
			// runs to its timeout.
			name: "stuck logs", slow: []string{"logs", "result"}, phases: []string{finalFlushDeadline, "logs", "result"},
			route: timeouts.TerminationGrace + timeouts.ProcessReap + timeouts.Finalization + 2*timeouts.Operation,
		},
		{
			// Delivered logs: the live completion runs to its timeout.
			name: "slow completion", slow: []string{"complete"}, phases: []string{"complete"},
			route: timeouts.TerminationGrace + timeouts.ProcessReap + timeouts.Operation,
		},
		{
			// The completion lands at once, and the result upload runs to its
			// timeout.
			name: "slow result upload", slow: []string{"result"}, phases: []string{"result"},
			route: timeouts.TerminationGrace + timeouts.ProcessReap + timeouts.Operation,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bound := ForcedShutdownBound(timeouts)
			if test.route > bound {
				t.Fatalf("ForcedShutdownBound %s does not cover this route's phases %s", bound, test.route)
			}
			elapsed, events, holds := forcedShutdownAgainstSlowL1(t, timeouts, test.slow)
			// Order, not wall-clock slack, ties the phases together: the agent
			// starts a phase's call only after the phase before has ended,
			// however late a busy runner schedules either.
			after := time.Duration(0)
			for _, phase := range test.phases {
				found := slices.IndexFunc(events, func(event shutdownEvent) bool {
					return event.kind == phase && event.at >= after && event.reachedTimeoutPath()
				})
				if found < 0 {
					t.Fatalf("the %s phase never reached its timeout path after the phases before it; events %v; L1 held %v", phase, events, holds)
				}
				after = events[found].end
			}
			// A stall stretches the phases, but the bound must still cover
			// them: it is the only ceiling.
			if elapsed > bound {
				t.Fatalf("forced shutdown took %s, more than ForcedShutdownBound %s; events %v; L1 held %v", elapsed, bound, events, holds)
			}
			t.Logf("forced shutdown took %s; route phases %s; bound %s; events %v; L1 held %v", elapsed, test.route, bound, events, holds)
		})
	}
}

// forcedShutdownAgainstSlowL1 runs one resident attempt, makes L1 hold the
// slow call kinds, cancels the agent, and reports how long Run took to
// return, the shutdown events in the order they happened, and how long L1
// held each call.
func forcedShutdownAgainstSlowL1(t *testing.T, timeouts ShutdownTimeouts, slowKinds []string) (time.Duration, []shutdownEvent, []l1Hold) {
	t.Helper()
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "bound.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// A loaded runner can hold the job submission's post-change read past
	// the 200 ms production limit, which answers a retryable 503 (#839).
	store.SetReadSnapshotHardLimitForTest(2 * time.Second)
	l1Server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		"node-1": l1.DefaultNodePolicy("linux"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	var slow atomic.Bool
	var mu sync.Mutex
	// Events and holds are stamped on one clock and measured from the
	// cancellation once Run has returned.
	type stampedEvent struct {
		event   shutdownEvent
		at, end time.Time
	}
	type stampedHold struct {
		kind string
		at   time.Time
		held time.Duration
	}
	var stamped []stampedEvent
	var stampedHolds []stampedHold
	record := func(event shutdownEvent, at, end time.Time) {
		mu.Lock()
		stamped = append(stamped, stampedEvent{event: event, at: at, end: end})
		mu.Unlock()
	}
	slowCall := func(r *http.Request) (string, bool) {
		kind := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		return kind, slow.Load() && strings.HasPrefix(r.URL.Path, "/v1/agent/jobs/") && slices.Contains(slowKinds, kind)
	}
	var holding sync.WaitGroup
	heldLogs := make(chan struct{}, 1)
	handler := l1Server.Handler()
	slowed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if kind, held := slowCall(r); held {
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
			stampedHolds = append(stampedHolds, stampedHold{kind: kind, at: started, held: time.Since(started)})
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
		Logf: func(format string, args ...any) {
			// The agent reports a final flush cut off by its finalization
			// deadline in this one line.
			if strings.Contains(fmt.Sprintf(format, args...), string(logFinalizationStageUpload)+" exceeded the bounded finalization deadline") {
				now := time.Now()
				record(shutdownEvent{kind: finalFlushDeadline}, now, now)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	agentClient := nodeAgent.session.client
	agentClient.httpClient.Transport = shutdownCallObserver{base: agentClient.httpClient.Transport, observe: slowCall, record: record}
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
		case <-time.After(testwait.Bound):
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
	case <-time.After(testwait.Bound):
		t.Fatal("a held L1 call was never abandoned by the agent")
	}
	mu.Lock()
	defer mu.Unlock()
	events := make([]shutdownEvent, 0, len(stamped))
	for _, stamp := range stamped {
		stamp.event.at, stamp.event.end = stamp.at.Sub(started), stamp.end.Sub(started)
		events = append(events, stamp.event)
	}
	slices.SortStableFunc(events, func(a, b shutdownEvent) int { return cmp.Compare(a.at, b.at) })
	holds := make([]l1Hold, 0, len(stampedHolds))
	for _, hold := range stampedHolds {
		holds = append(holds, l1Hold{kind: hold.kind, at: hold.at.Sub(started), held: hold.held})
	}
	slices.SortStableFunc(holds, func(a, b l1Hold) int { return cmp.Compare(a.at, b.at) })
	return elapsed, events, holds
}
