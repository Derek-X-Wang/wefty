//go:build darwin || linux

package agent

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestHandoffPathLockSpansPrepareThroughFinish(t *testing.T) {
	assertHandoffPathLockSpansPrepareThroughFinish(t)
}

func assertHandoffPathLockSpansPrepareThroughFinish(t *testing.T) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "handoffs")
	runID := "run-shared-path"
	path := filepath.Join(root, runID)
	manager := newHandoffManager(root, time.Hour)
	spec := handoffClaim(runID, path, nil).Job.Spec

	unlockFirst, err := manager.lock(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.prepare(spec, "node-1"); err != nil {
		unlockFirst()
		t.Fatal(err)
	}

	secondPrepared := make(chan error, 1)
	releaseSecond := make(chan struct{})
	go func() {
		unlockSecond, err := manager.lock(context.Background(), spec)
		if err != nil {
			secondPrepared <- err
			return
		}
		defer unlockSecond()
		secondPrepared <- manager.prepare(spec, "node-1")
		<-releaseSecond
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		manager.mu.Lock()
		refs := 0
		if pathLock := manager.paths[path]; pathLock != nil {
			refs = pathLock.refs
		}
		manager.mu.Unlock()
		if refs == 2 {
			break
		}
		if time.Now().After(deadline) {
			unlockFirst()
			t.Fatal("second attempt did not wait on the shared handoff path")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-secondPrepared:
		unlockFirst()
		t.Fatalf("second prepare completed before first finish: %v", err)
	default:
	}

	if err := manager.finish(spec, "node-1", true); err != nil {
		unlockFirst()
		t.Fatal(err)
	}
	unlockFirst()
	if err := <-secondPrepared; err != nil {
		close(releaseSecond)
		t.Fatalf("second prepare after first finish: %v", err)
	}
	close(releaseSecond)

	deadline = time.Now().Add(5 * time.Second)
	for {
		manager.mu.Lock()
		remaining := len(manager.paths)
		manager.mu.Unlock()
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("handoff path lock registry retained %d entries", remaining)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestOutputSinkFactorySupportsConcurrentAttemptIsolation(t *testing.T) {
	assertOutputSinkFactorySupportsConcurrentAttemptIsolation(t)
}

func assertOutputSinkFactorySupportsConcurrentAttemptIsolation(t *testing.T) {
	t.Helper()
	entered := make(chan string, 2)
	release := make(chan struct{})
	outputs := make(map[string]string)
	var outputsMu sync.Mutex
	factory := func(claim l1.Claim) processrunner.OutputSink {
		attemptID := claim.Lease.AttemptID
		entered <- attemptID
		<-release
		return processrunner.OutputSinkFunc(func(_ context.Context, event contract.LogEvent) error {
			outputsMu.Lock()
			outputs[attemptID] += string(event.Bytes)
			outputsMu.Unlock()
			return nil
		})
	}
	a := &Agent{runtimes: testRuntimeSet(concurrencyOutputRunner{}), outputSinkFactory: factory}
	done := make(chan error, 2)
	for index := 1; index <= 2; index++ {
		attemptID := fmt.Sprintf("attempt-%d", index)
		claim := l1.Claim{
			Job: l1.Job{JobID: fmt.Sprintf("job-%d", index), Spec: contract.JobSpec{
				Kind: "process", Class: contract.JobClassOneShot,
			}},
			Lease: l1.AttemptLease{AttemptID: attemptID},
		}
		go func() {
			_, err := a.runWorkload(context.Background(), claim)
			done <- err
		}()
	}

	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case attemptID := <-entered:
			seen[attemptID] = true
		case <-time.After(5 * time.Second):
			t.Fatal("output sink factory was not invoked concurrently")
		}
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	outputsMu.Lock()
	defer outputsMu.Unlock()
	for attemptID := range seen {
		if outputs[attemptID] != attemptID {
			t.Fatalf("sink output for %s = %q, want attempt-isolated attribution", attemptID, outputs[attemptID])
		}
	}
}

func TestSerialLogfContainsConcurrentCallers(t *testing.T) {
	assertSerialLogfContainsConcurrentCallers(t)
}

func assertSerialLogfContainsConcurrentCallers(t *testing.T) {
	t.Helper()
	entered := make(chan struct{}, 2)
	release := make(chan struct{}, 2)
	var active atomic.Int32
	var overlapped atomic.Bool
	logf := serialLogf(func(string, ...any) {
		if active.Add(1) != 1 {
			overlapped.Store(true)
		}
		entered <- struct{}{}
		<-release
		active.Add(-1)
	})
	done := make(chan struct{}, 2)
	for range 2 {
		go func() {
			logf("attempt log")
			done <- struct{}{}
		}()
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first logger call did not enter")
	}
	select {
	case <-entered:
		t.Fatal("configured logger was called concurrently")
	case <-time.After(25 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("second logger call did not enter after serialization")
	}
	release <- struct{}{}
	<-done
	<-done
	if overlapped.Load() {
		t.Fatal("configured logger observed overlapping calls")
	}
}

type concurrencyOutputRunner struct{}

func (concurrencyOutputRunner) Run(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	if err := sink.WriteOutput(ctx, contract.LogEvent{
		AttemptID: request.AttemptID, Stream: contract.LogStdout, Bytes: []byte(request.AttemptID),
	}); err != nil {
		return contract.ProcessResult{}, err
	}
	exitCode := 0
	return contract.ProcessResult{ExitCode: &exitCode}, nil
}
