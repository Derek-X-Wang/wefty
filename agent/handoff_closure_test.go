//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func retentionLifecycle(t *testing.T, manager *handoffManager, rejectCompletion bool, run completionDirectiveRunFunc) *attemptLifecycle {
	t.Helper()
	client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if rejectCompletion {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorLeaseExpired, Message: "attempt no longer owns completion"}})
			return
		}
		_ = json.NewEncoder(w).Encode(l1.Job{})
	}), time.Second)
	t.Cleanup(stop)
	t.Cleanup(client.Close)
	return newAttemptLifecycle(attemptLifecycleDependencies{
		client: client, handoffs: manager, runtimes: testRuntimeSet(run),
		nodeID: "node-1", bootSessionID: "retention-boot", clock: systemClock{},
		observer: newLifecycleObserver(systemClock{}), renewalInterval: 10 * time.Second,
		completionRetry: time.Millisecond,
	})
}

func retentionClaim(t *testing.T, runID, path string) l1.Claim {
	t.Helper()
	claim := handoffClaim(runID, path, nil)
	claim.Job.JobID = "job-" + runID
	claim.Job.Spec.Execution.WorkingDirectory = t.TempDir()
	claim.Lease.LeaseTTL = time.Minute
	claim.Lease.FencingToken = "retention-fence"
	return claim
}

func successfulRetentionRun(_ context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	zero := 0
	return contract.ProcessResult{ExitCode: &zero}, nil
}

func TestFinishingAnAttemptCollectsWithoutAnyoneAskingIt(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	stale := harness.retain("run_stale", true, true, map[string]int{"result.json": 32})
	harness.now = harness.now.Add(2 * time.Hour)
	path := filepath.Join(harness.root, "run_fresh")
	lifecycle := retentionLifecycle(t, harness.manager, false, successfulRetentionRun)
	if _, err := lifecycle.execute(t.Context(), retentionClaim(t, "run_fresh", path), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("execute did not expire old results: %v", err)
	}
	record := requireRetentionRecord(t, harness.manager, "run_fresh")
	if !record.Succeeded || !record.Published {
		t.Fatalf("fallback won before completion: %#v", record)
	}
	if !record.RetainedAt.Equal(harness.now) || !record.RetainUntil.Equal(harness.now.Add(time.Hour)) {
		t.Fatalf("wrong retention window: %#v", record)
	}
}

func TestAnAttemptThatNeverCompletesStillRetainsItsResults(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.manager.runBytes = 1024
	stale := harness.retain("run_stale", true, true, map[string]int{"result.json": 32})
	harness.now = harness.now.Add(2 * time.Hour)
	path := filepath.Join(harness.root, "run_abandoned")
	lifecycle := retentionLifecycle(t, harness.manager, true, func(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
		if err := os.WriteFile(filepath.Join(path, "payload.bin"), make([]byte, 4096), 0600); err != nil {
			return contract.ProcessResult{}, err
		}
		if err := os.WriteFile(filepath.Join(path, "result.json"), []byte("{}"), 0600); err != nil {
			return contract.ProcessResult{}, err
		}
		return successfulRetentionRun(ctx, request, sink)
	})
	if _, err := lifecycle.execute(t.Context(), retentionClaim(t, "run_abandoned", path), time.Now()); err == nil {
		t.Fatal("rejected completion unexpectedly succeeded")
	}
	record := requireRetentionRecord(t, harness.manager, "run_abandoned")
	if record.Succeeded || record.Published {
		t.Fatalf("fallback invented completion: %#v", record)
	}
	if _, err := os.Stat(filepath.Join(path, "payload.bin")); !os.IsNotExist(err) {
		t.Fatalf("fallback did not trim: %v", err)
	}
	if _, err := os.Stat(filepath.Join(path, "result.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("fallback did not collect: %v", err)
	}
	harness.manager.mu.Lock()
	remaining := len(harness.manager.paths)
	harness.manager.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("fallback retained %d path locks", remaining)
	}
}

func waitHandoffReferences(t *testing.T, manager *handoffManager, path string, want int) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		manager.mu.Lock()
		refs := 0
		if entry := manager.paths[path]; entry != nil {
			refs = entry.refs
		}
		manager.mu.Unlock()
		if refs == want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("path references never reached %d (last %d)", want, refs)
		case <-tick.C:
		}
	}
}

func TestHandoffCancelledWaiterCannotRetainOwnersResults(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.manager.runBytes = 1024
	path := filepath.Join(harness.root, "run_shared")
	claim := retentionClaim(t, "run_shared", path)
	owner := prepareHandoffForTest(t, harness.manager, claim.Job.Spec)
	payload := filepath.Join(path, "live.bin")
	if err := os.WriteFile(payload, make([]byte, 4096), 0600); err != nil {
		t.Fatal(err)
	}
	lifecycle := retentionLifecycle(t, harness.manager, true, func(context.Context, processrunner.Request, processrunner.OutputSink) (contract.ProcessResult, error) {
		t.Error("canceled waiter ran a workload")
		return contract.ProcessResult{}, errors.New("unexpected execution")
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := lifecycle.execute(ctx, claim, time.Now()); done <- err }()
	waitHandoffReferences(t, harness.manager, path, 2)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled waiter succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled waiter did not exit")
	}
	if lifecycle.handoffOwnership != nil || lifecycle.resultsRetained.Load() {
		t.Fatal("waiter acquired a retention receipt or latched finalization")
	}
	info, err := os.Stat(payload)
	if err != nil || info.Size() != 4096 {
		t.Fatalf("waiter changed owner's live file: %v %v", info, err)
	}
	if records := harness.manager.loadRecords(); len(records) != 0 {
		t.Fatalf("waiter recorded owner's run: %#v", records)
	}
	if err := harness.manager.finish(nil, claim.Job.Spec, "node-1", false, false); err != nil {
		t.Fatal(err)
	}
	owner.lease.release()
	if err := harness.manager.finish(owner, claim.Job.Spec, "node-1", false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(payload); err != nil {
		t.Fatalf("released receipt remained usable: %v", err)
	}
}

func TestHandoffAttemptArrivingDuringExpiryAcquiresAfterDeletion(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path := harness.retain("run_expiring", true, true, map[string]int{"result.json": 32})
	harness.now = harness.now.Add(2 * time.Hour)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	harness.manager.logf = func(format string, args ...any) {
		if format == "agent: removing expired results for run %s" {
			close(entered)
			<-release
		}
	}
	collected := make(chan error, 1)
	go func() { collected <- harness.manager.collect() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("collector did not reach deletion")
	}
	acquired := make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	spec := handoffClaim("run_expiring", path, nil).Job.Spec
	go func() {
		lease, err := harness.manager.lock(ctx, spec)
		if err != nil {
			acquired <- err
			return
		}
		defer lease.release()
		_, err = harness.manager.prepare(lease, spec, "node-1")
		acquired <- err
	}()
	waitHandoffReferences(t, harness.manager, path, 2)
	unblock()
	select {
	case err := <-collected:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("collection blocked")
	}
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("collector stranded arriving attempt")
	}
	if _, err := os.Stat(filepath.Join(path, "result.json")); !os.IsNotExist(err) {
		t.Fatalf("old results survived: %v", err)
	}
	if _, err := os.Stat(filepath.Join(path, handoffMarkerName)); err != nil {
		t.Fatalf("new directory was not prepared: %v", err)
	}
	waitHandoffReferences(t, harness.manager, path, 0)
}

func TestHandoffFIFOMarkerDoesNotBlockPreparation(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path := filepath.Join(harness.root, "run_fifo")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(path, handoffMarkerName)
	if err := syscall.Mkfifo(marker, 0600); err != nil {
		t.Fatal(err)
	}
	spec := handoffClaim("run_fifo", path, nil).Job.Spec
	lease, err := harness.manager.lock(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	done := make(chan error, 1)
	go func() { _, err := harness.manager.prepare(lease, spec, "node-1"); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO marker accepted")
		}
	case <-time.After(time.Second):
		// Unblock a regressed blocking open before reporting the failure.
		file, _ := os.OpenFile(marker, os.O_RDWR|syscall.O_NONBLOCK, 0)
		if file != nil {
			file.Close()
		}
		t.Fatal("FIFO marker blocked preparation")
	}
}

func TestHandoffNonemptyResultDirectoryIsRemovedBeforeBounding(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.manager.runBytes = 1024
	path := filepath.Join(harness.root, "run_directory_result")
	spec := handoffClaim("run_directory_result", path, nil).Job.Spec
	owner := prepareHandoffForTest(t, harness.manager, spec)
	if err := os.Mkdir(filepath.Join(path, "result.json"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"result.json/nested.bin", "oversize.bin"} {
		if err := os.WriteFile(filepath.Join(path, name), make([]byte, 4096), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := harness.manager.finish(owner, spec, "node-1", true, true); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"result.json", "oversize.bin"} {
		if _, err := os.Lstat(filepath.Join(path, name)); !os.IsNotExist(err) {
			t.Fatalf("unbounded entry %s survived: %v", name, err)
		}
	}
	if size := measureRun(t, harness.manager, "run_directory_result"); size > harness.manager.runBytes {
		t.Fatalf("still over bound: %d", size)
	}
}

func TestHandoffFinalizationUsesPreparedDirectoryAfterPathReplacement(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.manager.runBytes = 1024
	path := filepath.Join(harness.root, "run_original")
	spec := handoffClaim("run_original", path, nil).Job.Spec
	owner := prepareHandoffForTest(t, harness.manager, spec)
	if err := os.WriteFile(filepath.Join(path, "large.bin"), make([]byte, 4096), 0600); err != nil {
		t.Fatal(err)
	}
	moved := path + "-moved"
	if err := os.Rename(path, moved); err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(harness.root, "run_victim")
	if err := os.Mkdir(victim, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(victim, "keep.bin"), make([]byte, 4096), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("run_victim", path); err != nil {
		t.Fatal(err)
	}
	if root, err := harness.manager.openRun("run_original"); err == nil {
		root.Close()
		t.Fatal("symlinked run opened")
	}
	if err := harness.manager.finish(owner, spec, "node-1", true, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(victim, "keep.bin")); err != nil {
		t.Fatalf("trim redirected to another run: %v", err)
	}
	if _, err := os.Stat(filepath.Join(moved, "large.bin")); !os.IsNotExist(err) {
		t.Fatalf("prepared handle was not trimmed: %v", err)
	}
}

func TestHandoffNoFollowOpenRefusesRelativeSymlinks(t *testing.T) {
	path := t.TempDir()
	if err := os.Mkdir(filepath.Join(path, "directory"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "regular"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	for _, test := range []struct {
		name  string
		flags int
	}{
		{"directory", syscall.O_DIRECTORY}, {"regular", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			alias := test.name + "-alias"
			if err := root.Symlink(test.name, alias); err != nil {
				t.Fatal(err)
			}
			file, err := openHandoffFile(root, alias, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|test.flags, 0)
			if err == nil {
				file.Close()
				t.Fatal("no-follow open resolved a relative symlink")
			}
		})
	}
}

func TestHandoffExpiryKeepsRootHandleAfterAncestorReplacement(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	original := harness.retain("run_expiring", true, true, map[string]int{"result.json": 32})
	harness.now = harness.now.Add(2 * time.Hour)
	elsewhere := t.TempDir()
	victim := filepath.Join(elsewhere, "run_expiring")
	if err := os.Mkdir(victim, 0700); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(victim, "keep.bin")
	if err := os.WriteFile(keep, []byte("not retained by this agent"), 0600); err != nil {
		t.Fatal(err)
	}
	moved := harness.root + "-moved"
	harness.manager.logf = func(format string, args ...any) {
		if format != "agent: removing expired results for run %s" {
			return
		}
		if err := os.Rename(harness.root, moved); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(elsewhere, harness.root); err != nil {
			t.Fatal(err)
		}
	}
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("expiry followed replacement ancestor: %v", err)
	}
	if _, err := os.Stat(filepath.Join(moved, filepath.Base(original))); !os.IsNotExist(err) {
		t.Fatalf("expiry lost original root handle: %v", err)
	}
}
