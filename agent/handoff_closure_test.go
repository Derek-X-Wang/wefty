//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
	// Preparation records the owner's admission, so what must not exist here
	// is a *terminal* record: the waiter retaining results for a run whose
	// directory it never held.
	for _, record := range harness.manager.loadRecords() {
		if record.RunID != "run_shared" || !record.RetainUntil.IsZero() {
			t.Fatalf("waiter recorded owner's run as retained: %#v", record)
		}
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
		if format == "agent: removing run %s's retained results: %s" {
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

// TestHandoffFIFOMarkerDoesNotBlockPreparation covers both halves of the marker
// read, and the second half is the one that matters.
//
// A FIFO planted before the Lstat is refused by the mode check, so a test that
// plants it there passes whether or not the non-blocking open and the post-open
// identity check exist at all -- it proves nothing about either. The guards
// those two exist for are only reached when the name is a regular file at the
// check and a FIFO by the time it is opened, which is exactly the swap a
// same-UID workload can perform, so that is what the second case does.
func TestHandoffFIFOMarkerDoesNotBlockPreparation(t *testing.T) {
	t.Run("a FIFO already in place is refused by the mode check", func(t *testing.T) {
		harness := newRetentionHarness(t, time.Hour)
		path := filepath.Join(harness.root, "run_fifo")
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
		marker := filepath.Join(path, handoffMarkerName)
		if err := syscall.Mkfifo(marker, 0600); err != nil {
			t.Fatal(err)
		}
		err := prepareWithoutBlocking(t, harness, "run_fifo", path, marker)
		if err == nil || !strings.Contains(err.Error(), "not a regular file") {
			t.Fatalf("preparation error = %v, want one naming the mode check", err)
		}
	})

	t.Run("a regular marker swapped for a FIFO after its check is refused", func(t *testing.T) {
		harness := newRetentionHarness(t, time.Hour)
		path := filepath.Join(harness.root, "run_fifo_swapped")
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
		marker := filepath.Join(path, handoffMarkerName)
		// Regular, valid, and this run's own: the mode check has no reason to
		// refuse it, so everything below is the later guards' work.
		if err := os.WriteFile(marker, markerPayload(t, "run_fifo_swapped", "node-1", harness.now), 0600); err != nil {
			t.Fatal(err)
		}
		// The swap happens in the window the seam names: after the Lstat that
		// found a regular file, before the open. No writer is attached, so a
		// regressed blocking open hangs here and the deadline below reports it.
		var once sync.Once
		handoffMarkerOpenRace = func() {
			once.Do(func() {
				if err := os.Remove(marker); err != nil {
					t.Error(err)
					return
				}
				if err := syscall.Mkfifo(marker, 0600); err != nil {
					t.Error(err)
				}
			})
		}
		t.Cleanup(func() { handoffMarkerOpenRace = nil })

		err := prepareWithoutBlocking(t, harness, "run_fifo_swapped", path, marker)
		if err == nil || !strings.Contains(err.Error(), "changed identity while opening") {
			t.Fatalf("preparation error = %v, want the post-open identity refusal", err)
		}
	})

	t.Run("a regular marker swapped for a different regular file is refused", func(t *testing.T) {
		// The swapped-FIFO case above reaches the post-open check with a
		// non-regular mode, so the mode half of it fires and SameFile is never
		// consulted. This case swaps in another *regular* file, so the mode
		// half passes and only SameFile can refuse. The original inode is kept
		// alive by an open descriptor for the whole swap, so the replacement
		// cannot land on a recycled inode number and pass by accident.
		harness := newRetentionHarness(t, time.Hour)
		path := filepath.Join(harness.root, "run_marker_swapped")
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
		marker := filepath.Join(path, handoffMarkerName)
		if err := os.WriteFile(marker, markerPayload(t, "run_marker_swapped", "node-1", harness.now), 0600); err != nil {
			t.Fatal(err)
		}
		// A marker naming another node: if SameFile is gone, this one is read
		// and preparation fails on the node mismatch instead, which is a
		// different error and a failing assertion below.
		replacement := markerPayload(t, "run_marker_swapped", "some-other-node", harness.now)
		var once sync.Once
		handoffMarkerOpenRace = func() {
			once.Do(func() {
				original, err := os.Open(marker)
				if err != nil {
					t.Error(err)
					return
				}
				t.Cleanup(func() { original.Close() })
				if err := os.Remove(marker); err != nil {
					t.Error(err)
					return
				}
				if err := os.WriteFile(marker, replacement, 0600); err != nil {
					t.Error(err)
				}
			})
		}
		t.Cleanup(func() { handoffMarkerOpenRace = nil })

		err := prepareWithoutBlocking(t, harness, "run_marker_swapped", path, marker)
		if err == nil || !strings.Contains(err.Error(), "changed identity while opening") {
			t.Fatalf("preparation error = %v, want the post-open SameFile refusal", err)
		}
	})
}

// markerPayload encodes one ownership marker exactly as the agent writes it.
func markerPayload(t *testing.T, runID, nodeID string, now time.Time) []byte {
	t.Helper()
	payload, err := json.Marshal(handoffMarker{
		RunID: runID, NodeID: nodeID, RetainUntil: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

// prepareWithoutBlocking runs preparation with a deadline and unblocks a
// regressed blocking open on the marker before reporting the failure, so one
// hung open cannot wedge the whole package's test binary.
func prepareWithoutBlocking(t *testing.T, harness *retentionHarness, runID, path, marker string) error {
	t.Helper()
	spec := handoffClaim(runID, path, nil).Job.Spec
	lease, err := harness.manager.lock(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lease.release)
	done := make(chan error, 1)
	go func() { _, err := harness.manager.prepare(lease, spec, "node-1"); done <- err }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		file, _ := os.OpenFile(marker, os.O_RDWR|syscall.O_NONBLOCK, 0)
		if file != nil {
			file.Close()
		}
		t.Fatal("the marker read blocked preparation")
		return nil
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

// TestHandoffFinalizationTrimsNothingWhenTheRunNameWasReplaced: the run's name
// is a symlink to another run by the time the attempt finishes. Nothing is
// trimmed -- not through the link, and not through the prepared handle either,
// which the name no longer leads to -- and the fact that the bound did not run
// is on the record instead of nowhere.
func TestHandoffFinalizationTrimsNothingWhenTheRunNameWasReplaced(t *testing.T) {
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
		t.Fatalf("trim followed the link into another run: %v", err)
	}
	if info, err := os.Stat(filepath.Join(moved, "large.bin")); err != nil || info.Size() != 4096 {
		t.Fatalf("a directory the run's name no longer leads to was trimmed: %v, %v", info, err)
	}
	if !harness.logged("no longer the directory preparation pinned") {
		t.Fatalf("the drift was silent: %v", harness.logs)
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
		if format != "agent: removing run %s's retained results: %s" {
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
