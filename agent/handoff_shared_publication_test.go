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
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// A handoff belongs to its owner, and more than one attempt can write into the
// same one: a rerun, a retry, or a child one-shot naming its parent's run as
// handoff_owner_run_id. Publication is decided per attempt, so these prove the
// node publishes a shared handoff only when every attempt that wrote into it
// published (#661) -- live, after an interrupted attempt, and across an
// upgrade from records that cannot say.
//
// Nothing here names the record field that carries the owner's answer, so the
// same file compiles against the per-attempt rule it replaced and fails there.

const sharedHandoffOwner = "run_shared_owner"

// sharerOutcome is what became of one attempt that wrote into the shared
// handoff.
type sharerOutcome string

const (
	// sharerPublishes: L1 accepted the result and there was no mailbox.
	sharerPublishes sharerOutcome = "publishes"
	// sharerUndrained: L1 accepted the result and the run mailbox did not
	// drain, so the pending events in the handoff are the only copy.
	sharerUndrained sharerOutcome = "undrained"
	// sharerUploadRefused: L1 refused the upload and there was no mailbox.
	sharerUploadRefused sharerOutcome = "upload_refused"
	// sharerRefusedBeforeRun: an OCI attempt whose image observation L1
	// refused before helper Run (#652). It uploads an accepted `absent`,
	// which publishes that attempt.
	sharerRefusedBeforeRun sharerOutcome = "refused_before_run"
)

const sharedHandoffPendingEvent = "wefty-protocol: 1\nkind: gate\nname: test\noutcome: fail\n--\nboom\n"

// runSharer runs one attempt into the shared handoff. The parent's run owns
// the handoff; any other run is a child naming it as handoff_owner_run_id.
func runSharer(t *testing.T, h *retentionHarness, kind, runID, attemptID string, outcome sharerOutcome) {
	t.Helper()
	runSharerFor(t, h, kind, sharedHandoffOwner, runID, attemptID, outcome)
}

// runSharerFor is runSharer for a handoff owned by the run named owner.
func runSharerFor(t *testing.T, h *retentionHarness, kind, owner, runID, attemptID string, outcome sharerOutcome) {
	t.Helper()
	if kind == "process" {
		runProcessSharer(t, h, owner, runID, attemptID, outcome)
		return
	}
	runOCISharer(t, h, owner, runID, attemptID, outcome)
}

func runProcessSharer(t *testing.T, h *retentionHarness, owner, runID, attemptID string, outcome sharerOutcome) {
	t.Helper()
	path := filepath.Join(h.root, owner)
	claim := retentionClaim(t, runID, path)
	claim.Lease.AttemptID = attemptID
	claim.Job.Spec.RoutingTags = []string{contract.StableNodeTagPrefix + "node-1"}
	if runID != owner {
		claim.Job.Spec.Labels["handoff_owner_run_id"] = owner
	}
	recorder := &resultUploadRecorder{}
	if outcome == sharerUploadRefused {
		recorder.status = http.StatusConflict
	}
	run := writingRun(path, "result.json", []byte(`{"ok":true}`))
	lifecycle := uploadingLifecycle(t, h.manager, recorder, run)
	if outcome == sharerUndrained {
		claim.SubmittedByRunLedger = true
		claim.Job.Spec.Execution.Env = map[string]string{contract.EnvRunID: runID, contract.EnvL3Endpoint: "http://ledger.invalid"}
		claim.Job.Spec.Execution.SensitiveEnv = map[string]string{contract.EnvRunToken: mailboxTestToken}
		appender := newRecordingAppender(path)
		appender.failWith(errors.New("run ledger is unreachable"))
		lifecycle.dependencies.runLedger = appender
		lifecycle.dependencies.runtimes = testRuntimeSet(completionDirectiveRunFunc(func(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
			writeMailboxEvent(t, request.Execution.Env[contract.EnvRunDir], "0001-gate-test", sharedHandoffPendingEvent)
			return run(ctx, request, sink)
		}))
	}
	if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil {
		t.Fatal(err)
	}
	if outcome == sharerUndrained && !lifecycle.mailbox.Load().publicationIncomplete() {
		t.Fatal("the fixture's refused events did not leave the drain incomplete")
	}
}

func runOCISharer(t *testing.T, h *retentionHarness, owner, runID, attemptID string, outcome sharerOutcome) {
	t.Helper()
	claim := remoteMailboxClaim("")
	claim.Job.JobID = "job-" + attemptID
	claim.Job.Spec.Labels = map[string]string{"run_id": runID}
	if runID != owner {
		claim.Job.Spec.Labels["handoff_owner_run_id"] = owner
	}
	claim.Lease.AttemptID, claim.Lease.FencingToken, claim.Lease.LeaseTTL = attemptID, "fence-"+attemptID, time.Minute
	recorder := &resultUploadRecorder{}
	if outcome == sharerUploadRefused {
		recorder.status = http.StatusConflict
	}
	handler := recorder.handler()
	runtime := &publicationOCIRuntime{fakeRunMailboxRuntime: newFakeRunMailboxRuntime()}
	var adapter WorkloadRuntime = runtime
	if outcome == sharerRefusedBeforeRun {
		handler = refusingImageObservation(handler)
		adapter = &refusedBeforeRunOCIRuntime{publicationOCIRuntime: runtime}
	}
	lifecycle := lifecycleWithLedger(t, h.manager, handler)
	lifecycle.dependencies.runtimes = workloadRuntimeSet{contract.JobKindOCI: adapter}
	if outcome == sharerUndrained {
		claim.SubmittedByRunLedger = true
		claim.Job.Spec.Execution.Env[contract.EnvRunID] = runID
		appender := newRecordingAppender("")
		appender.failWith(errors.New("run ledger is unreachable"))
		lifecycle.dependencies.runLedger = appender
		lifecycle.dependencies.mailboxStateRoot = t.TempDir()
		runtime.put("0001-gate-test", sharedHandoffPendingEvent)
	} else {
		claim.SubmittedByRunLedger = false
		claim.Job.Spec.Execution.Env = nil
		claim.Job.Spec.Execution.SensitiveEnv = nil
	}
	if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil && outcome != sharerRefusedBeforeRun {
		t.Fatal(err)
	}
	if outcome == sharerUndrained && (!lifecycle.mailbox.Load().publicationIncomplete() || len(runtime.remaining()) != 1) {
		t.Fatal("the fixture's refused event did not stay pending in the volume")
	}
	if outcome == sharerRefusedBeforeRun && runtime.resultRead.Load() {
		t.Fatal("an attempt refused before Run read a result it never produced")
	}
}

// lifecycleWithLedger is uploadingLifecycle with the whole L1 handler supplied.
func lifecycleWithLedger(t *testing.T, manager *handoffManager, handler http.Handler) *attemptLifecycle {
	t.Helper()
	client, stop := startEvidenceReplayServer(t, handler, time.Second)
	t.Cleanup(stop)
	t.Cleanup(client.Close)
	return newAttemptLifecycle(attemptLifecycleDependencies{
		client: client, handoffs: manager, runtimes: testRuntimeSet(completionDirectiveRunFunc(successfulRetentionRun)),
		nodeID: "node-1", bootSessionID: "upload-boot", clock: systemClock{},
		observer: newLifecycleObserver(systemClock{}), renewalInterval: 10 * time.Second,
		completionRetry: time.Millisecond,
	})
}

// refusingImageObservation is L1 refusing the image observation of an attempt
// whose job was canceled during the pull, as it does once cancellation has
// committed: HTTP 409 conflict, not retryable.
func refusingImageObservation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/image") {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
			Code: contract.ErrorConflict, Message: "the job was canceled before its payload started"}})
	})
}

// refusedBeforeRunOCIRuntime reports its image observation and stops when L1
// refuses it, without ever reaching helper Run.
type refusedBeforeRunOCIRuntime struct {
	*publicationOCIRuntime
}

func (r *refusedBeforeRunOCIRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	r.request = request
	digest := "sha256:" + strings.Repeat("a", 64)
	observation := workloadrunner.OCIImageObservation{SubmittedReference: request.Execution.OCI.Image.Reference,
		TopLevelDigest: digest, TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json", PlatformManifestDigest: digest,
		PlatformOS: "linux", PlatformArchitecture: "amd64", RuntimeHandler: request.RuntimeHandler, Snapshotter: "overlayfs"}
	if request.OCIImageResolved == nil {
		return workloadrunner.Result{}, errors.New("the image observation callback is not wired")
	}
	if err := request.OCIImageResolved(context.WithoutCancel(ctx), observation); err != nil {
		return workloadrunner.Result{Outcome: contract.ProcessResult{SpawnError: &contract.SpawnFailure{
			Code: contract.SpawnFailureProcessRequest, Message: err.Error()}}}, err
	}
	return workloadrunner.Result{}, errors.New("the fixture's L1 accepted an image observation it exists to refuse")
}

// sharedHandoffPublished is the owner's answer as the eviction order reads it.
func sharedHandoffPublished(t *testing.T, manager *handoffManager, kind string) bool {
	t.Helper()
	return handoffPublishedFor(t, manager, kind, sharedHandoffOwner)
}

// handoffPublishedFor is sharedHandoffPublished for the handoff owned by owner.
func handoffPublishedFor(t *testing.T, manager *handoffManager, kind, owner string) bool {
	t.Helper()
	if kind == "process" {
		return requireRetentionRecord(t, manager, owner).evidenceReachedLedger()
	}
	record, found, err := manager.readOCIRecord(owner)
	if err != nil || !found || record.live() {
		t.Fatalf("the shared volume has no finished record: %+v found=%t err=%v", record, found, err)
	}
	return record.evidenceReachedLedger()
}

// sharedHandoffCandidates is which class the budget would put the shared
// handoff in, once it holds bytes to give up.
func sharedHandoffCandidates(t *testing.T, h *retentionHarness, kind string) (published, unpublished int) {
	t.Helper()
	if kind == "process" {
		if err := os.WriteFile(filepath.Join(h.root, sharedHandoffOwner, "payload.bin"), make([]byte, 4096), 0o600); err != nil {
			t.Fatal(err)
		}
	} else {
		h.manager.ociHandoffs = &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
			ociVolume(t, sharedHandoffOwner, 4096, 2, h.now)}}
	}
	evictFirst, evictLast := h.manager.evictionCandidates(h.account())
	return len(evictFirst), len(evictLast)
}

func reopenedHandoffManager(t *testing.T, h *retentionHarness) *handoffManager {
	t.Helper()
	reopened := newHandoffManager(h.root, h.manager.stateRoot, "node-1", time.Hour, nil)
	reopened.now = h.manager.now
	if err := reopened.adoptResidue(); err != nil {
		t.Fatal(err)
	}
	return reopened
}

// TestASharedHandoffIsPublishedOnlyWhenEverySharerPublished is #661's
// scenario and the rule behind it. A parent's attempt ends with something of
// its own still only on this node; then a child naming the parent's run as its
// handoff owner is admitted, its result reaches L1, and with no mailbox of its
// own it has nothing to drain. The child's attempt published. The handoff it
// shares did not, and must not move to the front of the eviction order while
// the parent's evidence is in it.
func TestASharedHandoffIsPublishedOnlyWhenEverySharerPublished(t *testing.T) {
	for _, tc := range []struct {
		name           string
		kind           string
		parent, child  sharerOutcome
		ownerPublished bool
	}{
		// The scenario in the ticket: the parent's mailbox did not drain.
		{"process/parent_undrained", "process", sharerUndrained, sharerPublishes, false},
		{"oci/parent_undrained", "oci", sharerUndrained, sharerPublishes, false},
		// The parent's result never reached L1.
		{"process/parent_upload_refused", "process", sharerUploadRefused, sharerPublishes, false},
		{"oci/parent_upload_refused", "oci", sharerUploadRefused, sharerPublishes, false},
		// #652's path to the same gap: a later job on the same run owner is
		// refused before Run and uploads `absent`, which publishes it.
		{"oci/parent_upload_refused_child_refused_before_run", "oci", sharerUploadRefused, sharerRefusedBeforeRun, false},
		{"oci/parent_undrained_child_refused_before_run", "oci", sharerUndrained, sharerRefusedBeforeRun, false},
		// A child that did not publish keeps it unpublished, as before.
		{"process/child_undrained", "process", sharerPublishes, sharerUndrained, false},
		{"oci/child_upload_refused", "oci", sharerPublishes, sharerUploadRefused, false},
		// And sharing is not itself a reason: when every sharer published,
		// the handoff is published.
		{"process/every_sharer_published", "process", sharerPublishes, sharerPublishes, true},
		{"oci/every_sharer_published", "oci", sharerPublishes, sharerPublishes, true},
		{"oci/every_sharer_published_child_refused_before_run", "oci", sharerPublishes, sharerRefusedBeforeRun, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRetentionHarness(t, time.Hour)
			runSharer(t, h, tc.kind, sharedHandoffOwner, "attempt-parent", tc.parent)
			h.now = h.now.Add(time.Minute)
			runSharer(t, h, tc.kind, "run_child", "attempt-child", tc.child)

			// The child's own verdict is what the per-attempt rule used to
			// stand on: it is the last upload recorded for this owner.
			upload := requireUploadRecord(t, h.manager, sharedHandoffOwner)
			childPublished := tc.child == sharerPublishes || tc.child == sharerRefusedBeforeRun
			if upload.AttemptID != "attempt-child" || upload.publishes() != childPublished {
				t.Fatalf("the child's own upload = %+v, want publishes=%t", upload, childPublished)
			}
			if tc.child == sharerRefusedBeforeRun && upload.Reason != contract.ResultUploadSkipAbsent {
				t.Fatalf("an attempt refused before Run uploaded %+v, want an accepted absent", upload)
			}
			if got := sharedHandoffPublished(t, h.manager, tc.kind); got != tc.ownerPublished {
				t.Fatalf("shared handoff published = %t, want %t", got, tc.ownerPublished)
			}
			if tc.kind == "process" && tc.parent == sharerUndrained {
				pending := filepath.Join(h.root, sharedHandoffOwner, runMailboxDirectoryName, sharedHandoffOwner,
					runMailboxEventsDirectoryName, "0001-gate-test")
				if _, err := os.Stat(pending); err != nil {
					t.Fatalf("the only copy of the parent's event is gone: %v", err)
				}
			}
			published, unpublished := sharedHandoffCandidates(t, h, tc.kind)
			if want := map[bool]int{true: 1}[tc.ownerPublished]; published != want || published+unpublished != 1 {
				t.Fatalf("evict-first candidates = %d, evict-last = %d, want published=%t", published, unpublished, tc.ownerPublished)
			}
			// Restart reads the same answer back from what was persisted.
			if got := sharedHandoffPublished(t, reopenedHandoffManager(t, h), tc.kind); got != tc.ownerPublished {
				t.Fatalf("after restart shared handoff published = %t, want %t", got, tc.ownerPublished)
			}
		})
	}
}

// TestASharedHandoffInterruptedAfterUploadRecoversTheOwnersAnswer is the same
// rule through startup recovery. The child's upload is durable and its
// attempt published, then the agent dies before terminal retention. Recovery
// restores the child's verdict from its own upload record, and must still
// answer for the parent's files beside it.
func TestASharedHandoffInterruptedAfterUploadRecoversTheOwnersAnswer(t *testing.T) {
	for _, tc := range []struct {
		kind           string
		parent         sharerOutcome
		ownerPublished bool
	}{
		{"process", sharerUndrained, false},
		{"process", sharerUploadRefused, false},
		{"process", sharerPublishes, true},
		{"oci", sharerUndrained, false},
		{"oci", sharerUploadRefused, false},
		{"oci", sharerPublishes, true},
	} {
		t.Run(tc.kind+"/parent_"+string(tc.parent), func(t *testing.T) {
			h := newRetentionHarness(t, time.Hour)
			runSharer(t, h, tc.kind, sharedHandoffOwner, "attempt-parent", tc.parent)
			h.now = h.now.Add(time.Minute)
			runSharer(t, h, tc.kind, "run_child", "attempt-child", sharerPublishes)
			if upload := requireUploadRecord(t, h.manager, sharedHandoffOwner); upload.AttemptID != "attempt-child" || !upload.publishes() {
				t.Fatalf("the child's own upload = %+v, want a published attempt", upload)
			}
			// Leave the child's admission and its durable upload outcome, as a
			// crash between upload and terminal retention does.
			path := h.manager.recordPath(sharedHandoffOwner)
			if tc.kind == "oci" {
				path = h.manager.ociRecordPath(sharedHandoffOwner)
			}
			payload, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var record map[string]any
			if err := json.Unmarshal(payload, &record); err != nil {
				t.Fatal(err)
			}
			if record["attempt_id"] != "attempt-child" {
				t.Fatalf("the record is not the child's admission: %v", record)
			}
			for _, field := range []string{"retained_at", "retain_until", "published", "uploaded", "succeeded"} {
				delete(record, field)
			}
			if payload, err = json.Marshal(record); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, payload, 0o600); err != nil {
				t.Fatal(err)
			}

			reopened := reopenedHandoffManager(t, h)
			if tc.kind == "process" {
				got := requireRetentionRecord(t, reopened, sharedHandoffOwner)
				if !got.Adopted || !got.Published || got.evidenceReachedLedger() != tc.ownerPublished {
					t.Fatalf("recovered %+v, want the child's verdict restored and the handoff published=%t", got, tc.ownerPublished)
				}
				return
			}
			got, found, err := reopened.readOCIRecord(sharedHandoffOwner)
			if err != nil || !found || !got.Adopted || !got.Published || got.evidenceReachedLedger() != tc.ownerPublished {
				t.Fatalf("recovered %+v found=%t err=%v, want the child's verdict restored and the volume published=%t",
					got, found, err, tc.ownerPublished)
			}
		})
	}
}

// TestAnEarlierRecordThatCannotVouchForItsSharersKeepsTheHandoffUnpublished
// is the upgrade. A record written before this node tracked earlier sharers
// carries one attempt's verdict and nothing about who else wrote into the
// handoff, so it reads as unpublished, and the next attempt to share it cannot
// publish it either. So does an OCI owner this node knows only through an
// upload record. Both err toward keeping data.
func TestAnEarlierRecordThatCannotVouchForItsSharersKeepsTheHandoffUnpublished(t *testing.T) {
	for _, tc := range []struct{ kind, standing string }{
		{"process", "per_attempt_record"},
		{"oci", "per_attempt_record"},
		{"oci", "upload_record_only"},
	} {
		t.Run(tc.kind+"/"+tc.standing, func(t *testing.T) {
			h := newRetentionHarness(t, time.Hour)
			runSharer(t, h, tc.kind, sharedHandoffOwner, "attempt-parent", sharerPublishes)
			path := h.manager.recordPath(sharedHandoffOwner)
			if tc.kind == "oci" {
				path = h.manager.ociRecordPath(sharedHandoffOwner)
			}
			if tc.standing == "upload_record_only" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				h.manager.ociHandoffs = &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
					ociVolume(t, sharedHandoffOwner, 4096, 2, h.now)}}
				if published, unpublished := h.manager.evictionCandidates(h.account()); len(published) != 0 || len(unpublished) != 1 {
					t.Fatalf("a volume known only by its upload record is evict-first: %v, evict-last %v", published, unpublished)
				}
			} else {
				// Keep exactly the members the per-attempt rule wrote.
				payload, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var record map[string]any
				if err := json.Unmarshal(payload, &record); err != nil {
					t.Fatal(err)
				}
				for name := range record {
					switch name {
					case "run_id", "node_id", "directory", "handoff_owner_key", "admitted_at", "retained_at", "retain_until",
						"published", "uploaded", "attempt_id", "succeeded", "owner_key":
					default:
						delete(record, name)
					}
				}
				if record["published"] != true || record["uploaded"] != true {
					t.Fatalf("the fixture's attempt did not publish: %v", record)
				}
				if payload, err = json.Marshal(record); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, payload, 0o600); err != nil {
					t.Fatal(err)
				}
				if sharedHandoffPublished(t, h.manager, tc.kind) {
					t.Fatal("a record that cannot say who else wrote into its handoff reads as published")
				}
			}
			h.now = h.now.Add(time.Minute)
			runSharer(t, h, tc.kind, "run_child", "attempt-child", sharerPublishes)
			if sharedHandoffPublished(t, h.manager, tc.kind) {
				t.Fatal("a sharer admitted over a record that could not vouch for earlier sharers published the handoff")
			}
			if sharedHandoffPublished(t, reopenedHandoffManager(t, h), tc.kind) {
				t.Fatal("restart published a handoff whose earlier sharers nobody can vouch for")
			}
		})
	}
}

// TestAnotherOwnersRecordAtThisOwnersNameIsNotAbsence is the older file-name
// mapping meeting the owner-wide rule. That mapping folded every byte outside
// [A-Za-z0-9_-] to "_", so "run.collide" and "run_collide" were filed under
// one name, and an older agent's record for one of them may have been
// replaced by the other's. Another owner's record at a name this owner's could
// be under says this owner's earlier outcome is unknown, never that there was
// none, so a later sharer's success must not publish this owner's handoff.
//
// The process rows are guards rather than red checks: a process admission
// decides from the directory itself, whose contents are whatever an earlier
// attempt left, so a foreign record at the older name never made it fresh.
// The current-name process case is not reachable at all: writeRecord refuses
// to write over another run's record, so the sharer is never admitted.
func TestAnotherOwnersRecordAtThisOwnersNameIsNotAbsence(t *testing.T) {
	const dotted, underscored = "run.collide", "run_collide"
	if legacyRecordComponent(dotted) != recordComponent(underscored) {
		t.Fatalf("the fixture's owners do not collide: %q, %q", legacyRecordComponent(dotted), recordComponent(underscored))
	}
	for _, tc := range []struct {
		kind, collision string
	}{
		// This owner has no record under its current name, and its older
		// name holds the other owner's current record.
		{"oci", "legacy_name_holds_another_owner"},
		// This owner's current name holds the record an older agent filed
		// there for the other owner.
		{"oci", "current_name_holds_another_owner"},
		{"process", "legacy_name_holds_another_owner"},
	} {
		t.Run(tc.kind+"/"+tc.collision, func(t *testing.T) {
			h := newRetentionHarness(t, time.Hour)
			owner, other := dotted, underscored
			if tc.collision == "current_name_holds_another_owner" {
				owner, other = underscored, dotted
			}
			// This owner's earlier attempt left its only copy on the node.
			parent := sharerUploadRefused
			if tc.kind == "process" {
				parent = sharerUndrained
			}
			runSharerFor(t, h, tc.kind, owner, owner, "attempt-parent", parent)
			if handoffPublishedFor(t, h.manager, tc.kind, owner) {
				t.Fatal("the fixture's earlier attempt published")
			}
			// What the node is left with after an older agent's shared name:
			// nothing under this owner's own current name, and the other
			// owner's record where this owner's could be.
			ownRecord := h.manager.recordPath(owner)
			if tc.kind == "oci" {
				ownRecord = h.manager.ociRecordPath(owner)
			}
			if err := os.Remove(ownRecord); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(h.manager.uploadRecordRoot(), recordComponent(owner))); err != nil {
				t.Fatal(err)
			}
			h.now = h.now.Add(time.Minute)
			if tc.collision == "current_name_holds_another_owner" {
				foreign := h.manager.newUploadRecord(other, "node-1", "attempt-other", attemptResult{document: []byte(`{}`)}, true)
				if err := writeStateDocument(h.manager.stateRoot, uploadRecordDirectoryName, legacyRecordComponent(other), foreign); err != nil {
					t.Fatal(err)
				}
			} else {
				runSharerFor(t, h, tc.kind, other, other, "attempt-other", sharerPublishes)
				if !handoffPublishedFor(t, h.manager, tc.kind, other) {
					t.Fatal("the other owner's own handoff did not publish")
				}
			}

			h.now = h.now.Add(time.Minute)
			runSharerFor(t, h, tc.kind, owner, "run_child", "attempt-child", sharerPublishes)
			if upload := requireUploadRecord(t, h.manager, owner); upload.AttemptID != "attempt-child" || !upload.publishes() {
				t.Fatalf("the child's own upload = %+v, want a published attempt", upload)
			}
			if handoffPublishedFor(t, h.manager, tc.kind, owner) {
				t.Fatalf("another owner's record at %s's name read as no earlier attempt, and a later sharer published its handoff", owner)
			}
			if handoffPublishedFor(t, reopenedHandoffManager(t, h), tc.kind, owner) {
				t.Fatal("restart published a handoff whose earlier attempt's outcome is unknown")
			}
		})
	}
}
