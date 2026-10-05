//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// Exercise completion through the process lifecycle, including a real mailbox
// drain. Its success says nothing about the separate result-document upload.
func TestHandoffPublicationProcess(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mailbox   bool
		wrote     string // "", "json", "not_json" or "directory" at result.json
		status    int
		published bool
		reason    contract.ResultUploadSkipReason
	}{
		// A run that wrote no result.json has no document to lose once L1
		// has recorded that it wrote none.
		{"missing_result", false, "", 0, true, contract.ResultUploadSkipAbsent},
		{"missing_result_after_mailbox_drain", true, "", 0, true, contract.ResultUploadSkipAbsent},
		{"missing_result_refused", false, "", http.StatusConflict, false, contract.ResultUploadSkipTransport},
		// Every other skip names a file that is on this node and nowhere else.
		{"unusable_result", false, "not_json", 0, false, contract.ResultUploadSkipNotJSON},
		{"result_not_a_file", false, "directory", 0, false, contract.ResultUploadSkipNotFile},
		{"failed_without_mailbox", false, "json", http.StatusConflict, false, contract.ResultUploadSkipTransport},
		{"failed_after_mailbox_drain", true, "json", http.StatusConflict, false, contract.ResultUploadSkipTransport},
		{"uploaded_without_mailbox", false, "json", 0, true, ""},
		{"uploaded_after_mailbox_drain", true, "json", 0, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRetentionHarness(t, time.Hour)
			path := filepath.Join(h.root, "run_publication")
			claim := retentionClaim(t, "run_publication", path)
			run := successfulRetentionRun
			switch tc.wrote {
			case "json":
				run = writingRun(path, "result.json", []byte(`{"ok":true}`))
			case "not_json":
				run = writingRun(path, "result.json", []byte("not JSON"))
			case "directory":
				run = func(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
					if err := os.Mkdir(filepath.Join(path, "result.json"), 0o700); err != nil {
						return contract.ProcessResult{}, err
					}
					return successfulRetentionRun(ctx, request, sink)
				}
			}
			lifecycle := uploadingLifecycle(t, h.manager, &resultUploadRecorder{status: tc.status}, run)
			if tc.mailbox {
				claim.SubmittedByRunLedger = true
				claim.Job.Spec.Execution.Env = map[string]string{contract.EnvRunID: "run_publication", contract.EnvL3Endpoint: "http://ledger.invalid"}
				claim.Job.Spec.Execution.SensitiveEnv = map[string]string{contract.EnvRunToken: mailboxTestToken}
				appender := newRecordingAppender(path)
				lifecycle.dependencies.runLedger = appender
				lifecycle.dependencies.runtimes = testRuntimeSet(completionDirectiveRunFunc(func(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
					writeMailboxEvent(t, request.Execution.Env[contract.EnvRunDir], "0001-step-work", "wefty-protocol: 1\nkind: step\nname: work\n--\n")
					return run(ctx, request, sink)
				}))
				t.Cleanup(func() {
					if len(appender.snapshot()) != 1 {
						t.Error("mailbox did not publish its event")
					}
				})
			}
			if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil {
				t.Fatal(err)
			}
			mailbox := lifecycle.mailbox.Load()
			if (mailbox != nil) != tc.mailbox || !lifecycle.mailboxDrained() {
				t.Fatal("mailbox did not drain as arranged")
			}
			if got := requireRetentionRecord(t, h.manager, "run_publication"); got.evidenceReachedLedger() != tc.published {
				t.Fatalf("publication = %+v, want %t", got, tc.published)
			}
			upload := requireUploadRecord(t, h.manager, "run_publication")
			if upload.Uploaded != (tc.published && tc.reason == "") || upload.Reason != tc.reason || !upload.MailboxDrained {
				t.Fatalf("upload fact = %+v", upload)
			}
			// Bytes to charge, so the record is a candidate whatever the run wrote.
			if err := os.WriteFile(filepath.Join(path, "payload.bin"), make([]byte, 4096), 0o600); err != nil {
				t.Fatal(err)
			}
			if published, _ := h.manager.evictionCandidates(h.account()); (len(published) == 1) != tc.published {
				t.Fatalf("evict-first candidates = %v, want published=%t", published, tc.published)
			}
			// Restart must preserve the observed fact, not infer it from success.
			reopened := newHandoffManager(h.root, h.manager.stateRoot, "node-1", time.Hour, nil)
			reopened.now = h.manager.now
			if err := reopened.adoptResidue(); err != nil {
				t.Fatal(err)
			}
			if got := requireRetentionRecord(t, reopened, "run_publication"); got.evidenceReachedLedger() != tc.published {
				t.Fatalf("restart publication = %+v", got)
			}
		})
	}
}

// TestHandoffPublicationNeedsACompleteMailboxDrain is the #494 invariant at the
// publication rule: L1 holding the result says nothing about L3 holding the
// events. When the drain fails, the pending events in the handoff are the only
// copy, so a successful upload must not make the handoff evict-first.
func TestHandoffPublicationNeedsACompleteMailboxDrain(t *testing.T) {
	const event = "wefty-protocol: 1\nkind: gate\nname: test\noutcome: fail\n--\nboom\n"
	t.Run("process", func(t *testing.T) {
		h := newRetentionHarness(t, time.Hour)
		path := filepath.Join(h.root, "run_undrained")
		claim := retentionClaim(t, "run_undrained", path)
		claim.SubmittedByRunLedger = true
		claim.Job.Spec.Execution.Env = map[string]string{contract.EnvRunID: "run_undrained", contract.EnvL3Endpoint: "http://ledger.invalid"}
		claim.Job.Spec.Execution.SensitiveEnv = map[string]string{contract.EnvRunToken: mailboxTestToken}
		recorder := &resultUploadRecorder{}
		run := writingRun(path, "result.json", []byte(`{"ok":true}`))
		lifecycle := uploadingLifecycle(t, h.manager, recorder, run)
		appender := newRecordingAppender(path)
		appender.failWith(errors.New("run ledger is unreachable"))
		lifecycle.dependencies.runLedger = appender
		lifecycle.dependencies.runtimes = testRuntimeSet(completionDirectiveRunFunc(func(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
			writeMailboxEvent(t, request.Execution.Env[contract.EnvRunDir], "0001-gate-test", event)
			return run(ctx, request, sink)
		}))
		if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil {
			t.Fatal(err)
		}
		if requests, _ := recorder.observed(); len(requests) != 1 || len(requests[0].Document) == 0 {
			t.Fatalf("the fixture did not upload the document: %+v", requests)
		}
		if !lifecycle.mailbox.Load().publicationIncomplete() {
			t.Fatal("the fixture's refused events did not leave the drain incomplete")
		}
		if got := requireRetentionRecord(t, h.manager, "run_undrained"); got.Published || got.evidenceReachedLedger() {
			t.Fatalf("an upload hid a failed mailbox drain: %+v", got)
		}
		if upload := requireUploadRecord(t, h.manager, "run_undrained"); !upload.Uploaded || upload.MailboxDrained {
			t.Fatalf("upload record = %+v, want the upload and the failed drain both recorded", upload)
		}
		pending := filepath.Join(path, runMailboxDirectoryName, "run_undrained", runMailboxEventsDirectoryName, "0001-gate-test")
		if _, err := os.Stat(pending); err != nil {
			t.Fatalf("the only copy of the event is gone: %v", err)
		}
		published, unpublished := h.manager.evictionCandidates(h.account())
		if len(published) != 0 || len(unpublished) != 1 {
			t.Fatalf("evict-first candidates = %v, unpublished = %v", published, unpublished)
		}
	})
	t.Run("oci", func(t *testing.T) {
		h := newRetentionHarness(t, time.Hour)
		claim := remoteMailboxClaim("")
		claim.Job.Spec.Labels["run_id"] = "run_oci_undrained"
		claim.Job.Spec.Execution.Env[contract.EnvRunID] = "run_oci_undrained"
		claim.Lease.AttemptID, claim.Lease.FencingToken, claim.Lease.LeaseTTL = "attempt-1", "fence-1", time.Minute
		claim.SubmittedByRunLedger = true
		recorder := &resultUploadRecorder{}
		lifecycle := uploadingLifecycle(t, h.manager, recorder, successfulRetentionRun)
		runtime := &publicationOCIRuntime{fakeRunMailboxRuntime: newFakeRunMailboxRuntime()}
		lifecycle.dependencies.runtimes = workloadRuntimeSet{contract.JobKindOCI: runtime}
		appender := newRecordingAppender("")
		appender.failWith(errors.New("run ledger is unreachable"))
		lifecycle.dependencies.runLedger = appender
		lifecycle.dependencies.mailboxStateRoot = t.TempDir()
		runtime.put("0001-gate-test", event)
		if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil {
			t.Fatal(err)
		}
		if requests, _ := recorder.observed(); len(requests) != 1 || len(requests[0].Document) == 0 {
			t.Fatalf("the fixture did not upload the document: %+v", requests)
		}
		if !lifecycle.mailbox.Load().publicationIncomplete() || len(runtime.remaining()) != 1 {
			t.Fatal("the fixture's refused event did not stay pending in the volume")
		}
		record, found, err := h.manager.readOCIRecord("run_oci_undrained")
		if err != nil || !found || record.Published || record.evidenceReachedLedger() {
			t.Fatalf("an upload hid a failed mailbox drain: %+v found=%t err=%v", record, found, err)
		}
		if upload := requireUploadRecord(t, h.manager, "run_oci_undrained"); !upload.Uploaded || upload.MailboxDrained {
			t.Fatalf("upload record = %+v, want the upload and the failed drain both recorded", upload)
		}
		h.manager.ociHandoffs = &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
			ociVolume(t, "run_oci_undrained", 4096, 2, h.now)}}
		published, unpublished := h.manager.evictionCandidates(h.account())
		if len(published) != 0 || len(unpublished) != 1 {
			t.Fatalf("evict-first candidates = %v, unpublished = %v", published, unpublished)
		}
	})
}

// A drain counts only once finalization has observed it. Until then the
// pending events may be the only copy, whatever the incomplete flag says.
func TestHandoffPublicationDrainIsObservedNotAssumed(t *testing.T) {
	appender := newRecordingAppender("")
	mailbox, _, _ := newTestMailbox(t, appender, "")
	writeMailboxEvent(t, mailbox.directory, "0001-step-work", "wefty-protocol: 1\nkind: step\nname: work\n--\n")
	lifecycle := &attemptLifecycle{}
	if !lifecycle.mailboxDrained() {
		t.Fatal("an attempt with no mailbox has something to drain")
	}
	lifecycle.storeMailbox(mailbox)
	if mailbox.publicationIncomplete() || lifecycle.mailboxDrained() {
		t.Fatal("an unfinalized mailbox counted as drained")
	}
	mailbox.finalize(t.Context())
	if len(appender.snapshot()) != 1 || !lifecycle.mailboxDrained() {
		t.Fatal("a finalized mailbox that published everything did not count as drained")
	}
}

func TestHandoffPublicationOCI(t *testing.T) {
	for _, tc := range []struct {
		name      string
		captured  bool
		absent    bool
		status    int
		published bool
		reason    contract.ResultUploadSkipReason
	}{
		// Result capture and publication do not depend on a run mailbox.
		{"without_mailbox", false, false, 0, true, ""},
		{"without_mailbox_failed_upload", false, false, http.StatusConflict, false, contract.ResultUploadSkipTransport},
		{"without_mailbox_absent", false, true, 0, true, contract.ResultUploadSkipAbsent},
		{"without_mailbox_not_json", false, false, 0, false, contract.ResultUploadSkipNotJSON},
		{"without_mailbox_not_file", false, false, 0, false, contract.ResultUploadSkipNotFile},
		{"without_mailbox_oversize", false, false, 0, false, contract.ResultUploadSkipOversize},
		{"without_mailbox_unreadable", false, false, 0, false, contract.ResultUploadSkipUnreadable},
		{"failed_after_mailbox_drain", true, false, http.StatusConflict, false, contract.ResultUploadSkipTransport},
		{"uploaded", true, false, 0, true, ""},
		{"absent_result_uploaded", true, true, 0, true, contract.ResultUploadSkipAbsent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRetentionHarness(t, time.Hour)
			claim := remoteMailboxClaim("")
			claim.Job.Spec.Labels["run_id"] = "run_oci_publication"
			claim.Job.Spec.Execution.Env[contract.EnvRunID] = "run_oci_publication"
			claim.Lease.AttemptID, claim.Lease.FencingToken, claim.Lease.LeaseTTL = "attempt-1", "fence-1", time.Minute
			claim.SubmittedByRunLedger = tc.captured
			if !tc.captured {
				claim.Job.Spec.Execution.Env = nil
				claim.Job.Spec.Execution.SensitiveEnv = nil
				claim.Job.Spec.Labels["handoff_owner_run_id"] = "run_oci_publication"
				delete(claim.Job.Spec.Labels, "run_id")
			}
			lifecycle := uploadingLifecycle(t, h.manager, &resultUploadRecorder{status: tc.status}, successfulRetentionRun)
			runtime := &publicationOCIRuntime{fakeRunMailboxRuntime: newFakeRunMailboxRuntime(), absent: tc.absent}
			switch tc.name {
			case "without_mailbox_not_json":
				runtime.document = []byte("invalid JSON")
			case "without_mailbox_not_file":
				runtime.readErr = workloadrunner.ErrRunMailboxEntryUnusable
			case "without_mailbox_oversize":
				runtime.document = sizedJSONDocument(maxHandoffFileReadBytes)
				runtime.truncated = true
			case "without_mailbox_unreadable":
				runtime.readErr = errors.New("attempt authority unavailable")
			}
			lifecycle.dependencies.runtimes = workloadRuntimeSet{contract.JobKindOCI: runtime}
			if tc.captured {
				appender := newRecordingAppender("")
				lifecycle.dependencies.runLedger = appender
				lifecycle.dependencies.mailboxStateRoot = t.TempDir()
				runtime.put("0001-step-work", "wefty-protocol: 1\nkind: step\nname: work\n--\n")
				t.Cleanup(func() {
					if len(appender.snapshot()) != 1 {
						t.Error("remote mailbox did not drain its event")
					}
				})
			}
			if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil {
				t.Fatal(err)
			}
			if !runtime.resultRead.Load() || !runtime.reaped.Load() {
				t.Fatal("OCI result was not captured before reap")
			}
			record, found, err := h.manager.readOCIRecord("run_oci_publication")
			if err != nil || !found || record.evidenceReachedLedger() != tc.published {
				t.Fatalf("OCI publication = %+v, found=%t err=%v", record, found, err)
			}
			upload, found, err := h.manager.readUploadRecord("run_oci_publication")
			if err != nil || !found {
				t.Fatalf("capture/upload availability = %+v found=%t err=%v", upload, found, err)
			}
			if upload.Uploaded != (tc.published && tc.reason == "") || upload.Reason != tc.reason || !upload.MailboxDrained {
				t.Fatalf("upload fact = %+v", upload)
			}
			h.manager.ociHandoffs = &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
				ociVolume(t, "run_oci_publication", 4096, 2, h.now)}}
			if published, _ := h.manager.evictionCandidates(h.account()); (len(published) == 1) != tc.published {
				t.Fatalf("evict-first candidates = %v, want published=%t", published, tc.published)
			}
			reopened := newHandoffManager(h.root, h.manager.stateRoot, "node-1", time.Hour, nil)
			reopened.now = h.manager.now
			if err := reopened.adoptResidue(); err != nil {
				t.Fatal(err)
			}
			record, _, _ = reopened.readOCIRecord("run_oci_publication")
			if record.evidenceReachedLedger() != tc.published {
				t.Fatalf("restart publication = %+v", record)
			}
		})
	}
}

func TestHandoffPublicationRerunAndEviction(t *testing.T) {
	for _, kind := range []string{"process", "oci"} {
		t.Run(kind, func(t *testing.T) {
			h := newRetentionHarness(t, 7*24*time.Hour)
			finish := func(attempt string, status int) {
				lifecycle := uploadingLifecycle(t, h.manager, &resultUploadRecorder{status: status}, successfulRetentionRun)
				claim := ociHandoffClaim("run_owner", attempt)
				if kind == "oci" {
					lease := h.admitOCI("run_owner", attempt)
					defer lease.release()
					result := attemptResult{document: []byte(`{"ok":true}`)}
					lifecycle.capturedResult.Store(&result)
					zero := 0
					if _, err := lifecycle.finishCompletedAttempt(t.Context(), claim, contract.ProcessResult{ExitCode: &zero}, nil); err != nil {
						t.Fatal(err)
					}
				} else {
					path := filepath.Join(h.root, "run_owner")
					claim = retentionClaim(t, "run_rerun", path)
					claim.Job.Spec.Labels["handoff_owner_run_id"] = "run_owner"
					claim.Lease.AttemptID = attempt
					claim.Job.Spec.RoutingTags = []string{"wefty:node:node-1"}
					lifecycle.dependencies.runtimes = testRuntimeSet(writingRun(path, "result.json", []byte(`{"ok":true}`)))
					if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil {
						t.Fatal(err)
					}
				}
			}
			finish("attempt-1", 0)
			finish("attempt-2", http.StatusConflict)
			// The earlier successful attempt cannot publish its rerun's bytes.
			if got := requireUploadRecord(t, h.manager, "run_owner"); got.AttemptID != "attempt-2" || got.Uploaded {
				t.Fatalf("rerun upload = %+v", got)
			}
			h.now = h.now.Add(time.Hour)
			if kind == "process" {
				h.retain("run_other", true, true, map[string]int{"payload.bin": 2 << 20})
				if err := os.WriteFile(filepath.Join(h.root, "run_owner", "payload.bin"), make([]byte, 2<<20), 0600); err != nil {
					t.Fatal(err)
				}
				h.budget(3 << 20)
				if !h.exists("run_owner") || h.exists("run_other") {
					t.Fatal("eviction did not prefer the uploaded result over the failed rerun")
				}
			} else {
				h.retainOCI("run_other", "attempt-other", true)
				helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{ociVolume(t, "run_owner", 2<<20, 2, h.now.Add(-time.Hour)), ociVolume(t, "run_other", 2<<20, 2, h.now)}}
				h.manager.ociHandoffs, h.manager.ociEvictor = helper, helper
				h.budget(3 << 20)
				if len(helper.evicted) != 1 || helper.evicted[0] != "run_other" {
					t.Fatalf("evicted = %v", helper.evicted)
				}
			}
		})
	}
}

func TestHandoffPublicationCrashAfterUpload(t *testing.T) {
	for _, tc := range []struct {
		kind, upload string
		published    bool
	}{
		{"process", "current", true}, {"process", "older", false}, {"process", "failed", false}, {"process", "missing", false},
		{"process", "undrained", false}, {"process", "absent", true}, {"process", "legacy", false},
		{"oci", "current", true}, {"oci", "older", false}, {"oci", "failed", false}, {"oci", "missing", false},
		{"oci", "undrained", false}, {"oci", "absent", true}, {"oci", "legacy", false},
	} {
		t.Run(tc.kind+"/"+tc.upload, func(t *testing.T) {
			kind := tc.kind
			h := newRetentionHarness(t, time.Hour)
			claim := retentionClaim(t, "run_crash", filepath.Join(h.root, "run_crash"))
			lifecycle := uploadingLifecycle(t, h.manager, &resultUploadRecorder{}, writingRun(claim.Job.Spec.Execution.HandoffDirectory, "result.json", []byte(`{}`)))
			if kind == "process" {
				if _, err := lifecycle.execute(t.Context(), claim, time.Now()); err != nil {
					t.Fatal(err)
				}
			} else {
				claim = ociHandoffClaim("run_crash", "attempt-1")
				lease := h.admitOCI("run_crash", "attempt-1")
				result := attemptResult{document: []byte(`{}`)}
				lifecycle.capturedResult.Store(&result)
				lifecycle.uploadResult(t.Context(), claim)
				lease.release()
			}
			// Leave only the admission and the durable upload outcome, as a
			// crash between upload and terminal retention does. JSON keeps this
			// regression compilable against the original record schema too.
			path := h.manager.recordPath("run_crash")
			if kind == "oci" {
				path = h.manager.ociRecordPath("run_crash")
			}
			payload, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var record map[string]any
			if err := json.Unmarshal(payload, &record); err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"retained_at", "retain_until", "published", "uploaded", "succeeded"} {
				delete(record, field)
			}
			payload, _ = json.Marshal(record)
			if err := os.WriteFile(path, payload, 0600); err != nil {
				t.Fatal(err)
			}
			switch tc.upload {
			case "older":
				if err := h.manager.recordUpload("run_crash", "node-1", "attempt-older", attemptResult{document: []byte(`{}`)}, true); err != nil {
					t.Fatal(err)
				}
			case "failed":
				if err := h.manager.recordUpload("run_crash", "node-1", claim.Lease.AttemptID, attemptResult{skip: contract.ResultUploadSkipTransport}, true); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err := os.Remove(filepath.Join(h.manager.uploadRecordRoot(), recordComponent("run_crash"))); err != nil {
					t.Fatal(err)
				}
			case "undrained":
				// The document reached L1 and the mailbox did not drain: the
				// events still pending in the handoff are the only copy.
				if err := h.manager.recordUpload("run_crash", "node-1", claim.Lease.AttemptID, attemptResult{document: []byte(`{}`)}, false); err != nil {
					t.Fatal(err)
				}
			case "absent":
				if err := h.manager.recordUpload("run_crash", "node-1", claim.Lease.AttemptID, attemptResult{skip: contract.ResultUploadSkipAbsent}, true); err != nil {
					t.Fatal(err)
				}
			case "legacy":
				// An older agent's upload record says nothing about the drain.
				upload := h.manager.newUploadRecord("run_crash", "node-1", claim.Lease.AttemptID, attemptResult{document: []byte(`{}`)}, true)
				payload, err := json.Marshal(upload)
				if err != nil {
					t.Fatal(err)
				}
				var legacy map[string]any
				if err := json.Unmarshal(payload, &legacy); err != nil {
					t.Fatal(err)
				}
				delete(legacy, "mailbox_drained")
				if payload, err = json.Marshal(legacy); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(h.manager.uploadRecordRoot(), recordComponent("run_crash")), payload, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			reopened := newHandoffManager(h.root, h.manager.stateRoot, "node-1", time.Hour, nil)
			reopened.now = h.manager.now
			if err := reopened.adoptResidue(); err != nil {
				t.Fatal(err)
			}
			if kind == "process" {
				if got := requireRetentionRecord(t, reopened, "run_crash"); got.Published != tc.published || !got.Adopted {
					t.Fatalf("upload was lost in crash recovery: %+v", got)
				}
			} else {
				got, _, _ := reopened.readOCIRecord("run_crash")
				if got.evidenceReachedLedger() != tc.published || !got.Adopted {
					t.Fatalf("upload was lost in crash recovery: %+v", got)
				}
			}
		})
	}
}

// Old agents stored the mailbox verdict in published. That is not proof that
// the retained document has another copy, even after restarting the node. An
// old OCI record also carried the document upload in uploaded, and a document
// that reached L1 from an attempt whose mailbox did not drain is not
// published either.
func TestHandoffPublicationLegacyMailboxVerdict(t *testing.T) {
	for _, tc := range []struct {
		name     string
		kind     string
		drained  bool
		uploaded any // nil: an older process record has no uploaded member
	}{
		{"process_drained", "process", true, nil},
		{"oci_drained_not_uploaded", "oci", true, nil},
		{"oci_uploaded_not_drained", "oci", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRetentionHarness(t, time.Hour)
			path := h.manager.recordPath("run_legacy")
			if tc.kind == "process" {
				h.retain("run_legacy", true, true, map[string]int{"result.json": 32})
			} else {
				h.retainOCI("run_legacy", "attempt-1", true)
				path = h.manager.ociRecordPath("run_legacy")
			}
			payload, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var record map[string]any
			if err := json.Unmarshal(payload, &record); err != nil {
				t.Fatal(err)
			}
			record["published"] = tc.drained
			delete(record, "uploaded")
			if tc.uploaded != nil {
				record["uploaded"] = tc.uploaded
			}
			payload, _ = json.Marshal(record)
			if err := os.WriteFile(path, payload, 0600); err != nil {
				t.Fatal(err)
			}
			if tc.kind == "oci" {
				helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{ociVolume(t, "run_legacy", 4096, 2, h.now)}}
				h.manager.ociHandoffs = helper
			}
			published, unpublished := h.manager.evictionCandidates(h.account())
			if len(published) != 0 || len(unpublished) != 1 {
				t.Fatalf("legacy record granted publication: published=%v unpublished=%v", published, unpublished)
			}
		})
	}
}

// The OCI storage reader rejects post-reap reads, matching the helper's live
// attempt boundary. It keeps mailbox events separate from the handoff root.
type publicationOCIRuntime struct {
	captureRuntime
	*fakeRunMailboxRuntime
	reaped, resultRead atomic.Bool
	// absent makes the volume hold no result.json at all.
	absent    bool
	document  []byte
	truncated bool
	readErr   error
}

func (r *publicationOCIRuntime) ReadRunMailbox(ctx context.Context, ref workloadrunner.RunMailboxReference, name string, limit int) ([]byte, bool, error) {
	if r.reaped.Load() {
		return nil, false, errors.New("attempt already reaped")
	}
	if ref.Scope == workloadrunner.RunMailboxScopeHandoffFiles {
		r.resultRead.Store(true)
		if r.absent {
			return nil, false, fs.ErrNotExist
		}
		if r.readErr != nil {
			return nil, false, r.readErr
		}
		if r.document != nil {
			return r.document, r.truncated, nil
		}
		return []byte(`{"ok":true}`), false, nil
	}
	return r.fakeRunMailboxRuntime.ReadRunMailbox(ctx, ref, name, limit)
}
func (r *publicationOCIRuntime) ReadHandoffFile(ctx context.Context, ref workloadrunner.HandoffFileReference, name string, limit int) ([]byte, bool, error) {
	if ref.Authority != r.request.Authority || ref.OwnerKey != handoffOwnerRunIDFromRequest(r.request) {
		return nil, false, errors.New("handoff reader did not carry admitted attempt and owner")
	}
	return r.ReadRunMailbox(ctx, workloadrunner.RunMailboxReference{Authority: ref.Authority, OwnerKey: ref.OwnerKey, Scope: workloadrunner.RunMailboxScopeHandoffFiles}, name, limit)
}

func handoffOwnerRunIDFromRequest(request workloadrunner.Request) string {
	for _, volume := range request.ManagedVolumes {
		if volume.Kind == workloadrunner.ManagedVolumeHandoff {
			return volume.OwnerKey
		}
	}
	return ""
}

func (r *publicationOCIRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	r.reaped.Store(true)
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
}
