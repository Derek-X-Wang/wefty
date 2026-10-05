//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
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
		name              string
		mailbox, document bool
		status            int
		published         bool
		reason            contract.ResultUploadSkipReason
	}{
		{"missing_result", false, false, 0, false, contract.ResultUploadSkipAbsent},
		{"unusable_result", false, true, 0, false, contract.ResultUploadSkipNotJSON},
		{"failed_without_mailbox", false, true, http.StatusConflict, false, contract.ResultUploadSkipTransport},
		{"failed_after_mailbox_drain", true, true, http.StatusConflict, false, contract.ResultUploadSkipTransport},
		{"uploaded_without_mailbox", false, true, 0, true, ""},
		{"uploaded_after_mailbox_drain", true, true, 0, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRetentionHarness(t, time.Hour)
			path := filepath.Join(h.root, "run_publication")
			claim := retentionClaim(t, "run_publication", path)
			run := successfulRetentionRun
			if tc.document {
				document := []byte(`{"ok":true}`)
				if tc.reason == contract.ResultUploadSkipNotJSON {
					document = []byte("not JSON")
				}
				run = writingRun(path, "result.json", document)
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
			if (mailbox != nil) != tc.mailbox || mailbox.publicationIncomplete() {
				t.Fatal("mailbox did not drain as arranged")
			}
			if got := requireRetentionRecord(t, h.manager, "run_publication"); got.Published != tc.published {
				t.Fatalf("publication = %t, want %t", got.Published, tc.published)
			}
			upload := requireUploadRecord(t, h.manager, "run_publication")
			if upload.Uploaded != tc.published || upload.Reason != tc.reason {
				t.Fatalf("upload fact = %+v", upload)
			}
			// Restart must preserve the observed fact, not infer it from success.
			reopened := newHandoffManager(h.root, h.manager.stateRoot, "node-1", time.Hour, nil)
			reopened.now = h.manager.now
			if err := reopened.adoptResidue(); err != nil {
				t.Fatal(err)
			}
			if got := requireRetentionRecord(t, reopened, "run_publication"); got.Published != tc.published {
				t.Fatalf("restart publication = %+v", got)
			}
		})
	}
}

func TestHandoffPublicationOCI(t *testing.T) {
	for _, tc := range []struct {
		name      string
		captured  bool
		status    int
		published bool
	}{
		{"missing_mailbox", false, 0, false},
		{"failed_after_mailbox_drain", true, http.StatusConflict, false},
		{"uploaded", true, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRetentionHarness(t, time.Hour)
			claim := remoteMailboxClaim("")
			claim.Job.Spec.Labels["run_id"] = "run_oci_publication"
			claim.Job.Spec.Execution.Env[contract.EnvRunID] = "run_oci_publication"
			claim.Lease.AttemptID, claim.Lease.FencingToken, claim.Lease.LeaseTTL = "attempt-1", "fence-1", time.Minute
			claim.SubmittedByRunLedger = tc.captured
			lifecycle := uploadingLifecycle(t, h.manager, &resultUploadRecorder{status: tc.status}, successfulRetentionRun)
			runtime := &publicationOCIRuntime{fakeRunMailboxRuntime: newFakeRunMailboxRuntime()}
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
			if runtime.resultRead.Load() != tc.captured || !runtime.reaped.Load() {
				t.Fatal("OCI result capture/reap order did not match mailbox availability")
			}
			record, found, err := h.manager.readOCIRecord("run_oci_publication")
			if err != nil || !found || record.evidenceReachedLedger() != tc.published {
				t.Fatalf("OCI publication = %+v, found=%t err=%v", record, found, err)
			}
			upload, found, err := h.manager.readUploadRecord("run_oci_publication")
			if err != nil || found != tc.captured {
				t.Fatalf("capture/upload availability = %+v found=%t err=%v", upload, found, err)
			}
			if tc.captured && (upload.Uploaded != tc.published || (!tc.published && upload.Reason != contract.ResultUploadSkipTransport)) {
				t.Fatalf("upload fact = %+v", upload)
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
		{"oci", "current", true}, {"oci", "older", false}, {"oci", "failed", false}, {"oci", "missing", false},
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
				if err := h.manager.recordUpload("run_crash", "node-1", "attempt-older", attemptResult{document: []byte(`{}`)}); err != nil {
					t.Fatal(err)
				}
			case "failed":
				if err := h.manager.recordUpload("run_crash", "node-1", claim.Lease.AttemptID, attemptResult{skip: contract.ResultUploadSkipTransport}); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err := os.Remove(filepath.Join(h.manager.uploadRecordRoot(), recordComponent("run_crash"))); err != nil {
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
// the retained document has another copy, even after restarting the node.
func TestHandoffPublicationLegacyMailboxVerdict(t *testing.T) {
	for _, kind := range []string{"process", "oci"} {
		t.Run(kind, func(t *testing.T) {
			h := newRetentionHarness(t, time.Hour)
			path := h.manager.recordPath("run_legacy")
			if kind == "process" {
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
			delete(record, "uploaded")
			payload, _ = json.Marshal(record)
			if err := os.WriteFile(path, payload, 0600); err != nil {
				t.Fatal(err)
			}
			if kind == "oci" {
				helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{ociVolume(t, "run_legacy", 4096, 2, h.now)}}
				h.manager.ociHandoffs = helper
			}
			published, unpublished := h.manager.evictionCandidates(h.account())
			if len(published) != 0 || len(unpublished) != 1 {
				t.Fatalf("legacy mailbox verdict granted publication: published=%v unpublished=%v", published, unpublished)
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
}

func (r *publicationOCIRuntime) ReadRunMailbox(ctx context.Context, ref workloadrunner.RunMailboxReference, name string, limit int) ([]byte, bool, error) {
	if r.reaped.Load() {
		return nil, false, errors.New("attempt already reaped")
	}
	if ref.Scope == workloadrunner.RunMailboxScopeHandoffFiles {
		r.resultRead.Store(true)
		return []byte(`{"ok":true}`), false, nil
	}
	return r.fakeRunMailboxRuntime.ReadRunMailbox(ctx, ref, name, limit)
}
func (r *publicationOCIRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	r.reaped.Store(true)
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
}
