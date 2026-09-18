//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
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

// resultUploadRecorder is the ledger's side of the upload: what arrived, how
// often, and on which route.
type resultUploadRecorder struct {
	mu       sync.Mutex
	requests []l1.AttemptResultRequest
	paths    []string
	status   int
}

func (recorder *resultUploadRecorder) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !strings.HasSuffix(r.URL.Path, "/result") {
			_ = json.NewEncoder(w).Encode(l1.Job{})
			return
		}
		var request l1.AttemptResultRequest
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &request)
		recorder.mu.Lock()
		recorder.requests = append(recorder.requests, request)
		recorder.paths = append(recorder.paths, r.URL.Path)
		status := recorder.status
		recorder.mu.Unlock()
		if status != 0 && status != http.StatusOK {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
				Code: contract.ErrorLeaseExpired, Message: "attempt no longer owns its result"}})
			return
		}
		_ = json.NewEncoder(w).Encode(l1.AttemptResultResponse{Bytes: len(request.Document), UploadedAt: time.Now().UTC()})
	})
}

func (recorder *resultUploadRecorder) observed() ([]l1.AttemptResultRequest, []string) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]l1.AttemptResultRequest(nil), recorder.requests...), append([]string(nil), recorder.paths...)
}

func uploadingLifecycle(t *testing.T, manager *handoffManager, recorder *resultUploadRecorder,
	run completionDirectiveRunFunc) *attemptLifecycle {
	t.Helper()
	client, stop := startEvidenceReplayServer(t, recorder.handler(), time.Second)
	t.Cleanup(stop)
	t.Cleanup(client.Close)
	return newAttemptLifecycle(attemptLifecycleDependencies{
		client: client, handoffs: manager, runtimes: testRuntimeSet(run),
		nodeID: "node-1", bootSessionID: "upload-boot", clock: systemClock{},
		observer: newLifecycleObserver(systemClock{}), renewalInterval: 10 * time.Second,
		completionRetry: time.Millisecond,
	})
}

func writingRun(path, name string, payload []byte) completionDirectiveRunFunc {
	return func(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
		if err := os.WriteFile(filepath.Join(path, name), payload, 0o600); err != nil {
			return contract.ProcessResult{}, err
		}
		return successfulRetentionRun(ctx, request, sink)
	}
}

// TestASucceedingRunUploadsItsResultExactlyOnce is the whole feature in one
// test: the document the workload wrote reaches the ledger, unaltered, on the
// attempt's own route, and one completion produces one upload.
func TestASucceedingRunUploadsItsResultExactlyOnce(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	recorder := &resultUploadRecorder{}
	path := filepath.Join(harness.root, "run_uploaded")
	document := []byte(`{"passed":true,"gates":[]}`)
	lifecycle := uploadingLifecycle(t, harness.manager, recorder, writingRun(path, "result.json", document))
	if _, err := lifecycle.execute(t.Context(), retentionClaim(t, "run_uploaded", path), time.Now()); err != nil {
		t.Fatal(err)
	}
	requests, paths := recorder.observed()
	if len(requests) != 1 {
		t.Fatalf("uploaded %d times, want exactly one", len(requests))
	}
	if string(requests[0].Document) != string(document) {
		t.Fatalf("uploaded document = %q, want %q", requests[0].Document, document)
	}
	if requests[0].SkipReason != "" {
		t.Fatalf("uploaded a skip reason alongside a document: %q", requests[0].SkipReason)
	}
	if requests[0].FencingToken != "retention-fence" {
		t.Fatalf("uploaded without the attempt's fence: %#v", requests[0])
	}
	if requests[0].SHA256 == "" {
		t.Fatal("uploaded a document with no digest")
	}
	if !strings.HasSuffix(paths[0], "/attempts/"+retentionClaim(t, "run_uploaded", path).Lease.AttemptID+"/result") {
		t.Fatalf("uploaded to %q", paths[0])
	}
	// The file is copied, never moved: the node keeps its own copy for the
	// retention window whether or not the upload happened.
	if _, err := os.Stat(filepath.Join(path, "result.json")); err != nil {
		t.Fatalf("upload consumed the node's own copy: %v", err)
	}
	record := requireRetentionRecord(t, harness.manager, "run_uploaded")
	if !record.Uploaded || record.UploadSkipReason != "" {
		t.Fatalf("retention record does not record the upload: %#v", record)
	}
}

// TestARunThatWroteNoResultUploadsNothing keeps the common case free. Absence is
// what an empty answer already means, so it costs no row and no request.
func TestARunThatWroteNoResultUploadsNothing(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	recorder := &resultUploadRecorder{}
	path := filepath.Join(harness.root, "run_silent")
	lifecycle := uploadingLifecycle(t, harness.manager, recorder, successfulRetentionRun)
	if _, err := lifecycle.execute(t.Context(), retentionClaim(t, "run_silent", path), time.Now()); err != nil {
		t.Fatal(err)
	}
	if requests, _ := recorder.observed(); len(requests) != 0 {
		t.Fatalf("uploaded %d requests for a run with no result: %#v", len(requests), requests)
	}
	record := requireRetentionRecord(t, harness.manager, "run_silent")
	if record.Uploaded || record.UploadSkipReason != contract.ResultUploadSkipAbsent {
		t.Fatalf("retention record = %#v", record)
	}
}

// TestAResultTooLargeToUploadSaysSoInsteadOfTravellingTruncated is the bound
// doing its job. A truncated result document still parses as a result, so half
// of one is worse than none, and the reader is told where the whole one is.
func TestAResultTooLargeToUploadSaysSoInsteadOfTravellingTruncated(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	recorder := &resultUploadRecorder{}
	path := filepath.Join(harness.root, "run_oversize")
	oversize := append([]byte(`{"big":"`), make([]byte, contract.MaxUploadedResultBytes)...)
	lifecycle := uploadingLifecycle(t, harness.manager, recorder, writingRun(path, "result.json", oversize))
	if _, err := lifecycle.execute(t.Context(), retentionClaim(t, "run_oversize", path), time.Now()); err != nil {
		t.Fatal(err)
	}
	requests, _ := recorder.observed()
	if len(requests) != 1 {
		t.Fatalf("uploaded %d requests, want one skip receipt", len(requests))
	}
	if len(requests[0].Document) != 0 {
		t.Fatalf("uploaded %d bytes of an oversize document", len(requests[0].Document))
	}
	if requests[0].SkipReason != contract.ResultUploadSkipOversize {
		t.Fatalf("skip reason = %q", requests[0].SkipReason)
	}
	// The per-run bound never trims result.json, so the whole document is
	// still on the node where the reader was told to look.
	info, err := os.Stat(filepath.Join(path, "result.json"))
	if err != nil || info.Size() != int64(len(oversize)) {
		t.Fatalf("node did not keep the oversize result: %v", err)
	}
}

// TestAnUploadTheLedgerRefusesIsRecordedAsOneAndDoesNotFailTheRun keeps the
// ledger out of the run's verdict. A result that did not travel is a fact about
// the result, not a reason to fail a job that succeeded.
func TestAnUploadTheLedgerRefusesIsRecordedAsOneAndDoesNotFailTheRun(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	recorder := &resultUploadRecorder{status: http.StatusConflict}
	path := filepath.Join(harness.root, "run_refused")
	lifecycle := uploadingLifecycle(t, harness.manager, recorder, writingRun(path, "result.json", []byte(`{"ok":true}`)))
	if _, err := lifecycle.execute(t.Context(), retentionClaim(t, "run_refused", path), time.Now()); err != nil {
		t.Fatalf("a refused upload failed the attempt: %v", err)
	}
	record := requireRetentionRecord(t, harness.manager, "run_refused")
	if record.Uploaded || record.UploadSkipReason != contract.ResultUploadSkipTransport {
		t.Fatalf("retention record = %#v", record)
	}
}

// TestReadingAResultRequiresThisAttemptsOwnReceipt is the same rule the
// retention path enforces: a handle is not authority, the receipt is.
func TestReadingAResultRequiresThisAttemptsOwnReceipt(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path := filepath.Join(harness.root, "run_receipt")
	spec := handoffClaim("run_receipt", path, nil).Job.Spec
	owner := prepareHandoffForTest(t, harness.manager, spec)
	if err := os.WriteFile(filepath.Join(path, "result.json"), []byte(`{"v":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if result := harness.manager.readResult(owner, spec, "node-1"); string(result.document) != `{"v":1}` {
		t.Fatalf("the owner could not read its own result: %#v", result)
	}
	if result := harness.manager.readResult(owner, spec, "node-2"); result.skip != contract.ResultUploadSkipAbsent {
		t.Fatalf("another node read through this receipt: %#v", result)
	}
	if result := harness.manager.readResult(nil, spec, "node-1"); result.skip != contract.ResultUploadSkipAbsent {
		t.Fatalf("a caller with no receipt read a result: %#v", result)
	}
	owner.lease.release()
	if result := harness.manager.readResult(owner, spec, "node-1"); result.skip != contract.ResultUploadSkipAbsent {
		t.Fatalf("a released receipt still read a result: %#v", result)
	}
}

// TestWhatCountsAsAResultDocument pins every way the read can refuse, because
// each one is a different sentence a person reads.
func TestWhatCountsAsAResultDocument(t *testing.T) {
	directory := t.TempDir()
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })

	if result := readHandoffResult(root); result.skip != contract.ResultUploadSkipAbsent {
		t.Fatalf("missing result = %#v", result)
	}

	if err := os.Mkdir(filepath.Join(directory, "result.json"), 0o700); err != nil {
		t.Fatal(err)
	}
	if result := readHandoffResult(root); result.skip != contract.ResultUploadSkipNotFile {
		t.Fatalf("directory named result.json = %#v", result)
	}
	if err := os.Remove(filepath.Join(directory, "result.json")); err != nil {
		t.Fatal(err)
	}

	// A symlink is refused as an object, not followed. The agent runs as the
	// same OS user as a process workload, so a link is the workload pointing
	// the read somewhere it did not write.
	if err := os.Symlink(filepath.Join(directory, "elsewhere.json"), filepath.Join(directory, "result.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "elsewhere.json"), []byte(`{"stolen":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if result := readHandoffResult(root); result.skip != contract.ResultUploadSkipNotFile {
		t.Fatalf("symlinked result = %#v", result)
	}
	if err := os.Remove(filepath.Join(directory, "result.json")); err != nil {
		t.Fatal(err)
	}

	if err := syscall.Mkfifo(filepath.Join(directory, "result.json"), 0o600); err != nil {
		t.Skipf("mkfifo is unavailable: %v", err)
	}
	if result := readHandoffResult(root); result.skip != contract.ResultUploadSkipNotFile {
		t.Fatalf("FIFO named result.json = %#v", result)
	}
	if err := os.Remove(filepath.Join(directory, "result.json")); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(directory, "result.json"), []byte("not json at all"), 0o600); err != nil {
		t.Fatal(err)
	}
	if result := readHandoffResult(root); result.skip != contract.ResultUploadSkipNotJSON {
		t.Fatalf("non-JSON result = %#v", result)
	}

	if err := os.WriteFile(filepath.Join(directory, "result.json"), []byte(`{"ok":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if result := readHandoffResult(root); string(result.document) != `{"ok":1}` || result.skip != "" {
		t.Fatalf("valid result = %#v", result)
	}
}

// stubHandoffFileReader stands in for the helper on the OCI read path.
type stubHandoffFileReader struct {
	payload   []byte
	truncated bool
	err       error
	name      string
}

func (reader *stubHandoffFileReader) readHandoffFile(_ context.Context, name string, _ int64) ([]byte, bool, error) {
	reader.name = name
	return reader.payload, reader.truncated, reader.err
}

// TestTheHelperReadPathClassifiesTheSameWayTheLocalOneDoes keeps the two kinds
// answering with the same vocabulary, and keeps a transport failure from ever
// reading as a verdict about the document.
func TestTheHelperReadPathClassifiesTheSameWayTheLocalOneDoes(t *testing.T) {
	reader := &stubHandoffFileReader{payload: []byte(`{"ok":1}`)}
	if result := readRemoteHandoffResult(t.Context(), reader); string(result.document) != `{"ok":1}` {
		t.Fatalf("valid result = %#v", result)
	}
	if reader.name != "result.json" {
		t.Fatalf("read %q, want result.json", reader.name)
	}
	if result := readRemoteHandoffResult(t.Context(), &stubHandoffFileReader{
		payload: make([]byte, 16), truncated: true}); result.skip != contract.ResultUploadSkipOversize {
		t.Fatalf("truncated result = %#v", result)
	}
	if result := readRemoteHandoffResult(t.Context(), &stubHandoffFileReader{
		err: errRunMailboxEntryUnusable}); result.skip != contract.ResultUploadSkipNotFile {
		t.Fatalf("unusable result = %#v", result)
	}
	if result := readRemoteHandoffResult(t.Context(), &stubHandoffFileReader{
		err: errors.New("helper session lost")}); result.skip != contract.ResultUploadSkipUnreadable {
		t.Fatalf("unreachable helper = %#v", result)
	}
	if result := readRemoteHandoffResult(t.Context(), &stubHandoffFileReader{
		err: fs.ErrNotExist}); result.skip != contract.ResultUploadSkipAbsent {
		t.Fatalf("absent result = %#v", result)
	}
	if result := readRemoteHandoffResult(t.Context(), nil); result.skip != contract.ResultUploadSkipAbsent {
		t.Fatalf("no reader = %#v", result)
	}
}
