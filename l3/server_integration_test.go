package l3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

type integrationHarness struct {
	t          *testing.T
	network    *plain.Network
	l1Store    *l1.Store
	l3Store    *Store
	l3Path     string
	l1Client   *L1Client
	caller     *http.Client
	callerUser string
	cancel     context.CancelFunc
	served     []chan error
	clients    []*http.Client
}

func newIntegrationHarness(t *testing.T) *integrationHarness {
	t.Helper()
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	ledgerFabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})

	l1Store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	l1Server, err := l1.NewServer(controlFabric, l1Store, l1.ServerConfig{AuthoritativeNodeTags: map[string][]string{"node-1": {"linux"}}})
	if err != nil {
		l1Store.Close()
		t.Fatal(err)
	}
	l1Listener, err := controlFabric.Listen("tcp", DefaultL1Address)
	if err != nil {
		l1Store.Close()
		t.Fatal(err)
	}

	l3Path := filepath.Join(t.TempDir(), "l3.sqlite")
	l3Store, err := OpenStore(l3Path, StoreOptions{})
	if err != nil {
		l1Listener.Close()
		l1Store.Close()
		t.Fatal(err)
	}
	l1Client, err := NewL1Client(ledgerFabric, DefaultL1Address)
	if err != nil {
		l3Store.Close()
		l1Listener.Close()
		l1Store.Close()
		t.Fatal(err)
	}
	l3Server, err := NewServer(ledgerFabric, l3Store, ServerConfig{})
	if err != nil {
		l1Client.CloseIdleConnections()
		l3Store.Close()
		l1Listener.Close()
		l1Store.Close()
		t.Fatal(err)
	}
	l3Listener, err := ledgerFabric.Listen("tcp", DefaultL3Address)
	if err != nil {
		l1Client.CloseIdleConnections()
		l3Store.Close()
		l1Listener.Close()
		l1Store.Close()
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	h := &integrationHarness{
		t: t, network: network, l1Store: l1Store, l3Store: l3Store, l3Path: l3Path, l1Client: l1Client,
		callerUser: "alice@example.test", cancel: cancel,
	}
	h.served = append(h.served, serveL1(ctx, l1Server, l1Listener), serveL3(ctx, l3Server, l3Listener))
	h.caller = h.client(fabric.Identity{NodeID: "caller", User: h.callerUser, Tags: []string{DefaultCallerPrincipalTag}}, DefaultL3Address)
	t.Cleanup(func() {
		for _, client := range h.clients {
			client.CloseIdleConnections()
		}
		l1Client.CloseIdleConnections()
		cancel()
		for _, served := range h.served {
			if err := <-served; err != nil {
				t.Errorf("serve: %v", err)
			}
		}
		if err := l3Store.Close(); err != nil {
			t.Errorf("close L3 store: %v", err)
		}
		if err := l1Store.Close(); err != nil {
			t.Errorf("close L1 store: %v", err)
		}
	})
	return h
}

func (h *integrationHarness) restartLedger() {
	h.t.Helper()
	if err := h.l3Store.Close(); err != nil {
		h.t.Fatal(err)
	}
	store, err := OpenStore(h.l3Path, StoreOptions{})
	if err != nil {
		h.t.Fatal(err)
	}
	h.l3Store = store
}

func serveL1(ctx context.Context, server *l1.Server, listener net.Listener) chan error {
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	return done
}

func serveL3(ctx context.Context, server *Server, listener net.Listener) chan error {
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	return done
}

func (h *integrationHarness) client(identity fabric.Identity, address string) *http.Client {
	participant := h.network.NewFabric(identity)
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return participant.Dial(ctx, network, address)
	}}
	client := &http.Client{Transport: transport}
	h.clients = append(h.clients, client)
	return client
}

func (h *integrationHarness) agent() *http.Client {
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{l1.DefaultAgentPrincipalTag}}, DefaultL1Address)
	registration := contract.NodeRegistration{
		NodeID: "node-1", BootSessionID: "boot-1", OS: "linux", Architecture: "arm64", AgentVersion: "test",
	}
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration, nil)
	if status != http.StatusOK {
		h.t.Fatalf("register status = %d body=%s", status, body)
	}
	return agent
}

func inlineRunRequest(content string) CreateRunRequest {
	digest := sha256.Sum256([]byte(content))
	return CreateRunRequest{
		InlineScript: &InlineScriptInput{
			Content: content, SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{"/bin/sh"},
		},
		Params:         json.RawMessage(`{"branch":"feature/run-ledger","attempt":1}`),
		Tags:           []string{"linux"},
		Limits:         &contract.RunLimits{MaxRuntimeSeconds: 300, MaxCost: 4.25},
		EnvelopeSchema: json.RawMessage(`{"type":"object"}`),
	}
}

func (h *integrationHarness) submit(request CreateRunRequest, idempotencyKey string) RunAccepted {
	h.t.Helper()
	status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs", request, http.Header{"Idempotency-Key": []string{idempotencyKey}})
	if status != http.StatusCreated {
		h.t.Fatalf("submit status = %d body=%s", status, body)
	}
	var accepted RunAccepted
	if err := json.Unmarshal(body, &accepted); err != nil {
		h.t.Fatal(err)
	}
	return accepted
}

func (h *integrationHarness) do(client *http.Client, method, path string, body any, headers http.Header) (int, http.Header, []byte) {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			h.t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, "http://wefty.invalid"+path, reader)
	if err != nil {
		h.t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for name, values := range headers {
		request.Header[name] = append([]string(nil), values...)
	}
	response, err := client.Do(request)
	if err != nil {
		h.t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	return response.StatusCode, response.Header.Clone(), responseBody
}

type loseSubmitResponseClient struct {
	JobClient
	submitted l1.Job
}

func (c *loseSubmitResponseClient) SubmitJob(ctx context.Context, spec contract.JobSpec) (l1.Job, error) {
	job, err := c.JobClient.SubmitJob(ctx, spec)
	if err != nil {
		return l1.Job{}, err
	}
	c.submitted = job
	return l1.Job{}, errors.New("injected crash after L1 committed the job")
}

func TestDispatchRecoveryCreatesExactlyOneL1JobAndPreservesScript(t *testing.T) {
	h := newIntegrationHarness(t)
	content := "#!/bin/sh\nprintf 'durable run\\n'\n"
	accepted := h.submit(inlineRunRequest(content), "run-crash-1")

	before, err := h.l3Store.GetRun(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if before.Status != contract.RunPending {
		t.Fatalf("run status before reconciliation = %q, want pending", before.Status)
	}
	var outboxCount int
	if err := h.l3Store.db.QueryRow(`SELECT count(*) FROM dispatch_outbox WHERE run_id=?`, accepted.RunID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("dispatch intents = %d, want 1", outboxCount)
	}
	// Simulate the requested crash window: the process exits after the atomic
	// run/outbox commit and before it attempts any L1 dispatch.
	h.restartLedger()

	crashingClient := &loseSubmitResponseClient{JobClient: h.l1Client}
	crashingReconciler, err := NewReconciler(h.l3Store, crashingClient, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := crashingReconciler.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("crash-injected reconciliation returned nil error")
	}
	if crashingClient.submitted.JobID == "" {
		t.Fatal("L1 did not commit a job before the injected crash")
	}
	// Exercise the second dangerous window too: L1 committed, but the ledger
	// never recorded the response. A fresh process must replay the same key.
	h.restartLedger()

	restarted, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	agent := h.agent()
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1"}, nil)
	if status != http.StatusOK {
		t.Fatalf("claim status = %d body=%s", status, body)
	}
	var claim l1.Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.StdEncoding.DecodeString(claim.Job.Spec.Execution.Executable.InlineBase64)
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded) != content {
		t.Fatalf("claimed inline bytes = %q, want %q", decoded, content)
	}
	digest := sha256.Sum256([]byte(content))
	if claim.Job.Spec.Execution.Executable.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("claimed SHA-256 = %q", claim.Job.Spec.Execution.Executable.SHA256)
	}
	if claim.Job.Spec.DispatchKey != before.DispatchKey {
		t.Fatalf("dispatch key = %q, want %q", claim.Job.Spec.DispatchKey, before.DispatchKey)
	}
	if !reflect.DeepEqual(claim.Job.Spec.Execution.Executable.Interpreter, []string{"/bin/sh"}) || claim.Job.Spec.Execution.Executable.Mode != 0o755 {
		t.Fatalf("claimed interpreter/mode = %v/%#o", claim.Job.Spec.Execution.Executable.Interpreter, claim.Job.Spec.Execution.Executable.Mode)
	}
	if claim.Job.Spec.Limits == nil || claim.Job.Spec.Limits.MaxRuntimeSeconds != 300 {
		t.Fatalf("claimed limits = %+v", claim.Job.Spec.Limits)
	}

	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1"}, nil)
	if status != http.StatusNoContent {
		t.Fatalf("second claim status = %d, want 204 (no duplicate job), body=%s", status, body)
	}
	var attempts int
	if err := h.l3Store.db.QueryRow(`SELECT attempt_count FROM dispatch_outbox WHERE run_id=?`, accepted.RunID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("dispatch attempts = %d, want 2", attempts)
	}
}

func TestInlineScriptAndTriggerProvenanceAreImmutable(t *testing.T) {
	h := newIntegrationHarness(t)
	request := inlineRunRequest("#!/bin/sh\necho immutable\n")
	accepted := h.submit(request, "run-immutable-1")

	provenance, err := h.l3Store.GetTrigger(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if provenance.Actor != h.callerUser || provenance.Source != "manual" {
		t.Fatalf("provenance = actor %q source %q", provenance.Actor, provenance.Source)
	}
	if !jsonEqual(provenance.Params, request.Params) {
		t.Fatalf("provenance params = %s, want %s", provenance.Params, request.Params)
	}

	if _, err := h.l3Store.db.Exec(`UPDATE run_scripts SET content=? WHERE run_id=?`, []byte("changed"), accepted.RunID); err == nil {
		t.Fatal("inline script update succeeded; want immutable trigger rejection")
	}
	if _, err := h.l3Store.db.Exec(`UPDATE run_triggers SET actor=? WHERE run_id=?`, "mallory", accepted.RunID); err == nil {
		t.Fatal("trigger provenance update succeeded; want immutable trigger rejection")
	}
	record, err := h.l3Store.GetRun(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Workflow.InlineScript.Content != request.InlineScript.Content || record.Workflow.InlineScript.SHA256 != request.InlineScript.SHA256 {
		t.Fatalf("stored inline script changed: %+v", record.Workflow.InlineScript)
	}
	if !reflect.DeepEqual(record.Tags, []string{"linux"}) || record.Limits == nil || record.Limits.MaxCost != 4.25 {
		t.Fatalf("stored tags/limits = %v/%+v", record.Tags, record.Limits)
	}
	var envelopeSchema []byte
	if err := h.l3Store.db.QueryRow(`SELECT envelope_schema_json FROM runs WHERE run_id=?`, accepted.RunID).Scan(&envelopeSchema); err != nil {
		t.Fatal(err)
	}
	if !jsonEqual(envelopeSchema, request.EnvelopeSchema) {
		t.Fatalf("stored envelope schema = %s, want %s", envelopeSchema, request.EnvelopeSchema)
	}
}

func TestParentRunRecordsChainProvenance(t *testing.T) {
	h := newIntegrationHarness(t)
	parent := h.submit(inlineRunRequest("#!/bin/sh\necho parent\n"), "parent-run")
	childRequest := inlineRunRequest("#!/bin/sh\necho child\n")
	childRequest.ParentRunID = parent.RunID
	child := h.submit(childRequest, "child-run")

	record, err := h.l3Store.GetRun(context.Background(), child.RunID)
	if err != nil {
		t.Fatal(err)
	}
	provenance, err := h.l3Store.GetTrigger(context.Background(), child.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.ParentRunID != parent.RunID || record.Trigger.Type != "chain" || record.Trigger.SourceRunID != parent.RunID {
		t.Fatalf("child record provenance = parent %q trigger %+v", record.ParentRunID, record.Trigger)
	}
	if provenance.Source != "chain" || provenance.SourceRunID != parent.RunID || provenance.Actor != h.callerUser {
		t.Fatalf("child trigger row = %+v", provenance)
	}
}

func TestJobTerminalStatesProjectOntoRun(t *testing.T) {
	for _, test := range []struct {
		name     string
		exitCode int
		want     contract.RunState
	}{
		{name: "success", exitCode: 0, want: contract.RunSucceeded},
		{name: "failure", exitCode: 17, want: contract.RunFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newIntegrationHarness(t)
			accepted := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "projection-"+test.name)
			reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := reconciler.ReconcileOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			agent := h.agent()
			status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1"}, nil)
			if status != http.StatusOK {
				t.Fatalf("claim status = %d body=%s", status, body)
			}
			var claim l1.Claim
			if err := json.Unmarshal(body, &claim); err != nil {
				t.Fatal(err)
			}
			completionPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", claim.Job.JobID, claim.Lease.AttemptID)
			completion := l1.CompletionRequest{
				FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion-1",
				Result: l1.ProcessResult{ExitCode: &test.exitCode},
			}
			status, _, body = h.do(agent, http.MethodPost, completionPath, completion, nil)
			if status != http.StatusOK {
				t.Fatalf("complete status = %d body=%s", status, body)
			}
			if err := reconciler.ReconcileOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			record, err := h.l3Store.GetRun(context.Background(), accepted.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if record.Status != test.want || record.StartedAt == nil || record.FinishedAt == nil {
				t.Fatalf("projected run = status %q started %v finished %v; want %q with timestamps", record.Status, record.StartedAt, record.FinishedAt, test.want)
			}
		})
	}
}

func TestRunSubmissionValidationAndIdempotency(t *testing.T) {
	h := newIntegrationHarness(t)
	request := inlineRunRequest("#!/bin/sh\necho replay\n")
	accepted := h.submit(request, "run-replay-1")
	status, headers, body := h.do(h.caller, http.MethodPost, "/v1/runs", request, http.Header{"Idempotency-Key": []string{"run-replay-1"}})
	if status != http.StatusOK || headers.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay status/header = %d/%q body=%s", status, headers.Get("Idempotency-Replayed"), body)
	}
	var replay RunAccepted
	if err := json.Unmarshal(body, &replay); err != nil {
		t.Fatal(err)
	}
	if replay.RunID != accepted.RunID {
		t.Fatalf("replay run ID = %q, want %q", replay.RunID, accepted.RunID)
	}

	changed := request
	changed.Params = json.RawMessage(`{"different":true}`)
	status, _, body = h.do(h.caller, http.MethodPost, "/v1/runs", changed, http.Header{"Idempotency-Key": []string{"run-replay-1"}})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorIdempotencyConflict)

	both := request
	both.WorkflowRef = "workflow/example@v1"
	status, _, body = h.do(h.caller, http.MethodPost, "/v1/runs", both, http.Header{"Idempotency-Key": []string{"run-both"}})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)

	refOnly := request
	refOnly.WorkflowRef = "workflow/example@v1"
	refOnly.InlineScript = nil
	status, _, body = h.do(h.caller, http.MethodPost, "/v1/runs", refOnly, http.Header{"Idempotency-Key": []string{"run-ref"}})
	assertAPIError(t, status, body, http.StatusNotImplemented, contract.ErrorNotImplemented)

	badHash := request
	badScript := *request.InlineScript
	badScript.SHA256 = string(make([]byte, 64))
	badHash.InlineScript = &badScript
	status, _, body = h.do(h.caller, http.MethodPost, "/v1/runs", badHash, http.Header{"Idempotency-Key": []string{"run-bad-hash"}})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
}

func TestProjectJobStateMatrix(t *testing.T) {
	tests := []struct {
		name    string
		current contract.RunState
		job     contract.JobState
		want    contract.RunState
		change  bool
	}{
		{"queued", contract.RunQueued, contract.JobQueued, contract.RunQueued, false},
		{"claimed remains queued", contract.RunQueued, contract.JobClaimed, contract.RunQueued, false},
		{"running", contract.RunQueued, contract.JobRunning, contract.RunRunning, true},
		{"awaiting input", contract.RunRunning, contract.JobAwaitingInput, contract.RunAwaitingInput, true},
		{"resumed", contract.RunAwaitingInput, contract.JobRunning, contract.RunRunning, true},
		{"succeeded", contract.RunRunning, contract.JobSucceeded, contract.RunSucceeded, true},
		{"failed from queued", contract.RunQueued, contract.JobFailed, contract.RunFailed, true},
		{"missed intermediate success", contract.RunQueued, contract.JobSucceeded, contract.RunSucceeded, true},
		{"stale queued ignored", contract.RunRunning, contract.JobQueued, contract.RunRunning, false},
		{"terminal unchanged", contract.RunSucceeded, contract.JobFailed, contract.RunSucceeded, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, change, err := ProjectJobState(test.current, test.job)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want || change != test.change {
				t.Fatalf("ProjectJobState(%q, %q) = (%q, %v), want (%q, %v)", test.current, test.job, got, change, test.want, test.change)
			}
		})
	}
}

func TestL3StoreUsesWAL(t *testing.T) {
	h := newIntegrationHarness(t)
	var mode string
	if err := h.l3Store.db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}
}

func assertAPIError(t *testing.T, status int, body []byte, wantStatus int, wantCode contract.ErrorCode) {
	t.Helper()
	if status != wantStatus {
		t.Fatalf("status = %d, want %d body=%s", status, wantStatus, body)
	}
	var response contract.ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != wantCode {
		t.Fatalf("error code = %q, want %q body=%s", response.Error.Code, wantCode, body)
	}
}

func jsonEqual(a, b []byte) bool {
	var av, bv any
	return json.Unmarshal(a, &av) == nil && json.Unmarshal(b, &bv) == nil && reflect.DeepEqual(av, bv)
}
