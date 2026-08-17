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
	"net/url"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

type integrationHarness struct {
	t           *testing.T
	network     *plain.Network
	l1Store     *l1.Store
	l3Store     *Store
	l3Path      string
	l1Client    *L1Client
	caller      *http.Client
	agentClient *http.Client
	callerUser  string
	cancel      context.CancelFunc
	served      []chan error
	clients     []*http.Client
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
	l1Server, err := l1.NewServer(controlFabric, l1Store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		"node-1": l1.DefaultNodePolicy("linux", contract.StableNodeTagPrefix+"node-1"),
	}})
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
	l3Server, err := NewServer(ledgerFabric, l3Store, ServerConfig{Logs: l1Client})
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
	if h.agentClient != nil {
		return h.agentClient
	}
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{l1.DefaultAgentPrincipalTag}}, DefaultL1Address)
	registration := contract.NodeRegistration{
		NodeID: "node-1", BootSessionID: "boot-1", OS: "linux", Architecture: "arm64", AgentVersion: "test",
	}
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration, nil)
	if status != http.StatusOK {
		h.t.Fatalf("register status = %d body=%s", status, body)
	}
	h.agentClient = agent
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

func workflowVersionInput(content string) WorkflowVersionInput {
	digest := sha256.Sum256([]byte(content))
	return WorkflowVersionInput{
		Content: content, SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{"/bin/sh"},
	}
}

func (h *integrationHarness) createWorkflowVersion(workflowID, content string) WorkflowVersion {
	h.t.Helper()
	status, _, body := h.do(h.caller, http.MethodPost, "/v1/workflows/"+workflowID+"/versions", workflowVersionInput(content), nil)
	if status != http.StatusCreated {
		h.t.Fatalf("create workflow version status = %d body=%s", status, body)
	}
	var version WorkflowVersion
	if err := json.Unmarshal(body, &version); err != nil {
		h.t.Fatal(err)
	}
	return version
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

type permanentSubmitErrorClient struct {
	JobClient
	runID string
}

func (c *permanentSubmitErrorClient) SubmitJob(ctx context.Context, spec contract.JobSpec) (l1.Job, error) {
	if spec.Labels["run_id"] == c.runID {
		return l1.Job{}, &Error{Code: contract.ErrorDispatchKeyConflict, Message: "dispatch key conflict", Retryable: false}
	}
	return c.JobClient.SubmitJob(ctx, spec)
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
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1", Class: contract.JobClassOneShot}, nil)
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

	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1", Class: contract.JobClassOneShot}, nil)
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

func TestPermanentDispatchFailureFailsRunAndReleasesSucceededParent(t *testing.T) {
	h := newIntegrationHarness(t)
	parent := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "permanent-parent")
	parentClaim := dispatchAndClaimRun(t, h, parent.RunID)
	completeClaim(t, h, parentClaim, 0, "complete-permanent-parent")

	childRequest := inlineRunRequest("#!/bin/sh\nexit 0\n")
	childRequest.ParentRunID = parent.RunID
	child := h.submit(childRequest, "permanent-child")
	client := &permanentSubmitErrorClient{JobClient: h.l1Client, runID: child.RunID}
	reconciler, err := NewReconciler(h.l3Store, client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("permanent dispatch failure was not reported")
	}
	childRecord, err := h.l3Store.GetRun(context.Background(), child.RunID)
	if err != nil {
		t.Fatal(err)
	}
	parentRecord, err := h.l3Store.GetRun(context.Background(), parent.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if childRecord.Status != contract.RunFailed || parentRecord.Status != contract.RunFailed {
		t.Fatalf("permanent dispatch states child/parent = %q/%q, want failed/failed", childRecord.Status, parentRecord.Status)
	}
	var staged *string
	if err := h.l3Store.db.QueryRow(`SELECT token_delivery FROM dispatch_outbox WHERE run_id=?`, child.RunID).Scan(&staged); err != nil {
		t.Fatal(err)
	}
	if staged != nil {
		t.Fatal("permanently failed dispatch retained staged run token")
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

func TestSavedWorkflowVersionsAreImmutableAndRunsPinSnapshots(t *testing.T) {
	h := newIntegrationHarness(t)
	v1Content := "#!/bin/sh\necho workflow-v1\n"
	v1 := h.createWorkflowVersion("dogfood", v1Content)
	if v1.Version != 1 || v1.WorkflowRef != "workflow://dogfood/v1" || v1.Mode != 0o755 {
		t.Fatalf("workflow v1 = %+v", v1)
	}
	status, _, body := h.do(h.caller, http.MethodGet, "/v1/workflows/dogfood/versions/v1", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("get workflow v1 status = %d body=%s", status, body)
	}
	var fetched WorkflowVersion
	if err := json.Unmarshal(body, &fetched); err != nil {
		t.Fatal(err)
	}
	if fetched.Content != v1Content || fetched.SHA256 != v1.SHA256 {
		t.Fatalf("fetched workflow v1 = %+v", fetched)
	}
	if _, err := h.l3Store.db.Exec(`UPDATE workflow_versions SET content=? WHERE workflow_id=? AND version=?`, []byte("changed"), "dogfood", 1); err == nil {
		t.Fatal("workflow version update succeeded; want immutable trigger rejection")
	}
	if _, err := h.l3Store.db.Exec(`DELETE FROM workflow_versions WHERE workflow_id=? AND version=?`, "dogfood", 1); err == nil {
		t.Fatal("workflow version delete succeeded; want immutable trigger rejection")
	}

	runRequest := inlineRunRequest("unused")
	runRequest.InlineScript = nil
	runRequest.WorkflowRef = "workflow://dogfood"
	accepted := h.submit(runRequest, "saved-workflow-run-v1")

	v2Content := "#!/bin/sh\necho workflow-v2\n"
	v2 := h.createWorkflowVersion("dogfood", v2Content)
	if v2.Version != 2 || v2.WorkflowRef != "workflow://dogfood/v2" {
		t.Fatalf("workflow v2 = %+v", v2)
	}
	record, err := h.l3Store.GetRun(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Workflow.WorkflowRef != v1.WorkflowRef || record.Workflow.InlineScript != nil {
		t.Fatalf("run workflow source = %+v, want pinned %q", record.Workflow, v1.WorkflowRef)
	}
	var snapContent []byte
	var snapSHA string
	if err := h.l3Store.db.QueryRow(`SELECT content, sha256 FROM run_scripts WHERE run_id=?`, accepted.RunID).Scan(&snapContent, &snapSHA); err != nil {
		t.Fatal(err)
	}
	if string(snapContent) != v1Content || snapSHA != v1.SHA256 {
		t.Fatalf("run snapshot = %q/%q, want v1", snapContent, snapSHA)
	}

	pinned := runRequest
	pinned.WorkflowRef = "workflow://dogfood/v1"
	pinnedRun := h.submit(pinned, "saved-workflow-pinned-v1")
	pinnedRecord, err := h.l3Store.GetRun(context.Background(), pinnedRun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if pinnedRecord.Workflow.WorkflowRef != v1.WorkflowRef {
		t.Fatalf("explicitly pinned run ref = %q, want %q", pinnedRecord.Workflow.WorkflowRef, v1.WorkflowRef)
	}
}

func TestSavedWorkflowNotFoundAndRerunSnapshotReuse(t *testing.T) {
	h := newIntegrationHarness(t)
	status, _, body := h.do(h.caller, http.MethodGet, "/v1/workflows/missing/versions/v1", nil, nil)
	assertAPIError(t, status, body, http.StatusNotFound, contract.ErrorNotFound)

	missing := inlineRunRequest("unused")
	missing.InlineScript = nil
	missing.WorkflowRef = "workflow://missing/v9"
	status, _, body = h.do(h.caller, http.MethodPost, "/v1/runs", missing, http.Header{"Idempotency-Key": []string{"missing-workflow"}})
	assertAPIError(t, status, body, http.StatusNotFound, contract.ErrorNotFound)

	v1Content := "#!/bin/sh\necho rerun-v1\n"
	v1 := h.createWorkflowVersion("rerunnable", v1Content)
	request := inlineRunRequest("unused")
	request.InlineScript = nil
	request.WorkflowRef = "workflow://rerunnable"
	request.Tags = append(request.Tags, contract.StableNodeTagPrefix+"node-1")
	source := h.submit(request, "rerun-source")
	h.createWorkflowVersion("rerunnable", "#!/bin/sh\necho rerun-v2\n")

	status, headers, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+source.RunID+"/rerun", nil, http.Header{"Idempotency-Key": []string{"rerun-copy"}})
	if status != http.StatusCreated {
		t.Fatalf("rerun status = %d body=%s", status, body)
	}
	var rerun RunAccepted
	if err := json.Unmarshal(body, &rerun); err != nil {
		t.Fatal(err)
	}
	if rerun.RunID == source.RunID {
		t.Fatal("rerun returned source run id; want a new run")
	}
	record, err := h.l3Store.GetRun(context.Background(), rerun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Trigger.Type != "rerun" || record.Trigger.SourceRunID != source.RunID || record.Workflow.WorkflowRef != v1.WorkflowRef {
		t.Fatalf("rerun record = trigger %+v workflow %+v", record.Trigger, record.Workflow)
	}
	if !jsonEqual(record.Params, request.Params) {
		t.Fatalf("rerun params = %s, want %s", record.Params, request.Params)
	}

	status, replayHeaders, replayBody := h.do(h.caller, http.MethodPost, "/v1/runs/"+source.RunID+"/rerun", nil, http.Header{"Idempotency-Key": []string{"rerun-copy"}})
	if status != http.StatusOK || replayHeaders.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("rerun replay status/header = %d/%q body=%s", status, replayHeaders.Get("Idempotency-Replayed"), replayBody)
	}
	var replay RunAccepted
	if err := json.Unmarshal(replayBody, &replay); err != nil {
		t.Fatal(err)
	}
	if replay.RunID != rerun.RunID || headers.Get("Idempotency-Replayed") != "" {
		t.Fatalf("rerun replay id/header = %q/%q", replay.RunID, headers.Get("Idempotency-Replayed"))
	}

	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	agent := h.agent()
	for i := 0; i < 2; i++ {
		status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1", Class: contract.JobClassOneShot}, nil)
		if status != http.StatusOK {
			t.Fatalf("claim %d status = %d body=%s", i, status, body)
		}
		var claim l1.Claim
		if err := json.Unmarshal(body, &claim); err != nil {
			t.Fatal(err)
		}
		decoded, err := base64.StdEncoding.DecodeString(claim.Job.Spec.Execution.Executable.InlineBase64)
		if err != nil {
			t.Fatal(err)
		}
		if string(decoded) != v1Content || claim.Job.Spec.Execution.Executable.SHA256 != v1.SHA256 {
			t.Fatalf("claim %d snapshot = %q/%q, want v1", i, decoded, claim.Job.Spec.Execution.Executable.SHA256)
		}
		if claim.Job.Spec.Execution.HandoffDirectory != filepath.Join(DefaultHandoffRoot, source.RunID) {
			t.Fatalf("claim %d handoff directory = %q, want source-run directory", i, claim.Job.Spec.Execution.HandoffDirectory)
		}
		if claim.Job.Spec.Labels["run_id"] == rerun.RunID && claim.Job.Spec.Labels["handoff_owner_run_id"] != source.RunID {
			t.Fatalf("rerun handoff owner label = %#v", claim.Job.Spec.Labels)
		}
	}
}

func TestRunLogsProxyL1OpaqueCursor(t *testing.T) {
	h := newIntegrationHarness(t)
	accepted := h.submit(inlineRunRequest("#!/bin/sh\nprintf 'one\\ntwo\\n'\n"), "run-logs")

	status, _, body := h.do(h.caller, http.MethodGet, "/v1/runs/"+accepted.RunID+"/logs", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("pending logs status = %d body=%s", status, body)
	}
	var pending l1.LogPage
	if err := json.Unmarshal(body, &pending); err != nil {
		t.Fatal(err)
	}
	if len(pending.Events) != 0 {
		t.Fatalf("pending logs = %#v, want empty", pending.Events)
	}

	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	agent := h.agent()
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1", Class: contract.JobClassOneShot}, nil)
	if status != http.StatusOK {
		t.Fatalf("claim status = %d body=%s", status, body)
	}
	var claim l1.Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	logPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", claim.Job.JobID, claim.Lease.AttemptID)
	logs := l1.AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{
		{AttemptID: claim.Lease.AttemptID, Stream: contract.LogStdout, Sequence: 0, Timestamp: time.Now().UTC(), Bytes: []byte("one\n")},
		{AttemptID: claim.Lease.AttemptID, Stream: contract.LogStdout, Sequence: 1, Timestamp: time.Now().UTC(), Bytes: []byte("two\n")},
	}}
	status, _, body = h.do(agent, http.MethodPost, logPath, logs, nil)
	if status != http.StatusOK {
		t.Fatalf("append logs status = %d body=%s", status, body)
	}

	status, _, body = h.do(h.caller, http.MethodGet, "/v1/runs/"+accepted.RunID+"/logs?limit=1", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("first run logs status = %d body=%s", status, body)
	}
	var first l1.LogPage
	if err := json.Unmarshal(body, &first); err != nil {
		t.Fatal(err)
	}
	if len(first.Events) != 1 || string(first.Events[0].Bytes) != "one\n" || first.NextCursor == "" {
		t.Fatalf("first run logs = %#v", first)
	}
	status, _, body = h.do(h.caller, http.MethodGet, "/v1/runs/"+accepted.RunID+"/logs?limit=1&cursor="+url.QueryEscape(first.NextCursor), nil, nil)
	if status != http.StatusOK {
		t.Fatalf("second run logs status = %d body=%s", status, body)
	}
	var second l1.LogPage
	if err := json.Unmarshal(body, &second); err != nil {
		t.Fatal(err)
	}
	if len(second.Events) != 1 || string(second.Events[0].Bytes) != "two\n" || second.NextCursor == first.NextCursor {
		t.Fatalf("second run logs = %#v", second)
	}
}

func TestRerunWithoutStableNodePinFailsExplicitly(t *testing.T) {
	h := newIntegrationHarness(t)
	source := h.submit(inlineRunRequest("#!/bin/sh\necho unpinned\n"), "rerun-unpinned-source")
	status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+source.RunID+"/rerun", nil, http.Header{
		"Idempotency-Key": []string{"rerun-unpinned"},
	})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
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
			status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1", Class: contract.JobClassOneShot}, nil)
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
			if test.exitCode == 0 {
				if err := reconciler.ReconcileOnce(context.Background()); err != nil {
					t.Fatal(err)
				}
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
	refOnly.WorkflowRef = "workflow://example/v1"
	refOnly.InlineScript = nil
	status, _, body = h.do(h.caller, http.MethodPost, "/v1/runs", refOnly, http.Header{"Idempotency-Key": []string{"run-ref"}})
	assertAPIError(t, status, body, http.StatusNotFound, contract.ErrorNotFound)

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
		{"missed intermediate success passes through running", contract.RunQueued, contract.JobSucceeded, contract.RunRunning, true},
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

func TestReservedCancelRouteReturnsNotImplemented(t *testing.T) {
	h := newIntegrationHarness(t)
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "cancel-reserved")
	status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
	assertAPIError(t, status, body, http.StatusNotImplemented, contract.ErrorNotImplemented)
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
