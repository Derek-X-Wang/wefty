package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

type forwardingRoundTripper func(*http.Request) (*http.Response, error)

func (f forwardingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestComputerSubmissionAndOriginCLIOverRealRoutes(t *testing.T) {
	assertComputerSubmissionAndOriginCLIOverRealRoutes(t)
}

func TestComputerSubmissionOutputProjectsExactPassUnavailable(t *testing.T) {
	ready := false
	failure := contract.SpawnFailure{Code: contract.SpawnFailurePassUnavailable, Message: "L3 grant synchronization unavailable"}
	failureJSON, err := json.Marshal(l1.ProcessResult{SpawnError: &failure})
	if err != nil {
		t.Fatal(err)
	}
	projection := l1.ComputerSubmissionState{ComputerID: "computer-pass", SubmitEnabled: true,
		SubmitIntentRevision: 7, SubmitMaxInflight: 4, PolicyRevision: 11, Status: "pass_unavailable",
		Ready: &ready, PassUnavailable: l1.ComputerPassUnavailable(failureJSON)}
	if projection.Ready == nil || *projection.Ready || projection.PassUnavailable == nil ||
		projection.PassUnavailable.Code != contract.SpawnFailurePassUnavailable || projection.PassUnavailable.Message != failure.Message {
		t.Fatalf("pass unavailable projection = %#v", projection)
	}
	var human bytes.Buffer
	if err := writeComputerSubmissionOutput(&human, computerSubmissionOutput{ComputerSubmissionMutationResult: l1.ComputerSubmissionMutationResult{
		ComputerSubmissionState: projection}}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(human.String(), "pass_unavailable: "+failure.Message) || !strings.Contains(human.String(), "false") {
		t.Fatalf("human pass unavailable projection:\n%s", human.String())
	}
}

func TestComputerSubmissionAndOriginUsageValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"unknown submission verb", []string{"computers", "submission", "rotate", "computer-one"}},
		{"missing CAS", []string{"computers", "submission", "disable", "computer-one"}},
		{"set inflight without limit", []string{"computers", "submission", "set-inflight", "computer-one", "--policy-revision", "1", "--submit-intent-revision", "0"}},
		{"set inflight below range", []string{"computers", "submission", "set-inflight", "computer-one", "--max-inflight", "0", "--policy-revision", "1", "--submit-intent-revision", "0"}},
		{"set inflight above range", []string{"computers", "submission", "set-inflight", "computer-one", "--max-inflight", "1001", "--policy-revision", "1", "--submit-intent-revision", "0"}},
		{"expect current conflict", []string{"computers", "submission", "disable", "computer-one", "--expect-current", "--policy-revision", "1"}},
		{"self origin", []string{"runs", "list", "--origin", "computer:self"}},
		{"whitespace origin", []string{"runs", "list", "--origin", "computer: computer-one"}},
		{"runs positional", []string{"runs", "list", "computer:one", "--origin", "computer:one"}},
		{"limit below range", []string{"runs", "list", "--origin", "computer:one", "--limit", "0"}},
		{"limit above range", []string{"runs", "list", "--origin", "computer:one", "--limit", "1001"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := execute(context.Background(), nil, true, test.args, &bytes.Buffer{}, &bytes.Buffer{})
			if _, ok := err.(usageError); !ok {
				t.Fatalf("error = %T %v, want usageError", err, err)
			}
		})
	}
}

func assertComputerSubmissionAndOriginCLIOverRealRoutes(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	ledgerFabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})
	adminFabric := network.NewFabric(fabric.Identity{NodeID: "admin-device", UserID: "admin-one", DeviceID: "device-one"})
	nonAdminFabric := network.NewFabric(fabric.Identity{NodeID: "viewer-device", UserID: "viewer", DeviceID: "device-viewer"})
	machineFabric := network.NewFabric(fabric.Identity{NodeID: "machine", UserID: "admin-one", DeviceID: "machine-device", Kind: fabric.IdentityKindMachine})
	callerFabric := network.NewFabric(fabric.Identity{NodeID: "caller", Tags: []string{l3.DefaultCallerPrincipalTag}})
	fixed := time.Date(2026, 8, 28, 18, 0, 0, 0, time.UTC)

	l3Store, err := l3.OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), l3.StoreOptions{Clock: l3.ClockFunc(func() time.Time { return fixed })})
	if err != nil {
		t.Fatal(err)
	}
	l3Server, err := l3.NewServer(ledgerFabric, l3Store, l3.ServerConfig{ControlPlaneNodeID: "control-plane"})
	if err != nil {
		t.Fatal(err)
	}
	l3Listener, err := ledgerFabric.Listen("tcp", l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	l3Done := serveTestServer(ctx, func() error { return l3Server.Serve(ctx, l3Listener) })

	revoker, err := l1.NewComputerTokenRevocationClient(controlFabric, l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	l1Store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{Clock: l1.ClockFunc(func() time.Time { return fixed })})
	if err != nil {
		t.Fatal(err)
	}
	l1Server, err := l1.NewServer(controlFabric, l1Store, l1.ServerConfig{
		AllowSelfAssertedPersonIdentities: true, ComputerTokenRevoker: revoker, RunLedgerNodeID: "run-ledger",
	})
	if err != nil {
		t.Fatal(err)
	}
	l1Listener, err := controlFabric.Listen("tcp", l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	l1Done := serveTestServer(ctx, func() error { return l1Server.Serve(ctx, l1Listener) })
	t.Cleanup(func() {
		cancel()
		revoker.CloseIdleConnections()
		for name, done := range map[string]<-chan error{"L1": l1Done, "L3": l3Done} {
			if err := <-done; err != nil {
				t.Errorf("%s server: %v", name, err)
			}
		}
		if err := l1Store.Close(); err != nil {
			t.Errorf("close L1: %v", err)
		}
		if err := l3Store.Close(); err != nil {
			t.Errorf("close L3: %v", err)
		}
	})

	adminClients := mustTestAPIClients(t, adminFabric)
	defer adminClients.close()
	challenge, err := l1Store.InitiateAdminBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var bootstrapOut bytes.Buffer
	if err := execute(ctx, adminClients, true, []string{"admin", "bootstrap", challenge.Nonce}, &bootstrapOut, &bytes.Buffer{}); err != nil {
		t.Fatalf("bootstrap admin: %v", err)
	}
	var policy l1.AdminPolicy
	if err := json.Unmarshal(bootstrapOut.Bytes(), &policy); err != nil {
		t.Fatal(err)
	}
	if policy.Revision != 1 || len(policy.Admins) != 1 {
		t.Fatalf("bootstrap policy = %#v", policy)
	}
	computer, _, err := l1Store.CreateComputer(ctx, l1.CreateComputerRequest{
		Name: "cli-submission", Spec: cliComputerSpec("computer:cli-submission"), Actor: "acceptance",
	})
	if err != nil {
		t.Fatal(err)
	}

	noopDisabled := executeComputerSubmissionJSON(t, ctx, adminClients, "disable", computer.ComputerID,
		"--policy-revision", "1", "--submit-intent-revision", "0")
	if noopDisabled.SubmitEnabled || noopDisabled.SubmitIntentRevision != 0 || noopDisabled.PolicyRevision != 1 ||
		noopDisabled.MutationApplied || noopDisabled.IdempotentReplay || noopDisabled.Revoked != nil {
		t.Fatalf("audited disable no-op = %#v", noopDisabled)
	}
	enabled := executeComputerSubmissionJSON(t, ctx, adminClients, "enable", computer.ComputerID,
		"--policy-revision", "1", "--submit-intent-revision", "0")
	if !enabled.SubmitEnabled || enabled.SubmitIntentRevision != 1 || enabled.PolicyRevision != 2 ||
		enabled.SubmitMaxInflight != l1.DefaultComputerSubmitMaxInflight || !enabled.MutationApplied ||
		enabled.Revoked == nil || enabled.Revoked.SubmitIntentRevision != 1 || enabled.IdempotentReplay {
		t.Fatalf("enable receipt = %#v", enabled)
	}
	replayedEnable := executeComputerSubmissionJSON(t, ctx, adminClients, "enable", computer.ComputerID,
		"--policy-revision", "1", "--submit-intent-revision", "0")
	if replayedEnable.SubmitIntentRevision != enabled.SubmitIntentRevision || replayedEnable.PolicyRevision != enabled.PolicyRevision ||
		replayedEnable.MutationApplied || !replayedEnable.IdempotentReplay || replayedEnable.Revoked != nil {
		t.Fatalf("keyless enable replay = %#v after %#v", replayedEnable, enabled)
	}
	roots, child := seedComputerOriginRuns(t, l3Store, computer.ComputerID)
	var humanSubmission, warning bytes.Buffer
	if err := execute(ctx, adminClients, false, []string{"computers", "submission", "set-inflight", computer.ComputerID,
		"--max-inflight", "2", "--policy-revision", "2", "--submit-intent-revision", "1",
		"--idempotency-key", "cli-resize"}, &humanSubmission, &warning); err != nil {
		t.Fatalf("human set-inflight: %v", err)
	}
	if !strings.Contains(humanSubmission.String(), "2/2") || !strings.Contains(humanSubmission.String(), "revision 2") ||
		!strings.Contains(warning.String(), "saturated at inflight 2/2") {
		t.Fatalf("human submission output=%q warning=%q", humanSubmission.String(), warning.String())
	}
	resizedState, err := adminClients.getComputerSubmission(ctx, computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	resized := computerSubmissionOutput{ComputerSubmissionMutationResult: l1.ComputerSubmissionMutationResult{
		ComputerSubmissionState: resizedState, MutationApplied: true}}
	if !resized.SubmitEnabled || resized.SubmitIntentRevision != 2 || resized.PolicyRevision != 3 || resized.SubmitMaxInflight != 2 {
		t.Fatalf("resize receipt = %#v", resized)
	}
	replayed := executeComputerSubmissionJSON(t, ctx, adminClients, "set-inflight", computer.ComputerID,
		"--max-inflight", "2", "--policy-revision", "2", "--submit-intent-revision", "1",
		"--idempotency-key", "cli-resize")
	if replayed.SubmitIntentRevision != resized.SubmitIntentRevision || replayed.PolicyRevision != resized.PolicyRevision ||
		replayed.MutationApplied || !replayed.IdempotentReplay || replayed.Revoked != nil {
		t.Fatalf("resize replay receipt = %#v after %#v", replayed, resized)
	}
	var staleOut bytes.Buffer
	err = execute(ctx, adminClients, true, []string{"computers", "submission", "disable", computer.ComputerID,
		"--policy-revision", "2", "--submit-intent-revision", "1", "--idempotency-key", "cli-stale"}, &staleOut, &bytes.Buffer{})
	assertCLIAPIError(t, err, contract.ErrorStalePolicyRevision)

	nonAdminClients := mustTestAPIClients(t, nonAdminFabric)
	defer nonAdminClients.close()
	err = execute(ctx, nonAdminClients, true, []string{"computers", "submission", "disable", computer.ComputerID,
		"--policy-revision", "3", "--submit-intent-revision", "2"}, &bytes.Buffer{}, &bytes.Buffer{})
	assertCLIAPIError(t, err, contract.ErrorAdminRequired)
	machineClients := mustTestAPIClients(t, machineFabric)
	defer machineClients.close()
	err = execute(ctx, machineClients, true, []string{"computers", "submission", "disable", computer.ComputerID,
		"--policy-revision", "3", "--submit-intent-revision", "2"}, &bytes.Buffer{}, &bytes.Buffer{})
	assertCLIAPIError(t, err, contract.ErrorPrincipalForbidden)

	disabled := executeComputerSubmissionJSON(t, ctx, adminClients, "disable", computer.ComputerID,
		"--policy-revision", "3", "--submit-intent-revision", "2", "--idempotency-key", "cli-disable")
	if disabled.SubmitEnabled || disabled.SubmitIntentRevision != 3 || disabled.Revoked == nil {
		t.Fatalf("disable receipt = %#v", disabled)
	}
	reenabled := executeComputerSubmissionJSON(t, ctx, adminClients, "enable", computer.ComputerID,
		"--policy-revision", "4", "--submit-intent-revision", "3", "--idempotency-key", "cli-reenable")
	if !reenabled.SubmitEnabled || reenabled.SubmitIntentRevision != 4 || reenabled.Revoked == nil {
		t.Fatalf("re-enable receipt = %#v", reenabled)
	}

	adminIdentity := fabric.Identity{FabricID: policy.Admins[0].FabricID, UserID: "admin-one", DeviceID: "device-one"}
	policy, err = l1Store.AddAdmin(ctx, adminIdentity, "admin-two", reenabled.PolicyRevision)
	if err != nil {
		t.Fatal(err)
	}
	baseTransport := adminClients.l1.client.Transport
	var revokeOnce sync.Once
	adminClients.l1.client.Transport = forwardingRoundTripper(func(request *http.Request) (*http.Response, error) {
		response, roundTripErr := baseTransport.RoundTrip(request)
		if roundTripErr == nil && request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/submission") {
			revokeOnce.Do(func() {
				adminTwo := fabric.Identity{FabricID: policy.Admins[0].FabricID, UserID: "admin-two", DeviceID: "device-two"}
				if _, removeErr := l1Store.RemoveAdmin(ctx, adminTwo, "admin-one", policy.Revision); removeErr != nil {
					t.Errorf("revoke acting admin between CLI read and CAS: %v", removeErr)
				}
			})
		}
		return response, roundTripErr
	})
	err = execute(ctx, adminClients, true, []string{"computers", "submission", "disable", computer.ComputerID,
		"--expect-current", "--idempotency-key", "cli-revoked-mid-command"}, &bytes.Buffer{}, &bytes.Buffer{})
	assertCLIAPIError(t, err, contract.ErrorAdminRequired)

	callerClients := mustTestAPIClients(t, callerFabric)
	defer callerClients.close()
	var firstOut bytes.Buffer
	if err := execute(ctx, callerClients, true, []string{"runs", "list", "--origin", "computer:" + computer.ComputerID, "--limit", "1"}, &firstOut, &bytes.Buffer{}); err != nil {
		t.Fatalf("first origin page: %v", err)
	}
	var first l3.ComputerRunPage
	if err := json.Unmarshal(firstOut.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if len(first.Runs) != 1 || first.NextCursor == "" || first.Runs[0].Trigger.Type != "computer" ||
		first.Runs[0].Trigger.ComputerID != computer.ComputerID {
		t.Fatalf("first CLI origin page = %#v", first)
	}
	var secondOut bytes.Buffer
	if err := execute(ctx, callerClients, true, []string{"runs", "list", "--origin", "computer:" + computer.ComputerID,
		"--limit", "1", "--cursor", first.NextCursor}, &secondOut, &bytes.Buffer{}); err != nil {
		t.Fatalf("second origin page: %v", err)
	}
	var second l3.ComputerRunPage
	if err := json.Unmarshal(secondOut.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if len(second.Runs) != 1 || second.NextCursor != "" ||
		second.Runs[0].Trigger.ComputerStorageGeneration == first.Runs[0].Trigger.ComputerStorageGeneration {
		t.Fatalf("second CLI origin page = %#v after %#v", second, first)
	}
	var human bytes.Buffer
	if err := execute(ctx, callerClients, false, []string{"runs", "list", "--origin", "computer:" + computer.ComputerID,
		"--include-descendants"}, &human, &bytes.Buffer{}); err != nil {
		t.Fatalf("descendant origin page: %v", err)
	}
	for _, want := range []string{roots[0].RunID, roots[1].RunID, child.RunID, "computer", "chain", "STORAGE GENERATION", "SUBMIT REVISION"} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("human origin output missing %q:\n%s", want, human.String())
		}
	}

	receipt := struct {
		ComputerID               string   `json:"computer_id"`
		SubmissionRevisions      []int64  `json:"submission_revisions"`
		OriginStorageGenerations []int64  `json:"origin_storage_generations"`
		OriginRunIDs             []string `json:"origin_run_ids"`
	}{ComputerID: computer.ComputerID, SubmissionRevisions: []int64{enabled.SubmitIntentRevision, resized.SubmitIntentRevision,
		disabled.SubmitIntentRevision, reenabled.SubmitIntentRevision}, OriginStorageGenerations: []int64{
		roots[0].Trigger.ComputerStorageGeneration, roots[1].Trigger.ComputerStorageGeneration},
		OriginRunIDs: []string{roots[0].RunID, roots[1].RunID, child.RunID}}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("assertion-derived Computer submission CLI receipt: %s", encoded)
}

func mustTestAPIClients(t *testing.T, participant fabric.Fabric) *apiClients {
	t.Helper()
	clients, err := newAPIClients(participant, l3.DefaultL1Address, l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	return clients
}

func executeComputerSubmissionJSON(t *testing.T, ctx context.Context, clients *apiClients, verb, computerID string, args ...string) computerSubmissionOutput {
	t.Helper()
	command := []string{"computers", "submission", verb, computerID}
	command = append(command, args...)
	var output bytes.Buffer
	if err := execute(ctx, clients, true, command, &output, &bytes.Buffer{}); err != nil {
		t.Fatalf("%s Computer submission: %v", verb, err)
	}
	var receipt computerSubmissionOutput
	if err := json.Unmarshal(output.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	var narrow map[string]json.RawMessage
	if err := json.Unmarshal(output.Bytes(), &narrow); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"grants", "current_job", "storage_id", "display_endpoint"} {
		if _, present := narrow[forbidden]; present {
			t.Fatalf("CLI submission receipt leaked %q: %s", forbidden, output.Bytes())
		}
	}
	return receipt
}

func assertCLIAPIError(t *testing.T, err error, code contract.ErrorCode) {
	t.Helper()
	response, ok := err.(*apiResponseError)
	if !ok || response.APIError.Code != code {
		t.Fatalf("CLI error = %T %v, want %s", err, err, code)
	}
}

func cliComputerSpec(dispatchKey string) contract.JobSpec {
	memoryBytes := int64(64 << 20)
	diskBytes := int64(1 << 30)
	digest := "sha256:" + strings.Repeat("a", 64)
	return contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: dispatchKey, Kind: contract.JobKindOCI,
		Class: contract.JobClassService, Restart: contract.RestartAlways,
		RoutingTags: []string{contract.StableNodeTagPrefix + "computer-node"},
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image:  contract.OCIImageSpec{Reference: "ghcr.io/example/computer:latest", Digest: &digest},
			Limits: &contract.OCILimits{MemoryBytes: &memoryBytes},
			Computer: &contract.OCIComputerSpec{Display: contract.OCIComputerDisplaySpec{
				Protocol: contract.ComputerDisplayProtocolRFBWebSocketV1}, DiskBytes: diskBytes},
		}},
	}
}

func seedComputerOriginRuns(t *testing.T, store *l3.Store, computerID string) ([]contract.RunRecord, contract.RunRecord) {
	t.Helper()
	roots := make([]contract.RunRecord, 0, 2)
	for generation := int64(1); generation <= 2; generation++ {
		proof := l3.ComputerTokenScopeProof{ComputerID: computerID, ComputerAttemptID: "attempt-" + strconv.FormatInt(generation, 10),
			ComputerStorageGeneration: generation, SubmitIntentRevision: generation, HostNodeID: "computer-node", SubmitMaxInflight: 20}
		grant, err := store.MintComputerToken(t.Context(), proof)
		if err != nil {
			t.Fatal(err)
		}
		scope, err := store.AuthenticateComputerToken(t.Context(), grant.Token)
		if err != nil {
			t.Fatal(err)
		}
		request := computerRunRequest("exit " + strconv.FormatInt(generation, 10) + "\n")
		run, _, err := store.CreateRun(t.Context(), l3.CreateRunInput{IdempotencyKey: "cli-origin-" + strconv.FormatInt(generation, 10),
			Actor: "computer:" + computerID, ComputerScope: &scope,
			VerifyComputerScope: func(context.Context, l3.ComputerTokenScope) error { return nil }, Request: request})
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, run)
	}
	childRequest := computerRunRequest("exit 3\n")
	childRequest.ParentRunID = roots[0].RunID
	child, _, err := store.CreateRun(t.Context(), l3.CreateRunInput{IdempotencyKey: "cli-origin-child",
		Actor: "run:" + roots[0].RunID, Request: childRequest})
	if err != nil {
		t.Fatal(err)
	}
	foreignProof := l3.ComputerTokenScopeProof{ComputerID: "computer-foreign", ComputerAttemptID: "attempt-foreign",
		ComputerStorageGeneration: 1, SubmitIntentRevision: 1, HostNodeID: "computer-node", SubmitMaxInflight: 20}
	foreignGrant, err := store.MintComputerToken(t.Context(), foreignProof)
	if err != nil {
		t.Fatal(err)
	}
	foreignScope, err := store.AuthenticateComputerToken(t.Context(), foreignGrant.Token)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateRun(t.Context(), l3.CreateRunInput{IdempotencyKey: "cli-origin-foreign",
		Actor: "computer:computer-foreign", ComputerScope: &foreignScope,
		VerifyComputerScope: func(context.Context, l3.ComputerTokenScope) error { return nil }, Request: computerRunRequest("exit 4\n")}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal([]int64{roots[0].Trigger.ComputerStorageGeneration, roots[1].Trigger.ComputerStorageGeneration}, []int64{1, 2}) {
		t.Fatalf("seeded origins = %#v", roots)
	}
	return roots, child
}

func computerRunRequest(content string) l3.CreateRunRequest {
	digest := sha256.Sum256([]byte(content))
	return l3.CreateRunRequest{InlineScript: &l3.InlineScriptInput{Content: content, SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{"/bin/sh"}}, Params: json.RawMessage(`{}`)}
}
