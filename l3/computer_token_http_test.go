package l3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

type emptyComputerJobLogs struct{}

func (emptyComputerJobLogs) GetJobLogs(context.Context, string, string, int) (l1.LogPage, error) {
	return l1.LogPage{Events: []contract.LogEvent{}}, nil
}

type controlledComputerGrantVerifier struct {
	mu           sync.Mutex
	proof        ComputerTokenScopeProof
	calls        int
	blockCall    int
	blocked      chan struct{}
	release      chan struct{}
	transientErr error
}

func (verifier *controlledComputerGrantVerifier) ProveComputerTokenScope(context.Context, string, string, string, string) (ComputerTokenScopeProof, error) {
	verifier.mu.Lock()
	verifier.calls++
	call := verifier.calls
	block := verifier.blockCall == call
	blocked, release := verifier.blocked, verifier.release
	err := verifier.transientErr
	proof := verifier.proof
	verifier.mu.Unlock()
	if block {
		close(blocked)
		<-release
	}
	if err != nil {
		return ComputerTokenScopeProof{}, err
	}
	return proof, nil
}

type computerHTTPHarness struct {
	t       *testing.T
	store   *Store
	server  *Server
	network *plain.Network
	address string
	cancel  context.CancelFunc
	done    chan error
}

func newComputerHTTPHarness(t *testing.T, verifier ComputerGrantVerifier) *computerHTTPHarness {
	t.Helper()
	network := plain.NewNetwork()
	ledger := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	store, err := OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), StoreOptions{ComputerAuthorityInstanceID: "http-test"})
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ledger, store, ServerConfig{ComputerGrants: verifier, Logs: emptyComputerJobLogs{}})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	address := DefaultL3Address
	listener, err := ledger.Listen("tcp", address)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	h := &computerHTTPHarness{t: t, store: store, server: server, network: network, address: address, cancel: cancel, done: done}
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve L3: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close L3 store: %v", err)
		}
	})
	return h
}

func (h *computerHTTPHarness) client(nodeID string) *http.Client {
	participant := h.network.NewFabric(fabric.Identity{NodeID: nodeID})
	return &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return participant.Dial(ctx, network, h.address)
	}}}
}

func computerHTTPRunRequest(content string) CreateRunRequest {
	digest := sha256.Sum256([]byte(content))
	return CreateRunRequest{InlineScript: &InlineScriptInput{Content: content, SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{"/bin/sh"}}, Params: json.RawMessage(`{}`)}
}

func doComputerHTTP(t *testing.T, client *http.Client, method, path, token, key string, body any) (int, http.Header, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(payload)
	}
	request, err := http.NewRequest(method, "http://l3.invalid"+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, response.Header.Clone(), payload
}

func mintComputerHTTPToken(t *testing.T, h *computerHTTPHarness, client *http.Client, proof ComputerTokenScopeProof) ComputerTokenGrant {
	t.Helper()
	status, _, body := doComputerHTTP(t, client, http.MethodPost, "/v1/computer-token/mint", "", "",
		ComputerTokenMintRequest{ComputerID: proof.ComputerID, ComputerAttemptID: proof.ComputerAttemptID})
	if status != http.StatusCreated {
		t.Fatalf("mint status=%d body=%s", status, body)
	}
	var grant ComputerTokenGrant
	if err := json.Unmarshal(body, &grant); err != nil {
		t.Fatal(err)
	}
	return grant
}

func TestComputerHTTPAuthoritySurfaceAndNodeBinding(t *testing.T) {
	proof := ComputerTokenScopeProof{ComputerID: "computer-http", ComputerAttemptID: "attempt-current",
		ComputerStorageGeneration: 7, SubmitIntentRevision: 4, HostNodeID: "fabric-node-1", SubmitMaxInflight: 3}
	verifier := &controlledComputerGrantVerifier{proof: proof}
	h := newComputerHTTPHarness(t, verifier)
	node := h.client("fabric-node-1")
	foreignNode := h.client("fabric-node-2")

	earlierProof := proof
	earlierProof.ComputerAttemptID = "attempt-earlier"
	earlierProof.ComputerStorageGeneration = 6
	earlierGrant, err := h.store.MintComputerToken(context.Background(), earlierProof)
	if err != nil {
		t.Fatal(err)
	}
	earlierScope, err := h.store.AuthenticateComputerToken(context.Background(), earlierGrant.Token)
	if err != nil {
		t.Fatal(err)
	}
	earlierRun, _, err := h.store.CreateRun(context.Background(), CreateRunInput{IdempotencyKey: "earlier-root",
		Actor: "computer:" + proof.ComputerID, ComputerScope: &earlierScope, VerifyComputerScope: func(context.Context, ComputerTokenScope) error { return nil },
		Request: computerHTTPRunRequest("exit 6\n")})
	if err != nil {
		t.Fatal(err)
	}

	grant := mintComputerHTTPToken(t, h, node, proof)
	status, _, body := doComputerHTTP(t, foreignNode, http.MethodGet, "/v1/computer/self", grant.Token, "", nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("copied bearer status=%d body=%s", status, body)
	}
	status, _, body = doComputerHTTP(t, node, http.MethodGet, "/v1/computer/self", grant.Token, "", nil)
	if status != http.StatusOK {
		t.Fatalf("self status=%d body=%s", status, body)
	}
	var self map[string]any
	if err := json.Unmarshal(body, &self); err != nil {
		t.Fatal(err)
	}
	if len(self) != 4 || self["computer_id"] != proof.ComputerID {
		t.Fatalf("minimal /self = %#v", self)
	}

	request := computerHTTPRunRequest("exit 0\n")
	status, _, body = doComputerHTTP(t, node, http.MethodPost, "/v1/runs", grant.Token, "computer-http-root", request)
	if status != http.StatusCreated {
		t.Fatalf("create root status=%d body=%s", status, body)
	}
	var accepted RunAccepted
	if err := json.Unmarshal(body, &accepted); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/runs/" + accepted.RunID, "/v1/runs/" + accepted.RunID + "/lineage", "/v1/runs/" + accepted.RunID + "/logs"} {
		status, _, body = doComputerHTTP(t, node, http.MethodGet, path, grant.Token, "", nil)
		if status != http.StatusOK {
			t.Fatalf("authorized read %s status=%d body=%s", path, status, body)
		}
	}
	status, _, body = doComputerHTTP(t, node, http.MethodGet, "/v1/runs/"+earlierRun.RunID, grant.Token, "", nil)
	if status != http.StatusForbidden {
		t.Fatalf("earlier generation read status=%d body=%s", status, body)
	}

	negative := []struct {
		method, path string
		body         any
	}{
		{http.MethodGet, "/v1/runs/" + accepted.RunID + "/execution", nil},
		{http.MethodPost, "/v1/runs/" + accepted.RunID + "/envelopes", map[string]any{}},
		{http.MethodPost, "/v1/runs/" + accepted.RunID + "/gates", map[string]any{}},
		{http.MethodPost, "/v1/runs/" + accepted.RunID + "/rerun", map[string]any{}},
		{http.MethodPost, "/v1/runs/" + accepted.RunID + "/cancel", map[string]any{}},
		{http.MethodPost, "/v1/workflows/forbidden/versions", WorkflowVersionInput{}},
	}
	for _, test := range negative {
		status, _, body = doComputerHTTP(t, node, test.method, test.path, grant.Token, "negative", test.body)
		if status != http.StatusForbidden {
			t.Fatalf("negative %s %s status=%d body=%s", test.method, test.path, status, body)
		}
	}
	parented := request
	parented.ParentRunID = accepted.RunID
	status, _, body = doComputerHTTP(t, node, http.MethodPost, "/v1/runs", grant.Token, "parented", parented)
	if status != http.StatusForbidden {
		t.Fatalf("parented submit status=%d body=%s", status, body)
	}
	status, _, body = doComputerHTTP(t, node, http.MethodPost, "/v1/runs", grant.Token, "forged",
		map[string]any{"params": map[string]any{}, "computer_id": "forged", "inline_script": request.InlineScript})
	if status != http.StatusBadRequest {
		t.Fatalf("forged provenance status=%d body=%s", status, body)
	}

	if err := h.store.RevokeComputerTokens(context.Background(), ComputerTokenRevocationRequest{
		ComputerID: proof.ComputerID, SubmitIntentRevision: proof.SubmitIntentRevision + 1, Reason: "disabled"}); err != nil {
		t.Fatal(err)
	}
	disabled := append([]struct {
		method, path string
		body         any
	}{}, negative...)
	disabled = append(disabled,
		struct {
			method, path string
			body         any
		}{http.MethodGet, "/v1/computer/self", nil},
		struct {
			method, path string
			body         any
		}{http.MethodGet, "/v1/runs/" + accepted.RunID, nil},
		struct {
			method, path string
			body         any
		}{http.MethodGet, "/v1/runs/" + accepted.RunID + "/lineage", nil},
		struct {
			method, path string
			body         any
		}{http.MethodGet, "/v1/runs/" + accepted.RunID + "/logs", nil},
		struct {
			method, path string
			body         any
		}{http.MethodPost, "/v1/runs", request},
	)
	for _, test := range disabled {
		status, _, body = doComputerHTTP(t, node, test.method, test.path, grant.Token, "disabled", test.body)
		if status != http.StatusUnauthorized {
			t.Fatalf("disabled %s %s status=%d body=%s", test.method, test.path, status, body)
		}
	}
}

func TestComputerRootRunListIsPaginatedAndCurrentGenerationScoped(t *testing.T) {
	proof := ComputerTokenScopeProof{ComputerID: "computer-list", ComputerAttemptID: "attempt-current",
		ComputerStorageGeneration: 7, SubmitIntentRevision: 4, HostNodeID: "fabric-node-1", SubmitMaxInflight: 20}
	h := newComputerHTTPHarness(t, &controlledComputerGrantVerifier{proof: proof})
	client := h.client(proof.HostNodeID)
	grant := mintComputerHTTPToken(t, h, client, proof)
	created := make([]RunAccepted, 0, 2)
	for index, content := range []string{"exit 1\n", "exit 2\n"} {
		status, _, body := doComputerHTTP(t, client, http.MethodPost, "/v1/runs", grant.Token,
			fmt.Sprintf("computer-list-root-%d", index), computerHTTPRunRequest(content))
		if status != http.StatusCreated {
			t.Fatalf("create root %d status=%d body=%s", index, status, body)
		}
		var accepted RunAccepted
		if err := json.Unmarshal(body, &accepted); err != nil {
			t.Fatal(err)
		}
		created = append(created, accepted)
	}
	childRequest := computerHTTPRunRequest("exit 3\n")
	childRequest.ParentRunID = created[0].RunID
	child, _, err := h.store.CreateRun(t.Context(), CreateRunInput{IdempotencyKey: "computer-list-child", Actor: "run:" + created[0].RunID, Request: childRequest})
	if err != nil {
		t.Fatal(err)
	}
	earlierProof := proof
	earlierProof.ComputerAttemptID = "attempt-earlier"
	earlierProof.ComputerStorageGeneration = 6
	earlierGrant, err := h.store.MintComputerToken(t.Context(), earlierProof)
	if err != nil {
		t.Fatal(err)
	}
	earlierScope, err := h.store.AuthenticateComputerToken(t.Context(), earlierGrant.Token)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.store.CreateRun(t.Context(), CreateRunInput{IdempotencyKey: "computer-list-earlier", Actor: "computer:" + proof.ComputerID,
		ComputerScope: &earlierScope, VerifyComputerScope: func(context.Context, ComputerTokenScope) error { return nil }, Request: computerHTTPRunRequest("exit 6\n")}); err != nil {
		t.Fatal(err)
	}
	// Re-mint current authority because minting the earlier fixture revoked it.
	grant, err = h.store.MintComputerToken(t.Context(), proof)
	if err != nil {
		t.Fatal(err)
	}

	status, _, body := doComputerHTTP(t, client, http.MethodGet, "/v1/runs?origin=computer:self&limit=1", grant.Token, "", nil)
	if status != http.StatusOK {
		t.Fatalf("first page status=%d body=%s", status, body)
	}
	var first ComputerRunPage
	if err := json.Unmarshal(body, &first); err != nil {
		t.Fatal(err)
	}
	if len(first.Runs) != 1 || first.NextCursor == "" {
		t.Fatalf("first page = %+v", first)
	}
	status, _, body = doComputerHTTP(t, client, http.MethodGet, "/v1/runs?origin=computer:self&limit=1&cursor="+url.QueryEscape(first.NextCursor), grant.Token, "", nil)
	if status != http.StatusOK {
		t.Fatalf("second page status=%d body=%s", status, body)
	}
	var second ComputerRunPage
	if err := json.Unmarshal(body, &second); err != nil {
		t.Fatal(err)
	}
	if len(second.Runs) != 1 || second.NextCursor != "" || second.Runs[0].RunID == first.Runs[0].RunID {
		t.Fatalf("second page = %+v after %+v", second, first)
	}
	status, _, body = doComputerHTTP(t, client, http.MethodGet, "/v1/runs?origin=computer:self&include_descendants=true", grant.Token, "", nil)
	if status != http.StatusOK {
		t.Fatalf("descendant page status=%d body=%s", status, body)
	}
	var descendants ComputerRunPage
	if err := json.Unmarshal(body, &descendants); err != nil {
		t.Fatal(err)
	}
	if len(descendants.Runs) != 3 || !slices.ContainsFunc(descendants.Runs, func(run contract.RunRecord) bool { return run.RunID == child.RunID }) {
		t.Fatalf("descendant page = %+v", descendants)
	}
	for _, invalid := range []string{"/v1/runs", "/v1/runs?origin=computer:other", "/v1/runs?origin=computer:self&origin=computer:self"} {
		status, _, body = doComputerHTTP(t, client, http.MethodGet, invalid, grant.Token, "", nil)
		if status != http.StatusBadRequest {
			t.Fatalf("invalid list %q status=%d body=%s", invalid, status, body)
		}
	}
}

func TestComputerCreateRunRechecksRevocationAfterAuthentication(t *testing.T) {
	proof := ComputerTokenScopeProof{ComputerID: "computer-race", ComputerAttemptID: "attempt-race",
		ComputerStorageGeneration: 1, SubmitIntentRevision: 1, HostNodeID: "fabric-node-race", SubmitMaxInflight: 2}
	verifier := &controlledComputerGrantVerifier{proof: proof, blockCall: 2, blocked: make(chan struct{}), release: make(chan struct{})}
	h := newComputerHTTPHarness(t, verifier)
	client := h.client(proof.HostNodeID)
	grant := mintComputerHTTPToken(t, h, client, proof)
	type response struct {
		status int
		body   []byte
	}
	result := make(chan response, 1)
	go func() {
		status, _, body := doComputerHTTP(t, client, http.MethodPost, "/v1/runs", grant.Token, "disable-race", computerHTTPRunRequest("exit 0\n"))
		result <- response{status: status, body: body}
	}()
	select {
	case <-verifier.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("submission did not pause after bearer authentication")
	}
	if err := h.store.RevokeComputerTokens(context.Background(), ComputerTokenRevocationRequest{
		ComputerID: proof.ComputerID, SubmitIntentRevision: proof.SubmitIntentRevision + 1, Reason: "disabled"}); err != nil {
		t.Fatal(err)
	}
	close(verifier.release)
	got := <-result
	if got.status != http.StatusUnauthorized {
		t.Fatalf("paused submission committed after disable: status=%d body=%s", got.status, got.body)
	}
	var runs int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM run_triggers WHERE computer_id=?`, proof.ComputerID).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 0 {
		t.Fatalf("paused submission committed %d Runs after disable", runs)
	}
}

func TestComputerTransientScopeProofFailureDoesNotRevokeGrant(t *testing.T) {
	proof := ComputerTokenScopeProof{ComputerID: "computer-transient", ComputerAttemptID: "attempt-transient",
		ComputerStorageGeneration: 1, SubmitIntentRevision: 1, HostNodeID: "fabric-node-transient", SubmitMaxInflight: 2}
	verifier := &controlledComputerGrantVerifier{proof: proof}
	h := newComputerHTTPHarness(t, verifier)
	client := h.client(proof.HostNodeID)
	grant := mintComputerHTTPToken(t, h, client, proof)
	verifier.mu.Lock()
	verifier.transientErr = &Error{Code: contract.ErrorInternal, Message: "L1 reconfiguration in flight", Retryable: true}
	verifier.mu.Unlock()
	status, _, body := doComputerHTTP(t, client, http.MethodGet, "/v1/computer/self", grant.Token, "", nil)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("transient proof status=%d body=%s", status, body)
	}
	verifier.mu.Lock()
	verifier.transientErr = nil
	verifier.mu.Unlock()
	status, _, body = doComputerHTTP(t, client, http.MethodGet, "/v1/computer/self", grant.Token, "", nil)
	if status != http.StatusOK {
		t.Fatalf("transient proof revoked live grant: status=%d body=%s", status, body)
	}
}

func TestRunTokenLineageFilterErrorNeverReturnsUnfilteredDescendants(t *testing.T) {
	proof := ComputerTokenScopeProof{ComputerID: "unused", ComputerAttemptID: "unused", ComputerStorageGeneration: 1,
		SubmitIntentRevision: 1, HostNodeID: "unused", SubmitMaxInflight: 1}
	h := newComputerHTTPHarness(t, &controlledComputerGrantVerifier{proof: proof})
	root, _, err := h.store.CreateRun(context.Background(), CreateRunInput{IdempotencyKey: "lineage-root", Actor: "caller",
		Request: computerHTTPRunRequest("exit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.store.CreateRun(context.Background(), CreateRunInput{IdempotencyKey: "lineage-child", Actor: "run:" + root.RunID,
		Request: func() CreateRunRequest {
			request := computerHTTPRunRequest("exit 1\n")
			request.ParentRunID = root.RunID
			return request
		}()}); err != nil {
		t.Fatal(err)
	}
	token, err := h.store.ensureRunToken(context.Background(), root.RunID)
	if err != nil {
		t.Fatal(err)
	}
	h.server.filterRunVisibility = func(context.Context, string, string) (bool, error) {
		return false, internalError(context.DeadlineExceeded, "filter visible lineage")
	}
	status, _, body := doComputerHTTP(t, h.client("run-node"), http.MethodGet, "/v1/runs/"+root.RunID+"/lineage", token, "", nil)
	if status != http.StatusInternalServerError {
		t.Fatalf("lineage filter failure status=%d body=%s", status, body)
	}
}

var _ ComputerGrantVerifier = (*controlledComputerGrantVerifier)(nil)
