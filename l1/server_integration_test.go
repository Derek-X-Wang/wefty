package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type integrationHarness struct {
	t       *testing.T
	network *plain.Network
	store   *Store
	clock   *fakeClock
	cancel  context.CancelFunc
	served  chan error
	clients []*http.Client
}

func newIntegrationHarness(t *testing.T, nodeTags map[string][]string) *integrationHarness {
	t.Helper()
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	clock := &fakeClock{now: time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)}
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), StoreOptions{Clock: clock, LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(serverFabric, store, ServerConfig{AuthoritativeNodeTags: nodeTags})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &integrationHarness{t: t, network: network, store: store, clock: clock, cancel: cancel, served: make(chan error, 1)}
	go func() { h.served <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		for _, client := range h.clients {
			client.CloseIdleConnections()
		}
		cancel()
		if err := <-h.served; err != nil {
			t.Errorf("serve L1: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return h
}

func (h *integrationHarness) client(identity fabric.Identity) *http.Client {
	participant := h.network.NewFabric(identity)
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return participant.Dial(ctx, network, "wefty://control-plane")
	}}
	client := &http.Client{Transport: transport}
	h.clients = append(h.clients, client)
	return client
}

func (h *integrationHarness) do(client *http.Client, method, path string, body any) (int, http.Header, []byte) {
	h.t.Helper()
	status, headers, responseBody, err := doRequest(client, method, path, body)
	if err != nil {
		h.t.Fatal(err)
	}
	return status, headers, responseBody
}

func doRequest(client *http.Client, method, path string, body any) (int, http.Header, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, "http://control-plane.invalid"+path, reader)
	if err != nil {
		return 0, nil, nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, nil, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return response.StatusCode, response.Header.Clone(), responseBody, nil
}

func (h *integrationHarness) register(client *http.Client, nodeID string) Node {
	h.t.Helper()
	registration := contract.NodeRegistration{
		NodeID: nodeID, BootSessionID: "boot-" + nodeID, OS: "linux", Architecture: "arm64", AgentVersion: "test",
	}
	status, _, body := h.do(client, http.MethodPost, "/v1/agent/nodes/register", registration)
	if status != http.StatusOK {
		h.t.Fatalf("register status = %d body=%s", status, body)
	}
	var node Node
	if err := json.Unmarshal(body, &node); err != nil {
		h.t.Fatal(err)
	}
	return node
}

func (h *integrationHarness) submit(client *http.Client, dispatchKey string, tags []string) Job {
	h.t.Helper()
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", validJobSpec(dispatchKey, tags))
	if status != http.StatusCreated {
		h.t.Fatalf("submit status = %d body=%s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		h.t.Fatal(err)
	}
	return job
}

func validJobSpec(dispatchKey string, tags []string) contract.JobSpec {
	return contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   dispatchKey,
		Kind:          "process",
		RoutingTags:   tags,
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: "/bin/echo"},
			Argv:             []string{"echo", "hello"},
			WorkingDirectory: "/tmp",
			HandoffDirectory: "/tmp/handoff",
		},
	}
}

func TestConcurrentClaimsExactlyOneWinner(t *testing.T) {
	const claimers = 12
	nodeTags := make(map[string][]string, claimers)
	for i := 0; i < claimers; i++ {
		nodeTags[fmt.Sprintf("node-%02d", i)] = []string{"linux", "arm64"}
	}
	h := newIntegrationHarness(t, nodeTags)
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	h.submit(client, "dispatch-race", []string{"linux"})

	agents := make([]*http.Client, claimers)
	for i := range agents {
		nodeID := fmt.Sprintf("node-%02d", i)
		agents[i] = h.client(fabric.Identity{NodeID: nodeID, Tags: []string{DefaultAgentPrincipalTag}})
		h.register(agents[i], nodeID)
	}

	ready := sync.WaitGroup{}
	ready.Add(claimers)
	start := make(chan struct{})
	type claimResult struct {
		status int
		err    error
	}
	results := make(chan claimResult, claimers)
	for i := range agents {
		go func(i int) {
			ready.Done()
			<-start
			nodeID := fmt.Sprintf("node-%02d", i)
			status, _, _, err := doRequest(agents[i], http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: nodeID, BootSessionID: "boot-" + nodeID})
			results <- claimResult{status: status, err: err}
		}(i)
	}
	ready.Wait()
	close(start)
	wins := 0
	for range claimers {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		switch status := result.status; status {
		case http.StatusOK:
			wins++
		case http.StatusNoContent:
		default:
			t.Fatalf("claim status = %d, want 200 or 204", status)
		}
	}
	if wins != 1 {
		t.Fatalf("claim winners = %d, want exactly 1", wins)
	}
}

func TestLeaseRenewalAndFencedCompletion(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
	job := h.submit(client, "dispatch-fence", []string{"linux"})

	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1"})
	if status != http.StatusOK {
		t.Fatalf("claim status = %d body=%s", status, body)
	}
	var claim Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	completionPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	exitCode := 0
	stale := CompletionRequest{FencingToken: claim.Lease.FencingToken + "-stale", IdempotencyKey: "completion-1", Result: ProcessResult{ExitCode: &exitCode}}
	status, _, body = h.do(agent, http.MethodPost, completionPath, stale)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorStaleFence)

	h.clock.Advance(10 * time.Second)
	renewPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/lease", job.JobID, claim.Lease.AttemptID)
	status, _, body = h.do(agent, http.MethodPost, renewPath, RenewalRequest{FencingToken: claim.Lease.FencingToken})
	if status != http.StatusOK {
		t.Fatalf("renew status = %d body=%s", status, body)
	}
	var renewed AttemptLease
	if err := json.Unmarshal(body, &renewed); err != nil {
		t.Fatal(err)
	}
	if !renewed.LeaseExpires.After(claim.Lease.LeaseExpires) {
		t.Fatalf("renewed expiry %s did not extend %s", renewed.LeaseExpires, claim.Lease.LeaseExpires)
	}

	completion := CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion-1", Result: ProcessResult{ExitCode: &exitCode}}
	status, _, body = h.do(agent, http.MethodPost, completionPath, completion)
	if status != http.StatusOK {
		t.Fatalf("complete status = %d body=%s", status, body)
	}
	var completed Job
	if err := json.Unmarshal(body, &completed); err != nil {
		t.Fatal(err)
	}
	if completed.State != contract.JobSucceeded {
		t.Fatalf("completed state = %q, want %q", completed.State, contract.JobSucceeded)
	}
	status, _, body = h.do(agent, http.MethodPost, completionPath, completion)
	if status != http.StatusOK {
		t.Fatalf("completion replay status = %d body=%s", status, body)
	}
}

func TestTagMatchingOverPlainFabric(t *testing.T) {
	tests := []struct {
		name     string
		jobTags  []string
		nodeTags []string
		want     int
	}{
		{name: "both empty", want: http.StatusOK},
		{name: "empty requirement", nodeTags: []string{"linux"}, want: http.StatusOK},
		{name: "normalized duplicates", jobTags: []string{" Linux ", "linux", ""}, nodeTags: []string{"LINUX", "linux"}, want: http.StatusOK},
		{name: "proper subset", jobTags: []string{"linux"}, nodeTags: []string{"arm64", "linux"}, want: http.StatusOK},
		{name: "missing tag", jobTags: []string{"gpu"}, nodeTags: []string{"linux"}, want: http.StatusNoContent},
		{name: "partial subset", jobTags: []string{"linux", "gpu"}, nodeTags: []string{"linux"}, want: http.StatusNoContent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newIntegrationHarness(t, map[string][]string{"node-1": test.nodeTags})
			client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
			agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
			node := h.register(agent, "node-1")
			if !TagsMatch(node.AuthoritativeTags, test.nodeTags) || !TagsMatch(test.nodeTags, node.AuthoritativeTags) {
				t.Fatalf("authoritative tags = %v, configured %v", node.AuthoritativeTags, test.nodeTags)
			}
			h.submit(client, "dispatch-tags", test.jobTags)
			status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1"})
			if status != test.want {
				t.Fatalf("claim status = %d, want %d body=%s", status, test.want, body)
			}
		})
	}
}

func TestDispatchKeyIdempotency(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	spec := validJobSpec("dispatch-idempotent", []string{" Linux ", "linux"})
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("first submit status = %d body=%s", status, body)
	}
	var first Job
	if err := json.Unmarshal(body, &first); err != nil {
		t.Fatal(err)
	}
	status, headers, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusOK || headers.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay status/header = %d/%q body=%s", status, headers.Get("Idempotency-Replayed"), body)
	}
	var replay Job
	if err := json.Unmarshal(body, &replay); err != nil {
		t.Fatal(err)
	}
	if replay.JobID != first.JobID {
		t.Fatalf("replay job ID = %q, want %q", replay.JobID, first.JobID)
	}
	var count int
	if err := h.store.db.QueryRow("SELECT count(*) FROM jobs WHERE dispatch_key=?", spec.DispatchKey).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("jobs for dispatch key = %d, want 1", count)
	}

	spec.Labels = map[string]string{"different": "body"}
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs", spec)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorDispatchKeyConflict)
}

func TestProtocolPrincipalsCannotCrossRouteGroups(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	registration := contract.NodeRegistration{NodeID: "node-1", BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test"}

	status, _, body := h.do(client, http.MethodPost, "/v1/agent/nodes/register", registration)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	status, _, body = h.do(agent, http.MethodPost, "/v1/jobs", validJobSpec("cross-route", nil))
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
}

func TestRegistrationRejectsSelfReportedTags(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"configured"}})
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag, "self-claimed"}})
	body := map[string]any{
		"node_id": "node-1", "boot_session_id": "boot", "os": "linux", "architecture": "arm64", "agent_version": "test",
		"tags": []string{"self-claimed"},
	}
	status, _, responseBody := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", body)
	assertAPIError(t, status, responseBody, http.StatusBadRequest, contract.ErrorInvalidRequest)

	node := h.register(agent, "node-1")
	if len(node.AuthoritativeTags) != 1 || node.AuthoritativeTags[0] != "configured" {
		t.Fatalf("authoritative tags = %v, want [configured]", node.AuthoritativeTags)
	}
}

func TestRegistrationKeepsOneStableNodeAcrossBootSessions(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"stable-node": {"configured"}})
	agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
	intruder := h.client(fabric.Identity{NodeID: "other-fabric-node", Tags: []string{DefaultAgentPrincipalTag}})

	registration := contract.NodeRegistration{
		NodeID: "stable-node", BootSessionID: "boot-1", OS: "linux", Architecture: "amd64", AgentVersion: "test",
	}
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration)
	if status != http.StatusOK {
		t.Fatalf("first registration status = %d body=%s", status, body)
	}
	registration.BootSessionID = "boot-intruder"
	status, _, body = h.do(intruder, http.MethodPost, "/v1/agent/nodes/register", registration)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)

	registration.BootSessionID = "boot-2"
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration)
	if status != http.StatusOK {
		t.Fatalf("restart registration status = %d body=%s", status, body)
	}
	var restarted Node
	if err := json.Unmarshal(body, &restarted); err != nil {
		t.Fatal(err)
	}
	if restarted.BootSessionID != "boot-2" || len(restarted.AuthoritativeTags) != 1 || restarted.AuthoritativeTags[0] != "configured" {
		t.Fatalf("restarted node = %#v", restarted)
	}

	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/stable-node/heartbeat", HeartbeatRequest{BootSessionID: "boot-1"})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorConflict)
	var count int
	if err := h.store.db.QueryRow("SELECT count(*) FROM nodes WHERE node_id=?", "stable-node").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("stable-node rows = %d, want 1", count)
	}
}

func TestStoreUsesWAL(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	var mode string
	if err := h.store.db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
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
