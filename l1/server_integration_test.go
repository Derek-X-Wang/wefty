package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
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
	policies := make(map[string]NodePolicy, len(nodeTags))
	for nodeID, tags := range nodeTags {
		policies[nodeID] = NodePolicy{
			Tags: tags, MaxOneshotSlots: DefaultMaxOneshotSlots, MaxServiceSlots: DefaultMaxServiceSlots,
		}
	}
	return newIntegrationHarnessWithPolicies(t, policies)
}

func newIntegrationHarnessWithPolicies(t *testing.T, policies map[string]NodePolicy) *integrationHarness {
	t.Helper()
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	clock := &fakeClock{now: time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)}
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), StoreOptions{Clock: clock, LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(serverFabric, store, ServerConfig{NodePolicies: policies})
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
		Class:         contract.JobClassOneShot,
		RoutingTags:   tags,
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: "/bin/echo"},
			Argv:             []string{"echo", "hello"},
			WorkingDirectory: "/tmp",
			HandoffDirectory: "/tmp/handoff",
		},
	}
}

func TestServiceJobSpecAcceptsPortlessExecutionWithoutHandoff(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	maxRestarts := 4
	spec := validJobSpec("service-contract", nil)
	spec.Class = contract.JobClassService
	spec.Execution.HandoffDirectory = ""
	spec.Restart = contract.RestartAlways
	spec.MaxRestartStreak = &maxRestarts

	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("submit service status = %d body=%s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	if job.Spec.Class != contract.JobClassService || job.Spec.PublishedPort != nil || job.Spec.Restart != contract.RestartAlways {
		t.Fatalf("persisted service spec = %#v", job.Spec)
	}
}

func TestJobSpecClassValidationIsConditional(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})

	tests := []struct {
		name   string
		mutate func(*contract.JobSpec)
		want   int
	}{
		{name: "missing class", mutate: func(spec *contract.JobSpec) { spec.Class = "" }, want: http.StatusBadRequest},
		{name: "one-shot missing handoff", mutate: func(spec *contract.JobSpec) { spec.Execution.HandoffDirectory = "" }, want: http.StatusBadRequest},
		{name: "service missing restart", mutate: func(spec *contract.JobSpec) {
			spec.Class = contract.JobClassService
			spec.Execution.HandoffDirectory = ""
		}, want: http.StatusBadRequest},
		{name: "unknown open class", mutate: func(spec *contract.JobSpec) {
			spec.Class = "scheduled"
			spec.Execution.HandoffDirectory = ""
		}, want: http.StatusCreated},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := validJobSpec(fmt.Sprintf("class-validation-%d", index), nil)
			test.mutate(&spec)
			status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
			if status != test.want {
				t.Fatalf("submit status = %d, want %d body=%s", status, test.want, body)
			}
		})
	}
}

func TestSensitiveEnvironmentIsRedactedFromClientJobAPIs(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")

	spec := validJobSpec("sensitive-redaction", []string{"linux"})
	spec.Execution.SensitiveEnv = map[string]string{contract.EnvRunToken: "wrun_top_secret"}
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated || bytes.Contains(body, []byte("wrun_top_secret")) {
		t.Fatalf("create response status/body = %d/%s; sensitive value must be redacted", status, body)
	}
	var created Job
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	if created.Spec.Execution.SensitiveEnv != nil {
		t.Fatalf("create response sensitive_env = %#v, want omitted", created.Spec.Execution.SensitiveEnv)
	}

	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+created.JobID, nil)
	if status != http.StatusOK || bytes.Contains(body, []byte("wrun_top_secret")) {
		t.Fatalf("get response status/body = %d/%s; sensitive value must be redacted", status, body)
	}

	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1"})
	if status != http.StatusOK {
		t.Fatalf("claim status = %d body=%s", status, body)
	}
	var claim Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	if claim.Job.Spec.Execution.SensitiveEnv[contract.EnvRunToken] != "wrun_top_secret" {
		t.Fatalf("claim sensitive_env = %#v, want agent-only token delivery", claim.Job.Spec.Execution.SensitiveEnv)
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
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorPrincipalForbidden)
	status, _, body = h.do(agent, http.MethodPost, "/v1/jobs", validJobSpec("cross-route", nil))
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorPrincipalForbidden)
}

func TestClientListsNodeLivenessAndDrainsNode(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{
		"alive-node": {"linux", "arm64"},
		"stale-node": {"linux"},
		"dead-node":  {"linux"},
	})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	deadAgent := h.client(fabric.Identity{NodeID: "dead-node", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(deadAgent, "dead-node")
	h.clock.Advance(DefaultNodeDeadAfter - DefaultNodeStaleAfter)
	staleAgent := h.client(fabric.Identity{NodeID: "stale-node", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(staleAgent, "stale-node")
	h.clock.Advance(DefaultNodeStaleAfter)
	aliveAgent := h.client(fabric.Identity{NodeID: "alive-node", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(aliveAgent, "alive-node")

	status, _, body := h.do(client, http.MethodGet, "/v1/nodes", nil)
	if status != http.StatusOK {
		t.Fatalf("list nodes status = %d body=%s", status, body)
	}
	var listed NodeList
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Nodes) != 3 {
		t.Fatalf("listed nodes = %d, want 3: %s", len(listed.Nodes), body)
	}
	wantStates := []contract.NodeState{contract.NodeAlive, contract.NodeDead, contract.NodeStale}
	for i, want := range wantStates {
		if listed.Nodes[i].State != want {
			t.Fatalf("node %q state = %q, want %q", listed.Nodes[i].NodeID, listed.Nodes[i].State, want)
		}
	}
	if got := listed.Nodes[0].AuthoritativeTags; len(got) != 2 || got[0] != "arm64" || got[1] != "linux" {
		t.Fatalf("alive tags = %v, want [arm64 linux]", got)
	}
	if listed.Nodes[0].AgentVersion != "test" {
		t.Fatalf("alive agent version = %q, want test", listed.Nodes[0].AgentVersion)
	}

	status, _, body = h.do(client, http.MethodPost, "/v1/nodes/alive-node/drain", nil)
	if status != http.StatusOK {
		t.Fatalf("operator drain status = %d body=%s", status, body)
	}
	var drained Node
	if err := json.Unmarshal(body, &drained); err != nil {
		t.Fatal(err)
	}
	if drained.State != contract.NodeDraining {
		t.Fatalf("drained state = %q, want draining", drained.State)
	}
	status, _, body = h.do(client, http.MethodPost, "/v1/nodes/alive-node/drain", nil)
	if status != http.StatusOK {
		t.Fatalf("operator drain replay status = %d body=%s", status, body)
	}
	status, _, body = h.do(client, http.MethodPost, "/v1/nodes/missing/drain", nil)
	assertAPIError(t, status, body, http.StatusNotFound, contract.ErrorNotFound)
}

func TestRegistrationRejectsSelfReportedEligibilityPolicy(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
		"node-1": {Tags: []string{"configured"}, MaxOneshotSlots: 7, MaxServiceSlots: 3},
	})
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag, "self-claimed"}})
	base := map[string]any{
		"node_id": "node-1", "boot_session_id": "boot", "os": "linux", "architecture": "arm64", "agent_version": "test",
	}
	withTags := maps.Clone(base)
	withTags["tags"] = []string{"self-claimed"}
	status, _, responseBody := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", withTags)
	assertAPIError(t, status, responseBody, http.StatusBadRequest, contract.ErrorInvalidRequest)

	withCapacity := maps.Clone(base)
	withCapacity["max_oneshot_slots"] = 99
	withCapacity["max_service_slots"] = 99
	status, _, responseBody = h.do(agent, http.MethodPost, "/v1/agent/nodes/register", withCapacity)
	assertAPIError(t, status, responseBody, http.StatusBadRequest, contract.ErrorInvalidRequest)

	withCapacityCapabilities := maps.Clone(base)
	withCapacityCapabilities["capabilities"] = map[string]bool{
		"process": true, "max_oneshot_slots": true, "max_service_slots": true,
	}
	status, _, responseBody = h.do(agent, http.MethodPost, "/v1/agent/nodes/register", withCapacityCapabilities)
	assertAPIError(t, status, responseBody, http.StatusBadRequest, contract.ErrorInvalidRequest)

	node := h.register(agent, "node-1")
	if len(node.AuthoritativeTags) != 1 || node.AuthoritativeTags[0] != "configured" {
		t.Fatalf("authoritative tags = %v, want [configured]", node.AuthoritativeTags)
	}
	if node.MaxOneshotSlots != 7 || node.MaxServiceSlots != 3 {
		t.Fatalf("authoritative capacities = %d/%d, want 7/3", node.MaxOneshotSlots, node.MaxServiceSlots)
	}

	status, _, responseBody = h.do(agent, http.MethodPost, "/v1/agent/nodes/node-1/heartbeat", HeartbeatRequest{BootSessionID: node.BootSessionID})
	if status != http.StatusOK {
		t.Fatalf("heartbeat status = %d body=%s", status, responseBody)
	}
	var heartbeat Node
	if err := json.Unmarshal(responseBody, &heartbeat); err != nil {
		t.Fatal(err)
	}
	if heartbeat.MaxOneshotSlots != 7 || heartbeat.MaxServiceSlots != 3 {
		t.Fatalf("heartbeat capacities = %d/%d, want 7/3", heartbeat.MaxOneshotSlots, heartbeat.MaxServiceSlots)
	}
	var storedOneshot, storedService int
	if err := h.store.db.QueryRow("SELECT max_oneshot_slots, max_service_slots FROM nodes WHERE node_id=?", "node-1").Scan(&storedOneshot, &storedService); err != nil {
		t.Fatal(err)
	}
	if storedOneshot != 7 || storedService != 3 {
		t.Fatalf("stored capacities = %d/%d, want 7/3", storedOneshot, storedService)
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
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorIdentityBound)

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
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeSessionReplaced)
	var count int
	if err := h.store.db.QueryRow("SELECT count(*) FROM nodes WHERE node_id=?", "stable-node").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("stable-node rows = %d, want 1", count)
	}
}

func TestSemanticAgentAuthorityErrors(t *testing.T) {
	assertSemanticAgentAuthorityErrors(t)
}

func assertSemanticAgentAuthorityErrors(t *testing.T) {
	t.Helper()

	t.Run("principal forbidden", func(t *testing.T) {
		h := newIntegrationHarness(t, nil)
		client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
		status, _, body := h.do(client, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1"})
		assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorPrincipalForbidden)
	})

	t.Run("identity bound", func(t *testing.T) {
		h := newIntegrationHarness(t, nil)
		owner := h.client(fabric.Identity{NodeID: "fabric-owner", Tags: []string{DefaultAgentPrincipalTag}})
		intruder := h.client(fabric.Identity{NodeID: "fabric-intruder", Tags: []string{DefaultAgentPrincipalTag}})
		h.register(owner, "stable-node")
		registration := contract.NodeRegistration{
			NodeID: "stable-node", BootSessionID: "boot-intruder", OS: "linux", Architecture: "arm64", AgentVersion: "test",
		}
		status, _, body := h.do(intruder, http.MethodPost, "/v1/agent/nodes/register", registration)
		assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorIdentityBound)
	})

	t.Run("node not registered", func(t *testing.T) {
		h := newIntegrationHarness(t, nil)
		agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "missing-node", BootSessionID: "boot-missing"})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeNotRegistered)
	})

	t.Run("node dead", func(t *testing.T) {
		h := newIntegrationHarness(t, nil)
		agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
		h.register(agent, "node-1")
		h.clock.Advance(DefaultNodeDeadAfter)
		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1"})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeDead)
	})

	t.Run("node draining", func(t *testing.T) {
		h := newIntegrationHarness(t, nil)
		agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
		h.register(agent, "node-1")
		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/node-1/drain", DrainRequest{BootSessionID: "boot-node-1"})
		if status != http.StatusOK {
			t.Fatalf("drain status = %d body=%s", status, body)
		}
		status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1"})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeDraining)
	})

	t.Run("node session replaced", func(t *testing.T) {
		h := newIntegrationHarness(t, nil)
		agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
		h.register(agent, "node-1")
		registration := contract.NodeRegistration{
			NodeID: "node-1", BootSessionID: "boot-new", OS: "linux", Architecture: "arm64", AgentVersion: "test",
		}
		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration)
		if status != http.StatusOK {
			t.Fatalf("replacement registration status = %d body=%s", status, body)
		}
		status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/node-1/heartbeat", HeartbeatRequest{BootSessionID: "boot-node-1"})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeSessionReplaced)
	})

	t.Run("attempt scope", func(t *testing.T) {
		h := newIntegrationHarness(t, nil)
		client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
		owner := h.client(fabric.Identity{NodeID: "fabric-owner", Tags: []string{DefaultAgentPrincipalTag}})
		intruder := h.client(fabric.Identity{NodeID: "fabric-intruder", Tags: []string{DefaultAgentPrincipalTag}})
		h.register(owner, "node-1")
		job := h.submit(client, "semantic-attempt-errors", nil)
		status, _, body := h.do(owner, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1"})
		if status != http.StatusOK {
			t.Fatalf("claim status = %d body=%s", status, body)
		}
		var claim Claim
		if err := json.Unmarshal(body, &claim); err != nil {
			t.Fatal(err)
		}

		status, _, body = h.do(owner, http.MethodPost, "/v1/agent/jobs/"+job.JobID+"/attempts/missing-attempt/lease", RenewalRequest{FencingToken: claim.Lease.FencingToken})
		assertAPIError(t, status, body, http.StatusNotFound, contract.ErrorAttemptNotFound)

		renewPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/lease", job.JobID, claim.Lease.AttemptID)
		status, _, body = h.do(intruder, http.MethodPost, renewPath, RenewalRequest{FencingToken: claim.Lease.FencingToken})
		assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorAttemptNotOwned)
	})
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
	if response.Error.Retryable {
		t.Fatalf("error retryable = true, want false body=%s", body)
	}
}
