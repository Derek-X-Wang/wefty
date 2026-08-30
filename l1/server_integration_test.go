package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

func TestServeReportsReconcileFailureRacingShutdown(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := NewServer(serverFabric, store, ServerConfig{ReconcileInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	durableFailure := errors.New("SQLITE_CORRUPT")
	calls := 0
	server.reconcile = func(ctx context.Context) (ReconcileResult, error) {
		calls++
		if calls == 1 {
			return store.Reconcile(ctx)
		}
		close(entered)
		<-release
		return ReconcileResult{}, internalError(durableFailure, "resume removal after attestation crash")
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	<-entered
	cancel()
	close(release)
	serveErr := <-done
	if serveErr == nil || !errors.Is(serveErr, durableFailure) ||
		!strings.Contains(serveErr.Error(), "resume removal after attestation crash") {
		t.Fatalf("Serve shutdown race = %v, want durable reconcile failure", serveErr)
	}
}

func TestServeContinuesAfterCanceledReconcileErrorUnderLiveContext(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := NewServer(serverFabric, store, ServerConfig{ReconcileInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	continued := make(chan struct{})
	calls := 0
	server.reconcile = func(ctx context.Context) (ReconcileResult, error) {
		calls++
		switch calls {
		case 1:
			return store.Reconcile(ctx)
		case 2:
			return ReconcileResult{}, internalError(context.Canceled, "read service log retention applicability")
		default:
			select {
			case <-continued:
			default:
				close(continued)
			}
			return store.Reconcile(ctx)
		}
	}
	var logs bytes.Buffer
	server.logf = func(format string, args ...any) { fmt.Fprintf(&logs, format, args...) }
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	select {
	case <-continued:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not resume after a live-context cancellation")
	}
	if !strings.Contains(logs.String(), "event=l1_reconcile_live_context_canceled") {
		t.Fatalf("live-context cancellation log = %q", logs.String())
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Serve shutdown after recovered reconciliation = %v", err)
	}
}

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeClockTimer
}

type fakeClockTimer struct {
	clock    *fakeClock
	deadline time.Time
	channel  chan time.Time
	active   bool
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	now := c.now
	var ready []*fakeClockTimer
	for _, timer := range c.timers {
		if timer.active && !timer.deadline.After(now) {
			timer.active = false
			ready = append(ready, timer)
		}
	}
	c.mu.Unlock()
	for _, timer := range ready {
		timer.channel <- now
	}
}

func (c *fakeClock) NewTimer(duration time.Duration) clockTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &fakeClockTimer{clock: c, deadline: c.now.Add(duration), channel: make(chan time.Time, 1), active: true}
	c.timers = append(c.timers, timer)
	return timer
}

func (timer *fakeClockTimer) C() <-chan time.Time { return timer.channel }

func (timer *fakeClockTimer) Stop() bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	wasActive := timer.active
	timer.active = false
	return wasActive
}

func (c *fakeClock) waitForTimers(t *testing.T, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := len(c.timers)
		c.mu.Unlock()
		if got >= count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("clock did not receive %d timer(s)", count)
}

type integrationHarness struct {
	t       *testing.T
	network *plain.Network
	store   *Store
	server  *Server
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
	return newIntegrationHarnessWithOptions(t, StoreOptions{}, policies)
}

func newIntegrationHarnessWithOptions(t *testing.T, options StoreOptions, policies map[string]NodePolicy) *integrationHarness {
	return newIntegrationHarnessWithPersonIdentityMode(t, options, policies, true)
}

func newIntegrationHarnessWithPersonIdentityMode(
	t *testing.T,
	options StoreOptions,
	policies map[string]NodePolicy,
	allowSelfAsserted bool,
) *integrationHarness {
	t.Helper()
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	clock := &fakeClock{now: time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)}
	options.Clock = clock
	if options.LeaseDuration <= 0 {
		options.LeaseDuration = 30 * time.Second
	}
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), options)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(serverFabric, store, ServerConfig{
		NodePolicies: policies, AllowSelfAssertedPersonIdentities: allowSelfAsserted,
	})
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
	h := &integrationHarness{t: t, network: network, store: store, server: server, clock: clock, cancel: cancel, served: make(chan error, 1)}
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
	return h.registerWithCapabilities(client, nodeID, map[string]bool{"kind:process": true})
}

func (h *integrationHarness) registerWithCapabilities(client *http.Client, nodeID string, capabilities map[string]bool) Node {
	h.t.Helper()
	revision := int64(1)
	if current, err := getNode(context.Background(), h.store.db, nodeID); err == nil && current.BootSessionID == "boot-"+nodeID {
		revision = current.CapabilityRevision + 1
	}
	registration := contract.NodeRegistration{
		NodeID: nodeID, BootSessionID: "boot-" + nodeID, RootInstanceID: "root-" + nodeID,
		OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities: capabilities, CapabilityRevision: revision, CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
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

func heartbeatRequestForNode(node Node) HeartbeatRequest {
	return heartbeatRequestForBoot(node, node.BootSessionID)
}

func heartbeatRequestForBoot(node Node, bootSessionID string) HeartbeatRequest {
	return HeartbeatRequest{
		BootSessionID: bootSessionID, Capabilities: node.Capabilities, CapabilityRevision: node.CapabilityRevision,
		CapabilityObservedAt: node.CapabilityObservedAt, MissingCapabilities: node.MissingCapabilities,
		CapabilityReasonCode: node.CapabilityReasonCode,
	}
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

func TestL1AcceptsOCIForCapabilityAwareClaiming(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	spec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   "oci-contract",
		Kind:          contract.JobKindOCI,
		Class:         contract.JobClassOneShot,
		Execution: contract.ExecutionSpec{
			OCI: &contract.OCIExecutionSpec{
				Image: contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest"},
			},
		},
	}

	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("submit OCI job status = %d, want %d body=%s", status, http.StatusCreated, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	if job.State != contract.JobQueued || job.Spec.Kind != contract.JobKindOCI {
		t.Fatalf("submitted OCI job = %#v", job)
	}
}

func TestL1RequiresServiceImageDigestBeforeRuntimeSupportCheck(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	spec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   "oci-service-without-digest",
		Kind:          contract.JobKindOCI,
		Class:         contract.JobClassService,
		Restart:       contract.RestartAlways,
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest"},
		}},
	}

	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusBadRequest {
		t.Fatalf("submit unresolved OCI service status = %d, want %d body=%s", status, http.StatusBadRequest, body)
	}
	var response contract.ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != contract.ErrorInvalidRequest || !strings.Contains(response.Error.Message, "image digest") {
		t.Fatalf("unresolved OCI service error = %#v", response.Error)
	}
}

func TestL1ReturnsTypedUnsupportedRuntimeHandler(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	spec := validJobSpec("process-runtime-handler", nil)
	spec.RuntimeHandler = "io.containerd.runc.v2"

	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("submit process job status = %d, want %d body=%s", status, http.StatusUnprocessableEntity, body)
	}
	var response contract.ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != contract.ErrorUnsupportedRuntimeHandler {
		t.Fatalf("submit process job error code = %q, want %q body=%s", response.Error.Code, contract.ErrorUnsupportedRuntimeHandler, body)
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
	if job.ServiceJob == nil || job.DesiredState != contract.ServiceDesiredRunning || job.RestartStreak != 0 {
		t.Fatalf("persisted service metadata = %#v", job.ServiceJob)
	}
	var serviceRows int
	if err := h.store.db.QueryRow("SELECT COUNT(*) FROM service_jobs WHERE job_id=?", job.JobID).Scan(&serviceRows); err != nil {
		t.Fatal(err)
	}
	if serviceRows != 1 {
		t.Fatalf("service_jobs rows for %s = %d, want 1", job.JobID, serviceRows)
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

	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1", Class: contract.JobClassOneShot})
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
			status, _, _, err := doRequest(agents[i], http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: nodeID, BootSessionID: "boot-" + nodeID, Class: contract.JobClassOneShot})
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

	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1", Class: contract.JobClassOneShot})
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
	if claim.Lease.LeaseTTL != 30*time.Second || renewed.LeaseTTL != 30*time.Second {
		t.Fatalf("lease TTLs = %s and %s, want 30s", claim.Lease.LeaseTTL, renewed.LeaseTTL)
	}
	if renewed.Directive != "" {
		t.Fatalf("one-shot renewal directive = %q, want none", renewed.Directive)
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

func TestLeaseTTLAndAttemptScopedDirectives(t *testing.T) {
	assertLeaseTTLAndAttemptScopedDirectives(t)
}

func assertLeaseTTLAndAttemptScopedDirectives(t *testing.T) {
	t.Helper()
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
	job := h.submit(client, "dispatch-lease-contract", []string{"linux"})

	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1", Class: contract.JobClassOneShot})
	if status != http.StatusOK {
		t.Fatalf("claim status = %d body=%s", status, body)
	}
	var claim Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	assertLeaseContract(t, body, claim.Lease, 30*time.Second, "")

	renewPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/lease", job.JobID, claim.Lease.AttemptID)
	renew := func(wantDirective AttemptDirective) {
		t.Helper()
		h.clock.Advance(time.Second)
		status, _, body := h.do(agent, http.MethodPost, renewPath, RenewalRequest{FencingToken: claim.Lease.FencingToken})
		if status != http.StatusOK {
			t.Fatalf("renew status = %d body=%s", status, body)
		}
		var lease AttemptLease
		if err := json.Unmarshal(body, &lease); err != nil {
			t.Fatal(err)
		}
		assertLeaseContract(t, body, lease, 30*time.Second, wantDirective)
	}

	renew("")

	// The desired-state and restart routes land in later tickets. Seed their
	// durable result directly so this ticket's acceptance stays at the L1 HTTP
	// seam while covering the renewal response that agents actually consume.
	tx, err := h.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO service_jobs(job_id, desired_state, bound_node_id) VALUES(?, 'stopped', ?)", job.JobID, "node-1"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	renew(AttemptDirectiveStop)

	tx, err = h.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("UPDATE service_jobs SET desired_state='running' WHERE job_id=?", job.JobID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO service_restart_requests(job_id, idempotency_key, request_hash, created_ns)
VALUES(?, 'restart-1', 'restart-hash-1', ?)`, job.JobID, h.clock.Now().UnixNano()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	renew(AttemptDirectiveRestart)
}

func assertLeaseContract(t *testing.T, body []byte, lease AttemptLease, wantTTL time.Duration, wantDirective AttemptDirective) {
	t.Helper()
	if lease.LeaseExpires.IsZero() {
		t.Fatal("lease_expires_at compatibility field is absent")
	}
	if lease.LeaseTTL != wantTTL {
		t.Fatalf("lease_ttl = %s, want %s", lease.LeaseTTL, wantTTL)
	}
	if lease.Directive != wantDirective {
		t.Fatalf("directive = %q, want %q", lease.Directive, wantDirective)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatal(err)
	}
	if nested, ok := wire["lease"]; ok {
		if err := json.Unmarshal(nested, &wire); err != nil {
			t.Fatal(err)
		}
	}
	for _, field := range []string{"lease_expires_at", "lease_ttl"} {
		if _, ok := wire[field]; !ok {
			t.Fatalf("wire response is missing %q: %s", field, body)
		}
	}
	_, hasDirective := wire["directive"]
	if (wantDirective != "") != hasDirective {
		t.Fatalf("directive presence = %t, want %t: %s", hasDirective, wantDirective != "", body)
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
			status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1", Class: contract.JobClassOneShot})
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
	if status != http.StatusOK || headers.Get("Idempotent-Replay") != "true" {
		t.Fatalf("replay status/header = %d/%q body=%s", status, headers.Get("Idempotent-Replay"), body)
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
	registration := contract.NodeRegistration{
		NodeID: "node-1", BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1,
		CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
	}

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

	status, _, body = h.do(client, http.MethodPost, "/v1/nodes/alive-node/drain", NodeIntentRequest{
		ClaimsEnabled: false, IntentRevision: 0, Reason: "maintenance",
	})
	if status != http.StatusOK {
		t.Fatalf("operator drain status = %d body=%s", status, body)
	}
	var drained Node
	if err := json.Unmarshal(body, &drained); err != nil {
		t.Fatal(err)
	}
	if drained.State != contract.NodeAlive || drained.ClaimsEnabled || drained.IntentRevision != 1 {
		t.Fatalf("drained node = %#v, want alive with claims disabled at revision 1", drained)
	}
	status, _, body = h.do(client, http.MethodPost, "/v1/nodes/alive-node/drain", NodeIntentRequest{
		ClaimsEnabled: false, IntentRevision: 0, Reason: "stale replay",
	})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorConflict)
	status, _, body = h.do(client, http.MethodPost, "/v1/nodes/missing/drain", NodeIntentRequest{
		ClaimsEnabled: false, IntentRevision: 0, Reason: "maintenance",
	})
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
		"kind:process": true, "max_oneshot_slots": true, "max_service_slots": true,
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

	status, _, responseBody = h.do(agent, http.MethodPost, "/v1/agent/nodes/node-1/heartbeat", heartbeatRequestForNode(node))
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
		Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1,
		CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
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

	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/stable-node/heartbeat", heartbeatRequestForBoot(restarted, "boot-1"))
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
		status, _, body := h.do(client, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1", Class: contract.JobClassOneShot})
		assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorPrincipalForbidden)
	})

	t.Run("identity bound", func(t *testing.T) {
		h := newIntegrationHarness(t, nil)
		owner := h.client(fabric.Identity{NodeID: "fabric-owner", Tags: []string{DefaultAgentPrincipalTag}})
		intruder := h.client(fabric.Identity{NodeID: "fabric-intruder", Tags: []string{DefaultAgentPrincipalTag}})
		h.register(owner, "stable-node")
		registration := contract.NodeRegistration{
			NodeID: "stable-node", BootSessionID: "boot-intruder", OS: "linux", Architecture: "arm64", AgentVersion: "test",
			Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1,
			CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
		}
		status, _, body := h.do(intruder, http.MethodPost, "/v1/agent/nodes/register", registration)
		assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorIdentityBound)
	})

	t.Run("node not registered", func(t *testing.T) {
		h := newIntegrationHarness(t, nil)
		agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "missing-node", BootSessionID: "boot-missing", Class: contract.JobClassOneShot})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeNotRegistered)
	})

	t.Run("node dead", func(t *testing.T) {
		h := newIntegrationHarness(t, nil)
		agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
		h.register(agent, "node-1")
		h.clock.Advance(DefaultNodeDeadAfter)
		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1", Class: contract.JobClassOneShot})
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
		status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1", Class: contract.JobClassOneShot})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeDraining)
	})

	t.Run("node session replaced", func(t *testing.T) {
		h := newIntegrationHarness(t, nil)
		agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
		h.register(agent, "node-1")
		registration := contract.NodeRegistration{
			NodeID: "node-1", BootSessionID: "boot-new", OS: "linux", Architecture: "arm64", AgentVersion: "test",
			Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1,
			CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
		}
		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration)
		if status != http.StatusOK {
			t.Fatalf("replacement registration status = %d body=%s", status, body)
		}
		var replaced Node
		if err := json.Unmarshal(body, &replaced); err != nil {
			t.Fatal(err)
		}
		status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/node-1/heartbeat", heartbeatRequestForBoot(replaced, "boot-node-1"))
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeSessionReplaced)
	})

	t.Run("attempt scope", func(t *testing.T) {
		h := newIntegrationHarness(t, map[string][]string{"node-1": nil})
		client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
		owner := h.client(fabric.Identity{NodeID: "fabric-owner", Tags: []string{DefaultAgentPrincipalTag}})
		intruder := h.client(fabric.Identity{NodeID: "fabric-intruder", Tags: []string{DefaultAgentPrincipalTag}})
		h.register(owner, "node-1")
		job := h.submit(client, "semantic-attempt-errors", nil)
		status, _, body := h.do(owner, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1", Class: contract.JobClassOneShot})
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
