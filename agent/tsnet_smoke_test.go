//go:build tsnet_smoke

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/tsnet"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestTSNetSmoke(t *testing.T) {
	authKey := os.Getenv("TS_AUTHKEY")
	if authKey == "" {
		t.Skip("TS_AUTHKEY is not available")
	}
	controlURL := os.Getenv("TS_CONTROL_URL")
	agentPrincipalTag := os.Getenv("TS_AGENT_PRINCIPAL_TAG")
	if agentPrincipalTag == "" {
		agentPrincipalTag = l1.DefaultAgentPrincipalTag
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	serverName := "wefty://node/agent-smoke-server-" + suffix
	agentName := "wefty://node/agent-smoke-client-" + suffix
	stableNodeID := "agent-smoke-" + suffix

	serverFabric, err := tsnet.New(tsnet.Config{
		Name: serverName, StateDir: t.TempDir(), Credential: fabric.Credential{Value: authKey},
		Ephemeral: true, CoordinatorURL: controlURL, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer serverFabric.Close()
	agentFabric, err := tsnet.New(tsnet.Config{
		Name: agentName, StateDir: t.TempDir(), Credential: fabric.Credential{Value: authKey},
		Ephemeral: true, CoordinatorURL: controlURL, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer agentFabric.Close()
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{
		AgentPrincipalTag: agentPrincipalTag,
		NodePolicies: map[string]l1.NodePolicy{
			stableNodeID: l1.DefaultNodePolicy("tsnet-smoke"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", serverName)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, listener) }()

	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: serverName,
		NodeID: stableNodeID, BootSessionID: "boot-" + suffix, Version: "tsnet-smoke",
		LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	node, err := nodeAgent.Register(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if node.NodeID != stableNodeID || node.BootSessionID != "boot-"+suffix {
		t.Fatalf("registered node = %#v", node)
	}
	cancel()
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}

func TestTSNetServiceReachability(t *testing.T) {
	authKey := os.Getenv("TS_AUTHKEY")
	if authKey == "" {
		t.Skip("TS_AUTHKEY is not available")
	}
	controlURL := os.Getenv("TS_CONTROL_URL")
	agentPrincipalTag := os.Getenv("TS_AGENT_PRINCIPAL_TAG")
	if agentPrincipalTag == "" {
		agentPrincipalTag = l1.DefaultAgentPrincipalTag
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	serverName := "wefty://node/service-smoke-control-" + suffix
	agentName := "wefty://node/service-smoke-agent-" + suffix
	clientName := "wefty://node/service-smoke-client-" + suffix
	stableNodeID := "service-smoke-" + suffix
	publishedPort := 80

	serverFabric, err := tsnet.New(tsnet.Config{
		Name: serverName, StateDir: t.TempDir(), Credential: fabric.Credential{Value: authKey},
		Ephemeral: true, CoordinatorURL: controlURL, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer serverFabric.Close()
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{
		AgentPrincipalTag: agentPrincipalTag,
		NodePolicies: map[string]l1.NodePolicy{
			stableNodeID: l1.DefaultNodePolicy("tsnet-service-smoke"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", serverName)
	if err != nil {
		t.Fatal(err)
	}
	serverContext, stopServer := context.WithTimeout(context.Background(), 3*time.Minute)
	served := make(chan error, 1)
	go func() { served <- server.Serve(serverContext, listener) }()
	defer func() {
		stopServer()
		select {
		case serveErr := <-served:
			if serveErr != nil {
				t.Errorf("serve tsnet control plane: %v", serveErr)
			}
		case <-time.After(10 * time.Second):
			t.Error("timed out stopping tsnet control plane")
		}
	}()

	agentFabric, err := tsnet.New(tsnet.Config{
		Name: agentName, StateDir: t.TempDir(), Credential: fabric.Credential{Value: authKey},
		Ephemeral: true, CoordinatorURL: controlURL, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer agentFabric.Close()
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: serverName,
		NodeID: stableNodeID, BootSessionID: "boot-" + suffix, Version: "tsnet-service-smoke",
		Capabilities:      map[string]bool{"process": true},
		HeartbeatInterval: 5 * time.Second, ClaimInterval: 100 * time.Millisecond,
		RenewalInterval: 5 * time.Second, LogSpoolDirectory: t.TempDir(),
		ManagedRootDirectory: t.TempDir(), GuardianExecutable: agentBinaryPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	agentContext, stopAgent := context.WithCancel(context.Background())
	agentDone := make(chan error, 1)
	go func() { agentDone <- nodeAgent.Run(agentContext) }()
	defer func() {
		stopAgent()
		select {
		case agentErr := <-agentDone:
			if agentErr != nil {
				t.Errorf("run tsnet service agent: %v", agentErr)
			}
		case <-time.After(20 * time.Second):
			t.Error("timed out stopping tsnet service agent")
		}
		nodeAgent.Close()
	}()

	node := waitForTSNetSmokeNode(t, store, stableNodeID, 90*time.Second)
	job, _, err := store.CreateJob(context.Background(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   "tsnet-service-smoke-" + suffix,
		Kind:          "process",
		Class:         contract.JobClassService,
		PublishedPort: &publishedPort,
		Restart:       contract.RestartAlways,
		RoutingTags:   []string{"tsnet-service-smoke"},
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: echoServiceBinaryPath},
			Argv:             []string{echoServiceBinaryPath},
			WorkingDirectory: t.TempDir(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	running := waitForTSNetSmokeService(t, store, job.JobID, 60*time.Second)
	if running.BoundNodeID != stableNodeID || running.CurrentAttemptID == "" {
		t.Fatalf("running service placement = node %q attempt %q", running.BoundNodeID, running.CurrentAttemptID)
	}

	clientFabric, err := tsnet.New(tsnet.Config{
		Name: clientName, StateDir: t.TempDir(), Credential: fabric.Credential{Value: authKey},
		Ephemeral: true, CoordinatorURL: controlURL, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clientFabric.Close()
	publishedAddress := net.JoinHostPort(node.ConnectHost, strconv.Itoa(publishedPort))
	remoteClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return clientFabric.Dial(ctx, network, publishedAddress)
		}},
	}
	defer remoteClient.CloseIdleConnections()

	response, err := remoteClient.Get("http://service.invalid/healthz")
	if err != nil {
		t.Fatalf("GET %s/healthz over the tailnet: %v", publishedAddress, err)
	}
	var health struct {
		PID           int `json:"pid"`
		ListeningPort int `json:"listening_port"`
	}
	decodeErr := json.NewDecoder(response.Body).Decode(&health)
	closeErr := response.Body.Close()
	if response.StatusCode != http.StatusOK || decodeErr != nil || closeErr != nil {
		t.Fatalf("tailnet health response = status %d decode=%v close=%v", response.StatusCode, decodeErr, closeErr)
	}
	if health.PID <= 0 || health.ListeningPort <= 0 || health.ListeningPort == publishedPort {
		t.Fatalf("tailnet health payload = %#v; want a distinct injected backend port", health)
	}

	echoPayload := []byte("tsnet service reachability\n")
	request, err := http.NewRequest(http.MethodPost, "http://service.invalid/echo", bytes.NewReader(echoPayload))
	if err != nil {
		t.Fatal(err)
	}
	response, err = remoteClient.Do(request)
	if err != nil {
		t.Fatalf("POST %s/echo over the tailnet: %v", publishedAddress, err)
	}
	echoed, readErr := io.ReadAll(response.Body)
	closeErr = response.Body.Close()
	if response.StatusCode != http.StatusOK || readErr != nil || closeErr != nil || !bytes.Equal(echoed, echoPayload) {
		t.Fatalf("tailnet echo response = status %d body=%q read=%v close=%v", response.StatusCode, echoed, readErr, closeErr)
	}
}

func waitForTSNetSmokeNode(t *testing.T, store *l1.Store, nodeID string, timeout time.Duration) l1.Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		nodes, err := store.ListNodes(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, node := range nodes {
			if node.NodeID == nodeID && node.State == contract.NodeAlive && node.ConnectHost != "" {
				return node
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for tsnet node %q to register", nodeID)
	return l1.Node{}
}

func waitForTSNetSmokeService(t *testing.T, store *l1.Store, jobID string, timeout time.Duration) l1.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := store.GetJob(context.Background(), jobID)
		if err != nil {
			t.Fatal(err)
		}
		if job.State == contract.JobFailed {
			t.Fatalf("tsnet service %q failed: %s", jobID, job.LastFailure)
		}
		if job.State == contract.JobRunning && job.Ready != nil && *job.Ready &&
			job.PublishedPort != nil && *job.PublishedPort == 80 {
			return job
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for tsnet service %q to become ready", jobID)
	return l1.Job{}
}
