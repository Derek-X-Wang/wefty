package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
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

func TestNodeCLIIntentAndCapacity(t *testing.T) {
	assertNodeCLIIntentAndCapacity(t)
}

func assertNodeCLIIntentAndCapacity(t *testing.T) {
	t.Helper()
	harness := newNodeCLIHarness(t)
	ctx := context.Background()

	capacityNode := harness.registerNode(t, "capacity-node", "capacity", l1.NodePolicy{
		Tags: []string{"capacity"}, MaxOneshotSlots: 8, MaxServiceSlots: 4,
	})
	for index := 0; index < 2; index++ {
		harness.createAndClaim(t, capacityNode, contract.JobClassOneShot, "one-shot", index)
	}
	for index := 0; index < 3; index++ {
		harness.createAndClaim(t, capacityNode, contract.JobClassService, "service", index)
	}

	initial := runNodeCLI(t, ctx, harness.clients, false, "nodes", "list")
	for _, want := range []string{
		"REACHABILITY", "CLAIMS ENABLED (ELIGIBILITY)", "ONE-SHOT SLOTS", "SERVICE SLOTS",
		"capacity-node", "alive", "OVERCOMMITTED",
	} {
		if !strings.Contains(initial, want) {
			t.Fatalf("initial node output missing %q:\n%s", want, initial)
		}
	}
	initialFields := nodeTableFields(t, initial, capacityNode.node.NodeID)
	if initialFields[3] != "2/8" || initialFields[4] != "3/4" || initialFields[5] != "false" {
		t.Fatalf("initial capacity fields = %q, want one-shot 2/8, service 3/4, overcommitted false", initialFields)
	}

	if _, err := harness.store.HeartbeatNodeWithPolicy(ctx, capacityNode.identity.NodeID, capacityNode.node.NodeID,
		capacityNode.node.BootSessionID, l1.NodePolicy{Tags: []string{"capacity"}, MaxOneshotSlots: 1, MaxServiceSlots: 2}); err != nil {
		t.Fatal(err)
	}
	overcommitted := runNodeCLI(t, ctx, harness.clients, false, "nodes", "list")
	overcommittedFields := nodeTableFields(t, overcommitted, capacityNode.node.NodeID)
	if overcommittedFields[3] != "2/1" || overcommittedFields[4] != "3/2" || overcommittedFields[5] != "true" {
		t.Fatalf("reduced capacity fields = %q, want one-shot 2/1, service 3/2, overcommitted true", overcommittedFields)
	}

	deadNode := harness.registerNode(t, "dead-node", "dead", l1.NodePolicy{
		Tags: []string{"dead"}, MaxOneshotSlots: 4, MaxServiceSlots: 2,
	})
	harness.clock.Advance(l1.DefaultNodeDeadAfter)
	if _, err := harness.store.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}

	mutatedJSON := runNodeCLI(t, ctx, harness.clients, true,
		"nodes", "set-claims", deadNode.node.NodeID,
		"--claims-enabled=false", "--intent-revision=0", "--reason", "forbid work while dead",
	)
	var mutated l1.Node
	if err := json.Unmarshal([]byte(mutatedJSON), &mutated); err != nil {
		t.Fatal(err)
	}
	if mutated.State != contract.NodeDead || mutated.ClaimsEnabled || mutated.IntentRevision != 1 ||
		mutated.IntentReason != "forbid work while dead" || mutated.IntentActor != "node-cli-operator" {
		t.Fatalf("dead-node intent mutation = %#v", mutated)
	}

	var conflictOut, conflictErr bytes.Buffer
	err := execute(ctx, harness.clients, true, []string{
		"nodes", "set-claims", deadNode.node.NodeID,
		"--claims-enabled=true", "--intent-revision=0", "--reason", "stale enable",
	}, &conflictOut, &conflictErr)
	var responseError *apiResponseError
	if !errors.As(err, &responseError) || responseError.APIError.Code != contract.ErrorConflict {
		t.Fatalf("stale intent mutation = %v, want typed conflict", err)
	}

	listed := runNodeCLI(t, ctx, harness.clients, false, "nodes", "list")
	for _, want := range []string{"dead-node", "dead", "false", "forbid work while dead", "node-cli-operator"} {
		if !strings.Contains(listed, want) {
			t.Fatalf("intent projection missing %q:\n%s", want, listed)
		}
	}
	for _, args := range [][]string{
		{"nodes", "set-claims", "dead-node", "--intent-revision=1", "--reason", "missing bit"},
		{"nodes", "set-claims", "dead-node", "--claims-enabled=true", "--reason", "missing revision"},
		{"nodes", "set-claims", "dead-node", "--claims-enabled=true", "--intent-revision=1"},
	} {
		var stdout, stderr bytes.Buffer
		if err := execute(ctx, harness.clients, false, args, &stdout, &stderr); err == nil {
			t.Fatalf("node intent command accepted incomplete arguments %q", args)
		}
	}
}

type nodeCLIClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *nodeCLIClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *nodeCLIClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(duration)
}

type nodeCLIHarness struct {
	clock   *nodeCLIClock
	store   *l1.Store
	clients *apiClients
}

type registeredCLINode struct {
	identity fabric.Identity
	node     l1.Node
	tag      string
}

func newNodeCLIHarness(t *testing.T) *nodeCLIHarness {
	t.Helper()
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "node-cli-control"})
	operatorFabric := network.NewFabric(fabric.Identity{
		NodeID: "node-cli-operator", Tags: []string{l1.DefaultClientPrincipalTag},
	})
	clock := &nodeCLIClock{now: time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)}
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	server, err := l1.NewServer(controlFabric, store, l1.ServerConfig{})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	listener, err := controlFabric.Listen("tcp", l3.DefaultL1Address)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := serveTestServer(ctx, func() error { return server.Serve(ctx, listener) })
	clients, err := newAPIClients(operatorFabric, l3.DefaultL1Address, l3.DefaultL3Address)
	if err != nil {
		cancel()
		_ = <-done
		_ = store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		clients.close()
		cancel()
		if err := <-done; err != nil {
			t.Errorf("L1 server: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close L1 store: %v", err)
		}
	})
	return &nodeCLIHarness{clock: clock, store: store, clients: clients}
}

func (h *nodeCLIHarness) registerNode(t *testing.T, nodeID, tag string, policy l1.NodePolicy) registeredCLINode {
	t.Helper()
	identity := fabric.Identity{NodeID: "fabric-" + nodeID}
	node, err := h.store.RegisterNode(context.Background(), identity, contract.NodeRegistration{
		NodeID: nodeID, BootSessionID: "boot-" + nodeID, RootInstanceID: "root-" + nodeID,
		OS: "linux", Architecture: "arm64", AgentVersion: "node-cli-test",
		Capabilities: map[string]bool{"process": true},
	}, policy, true)
	if err != nil {
		t.Fatal(err)
	}
	return registeredCLINode{identity: identity, node: node, tag: tag}
}

func (h *nodeCLIHarness) createAndClaim(t *testing.T, registered registeredCLINode, class, prefix string, index int) {
	t.Helper()
	key := prefix + "-" + string(rune('a'+index))
	if _, _, err := h.store.CreateJob(context.Background(), nodeCLIJobSpec(key, class, registered.tag)); err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(context.Background(), registered.identity.NodeID, registered.node.NodeID,
		registered.node.BootSessionID, class)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil {
		t.Fatalf("claim %s %d returned no work", class, index)
	}
}

func nodeCLIJobSpec(dispatchKey, class, tag string) contract.JobSpec {
	contents := []byte("#!/bin/sh\nexit 0\n")
	digest := sha256.Sum256(contents)
	spec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   dispatchKey,
		Kind:          "process",
		Class:         class,
		RoutingTags:   []string{tag},
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{
				InlineBase64: base64.StdEncoding.EncodeToString(contents),
				SHA256:       hex.EncodeToString(digest[:]),
				Interpreter:  []string{"/bin/sh"},
			},
			Argv:             []string{"wefty-node-cli-test"},
			WorkingDirectory: "/tmp",
		},
	}
	if class == contract.JobClassService {
		spec.Restart = contract.RestartAlways
	} else {
		spec.Execution.HandoffDirectory = "/tmp/wefty/node-cli-test"
	}
	return spec
}

func runNodeCLI(t *testing.T, ctx context.Context, clients *apiClients, jsonOutput bool, args ...string) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if err := execute(ctx, clients, jsonOutput, args, &stdout, &stderr); err != nil {
		t.Fatalf("execute %q: %v stderr=%s", args, err, stderr.String())
	}
	return stdout.String()
}

func nodeTableFields(t *testing.T, output, nodeID string) []string {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 6 && fields[0] == nodeID {
			return fields
		}
	}
	t.Fatalf("node %q has no table row:\n%s", nodeID, output)
	return nil
}
