package l3

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

type deadHostClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *deadHostClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *deadHostClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

// TestComputerPassRefusedOnceL1MarksItsHostDead runs L3 against a real L1
// over the wire (#623). L1 settles a dead host's owed revocations as moot
// because its passes are already refused; this pins that the refusal really
// happens at L3 on the pass's next use while the attempt's lease is still
// unexpired, and that a host that comes back by registering again does not
// revive the pass.
func TestComputerPassRefusedOnceL1MarksItsHostDead(t *testing.T) {
	ctx := context.Background()
	clock := &deadHostClock{now: time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)}
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	ledgerFabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})

	l1Store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{Clock: clock, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l1Store.Close() })
	l1Server, err := l1.NewServer(controlFabric, l1Store, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	l1Listener, err := controlFabric.Listen("tcp", DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	l1Client, err := NewL1Client(ledgerFabric, DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	l3Store, err := OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), StoreOptions{ComputerAuthorityInstanceID: "dead-host-test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l3Store.Close() })
	l3Server, err := NewServer(ledgerFabric, l3Store, ServerConfig{Jobs: l1Client, Logs: l1Client, ComputerGrants: l1Client})
	if err != nil {
		t.Fatal(err)
	}
	l3Listener, err := ledgerFabric.Listen("tcp", DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancel := context.WithCancel(ctx)
	served := []chan error{serveL1(serveCtx, l1Server, l1Listener), serveL3(serveCtx, l3Server, l3Listener)}
	t.Cleanup(func() {
		l1Client.CloseIdleConnections()
		cancel()
		for _, done := range served {
			if err := <-done; err != nil {
				t.Errorf("serve: %v", err)
			}
		}
	})

	const nodeID, identityNodeID, bootSessionID = "computer-node", "fabric-computer-node", "boot-computer-node"
	register := func() l1.Node {
		t.Helper()
		node, err := l1Store.RegisterNode(ctx, fabric.Identity{NodeID: identityNodeID}, contract.NodeRegistration{
			NodeID: nodeID, BootSessionID: bootSessionID, RootInstanceID: "root-" + nodeID,
			OS: "linux", Architecture: "amd64", AgentVersion: "test",
			Capabilities:       map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true},
			CapabilityRevision: 1, CapabilityObservedAt: clock.Now(), MissingCapabilities: []string{},
		}, l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + nodeID}, MaxServiceSlots: 1}, true)
		if err != nil {
			t.Fatal(err)
		}
		return node
	}
	register()
	admin := fabric.Identity{FabricID: "fabric-test", UserID: "admin", DeviceID: "device-1"}
	challenge, err := l1Store.InitiateAdminBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := l1Store.BootstrapAdmin(ctx, admin, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	memoryBytes := int64(64 << 20)
	computer, _, err := l1Store.CreateComputer(ctx, l1.CreateComputerRequest{Name: "dead-host", Actor: "operator",
		Spec: contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "computer:dead-host",
			Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways,
			RoutingTags: []string{contract.StableNodeTagPrefix + nodeID},
			Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
				Image:  contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest", Digest: &digest},
				Limits: &contract.OCILimits{MemoryBytes: &memoryBytes},
				Computer: &contract.OCIComputerSpec{DiskBytes: 1 << 30,
					Display: contract.OCIComputerDisplaySpec{Protocol: contract.ComputerDisplayProtocolRFBWebSocketV1}},
			}}}})
	if err != nil {
		t.Fatal(err)
	}
	enabled := true
	if _, _, _, err := l1Store.MutateComputerSubmission(ctx, admin, computer.ComputerID, l1.ComputerSubmissionRequest{
		PolicyRevision: policy.Revision, SubmitEnabled: &enabled, IdempotencyKey: "dead-host-enable",
	}); err != nil {
		t.Fatal(err)
	}
	claim, err := l1Store.ClaimJob(ctx, identityNodeID, nodeID, bootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim = (%#v, %v)", claim, err)
	}
	snapshot, err := l1Store.IssueComputerPolicySnapshot(ctx, identityNodeID, "fabric-test", nodeID, bootSessionID, time.Minute)
	if err != nil || snapshot == nil {
		t.Fatalf("snapshot = (%#v, %v)", snapshot, err)
	}
	if err := l1Store.AcknowledgeComputerPolicyInstallation(ctx, identityNodeID, l1.ComputerPolicyInstallAcknowledgement{
		NodeID: snapshot.NodeID, BootSessionID: snapshot.BootSessionID, PolicyGeneration: snapshot.PolicyGeneration,
		PolicyRevision: snapshot.PolicyRevision, SnapshotDigest: snapshot.SnapshotDigest,
	}); err != nil {
		t.Fatal(err)
	}

	participant := network.NewFabric(fabric.Identity{NodeID: identityNodeID})
	host := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return participant.Dial(ctx, network, DefaultL3Address)
	}}}
	t.Cleanup(host.CloseIdleConnections)
	status, _, body := doComputerHTTP(t, host, http.MethodPost, "/v1/computer-token/mint", "", "",
		ComputerTokenMintRequest{ComputerID: computer.ComputerID, ComputerAttemptID: claim.Lease.AttemptID})
	if status != http.StatusCreated {
		t.Fatalf("mint status=%d body=%s", status, body)
	}
	var grant ComputerTokenGrant
	if err := json.Unmarshal(body, &grant); err != nil {
		t.Fatal(err)
	}
	useAnswers := func(want int, when string) {
		t.Helper()
		status, _, body := doComputerHTTP(t, host, http.MethodGet, "/v1/computer/self", grant.Token, "", nil)
		if status != want {
			t.Fatalf("pass use %s: status=%d body=%s, want %d", when, status, body, want)
		}
	}
	useAnswers(http.StatusOK, "on a live host")

	// The workload keeps renewing its lease while the host's heartbeats stop,
	// until L1's reconcile marks the host dead.
	var lease l1.AttemptLease
	for elapsed := time.Duration(0); elapsed <= l1.DefaultNodeDeadAfter; elapsed += 40 * time.Second {
		clock.Advance(40 * time.Second)
		if lease, err = l1Store.RenewLease(ctx, identityNodeID, claim.Job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
			t.Fatalf("renew while heartbeats are silent: %v", err)
		}
	}
	if result, err := l1Store.Reconcile(ctx); err != nil || result.DeadNodes != 1 {
		t.Fatalf("reconcile = (%#v, %v), want the host marked dead", result, err)
	}
	if !lease.LeaseExpires.After(clock.Now()) {
		t.Fatalf("lease expires %s, not after now %s: the test needs a live lease", lease.LeaseExpires, clock.Now())
	}
	useAnswers(http.StatusUnauthorized, "after L1 marked its host dead")

	if node := register(); node.State != contract.NodeAlive {
		t.Fatalf("rejoined host state = %q, want alive", node.State)
	}
	useAnswers(http.StatusUnauthorized, "after its dead host registered again")
}
