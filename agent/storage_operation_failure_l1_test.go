//go:build darwin || linux

package agent

import (
	"context"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

// TestBackupWhoseHelperAlwaysFailsEndsByTheBudget runs the agent's Backup
// controller against a real L1 store and server (#1608). The helper refuses
// every creation; the agent reports each refusal and waits L1's
// retry_after_seconds; L1 ends the Backup once 5 counted failures and 10
// minutes on its own clock are both spent. Only the helper is stubbed.
func TestBackupWhoseHelperAlwaysFailsEndsByTheBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	// Both sides read one moved clock: L1 measures the window, the agent
	// waits the retry_after L1 answered.
	var offset atomic.Int64
	clock := func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{
		Clock: l1.ClockFunc(clock), ComputerBackupCap: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	policy := l1.DefaultNodePolicy("linux")
	policy.Tags = append(policy.Tags, contract.StableNodeTagPrefix+"budget-node")
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{
		NodePolicies: map[string]l1.NodePolicy{"budget-node": policy},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: server.Handler()}
	serveDone := make(chan error, 1)
	go func() { serveDone <- httpServer.Serve(listener) }()
	defer func() {
		_ = httpServer.Close()
		if err := <-serveDone; err != nil && err != http.ErrServerClosed {
			t.Errorf("serve: %v", err)
		}
	}()
	client, err := NewClient(network.NewFabric(fabric.Identity{
		NodeID: "fabric-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Register(ctx, contract.NodeRegistration{
		NodeID: "budget-node", BootSessionID: "budget-boot", RootInstanceID: "budget-root",
		OS: "linux", Architecture: "arm64", AgentVersion: "test", CapabilityRevision: 1,
		CapabilityObservedAt: time.Now(), MissingCapabilities: []string{},
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": true,
			"runtime_handler:io.containerd.runc.v2": true, "cgroup_v2": true, "computer": true},
	}); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:0000000000000000000000000000000000000000000000000000000000000001"
	memoryBytes := int64(64 << 20)
	spec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "computer:budget", Kind: contract.JobKindOCI,
		Class: contract.JobClassService, Restart: contract.RestartAlways,
		RoutingTags: []string{contract.StableNodeTagPrefix + "budget-node"},
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image:  contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest", Digest: &digest},
			Limits: &contract.OCILimits{MemoryBytes: &memoryBytes},
			Computer: &contract.OCIComputerSpec{
				Display:   contract.OCIComputerDisplaySpec{Protocol: contract.ComputerDisplayProtocolRFBWebSocketV1},
				DiskBytes: 1 << 30,
			}}},
	}
	if err := contract.ValidateJobSpec(&spec); err != nil {
		t.Fatal(err)
	}
	computer, _, err := store.CreateComputer(ctx, l1.CreateComputerRequest{Name: "budget", Spec: spec, Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := client.Claim(ctx, "budget-node", "budget-boot", contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != computer.CurrentJobID {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	// A cold Backup of a running Computer: the attempt runs, then quiesces.
	if _, err := store.ObserveAttemptImage(ctx, "fabric-agent", claim.Job.JobID, claim.Lease.AttemptID,
		l1.ImageObservationRequest{FencingToken: claim.Lease.FencingToken,
			SubmittedReference: "ghcr.io/example/tool:latest", TopLevelDigest: digest,
			TopLevelMediaType:      "application/vnd.oci.image.index.v1+json",
			PlatformManifestDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Platform:               l1.OCIPlatform{OS: "linux", Architecture: "arm64", Variant: "v8"},
			RuntimeHandler:         "io.containerd.runc.v2", Snapshotter: "overlayfs"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartAttempt(ctx, "fabric-agent", claim.Job.JobID, claim.Lease.AttemptID,
		l1.StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.BeginComputerBackup(ctx, computer.ComputerID, l1.ComputerBackupCreateRequest{
		ComputerMutationPrecondition: l1.ComputerMutationPrecondition{IntentRevision: computer.IntentRevision,
			StorageID: computer.StorageID, StorageGeneration: computer.StorageGeneration, Actor: "operator"},
		IdempotencyKey: "budget-backup", AllowPowerOff: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteAttempt(ctx, "fabric-agent", claim.Job.JobID, claim.Lease.AttemptID,
		l1.CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "budget-quiescence",
			Result:                    l1.ProcessResult{OutputError: "quiesced for cold Backup"},
			RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt}); err != nil {
		t.Fatal(err)
	}
	directives, err := store.ListNodeComputerBackupDirectives(ctx, "fabric-agent", "budget-node", "budget-boot")
	if err != nil || len(directives) != 1 {
		t.Fatalf("Backup directives = %#v err=%v", directives, err)
	}
	directive := directives[0]

	backups := newBackupController(client, nil, failingBackupper{err: helperEngineFailure()}, "budget-node",
		"budget-boot", "budget-root", t.Logf)
	backups.failures.now = clock
	for heartbeat := 0; heartbeat < 200; heartbeat++ {
		listed, err := store.ListNodeComputerBackupDirectives(ctx, "fabric-agent", "budget-node", "budget-boot")
		if err != nil {
			t.Fatal(err)
		}
		if len(listed) == 0 {
			break
		}
		backups.enqueueCreate(ctx, directive, nil)
		backups.wait()
		offset.Add(int64(DefaultHeartbeatInterval))
	}
	observed, err := store.GetComputer(ctx, computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	outcome := observed.LastBackupOperation
	if outcome == nil || outcome.Status != "failed" || outcome.FailureCode != l1.ComputerBackupFailureEngine ||
		outcome.Failure == nil || outcome.Failure.FailedAttempts < l1.MaximumStorageOperationFailures ||
		outcome.Failure.FirstFailedAt == nil || outcome.Failure.LastFailedAt.Sub(*outcome.Failure.FirstFailedAt) < l1.StorageOperationFailureWindow {
		t.Fatalf("Backup outcome = %#v failure=%#v, want failed by the budget", outcome, outcome.Failure)
	}
	if observed.ReconfigurationPhase != l1.ComputerReconfigurationStable {
		t.Fatalf("Computer phase = %q, want stable", observed.ReconfigurationPhase)
	}
	// Paced by L1, the helper saw one creation per retry_after, not one per
	// heartbeat: at most a handful past the 5 the budget needs.
	if attempts := outcome.Failure.FailedAttempts; attempts > l1.MaximumStorageOperationFailures+3 {
		t.Fatalf("failed attempts = %d: the agent did not honour retry_after", attempts)
	}
}
