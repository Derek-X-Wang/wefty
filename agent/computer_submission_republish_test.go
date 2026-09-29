package agent

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// computerSubmissionRepublishBound is how long the agent may take to put the
// display back after it has installed a submission change: one mint and one
// publication round trip against a local L1. It is the existing Computer
// publication bound, far inside the minutes run 4 waited (wefty #559).
const computerSubmissionRepublishBound = 5 * time.Second

type revisionComputerTokenMinter struct {
	mu    sync.Mutex
	grant l3.ComputerTokenGrant
}

func (minter *revisionComputerTokenMinter) set(grant l3.ComputerTokenGrant) {
	minter.mu.Lock()
	minter.grant = grant
	minter.mu.Unlock()
}

func (minter *revisionComputerTokenMinter) MintComputerToken(context.Context, l3.ComputerTokenMintRequest) (l3.ComputerTokenGrant, error) {
	minter.mu.Lock()
	defer minter.mu.Unlock()
	return minter.grant, nil
}

// recordingSubmissionRuntime notes each submission-file write so the test can
// prove the display came back only after the new authority was in place.
type recordingSubmissionRuntime struct {
	*opaqueEndpointRuntime
	events *orderedEvents
}

func (runtime *recordingSubmissionRuntime) SetComputerSubmission(_ context.Context, _ workloadrunner.AttemptAuthority, token, _ string) error {
	if token == "" {
		runtime.events.add("files:removed")
	} else {
		runtime.events.add("files:" + token)
	}
	return nil
}

type orderedEvents struct {
	mu     sync.Mutex
	events []string
}

func (events *orderedEvents) add(event string) {
	events.mu.Lock()
	events.events = append(events.events, event)
	events.mu.Unlock()
}

func (events *orderedEvents) since(mark int) []string {
	events.mu.Lock()
	defer events.mu.Unlock()
	return append([]string(nil), events.events[mark:]...)
}

func (events *orderedEvents) mark() int {
	events.mu.Lock()
	defer events.mu.Unlock()
	return len(events.events)
}

// Run 4's shape against a real L1: a running Computer has published its
// screen, an administrator enables and then disables in-job submission, and
// after each change the same attempt's display endpoint is back for take-over
// once the agent has installed the new authority. L1 still clears readiness
// when it commits the change (wefty #242); the agent must earn it again rather
// than wait for a fresh attempt (wefty #559).
func TestComputerSubmissionChangeRepublishesSameAttemptDisplay(t *testing.T) {
	network, err := plain.NewNetworkWithID("plain-submission-republish")
	if err != nil {
		t.Fatal(err)
	}
	const nodeID, bootID, fabricNodeID = "republish-node", "republish-boot", "republish-agent"
	policy := l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + nodeID}, MaxOneshotSlots: 1, MaxServiceSlots: 1}
	store, stopServer := startFailureServerWithPolicies(t, network, nil, map[string]l1.NodePolicy{nodeID: policy})
	defer stopServer()
	admin := fabric.Identity{FabricID: "plain-submission-republish", UserID: "republish-admin", DeviceID: "republish-device"}
	challenge, err := store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.BootstrapAdmin(t.Context(), admin, challenge.Nonce); err != nil {
		t.Fatal(err)
	}
	agentFabric := network.NewFabric(fabric.Identity{NodeID: fabricNodeID, Tags: []string{l1.DefaultAgentPrincipalTag}})
	client, err := newClient(agentFabric, "wefty://control-plane", DefaultOperationTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Register(t.Context(), contract.NodeRegistration{
		NodeID: nodeID, BootSessionID: bootID, RootInstanceID: "republish-root", OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities:       map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true},
		CapabilityRevision: 1, CapabilityObservedAt: time.Now(), MissingCapabilities: []string{},
	}); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("b", 64)
	memoryBytes := int64(64 << 20)
	computer, _, err := store.CreateComputer(t.Context(), l1.CreateComputerRequest{
		Name: "submission-republish", Actor: "operator",
		Spec: contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "computer:submission-republish",
			Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways,
			RoutingTags: []string{contract.StableNodeTagPrefix + nodeID},
			Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
				Image:    contract.OCIImageSpec{Reference: "ghcr.io/example/computer:v1", Digest: &digest},
				Limits:   &contract.OCILimits{MemoryBytes: &memoryBytes},
				Computer: &contract.OCIComputerSpec{Display: contract.OCIComputerDisplaySpec{Protocol: contract.ComputerDisplayProtocolRFBWebSocketV1}, DiskBytes: 1 << 30},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := client.Claim(t.Context(), nodeID, bootID, contract.JobClassService)
	if err != nil || claim == nil || claim.ComputerStorage == nil {
		t.Fatalf("claim=%v err=%v", claim != nil, err)
	}
	if _, err := client.ObserveAttemptImage(t.Context(), claim.Job.JobID, claim.Lease.AttemptID, l1.ImageObservationRequest{
		FencingToken: claim.Lease.FencingToken, SubmittedReference: "ghcr.io/example/computer:v1",
		TopLevelDigest: digest, TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json", PlatformManifestDigest: digest,
		Platform: l1.OCIPlatform{OS: "linux", Architecture: "amd64"}, RuntimeHandler: "io.containerd.runc.v2", Snapshotter: "overlayfs",
	}); err != nil {
		t.Fatal(err)
	}
	cache := NewComputerPolicyCache(systemClock{}, nodeID, bootID)
	defer cache.Close()
	// installPolicy stands in for the agent's policy watch delivering L1's
	// current snapshot after each change.
	installPolicy := func() {
		t.Helper()
		snapshot, err := store.IssueComputerPolicySnapshot(t.Context(), fabricNodeID, admin.FabricID, nodeID, bootID, l1.DefaultComputerPolicyFreshness)
		if err != nil || snapshot == nil {
			t.Fatalf("policy snapshot present=%t err=%v", snapshot != nil, err)
		}
		if _, err := cache.Install(*snapshot); err != nil {
			t.Fatal(err)
		}
	}
	installPolicy()

	backend := newComputerBackend(t, computerBackendOptions{})
	defer backend.Close()
	events := &orderedEvents{}
	runtime := &recordingSubmissionRuntime{opaqueEndpointRuntime: &opaqueEndpointRuntime{release: make(chan struct{})}, events: events}
	minter := &revisionComputerTokenMinter{}
	ctx, cancel := context.WithCancel(t.Context())
	bridgeParticipant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "republish-bridge"})
	bridge := newComputerAttemptBridgeController(ctx, func(ctx context.Context, _ string, _ contract.ExecutionSpec, _ bool) (*workflowBridge, error) {
		return newComputerAttemptBridge(ctx, bridgeParticipant, "wefty://run-ledger", true)
	}, contract.JobKindOCI, claim.Job.Spec.Execution)
	serviceDone := make(chan error, 1)
	defer func() {
		cancel()
		select {
		case <-serviceDone:
		case <-time.After(5 * time.Second): // Existing Computer-service shutdown bound.
			t.Error("Computer service did not stop")
		}
	}()
	go func() {
		_, err := runComputerService(ctx, runtime, workloadrunner.Request{
			Started: func() {
				_, _ = client.StartAttempt(ctx, claim.Job.JobID, claim.Lease.AttemptID, l1.StartedRequest{FencingToken: claim.Lease.FencingToken})
			},
		}, nil, computerServiceConfig{
			publicationOperation: client.boundedContext,
			clock:                systemClock{}, fabric: agentFabric, authorizer: cache, auditor: client,
			computerTokens: minter, computerBridge: bridge,
			submission: ComputerSubmissionAuthority{ComputerID: computer.ComputerID, Enabled: claim.ComputerStorage.SubmitEnabled,
				SubmitIntentRevision: claim.ComputerStorage.SubmitIntentRevision, SubmitMaxInflight: claim.ComputerStorage.SubmitMaxInflight,
				SubmitPolicyRevision: claim.ComputerStorage.SubmitPolicyRevision},
			computerID: computer.ComputerID, jobID: claim.Job.JobID, attemptID: claim.Lease.AttemptID,
			storageID: computer.StorageID, storageGeneration: computer.StorageGeneration, fencingToken: claim.Lease.FencingToken,
			dial: func(ctx context.Context, _ string) (net.Conn, error) { return backend.dial(ctx) },
			publish: func(publishContext context.Context, ready bool, endpoint string) error {
				request := l1.PublicationRequest{FencingToken: claim.Lease.FencingToken, Ready: &ready}
				if ready {
					request.DisplayEndpoint = &endpoint
				}
				_, err := client.SetAttemptPublication(publishContext, claim.Job.JobID, claim.Lease.AttemptID, request)
				if err == nil {
					events.add(map[bool]string{true: "publish:true", false: "publish:false"}[ready])
				}
				return err
			},
		})
		serviceDone <- err
	}()

	// screen reports what `wefty takeover view` would find for this Computer,
	// and proves any endpoint belongs to the claimed attempt.
	screen := func() *string {
		t.Helper()
		availability, err := store.GetComputerTakeoverAvailability(t.Context(), admin, computer.ComputerID)
		if err != nil {
			t.Fatal(err)
		}
		current, err := store.GetComputer(t.Context(), computer.ComputerID)
		if err != nil {
			t.Fatal(err)
		}
		if current.CurrentJob.CurrentAttemptID != claim.Lease.AttemptID {
			t.Fatalf("Computer moved to attempt %q; the fix must keep %q", current.CurrentJob.CurrentAttemptID, claim.Lease.AttemptID)
		}
		return availability.DisplayEndpoint
	}
	awaitScreen := func(stage string, bound time.Duration) string {
		t.Helper()
		deadline := time.Now().Add(bound)
		for {
			if endpoint := screen(); endpoint != nil {
				return *endpoint
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: take-over view still has no display endpoint after %s; events=%v", stage, bound, events.since(0))
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	endpoint := awaitScreen("initial publication", 5*time.Second)

	for _, step := range []struct {
		name    string
		enabled bool
		token   string
	}{{name: "enable", enabled: true, token: "pass-after-enable"}, {name: "disable", enabled: false}} {
		state, err := store.GetComputerSubmissionState(t.Context(), admin, computer.ComputerID)
		if err != nil {
			t.Fatal(err)
		}
		enabled := step.enabled
		mutated, _, applied, err := store.MutateComputerSubmission(t.Context(), admin, computer.ComputerID, l1.ComputerSubmissionRequest{
			PolicyRevision: state.PolicyRevision, SubmitIntentRevision: state.SubmitIntentRevision,
			SubmitEnabled: &enabled, IdempotencyKey: "republish-" + step.name,
		})
		if err != nil || !applied {
			t.Fatalf("%s: applied=%v err=%v", step.name, applied, err)
		}
		// L1 still withdraws readiness when it commits the change.
		if cleared := screen(); cleared != nil {
			t.Fatalf("%s: L1 kept readiness across a submission change: %q", step.name, *cleared)
		}
		mark := events.mark()
		minter.set(l3.ComputerTokenGrant{Token: step.token, ComputerID: computer.ComputerID, ComputerAttemptID: claim.Lease.AttemptID,
			SubmitIntentRevision: mutated.SubmitIntentRevision, SubmitMaxInflight: mutated.SubmitMaxInflight})
		installPolicy()
		if restored := awaitScreen(step.name, computerSubmissionRepublishBound); restored != endpoint {
			t.Fatalf("%s: restored endpoint %q, want the same attempt's %q", step.name, restored, endpoint)
		}
		wantFiles := "files:removed"
		if step.enabled {
			wantFiles = "files:" + step.token
		}
		after := events.since(mark)
		filesAt, publishAt := -1, -1
		for index, event := range after {
			if event == wantFiles {
				filesAt = index
			}
			if event == "publish:true" && publishAt < 0 {
				publishAt = index
			}
		}
		if filesAt < 0 || publishAt < filesAt {
			t.Fatalf("%s: display republished before the new submission authority was installed: %v", step.name, after)
		}
	}
	select {
	case err := <-serviceDone:
		t.Fatalf("submission changes ended the attempt: %v", err)
	default:
	}
}
