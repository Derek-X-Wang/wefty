package agent

import (
	"context"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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

type submissionRepublishHarness struct {
	t              *testing.T
	store          *l1.Store
	admin          fabric.Identity
	computer       l1.Computer
	claim          *l1.Claim
	events         *orderedEvents
	minter         *revisionComputerTokenMinter
	installPolicy  func()
	serviceDone    chan error
	publishedFirst string
}

// startSubmissionRepublishHarness runs one Computer attempt against a real L1
// and waits for its first publication. gate, when set, runs before each ready
// publication reaches L1, and observe sees each ready publication's result.
func startSubmissionRepublishHarness(t *testing.T, name string, gate func(), observe func(error)) *submissionRepublishHarness {
	t.Helper()
	network, err := plain.NewNetworkWithID("plain-" + name)
	if err != nil {
		t.Fatal(err)
	}
	const nodeID, bootID, fabricNodeID = "republish-node", "republish-boot", "republish-agent"
	policy := l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + nodeID}, MaxOneshotSlots: 1, MaxServiceSlots: 1}
	store, stopServer := startFailureServerWithPolicies(t, network, nil, map[string]l1.NodePolicy{nodeID: policy})
	t.Cleanup(stopServer)
	admin := fabric.Identity{FabricID: "plain-" + name, UserID: "republish-admin", DeviceID: "republish-device"}
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
	t.Cleanup(client.Close)
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
	t.Cleanup(cache.Close)
	h := &submissionRepublishHarness{t: t, store: store, admin: admin, computer: computer, claim: claim,
		events: &orderedEvents{}, minter: &revisionComputerTokenMinter{}, serviceDone: make(chan error, 1)}
	// installPolicy stands in for the agent's policy watch delivering L1's
	// current snapshot after each change.
	h.installPolicy = func() {
		t.Helper()
		snapshot, err := store.IssueComputerPolicySnapshot(t.Context(), fabricNodeID, admin.FabricID, nodeID, bootID, l1.DefaultComputerPolicyFreshness)
		if err != nil || snapshot == nil {
			t.Fatalf("policy snapshot present=%t err=%v", snapshot != nil, err)
		}
		if _, err := cache.Install(*snapshot); err != nil {
			t.Fatal(err)
		}
	}
	h.installPolicy()

	backend := newComputerBackend(t, computerBackendOptions{})
	t.Cleanup(backend.Close)
	runtime := &recordingSubmissionRuntime{opaqueEndpointRuntime: &opaqueEndpointRuntime{release: make(chan struct{})}, events: h.events}
	ctx, cancel := context.WithCancel(t.Context())
	bridgeParticipant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "republish-bridge"})
	bridge := newComputerAttemptBridgeController(ctx, func(ctx context.Context, _ string, _ contract.ExecutionSpec, _ bool) (*workflowBridge, error) {
		return newComputerAttemptBridge(ctx, bridgeParticipant, "wefty://run-ledger", true)
	}, contract.JobKindOCI, claim.Job.Spec.Execution)
	t.Cleanup(func() {
		cancel()
		select {
		case <-h.serviceDone:
		case <-time.After(5 * time.Second): // Existing Computer-service shutdown bound.
			t.Error("Computer service did not stop")
		}
	})
	go func() {
		_, err := runComputerService(ctx, runtime, workloadrunner.Request{
			Started: func() {
				_, _ = client.StartAttempt(ctx, claim.Job.JobID, claim.Lease.AttemptID, l1.StartedRequest{FencingToken: claim.Lease.FencingToken})
			},
		}, nil, computerServiceConfig{
			publicationOperation: client.boundedContext,
			clock:                systemClock{}, fabric: agentFabric, authorizer: cache, auditor: client,
			computerTokens: h.minter, computerBridge: bridge,
			submission: ComputerSubmissionAuthority{ComputerID: computer.ComputerID, Enabled: claim.ComputerStorage.SubmitEnabled,
				SubmitIntentRevision: claim.ComputerStorage.SubmitIntentRevision, SubmitMaxInflight: claim.ComputerStorage.SubmitMaxInflight,
				SubmitPolicyRevision: claim.ComputerStorage.SubmitPolicyRevision},
			computerID: computer.ComputerID, jobID: claim.Job.JobID, attemptID: claim.Lease.AttemptID,
			storageID: computer.StorageID, storageGeneration: computer.StorageGeneration, fencingToken: claim.Lease.FencingToken,
			dial: func(ctx context.Context, _ string) (net.Conn, error) { return backend.dial(ctx) },
			publish: func(publishContext context.Context, ready bool, endpoint string, submitIntentRevision int64) error {
				request := l1.PublicationRequest{FencingToken: claim.Lease.FencingToken, Ready: &ready}
				if ready {
					request.DisplayEndpoint = &endpoint
					request.SubmitIntentRevision = &submitIntentRevision
					if gate != nil {
						gate()
					}
				}
				_, err := client.SetAttemptPublication(publishContext, claim.Job.JobID, claim.Lease.AttemptID, request)
				if ready && observe != nil {
					observe(err)
				}
				if err == nil {
					h.events.add(map[bool]string{true: "publish:true", false: "publish:false"}[ready])
				}
				return err
			},
		})
		h.serviceDone <- err
	}()
	h.publishedFirst = h.awaitScreen("initial publication", 5*time.Second)
	return h
}

// screen reports what `wefty takeover view` would find for this Computer,
// and proves the Computer is still on the claimed attempt.
func (h *submissionRepublishHarness) screen() *string {
	h.t.Helper()
	availability, err := h.store.GetComputerTakeoverAvailability(h.t.Context(), h.admin, h.computer.ComputerID)
	if err != nil {
		h.t.Fatal(err)
	}
	current, err := h.store.GetComputer(h.t.Context(), h.computer.ComputerID)
	if err != nil {
		h.t.Fatal(err)
	}
	if current.CurrentJob.CurrentAttemptID != h.claim.Lease.AttemptID {
		h.t.Fatalf("Computer moved to attempt %q; the fix must keep %q", current.CurrentJob.CurrentAttemptID, h.claim.Lease.AttemptID)
	}
	return availability.DisplayEndpoint
}

func (h *submissionRepublishHarness) awaitScreen(stage string, bound time.Duration) string {
	h.t.Helper()
	deadline := time.Now().Add(bound)
	for {
		if endpoint := h.screen(); endpoint != nil {
			return *endpoint
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("%s: take-over view still has no display endpoint after %s; events=%v", stage, bound, h.events.since(0))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// commit applies one submission change in L1 and proves L1 withdrew the
// screen with it. It also arms the minter for that revision.
func (h *submissionRepublishHarness) commit(step string, enabled bool, token string) l1.Computer {
	h.t.Helper()
	state, err := h.store.GetComputerSubmissionState(h.t.Context(), h.admin, h.computer.ComputerID)
	if err != nil {
		h.t.Fatal(err)
	}
	mutated, _, applied, err := h.store.MutateComputerSubmission(h.t.Context(), h.admin, h.computer.ComputerID, l1.ComputerSubmissionRequest{
		PolicyRevision: state.PolicyRevision, SubmitIntentRevision: state.SubmitIntentRevision,
		SubmitEnabled: &enabled, IdempotencyKey: "republish-" + step,
	})
	if err != nil || !applied {
		h.t.Fatalf("%s: applied=%v err=%v", step, applied, err)
	}
	if cleared := h.screen(); cleared != nil {
		h.t.Fatalf("%s: L1 kept readiness across a submission change: %q", step, *cleared)
	}
	h.minter.set(l3.ComputerTokenGrant{Token: token, ComputerID: h.computer.ComputerID, ComputerAttemptID: h.claim.Lease.AttemptID,
		SubmitIntentRevision: mutated.SubmitIntentRevision, SubmitMaxInflight: mutated.SubmitMaxInflight})
	return mutated
}

func (h *submissionRepublishHarness) assertRunning() {
	h.t.Helper()
	select {
	case err := <-h.serviceDone:
		h.t.Fatalf("submission changes ended the attempt: %v", err)
	default:
	}
}

// Run 4's shape against a real L1: a running Computer has published its
// screen, an administrator enables and then disables in-job submission, and
// after each change the same attempt's display endpoint is back for take-over
// once the agent has installed the new authority. L1 still clears readiness
// when it commits the change (wefty #242); the agent must earn it again rather
// than wait for a fresh attempt (wefty #559).
func TestComputerSubmissionChangeRepublishesSameAttemptDisplay(t *testing.T) {
	h := startSubmissionRepublishHarness(t, "submission-republish", nil, nil)
	for _, step := range []struct {
		name    string
		enabled bool
		token   string
	}{{name: "enable", enabled: true, token: "pass-after-enable"}, {name: "disable", enabled: false}} {
		h.commit(step.name, step.enabled, step.token)
		mark := h.events.mark()
		h.installPolicy()
		if restored := h.awaitScreen(step.name, computerSubmissionRepublishBound); restored != h.publishedFirst {
			t.Fatalf("%s: restored endpoint %q, want the same attempt's %q", step.name, restored, h.publishedFirst)
		}
		wantFiles := "files:removed"
		if step.enabled {
			wantFiles = "files:" + step.token
		}
		// L1 shows the screen as soon as the publication commits, but
		// publish:true is recorded only after SetAttemptPublication returns,
		// and awaitScreen polls L1 every 20 ms, so it can win that race.
		// Give the record its arrival under a 10 s watchdog before judging
		// the order.
		after := h.events.since(mark)
		deadline := time.Now().Add(10 * time.Second)
		for !slices.Contains(after, "publish:true") {
			if time.Now().After(deadline) {
				t.Fatalf("%s: publish:true was not recorded within 10s: %v", step.name, after)
			}
			time.Sleep(20 * time.Millisecond)
			after = h.events.since(mark)
		}
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
	h.assertRunning()
}

// The review race for #559, interleaved deterministically: the agent installs
// revision N and its readiness publication is in flight, revision N+1 commits
// in L1, and only then does N's publication arrive. L1 must refuse it, the
// screen must stay withdrawn, the attempt must keep running, and N+1's install
// must bring the screen back.
func TestComputerStaleSubmissionReadinessCannotRestoreDisplay(t *testing.T) {
	var armed atomic.Bool
	held := make(chan struct{})
	release := make(chan struct{})
	results := make(chan error, 8)
	gate := func() {
		if armed.CompareAndSwap(true, false) {
			close(held)
			<-release
		}
	}
	h := startSubmissionRepublishHarness(t, "stale-submission-readiness", gate, func(err error) { results <- err })
	drain := func() {
		for {
			select {
			case <-results:
			default:
				return
			}
		}
	}
	drain()

	h.commit("enable", true, "pass-revision-n")
	armed.Store(true)
	h.installPolicy()
	select {
	case <-held:
	case <-time.After(computerSubmissionRepublishBound):
		t.Fatal("revision N readiness publication was never attempted")
	}
	// Revision N+1 commits while N's readiness is still on its way to L1.
	h.commit("disable", false, "")
	close(release)
	select {
	case err := <-results:
		if protocolErrorCode(err) != contract.ErrorStalePolicyRevision {
			t.Fatalf("revision N readiness after N+1 committed = %v, want stale_policy_revision", err)
		}
	case <-time.After(computerSubmissionRepublishBound):
		t.Fatal("revision N readiness publication never returned")
	}
	if restored := h.screen(); restored != nil {
		t.Fatalf("readiness earned under revision N restored the screen after N+1 committed: %q", *restored)
	}
	h.assertRunning()

	h.installPolicy()
	if restored := h.awaitScreen("revision N+1 install", computerSubmissionRepublishBound); restored != h.publishedFirst {
		t.Fatalf("restored endpoint %q, want the same attempt's %q", restored, h.publishedFirst)
	}
	h.assertRunning()
}
