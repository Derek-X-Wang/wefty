package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/takeover"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func TestComputerPublicationFinalWithdrawal(t *testing.T) {
	for _, mode := range []string{"commits_after_payload_cancel", "transient_then_success", "transient_deadline", "earlier_caller_deadline", "authority_loss"} {
		t.Run(mode, func(t *testing.T) { assertComputerPublicationFinalWithdrawal(t, mode) })
	}
}

// Hold the final mutation until execution cancellation is observed. It must
// retain one independent caller-owned deadline through real L1 publication.
func assertComputerPublicationFinalWithdrawal(t *testing.T, mode string) {
	t.Helper()
	trace := func(event string, detail any) {
		t.Logf("publication-loss-probe at=%s pid=%d pgid=%d event=%s detail=%v",
			time.Now().UTC().Format(time.RFC3339Nano), os.Getpid(), syscall.Getpgrp(), event, detail)
	}
	network, err := plain.NewNetworkWithID("plain-publication-loss-probe")
	if err != nil {
		t.Fatal(err)
	}
	const nodeID, bootID, fabricNodeID = "publication-node", "publication-boot", "publication-agent"
	policy := l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + nodeID}, MaxOneshotSlots: 1, MaxServiceSlots: 1}
	store, stopServer := startFailureServerWithPolicies(t, network, nil, map[string]l1.NodePolicy{nodeID: policy})
	defer stopServer()
	identity := fabric.Identity{FabricID: "plain-publication-loss-probe", UserID: "probe-admin", DeviceID: "probe-device"}
	challenge, err := store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.BootstrapAdmin(t.Context(), identity, challenge.Nonce); err != nil {
		t.Fatal(err)
	}
	agentFabric := network.NewFabric(fabric.Identity{NodeID: fabricNodeID, Tags: []string{l1.DefaultAgentPrincipalTag}})
	operationTimeout := DefaultOperationTimeout
	if mode == "transient_deadline" {
		operationTimeout = 3 * DefaultPublicationRetryInterval
	}
	client, err := newClient(agentFabric, "wefty://control-plane", operationTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Register(t.Context(), contract.NodeRegistration{
		NodeID: nodeID, BootSessionID: bootID, RootInstanceID: "publication-root", OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities:       map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true},
		CapabilityRevision: 1, CapabilityObservedAt: time.Now(), MissingCapabilities: []string{},
	}); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	memoryBytes := int64(64 << 20)
	computer, _, err := store.CreateComputer(t.Context(), l1.CreateComputerRequest{
		Name: "publication-loss", Actor: "operator",
		Spec: contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "computer:publication-loss",
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
	if err != nil || claim == nil {
		t.Fatalf("claim=%v err=%v", claim != nil, err)
	}
	if _, err := client.ObserveAttemptImage(t.Context(), claim.Job.JobID, claim.Lease.AttemptID, l1.ImageObservationRequest{
		FencingToken: claim.Lease.FencingToken, SubmittedReference: "ghcr.io/example/computer:v1",
		TopLevelDigest: digest, TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json", PlatformManifestDigest: digest,
		Platform: l1.OCIPlatform{OS: "linux", Architecture: "amd64"}, RuntimeHandler: "io.containerd.runc.v2", Snapshotter: "overlayfs",
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.IssueComputerPolicySnapshot(t.Context(), fabricNodeID, identity.FabricID, nodeID, bootID, l1.DefaultComputerPolicyFreshness)
	if err != nil || snapshot == nil {
		t.Fatalf("policy snapshot present=%t err=%v", snapshot != nil, err)
	}
	cache := NewComputerPolicyCache(systemClock{}, nodeID, bootID)
	defer cache.Close()
	if _, err := cache.Install(*snapshot); err != nil {
		t.Fatal(err)
	}
	backend := newComputerBackend(t, computerBackendOptions{})
	defer backend.Close()
	ctx, cancel := context.WithCancel(t.Context())
	var earlierDeadline time.Time
	if mode == "earlier_caller_deadline" {
		cancel()
		earlierDeadline = time.Now().Add(3 * DefaultPublicationRetryInterval)
		ctx, cancel = context.WithDeadline(t.Context(), earlierDeadline)
	}
	runtime := &publicationLossProbeRuntime{opaqueEndpointRuntime: &opaqueEndpointRuntime{}, lost: make(chan struct{}), canceled: make(chan struct{})}
	releaseFalse := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFalse) }) }
	serviceDone := make(chan struct{})
	var serviceErr error
	var serviceResult contract.ProcessResult
	startedResult := make(chan error, 1)
	published := make(chan string, 1)
	falseResult := make(chan error, 1)
	falseCause := make(chan error, 1)
	falseEntered := make(chan struct{})
	var falseEnteredOnce sync.Once
	var operationCalls atomic.Int32
	var operationDeadline time.Time
	var falseDeadlines []time.Time
	var falseCalls int
	var trueCalls int
	closed := make(chan struct{})
	privateFabric := &publicationLossProbeFabric{Fabric: agentFabric, closed: closed, trace: trace}
	var view *takeover.Session
	// Registered before work starts: every failure releases gates and joins the
	// service before the backend, policy cache, client, and SQLite store close.
	defer func() {
		release()
		runtime.lose()
		cancel()
		if view != nil {
			_ = view.Close()
		}
		select {
		case <-serviceDone:
			trace("cleanup_joined", nil)
		case <-time.After(5 * time.Second): // Existing Computer-service shutdown bound.
			t.Error("probe service did not join within existing shutdown bound")
		}
	}()
	go func() {
		serviceResult, serviceErr = runComputerService(ctx, runtime, workloadrunner.Request{
			Started: func() {
				_, err := client.StartAttempt(ctx, claim.Job.JobID, claim.Lease.AttemptID, l1.StartedRequest{FencingToken: claim.Lease.FencingToken})
				startedResult <- err
			},
		}, nil, computerServiceConfig{
			publicationOperation: func(parent context.Context) (context.Context, context.CancelFunc) {
				operationCalls.Add(1)
				operationContext, cancelOperation := client.boundedContext(parent)
				operationDeadline, _ = operationContext.Deadline()
				trace("operation_anchored", operationDeadline.Format(time.RFC3339Nano))
				return operationContext, cancelOperation
			},
			clock: systemClock{}, fabric: privateFabric, authorizer: cache, auditor: client,
			computerID: computer.ComputerID, jobID: claim.Job.JobID, attemptID: claim.Lease.AttemptID,
			storageID: computer.StorageID, storageGeneration: computer.StorageGeneration, fencingToken: claim.Lease.FencingToken,
			dial: func(ctx context.Context, _ string) (net.Conn, error) { return backend.dial(ctx) },
			publish: func(publishContext context.Context, ready bool, endpoint string) error {
				request := l1.PublicationRequest{FencingToken: claim.Lease.FencingToken, Ready: &ready}
				if ready {
					trueCalls++
					request.DisplayEndpoint = &endpoint
				} else {
					falseCalls++
					deadline, ok := publishContext.Deadline()
					if !ok {
						return errors.New("final publication omitted its operation deadline")
					}
					falseDeadlines = append(falseDeadlines, deadline)
					falseEnteredOnce.Do(func() { close(falseEntered) })
					trace("false_entered_before_real_client", context.Cause(publishContext))
					select {
					case <-publishContext.Done():
					case <-releaseFalse:
					}
					if falseCalls == 1 {
						falseCause <- context.Cause(publishContext)
					}
					trace("false_released_before_real_client", context.Cause(publishContext))
					if mode == "transient_deadline" || mode == "earlier_caller_deadline" || (mode == "transient_then_success" && falseCalls == 1) {
						return &ProtocolError{StatusCode: http.StatusServiceUnavailable, APIError: contract.APIError{
							Code: contract.ErrorInternal, Message: "controlled transient final publication failure", Retryable: true,
						}}
					}
					if mode == "authority_loss" {
						return &ProtocolError{StatusCode: http.StatusConflict, APIError: contract.APIError{
							Code: contract.ErrorLeaseExpired, Message: "controlled final publication authority loss",
						}}
					}
				}
				_, err := client.SetAttemptPublication(publishContext, claim.Job.JobID, claim.Lease.AttemptID, request)
				trace(fmt.Sprintf("publication_%t_real_client_returned", ready), err)
				if ready && err == nil {
					published <- endpoint
				} else if !ready {
					falseResult <- err
				}
				return err
			},
		})
		trace("service_returned", serviceErr)
		close(serviceDone)
	}()
	var endpoint string
	select {
	case endpoint = <-published:
	case <-time.After(5 * time.Second): // Existing Computer-service publication bound.
		t.Fatal("probe did not publish")
	}
	if err := <-startedResult; err != nil {
		t.Fatalf("real L1 Started: %v", err)
	}
	personFabric := network.NewFabric(identity)
	view, err = takeover.OpenAtPolicyRevision(t.Context(), personFabric, endpoint, snapshot.PolicyRevision)
	if err != nil {
		t.Fatalf("establish admitted view: %v", err)
	}
	trace("admitted_view", fmt.Sprintf("computer=%s job=%s attempt=%s storage=%s generation=%d endpoint=%s lease_ttl=%s",
		computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, computer.StorageID, computer.StorageGeneration, endpoint, claim.Lease.LeaseTTL))
	availability, err := store.GetComputerTakeoverAvailability(t.Context(), identity, computer.ComputerID)
	if err != nil || availability.DisplayEndpoint == nil || *availability.DisplayEndpoint != endpoint {
		t.Fatalf("initial durable endpoint=%v err=%v", availability.DisplayEndpoint, err)
	}
	trace("drive_typed_runtime_loss", nil)
	runtime.lose()
	select {
	case <-falseEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("final false operation was not attempted")
	}
	select {
	case <-runtime.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("payload execution was not canceled before publication drain")
	}
	select {
	case <-serviceDone:
		t.Fatal("service returned while final false was still held")
	default:
	}
	release()
	select {
	case <-serviceDone:
	case <-time.After(5 * time.Second): // Do not extend the existing teardown observation.
		t.Fatal("final publication drain exceeded the existing shutdown observation")
	}
	select {
	case <-closed:
	default:
		t.Fatal("service returned before listener close was observed")
	}
	if serviceResult.RuntimeFailure == nil || serviceResult.RuntimeFailure.Code != contract.RuntimeFailureUnavailable {
		t.Fatalf("typed loss not retained: result=%+v err=%v", serviceResult, serviceErr)
	}
	if operationCalls.Load() != 1 || trueCalls != 1 || len(falseDeadlines) == 0 {
		t.Fatalf("operation anchors=%d true calls=%d false calls=%d", operationCalls.Load(), trueCalls, falseCalls)
	}
	for _, deadline := range falseDeadlines {
		if !deadline.Equal(operationDeadline) {
			t.Fatalf("final publication deadline renewed: got %s want %s", deadline, operationDeadline)
		}
	}
	if !earlierDeadline.IsZero() && !operationDeadline.Equal(earlierDeadline) {
		t.Fatalf("earlier caller deadline replaced: got %s want %s", operationDeadline, earlierDeadline)
	}
	var cause error
	select {
	case cause = <-falseCause:
	case <-time.After(5 * time.Second):
		t.Fatal("final false callback did not record its cancellation cause")
	}
	if cause != nil {
		t.Fatalf("payload cancellation canceled the independent publication operation: %v", cause)
	}
	switch mode {
	case "commits_after_payload_cancel", "transient_then_success":
		if clearErr := <-falseResult; clearErr != nil || strings.Contains(fmt.Sprint(serviceErr), "withdraw Computer publication") {
			t.Fatalf("healthy final clear failed: clear=%v service=%v", clearErr, serviceErr)
		}
	case "transient_deadline", "earlier_caller_deadline":
		if !errors.Is(serviceErr, context.DeadlineExceeded) || !strings.Contains(fmt.Sprint(serviceErr), "withdraw Computer publication") || falseCalls < 2 {
			t.Fatalf("deadline failure missing or no retry: calls=%d err=%v", falseCalls, serviceErr)
		}
	case "authority_loss":
		if protocolErrorCode(serviceErr) != contract.ErrorLeaseExpired || !strings.Contains(fmt.Sprint(serviceErr), "withdraw Computer publication") || falseCalls != 1 {
			t.Fatalf("authority failure not immediate/observable: calls=%d err=%v", falseCalls, serviceErr)
		}
	}
	availability, err = store.GetComputerTakeoverAvailability(t.Context(), identity, computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	retained := availability.DisplayEndpoint != nil && *availability.DisplayEndpoint == endpoint
	trace("after_return_durable_discovery", fmt.Sprintf("old_endpoint_retained=%t endpoint=%s", retained, endpoint))
	dialContext, cancelDial := context.WithTimeout(t.Context(), DefaultOperationTimeout)
	_, dialErr := takeover.OpenAtPolicyRevision(dialContext, personFabric, endpoint, snapshot.PolicyRevision)
	cancelDial()
	trace("after_return_real_view_dial", dialErr)
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		t.Fatalf("closed endpoint dial=%v, want connection refused", dialErr)
	}
	// Negative controls inject a typed refusal at the final publication seam.
	// The real store remains available and can clear the same live authority.
	ready := false
	_, controlErr := client.SetAttemptPublication(t.Context(), claim.Job.JobID, claim.Lease.AttemptID,
		l1.PublicationRequest{FencingToken: claim.Lease.FencingToken, Ready: &ready})
	controlAvailability, observationErr := store.GetComputerTakeoverAvailability(t.Context(), identity, computer.ComputerID)
	trace("same_authority_live_context_control", fmt.Sprintf("clear_error=%v discovery_error=%v endpoint_nil=%t", controlErr, observationErr, controlAvailability.DisplayEndpoint == nil))
	if controlErr != nil || observationErr != nil || controlAvailability.DisplayEndpoint != nil {
		t.Fatal("live-context control failed; no isolated cancellation attribution")
	}
	successfulClear := mode == "commits_after_payload_cancel" || mode == "transient_then_success"
	if mode == "transient_then_success" && falseCalls != 2 {
		t.Errorf("transient success calls=%d, want 2", falseCalls)
	}
	if successfulClear && retained {
		t.Error("Computer teardown returned before real L1 discovery acknowledged the final clear")
	}
	if !successfulClear && !retained {
		t.Error("negative control unexpectedly committed its injected failed publication")
	}
}

type publicationLossProbeRuntime struct {
	*opaqueEndpointRuntime
	lost     chan struct{}
	canceled chan struct{}
	once     sync.Once
}

func (runtime *publicationLossProbeRuntime) lose() { runtime.once.Do(func() { close(runtime.lost) }) }

func (runtime *publicationLossProbeRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	context.AfterFunc(ctx, func() { close(runtime.canceled) })
	request.OCIStartedAt(time.Now().UTC())
	request.Started()
	select {
	case <-ctx.Done():
		return workloadrunner.Result{}, ctx.Err()
	case <-runtime.lost:
		loss := &ocihelper.RuntimeLossError{Cause: errors.New("controlled helper generation loss")}
		return workloadrunner.Result{Outcome: contract.ProcessResult{RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: loss.Error()}}}, loss
	}
}

type publicationLossProbeFabric struct {
	fabric.Fabric
	closed chan struct{}
	trace  func(string, any)
}

func (f *publicationLossProbeFabric) Listen(network, address string) (net.Listener, error) {
	listener, err := f.Fabric.Listen(network, address)
	if err != nil {
		return nil, err
	}
	return &publicationLossProbeListener{Listener: listener, owner: f}, nil
}

type publicationLossProbeListener struct {
	net.Listener
	owner *publicationLossProbeFabric
	once  sync.Once
}

func (listener *publicationLossProbeListener) Close() error {
	err := listener.Listener.Close()
	listener.once.Do(func() {
		listener.owner.trace("listener_closed", err)
		close(listener.owner.closed)
	})
	return err
}
