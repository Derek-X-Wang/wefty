// Package oci implements the agent-side kind=oci workload adapter. Containerd
// remains entirely behind the privileged ocihelper session.
package oci

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// SessionSource returns the currently boot-barrier-pinned helper session.
type SessionSource interface {
	Session() (*ocihelper.Session, error)
}

type Adapter struct {
	sessions              SessionSource
	imagePolicy           ImagePolicy
	mu                    sync.Mutex
	consumedSweepEvidence map[string]struct{}
	attemptPins           map[workloadrunner.AttemptAuthority]attemptPinState
	probePlatforms        map[ocihelper.HelperSession]ocihelper.OCIPlatform
	pinLedger             workloadrunner.OCIImageBindingPinLedger
	cacheMaxBytes         int64
	probeDigest           string
	hostMountRoot         string
	mountGuards           map[string]*ocihelper.HostMountGuard
}

type memoryBindingPinLedger struct {
	mu   sync.Mutex
	pins map[string]workloadrunner.OCIImageBindingPin
}

type attemptPinState struct {
	attached   bool
	runEntered bool
	sweepEpoch string
}

func newMemoryBindingPinLedger() *memoryBindingPinLedger {
	return &memoryBindingPinLedger{pins: make(map[string]workloadrunner.OCIImageBindingPin)}
}

func (ledger *memoryBindingPinLedger) ListOCIImageBindingPins(context.Context) ([]workloadrunner.OCIImageBindingPin, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	pins := make([]workloadrunner.OCIImageBindingPin, 0, len(ledger.pins))
	for _, pin := range ledger.pins {
		pins = append(pins, pin)
	}
	return pins, nil
}

func (ledger *memoryBindingPinLedger) PutOCIImageBindingPin(_ context.Context, pin workloadrunner.OCIImageBindingPin) (workloadrunner.OCIImageBindingPin, bool, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if stored, ok := ledger.pins[pin.JobID]; ok {
		if stored != pin {
			return stored, false, fmt.Errorf("OCI binding pin for job %q conflicts with its first binding", pin.JobID)
		}
		return stored, false, nil
	}
	ledger.pins[pin.JobID] = pin
	return pin, true, nil
}

func (ledger *memoryBindingPinLedger) DeleteOCIImageBindingPin(_ context.Context, jobID string) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	delete(ledger.pins, jobID)
	return nil
}

type Option func(*Adapter)

func WithHostMountRoot(root string) Option {
	return func(adapter *Adapter) { adapter.hostMountRoot = root }
}

func WithImageCache(maxBytes int64, probeDigest string) Option {
	return func(adapter *Adapter) {
		adapter.cacheMaxBytes = maxBytes
		adapter.probeDigest = probeDigest
	}
}

func NewAdapter(sessions SessionSource, options ...Option) *Adapter {
	return NewAdapterWithPolicy(sessions, ImagePolicy{}, options...)
}

const DefaultImageBudget = 10 * time.Minute

// ImagePolicy is agent-owned policy around narrow helper mechanics. The
// helper never chooses retries, backoff, Retry-After treatment, or this total
// resolve/fetch/unpack/wait budget.
type ImagePolicy struct {
	Budget         time.Duration
	InitialBackoff time.Duration
	MaximumBackoff time.Duration
	Sleep          func(context.Context, time.Duration) error
}

func NewAdapterWithPolicy(sessions SessionSource, policy ImagePolicy, options ...Option) *Adapter {
	if policy.Budget <= 0 {
		policy.Budget = DefaultImageBudget
	}
	if policy.InitialBackoff <= 0 {
		policy.InitialBackoff = time.Second
	}
	if policy.MaximumBackoff <= 0 {
		policy.MaximumBackoff = 30 * time.Second
	}
	if policy.Sleep == nil {
		policy.Sleep = sleepContext
	}
	adapter := &Adapter{sessions: sessions, imagePolicy: policy, pinLedger: newMemoryBindingPinLedger(), cacheMaxBytes: ocihelper.DefaultImageCacheMaxBytes, consumedSweepEvidence: make(map[string]struct{}), attemptPins: make(map[workloadrunner.AttemptAuthority]attemptPinState), probePlatforms: make(map[ocihelper.HelperSession]ocihelper.OCIPlatform), mountGuards: make(map[string]*ocihelper.HostMountGuard)}
	for _, option := range options {
		option(adapter)
	}
	return adapter
}

func (adapter *Adapter) SetOCIImageBindingPinLedger(ledger workloadrunner.OCIImageBindingPinLedger) {
	adapter.mu.Lock()
	adapter.pinLedger = ledger
	adapter.mu.Unlock()
}

func (adapter *Adapter) ReconcileOCIImagePins(ctx context.Context, prove workloadrunner.OCIImageBindingProof) ([]workloadrunner.OCIImagePinReconciliationFailure, error) {
	adapter.mu.Lock()
	ledger := adapter.pinLedger
	cacheMaxBytes := adapter.cacheMaxBytes
	probeDigest := adapter.probeDigest
	adapter.mu.Unlock()
	if ledger == nil {
		return nil, errors.New("OCI binding pin ledger is unavailable")
	}
	pins, err := ledger.ListOCIImageBindingPins(ctx)
	if err != nil {
		return nil, err
	}
	if prove == nil {
		return nil, errors.New("positive L1 service-binding proof is required before OCI pin reconciliation")
	}
	currentPins := pins[:0]
	for _, pin := range pins {
		bound, proofErr := prove(ctx, pin.JobID)
		if proofErr != nil {
			return nil, fmt.Errorf("prove current L1 binding for %q: %w", pin.JobID, proofErr)
		}
		if !bound {
			if err := ledger.DeleteOCIImageBindingPin(ctx, pin.JobID); err != nil {
				return nil, err
			}
			continue
		}
		currentPins = append(currentPins, pin)
	}
	pins = currentPins
	bindings := make([]ocihelper.BindingImagePin, 0, len(pins))
	for _, pin := range pins {
		bindings = append(bindings, ocihelper.BindingImagePin{
			JobID: pin.JobID, Reference: pin.Reference, Digest: pin.Digest,
			Platform:    ocihelper.OCIPlatform{OS: pin.PlatformOS, Architecture: pin.PlatformArchitecture, Variant: pin.PlatformVariant},
			Snapshotter: pin.Snapshotter,
		})
	}
	session, err := adapter.sessions.Session()
	if err != nil {
		return nil, err
	}
	probeDigests := []string(nil)
	if probeDigest != "" {
		probeDigests = []string{probeDigest}
	}
	request := ocihelper.ReconcileImagePinsRequest{Bindings: bindings, ProbeDigests: probeDigests, CacheMaxBytes: cacheMaxBytes}
	response, err := session.ReconcileImagePins(ctx, request)
	if err != nil {
		return nil, err
	}
	if len(response.MissingDigests) == 0 {
		return nil, nil
	}
	missing := make(map[string]struct{}, len(response.MissingDigests))
	for _, value := range response.MissingDigests {
		missing[value] = struct{}{}
	}
	var failures []workloadrunner.OCIImagePinReconciliationFailure
	for _, pin := range pins {
		if _, ok := missing[pin.Digest]; !ok {
			continue
		}
		platform := ocihelper.OCIPlatform{OS: pin.PlatformOS, Architecture: pin.PlatformArchitecture, Variant: pin.PlatformVariant}
		_, failure, deliveryErr := adapter.ensureImage(ctx, session, pin.Reference, pin.Digest, platform, time.Time{}, nil)
		if deliveryErr != nil {
			if failure == nil {
				failure = imageSpawnFailure(contract.SpawnFailureRuntimeUnavailable, deliveryErr)
			}
			failures = append(failures, workloadrunner.OCIImagePinReconciliationFailure{JobID: pin.JobID, Failure: *failure})
		}
	}
	response, err = session.ReconcileImagePins(ctx, request)
	if err != nil {
		return failures, err
	}
	if len(response.MissingDigests) != 0 {
		if len(failures) != 0 {
			return failures, nil
		}
		return nil, fmt.Errorf("OCI binding pin reconciliation still reports missing digests: %s", strings.Join(response.MissingDigests, ", "))
	}
	return failures, nil
}

func (adapter *Adapter) ReleaseOCIImageBindingPin(ctx context.Context, jobID string) error {
	adapter.mu.Lock()
	ledger := adapter.pinLedger
	adapter.mu.Unlock()
	if ledger == nil {
		return errors.New("OCI binding pin ledger is unavailable")
	}
	pins, err := ledger.ListOCIImageBindingPins(ctx)
	if err != nil {
		return err
	}
	found := false
	for _, pin := range pins {
		if pin.JobID == jobID {
			found = true
			break
		}
	}
	if !found {
		return nil
	}
	session, err := adapter.sessions.Session()
	if err != nil {
		return err
	}
	if err := session.ReleaseImagePin(ctx, jobID); err != nil {
		return err
	}
	return ledger.DeleteOCIImageBindingPin(ctx, jobID)
}

// LoadImage is the agent-side offline-import seam used by the node-local
// operator control plane. The agent owns the same total policy budget while
// the helper exclusively validates and imports archive mechanics.
// TODO(#153): expose this seam through the operator-only local control socket.
func (adapter *Adapter) LoadImage(ctx context.Context, reference string, archive io.Reader) (ocihelper.EnsureImageResponse, error) {
	if adapter == nil || adapter.sessions == nil {
		return ocihelper.EnsureImageResponse{}, errors.New("OCI helper session is not configured")
	}
	if archive == nil {
		return ocihelper.EnsureImageResponse{}, errors.New("OCI image archive reader is required")
	}
	budgetContext, cancel := context.WithTimeout(ctx, adapter.imagePolicy.Budget)
	defer cancel()
	session, err := adapter.sessions.Session()
	if err != nil {
		return ocihelper.EnsureImageResponse{}, err
	}
	probePlatform, ok := adapter.probePlatform(session)
	if !ok {
		return ocihelper.EnsureImageResponse{}, errors.New("current OCI helper generation has no successful probe platform")
	}
	deadline, _ := budgetContext.Deadline()
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return ocihelper.EnsureImageResponse{}, errors.New("OCI image import budget exhausted")
	}
	var response ocihelper.EnsureImageResponse
	err = session.ImportImage(budgetContext, ocihelper.EnsureImageRequest{
		Reference: reference, Platform: probePlatform, Source: ocihelper.ImageSourceArchive, OperationTimeout: remaining,
	}, archive, func(event ocihelper.EnsureImageEvent) error {
		if event.Result != nil {
			response = *event.Result
		}
		return nil
	})
	if err != nil {
		if budgetContext.Err() != nil {
			return ocihelper.EnsureImageResponse{}, errors.New("OCI image import budget exhausted")
		}
		return ocihelper.EnsureImageResponse{}, err
	}
	if response.TopLevelDigest == "" || response.PlatformDigest == "" {
		return ocihelper.EnsureImageResponse{}, errors.New("OCI helper completed image import without immutable digests")
	}
	return response, nil
}

// DialAttemptPort exposes one named exact-authority host-to-guest stream to the
// agent-side readiness/front-door seam. It is never a general guest dialer.
func (adapter *Adapter) DialAttemptPort(ctx context.Context, authority workloadrunner.AttemptAuthority, name string) (net.Conn, error) {
	if adapter == nil || adapter.sessions == nil {
		return nil, errors.New("OCI helper session is not configured")
	}
	session, err := adapter.sessions.Session()
	if err != nil {
		return nil, err
	}
	return session.DialAttemptPort(ctx, ocihelper.DialAttemptPortRequest{Authority: HelperAuthority(authority), Name: name})
}

type sweepReceiptSource interface {
	SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool)
}

// Probe exercises the same pinned local image, runc-v2 task, Wait-before-Start,
// Watch, and verified Delete path used by production attempts.
func (adapter *Adapter) Probe(ctx context.Context, nodeID, bootSessionID, reference, digest string, deadman time.Duration) error {
	if adapter == nil || adapter.sessions == nil {
		return errors.New("OCI helper session is not configured")
	}
	if reference == "" || digest == "" {
		return errors.New("OCI functional probe requires a pinned local image reference and digest")
	}
	session, err := adapter.sessions.Session()
	if err != nil {
		return err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fmt.Errorf("generate OCI probe identity: %w", err)
	}
	id := hex.EncodeToString(nonce[:])
	authority := ocihelper.AttemptAuthority{
		NodeID: nodeID, BootSessionID: bootSessionID, JobID: "probe-" + id,
		AttemptID: "probe-" + id, FencingToken: "probe-" + id,
		Class: contract.JobClassOneShot, RemovalGeneration: "probe",
	}
	response, err := session.Run(ctx, ocihelper.RunRequest{
		Authority: authority, InitialDeadman: deadman,
		Workload: ocihelper.WorkloadInput{ImageReference: reference, ImageDigest: digest, Argv: []string{"/bin/true"}},
	})
	if err != nil {
		return err
	}
	cleanupNeeded := true
	defer func() {
		if !cleanupNeeded {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = session.Delete(cleanupCtx, ocihelper.DeleteRequest{Authority: authority})
	}()
	if !response.Started || response.Image == nil {
		return errors.New("OCI functional probe did not receive truthful Started evidence")
	}
	platform, err := canonicalProbePlatform(response.Image.Platform)
	if err != nil {
		return err
	}
	var completion *ocihelper.WatchResponse
	if err := session.Watch(ctx, ocihelper.WatchRequest{Authority: authority}, func(event ocihelper.WatchEvent) error {
		if event.Result != nil {
			copy := *event.Result
			completion = &copy
		}
		return nil
	}); err != nil {
		return err
	}
	if completion == nil || completion.ExitCode == nil || *completion.ExitCode != 0 || completion.Signal != "" || completion.RuntimeFailure != "" {
		return fmt.Errorf("OCI functional probe returned %+v", completion)
	}
	deleted, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: authority})
	if err != nil {
		return err
	}
	if !deleted.Deleted {
		return errors.New("OCI functional probe cleanup was not verified")
	}
	adapter.mu.Lock()
	adapter.probePlatforms[helperSession(session)] = platform
	adapter.mu.Unlock()
	cleanupNeeded = false
	return nil
}

func canonicalProbePlatform(platform ocihelper.OCIPlatform) (ocihelper.OCIPlatform, error) {
	canonical := ocihelper.OCIPlatform{
		OS:           strings.ToLower(strings.TrimSpace(platform.OS)),
		Architecture: strings.ToLower(strings.TrimSpace(platform.Architecture)),
		Variant:      strings.ToLower(strings.TrimSpace(platform.Variant)),
	}
	if canonical.OS == "" || canonical.Architecture == "" || canonical != platform {
		return ocihelper.OCIPlatform{}, errors.New("OCI functional probe returned a non-canonical platform")
	}
	return canonical, nil
}

func helperSession(session *ocihelper.Session) ocihelper.HelperSession {
	handshake := session.Handshake()
	return ocihelper.HelperSession{HelperInstanceID: handshake.HelperInstanceID, SessionGeneration: handshake.SessionGeneration}
}

func (adapter *Adapter) probePlatform(session *ocihelper.Session) (ocihelper.OCIPlatform, bool) {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	platform, ok := adapter.probePlatforms[helperSession(session)]
	return platform, ok
}

func (adapter *Adapter) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	adapter.trackAttemptPin(request.Authority)
	admission := workloadrunner.Admission{Request: request, Release: func() {}}
	if adapter == nil || adapter.sessions == nil {
		return failedAdmission(admission, contract.SpawnFailureRuntimeUnavailable, errors.New("OCI helper session is not configured"))
	}
	if request.Authority.NodeID == "" || request.Authority.BootSessionID == "" || request.Authority.JobID == "" ||
		request.Authority.AttemptID == "" || request.Authority.FencingToken == "" || request.Authority.WorkloadClass == "" || request.Authority.RemovalGeneration == "" {
		return failedAdmission(admission, contract.SpawnFailureProcessRequest, errors.New("OCI runtime authority is incomplete"))
	}
	if request.Execution.OCI == nil {
		return failedAdmission(admission, contract.SpawnFailureProcessRequest, errors.New("OCI execution arm is required"))
	}
	if request.RuntimeHandler != "" && request.RuntimeHandler != ocihelper.DefaultRuntimeHandler {
		return failedAdmission(admission, contract.SpawnFailureUnsupportedRuntimeHandler, fmt.Errorf("OCI runtime handler %q is unavailable", request.RuntimeHandler))
	}
	if request.OCIStarted == nil {
		return failedAdmission(admission, contract.SpawnFailureProcessRequest, errors.New("OCI Started acknowledgement hook is required"))
	}
	if request.OCIImageResolved == nil {
		return failedAdmission(admission, contract.SpawnFailureProcessRequest, errors.New("OCI image resolution hook is required"))
	}
	if request.InitialDeadman <= 0 {
		return failedAdmission(admission, contract.SpawnFailureProcessRequest, errors.New("OCI initial deadman is required"))
	}
	if len(request.Execution.OCI.Mounts) > 0 && adapter.hostMountRoot != "" {
		paths := make([]string, 0, len(request.Execution.OCI.Mounts))
		for _, mount := range request.Execution.OCI.Mounts {
			paths = append(paths, mount.NodePath)
		}
		guard, err := ocihelper.OpenHostMountGuard(paths, adapter.hostMountRoot)
		if err != nil {
			return failedAdmission(admission, contract.SpawnFailureProcessRequest, fmt.Errorf("validate Lima host mount sources: %w", err))
		}
		key := adapterAuthorityKey(request.Authority)
		adapter.mu.Lock()
		adapter.mountGuards[key] = guard
		adapter.mu.Unlock()
		admission.Release = func() {
			adapter.mu.Lock()
			if adapter.mountGuards[key] == guard {
				delete(adapter.mountGuards, key)
			}
			adapter.mu.Unlock()
			_ = guard.Close()
		}
	}
	if len(request.AttemptEndpoints) > 0 && request.AttemptEndpointReady == nil {
		return failedAdmission(admission, contract.SpawnFailureProcessRequest, errors.New("OCI attempt endpoint callback is required for portful work"))
	}
	return admission, workloadrunner.Result{}, nil
}

func failedAdmission(admission workloadrunner.Admission, code contract.SpawnFailureCode, err error) (workloadrunner.Admission, workloadrunner.Result, error) {
	return admission, workloadrunner.Result{Outcome: contract.ProcessResult{SpawnError: &contract.SpawnFailure{Code: code, Message: err.Error()}}}, err
}

func (adapter *Adapter) Run(ctx context.Context, request workloadrunner.Request, sink workloadrunner.OutputSink) (result workloadrunner.Result, runErr error) {
	adapter.trackAttemptPin(request.Authority)
	session, err := adapter.sessions.Session()
	if err != nil {
		return spawnResult(contract.SpawnFailureRuntimeUnavailable, err), err
	}
	if request.OCIImagePulling != nil {
		request.OCIImagePulling()
	}
	probePlatform, ok := adapter.probePlatform(session)
	if !ok {
		err := errors.New("current OCI helper generation has no successful probe platform")
		return spawnResult(contract.SpawnFailureRuntimeUnavailable, err), err
	}
	digest := ""
	if request.Execution.OCI.Image.Digest != nil {
		digest = *request.Execution.OCI.Image.Digest
	}
	pin := &ocihelper.ImagePin{Authority: HelperAuthority(request.Authority), Binding: request.Authority.WorkloadClass == contract.JobClassService}
	bindingPinCreated := false
	bindingDeliveryComplete := false
	defer func() {
		if pin.Binding && bindingPinCreated && !bindingDeliveryComplete {
			adapter.mu.Lock()
			ledger := adapter.pinLedger
			adapter.mu.Unlock()
			if ledger != nil {
				if err := ledger.DeleteOCIImageBindingPin(context.WithoutCancel(ctx), request.Authority.JobID); err != nil {
					runErr = errors.Join(runErr, fmt.Errorf("delete failed OCI binding-pin intent: %w", err))
					if result.Outcome.SpawnError == nil {
						result = spawnResult(contract.SpawnFailureRuntimeUnavailable, err)
					}
				}
			}
		}
	}()
	if pin.Binding {
		adapter.mu.Lock()
		ledger := adapter.pinLedger
		adapter.mu.Unlock()
		if ledger == nil {
			err := errors.New("OCI binding pin ledger is unavailable")
			return spawnResult(contract.SpawnFailureRuntimeUnavailable, err), err
		}
		if digest == "" {
			err := errors.New("service binding image digest is required before delivery")
			return spawnResult(contract.SpawnFailureProcessRequest, err), err
		}
		stored, created, err := ledger.PutOCIImageBindingPin(ctx, workloadrunner.OCIImageBindingPin{
			JobID: request.Authority.JobID, Reference: request.Execution.OCI.Image.Reference, Digest: digest,
			PlatformOS: probePlatform.OS, PlatformArchitecture: probePlatform.Architecture, PlatformVariant: probePlatform.Variant,
			Snapshotter: ocihelper.DefaultSnapshotter,
		})
		if err != nil {
			storedPlatform := ocihelper.OCIPlatform{OS: stored.PlatformOS, Architecture: stored.PlatformArchitecture, Variant: stored.PlatformVariant}
			if stored.JobID == request.Authority.JobID && stored.Reference == request.Execution.OCI.Image.Reference && stored.Digest == digest && stored.Snapshotter == ocihelper.DefaultSnapshotter && storedPlatform != probePlatform {
				platformErr := errors.New("service binding image platform differs from the current probed OCI runtime platform")
				return spawnResult(contract.SpawnFailureImagePlatformUnsupported, platformErr), platformErr
			}
			return spawnResult(contract.SpawnFailureRuntimeUnavailable, err), err
		}
		bindingPinCreated = created
		storedPlatform := ocihelper.OCIPlatform{OS: stored.PlatformOS, Architecture: stored.PlatformArchitecture, Variant: stored.PlatformVariant}
		if storedPlatform != probePlatform {
			err := errors.New("service binding image platform differs from the current probed OCI runtime platform")
			return spawnResult(contract.SpawnFailureImagePlatformUnsupported, err), err
		}
	}
	image, failure, err := adapter.ensureImage(ctx, session, request.Execution.OCI.Image.Reference, digest, probePlatform, request.OCIImageDeadline, pin)
	if err != nil {
		return workloadrunner.Result{Outcome: contract.ProcessResult{SpawnError: failure}}, err
	}
	adapter.markAttemptPinAttached(request.Authority)
	image.Evidence.SubmittedReference = request.Execution.OCI.Image.Reference
	if image.Evidence.Platform != probePlatform {
		err := errors.New("OCI image selection differs from the current probe platform")
		return spawnResult(contract.SpawnFailureImagePlatformUnsupported, err), err
	}
	if err := request.OCIImageResolved(ctx, imageObservation(image.Evidence)); err != nil {
		var refusal *workloadrunner.OCIObservationRefusal
		if errors.As(err, &refusal) {
			return spawnResult(contract.SpawnFailureProcessRequest, err), err
		}
		return spawnResult(contract.SpawnFailureRuntimeUnavailable, err), err
	}
	bindingDeliveryComplete = true
	if request.OCIImageReady != nil {
		request.OCIImageReady()
	}
	execution := *request.Execution.OCI
	execution.Image = request.Execution.OCI.Image
	execution.Image.Digest = &image.TopLevelDigest
	request.Execution.OCI = &execution
	authority := HelperAuthority(request.Authority)
	adapter.mu.Lock()
	guard := adapter.mountGuards[adapterAuthorityKey(request.Authority)]
	adapter.mu.Unlock()
	if guard != nil {
		if err := guard.Revalidate(); err != nil {
			return spawnResult(contract.SpawnFailureProcessRequest, err), err
		}
	}
	runResponse, err := session.Run(ctx, ocihelper.RunRequest{
		Authority: authority, InitialDeadman: request.InitialDeadman,
		AllocateEndpoints:        request.AttemptEndpoints,
		EnableHostBridgeFallback: request.HostBridgeDial != nil,
		Workload:                 workloadInput(request),
	})
	if err != nil {
		if failure := ocihelper.SpawnFailureForRunError(err); failure != nil {
			return workloadrunner.Result{Outcome: contract.ProcessResult{SpawnError: failure}}, err
		}
		return spawnResult(contract.SpawnFailureRuntimeUnavailable, err), err
	}
	adapter.markRunEntered(request.Authority, true)
	if runResponse.Image == nil {
		err := errors.New("OCI helper Started response omitted image evidence")
		_ = reapAfterFailedStart(session, authority)
		return spawnResult(contract.SpawnFailureRuntimeUnavailable, err), err
	}
	if runResponse.Image.Platform != probePlatform {
		err := errors.New("OCI Started evidence differs from the current probe platform")
		_ = reapAfterFailedStart(session, authority)
		return spawnResult(contract.SpawnFailureProcessRequest, err), err
	}
	if request.HostBridgeDial != nil && (!runResponse.HostBridgeReady || runResponse.BridgeCapability == "") {
		err := errors.New("OCI helper omitted the requested host bridge fallback authority")
		_ = reapAfterFailedStart(session, authority)
		return spawnResult(contract.SpawnFailureRuntimeUnavailable, err), err
	}
	if len(request.AttemptEndpoints) > 0 {
		if len(runResponse.Endpoints) != len(request.AttemptEndpoints) {
			err := errors.New("OCI helper omitted the requested attempt endpoints")
			_ = reapAfterFailedStart(session, authority)
			return spawnResult(contract.SpawnFailureRuntimeUnavailable, err), err
		}
		for _, name := range request.AttemptEndpoints {
			port := runResponse.Endpoints[name]
			if port == 0 {
				err := fmt.Errorf("OCI helper omitted requested attempt endpoint %q", name)
				_ = reapAfterFailedStart(session, authority)
				return spawnResult(contract.SpawnFailureRuntimeUnavailable, err), err
			}
			endpointName := name
			endpoint := workloadrunner.AttemptEndpoint{Port: port, Dial: func(dialContext context.Context) (net.Conn, error) {
				return adapter.DialAttemptPort(dialContext, request.Authority, endpointName)
			}}
			if err := request.AttemptEndpointReady(name, endpoint); err != nil {
				_ = reapAfterFailedStart(session, authority)
				return spawnResult(contract.SpawnFailureProcessRequest, err), err
			}
		}
	} else if len(runResponse.Endpoints) != 0 {
		err := errors.New("OCI helper returned unrequested attempt endpoints")
		_ = reapAfterFailedStart(session, authority)
		return spawnResult(contract.SpawnFailureRuntimeUnavailable, err), err
	}
	if err := request.OCIStarted(ctx, imageObservation(*runResponse.Image)); err != nil {
		_ = reapAfterFailedStart(session, authority)
		// A fencing/authority refusal is a terminal fact about this attempt, not
		// infrastructure loss eligible for the OCI pre-start retry budget.
		return spawnResult(contract.SpawnFailureProcessRequest, err), err
	}
	if request.Started != nil {
		request.Started()
	}

	var completion *ocihelper.WatchResponse
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- session.Watch(ctx, ocihelper.WatchRequest{Authority: authority}, func(event ocihelper.WatchEvent) error {
			if event.Log != nil && sink != nil {
				logEvent := contract.LogEvent{
					AttemptID: request.Authority.AttemptID, Stream: contract.LogStream(event.Log.Stream),
					Sequence: event.Log.Sequence, Timestamp: time.Now().UTC(), Bytes: event.Log.Bytes,
				}
				if event.Log.Gap != nil {
					logEvent.Gap = &contract.LogGap{ThroughSequence: event.Log.Gap.ThroughSequence, LostEventCount: event.Log.Gap.LostEventCount, LostByteCount: event.Log.Gap.LostByteCount, Reason: contract.LogGapLoggerSourceIncomplete}
				}
				return sink.WriteOutput(ctx, logEvent)
			}
			if event.Result != nil {
				copy := *event.Result
				completion = &copy
			}
			return nil
		})
	}()
	if request.HostBridgeDial != nil {
		bridgeContext, cancelBridge := context.WithCancel(ctx)
		const bridgeConcurrency = 4
		bridgeDone := make(chan struct{}, bridgeConcurrency)
		for range bridgeConcurrency {
			go func() {
				pumpHostBridge(bridgeContext, session, authority, runResponse.BridgeCapability, request.HostBridgeDial)
				bridgeDone <- struct{}{}
			}()
		}
		err = <-watchDone
		cancelBridge()
		for range bridgeConcurrency {
			<-bridgeDone
		}
	} else {
		err = <-watchDone
	}
	if err != nil {
		return runtimeFailure(err), err
	}
	if completion == nil {
		err := errors.New("OCI helper Watch ended without a completion result")
		return runtimeFailure(err), err
	}
	return workloadrunner.Result{Outcome: processResult(*completion)}, nil
}

func (adapter *Adapter) ensureImage(ctx context.Context, session *ocihelper.Session, reference, digest string, platform ocihelper.OCIPlatform, persistedDeadline time.Time, pin *ocihelper.ImagePin) (ocihelper.EnsureImageResponse, *contract.SpawnFailure, error) {
	requestedDigest := digest
	deadline := time.Now().Add(adapter.imagePolicy.Budget)
	if !persistedDeadline.IsZero() && persistedDeadline.Before(deadline) {
		deadline = persistedDeadline
	}
	budgetContext, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	deadline, _ = budgetContext.Deadline()
	backoff := adapter.imagePolicy.InitialBackoff
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			err := errors.New("OCI image delivery budget exhausted")
			return ocihelper.EnsureImageResponse{}, imageSpawnFailure(contract.SpawnFailureImageUnavailable, err), err
		}
		var response ocihelper.EnsureImageResponse
		err := session.EnsureImage(budgetContext, ocihelper.EnsureImageRequest{
			Reference: reference, Digest: digest, Platform: platform, Source: ocihelper.ImageSourceRegistry, OperationTimeout: remaining, Pin: pin,
		}, func(event ocihelper.EnsureImageEvent) error {
			if event.Progress != nil && event.Progress.TopLevelDigest != "" {
				digest = event.Progress.TopLevelDigest
			}
			if event.Result != nil {
				response = *event.Result
			}
			return nil
		})
		if err == nil {
			if response.TopLevelDigest == "" || response.PlatformDigest == "" {
				err = errors.New("OCI helper completed image delivery without immutable digests")
				return ocihelper.EnsureImageResponse{}, imageSpawnFailure(contract.SpawnFailureImageUnavailable, err), err
			}
			expectedDigest := digest
			if requestedDigest != "" {
				expectedDigest = requestedDigest
			}
			if expectedDigest != "" && response.TopLevelDigest != expectedDigest {
				err = errors.New("OCI helper returned a top-level digest different from the pinned request")
				return ocihelper.EnsureImageResponse{}, imageSpawnFailure(contract.SpawnFailureImageManifestInvalid, err), err
			}
			return response, nil, nil
		}
		var rpcErr *ocihelper.RPCError
		if !errors.As(err, &rpcErr) {
			if budgetContext.Err() != nil {
				budgetErr := errors.New("OCI image delivery budget exhausted")
				return ocihelper.EnsureImageResponse{}, imageSpawnFailure(contract.SpawnFailureImageUnavailable, budgetErr), budgetErr
			}
			return ocihelper.EnsureImageResponse{}, imageSpawnFailure(contract.SpawnFailureRuntimeUnavailable, err), err
		}
		if rpcErr.ImageFailure != nil && rpcErr.ImageFailure.TopLevelDigest != "" {
			digest = rpcErr.ImageFailure.TopLevelDigest
		}
		classification := classifyImageFailure(rpcErr)
		if classification.transient {
			delay := backoff
			if classification.retryAfter > delay {
				delay = classification.retryAfter
			}
			remaining = time.Until(deadline)
			if delay >= remaining {
				budgetErr := errors.New("OCI image delivery budget exhausted")
				return ocihelper.EnsureImageResponse{}, imageSpawnFailure(contract.SpawnFailureImageUnavailable, budgetErr), budgetErr
			}
			if sleepErr := adapter.imagePolicy.Sleep(budgetContext, delay); sleepErr != nil {
				budgetErr := errors.New("OCI image delivery budget exhausted")
				return ocihelper.EnsureImageResponse{}, imageSpawnFailure(contract.SpawnFailureImageUnavailable, budgetErr), budgetErr
			}
			backoff *= 2
			if backoff > adapter.imagePolicy.MaximumBackoff {
				backoff = adapter.imagePolicy.MaximumBackoff
			}
			continue
		}
		return ocihelper.EnsureImageResponse{}, imageSpawnFailure(classification.code, err), err
	}
}

type imageFailureClassification struct {
	code       contract.SpawnFailureCode
	transient  bool
	retryAfter time.Duration
}

func classifyImageFailure(rpcErr *ocihelper.RPCError) imageFailureClassification {
	fallback := imageFailureClassification{code: contract.SpawnFailureImageUnavailable}
	if rpcErr == nil {
		return fallback
	}
	if rpcErr.Code == ocihelper.CodeSessionStale || rpcErr.Code == ocihelper.CodeSweepRequired {
		return imageFailureClassification{code: contract.SpawnFailureRuntimeUnavailable}
	}
	fact := rpcErr.ImageFailure
	if fact == nil {
		return fallback
	}
	switch fact.Kind {
	case ocihelper.ImageFailureNetwork:
		return imageFailureClassification{code: contract.SpawnFailureImageUnavailable, transient: true, retryAfter: fact.RetryAfter}
	case ocihelper.ImageFailureHTTP:
		switch {
		case fact.HTTPStatus == 404:
			return imageFailureClassification{code: contract.SpawnFailureImageNotFound}
		case fact.HTTPStatus == 429 || fact.HTTPStatus >= 500:
			return imageFailureClassification{code: contract.SpawnFailureImageUnavailable, transient: true, retryAfter: fact.RetryAfter}
		case fact.HTTPStatus == 400 || fact.HTTPStatus == 406 || fact.HTTPStatus == 415 || fact.HTTPStatus == 422:
			return imageFailureClassification{code: contract.SpawnFailureImageManifestInvalid}
		default:
			return fallback
		}
	case ocihelper.ImageFailurePlatformMismatch:
		return imageFailureClassification{code: contract.SpawnFailureImagePlatformUnsupported}
	case ocihelper.ImageFailureEngineLoss, ocihelper.ImageFailureResourceExhausted:
		return imageFailureClassification{code: contract.SpawnFailureRuntimeUnavailable}
	case ocihelper.ImageFailureManifestRejected:
		return imageFailureClassification{code: contract.SpawnFailureImageManifestInvalid}
	default:
		return fallback
	}
}

func imageSpawnFailure(code contract.SpawnFailureCode, err error) *contract.SpawnFailure {
	return &contract.SpawnFailure{Code: code, Message: err.Error()}
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func pumpHostBridge(
	ctx context.Context,
	session *ocihelper.Session,
	authority ocihelper.AttemptAuthority,
	capability string,
	dialHost func(context.Context) (net.Conn, error),
) {
	for {
		helper, err := session.DialHostBridge(ctx, ocihelper.DialHostBridgeRequest{Authority: authority, BridgeCapability: capability})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("OCI host bridge connection retry: open constrained helper stream: %v", err)
			if !waitBridgeRetry(ctx) {
				return
			}
			continue
		}
		host, err := dialHost(ctx)
		if err != nil {
			_ = helper.Close()
			if ctx.Err() != nil {
				return
			}
			log.Printf("OCI host bridge connection retry: dial host-local bridge: %v", err)
			if !waitBridgeRetry(ctx) {
				return
			}
			continue
		}
		err = ocihelper.Relay(ctx, helper, host)
		_ = helper.Close()
		_ = host.Close()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("OCI host bridge connection retry: relay: %v", err)
		}
	}
}

func waitBridgeRetry(ctx context.Context) bool {
	timer := time.NewTimer(25 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (adapter *Adapter) ReapAndVerify(ctx context.Context, request workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	adapter.mu.Lock()
	state, tracked := adapter.attemptPins[request.Authority]
	delete(adapter.attemptPins, request.Authority)
	adapter.mu.Unlock()
	if tracked && !state.runEntered {
		if state.attached {
			session, err := adapter.sessions.Session()
			if err != nil {
				return workloadrunner.ReapReceipt{}, err
			}
			if err := session.ReleaseAttemptImagePin(ctx, HelperAuthority(request.Authority)); err != nil {
				return workloadrunner.ReapReceipt{}, err
			}
		}
		return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceNoRuntime, BootSessionID: request.Authority.BootSessionID}, nil
	}
	session, err := adapter.sessions.Session()
	if err != nil {
		return workloadrunner.ReapReceipt{}, err
	}
	authority := HelperAuthority(request.Authority)
	deleted, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: authority})
	if err != nil {
		if tracked {
			if receipt, sweepErr := adapter.reapFromReplacementSweep(request.Authority, state.sweepEpoch, err); sweepErr == nil {
				return receipt, nil
			}
		}
		return workloadrunner.ReapReceipt{}, err
	}
	if !deleted.Deleted {
		return workloadrunner.ReapReceipt{}, errors.New("OCI helper Delete did not positively verify attempt absence")
	}
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt, BootSessionID: request.Authority.BootSessionID}, nil
}

func (adapter *Adapter) markRunEntered(authority workloadrunner.AttemptAuthority, entered bool) {
	if adapter == nil {
		return
	}
	adapter.mu.Lock()
	state := adapter.attemptPins[authority]
	state.runEntered = entered
	if entered {
		state.attached = false
	}
	adapter.attemptPins[authority] = state
	adapter.mu.Unlock()
}

func (adapter *Adapter) trackAttemptPin(authority workloadrunner.AttemptAuthority) {
	if adapter == nil {
		return
	}
	adapter.mu.Lock()
	if _, exists := adapter.attemptPins[authority]; !exists {
		state := attemptPinState{}
		if source, ok := adapter.sessions.(sweepReceiptSource); ok {
			if receipt, receiptOK := source.SweepReceipt(); receiptOK {
				state.sweepEpoch = receipt.SweepEpoch
			}
		}
		adapter.attemptPins[authority] = state
	}
	adapter.mu.Unlock()
}

func (adapter *Adapter) markAttemptPinAttached(authority workloadrunner.AttemptAuthority) {
	adapter.mu.Lock()
	state := adapter.attemptPins[authority]
	state.attached = true
	adapter.attemptPins[authority] = state
	adapter.mu.Unlock()
}

func (adapter *Adapter) reapFromReplacementSweep(authority workloadrunner.AttemptAuthority, previousEpoch string, deleteErr error) (workloadrunner.ReapReceipt, error) {
	var rpcErr *ocihelper.RPCError
	if previousEpoch == "" || !errors.As(deleteErr, &rpcErr) || rpcErr.Code != ocihelper.CodeUnauthorizedAttempt {
		return workloadrunner.ReapReceipt{}, deleteErr
	}
	source, ok := adapter.sessions.(sweepReceiptSource)
	if !ok {
		return workloadrunner.ReapReceipt{}, deleteErr
	}
	receipt, ok := source.SweepReceipt()
	if !ok || receipt.SweepEpoch == "" || receipt.SweepEpoch == previousEpoch ||
		receipt.HelperSession.HelperInstanceID == "" || receipt.HelperSession.SessionGeneration == 0 {
		return workloadrunner.ReapReceipt{}, deleteErr
	}
	key := fmt.Sprintf("runtime\x00%s\x00%s\x00%d\x00%s", receipt.SweepEpoch, receipt.HelperSession.HelperInstanceID, receipt.HelperSession.SessionGeneration, adapterAuthorityKey(authority))
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if _, consumed := adapter.consumedSweepEvidence[key]; consumed {
		return workloadrunner.ReapReceipt{}, deleteErr
	}
	adapter.consumedSweepEvidence[key] = struct{}{}
	return workloadrunner.ReapReceipt{
		RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceOCIRuntimeSweep,
		BootSessionID: authority.BootSessionID, SweepEpoch: receipt.SweepEpoch,
		HelperGeneration: receipt.HelperSession.SessionGeneration,
	}, nil
}

func (adapter *Adapter) ReapPriorBoot(_ context.Context, request workloadrunner.PriorBootReapRequest) (workloadrunner.ReapReceipt, error) {
	if request.NodeID == "" || request.JobID == "" || request.PriorBootSessionID == "" || request.CurrentBootSessionID == "" || request.PriorBootSessionID == request.CurrentBootSessionID {
		return workloadrunner.ReapReceipt{}, workloadrunner.ErrPriorBootEvidenceUnavailable
	}
	source, ok := adapter.sessions.(sweepReceiptSource)
	if !ok {
		return workloadrunner.ReapReceipt{}, workloadrunner.ErrPriorBootEvidenceUnavailable
	}
	receipt, ok := source.SweepReceipt()
	if !ok || receipt.SweepEpoch == "" || receipt.HelperSession.HelperInstanceID == "" || receipt.HelperSession.SessionGeneration == 0 {
		return workloadrunner.ReapReceipt{}, workloadrunner.ErrPriorBootEvidenceUnavailable
	}
	found := false
	for _, attempt := range receipt.Attempts {
		if attempt.NodeID == request.NodeID && attempt.JobID == request.JobID && attempt.PriorBootSessionID == request.PriorBootSessionID && attempt.Class == contract.JobClassService {
			found = true
			break
		}
	}
	if !found {
		return workloadrunner.ReapReceipt{}, workloadrunner.ErrPriorBootEvidenceUnavailable
	}
	key := fmt.Sprintf("%s\x00%s\x00%d\x00%s\x00%s", receipt.SweepEpoch, receipt.HelperSession.HelperInstanceID, receipt.HelperSession.SessionGeneration, request.JobID, request.PriorBootSessionID)
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if _, consumed := adapter.consumedSweepEvidence[key]; consumed {
		return workloadrunner.ReapReceipt{}, workloadrunner.ErrPriorBootEvidenceUnavailable
	}
	adapter.consumedSweepEvidence[key] = struct{}{}
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidencePriorBootOCISweep, BootSessionID: request.PriorBootSessionID, SweepEpoch: receipt.SweepEpoch, HelperGeneration: receipt.HelperSession.SessionGeneration}, nil
}

// HelperAuthority maps the agent's complete fenced attempt tuple without
// allowing callers to omit class or removal-generation evidence.
func HelperAuthority(authority workloadrunner.AttemptAuthority) ocihelper.AttemptAuthority {
	return ocihelper.AttemptAuthority{
		NodeID: authority.NodeID, BootSessionID: authority.BootSessionID, JobID: authority.JobID,
		AttemptID: authority.AttemptID, FencingToken: authority.FencingToken,
		Class: authority.WorkloadClass, RemovalGeneration: authority.RemovalGeneration,
	}
}

func adapterAuthorityKey(authority workloadrunner.AttemptAuthority) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s", authority.NodeID, authority.BootSessionID, authority.JobID, authority.AttemptID, authority.FencingToken, authority.WorkloadClass, authority.RemovalGeneration)
}

func workloadInput(request workloadrunner.Request) ocihelper.WorkloadInput {
	execution := request.Execution.OCI
	digest := ""
	if execution.Image.Digest != nil {
		digest = *execution.Image.Digest
	}
	workingDirectory := ""
	if execution.WorkingDirectory != nil {
		workingDirectory = *execution.WorkingDirectory
	}
	public, reservedPublic := splitEnvironment(request.Execution.Env)
	sensitive, reservedSensitive := splitEnvironment(request.Execution.SensitiveEnv)
	reservedValues := make(map[string]string, len(reservedPublic)+len(reservedSensitive))
	for _, variable := range reservedPublic {
		reservedValues[variable.Name] = variable.Value
	}
	// SensitiveEnv has the same precedence it has for non-reserved variables.
	for _, variable := range reservedSensitive {
		reservedValues[variable.Name] = variable.Value
	}
	reserved := environment(reservedValues)
	input := ocihelper.WorkloadInput{
		ImageReference: execution.Image.Reference, ImageDigest: digest,
		Argv: append([]string(nil), execution.Argv...), WorkingDirectory: workingDirectory,
		Environment: public, SensitiveEnvironment: sensitive, ReservedEnvironment: reserved,
	}
	if request.Authority.WorkloadClass == contract.JobClassOneShot {
		input.ManagedVolumes = append(input.ManagedVolumes, ocihelper.ManagedVolumeDescriptor{Kind: ocihelper.ManagedVolumeHandoff})
	}
	for _, mount := range execution.Mounts {
		input.OperatorMounts = append(input.OperatorMounts, ocihelper.OperatorMount{NodePath: mount.NodePath, ContainerPath: mount.ContainerPath, ReadOnly: mount.ReadOnly})
	}
	if execution.Limits != nil {
		if execution.Limits.MemoryBytes != nil {
			input.Limits.MemoryBytes = *execution.Limits.MemoryBytes
		}
		if execution.Limits.CPUMillicores != nil {
			input.Limits.CPUMillicores = *execution.Limits.CPUMillicores
		}
	}
	return input
}

func splitEnvironment(values map[string]string) ([]ocihelper.EnvironmentVariable, []ocihelper.EnvironmentVariable) {
	public := make(map[string]string, len(values))
	reserved := make(map[string]string)
	for name, value := range values {
		if contract.IsOCIReservedEnvironmentName(name) {
			reserved[name] = value
		} else {
			public[name] = value
		}
	}
	return environment(public), environment(reserved)
}

func environment(values map[string]string) []ocihelper.EnvironmentVariable {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]ocihelper.EnvironmentVariable, 0, len(names))
	for _, name := range names {
		result = append(result, ocihelper.EnvironmentVariable{Name: name, Value: values[name]})
	}
	return result
}

func imageObservation(evidence ocihelper.ImageEvidence) workloadrunner.OCIImageObservation {
	return workloadrunner.OCIImageObservation{
		SubmittedReference: evidence.SubmittedReference, TopLevelDigest: evidence.TopLevelDigest,
		TopLevelMediaType: evidence.TopLevelMediaType, IndexDigest: evidence.IndexDigest,
		PlatformManifestDigest: evidence.PlatformManifestDigest,
		PlatformOS:             evidence.Platform.OS, PlatformArchitecture: evidence.Platform.Architecture, PlatformVariant: evidence.Platform.Variant,
		RuntimeHandler: evidence.RuntimeHandler, Snapshotter: evidence.Snapshotter,
	}
}

func processResult(response ocihelper.WatchResponse) contract.ProcessResult {
	result := contract.ProcessResult{ExitCode: response.ExitCode, OOM: response.OutOfMemory, LogEvidenceIncomplete: response.LogEvidenceIncomplete}
	if response.Signal != "" {
		result.ExitCode = nil
		result.Signal = signalName(response.Signal)
		result.TerminationCause = contract.TerminationCause(response.TerminationCause)
	}
	if response.RuntimeFailure != "" {
		result.ExitCode = nil
		result.RuntimeFailure = &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: response.RuntimeFailure}
	}
	return result
}

func signalName(signal ocihelper.Signal) string {
	if signal == ocihelper.SignalKILL {
		return "killed"
	}
	return "terminated"
}

func spawnResult(code contract.SpawnFailureCode, err error) workloadrunner.Result {
	return workloadrunner.Result{Outcome: contract.ProcessResult{SpawnError: &contract.SpawnFailure{Code: code, Message: err.Error()}}}
}

func runtimeFailure(err error) workloadrunner.Result {
	return workloadrunner.Result{Outcome: contract.ProcessResult{RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: err.Error()}}}
}

func reapAfterFailedStart(session *ocihelper.Session, authority ocihelper.AttemptAuthority) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = session.Signal(ctx, ocihelper.SignalRequest{Authority: authority, Signal: ocihelper.SignalKILL})
	_, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: authority})
	return err
}

var _ workloadrunner.WorkloadRuntime = (*Adapter)(nil)
var _ workloadrunner.PriorBootReaper = (*Adapter)(nil)
