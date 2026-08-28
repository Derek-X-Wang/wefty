package agent

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// CapabilityProbe exercises the local OCI runtime path. Capabilities contains
// the complete OCI-related set earned by this observation; process and future
// non-OCI facts remain owned by the agent's configured base set. Returning an
// error withdraws every OCI-related capability before the error is reported.
type CapabilityProbe interface {
	Probe(context.Context) (CapabilityProbeResult, error)
}

// OCIBootBarrier proves that a helper session exclusively owns an empty wefty
// runtime namespace. Ready is process-local admission state; L1 publication
// remains ordered by agentSession after pending removals and the probe.
type OCIBootBarrier interface {
	Ready() bool
	Ensure(context.Context) error
	Generation() (ocihelper.HelperSession, bool)
	SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool)
	SetLossHandler(func(ocihelper.HelperSession, error))
	Close() error
}

type ociBootBarrierReasoner interface {
	CapabilityReasonCode() contract.CapabilityReasonCode
}

type ociBootBarrierInvalidator interface {
	Invalidate()
}

func ociBootBarrierReason(barrier OCIBootBarrier) contract.CapabilityReasonCode {
	if reasoner, ok := barrier.(ociBootBarrierReasoner); ok {
		if reason := reasoner.CapabilityReasonCode(); reason.Valid() {
			return reason
		}
	}
	return contract.CapabilityReasonBootSweepFailed
}

// CapabilityProbeResult is the bounded, publishable portion of a functional
// probe. Diagnostic detail belongs in the returned error and remains local.
type CapabilityProbeResult struct {
	Capabilities        map[string]bool
	MissingCapabilities []string
	ReasonCode          contract.CapabilityReasonCode
}

// CapabilitySnapshot is the immutable agent-local capability view shared by
// admission, publication, and future doctor output. LastProbeAt advances for
// every completed probe even when the publishable observation is unchanged.
type CapabilitySnapshot struct {
	contract.CapabilityObservation
	LastProbeAt time.Time `json:"last_probe_at,omitempty"`
}

// capabilityState is the one process-wide source for registration, heartbeat,
// doctor, and local start admission. Probe serialization prevents a slower old
// result from being assigned a newer revision after a more recent event.
type capabilityState struct {
	mu                         sync.RWMutex
	claimPublication           sync.RWMutex
	probeMu                    sync.Mutex
	probeActive                bool
	clock                      Clock
	probe                      CapabilityProbe
	timeout                    time.Duration
	base                       map[string]bool
	current                    contract.CapabilityObservation
	lastProbeAt                time.Time
	pendingPublicationRevision int64
	ociSuppressionSequence     atomic.Uint64
}

func newCapabilityState(configured map[string]bool, probe CapabilityProbe, clock Clock, timeout time.Duration) *capabilityState {
	if timeout <= 0 {
		timeout = DefaultCapabilityProbeTimeout
	}
	base := normalizeConfiguredCapabilities(configured)
	for capability := range base {
		if isOCIProbeCapability(capability) {
			delete(base, capability)
		}
	}
	now := wallNow(clock).UTC().Round(0)
	return &capabilityState{
		clock: clock, probe: probe, timeout: timeout, base: base,
		current: contract.CapabilityObservation{
			Revision: 1, Capabilities: cloneCapabilities(base), ObservedAt: now, MissingCapabilities: []string{},
		},
	}
}

// refresh records a new observation before returning its diagnostic error.
// Callers therefore suppress local OCI starts even while heartbeat transport
// is blocked or unavailable.
func (state *capabilityState) refresh(ctx context.Context) error {
	return state.refreshValidated(ctx, nil)
}

// refreshValidated records a successful probe only when validate still proves
// the runtime identity while claim publication is fenced. This keeps the
// positive observation private until a helper loss racing the probe has been
// observed and converted to a restrictive result.
func (state *capabilityState) refreshValidated(ctx context.Context, validate func() error) error {
	if state == nil || state.probe == nil {
		return nil
	}
	state.probeMu.Lock()
	if state.probeActive {
		state.probeMu.Unlock()
		probeErr := errors.New("capability probe is still running")
		state.record(CapabilityProbeResult{}, probeErr)
		return probeErr
	}
	state.probeActive = true
	state.probeMu.Unlock()
	suppressionSequence := state.ociSuppressionSequence.Load()
	probeContext, cancel := context.WithTimeout(ctx, state.timeout)
	type probeResult struct {
		result CapabilityProbeResult
		err    error
	}
	completed := make(chan probeResult, 1)
	go func() {
		result, err := state.probe.Probe(probeContext)
		state.probeMu.Lock()
		state.probeActive = false
		state.probeMu.Unlock()
		completed <- probeResult{result: result, err: err}
	}()
	var result CapabilityProbeResult
	var probeErr error
	select {
	case completedProbe := <-completed:
		result, probeErr = completedProbe.result, completedProbe.err
	case <-probeContext.Done():
		probeErr = probeContext.Err()
	}
	cancel()
	return state.recordProbeResult(suppressionSequence, result, probeErr, validate)
}

func (state *capabilityState) suppressOCI(reason contract.CapabilityReasonCode, err error) {
	if state == nil {
		return
	}
	state.claimPublication.Lock()
	defer state.claimPublication.Unlock()
	state.suppressOCILocked(reason, err)
}

// suppressOCILocked records a restrictive observation while the caller holds
// claimPublication for a larger publication transaction.
func (state *capabilityState) suppressOCILocked(reason contract.CapabilityReasonCode, err error) {
	if state == nil {
		return
	}
	if err == nil {
		err = errors.New("OCI runtime is not admitted")
	}
	state.ociSuppressionSequence.Add(1)
	state.recordLocked(CapabilityProbeResult{ReasonCode: reason}, err)
}

// recordProbeResult discards a positive result that began before a concurrent
// runtime-loss callback withdrew OCI admission. The callback is synchronous,
// but the probe itself deliberately runs outside claimPublication, so this
// sequence closes the otherwise possible stale-positive overwrite.
func (state *capabilityState) recordProbeResult(
	suppressionSequence uint64,
	result CapabilityProbeResult,
	probeErr error,
	validate func() error,
) error {
	state.claimPublication.Lock()
	defer state.claimPublication.Unlock()
	if probeErr == nil && state.ociSuppressionSequence.Load() != suppressionSequence {
		return errors.New("capability probe was superseded by OCI runtime suppression")
	}
	if probeErr == nil && validate != nil {
		probeErr = validate()
		if probeErr != nil {
			result.ReasonCode = contract.CapabilityReasonBootSweepFailed
		}
	}
	state.recordLocked(result, probeErr)
	return probeErr
}

func (state *capabilityState) record(result CapabilityProbeResult, probeErr error) {
	// Linearize a completed observation after every claim RPC that began under
	// the prior snapshot. Once a restrictive refresh returns, no stale in-flight
	// claim can consume work created behind its publication barrier.
	state.claimPublication.Lock()
	defer state.claimPublication.Unlock()
	state.recordLocked(result, probeErr)
}

func (state *capabilityState) recordLocked(result CapabilityProbeResult, probeErr error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	capabilities := cloneCapabilities(state.base)
	missing := make(map[string]struct{}, len(result.MissingCapabilities)+1)
	for _, capability := range result.MissingCapabilities {
		capability = normalizeCapabilityName(capability)
		if capability != "" && isOCIProbeCapability(capability) {
			missing[capability] = struct{}{}
		}
	}
	if probeErr == nil {
		for capability, enabled := range result.Capabilities {
			capability = normalizeCapabilityName(capability)
			if capability != "" && isOCIProbeCapability(capability) {
				capabilities[capability] = enabled
			}
		}
		if !capabilities["kind:oci"] {
			missing["kind:oci"] = struct{}{}
		}
	} else {
		for capability, enabled := range state.current.Capabilities {
			if enabled && isOCIProbeCapability(capability) {
				missing[capability] = struct{}{}
			}
		}
		missing["kind:oci"] = struct{}{}
	}
	for capability := range missing {
		delete(capabilities, capability)
	}
	missingCapabilities := make([]string, 0, len(missing))
	for capability := range missing {
		missingCapabilities = append(missingCapabilities, capability)
	}
	sort.Strings(missingCapabilities)
	reason := normalizeCapabilityReason(result.ReasonCode)
	if len(missingCapabilities) == 0 {
		reason = ""
	} else if reason == "" {
		reason = contract.CapabilityReasonProbeFailed
	}
	now := wallNow(state.clock).UTC().Round(0)
	revision := state.current.Revision
	if !maps.Equal(state.current.Capabilities, capabilities) ||
		!slices.Equal(state.current.MissingCapabilities, missingCapabilities) || state.current.ReasonCode != reason {
		revision++
		state.pendingPublicationRevision = revision
	}
	state.current = contract.CapabilityObservation{
		Revision: revision, Capabilities: capabilities,
		ObservedAt: now, MissingCapabilities: missingCapabilities, ReasonCode: reason,
	}
	state.lastProbeAt = now
}

// adoptRestrictive learns the authoritative N+1 that L1 assigned atomically
// while registering this barrier-bound session.
func (state *capabilityState) adoptRestrictive(node l1.Node) error {
	if state == nil {
		return nil
	}
	if node.Capabilities["kind:oci"] || !slices.Contains(node.MissingCapabilities, "kind:oci") ||
		!node.CapabilityReasonCode.ValidOCIRestriction() {
		return errors.New("L1 did not atomically publish the restrictive OCI observation")
	}
	state.claimPublication.Lock()
	defer state.claimPublication.Unlock()
	state.mu.Lock()
	defer state.mu.Unlock()
	state.current = contract.CapabilityObservation{
		Revision:            node.CapabilityRevision,
		Capabilities:        cloneCapabilities(node.Capabilities),
		ObservedAt:          node.CapabilityObservedAt,
		MissingCapabilities: append([]string(nil), node.MissingCapabilities...),
		ReasonCode:          node.CapabilityReasonCode,
	}
	state.pendingPublicationRevision = 0
	return nil
}

func (state *capabilityState) beginClaim() func() {
	if state == nil {
		return func() {}
	}
	state.claimPublication.RLock()
	return state.claimPublication.RUnlock
}

func (state *capabilityState) snapshot() contract.CapabilityObservation {
	if state == nil {
		return contract.CapabilityObservation{Revision: 1, Capabilities: map[string]bool{}, MissingCapabilities: []string{}}
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	return cloneCapabilityObservation(state.current)
}

func (state *capabilityState) capabilitySnapshot() CapabilitySnapshot {
	if state == nil {
		return CapabilitySnapshot{CapabilityObservation: contract.CapabilityObservation{
			Revision: 1, Capabilities: map[string]bool{}, MissingCapabilities: []string{},
		}}
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	return CapabilitySnapshot{CapabilityObservation: cloneCapabilityObservation(state.current), LastProbeAt: state.lastProbeAt}
}

func cloneCapabilityObservation(observation contract.CapabilityObservation) contract.CapabilityObservation {
	observation.Capabilities = cloneCapabilities(observation.Capabilities)
	observation.MissingCapabilities = append([]string(nil), observation.MissingCapabilities...)
	return observation
}

// allowsClaim closes new claim admission as soon as a restrictive transition
// commits. A request already in flight began before that transition; local
// pre-start admission still reads the newly restricted snapshot.
func (state *capabilityState) allowsClaim() bool {
	if state == nil {
		return true
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	if state.pendingPublicationRevision != 0 {
		return false
	}
	return true
}

func (state *capabilityState) acknowledge(node l1.Node) {
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.pendingPublicationRevision == 0 || node.BootSessionID == "" ||
		node.CapabilityRevision < state.pendingPublicationRevision {
		return
	}
	if !maps.Equal(state.current.Capabilities, node.Capabilities) ||
		!slices.Equal(state.current.MissingCapabilities, node.MissingCapabilities) ||
		state.current.ReasonCode != node.CapabilityReasonCode {
		return
	}
	state.pendingPublicationRevision = 0
}

func (state *capabilityState) allows(spec contract.JobSpec) bool {
	observation := state.snapshot()
	return len(l1.MissingCapabilities(l1.RequiredCapabilities(spec), observation.Capabilities)) == 0
}

func isOCIProbeCapability(capability string) bool {
	capability = normalizeCapabilityName(capability)
	return capability == "kind:oci" || capability == "cgroup_v2" || capability == "apparmor" ||
		capability == "computer" ||
		strings.HasPrefix(capability, "runtime_handler:")
}

func normalizeCapabilityName(capability string) string {
	return strings.ToLower(strings.TrimSpace(capability))
}

func normalizeConfiguredCapabilities(configured map[string]bool) map[string]bool {
	normalized := make(map[string]bool, len(configured))
	for capability, enabled := range configured {
		capability = normalizeCapabilityName(capability)
		if capability != "" {
			normalized[capability] = enabled
		}
	}
	if legacyProcess, exists := normalized["process"]; exists {
		if _, canonical := normalized["kind:process"]; !canonical {
			normalized["kind:process"] = legacyProcess
		}
		delete(normalized, "process")
	}
	return normalized
}

func normalizeCapabilityReason(reason contract.CapabilityReasonCode) contract.CapabilityReasonCode {
	if reason.Valid() {
		return reason
	}
	return ""
}

func applyCapabilityObservation(registration contract.NodeRegistration, observation contract.CapabilityObservation) contract.NodeRegistration {
	registration.Capabilities = observation.Capabilities
	registration.CapabilityRevision = observation.Revision
	registration.CapabilityObservedAt = observation.ObservedAt
	registration.MissingCapabilities = observation.MissingCapabilities
	registration.CapabilityReasonCode = observation.ReasonCode
	return registration
}

func heartbeatRequest(bootSessionID string, observation contract.CapabilityObservation) l1.HeartbeatRequest {
	return l1.HeartbeatRequest{
		BootSessionID: bootSessionID, Capabilities: observation.Capabilities, CapabilityRevision: observation.Revision,
		CapabilityObservedAt: observation.ObservedAt, MissingCapabilities: observation.MissingCapabilities,
		CapabilityReasonCode: observation.ReasonCode,
	}
}
