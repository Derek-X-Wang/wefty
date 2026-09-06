// Package computerconformance owns the stable evidence vocabulary emitted by
// the Computer image conformance checker.
package computerconformance

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

const ReceiptVersion = 2
const ContainerdProfileNotRun = "harness profile is not the containerd wefty-v1 profile"

type Status string

const (
	StatusPass   Status = "PASS"
	StatusFail   Status = "FAIL"
	StatusNotRun Status = "NOT-RUN"
)

type Scope string

const (
	ScopeImage   Scope = "image"
	ScopeHarness Scope = "harness"
	ScopeProfile Scope = "containerd-profile"
)

type FailureReason string

const (
	FailureAssertionFailed  FailureReason = "assertion_failed"
	FailureMutationDetected FailureReason = "mutation_detected"
	FailureReadinessTimeout FailureReason = "readiness_timeout"
)

type ReadinessEvent string

const (
	ReadinessEventInputOracleReady     ReadinessEvent = "input_oracle_ready"
	ReadinessEventKeyObserverAdvanced  ReadinessEvent = "key_observer_advanced"
	ReadinessEventViewEndpointReady    ReadinessEvent = "view_endpoint_ready"
	ReadinessEventControlEndpointReady ReadinessEvent = "control_endpoint_ready"
	ReadinessEventFirstRFBFrame        ReadinessEvent = "first_rfb_frame"
)

type Check struct {
	ID             string         `json:"id"`
	Scope          Scope          `json:"scope"`
	Status         Status         `json:"status"`
	Summary        string         `json:"summary"`
	Detail         string         `json:"detail,omitempty"`
	FailureReason  FailureReason  `json:"failure_reason,omitempty"`
	ReadinessEvent ReadinessEvent `json:"readiness_event,omitempty"`
}

type Receipt struct {
	Version                     int       `json:"version"`
	Image                       string    `json:"image"`
	Runtime                     string    `json:"runtime"`
	Platform                    string    `json:"platform,omitempty"`
	StartedAt                   time.Time `json:"started_at"`
	FinishedAt                  time.Time `json:"finished_at"`
	Status                      Status    `json:"status"`
	ImageStatus                 Status    `json:"image_status"`
	HarnessStatus               Status    `json:"harness_status"`
	ContainerdProfileStatus     Status    `json:"containerd_profile_status"`
	Checks                      []Check   `json:"checks"`
	FirstBootReadinessSeconds   float64   `json:"first_boot_readiness_seconds,omitempty"`
	RestartBootReadinessSeconds float64   `json:"restart_boot_readiness_seconds,omitempty"`
	ProfilePersistent           *bool     `json:"profile_persistent,omitempty"`
	SignInPersistent            *bool     `json:"sign_in_persistent,omitempty"`
	RestartedEdgeRecovered      *bool     `json:"restarted_edge_recovered,omitempty"`
	// Compatibility fields remain publication inputs, but each is projected
	// from the named observed cells below rather than from the aggregate.
	Executed              bool                   `json:"executed"`
	RFBWebSocketV1        bool                   `json:"rfb_websocket_v1"`
	TransportAssertions   bool                   `json:"transport_assertions"`
	NegativeRows          CompatibilityNegatives `json:"negative_rows"`
	Endpoints             CompatibilityEndpoints `json:"endpoints"`
	Roles                 CompatibilityRoles     `json:"roles"`
	DriverSignalConsumed  bool                   `json:"driver_signal_consumed"`
	RootFSReadOnly        bool                   `json:"rootfs_read_only"`
	AttemptTmpfsDiscarded bool                   `json:"attempt_tmpfs_discarded"`
	Shm                   CompatibilityShm       `json:"shm"`
	ReadinessSeconds      float64                `json:"readiness_seconds"`
	Teardown              TeardownEvidence       `json:"teardown"`
}

type TeardownEvidence struct {
	RetriesUsed               int                   `json:"retries_used"`
	PermissionRepairPerformed bool                  `json:"permission_repair_performed"`
	PermissionRepairSeconds   float64               `json:"permission_repair_seconds,omitempty"`
	Observations              []TeardownObservation `json:"observations"`
	Leftovers                 []string              `json:"leftovers"`
}

type TeardownObservation struct {
	Reason string `json:"reason"`
	Detail string `json:"detail,omitempty"`
}

type CompatibilityNegatives struct {
	DriverFailClosed bool `json:"driver_fail_closed"`
}
type CompatibilityEndpoints struct {
	View    string `json:"view"`
	Control string `json:"control"`
}
type CompatibilityRoles struct {
	ViewProcessViewOnly       bool `json:"view_process_view_only"`
	ControlProcessInteractive bool `json:"control_process_interactive"`
	ViewPointerDiscarded      bool `json:"view_pointer_discarded"`
}
type CompatibilityShm struct {
	Private    bool `json:"private"`
	Conformant bool `json:"conformant"`
}

// Recorder starts every catalogued cell at NOT-RUN. A crash or early return
// therefore cannot silently turn missing evidence into a pass.
type Recorder struct {
	receipt Receipt
	index   map[string]int
}

func NewRecorder(image, runtimeName, platform string, startedAt time.Time) *Recorder {
	checks := make([]Check, len(CheckCatalog))
	index := make(map[string]int, len(CheckCatalog))
	for i, definition := range CheckCatalog {
		checks[i] = Check{ID: definition.ID, Scope: definition.Scope, Status: StatusNotRun, Summary: definition.Summary}
		index[definition.ID] = i
	}
	return &Recorder{receipt: Receipt{
		Version:   ReceiptVersion,
		Image:     image,
		Runtime:   runtimeName,
		Platform:  platform,
		StartedAt: startedAt.UTC(),
		Status:    StatusNotRun,
		Checks:    checks,
		Teardown: TeardownEvidence{
			Observations: make([]TeardownObservation, 0),
			Leftovers:    make([]string, 0),
		},
	}, index: index}
}

func (r *Recorder) Record(id string, status Status, detail string) error {
	reason := FailureReason("")
	if status == StatusFail {
		reason = FailureAssertionFailed
	}
	return r.RecordFailure(id, status, detail, reason, "")
}

func (r *Recorder) RecordFailure(id string, status Status, detail string, reason FailureReason, event ReadinessEvent) error {
	if status != StatusPass && status != StatusFail && status != StatusNotRun {
		return fmt.Errorf("invalid conformance status %q", status)
	}
	if status == StatusFail {
		if reason != FailureAssertionFailed && reason != FailureMutationDetected && reason != FailureReadinessTimeout {
			return fmt.Errorf("invalid conformance failure reason %q", reason)
		}
		if reason == FailureReadinessTimeout && !validReadinessEvent(event) {
			return fmt.Errorf("readiness timeout for %q has invalid event %q", id, event)
		}
	} else if reason != "" || event != "" {
		return fmt.Errorf("non-FAIL conformance check %q cannot carry failure evidence", id)
	}
	i, ok := r.index[id]
	if !ok {
		return fmt.Errorf("unknown Computer conformance check %q", id)
	}
	r.receipt.Checks[i].Status, r.receipt.Checks[i].Detail = status, detail
	r.receipt.Checks[i].FailureReason, r.receipt.Checks[i].ReadinessEvent = reason, event
	return nil
}

func validReadinessEvent(event ReadinessEvent) bool {
	switch event {
	case ReadinessEventInputOracleReady,
		ReadinessEventKeyObserverAdvanced,
		ReadinessEventViewEndpointReady,
		ReadinessEventControlEndpointReady,
		ReadinessEventFirstRFBFrame:
		return true
	default:
		return false
	}
}

func (r *Recorder) Finish(finishedAt time.Time) Receipt {
	r.receipt.FinishedAt = finishedAt.UTC()
	r.receipt.ImageStatus = aggregateScope(r.receipt.Checks, ScopeImage)
	r.receipt.HarnessStatus = aggregateScope(r.receipt.Checks, ScopeHarness)
	r.receipt.ContainerdProfileStatus = aggregateScope(r.receipt.Checks, ScopeProfile)
	r.receipt.Status = Aggregate([]Check{{Status: r.receipt.ImageStatus}, {Status: r.receipt.HarnessStatus}})
	r.projectObservedCompatibility()
	r.receipt.Checks = slices.Clone(r.receipt.Checks)
	return r.receipt
}

func (r *Recorder) projectObservedCompatibility() {
	passed := func(id string) bool {
		index, ok := r.index[id]
		return ok && index >= 0 && index < len(r.receipt.Checks) && r.receipt.Checks[index].ID == id && r.receipt.Checks[index].Status == StatusPass
	}
	all := func(ids ...string) bool {
		for _, id := range ids {
			if !passed(id) {
				return false
			}
		}
		return true
	}
	r.receipt.Executed = passed("runtime.started")
	r.receipt.RFBWebSocketV1 = all("transport.view-ready", "transport.control-ready")
	r.receipt.TransportAssertions = all("transport.plain-tcp-rejected", "transport.query-ignored", "transport.fragment-ignored", "transport.wrong-path-rejected", "transport.missing-subprotocol-rejected", "transport.wrong-subprotocol-rejected", "transport.text-frame-rejected")
	r.receipt.NegativeRows.DriverFailClosed = all("driver.malformed-fails-closed", "driver.unknown-version-fails-closed", "driver.missing-fails-closed")
	if passed("endpoints.view-loopback") {
		r.receipt.Endpoints.View = "loopback"
	}
	if passed("endpoints.control-loopback") {
		r.receipt.Endpoints.Control = "loopback"
	}
	viewIsolated := all("input.view-isolated", "input.view-isolated-during-tenure")
	r.receipt.Roles = CompatibilityRoles{ViewProcessViewOnly: viewIsolated, ControlProcessInteractive: passed("input.control-accepted"), ViewPointerDiscarded: viewIsolated}
	r.receipt.DriverSignalConsumed = all("driver.true-consumed", "driver.release-consumed")
	r.receipt.RootFSReadOnly = passed("harness.rootfs-read-only")
	r.receipt.AttemptTmpfsDiscarded = passed("persistence.rootfs-discarded")
	r.receipt.Shm = CompatibilityShm{Private: passed("harness.shm-private"), Conformant: all("harness.shm-size", "harness.shm-flags")}
	r.receipt.ReadinessSeconds = r.receipt.FirstBootReadinessSeconds
}

func (r *Recorder) RecordReadiness(restart bool, duration time.Duration) {
	if restart {
		r.receipt.RestartBootReadinessSeconds = duration.Seconds()
	} else {
		r.receipt.FirstBootReadinessSeconds = duration.Seconds()
	}
}
func (r *Recorder) RecordPersistence(profile, signIn bool) {
	r.receipt.ProfilePersistent, r.receipt.SignInPersistent = &profile, &signIn
}
func (r *Recorder) RecordEdgeRecovery(value bool) { r.receipt.RestartedEdgeRecovered = &value }

func (r *Recorder) RecordTeardownObservation(reason, detail string) {
	r.receipt.Teardown.Observations = append(r.receipt.Teardown.Observations, TeardownObservation{Reason: reason, Detail: detail})
}

func (r *Recorder) RecordTeardownRetry(reason, detail string) {
	r.receipt.Teardown.RetriesUsed++
	r.RecordTeardownObservation(reason, detail)
}

func (r *Recorder) RecordPermissionRepair(duration time.Duration) {
	r.receipt.Teardown.PermissionRepairPerformed = true
	r.receipt.Teardown.PermissionRepairSeconds += duration.Seconds()
}

func (r *Recorder) RecordTeardownLeftovers(leftovers []string) {
	r.receipt.Teardown.Leftovers = slices.Clone(leftovers)
}

func Aggregate(checks []Check) Status {
	status := StatusPass
	for _, check := range checks {
		switch check.Status {
		case StatusFail:
			return StatusFail
		case StatusNotRun:
			status = StatusNotRun
		case StatusPass:
		default:
			return StatusFail
		}
	}
	return status
}
func aggregateScope(checks []Check, scope Scope) Status {
	filtered := make([]Check, 0, len(checks))
	for _, check := range checks {
		if check.Scope == scope {
			filtered = append(filtered, check)
		}
	}
	return Aggregate(filtered)
}
func Marshal(receipt Receipt) ([]byte, error) { return json.MarshalIndent(receipt, "", "  ") }

type CheckDefinition struct {
	ID      string
	Scope   Scope
	Summary string
}

// CheckCatalog is append-only within receipt version 2. Stable IDs let CI and
// image authors compare evidence without scraping human prose.
var CheckCatalog = []CheckDefinition{
	{ID: "runtime.started", Scope: ScopeImage, Summary: "image starts under the Computer harness profile"},
	{ID: "runtime.image-config", Scope: ScopeImage, Summary: "image USER, ENTRYPOINT, CMD, and working directory retain OCI semantics"},
	{ID: "environment.service-dir", Scope: ScopeImage, Summary: "WEFTY_SERVICE_DIR is authoritative"},
	{ID: "environment.view-port", Scope: ScopeImage, Summary: "view port is authoritative"},
	{ID: "environment.control-port", Scope: ScopeImage, Summary: "control port is authoritative"},
	{ID: "environment.service-port-omitted", Scope: ScopeImage, Summary: "WEFTY_SERVICE_PORT is omitted"},
	{ID: "environment.handoff-dir-omitted", Scope: ScopeImage, Summary: "WEFTY_HANDOFF_DIR is omitted when no handoff is supplied"},
	{ID: "environment.authority-omitted", Scope: ScopeImage, Summary: "default-off Computer authority environment is omitted"},
	{ID: "environment.other-wefty-preserved", Scope: ScopeImage, Summary: "unreserved WEFTY_* environment remains tenant-owned"},
	{ID: "targets.service-nonshadowable", Scope: ScopeHarness, Summary: "/wefty/service hides image filesystem content"},
	{ID: "targets.control-nonshadowable", Scope: ScopeHarness, Summary: "/wefty/control hides image filesystem content"},
	{ID: "targets.handoff-nonshadowable", Scope: ScopeHarness, Summary: "/wefty/handoff hides image filesystem content"},
	{ID: "targets.token-mode", Scope: ScopeImage, Summary: "optional computer-token is absent or mode 0400"},
	{ID: "targets.endpoint-mode", Scope: ScopeImage, Summary: "optional l3-endpoint is absent or mode 0400"},
	{ID: "endpoints.distinct", Scope: ScopeHarness, Summary: "view and control use distinct allocated ports"},
	{ID: "endpoints.view-loopback", Scope: ScopeImage, Summary: "view binds IPv4 loopback only"},
	{ID: "endpoints.control-loopback", Scope: ScopeImage, Summary: "control binds IPv4 loopback only"},
	{ID: "transport.view-ready", Scope: ScopeImage, Summary: "view completes rfb-websocket-v1"},
	{ID: "transport.control-ready", Scope: ScopeImage, Summary: "control completes rfb-websocket-v1"},
	{ID: "transport.plain-tcp-rejected", Scope: ScopeImage, Summary: "plain TCP or HTTP without upgrade is not readiness"},
	{ID: "transport.query-ignored", Scope: ScopeImage, Summary: "query does not change routing"},
	{ID: "transport.fragment-ignored", Scope: ScopeImage, Summary: "fragment does not change routing"},
	{ID: "transport.wrong-path-rejected", Scope: ScopeImage, Summary: "wrong WebSocket path is rejected"},
	{ID: "transport.missing-subprotocol-rejected", Scope: ScopeImage, Summary: "missing binary subprotocol is rejected"},
	{ID: "transport.wrong-subprotocol-rejected", Scope: ScopeImage, Summary: "wrong subprotocol is rejected"},
	{ID: "transport.text-frame-rejected", Scope: ScopeImage, Summary: "text RFB frame is rejected"},
	{ID: "readiness.before-deadline", Scope: ScopeImage, Summary: "both endpoints are ready before the 60 second deadline"},
	{ID: "input.view-isolated", Scope: ScopeImage, Summary: "view discards pointer and observed key input while tenure is false"},
	{ID: "input.view-isolated-during-tenure", Scope: ScopeImage, Summary: "view discards pointer and observed key input while tenure is true"},
	{ID: "input.control-accepted", Scope: ScopeImage, Summary: "control accepts pointer and observed key input"},
	{ID: "driver.read-only", Scope: ScopeHarness, Summary: "driver.json is tenant read-only"},
	{ID: "driver.mode", Scope: ScopeHarness, Summary: "driver.json mode is exactly 0444"},
	{ID: "driver.initial-false", Scope: ScopeImage, Summary: "fresh attempt starts with exact false driver document"},
	{ID: "driver.true-consumed", Scope: ScopeImage, Summary: "tenant reopens and consumes true driver document"},
	{ID: "driver.release-consumed", Scope: ScopeImage, Summary: "tenant consumes the restored false document"},
	{ID: "driver.malformed-fails-closed", Scope: ScopeImage, Summary: "malformed and wrong-type driver documents fail closed after observation"},
	{ID: "driver.unknown-version-fails-closed", Scope: ScopeImage, Summary: "unknown driver version fails closed after observation"},
	{ID: "driver.missing-fails-closed", Scope: ScopeImage, Summary: "missing driver document fails closed after observation"},
	{ID: "harness.image-user", Scope: ScopeHarness, Summary: "Docker harness retains the image USER"},
	{ID: "harness.rootfs-read-only", Scope: ScopeHarness, Summary: "Docker harness root filesystem is read-only"},
	{ID: "harness.service-writable", Scope: ScopeHarness, Summary: "/wefty/service is the persistent writable path"},
	{ID: "harness.no-new-privileges", Scope: ScopeHarness, Summary: "Docker harness enables noNewPrivileges"},
	{ID: "harness.forbidden-privilege", Scope: ScopeHarness, Summary: "Docker harness adds no forbidden privilege"},
	{ID: "harness.shm-private", Scope: ScopeHarness, Summary: "/dev/shm is attempt-private"},
	{ID: "harness.shm-size", Scope: ScopeHarness, Summary: "/dev/shm ceiling is exactly 1 GiB"},
	{ID: "harness.shm-flags", Scope: ScopeHarness, Summary: "/dev/shm is mode 1777,nosuid,nodev,noexec"},
	{ID: "harness.tmp-ceilings", Scope: ScopeHarness, Summary: "/tmp and /var/tmp retain bounded ceilings"},
	{ID: "profile.capabilities", Scope: ScopeProfile, Summary: "containerd capability sets match wefty-v1"},
	{ID: "profile.seccomp", Scope: ScopeProfile, Summary: "containerd generated seccomp profile is active"},
	{ID: "profile.namespaces", Scope: ScopeProfile, Summary: "containerd namespace profile matches wefty-v1"},
	{ID: "profile.devices", Scope: ScopeProfile, Summary: "containerd deny-all device profile matches wefty-v1"},
	{ID: "profile.cgroup-memory-max", Scope: ScopeProfile, Summary: "memory.max equals the declared cap"},
	{ID: "profile.cgroup-oom-group", Scope: ScopeProfile, Summary: "memory.oom.group equals 1"},
	{ID: "profile.cgroup-swap-max", Scope: ScopeProfile, Summary: "memory.swap.max equals 0"},
	{ID: "persistence.service-survives", Scope: ScopeImage, Summary: "/wefty/service survives a fresh attempt"},
	{ID: "persistence.profile-survives", Scope: ScopeImage, Summary: "profile marker under image HOME survives restart"},
	{ID: "persistence.sign-in-survives", Scope: ScopeImage, Summary: "sign-in marker under image HOME survives restart"},
	{ID: "persistence.rootfs-discarded", Scope: ScopeImage, Summary: "attempt-local rootfs state does not survive"},
	{ID: "persistence.edge-recovers", Scope: ScopeImage, Summary: "killed WebSocket edge withdraws and both endpoints recover"},
	{ID: "targets.control-nonpersistent", Scope: ScopeImage, Summary: "/wefty/control state does not enter service data"},
}
