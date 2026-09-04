package ocicontrol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const (
	DoctorVersion            = 1
	DoctorUIDLimitation      = "process_kind_payload_uid_isolation_pending"
	DoctorUIDIssue           = "https://github.com/Derek-X-Wang/wefty/issues/220"
	DoctorLaunchUnmanaged    = "unmanaged"
	DoctorRunbookPrefix      = RunbookPath + "#doctor-code-"
	TestedContainerdVersion  = "2.3.4"
	TestedRuncVersion        = "1.5.1"
	MinimumContainerdVersion = "2.0.0"
	MinimumRuncVersion       = "1.0.0"
)

type DiagnosticOutcome string

const (
	DiagnosticOK     DiagnosticOutcome = "OK"
	DiagnosticFailed DiagnosticOutcome = "FAILED"
	DiagnosticNotRun DiagnosticOutcome = "NOT-RUN"
)

func (outcome DiagnosticOutcome) Valid() bool {
	return outcome == DiagnosticOK || outcome == DiagnosticFailed || outcome == DiagnosticNotRun
}

type DiagnosticSeverity string

const (
	DiagnosticInfo  DiagnosticSeverity = "INFO"
	DiagnosticWarn  DiagnosticSeverity = "WARN"
	DiagnosticError DiagnosticSeverity = "ERROR"
)

func (severity DiagnosticSeverity) Valid() bool {
	return severity == DiagnosticInfo || severity == DiagnosticWarn || severity == DiagnosticError
}

type NotRunCause string

const (
	NotRunSourceUnavailable  NotRunCause = "source_unavailable"
	NotRunHelperUnreachable  NotRunCause = "helper_unreachable"
	NotRunNotConfigured      NotRunCause = "not_configured"
	NotRunNotApplicable      NotRunCause = "not_applicable"
	NotRunNoProbeReceipt     NotRunCause = "no_probe_receipt"
	NotRunDependencyMissing  NotRunCause = "dependency_unavailable"
	NotRunDesiredUnavailable NotRunCause = "desired_unavailable"
)

func (cause NotRunCause) Valid() bool {
	switch cause {
	case NotRunSourceUnavailable, NotRunHelperUnreachable, NotRunNotConfigured, NotRunNotApplicable,
		NotRunNoProbeReceipt, NotRunDependencyMissing, NotRunDesiredUnavailable:
		return true
	default:
		return false
	}
}

type PlatformFacts struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	Variant      string `json:"variant,omitempty"`
}

type AgentFacts struct {
	User       string `json:"user"`
	LaunchUnit string `json:"launch_unit"`
}

type HelperDoctorFacts struct {
	Outcome                 DiagnosticOutcome `json:"outcome"`
	ProtocolVersion         int               `json:"protocol_version"`
	Version                 string            `json:"version,omitempty"`
	Checksum                string            `json:"checksum,omitempty"`
	InstanceID              string            `json:"instance_id,omitempty"`
	SessionGeneration       uint64            `json:"session_generation,omitempty"`
	HandshakeStalledWindows uint64            `json:"handshake_stalled_windows"`
}

type VersionFacts struct {
	Outcome            DiagnosticOutcome `json:"outcome"`
	Containerd         string            `json:"containerd,omitempty"`
	Runc               string            `json:"runc,omitempty"`
	RuncSource         string            `json:"runc_source,omitempty"`
	OutsideTestedRange bool              `json:"outside_tested_range"`
}

type ProbeDoctorFacts struct {
	Outcome                    DiagnosticOutcome             `json:"outcome"`
	Verdict                    string                        `json:"verdict"`
	ObservedAt                 *time.Time                    `json:"observed_at,omitempty"`
	AgeSeconds                 *int64                        `json:"age_seconds,omitempty"`
	ProbeRevision              int64                         `json:"probe_revision"`
	CapabilityRevision         int64                         `json:"capability_revision"`
	PendingPublicationRevision int64                         `json:"pending_publication_revision"`
	CapabilityObservedAt       time.Time                     `json:"capability_observed_at"`
	Capabilities               map[string]bool               `json:"capabilities"`
	MissingCapabilities        []string                      `json:"missing_capabilities"`
	ReasonCode                 contract.CapabilityReasonCode `json:"reason_code,omitempty"`
	CapabilityReasonCode       contract.CapabilityReasonCode `json:"capability_reason_code,omitempty"`
}

type IntentDoctorFacts struct {
	Outcome   DiagnosticOutcome `json:"outcome"`
	Version   int               `json:"version"`
	Revision  uint64            `json:"revision"`
	Enabled   bool              `json:"enabled"`
	UpdatedAt *time.Time        `json:"updated_at,omitempty"`
}

type CacheDoctorFacts struct {
	Outcome      DiagnosticOutcome             `json:"outcome"`
	Bytes        int64                         `json:"bytes"`
	CapBytes     int64                         `json:"cap_bytes"`
	WithinBound  bool                          `json:"within_bound"`
	LastEviction *ocihelper.ImageCacheEviction `json:"last_eviction,omitempty"`
}

type MountDoctorFacts struct {
	Outcome      DiagnosticOutcome `json:"outcome"`
	AllowedRoots []string          `json:"allowed_roots"`
}

type ProfileDoctorFacts struct {
	Outcome                   DiagnosticOutcome          `json:"outcome"`
	MemoryLimitBytes          int64                      `json:"memory_limit_bytes"`
	MemoryMaxBytes            int64                      `json:"memory_max_bytes"`
	MemoryOOMGroup            bool                       `json:"memory_oom_group"`
	MemorySwapMaxBytes        int64                      `json:"memory_swap_max_bytes"`
	ComputerTmpfsCeilingBytes int64                      `json:"computer_tmpfs_ceiling_bytes"`
	LargestTmpfsCeilingBytes  int64                      `json:"largest_tmpfs_ceiling_bytes"`
	Warnings                  []ocihelper.ProfileWarning `json:"warnings"`
}

type ComputerScreenIsolationDoctorFacts struct {
	Outcome                   DiagnosticOutcome `json:"outcome"`
	NetworkNamespacePresent   bool              `json:"network_namespace_present"`
	HostAbstractSocketVisible bool              `json:"host_abstract_socket_visible"`
}

type ConvergenceDoctorFacts struct {
	Outcome DiagnosticOutcome `json:"outcome"`
	Class   ConvergenceClass  `json:"class,omitempty"`
	State   *SetupState       `json:"state,omitempty"`
	Desired *SetupState       `json:"desired,omitempty"`
}

type LimaDoctorFacts struct {
	Applicable bool                 `json:"applicable"`
	Outcome    DiagnosticOutcome    `json:"outcome"`
	Facts      lima.SupervisorFacts `json:"facts,omitempty"`
}

type ComputerStorageRecoveryFacts struct {
	Outcome          DiagnosticOutcome                                 `json:"outcome"`
	DeferredCount    int                                               `json:"deferred_count"`
	QuarantinedCount int                                               `json:"quarantined_count"`
	Deferred         []ocihelper.ComputerStorageRecoveryInventoryEntry `json:"deferred"`
	Quarantined      []ocihelper.ComputerStorageRecoveryInventoryEntry `json:"quarantined"`
}

type DiagnosticFinding struct {
	Check       string                        `json:"check"`
	Outcome     DiagnosticOutcome             `json:"outcome"`
	Severity    DiagnosticSeverity            `json:"severity"`
	Code        string                        `json:"code"`
	ReasonCode  contract.CapabilityReasonCode `json:"reason_code,omitempty"`
	NotRunCause NotRunCause                   `json:"not_run_cause,omitempty"`
	Detail      string                        `json:"detail"`
	Runbook     string                        `json:"runbook"`
}

type DoctorLimitation struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
	Issue  string `json:"issue"`
}

type DoctorResponse struct {
	Version                 int                                 `json:"version"`
	ObservedAt              time.Time                           `json:"observed_at"`
	HostPlatform            PlatformFacts                       `json:"host_platform"`
	RuntimePlatform         *PlatformFacts                      `json:"runtime_platform,omitempty"`
	Agent                   AgentFacts                          `json:"agent"`
	Lima                    LimaDoctorFacts                     `json:"lima"`
	Helper                  HelperDoctorFacts                   `json:"helper"`
	Versions                VersionFacts                        `json:"versions"`
	Probe                   ProbeDoctorFacts                    `json:"probe"`
	Intent                  IntentDoctorFacts                   `json:"intent"`
	Cache                   CacheDoctorFacts                    `json:"cache"`
	Mounts                  MountDoctorFacts                    `json:"mounts"`
	Profile                 ProfileDoctorFacts                  `json:"profile"`
	ComputerScreenIsolation ComputerScreenIsolationDoctorFacts  `json:"computer_screen_isolation"`
	ResourceAdmission       *ocihelper.ResourceAdmissionReceipt `json:"resource_admission,omitempty"`
	Convergence             ConvergenceDoctorFacts              `json:"convergence"`
	ComputerStorageRecovery ComputerStorageRecoveryFacts        `json:"computer_storage_recovery"`
	Findings                []DiagnosticFinding                 `json:"findings"`
	Limitations             []DoctorLimitation                  `json:"limitations"`
}

type HelperDoctorSource func(context.Context) (HelperDoctorSnapshot, error)

type HelperDoctorSnapshot struct {
	ProtocolVersion         int
	Version                 string
	Checksum                string
	InstanceID              string
	SessionGeneration       uint64
	Runtime                 ocihelper.DoctorStatus
	RuntimeError            error
	RuntimePlatformRecorded bool
	SweepReceipt            ocihelper.VerifiedSweepReceipt
	SweepReceiptRecorded    bool
}

// CapabilitySnapshot is the immutable capability view consumed by doctor.
// Keeping this transport shape local prevents the operator-control package
// from depending on the agent implementation it controls.
type CapabilitySnapshot struct {
	contract.CapabilityObservation
	LastProbe                  *contract.CapabilityObservation `json:"last_probe,omitempty"`
	PendingPublicationRevision int64                           `json:"pending_publication_revision"`
}

type DoctorConfig struct {
	Clock                         Clock
	HostPlatform                  PlatformFacts
	AgentUser                     string
	LaunchUnit                    string
	CapabilitySnapshot            func() CapabilitySnapshot
	Intent                        func(context.Context) (lima.OCIIntent, error)
	LimaFacts                     func() lima.SupervisorFacts
	Helper                        HelperDoctorSource
	HelperHandshakeStalledWindows func() uint64
	SetupStatePath                string
	ReadSetupState                func(string) (SetupState, error)
	DesiredSetupStatePath         string
	ReadDesiredSetupState         func(string) (SetupState, error)
	InstalledSystemdVersion       func(context.Context) (int, error)
	InstalledHelperServiceUnit    func(context.Context) (string, error)
}

type diagnosticReceipt struct {
	ran         bool
	passed      bool
	code        string
	reasonCode  contract.CapabilityReasonCode
	notRunCause NotRunCause
	severity    DiagnosticSeverity
	detail      string
}

func finding(check string, receipt diagnosticReceipt) DiagnosticFinding {
	outcome := DiagnosticNotRun
	if receipt.ran && receipt.passed {
		outcome = DiagnosticOK
	} else if receipt.ran {
		outcome = DiagnosticFailed
	}
	code := receipt.code
	if code == "" {
		code = "oci_" + strings.ReplaceAll(check, "-", "_")
	}
	severity := receipt.severity
	if severity == "" {
		severity = DiagnosticWarn
		if outcome == DiagnosticOK {
			severity = DiagnosticInfo
		} else if outcome == DiagnosticFailed {
			severity = DiagnosticError
		}
	}
	reason := receipt.reasonCode
	cause := receipt.notRunCause
	if outcome == DiagnosticNotRun {
		reason = ""
		if !cause.Valid() {
			cause = NotRunSourceUnavailable
		}
	} else {
		cause = ""
	}
	return DiagnosticFinding{
		Check: check, Outcome: outcome, Severity: severity, Code: code, ReasonCode: reason, NotRunCause: cause,
		Detail: receipt.detail, Runbook: runbookFor(code),
	}
}

func runbookFor(code string) string {
	return DoctorRunbookPrefix + strings.ReplaceAll(code, "_", "-")
}

func BuildDoctor(ctx context.Context, config DoctorConfig) DoctorResponse {
	clock := config.Clock
	if clock == nil {
		clock = SystemClock{}
	}
	now := clock.Now().UTC().Round(0)
	host := config.HostPlatform
	if host.OS == "" {
		host = PlatformFacts{OS: runtime.GOOS, Architecture: runtime.GOARCH}
	}
	launchUnit := strings.TrimSpace(config.LaunchUnit)
	if launchUnit == "" {
		launchUnit = DoctorLaunchUnmanaged
	}
	report := DoctorResponse{
		Version: DoctorVersion, ObservedAt: now, HostPlatform: host,
		Agent:  AgentFacts{User: strings.TrimSpace(config.AgentUser), LaunchUnit: launchUnit},
		Lima:   LimaDoctorFacts{Applicable: host.OS == "darwin", Outcome: DiagnosticNotRun},
		Helper: HelperDoctorFacts{Outcome: DiagnosticNotRun}, Versions: VersionFacts{Outcome: DiagnosticNotRun},
		Probe:  ProbeDoctorFacts{Outcome: DiagnosticNotRun, Verdict: "not_run", Capabilities: map[string]bool{}, MissingCapabilities: []string{}},
		Intent: IntentDoctorFacts{Outcome: DiagnosticNotRun}, Cache: CacheDoctorFacts{Outcome: DiagnosticNotRun},
		Mounts:                  MountDoctorFacts{Outcome: DiagnosticNotRun, AllowedRoots: []string{}},
		Profile:                 ProfileDoctorFacts{Outcome: DiagnosticNotRun, Warnings: []ocihelper.ProfileWarning{}},
		ComputerScreenIsolation: ComputerScreenIsolationDoctorFacts{Outcome: DiagnosticNotRun},
		Convergence:             ConvergenceDoctorFacts{Outcome: DiagnosticNotRun},
		ComputerStorageRecovery: ComputerStorageRecoveryFacts{Outcome: DiagnosticNotRun,
			Deferred: []ocihelper.ComputerStorageRecoveryInventoryEntry{}, Quarantined: []ocihelper.ComputerStorageRecoveryInventoryEntry{}},
		Findings: []DiagnosticFinding{},
		Limitations: []DoctorLimitation{{
			Code:   DoctorUIDLimitation,
			Detail: "process-kind payloads currently share the agent user; operator peer credentials do not distinguish them",
			Issue:  DoctorUIDIssue,
		}},
	}
	report.Findings = append(report.Findings, finding("host-platform", diagnosticReceipt{
		ran: true, passed: host.OS != "" && host.Architecture != "", code: "oci_host_platform_observed", detail: "host platform was read without runtime mutation",
	}))
	report.Findings = append(report.Findings, finding("agent-user", diagnosticReceipt{
		ran: true, passed: report.Agent.User != "", code: "oci_agent_user_observed", detail: "agent user and launch unit were read from process configuration",
	}))

	buildIntent(ctx, config, &report)
	buildCapability(config, now, &report)
	buildLima(config, &report)
	buildConvergence(ctx, config, &report)
	buildHelperHandshakeStalls(config, &report)
	buildHelper(ctx, config, &report)
	return report
}

func buildHelperHandshakeStalls(config DoctorConfig, report *DoctorResponse) {
	if config.HelperHandshakeStalledWindows == nil {
		report.Findings = append(report.Findings, finding("helper-handshake-stalls", diagnosticReceipt{code: "oci_helper_handshake_stalls_not_read", notRunCause: NotRunSourceUnavailable, detail: "consecutive helper handshake stall windows were unavailable"}))
		return
	}
	count := config.HelperHandshakeStalledWindows()
	report.Helper.HandshakeStalledWindows = count
	passed := count == 0
	reason := contract.CapabilityReasonCode("")
	code := "oci_helper_handshake_stalls_clear"
	if !passed {
		reason = contract.CapabilityReasonHelperHandshakeStalled
		code = "oci_helper_handshake_stalls_observed"
	}
	report.Findings = append(report.Findings, finding("helper-handshake-stalls", diagnosticReceipt{ran: true, passed: passed, code: code, reasonCode: reason,
		detail: fmt.Sprintf("consecutive bounded helper handshake stall windows: %d", count)}))
}

func buildIntent(ctx context.Context, config DoctorConfig, report *DoctorResponse) {
	if config.Intent == nil {
		report.Findings = append(report.Findings, finding("intent", diagnosticReceipt{code: "oci_intent_not_read", notRunCause: NotRunSourceUnavailable, detail: "durable OCI intent was not available to the doctor"}))
		return
	}
	intent, err := config.Intent(ctx)
	if err != nil {
		report.Findings = append(report.Findings, finding("intent", diagnosticReceipt{code: "oci_intent_not_read", notRunCause: NotRunSourceUnavailable, detail: "durable OCI intent could not be read"}))
		return
	}
	if intent.Version != lima.OCIIntentVersion || intent.Revision == 0 {
		report.Intent.Outcome = DiagnosticFailed
		report.Findings = append(report.Findings, finding("intent", diagnosticReceipt{ran: true, code: "oci_intent_unavailable", reasonCode: contract.CapabilityReasonPrerequisiteMissing, detail: "durable OCI intent was read but is malformed"}))
		return
	}
	updatedAt := intent.UpdatedAt.UTC().Round(0)
	report.Intent = IntentDoctorFacts{Outcome: DiagnosticOK, Version: intent.Version, Revision: intent.Revision, Enabled: intent.Enabled, UpdatedAt: &updatedAt}
	reason := contract.CapabilityReasonCode("")
	code := "oci_intent_enabled"
	passed := intent.Enabled
	if !intent.Enabled {
		reason = contract.CapabilityReasonOCIIntentDisabled
		code = "oci_intent_disabled"
	}
	report.Intent.Outcome = outcomeFor(true, passed)
	report.Findings = append(report.Findings, finding("intent", diagnosticReceipt{ran: true, passed: passed, code: code, reasonCode: reason, detail: "durable OCI intent was read without mutation"}))
}

func buildCapability(config DoctorConfig, now time.Time, report *DoctorResponse) {
	if config.CapabilitySnapshot == nil {
		report.Findings = append(report.Findings, finding("probe", diagnosticReceipt{code: "oci_probe_not_recorded", notRunCause: NotRunSourceUnavailable, detail: "shared capability observation was unavailable"}))
		report.Findings = append(report.Findings, finding("capability-observation", diagnosticReceipt{code: "oci_capability_observation_not_read", notRunCause: NotRunSourceUnavailable, detail: "shared capability observation was unavailable"}))
		report.Findings = append(report.Findings, finding("capability-revision", diagnosticReceipt{code: "oci_capability_revision_not_read", notRunCause: NotRunSourceUnavailable, detail: "capability revision was not read"}))
		return
	}
	snapshot := config.CapabilitySnapshot()
	observation := snapshot.CapabilityObservation
	report.Probe.CapabilityRevision = observation.Revision
	report.Probe.PendingPublicationRevision = snapshot.PendingPublicationRevision
	report.Probe.CapabilityObservedAt = observation.ObservedAt.UTC().Round(0)
	report.Probe.Capabilities = cloneBoolMap(observation.Capabilities)
	report.Probe.MissingCapabilities = append([]string{}, observation.MissingCapabilities...)
	report.Probe.CapabilityReasonCode = observation.ReasonCode
	validTuple := observation.Revision > 0 && !observation.ObservedAt.IsZero() &&
		(len(observation.MissingCapabilities) == 0 && observation.ReasonCode == "" || len(observation.MissingCapabilities) > 0 && observation.ReasonCode.Valid())
	revisionCurrent := validTuple && snapshot.PendingPublicationRevision == 0
	code := "oci_capability_revision_current"
	reason := contract.CapabilityReasonCode("")
	if !revisionCurrent {
		code = "oci_capability_revision_pending"
		reason = observation.ReasonCode
		if !reason.Valid() {
			reason = ""
		}
	}
	report.Findings = append(report.Findings, finding("capability-revision", diagnosticReceipt{ran: true, passed: revisionCurrent, code: code, reasonCode: reason, detail: "the complete L1 capability metadata tuple was read from shared agent state"}))
	observationPassed := validTuple && observation.Capabilities["kind:oci"] && observation.ReasonCode == ""
	observationReason := observation.ReasonCode
	if !observationPassed && !observationReason.Valid() {
		observationReason = contract.CapabilityReasonProbeFailed
	}
	observationCode := "oci_capability_observation_restricted"
	if observationPassed {
		observationCode = "oci_capability_observation_current"
	}
	report.Findings = append(report.Findings, finding("capability-observation", diagnosticReceipt{ran: true, passed: observationPassed, code: observationCode, reasonCode: observationReason, detail: "the current admission and L1 capability observation was read independently of the last probe receipt"}))

	if snapshot.LastProbe == nil || snapshot.LastProbe.ObservedAt.IsZero() {
		report.Probe.Outcome = DiagnosticNotRun
		report.Findings = append(report.Findings, finding("probe", diagnosticReceipt{code: "oci_probe_not_recorded", notRunCause: NotRunNoProbeReceipt, detail: "no completed functional probe receipt is recorded"}))
		return
	}
	lastProbe := snapshot.LastProbe
	probeAt := lastProbe.ObservedAt.UTC().Round(0)
	age := now.Sub(probeAt) / time.Second
	if age < 0 {
		age = 0
	}
	ageSeconds := int64(age)
	report.Probe.ObservedAt = &probeAt
	report.Probe.AgeSeconds = &ageSeconds
	report.Probe.ProbeRevision = lastProbe.Revision
	passed := lastProbe.Capabilities["kind:oci"] && lastProbe.ReasonCode == ""
	report.Probe.Outcome = outcomeFor(true, passed)
	report.Probe.Verdict = "failed"
	if passed {
		report.Probe.Verdict = "passed"
	}
	reason = lastProbe.ReasonCode
	if !passed && !reason.Valid() {
		reason = contract.CapabilityReasonProbeFailed
	}
	report.Findings = append(report.Findings, finding("probe", diagnosticReceipt{ran: true, passed: passed, code: "oci_probe_" + report.Probe.Verdict, reasonCode: reason, detail: "the recorded functional-probe observation was reused; no probe ran"}))
}

func buildLima(config DoctorConfig, report *DoctorResponse) {
	if !report.Lima.Applicable {
		report.Findings = append(report.Findings, finding("lima", diagnosticReceipt{code: "oci_lima_not_applicable", notRunCause: NotRunNotApplicable, detail: "Lima is not applicable to this host platform"}))
		return
	}
	if config.LimaFacts == nil {
		report.Findings = append(report.Findings, finding("lima", diagnosticReceipt{code: "oci_lima_not_observed", notRunCause: NotRunSourceUnavailable, detail: "Lima supervisor facts were unavailable"}))
		return
	}
	facts := config.LimaFacts()
	report.Lima.Facts = facts
	passed := facts.State == lima.InstanceRunning && facts.Enabled && !facts.Recovering
	reason := facts.ReasonCode
	if !passed && !reason.Valid() {
		switch facts.State {
		case lima.InstanceStopped:
			reason = contract.CapabilityReasonLimaStopped
		case lima.InstanceBroken:
			reason = contract.CapabilityReasonLimaBroken
		default:
			reason = contract.CapabilityReasonProbeFailed
		}
	}
	report.Lima.Outcome = outcomeFor(true, passed)
	report.Findings = append(report.Findings, finding("lima", diagnosticReceipt{ran: true, passed: passed, code: "oci_lima_state", reasonCode: reason, detail: "recorded Lima supervisor facts were read without inspection or recovery"}))
}

func buildHelper(ctx context.Context, config DoctorConfig, report *DoctorResponse) {
	if config.Helper == nil {
		appendHelperNotRun(report, "OCI helper status source was unavailable")
		return
	}
	snapshot, err := config.Helper(ctx)
	if err != nil && snapshot.ProtocolVersion == 0 && snapshot.Version == "" && snapshot.InstanceID == "" {
		report.Helper.Outcome = DiagnosticFailed
		report.Findings = append(report.Findings, finding("helper-handshake", diagnosticReceipt{ran: true, code: "oci_helper_unreachable", reasonCode: contract.CapabilityReasonHelperUnreachable, detail: "the current helper session was absent or unreachable"}))
		appendHelperDependentsNotRun(report, NotRunHelperUnreachable)
		return
	}
	validHandshake := snapshot.ProtocolVersion == ocihelper.ProtocolVersion && snapshot.Version != "" && snapshot.Checksum != "" && snapshot.InstanceID != "" && snapshot.SessionGeneration > 0
	reason := contract.CapabilityReasonCode("")
	code := "oci_helper_handshake_ok"
	if !validHandshake {
		if snapshot.ProtocolVersion != 0 && snapshot.ProtocolVersion != ocihelper.ProtocolVersion {
			reason = contract.CapabilityReasonHelperVersionMismatch
			code = "oci_helper_version_mismatch"
		} else {
			reason = contract.CapabilityReasonHelperHandshakeFailed
			code = "oci_helper_handshake_failed"
		}
	}
	report.Helper = HelperDoctorFacts{
		Outcome: outcomeFor(true, validHandshake), ProtocolVersion: snapshot.ProtocolVersion,
		Version: snapshot.Version, Checksum: snapshot.Checksum, InstanceID: snapshot.InstanceID,
		SessionGeneration: snapshot.SessionGeneration, HandshakeStalledWindows: report.Helper.HandshakeStalledWindows,
	}
	report.Findings = append(report.Findings, finding("helper-handshake", diagnosticReceipt{ran: true, passed: validHandshake, code: code, reasonCode: reason, detail: "the existing authenticated helper handshake was read; no session was acquired"}))
	if !validHandshake {
		appendHelperDependentsNotRun(report, NotRunDependencyMissing)
		return
	}

	sweepOK := snapshot.SweepReceiptRecorded && snapshot.SweepReceipt.SweepEpoch != "" &&
		snapshot.SweepReceipt.HelperSession.HelperInstanceID == snapshot.InstanceID &&
		snapshot.SweepReceipt.HelperSession.SessionGeneration == snapshot.SessionGeneration &&
		snapshot.SweepReceipt.VerifiedAbsent &&
		snapshot.SweepReceipt.ComputerStorageDeferredCount == len(snapshot.SweepReceipt.VerifiedRetained.ComputerStorageDeferred) &&
		snapshot.SweepReceipt.ComputerStorageQuarantinedCount == len(snapshot.SweepReceipt.VerifiedRetained.ComputerStorageQuarantined)
	if snapshot.SweepReceiptRecorded {
		report.Findings = append(report.Findings, finding("boot-sweep", diagnosticReceipt{ran: true, passed: sweepOK, code: map[bool]string{true: "oci_boot_sweep_verified", false: "oci_boot_sweep_failed"}[sweepOK], reasonCode: reasonUnless(sweepOK, contract.CapabilityReasonBootSweepFailed), detail: "the barrier-pinned namespace sweep receipt was checked without running a sweep"}))
		if sweepOK {
			deferred := slices.Clone(snapshot.SweepReceipt.VerifiedRetained.ComputerStorageDeferred)
			quarantined := slices.Clone(snapshot.SweepReceipt.VerifiedRetained.ComputerStorageQuarantined)
			clear := len(deferred) == 0 && len(quarantined) == 0
			report.ComputerStorageRecovery = ComputerStorageRecoveryFacts{Outcome: outcomeFor(true, clear), DeferredCount: len(deferred), QuarantinedCount: len(quarantined), Deferred: deferred, Quarantined: quarantined}
			code := "oci_computer_storage_recovery_clear"
			if !clear {
				code = "oci_computer_storage_recovery_retained"
			}
			report.Findings = append(report.Findings, finding("computer-storage-recovery", diagnosticReceipt{ran: true, passed: clear, code: code, severity: DiagnosticWarn,
				detail: fmt.Sprintf("typed retained Computer Storage generations: deferred=%d quarantined=%d", len(deferred), len(quarantined))}))
		} else {
			appendComputerStorageRecoveryNotRun(report, NotRunDependencyMissing, "the boot-sweep receipt did not validate")
		}
	} else {
		report.Findings = append(report.Findings, finding("boot-sweep", diagnosticReceipt{code: "oci_boot_sweep_not_recorded", notRunCause: NotRunSourceUnavailable, detail: "no barrier-pinned namespace sweep receipt was available"}))
		appendComputerStorageRecoveryNotRun(report, NotRunSourceUnavailable, "no barrier-pinned namespace sweep receipt was available")
	}

	runtimeStatus := snapshot.Runtime
	platform := PlatformFacts{OS: runtimeStatus.RuntimePlatform.OS, Architecture: runtimeStatus.RuntimePlatform.Architecture, Variant: runtimeStatus.RuntimePlatform.Variant}
	platformOK := snapshot.RuntimePlatformRecorded && platform.OS != "" && platform.Architecture != ""
	if platformOK {
		report.RuntimePlatform = &platform
	}
	platformCode := "oci_runtime_platform_not_recorded"
	if snapshot.RuntimePlatformRecorded {
		platformCode = "oci_runtime_platform_observed"
	}
	report.Findings = append(report.Findings, finding("runtime-platform", diagnosticReceipt{ran: snapshot.RuntimePlatformRecorded, passed: platformOK, code: platformCode, notRunCause: NotRunNoProbeReceipt, detail: "runtime platform was read from the recorded functional probe"}))

	if snapshot.RuntimeError == nil {
		snapshot.RuntimeError = err
	}
	if snapshot.RuntimeError != nil {
		appendMechanicsDependentsNotRun(report, NotRunSourceUnavailable)
		return
	}

	containerdRead := runtimeStatus.ContainerdRead.Outcome == ocihelper.DiagnosticReadOK || runtimeStatus.ContainerdRead.Outcome == "" && runtimeStatus.ContainerdVersion != ""
	runcRead := runtimeStatus.RuncRead.Outcome == ocihelper.DiagnosticReadOK || runtimeStatus.RuncRead.Outcome == "" && runtimeStatus.RuncVersion != ""
	versionsRan := runtimeStatus.ContainerdRead.Outcome != "" || runtimeStatus.RuncRead.Outcome != "" || containerdRead || runcRead
	versionsRead := containerdRead && runcRead
	versionsSupported := versionsRead && supportedRuntimeVersions(runtimeStatus.ContainerdVersion, runtimeStatus.RuncVersion)
	outOfRange := versionsSupported && !testedRuntimeVersions(runtimeStatus.ContainerdVersion, runtimeStatus.RuncVersion)
	report.Versions = VersionFacts{Outcome: outcomeFor(versionsRan, versionsSupported), Containerd: runtimeStatus.ContainerdVersion, Runc: runtimeStatus.RuncVersion, RuncSource: runtimeStatus.RuncVersionSource, OutsideTestedRange: outOfRange}
	versionCode := "oci_runtime_versions_observed"
	severity := DiagnosticSeverity("")
	reasonCode := contract.CapabilityReasonCode("")
	if !versionsRead {
		versionCode = "oci_runtime_versions_unavailable"
	} else if !versionsSupported {
		versionCode = "oci_runtime_versions_unsupported"
		reasonCode = contract.CapabilityReasonRuntimeVersionUnsupported
		severity = DiagnosticError
	} else if outOfRange {
		versionCode = "oci_runtime_versions_outside_tested_range"
		severity = DiagnosticWarn
	} else {
		severity = DiagnosticInfo
	}
	report.Findings = append(report.Findings, finding("runtime-versions", diagnosticReceipt{ran: versionsRan, passed: versionsSupported, code: versionCode, severity: severity, reasonCode: reasonCode, notRunCause: NotRunSourceUnavailable, detail: "containerd and runc versions were compared with the supported minimums"}))

	cacheRead := runtimeStatus.CacheRead.Outcome == ocihelper.DiagnosticReadOK || runtimeStatus.CacheRead.Outcome == "" && runtimeStatus.Cache.CapBytes > 0
	cacheRan := runtimeStatus.CacheRead.Outcome != "" || cacheRead
	cache := runtimeStatus.Cache
	cacheOK := cacheRead && cache.CapBytes > 0 && cache.Bytes >= 0 && cache.Bytes <= cache.CapBytes && runtimeStatus.CacheLastErrorCode == ""
	cacheCodeValue := cacheCode(cacheOK)
	if runtimeStatus.CacheLastErrorCode != "" {
		cacheCodeValue = "oci_cache_eviction_failed"
	} else if !cacheRead {
		cacheCodeValue = "oci_cache_status_unavailable"
	}
	report.Cache = CacheDoctorFacts{Outcome: outcomeFor(cacheRan, cacheOK), Bytes: cache.Bytes, CapBytes: cache.CapBytes, WithinBound: cacheOK, LastEviction: cache.LastEviction}
	report.Findings = append(report.Findings, finding("cache", diagnosticReceipt{ran: cacheRan, passed: cacheOK, code: cacheCodeValue, notRunCause: NotRunSourceUnavailable, detail: "bounded cache accounting was read without enforcing or evicting"}))

	if runtimeStatus.LastProfile == nil {
		report.Findings = append(report.Findings, finding("profile-ceilings", diagnosticReceipt{code: "oci_profile_ceilings_not_recorded", notRunCause: NotRunNoProbeReceipt, detail: "no completed runtime profile receipt was recorded"}))
		report.Findings = append(report.Findings, finding("computer-screen-isolation", diagnosticReceipt{code: "oci_computer_screen_isolation_not_recorded", notRunCause: NotRunNoProbeReceipt, detail: "no completed Computer runtime profile receipt was recorded"}))
	} else {
		profile := runtimeStatus.LastProfile
		report.Profile = ProfileDoctorFacts{Outcome: DiagnosticOK, MemoryLimitBytes: profile.MemoryLimitBytes,
			MemoryMaxBytes: profile.MemoryMaxBytes, MemoryOOMGroup: profile.MemoryOOMGroup, MemorySwapMaxBytes: profile.MemorySwapMaxBytes,
			ComputerTmpfsCeilingBytes: profile.ComputerTmpfsCeilingBytes, LargestTmpfsCeilingBytes: profile.LargestTmpfsCeilingBytes,
			Warnings: append([]ocihelper.ProfileWarning{}, profile.Warnings...)}
		code := "oci_profile_tmpfs_ceilings_within_memory_limit"
		severity := DiagnosticInfo
		detail := "the last assertion-derived runtime profile receipt has tmpfs ceilings within its memory limit"
		if len(profile.Warnings) > 0 {
			code = "oci_profile_tmpfs_ceilings_exceed_memory_limit"
			severity = DiagnosticWarn
			detail = "tmpfs ceilings are caps rather than reservations; the last profile can reach its cgroup memory limit before those ceilings"
		}
		report.Findings = append(report.Findings, finding("profile-ceilings", diagnosticReceipt{ran: true, passed: true, code: code, severity: severity, detail: detail}))
		if profile.Computer {
			enforced := profile.NetworkNamespacePresent && !profile.HostAbstractSocketVisible
			report.ComputerScreenIsolation = ComputerScreenIsolationDoctorFacts{
				Outcome:                   outcomeFor(true, enforced),
				NetworkNamespacePresent:   profile.NetworkNamespacePresent,
				HostAbstractSocketVisible: profile.HostAbstractSocketVisible,
			}
			isolationCode := "oci_computer_screen_isolation_enforced"
			if !enforced {
				isolationCode = "oci_computer_screen_isolation_not_enforced"
			}
			report.Findings = append(report.Findings, finding("computer-screen-isolation", diagnosticReceipt{ran: true, passed: enforced, code: isolationCode, detail: "the last assertion-derived Computer profile receipt proves a private network namespace and no host-visible abstract X11 socket"}))
		} else {
			report.Findings = append(report.Findings, finding("computer-screen-isolation", diagnosticReceipt{code: "oci_computer_screen_isolation_not_recorded", notRunCause: NotRunNoProbeReceipt, detail: "the last completed runtime profile receipt was not for a Computer"}))
		}
	}
	if runtimeStatus.LastAdmission == nil {
		report.Findings = append(report.Findings, finding("resource-admission", diagnosticReceipt{code: "oci_resource_admission_not_recorded", notRunCause: NotRunNoProbeReceipt, detail: "no atomic resource-admission receipt was recorded"}))
	} else {
		receipt := *runtimeStatus.LastAdmission
		receipt.Warnings = append([]ocihelper.ProfileWarning{}, receipt.Warnings...)
		report.ResourceAdmission = &receipt
		code := "oci_resource_admission_admitted"
		severity := DiagnosticInfo
		detail := "the last atomic cap decision admitted the newcomer; timestamped memory/filesystem facts were read without forecasting fit"
		if !receipt.Admitted {
			code = "oci_resource_admission_refused"
			severity = DiagnosticWarn
			detail = "the last atomic cap decision refused the newcomer with a typed resource code; resident reservations were unchanged"
		}
		report.Findings = append(report.Findings, finding("resource-admission", diagnosticReceipt{ran: true, passed: receipt.Admitted, code: code, severity: severity, detail: detail}))
	}

	buildMountRoots(runtimeStatus, report)
}

func appendHelperNotRun(report *DoctorResponse, detail string) {
	report.Findings = append(report.Findings, finding("helper-handshake", diagnosticReceipt{code: "oci_helper_not_read", notRunCause: NotRunNotConfigured, detail: detail}))
	report.Findings = append(report.Findings, finding("boot-sweep", diagnosticReceipt{code: "oci_boot_sweep_not_recorded", notRunCause: NotRunDependencyMissing, detail: "the helper handshake did not run"}))
	appendComputerStorageRecoveryNotRun(report, NotRunDependencyMissing, "the dependent boot-sweep receipt was unavailable")
	appendRuntimeDependentsNotRun(report, NotRunDependencyMissing)
}

func appendHelperDependentsNotRun(report *DoctorResponse, cause NotRunCause) {
	report.Findings = append(report.Findings, finding("boot-sweep", diagnosticReceipt{code: "oci_boot_sweep_not_recorded", notRunCause: cause, detail: "the dependent helper receipt read did not run"}))
	appendComputerStorageRecoveryNotRun(report, cause, "the dependent boot-sweep receipt read did not run")
	appendRuntimeDependentsNotRun(report, cause)
}

func appendComputerStorageRecoveryNotRun(report *DoctorResponse, cause NotRunCause, detail string) {
	report.ComputerStorageRecovery.Outcome = DiagnosticNotRun
	report.Findings = append(report.Findings, finding("computer-storage-recovery", diagnosticReceipt{
		code: "oci_computer_storage_recovery_not_read", notRunCause: cause, detail: detail,
	}))
}

func appendRuntimeDependentsNotRun(report *DoctorResponse, cause NotRunCause) {
	for _, check := range []string{"runtime-platform", "runtime-versions", "cache", "profile-ceilings", "computer-screen-isolation", "resource-admission", "mount-roots"} {
		report.Findings = append(report.Findings, finding(check, diagnosticReceipt{code: "oci_" + strings.ReplaceAll(check, "-", "_") + "_not_run", notRunCause: cause, detail: "the dependent helper read did not run"}))
	}
}

func appendMechanicsDependentsNotRun(report *DoctorResponse, cause NotRunCause) {
	for _, check := range []string{"runtime-versions", "cache", "profile-ceilings", "computer-screen-isolation", "resource-admission", "mount-roots"} {
		report.Findings = append(report.Findings, finding(check, diagnosticReceipt{code: "oci_" + strings.ReplaceAll(check, "-", "_") + "_not_run", notRunCause: cause, detail: "the dependent helper read did not run"}))
	}
}

func buildConvergence(ctx context.Context, config DoctorConfig, report *DoctorResponse) {
	if config.SetupStatePath == "" {
		report.Findings = append(report.Findings, finding("convergence", diagnosticReceipt{code: "oci_convergence_not_read", notRunCause: NotRunNotConfigured, detail: "durable setup convergence state was not configured"}))
		appendHelperRestartPolicyNotRun(report, NotRunNotConfigured, "durable systemd and helper restart-policy state was not configured")
		return
	}
	read := config.ReadSetupState
	if read == nil {
		read = ReadSetupState
	}
	current, err := read(config.SetupStatePath)
	if err != nil {
		report.Convergence.Outcome = DiagnosticFailed
		report.Findings = append(report.Findings, finding("convergence", diagnosticReceipt{ran: true, code: "oci_convergence_state_unavailable", reasonCode: contract.CapabilityReasonPrerequisiteMissing, detail: "durable setup convergence state is missing, unreadable, or malformed"}))
		appendHelperRestartPolicyNotRun(report, NotRunSourceUnavailable, "installed systemd and helper restart-policy state was unavailable")
		return
	}
	report.Convergence.State = &current
	desiredPath := config.DesiredSetupStatePath
	if desiredPath == "" {
		desiredPath = DesiredSetupStatePath(config.SetupStatePath)
	}
	readDesired := config.ReadDesiredSetupState
	if readDesired == nil {
		readDesired = read
	}
	if desiredPath == "" {
		report.Findings = append(report.Findings, finding("convergence", diagnosticReceipt{code: "oci_convergence_desired_not_read", notRunCause: NotRunDesiredUnavailable, detail: "desired setup state was not configured"}))
		appendHelperRestartPolicyNotRun(report, NotRunDesiredUnavailable, "desired systemd and helper restart-policy state was not configured")
		return
	}
	desired, desiredErr := readDesired(desiredPath)
	if desiredErr != nil {
		report.Findings = append(report.Findings, finding("convergence", diagnosticReceipt{code: "oci_convergence_desired_not_read", notRunCause: NotRunDesiredUnavailable, detail: "desired setup state was unavailable"}))
		appendHelperRestartPolicyNotRun(report, NotRunDesiredUnavailable, "desired systemd and helper restart-policy state was unavailable")
		return
	}
	report.Convergence.Desired = &desired
	class := ClassifyConvergence(current, desired)
	passed := class == ConvergenceUnchanged || class == ConvergenceLiveSafe
	reason := contract.CapabilityReasonCode("")
	if class == ConvergenceRestartRequired {
		reason = contract.CapabilityReasonTemplateRestartRequired
	} else if class == ConvergenceRecreateRequired {
		reason = contract.CapabilityReasonTemplateRecreateRequired
	}
	report.Convergence.Outcome = outcomeFor(true, passed)
	report.Convergence.Class = class
	report.Findings = append(report.Findings, finding("convergence", diagnosticReceipt{ran: true, passed: passed, code: "oci_convergence_" + string(class), reasonCode: reason, detail: "current and desired durable setup states were compared without applying them"}))
	if config.InstalledSystemdVersion == nil {
		appendHelperRestartPolicyNotRun(report, NotRunSourceUnavailable, "the installed systemd version source was unavailable")
		return
	}
	installedSystemdVersion, versionErr := config.InstalledSystemdVersion(ctx)
	if versionErr != nil || installedSystemdVersion <= 0 {
		appendHelperRestartPolicyNotRun(report, NotRunSourceUnavailable, "the installed systemd version could not be read")
		return
	}
	if config.InstalledHelperServiceUnit == nil {
		appendHelperRestartPolicyNotRun(report, NotRunSourceUnavailable, "the installed helper service unit source was unavailable")
		return
	}
	installedUnit, unitErr := config.InstalledHelperServiceUnit(ctx)
	if unitErr != nil {
		appendHelperRestartPolicyNotRun(report, NotRunSourceUnavailable, "the installed helper service unit could not be read")
		return
	}
	unitPolicy, unitPolicyErr := helperRestartPolicyFromUnit(installedUnit)
	expectedUnitPolicy := expectedHelperRestartUnitPolicy(installedSystemdVersion)
	policyMatched := current.SystemdVersion == installedSystemdVersion &&
		current.HelperRestartPolicy == setupStateRestartPolicy(installedSystemdVersion) && unitPolicyErr == nil && maps.Equal(unitPolicy, expectedUnitPolicy)
	policyCode := "oci_helper_restart_policy_current"
	if !policyMatched {
		policyCode = "oci_helper_restart_policy_drift"
	}
	report.Findings = append(report.Findings, finding("helper-restart-policy", diagnosticReceipt{ran: true, passed: policyMatched, code: policyCode,
		reasonCode: reasonUnless(policyMatched, contract.CapabilityReasonPrerequisiteMissing), detail: fmt.Sprintf("installed systemd_version=%d durable systemd_version=%d helper restart policy=%s installed unit policy matched=%t", installedSystemdVersion, current.SystemdVersion, current.HelperRestartPolicy, unitPolicyErr == nil && maps.Equal(unitPolicy, expectedUnitPolicy))}))
}

func expectedHelperRestartUnitPolicy(systemdVersion int) map[string]string {
	policy := map[string]string{
		"Unit.StartLimitIntervalSec": "0",
		"Service.Restart":            "on-failure",
		"Service.RestartSec":         "1s",
	}
	if systemdVersion >= 254 {
		policy["Service.RestartSec"] = "250ms"
		policy["Service.RestartSteps"] = "6"
		policy["Service.RestartMaxDelaySec"] = "1s"
	}
	return policy
}

func helperRestartPolicyFromUnit(unit string) (map[string]string, error) {
	wanted := map[string]struct{}{
		"Unit.StartLimitIntervalSec": {},
		"Service.Restart":            {}, "Service.RestartSec": {}, "Service.RestartSteps": {}, "Service.RestartMaxDelaySec": {},
	}
	result := make(map[string]string)
	section := ""
	for _, raw := range strings.Split(unit, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		qualified := section + "." + strings.TrimSpace(key)
		if _, tracked := wanted[qualified]; !tracked {
			continue
		}
		if _, duplicate := result[qualified]; duplicate {
			return nil, errors.New("installed helper service unit repeats a restart-policy key")
		}
		result[qualified] = strings.TrimSpace(value)
	}
	return result, nil
}

func appendHelperRestartPolicyNotRun(report *DoctorResponse, cause NotRunCause, detail string) {
	report.Findings = append(report.Findings, finding("helper-restart-policy", diagnosticReceipt{
		code: "oci_helper_restart_policy_not_read", notRunCause: cause, detail: detail,
	}))
}

func buildMountRoots(runtimeStatus ocihelper.DoctorStatus, report *DoctorResponse) {
	rootsRead := runtimeStatus.MountRootsRead.Outcome == ocihelper.DiagnosticReadOK || runtimeStatus.MountRootsRead.Outcome == "" && runtimeStatus.AllowedMountRoots != nil
	if !rootsRead {
		report.Findings = append(report.Findings, finding("mount-roots", diagnosticReceipt{ran: runtimeStatus.MountRootsRead.Outcome != "", code: "oci_mount_roots_unavailable", notRunCause: NotRunSourceUnavailable, detail: "the configured helper mount allowlist was unavailable"}))
		return
	}
	roots := append([]string{}, runtimeStatus.AllowedMountRoots...)
	sort.Strings(roots)
	report.Mounts.AllowedRoots = roots
	if report.Convergence.State == nil {
		report.Findings = append(report.Findings, finding("mount-roots", diagnosticReceipt{code: "oci_mount_roots_not_run", notRunCause: NotRunDependencyMissing, detail: "current setup mount root was unavailable for comparison"}))
		return
	}
	hostRoot := filepath.Clean(report.Convergence.State.HostMountRoot)
	matched := false
	for _, root := range roots {
		cleanRoot := filepath.Clean(root)
		if hostRoot == cleanRoot || strings.HasPrefix(hostRoot, cleanRoot+string(filepath.Separator)) {
			matched = true
			break
		}
	}
	report.Mounts.Outcome = outcomeFor(true, matched)
	report.Findings = append(report.Findings, finding("mount-roots", diagnosticReceipt{ran: true, passed: matched, code: map[bool]string{true: "oci_mount_roots_observed", false: "oci_mount_root_unavailable"}[matched], reasonCode: reasonUnless(matched, contract.CapabilityReasonMountRootUnavailable), detail: "current setup mount root was compared with the helper allowlist"}))
}

func outcomeFor(ran, passed bool) DiagnosticOutcome {
	if !ran {
		return DiagnosticNotRun
	}
	if passed {
		return DiagnosticOK
	}
	return DiagnosticFailed
}

func reasonUnless(condition bool, reason contract.CapabilityReasonCode) contract.CapabilityReasonCode {
	if condition {
		return ""
	}
	if reason.Valid() {
		return reason
	}
	return contract.CapabilityReasonProbeFailed
}

func cacheCode(ok bool) string {
	if ok {
		return "oci_cache_within_bound"
	}
	return "oci_cache_over_bound"
}

func testedRuntimeVersions(containerdVersion, runcVersion string) bool {
	return strings.TrimPrefix(strings.TrimSpace(containerdVersion), "v") == TestedContainerdVersion &&
		strings.TrimPrefix(strings.TrimSpace(runcVersion), "v") == TestedRuncVersion
}

func supportedRuntimeVersions(containerdVersion, runcVersion string) bool {
	containerdParts, containerdOK := numericVersion(containerdVersion)
	minimumContainerd, minimumOK := numericVersion(MinimumContainerdVersion)
	runcParts, runcOK := numericVersion(runcVersion)
	minimumRunc, minimumRuncOK := numericVersion(MinimumRuncVersion)
	return containerdOK && minimumOK && runcOK && minimumRuncOK &&
		versionPartsAtLeast(containerdParts, minimumContainerd) && runcParts[0] == 1 && versionPartsAtLeast(runcParts, minimumRunc)
}

func numericVersion(value string) ([3]int, bool) {
	var parts [3]int
	value = strings.TrimPrefix(strings.TrimSpace(value), "v")
	n, err := fmt.Sscanf(value, "%d.%d.%d", &parts[0], &parts[1], &parts[2])
	return parts, err == nil && n == 3
}

func versionPartsAtLeast(actual, minimum [3]int) bool {
	for index := range actual {
		if actual[index] != minimum[index] {
			return actual[index] > minimum[index]
		}
	}
	return true
}

func cloneBoolMap(values map[string]bool) map[string]bool {
	cloned := make(map[string]bool, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func StableDoctorReasonCodes() []contract.CapabilityReasonCode {
	return []contract.CapabilityReasonCode{
		contract.CapabilityReasonOCIIntentDisabled, contract.CapabilityReasonPrerequisiteMissing,
		contract.CapabilityReasonRuntimeVersionUnsupported, contract.CapabilityReasonHelperUnreachable,
		contract.CapabilityReasonHelperUnitUnavailable,
		contract.CapabilityReasonHelperHandshakeStalled,
		contract.CapabilityReasonHelperHandshakeStalledPersistent,
		contract.CapabilityReasonHelperVersionMismatch, contract.CapabilityReasonHelperHandshakeFailed,
		contract.CapabilityReasonBootSweepFailed, contract.CapabilityReasonProbeFailed,
		contract.CapabilityReasonLimaStopped, contract.CapabilityReasonLimaBroken,
		contract.CapabilityReasonLimaStartTimeout, contract.CapabilityReasonTemplateRestartRequired,
		contract.CapabilityReasonTemplateRecreateRequired, contract.CapabilityReasonMountRootUnavailable,
		contract.CapabilityReasonLocalPermissionDenied,
	}
}

func StableDoctorCodes() []string {
	return []string{
		"oci_host_platform_observed", "oci_agent_user_observed",
		"oci_intent_not_read", "oci_intent_unavailable", "oci_intent_enabled", "oci_intent_disabled",
		"oci_capability_revision_not_read", "oci_capability_revision_current", "oci_capability_revision_pending",
		"oci_capability_observation_not_read", "oci_capability_observation_current", "oci_capability_observation_restricted",
		"oci_probe_not_recorded", "oci_probe_passed", "oci_probe_failed",
		"oci_lima_not_applicable", "oci_lima_not_observed", "oci_lima_state",
		"oci_helper_not_read", "oci_helper_unreachable", "oci_helper_handshake_ok", "oci_helper_handshake_failed", "oci_helper_version_mismatch",
		"oci_helper_handshake_stalls_not_read", "oci_helper_handshake_stalls_clear", "oci_helper_handshake_stalls_observed",
		"oci_boot_sweep_not_recorded", "oci_boot_sweep_verified", "oci_boot_sweep_failed",
		"oci_computer_storage_recovery_not_read", "oci_computer_storage_recovery_clear", "oci_computer_storage_recovery_retained",
		"oci_runtime_platform_not_run", "oci_runtime_platform_not_recorded", "oci_runtime_platform_observed",
		"oci_runtime_versions_not_run", "oci_runtime_versions_unavailable", "oci_runtime_versions_unsupported", "oci_runtime_versions_observed", "oci_runtime_versions_outside_tested_range",
		"oci_cache_not_run", "oci_cache_status_unavailable", "oci_cache_within_bound", "oci_cache_over_bound", "oci_cache_eviction_failed",
		"oci_profile_ceilings_not_run", "oci_profile_ceilings_not_recorded", "oci_profile_tmpfs_ceilings_within_memory_limit", "oci_profile_tmpfs_ceilings_exceed_memory_limit",
		"oci_computer_screen_isolation_not_run", "oci_computer_screen_isolation_not_recorded", "oci_computer_screen_isolation_enforced", "oci_computer_screen_isolation_not_enforced",
		"oci_resource_admission_not_run", "oci_resource_admission_not_recorded", "oci_resource_admission_admitted", "oci_resource_admission_refused",
		"oci_mount_roots_not_run", "oci_mount_roots_unavailable", "oci_mount_roots_observed", "oci_mount_root_unavailable",
		"oci_convergence_not_read", "oci_convergence_state_unavailable", "oci_convergence_desired_not_read",
		"oci_convergence_unchanged", "oci_convergence_live_safe", "oci_convergence_restart_required", "oci_convergence_recreate_required",
		"oci_helper_restart_policy_not_read", "oci_helper_restart_policy_current", "oci_helper_restart_policy_drift",
	}
}

func (report DoctorResponse) Validate() error {
	if report.Version != DoctorVersion || report.ObservedAt.IsZero() || report.HostPlatform.OS == "" || report.HostPlatform.Architecture == "" {
		return fmt.Errorf("invalid doctor header")
	}
	if len(report.Findings) == 0 || report.Probe.Capabilities == nil || report.Probe.MissingCapabilities == nil || report.Mounts.AllowedRoots == nil ||
		!report.ComputerStorageRecovery.Outcome.Valid() || !report.ComputerScreenIsolation.Outcome.Valid() {
		return fmt.Errorf("doctor report is incomplete")
	}
	if len(report.Limitations) != 1 || report.Limitations[0].Code != DoctorUIDLimitation || report.Limitations[0].Issue != DoctorUIDIssue || report.Limitations[0].Detail == "" {
		return fmt.Errorf("doctor UID-isolation limitation is missing")
	}
	seen := make(map[string]struct{}, len(report.Findings))
	stableCodes := make(map[string]struct{}, len(StableDoctorCodes()))
	for _, code := range StableDoctorCodes() {
		stableCodes[code] = struct{}{}
	}
	for _, item := range report.Findings {
		if item.Check == "" || !item.Outcome.Valid() || !item.Severity.Valid() || item.Code == "" || item.Detail == "" || !strings.HasPrefix(item.Runbook, DoctorRunbookPrefix) {
			return fmt.Errorf("invalid doctor finding for %q", item.Check)
		}
		if _, ok := stableCodes[item.Code]; !ok {
			return fmt.Errorf("doctor finding %q used undocumented code %q", item.Check, item.Code)
		}
		if item.ReasonCode != "" && !item.ReasonCode.Valid() {
			return fmt.Errorf("invalid doctor reason %q", item.ReasonCode)
		}
		if item.Outcome == DiagnosticNotRun {
			if item.ReasonCode != "" || !item.NotRunCause.Valid() {
				return fmt.Errorf("NOT-RUN finding %q must carry only a closed not-run cause", item.Check)
			}
		} else if item.NotRunCause != "" {
			return fmt.Errorf("executed finding %q carries a not-run cause", item.Check)
		}
		seen[item.Check] = struct{}{}
	}
	for _, check := range []string{"host-platform", "agent-user", "intent", "capability-revision", "capability-observation", "probe", "lima", "helper-handshake", "boot-sweep", "computer-storage-recovery", "runtime-platform", "runtime-versions", "cache", "computer-screen-isolation", "resource-admission", "mount-roots", "convergence", "helper-restart-policy"} {
		if _, ok := seen[check]; !ok {
			return fmt.Errorf("doctor finding %q is missing", check)
		}
	}
	return nil
}

func WriteDoctorHuman(writer io.Writer, report DoctorResponse) error {
	if err := report.Validate(); err != nil {
		return err
	}
	platform := "NOT-RUN"
	if report.RuntimePlatform != nil {
		platform = report.RuntimePlatform.OS + "/" + report.RuntimePlatform.Architecture
		if report.RuntimePlatform.Variant != "" {
			platform += "/" + report.RuntimePlatform.Variant
		}
	}
	probeAge := "NOT-RUN"
	if report.Probe.AgeSeconds != nil {
		probeAge = fmt.Sprintf("%ds", *report.Probe.AgeSeconds)
	}
	intentUpdated := "NOT-RUN"
	if report.Intent.UpdatedAt != nil {
		intentUpdated = report.Intent.UpdatedAt.Format(time.RFC3339Nano)
	}
	lastEviction := "none"
	if report.Cache.LastEviction != nil {
		lastEviction = fmt.Sprintf("digest=%s reason=%s bytes=%d at=%s", report.Cache.LastEviction.Digest, report.Cache.LastEviction.Reason, report.Cache.LastEviction.Bytes, report.Cache.LastEviction.EvictedAt.Format(time.RFC3339Nano))
	}
	convergenceState := "NOT-RUN"
	if report.Convergence.State != nil {
		state := report.Convergence.State
		convergenceState = fmt.Sprintf("memory=%s cpus=%d disk=%s vm_type=%s host_mount_root=%s probe_digest=%s", state.VMMemory, state.VMCPUs, state.VMDisk, state.VMType, state.HostMountRoot, state.ProbeDigest)
	}
	desiredConvergenceState := "NOT-RUN"
	if report.Convergence.Desired != nil {
		state := report.Convergence.Desired
		desiredConvergenceState = fmt.Sprintf("memory=%s cpus=%d disk=%s vm_type=%s host_mount_root=%s probe_digest=%s", state.VMMemory, state.VMCPUs, state.VMDisk, state.VMType, state.HostMountRoot, state.ProbeDigest)
	}
	lines := []string{
		fmt.Sprintf("WEFTY NODE DOCTOR v%d", report.Version),
		fmt.Sprintf("OBSERVED AT\t%s", report.ObservedAt.Format(time.RFC3339Nano)),
		fmt.Sprintf("HOST PLATFORM\t%s/%s", report.HostPlatform.OS, report.HostPlatform.Architecture),
		fmt.Sprintf("RUNTIME PLATFORM\t%s", platform),
		fmt.Sprintf("AGENT\tuser=%s unit=%s", report.Agent.User, report.Agent.LaunchUnit),
		fmt.Sprintf("INTENT\t%s enabled=%t revision=%d updated_at=%s", report.Intent.Outcome, report.Intent.Enabled, report.Intent.Revision, intentUpdated),
		fmt.Sprintf("PROBE\t%s verdict=%s age=%s observed_at=%s probe_revision=%d reason=%s capability_observed_at=%s capability_revision=%d pending=%d capability_reason=%s", report.Probe.Outcome, report.Probe.Verdict, probeAge, formatOptionalTime(report.Probe.ObservedAt), report.Probe.ProbeRevision, report.Probe.ReasonCode, report.Probe.CapabilityObservedAt.Format(time.RFC3339Nano), report.Probe.CapabilityRevision, report.Probe.PendingPublicationRevision, report.Probe.CapabilityReasonCode),
		fmt.Sprintf("HELPER\t%s protocol=%d version=%s checksum=%s instance=%s generation=%d", report.Helper.Outcome, report.Helper.ProtocolVersion, report.Helper.Version, report.Helper.Checksum, report.Helper.InstanceID, report.Helper.SessionGeneration),
		fmt.Sprintf("RUNTIMES\t%s containerd=%s runc=%s runc_source=%s outside_tested_range=%t", report.Versions.Outcome, report.Versions.Containerd, report.Versions.Runc, report.Versions.RuncSource, report.Versions.OutsideTestedRange),
		fmt.Sprintf("CACHE\t%s bytes=%d cap=%d within_bound=%t last_eviction=%s", report.Cache.Outcome, report.Cache.Bytes, report.Cache.CapBytes, report.Cache.WithinBound, lastEviction),
		fmt.Sprintf("PROFILE\t%s memory_limit=%d memory_max=%d memory_oom_group=%t memory_swap_max=%d computer_tmpfs_ceiling=%d largest_tmpfs_ceiling=%d warnings=%d", report.Profile.Outcome, report.Profile.MemoryLimitBytes, report.Profile.MemoryMaxBytes, report.Profile.MemoryOOMGroup, report.Profile.MemorySwapMaxBytes, report.Profile.ComputerTmpfsCeilingBytes, report.Profile.LargestTmpfsCeilingBytes, len(report.Profile.Warnings)),
		fmt.Sprintf("SCREEN ISOLATION\t%s network_namespace_present=%t host_abstract_socket_visible=%t", report.ComputerScreenIsolation.Outcome, report.ComputerScreenIsolation.NetworkNamespacePresent, report.ComputerScreenIsolation.HostAbstractSocketVisible),
		fmt.Sprintf("MOUNTS\t%s roots=%s", report.Mounts.Outcome, strings.Join(report.Mounts.AllowedRoots, ",")),
		fmt.Sprintf("CONVERGENCE\t%s class=%s current={%s} desired={%s}", report.Convergence.Outcome, report.Convergence.Class, convergenceState, desiredConvergenceState),
	}
	if report.ResourceAdmission != nil {
		admission := report.ResourceAdmission
		lines = append(lines, fmt.Sprintf("RESOURCE ADMISSION\tobserved_at=%s admitted=%t failure_code=%s memory_capacity=%d memory_reserve=%d memory_committed_before=%d requested_memory=%d memory_committed_after=%d disk_committed_before=%d requested_disk=%d disk_committed_after=%d mem_total=%d mem_available=%d filesystem_available=%d computer_tmpfs_ceiling=%d warnings=%d", admission.ObservedAt.Format(time.RFC3339Nano), admission.Admitted, admission.FailureCode, admission.MemoryCapacityBytes, admission.MemoryReserveBytes, admission.MemoryCommittedBeforeBytes, admission.RequestedMemoryBytes, admission.MemoryCommittedAfterBytes, admission.DiskCommittedBeforeBytes, admission.RequestedDiskBytes, admission.DiskCommittedAfterBytes, admission.MemTotalBytes, admission.MemAvailableBytes, admission.FilesystemAvailableBytes, admission.ComputerTmpfsCeilingBytes, len(admission.Warnings)))
	} else {
		lines = append(lines, "RESOURCE ADMISSION\tNOT-RUN no receipt")
	}
	if report.Lima.Applicable {
		lines = append(lines, fmt.Sprintf("LIMA\t%s instance=%s state=%s enabled=%t recovering=%t observed_at=%s repair_count=%d reason=%s", report.Lima.Outcome, report.Lima.Facts.Instance, report.Lima.Facts.State, report.Lima.Facts.Enabled, report.Lima.Facts.Recovering, report.Lima.Facts.ObservedAt.Format(time.RFC3339Nano), report.Lima.Facts.RepairCount, report.Lima.Facts.ReasonCode))
	} else {
		lines = append(lines, "LIMA\tNOT-RUN not applicable")
	}
	for _, item := range report.Findings {
		lines = append(lines, fmt.Sprintf("CHECK %s\t%s severity=%s code=%s reason=%s not_run_cause=%s runbook=%s detail=%s", item.Check, item.Outcome, item.Severity, item.Code, item.ReasonCode, item.NotRunCause, item.Runbook, item.Detail))
	}
	for _, limitation := range report.Limitations {
		lines = append(lines, fmt.Sprintf("LIMITATION\tcode=%s issue=%s detail=%s", limitation.Code, limitation.Issue, limitation.Detail))
	}
	missingCapabilities := append([]string(nil), report.Probe.MissingCapabilities...)
	sort.Strings(missingCapabilities)
	lines = append(lines,
		fmt.Sprintf("CAPABILITIES\t%s", sortedEnabledCapabilities(report.Probe.Capabilities)),
		fmt.Sprintf("MISSING CAPABILITIES\t%s", strings.Join(missingCapabilities, ",")),
	)
	_, err := fmt.Fprintln(writer, strings.Join(lines, "\n"))
	return err
}

func formatOptionalTime(value *time.Time) string {
	if value == nil {
		return "NOT-RUN"
	}
	return value.Format(time.RFC3339Nano)
}

func sortedEnabledCapabilities(capabilities map[string]bool) string {
	values := make([]string, 0, len(capabilities))
	for capability, enabled := range capabilities {
		if enabled {
			values = append(values, capability)
		}
	}
	sort.Strings(values)
	return strings.Join(values, ",")
}
