package ocicontrol

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const (
	DoctorVersion         = 1
	DoctorUIDLimitation   = "process_kind_payload_uid_isolation_pending"
	DoctorUIDIssue        = "https://github.com/Derek-X-Wang/wefty/issues/220"
	DoctorLaunchUnmanaged = "unmanaged"
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
	Outcome           DiagnosticOutcome `json:"outcome"`
	ProtocolVersion   int               `json:"protocol_version,omitempty"`
	Version           string            `json:"version,omitempty"`
	Checksum          string            `json:"checksum,omitempty"`
	InstanceID        string            `json:"instance_id,omitempty"`
	SessionGeneration uint64            `json:"session_generation,omitempty"`
}

type VersionFacts struct {
	Outcome           DiagnosticOutcome `json:"outcome"`
	Containerd        string            `json:"containerd,omitempty"`
	Runc              string            `json:"runc,omitempty"`
	Supported         bool              `json:"supported"`
	WithinTestedRange bool              `json:"within_tested_range"`
}

type ProbeDoctorFacts struct {
	Outcome                    DiagnosticOutcome             `json:"outcome"`
	Verdict                    string                        `json:"verdict"`
	ObservedAt                 *time.Time                    `json:"observed_at,omitempty"`
	AgeSeconds                 *int64                        `json:"age_seconds,omitempty"`
	CapabilityRevision         int64                         `json:"capability_revision"`
	PendingPublicationRevision int64                         `json:"pending_publication_revision,omitempty"`
	CapabilityObservedAt       time.Time                     `json:"capability_observed_at"`
	Capabilities               map[string]bool               `json:"capabilities"`
	MissingCapabilities        []string                      `json:"missing_capabilities"`
	ReasonCode                 contract.CapabilityReasonCode `json:"reason_code,omitempty"`
}

type IntentDoctorFacts struct {
	Outcome   DiagnosticOutcome `json:"outcome"`
	Version   int               `json:"version,omitempty"`
	Revision  uint64            `json:"revision,omitempty"`
	Enabled   bool              `json:"enabled"`
	UpdatedAt *time.Time        `json:"updated_at,omitempty"`
}

type CacheDoctorFacts struct {
	Outcome      DiagnosticOutcome             `json:"outcome"`
	Bytes        int64                         `json:"bytes,omitempty"`
	CapBytes     int64                         `json:"cap_bytes,omitempty"`
	WithinBound  bool                          `json:"within_bound"`
	LastEviction *ocihelper.ImageCacheEviction `json:"last_eviction,omitempty"`
}

type MountDoctorFacts struct {
	Outcome      DiagnosticOutcome `json:"outcome"`
	AllowedRoots []string          `json:"allowed_roots"`
}

type ConvergenceDoctorFacts struct {
	Outcome DiagnosticOutcome `json:"outcome"`
	Class   ConvergenceClass  `json:"class,omitempty"`
	State   *SetupState       `json:"state,omitempty"`
}

type LimaDoctorFacts struct {
	Applicable bool                 `json:"applicable"`
	Outcome    DiagnosticOutcome    `json:"outcome"`
	Facts      lima.SupervisorFacts `json:"facts,omitempty"`
}

type DiagnosticFinding struct {
	Check      string                        `json:"check"`
	Outcome    DiagnosticOutcome             `json:"outcome"`
	Code       string                        `json:"code"`
	ReasonCode contract.CapabilityReasonCode `json:"reason_code,omitempty"`
	Detail     string                        `json:"detail"`
	Runbook    string                        `json:"runbook"`
}

type DoctorLimitation struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
	Issue  string `json:"issue"`
}

type DoctorResponse struct {
	Version         int                    `json:"version"`
	ObservedAt      time.Time              `json:"observed_at"`
	HostPlatform    PlatformFacts          `json:"host_platform"`
	RuntimePlatform *PlatformFacts         `json:"runtime_platform,omitempty"`
	Agent           AgentFacts             `json:"agent"`
	Lima            LimaDoctorFacts        `json:"lima"`
	Helper          HelperDoctorFacts      `json:"helper"`
	Versions        VersionFacts           `json:"versions"`
	Probe           ProbeDoctorFacts       `json:"probe"`
	Intent          IntentDoctorFacts      `json:"intent"`
	Cache           CacheDoctorFacts       `json:"cache"`
	Mounts          MountDoctorFacts       `json:"mounts"`
	Convergence     ConvergenceDoctorFacts `json:"convergence"`
	Findings        []DiagnosticFinding    `json:"findings"`
	Limitations     []DoctorLimitation     `json:"limitations"`
}

type HelperDoctorSource func(context.Context) (HelperDoctorSnapshot, error)

type HelperDoctorSnapshot struct {
	ProtocolVersion         int
	Version                 string
	Checksum                string
	InstanceID              string
	SessionGeneration       uint64
	Runtime                 ocihelper.DoctorStatus
	RuntimePlatformRecorded bool
}

type DoctorConfig struct {
	Clock              Clock
	HostPlatform       PlatformFacts
	AgentUser          string
	LaunchUnit         string
	CapabilitySnapshot func() agent.CapabilitySnapshot
	Intent             func(context.Context) (lima.OCIIntent, error)
	LimaFacts          func() lima.SupervisorFacts
	Helper             HelperDoctorSource
	SetupStatePath     string
	ReadSetupState     func(string) (SetupState, error)
}

type diagnosticReceipt struct {
	ran        bool
	passed     bool
	code       string
	reasonCode contract.CapabilityReasonCode
	detail     string
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
	return DiagnosticFinding{
		Check: check, Outcome: outcome, Code: code, ReasonCode: receipt.reasonCode,
		Detail: receipt.detail, Runbook: runbookFor(receipt.reasonCode, code),
	}
}

func runbookFor(reason contract.CapabilityReasonCode, code string) string {
	anchor := code
	if reason.Valid() {
		anchor = string(reason)
	}
	anchor = strings.ReplaceAll(anchor, "_", "-")
	return "docs/runbooks/oci-node.md#doctor-code-" + anchor
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
		Mounts:      MountDoctorFacts{Outcome: DiagnosticNotRun, AllowedRoots: []string{}},
		Convergence: ConvergenceDoctorFacts{Outcome: DiagnosticNotRun},
		Findings:    []DiagnosticFinding{},
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
	buildHelper(ctx, config, &report)
	buildConvergence(config, &report)
	return report
}

func buildIntent(ctx context.Context, config DoctorConfig, report *DoctorResponse) {
	if config.Intent == nil {
		report.Findings = append(report.Findings, finding("intent", diagnosticReceipt{code: "oci_intent_not_read", reasonCode: contract.CapabilityReasonOCIIntentDisabled, detail: "durable OCI intent was not available to the doctor"}))
		return
	}
	intent, err := config.Intent(ctx)
	if err != nil || intent.Version != lima.OCIIntentVersion || intent.Revision == 0 {
		report.Intent.Outcome = DiagnosticFailed
		report.Findings = append(report.Findings, finding("intent", diagnosticReceipt{ran: true, code: "oci_intent_unavailable", reasonCode: contract.CapabilityReasonOCIIntentDisabled, detail: "durable OCI intent is missing, unreadable, or malformed"}))
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
		report.Findings = append(report.Findings, finding("probe", diagnosticReceipt{code: "oci_probe_not_recorded", reasonCode: contract.CapabilityReasonProbeFailed, detail: "shared capability observation was unavailable"}))
		report.Findings = append(report.Findings, finding("capability-revision", diagnosticReceipt{code: "oci_capability_revision_not_read", reasonCode: contract.CapabilityReasonProbeFailed, detail: "Capability revision was not read"}))
		return
	}
	snapshot := config.CapabilitySnapshot()
	observation := snapshot.CapabilityObservation
	report.Probe.CapabilityRevision = observation.Revision
	report.Probe.PendingPublicationRevision = snapshot.PendingPublicationRevision
	report.Probe.CapabilityObservedAt = observation.ObservedAt.UTC().Round(0)
	report.Probe.Capabilities = cloneBoolMap(observation.Capabilities)
	report.Probe.MissingCapabilities = append([]string{}, observation.MissingCapabilities...)
	report.Probe.ReasonCode = observation.ReasonCode
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

	if snapshot.LastProbeAt.IsZero() {
		report.Probe.Outcome = DiagnosticNotRun
		report.Findings = append(report.Findings, finding("probe", diagnosticReceipt{code: "oci_probe_not_recorded", reasonCode: contract.CapabilityReasonProbeFailed, detail: "no completed functional probe is recorded"}))
		return
	}
	probeAt := snapshot.LastProbeAt.UTC().Round(0)
	age := now.Sub(probeAt) / time.Second
	if age < 0 {
		age = 0
	}
	ageSeconds := int64(age)
	report.Probe.ObservedAt = &probeAt
	report.Probe.AgeSeconds = &ageSeconds
	passed := observation.Capabilities["kind:oci"] && observation.ReasonCode == ""
	report.Probe.Outcome = outcomeFor(true, passed)
	report.Probe.Verdict = "failed"
	if passed {
		report.Probe.Verdict = "passed"
	}
	reason = observation.ReasonCode
	if !passed && !reason.Valid() {
		reason = contract.CapabilityReasonProbeFailed
	}
	report.Findings = append(report.Findings, finding("probe", diagnosticReceipt{ran: true, passed: passed, code: "oci_probe_" + report.Probe.Verdict, reasonCode: reason, detail: "the recorded functional-probe observation was reused; no probe ran"}))
}

func buildLima(config DoctorConfig, report *DoctorResponse) {
	if !report.Lima.Applicable {
		report.Findings = append(report.Findings, finding("lima", diagnosticReceipt{code: "lima_not_applicable", detail: "Lima is not applicable to this host platform"}))
		return
	}
	if config.LimaFacts == nil {
		report.Findings = append(report.Findings, finding("lima", diagnosticReceipt{code: "lima_not_observed", reasonCode: contract.CapabilityReasonLimaStopped, detail: "Lima supervisor facts were unavailable"}))
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
	report.Findings = append(report.Findings, finding("lima", diagnosticReceipt{ran: true, passed: passed, code: "lima_" + string(facts.State), reasonCode: reason, detail: "recorded Lima supervisor facts were read without inspection or recovery"}))
}

func buildHelper(ctx context.Context, config DoctorConfig, report *DoctorResponse) {
	if config.Helper == nil {
		appendHelperNotRun(report, "OCI helper status source was unavailable")
		return
	}
	snapshot, err := config.Helper(ctx)
	if err != nil {
		report.Helper.Outcome = DiagnosticFailed
		report.Findings = append(report.Findings, finding("helper-handshake", diagnosticReceipt{ran: true, code: "oci_helper_unreachable", reasonCode: contract.CapabilityReasonHelperUnreachable, detail: "the current helper session was absent or unreachable"}))
		appendHelperDependentsNotRun(report, contract.CapabilityReasonHelperUnreachable)
		return
	}
	validHandshake := snapshot.ProtocolVersion == ocihelper.ProtocolVersion && snapshot.Version != "" && snapshot.Checksum != "" && snapshot.InstanceID != "" && snapshot.SessionGeneration > 0
	reason := contract.CapabilityReasonCode("")
	code := "oci_helper_handshake_ok"
	if !validHandshake {
		reason = contract.CapabilityReasonHelperHandshakeFailed
		code = "oci_helper_handshake_failed"
	}
	report.Helper = HelperDoctorFacts{
		Outcome: outcomeFor(true, validHandshake), ProtocolVersion: snapshot.ProtocolVersion,
		Version: snapshot.Version, Checksum: snapshot.Checksum, InstanceID: snapshot.InstanceID,
		SessionGeneration: snapshot.SessionGeneration,
	}
	report.Findings = append(report.Findings, finding("helper-handshake", diagnosticReceipt{ran: true, passed: validHandshake, code: code, reasonCode: reason, detail: "the existing authenticated helper handshake was read; no session was acquired"}))
	if !validHandshake {
		appendHelperDependentsNotRun(report, reason)
		return
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
	report.Findings = append(report.Findings, finding("runtime-platform", diagnosticReceipt{ran: snapshot.RuntimePlatformRecorded, passed: platformOK, code: platformCode, reasonCode: reasonUnless(platformOK, contract.CapabilityReasonProbeFailed), detail: "runtime platform was read from the recorded functional probe"}))

	versionsOK := minimumRuntimeVersions(runtimeStatus.ContainerdVersion, runtimeStatus.RuncVersion)
	testedRange := testedRuntimeVersions(runtimeStatus.ContainerdVersion, runtimeStatus.RuncVersion)
	versionCode := "oci_runtime_versions_tested"
	if versionsOK && !testedRange {
		versionCode = "oci_runtime_versions_outside_tested_range"
	} else if !versionsOK {
		versionCode = "oci_runtime_version_unsupported"
	}
	report.Versions = VersionFacts{Outcome: outcomeFor(true, versionsOK), Containerd: runtimeStatus.ContainerdVersion, Runc: runtimeStatus.RuncVersion, Supported: versionsOK, WithinTestedRange: testedRange}
	report.Findings = append(report.Findings, finding("runtime-versions", diagnosticReceipt{ran: true, passed: versionsOK, code: versionCode, reasonCode: reasonUnless(versionsOK, contract.CapabilityReasonRuntimeVersionUnsupported), detail: "containerd and runc versions were read without starting a workload"}))

	cache := runtimeStatus.Cache
	cacheOK := cache.CapBytes > 0 && cache.Bytes >= 0 && cache.Bytes <= cache.CapBytes
	report.Cache = CacheDoctorFacts{Outcome: outcomeFor(true, cacheOK), Bytes: cache.Bytes, CapBytes: cache.CapBytes, WithinBound: cacheOK, LastEviction: cache.LastEviction}
	report.Findings = append(report.Findings, finding("cache", diagnosticReceipt{ran: true, passed: cacheOK, code: cacheCode(cacheOK), detail: "cache accounting was read without enforcing or evicting"}))

	roots := append([]string{}, runtimeStatus.AllowedMountRoots...)
	sort.Strings(roots)
	report.Mounts = MountDoctorFacts{Outcome: DiagnosticOK, AllowedRoots: roots}
	report.Findings = append(report.Findings, finding("mount-roots", diagnosticReceipt{ran: true, passed: true, code: "oci_mount_roots_observed", detail: "the configured helper mount allowlist was read without path traversal"}))
}

func appendHelperNotRun(report *DoctorResponse, detail string) {
	report.Findings = append(report.Findings, finding("helper-handshake", diagnosticReceipt{code: "oci_helper_not_read", reasonCode: contract.CapabilityReasonHelperUnreachable, detail: detail}))
	appendHelperDependentsNotRun(report, contract.CapabilityReasonHelperUnreachable)
}

func appendHelperDependentsNotRun(report *DoctorResponse, reason contract.CapabilityReasonCode) {
	for _, check := range []string{"runtime-platform", "runtime-versions", "cache", "mount-roots"} {
		report.Findings = append(report.Findings, finding(check, diagnosticReceipt{code: "oci_" + strings.ReplaceAll(check, "-", "_") + "_not_run", reasonCode: reason, detail: "the dependent helper read did not run"}))
	}
}

func buildConvergence(config DoctorConfig, report *DoctorResponse) {
	if config.SetupStatePath == "" {
		report.Findings = append(report.Findings, finding("convergence", diagnosticReceipt{code: "oci_convergence_not_read", reasonCode: contract.CapabilityReasonPrerequisiteMissing, detail: "durable setup convergence state was not configured"}))
		return
	}
	read := config.ReadSetupState
	if read == nil {
		read = ReadSetupState
	}
	state, err := read(config.SetupStatePath)
	if err != nil {
		report.Convergence.Outcome = DiagnosticFailed
		report.Findings = append(report.Findings, finding("convergence", diagnosticReceipt{ran: true, code: "oci_convergence_state_unavailable", reasonCode: contract.CapabilityReasonPrerequisiteMissing, detail: "durable setup convergence state is missing, unreadable, or malformed"}))
		return
	}
	class := ConvergenceUnchanged
	reason := report.Probe.ReasonCode
	if reason == contract.CapabilityReasonTemplateRestartRequired {
		class = ConvergenceRestartRequired
	} else if reason == contract.CapabilityReasonTemplateRecreateRequired {
		class = ConvergenceRecreateRequired
	}
	passed := class == ConvergenceUnchanged || class == ConvergenceLiveSafe
	report.Convergence = ConvergenceDoctorFacts{Outcome: outcomeFor(true, passed), Class: class, State: &state}
	report.Findings = append(report.Findings, finding("convergence", diagnosticReceipt{ran: true, passed: passed, code: "oci_convergence_" + string(class), reasonCode: reasonUnless(passed, reason), detail: "durable convergence state was read without applying it"}))
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

func minimumRuntimeVersions(containerdVersion, runcVersion string) bool {
	containerdMajor, ok := versionMajor(containerdVersion)
	if !ok || containerdMajor < 2 {
		return false
	}
	runcMajor, ok := versionMajor(runcVersion)
	return ok && runcMajor == 1
}

func testedRuntimeVersions(containerdVersion, runcVersion string) bool {
	containerdMajor, containerdMinor, ok := versionMajorMinor(containerdVersion)
	if !ok || containerdMajor != 2 || containerdMinor != 3 {
		return false
	}
	runcMajor, runcMinor, ok := versionMajorMinor(runcVersion)
	return ok && runcMajor == 1 && runcMinor == 5
}

func versionMajorMinor(value string) (int, int, bool) {
	value = strings.TrimSpace(strings.TrimPrefix(value, "v"))
	parts := strings.Split(value, ".")
	if len(parts) < 2 {
		return 0, 0, false
	}
	major, majorErr := strconv.Atoi(parts[0])
	minor, minorErr := strconv.Atoi(parts[1])
	return major, minor, majorErr == nil && minorErr == nil
}

func versionMajor(value string) (int, bool) {
	value = strings.TrimSpace(strings.TrimPrefix(value, "v"))
	majorText, _, _ := strings.Cut(value, ".")
	major, err := strconv.Atoi(majorText)
	return major, err == nil
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
		contract.CapabilityReasonHelperVersionMismatch, contract.CapabilityReasonHelperHandshakeFailed,
		contract.CapabilityReasonBootSweepFailed, contract.CapabilityReasonProbeFailed,
		contract.CapabilityReasonLimaStopped, contract.CapabilityReasonLimaBroken,
		contract.CapabilityReasonLimaStartTimeout, contract.CapabilityReasonTemplateRestartRequired,
		contract.CapabilityReasonTemplateRecreateRequired, contract.CapabilityReasonMountRootUnavailable,
		contract.CapabilityReasonLocalPermissionDenied,
	}
}

func (report DoctorResponse) Validate() error {
	if report.Version != DoctorVersion || report.ObservedAt.IsZero() || report.HostPlatform.OS == "" || report.HostPlatform.Architecture == "" {
		return fmt.Errorf("invalid doctor header")
	}
	if len(report.Findings) == 0 || report.Probe.Capabilities == nil || report.Probe.MissingCapabilities == nil || report.Mounts.AllowedRoots == nil {
		return fmt.Errorf("doctor report is incomplete")
	}
	if len(report.Limitations) != 1 || report.Limitations[0].Code != DoctorUIDLimitation || report.Limitations[0].Issue != DoctorUIDIssue || report.Limitations[0].Detail == "" {
		return fmt.Errorf("doctor UID-isolation limitation is missing")
	}
	seen := make(map[string]struct{}, len(report.Findings))
	for _, item := range report.Findings {
		if item.Check == "" || !item.Outcome.Valid() || item.Code == "" || item.Detail == "" || !strings.HasPrefix(item.Runbook, "docs/runbooks/oci-node.md#doctor-code-") {
			return fmt.Errorf("invalid doctor finding for %q", item.Check)
		}
		if item.ReasonCode != "" && !item.ReasonCode.Valid() {
			return fmt.Errorf("invalid doctor reason %q", item.ReasonCode)
		}
		seen[item.Check] = struct{}{}
	}
	for _, check := range []string{"host-platform", "agent-user", "intent", "capability-revision", "probe", "lima", "helper-handshake", "runtime-platform", "runtime-versions", "cache", "mount-roots", "convergence"} {
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
	lines := []string{
		fmt.Sprintf("WEFTY NODE DOCTOR v%d", report.Version),
		fmt.Sprintf("OBSERVED AT\t%s", report.ObservedAt.Format(time.RFC3339Nano)),
		fmt.Sprintf("HOST PLATFORM\t%s/%s", report.HostPlatform.OS, report.HostPlatform.Architecture),
		fmt.Sprintf("RUNTIME PLATFORM\t%s", platform),
		fmt.Sprintf("AGENT\tuser=%s unit=%s", report.Agent.User, report.Agent.LaunchUnit),
		fmt.Sprintf("INTENT\t%s enabled=%t revision=%d updated_at=%s", report.Intent.Outcome, report.Intent.Enabled, report.Intent.Revision, intentUpdated),
		fmt.Sprintf("PROBE\t%s verdict=%s age=%s observed_at=%s capability_observed_at=%s revision=%d pending=%d reason=%s", report.Probe.Outcome, report.Probe.Verdict, probeAge, formatOptionalTime(report.Probe.ObservedAt), report.Probe.CapabilityObservedAt.Format(time.RFC3339Nano), report.Probe.CapabilityRevision, report.Probe.PendingPublicationRevision, report.Probe.ReasonCode),
		fmt.Sprintf("HELPER\t%s protocol=%d version=%s checksum=%s instance=%s generation=%d", report.Helper.Outcome, report.Helper.ProtocolVersion, report.Helper.Version, report.Helper.Checksum, report.Helper.InstanceID, report.Helper.SessionGeneration),
		fmt.Sprintf("RUNTIMES\t%s containerd=%s runc=%s supported=%t within_tested_range=%t", report.Versions.Outcome, report.Versions.Containerd, report.Versions.Runc, report.Versions.Supported, report.Versions.WithinTestedRange),
		fmt.Sprintf("CACHE\t%s bytes=%d cap=%d within_bound=%t last_eviction=%s", report.Cache.Outcome, report.Cache.Bytes, report.Cache.CapBytes, report.Cache.WithinBound, lastEviction),
		fmt.Sprintf("MOUNTS\t%s roots=%s", report.Mounts.Outcome, strings.Join(report.Mounts.AllowedRoots, ",")),
		fmt.Sprintf("CONVERGENCE\t%s class=%s %s", report.Convergence.Outcome, report.Convergence.Class, convergenceState),
	}
	if report.Lima.Applicable {
		lines = append(lines, fmt.Sprintf("LIMA\t%s instance=%s state=%s enabled=%t recovering=%t observed_at=%s repair_count=%d reason=%s", report.Lima.Outcome, report.Lima.Facts.Instance, report.Lima.Facts.State, report.Lima.Facts.Enabled, report.Lima.Facts.Recovering, report.Lima.Facts.ObservedAt.Format(time.RFC3339Nano), report.Lima.Facts.RepairCount, report.Lima.Facts.ReasonCode))
	} else {
		lines = append(lines, "LIMA\tNOT-RUN not applicable")
	}
	for _, item := range report.Findings {
		lines = append(lines, fmt.Sprintf("CHECK %s\t%s code=%s reason=%s runbook=%s detail=%s", item.Check, item.Outcome, item.Code, item.ReasonCode, item.Runbook, item.Detail))
	}
	for _, limitation := range report.Limitations {
		lines = append(lines, fmt.Sprintf("LIMITATION\tcode=%s issue=%s detail=%s", limitation.Code, limitation.Issue, limitation.Detail))
	}
	sort.Strings(report.Probe.MissingCapabilities)
	lines = append(lines,
		fmt.Sprintf("CAPABILITIES\t%s", sortedEnabledCapabilities(report.Probe.Capabilities)),
		fmt.Sprintf("MISSING CAPABILITIES\t%s", strings.Join(report.Probe.MissingCapabilities, ",")),
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
