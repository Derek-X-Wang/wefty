package contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const SchemaVersionV1 = 1

// Run execution environment names are shared wire-contract vocabulary. Keep
// credentials in SensitiveEnv so public job projections can redact them.
const (
	EnvRunID                     = "WEFTY_RUN_ID"
	EnvL3Endpoint                = "WEFTY_L3_ENDPOINT"
	EnvRunToken                  = "WEFTY_RUN_TOKEN"
	EnvHandoffDir                = "WEFTY_HANDOFF_DIR"
	EnvServiceDir                = "WEFTY_SERVICE_DIR"
	EnvServicePort               = "WEFTY_SERVICE_PORT"
	DefaultHandoffRoot           = "/tmp/wefty/handoffs"
	OCIContainerHandoffDirectory = "/wefty/handoff"
	OCIContainerServiceDirectory = "/wefty/service"

	// StableNodeTagPrefix reserves the routing tag used when a cold rerun
	// consumes node-local handoff files from an earlier execution.
	StableNodeTagPrefix = "wefty:node:"
)

// IsOCIReservedEnvironmentName reports whether name is one of the exact M3
// values that an OCI runtime strips before injecting authoritative values.
// Other tenant-defined WEFTY_* names are deliberately not reserved.
func IsOCIReservedEnvironmentName(name string) bool {
	switch name {
	case EnvHandoffDir, EnvServiceDir, EnvServicePort, EnvL3Endpoint, EnvRunToken:
		return true
	default:
		return false
	}
}

// JobSpec is the versioned, transport-neutral description of a job. Kind and
// Class are deliberately strings rather than closed enums: an agent decides
// whether it can execute an otherwise-valid workload.
type JobSpec struct {
	SchemaVersion    int               `json:"schema_version"`
	DispatchKey      string            `json:"dispatch_key"`
	Kind             string            `json:"kind"`
	Class            string            `json:"class"`
	PublishedPort    *int              `json:"published_port,omitempty"`
	Restart          string            `json:"restart,omitempty"`
	MaxRestartStreak *int              `json:"max_restart_streak,omitempty"`
	RuntimeHandler   string            `json:"runtime_handler,omitempty"`
	RoutingTags      []string          `json:"routing_tags,omitempty"`
	Execution        ExecutionSpec     `json:"execution"`
	Limits           *JobLimits        `json:"limits,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
}

const (
	JobKindProcess = "process"
	JobKindOCI     = "oci"

	JobClassOneShot = "one-shot"
	JobClassService = "service"
	RestartAlways   = "always"
)

type ExecutionSpec struct {
	Executable       ExecutableSpec    `json:"executable,omitzero"`
	Argv             []string          `json:"argv,omitzero"`
	Env              map[string]string `json:"env,omitempty"`
	SensitiveEnv     map[string]string `json:"sensitive_env,omitempty"`
	WorkingDirectory string            `json:"working_directory,omitzero"`
	HandoffDirectory string            `json:"handoff_directory,omitempty"`
	OCI              *OCIExecutionSpec `json:"oci,omitempty"`
	executableSet    bool
	argvSet          bool
	workingDirSet    bool
	handoffDirSet    bool
	ociSet           bool
}

// UnmarshalJSON records wire presence for the asymmetric process and OCI arms.
// A present null or zero-valued member is still present and therefore cannot
// evade a cross-arm prohibition.
func (s *ExecutionSpec) UnmarshalJSON(data []byte) error {
	type wire ExecutionSpec
	var decoded wire
	if err := decodeJSONStrict(data, &decoded); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		return err
	}
	*s = ExecutionSpec(decoded)
	_, s.executableSet = members["executable"]
	_, s.argvSet = members["argv"]
	_, s.workingDirSet = members["working_directory"]
	_, s.handoffDirSet = members["handoff_directory"]
	_, s.ociSet = members["oci"]
	return nil
}

// OCIExecutionSpec is the container payload arm selected by kind=oci. Env and
// SensitiveEnv remain on ExecutionSpec because they are shared by every kind.
type OCIExecutionSpec struct {
	Image            OCIImageSpec `json:"image"`
	Argv             []string     `json:"argv,omitempty"`
	WorkingDirectory *string      `json:"working_directory,omitempty"`
	Mounts           []OCIMount   `json:"mounts,omitempty"`
	Limits           *OCILimits   `json:"limits,omitempty"`
	argvNull         bool
	workingDirNull   bool
	mountsNull       bool
	limitsNull       bool
}

// UnmarshalJSON preserves explicit nulls on optional OCI fields so the Go
// validator enforces the same absent-versus-null contract as JSON Schema.
func (s *OCIExecutionSpec) UnmarshalJSON(data []byte) error {
	type wire OCIExecutionSpec
	var decoded wire
	if err := decodeJSONStrict(data, &decoded); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		return err
	}
	*s = OCIExecutionSpec(decoded)
	s.argvNull = rawJSONNull(members["argv"])
	s.workingDirNull = rawJSONNull(members["working_directory"])
	s.mountsNull = rawJSONNull(members["mounts"])
	s.limitsNull = rawJSONNull(members["limits"])
	return nil
}

// OCIImageSpec keeps the submitted reference as provenance while Digest, when
// present, freezes the registry object that the runtime will execute.
type OCIImageSpec struct {
	Reference  string  `json:"reference"`
	Digest     *string `json:"digest,omitempty"`
	digestNull bool
}

// UnmarshalJSON preserves the distinction between an absent optional digest
// and an explicit JSON null so Go validation matches the JSON Schema surface.
func (s *OCIImageSpec) UnmarshalJSON(data []byte) error {
	type wire OCIImageSpec
	var decoded wire
	if err := decodeJSONStrict(data, &decoded); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		return err
	}
	*s = OCIImageSpec(decoded)
	if rawJSONNull(members["digest"]) {
		s.digestNull = true
	}
	return nil
}

func rawJSONNull(raw json.RawMessage) bool {
	return string(raw) == "null"
}

type OCIMount struct {
	NodePath      string `json:"node_path"`
	ContainerPath string `json:"container_path"`
	ReadOnly      bool   `json:"read_only,omitempty"`
	readOnlyNull  bool
}

func (m *OCIMount) UnmarshalJSON(data []byte) error {
	type wire OCIMount
	var decoded wire
	if err := decodeJSONStrict(data, &decoded); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		return err
	}
	*m = OCIMount(decoded)
	m.readOnlyNull = rawJSONNull(members["read_only"])
	return nil
}

// OCILimits are optional cgroup-v2 hard limits. Each pointer distinguishes an
// absent, uncapped limit from an explicitly invalid zero value on the wire.
type OCILimits struct {
	MemoryBytes   *int64 `json:"memory_bytes,omitempty"`
	CPUMillicores *int64 `json:"cpu_millicores,omitempty"`
	memoryNull    bool
	cpuNull       bool
}

func (l *OCILimits) UnmarshalJSON(data []byte) error {
	type wire OCILimits
	var decoded wire
	if err := decodeJSONStrict(data, &decoded); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		return err
	}
	*l = OCILimits(decoded)
	l.memoryNull = rawJSONNull(members["memory_bytes"])
	l.cpuNull = rawJSONNull(members["cpu_millicores"])
	return nil
}

func decodeJSONStrict(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("json: multiple values")
		}
		return err
	}
	return nil
}

// ExecutableSpec uses either a node-local path or inline bytes. Inline content
// is base64 encoded and accompanied by its SHA-256 provenance hash.
type ExecutableSpec struct {
	Path         string   `json:"path,omitempty"`
	InlineBase64 string   `json:"inline_base64,omitempty"`
	SHA256       string   `json:"sha256,omitempty"`
	Interpreter  []string `json:"interpreter,omitempty"`
	Mode         uint32   `json:"mode,omitempty"`
}

type JobLimits struct {
	MaxRuntimeSeconds        int `json:"max_runtime_seconds,omitempty"`
	IdleTimeoutSeconds       int `json:"idle_timeout_seconds,omitempty"`
	CompletionTimeoutSeconds int `json:"completion_timeout_seconds,omitempty"`
}

type Envelope struct {
	SchemaVersion     int             `json:"schema_version"`
	EnvelopeID        string          `json:"envelope_id"`
	IdempotencyKey    string          `json:"idempotency_key"`
	RunID             string          `json:"run_id"`
	StepID            string          `json:"step_id"`
	AttemptID         string          `json:"attempt_id"`
	Status            EnvelopeStatus  `json:"status"`
	Summary           string          `json:"summary"`
	Artifacts         []Artifact      `json:"artifacts,omitempty"`
	NotesForNextAgent string          `json:"notes_for_next_agent,omitempty"`
	Extensions        json.RawMessage `json:"extensions,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
}

type EnvelopeStatus string

const (
	EnvelopeSucceeded EnvelopeStatus = "succeeded"
	EnvelopeFailed    EnvelopeStatus = "failed"
	EnvelopePartial   EnvelopeStatus = "partial"
)

type Artifact struct {
	Name      string `json:"name"`
	URI       string `json:"uri"`
	MediaType string `json:"media_type,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
}

type GateResult struct {
	SchemaVersion  int         `json:"schema_version"`
	GateID         string      `json:"gate_id"`
	IdempotencyKey string      `json:"idempotency_key"`
	RunID          string      `json:"run_id"`
	StepID         string      `json:"step_id"`
	AttemptID      string      `json:"attempt_id"`
	Name           string      `json:"name"`
	Outcome        GateOutcome `json:"outcome"`
	Evidence       []Evidence  `json:"evidence,omitempty"`
	EvaluatedAt    time.Time   `json:"evaluated_at"`
}

type GateOutcome string

const (
	GatePass    GateOutcome = "pass"
	GateFail    GateOutcome = "fail"
	GateError   GateOutcome = "error"
	GateSkipped GateOutcome = "skipped"
)

type Evidence struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

type RunRecord struct {
	SchemaVersion int             `json:"schema_version"`
	RunID         string          `json:"run_id"`
	L1JobID       string          `json:"l1_job_id,omitempty"`
	NodeID        string          `json:"node_id,omitempty"`
	ParentRunID   string          `json:"parent_run_id,omitempty"`
	DispatchKey   string          `json:"dispatch_key"`
	Status        RunState        `json:"status"`
	Trigger       Trigger         `json:"trigger"`
	Workflow      WorkflowSource  `json:"workflow"`
	Params        json.RawMessage `json:"params"`
	Tags          []string        `json:"tags,omitempty"`
	Limits        *RunLimits      `json:"limits,omitempty"`
	Envelopes     []Envelope      `json:"envelopes,omitempty"`
	Gates         []GateResult    `json:"gates,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	StartedAt     *time.Time      `json:"started_at,omitempty"`
	FinishedAt    *time.Time      `json:"finished_at,omitempty"`
}

type Trigger struct {
	Type        string `json:"type"`
	Principal   string `json:"principal"`
	SourceRunID string `json:"source_run_id,omitempty"`
}

type WorkflowSource struct {
	WorkflowRef  string        `json:"workflow_ref,omitempty"`
	InlineScript *InlineScript `json:"inline_script,omitempty"`
	Image        *ImageProgram `json:"image,omitempty"`
}

type InlineScript struct {
	Content string `json:"content"`
	SHA256  string `json:"sha256"`
}

// ImageProgram is the immutable L3 program arm for kind=oci work. It keeps
// the submitted reference as provenance while copying every operator-selected
// execution field unchanged into L1 on dispatch and rerun.
type ImageProgram struct {
	Reference        string     `json:"reference"`
	Digest           *string    `json:"digest,omitempty"`
	Argv             []string   `json:"argv,omitempty"`
	WorkingDirectory *string    `json:"working_directory,omitempty"`
	Mounts           []OCIMount `json:"mounts,omitempty"`
	Limits           *OCILimits `json:"limits,omitempty"`
	RuntimeHandler   string     `json:"runtime_handler,omitempty"`
	digestNull       bool
	argvNull         bool
	workingDirNull   bool
	mountsNull       bool
	limitsNull       bool
}

// UnmarshalJSON preserves explicit nulls so L3's Go validation agrees with
// its OpenAPI surface instead of silently defaulting malformed snapshots.
func (p *ImageProgram) UnmarshalJSON(data []byte) error {
	type wire ImageProgram
	var decoded wire
	if err := decodeJSONStrict(data, &decoded); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		return err
	}
	*p = ImageProgram(decoded)
	p.digestNull = rawJSONNull(members["digest"])
	p.argvNull = rawJSONNull(members["argv"])
	p.workingDirNull = rawJSONNull(members["working_directory"])
	p.mountsNull = rawJSONNull(members["mounts"])
	p.limitsNull = rawJSONNull(members["limits"])
	return nil
}

type RunLimits struct {
	MaxRuntimeSeconds int     `json:"max_runtime_seconds,omitempty"`
	MaxCost           float64 `json:"max_cost,omitempty"`
}

// LogEvent is ordered within one attempt and stream. Exactly one of Bytes or
// Gap is present. Bytes is base64 encoded by encoding/json. A gap's Sequence
// is the first lost sequence and ThroughSequence is the inclusive last one.
type LogEvent struct {
	AttemptID string    `json:"attempt_id"`
	Stream    LogStream `json:"stream"`
	Sequence  uint64    `json:"sequence"`
	Timestamp time.Time `json:"timestamp"`
	Bytes     []byte    `json:"bytes,omitempty"`
	Gap       *LogGap   `json:"gap,omitempty"`
}

// LogGap is an agent-side declaration that raw events in one stream are no
// longer available. SourceEventSHA256 is set when L1 converts a received raw
// event to a gap after the late-evidence observation window, preserving
// idempotency without retaining the raw payload.
type LogGap struct {
	ThroughSequence   uint64       `json:"through_sequence"`
	LostEventCount    uint64       `json:"lost_event_count"`
	LostByteCount     uint64       `json:"lost_byte_count"`
	Reason            LogGapReason `json:"reason"`
	SourceEventSHA256 string       `json:"source_event_sha256,omitempty"`
}

type LogGapReason string

const (
	LogGapSpoolEviction             LogGapReason = "spool_eviction"
	LogGapOversizedEvent            LogGapReason = "oversized_event"
	LogGapReplayRejected            LogGapReason = "replay_rejected"
	LogGapLateEvidenceWindowExpired LogGapReason = "late_evidence_window_expired"
)

type LogStream string

const (
	LogStdout LogStream = "stdout"
	LogStderr LogStream = "stderr"
)

// ProcessResult is the mutually exclusive execution/finalization outcome of
// one process. ExitCode is a pointer so a successful exit code of zero remains
// present on the wire when omitempty is applied. OutputError supersedes an
// otherwise-successful exit when durable output cannot be finalized. A signal
// outcome always names its structured initiator in TerminationCause.
type ProcessResult struct {
	SpawnError       *SpawnFailure    `json:"spawn_error,omitempty"`
	RuntimeFailure   *RuntimeFailure  `json:"runtime_failure,omitempty"`
	OutputError      string           `json:"output_error,omitempty"`
	ExitCode         *int             `json:"exit_code,omitempty"`
	Signal           string           `json:"signal,omitempty"`
	TerminationCause TerminationCause `json:"termination_cause,omitempty"`
	OOM              bool             `json:"oom,omitempty"`
}

// RuntimeFailure is stable machine-readable evidence that the runtime or
// helper disappeared after execution was authoritatively acknowledged.
// Message is diagnostic only; policy must key exclusively on Code.
type RuntimeFailure struct {
	Code    RuntimeFailureCode `json:"code"`
	Message string             `json:"message"`
}

type RuntimeFailureCode string

const RuntimeFailureUnavailable RuntimeFailureCode = "runtime_unavailable"

// SpawnFailure is a stable machine-readable pre-execution failure. Message is
// diagnostic only; policy must key exclusively on Code.
type SpawnFailure struct {
	Code    SpawnFailureCode `json:"code"`
	Message string           `json:"message"`
}

type SpawnFailureCode string

const (
	SpawnFailureUnsupportedClass           SpawnFailureCode = "unsupported_class"
	SpawnFailureUnsupportedKind            SpawnFailureCode = "unsupported_kind"
	SpawnFailureUnsupportedRuntimeHandler  SpawnFailureCode = "unsupported_runtime_handler"
	SpawnFailureManagedResourcePreparation SpawnFailureCode = "managed_resource_preparation_failed"
	SpawnFailureHandoffPreparation         SpawnFailureCode = "handoff_preparation_failed"
	SpawnFailureExecutableMaterialization  SpawnFailureCode = "executable_materialization_failed"
	SpawnFailureWorkflowBridgeCreation     SpawnFailureCode = "workflow_bridge_creation_failed"
	SpawnFailureLogSinkSetup               SpawnFailureCode = "log_sink_setup_failed"
	SpawnFailureProcessRequest             SpawnFailureCode = "process_request_invalid"
	SpawnFailureProcessGroupSetup          SpawnFailureCode = "process_group_setup_failed"
	SpawnFailureProcessSpawn               SpawnFailureCode = "process_spawn_failed"
	SpawnFailureProcessWait                SpawnFailureCode = "process_wait_failed"
	SpawnFailurePublishedPortOccupied      SpawnFailureCode = "published_port_occupied"
	SpawnFailurePublishedListener          SpawnFailureCode = "published_listener_failed"
	SpawnFailureStartupReadinessTimeout    SpawnFailureCode = "startup_readiness_timeout"
	SpawnFailureRuntimeUnavailable         SpawnFailureCode = "runtime_unavailable"
	SpawnFailureImageUnavailable           SpawnFailureCode = "image_unavailable"
	SpawnFailureImageNotFound              SpawnFailureCode = "image_not_found"
	SpawnFailureImageManifestInvalid       SpawnFailureCode = "image_manifest_invalid"
	SpawnFailureImagePlatformUnsupported   SpawnFailureCode = "image_platform_unsupported"
	SpawnFailureOCISpecRejected            SpawnFailureCode = "oci_spec_rejected"
)

// TerminationCause identifies who initiated a signal termination. A service
// policy may combine this with durable desired/node state, but must never infer
// intent by parsing Signal or an error message.
type TerminationCause string

const (
	TerminationCauseSpontaneous TerminationCause = "spontaneous"
	TerminationCauseAgent       TerminationCause = "agent"
	TerminationCauseGuardian    TerminationCause = "guardian"
)

// NodeRegistration intentionally has no tags or capacity fields. Claim
// eligibility uses authoritative policy obtained from control-plane
// configuration. Capabilities describes executable support and must not carry
// max_oneshot_slots or max_service_slots.
type NodeRegistration struct {
	NodeID        string `json:"node_id"`
	BootSessionID string `json:"boot_session_id"`
	// ConnectHost is a Fabric-produced operator hint. It is deliberately
	// non-authoritative and never participates in identity, authorization, or
	// scheduling.
	ConnectHost string `json:"connect_host,omitempty"`
	// RootInstanceID identifies the agent-owned managed resource root for this
	// stable node. It is a self-reported local-state fact, not scheduling or
	// execution authority.
	RootInstanceID string          `json:"root_instance_id,omitempty"`
	OS             string          `json:"os"`
	Architecture   string          `json:"architecture"`
	AgentVersion   string          `json:"agent_version"`
	Capabilities   map[string]bool `json:"capabilities,omitempty"`
}
