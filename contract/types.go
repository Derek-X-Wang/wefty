package contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
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
	EnvComputerToken             = "WEFTY_COMPUTER_TOKEN"
	EnvComputerViewPort          = "WEFTY_COMPUTER_VIEW_PORT"
	EnvComputerControlPort       = "WEFTY_COMPUTER_CONTROL_PORT"
	DefaultHandoffRoot           = "/tmp/wefty/handoffs"
	OCIContainerHandoffDirectory = "/wefty/handoff"
	OCIContainerServiceDirectory = "/wefty/service"
	OCIContainerControlDirectory = "/wefty/control"

	// Computer image compatibility is a fixed named/wire contract. Keep these
	// values in the transport-neutral contract package so the agent, helper,
	// reference image, and conformance checker cannot drift independently.
	ComputerDisplayEndpointView               = "view"
	ComputerDisplayEndpointControl            = "control"
	ComputerDisplayWebSocketPath              = "/websockify"
	ComputerDisplayWebSocketSubprotocol       = "binary"
	ComputerRFBVersionBannerBytes             = 12
	ComputerStartupReadinessTimeout           = 60 * time.Second
	ComputerDevShmBytes                 int64 = 1 << 30

	// StableNodeTagPrefix reserves the routing tag used when a cold rerun
	// consumes node-local handoff files from an earlier execution.
	StableNodeTagPrefix = "wefty:node:"
)

var ociReservedEnvironmentNames = [...]string{
	EnvHandoffDir,
	EnvServiceDir,
	EnvServicePort,
	EnvL3Endpoint,
	EnvRunToken,
	EnvComputerToken,
	EnvComputerViewPort,
	EnvComputerControlPort,
}

// IsOCIReservedEnvironmentName reports whether name is one of the exact
// ratified values that an OCI runtime strips before injecting authoritative
// values.
// Other tenant-defined WEFTY_* names are deliberately not reserved.
func IsOCIReservedEnvironmentName(name string) bool {
	for _, reserved := range ociReservedEnvironmentNames {
		if name == reserved {
			return true
		}
	}
	return false
}

// IsOCISensitiveReservedEnvironmentName reports the reserved values whose
// authoritative contents must travel only through the sensitive environment
// layer. Operator and image values are stripped for every reserved name.
func IsOCISensitiveReservedEnvironmentName(name string) bool {
	return name == EnvRunToken || name == EnvComputerToken
}

// IsComputerExecution is the single cross-layer discriminator for Computer
// mechanics. Durable disk attachment is a consequence of this trait, not a
// substitute signal for it.
func IsComputerExecution(execution ExecutionSpec) bool {
	return execution.OCI != nil && execution.OCI.Computer != nil
}

// ValidComputerRFBVersionBanner recognizes the 12-byte RFB version greeting
// required after the rfb-websocket-v1 upgrade. Version negotiation after the
// greeting remains the image's responsibility.
func ValidComputerRFBVersionBanner(banner []byte) bool {
	if len(banner) != ComputerRFBVersionBannerBytes || string(banner[:4]) != "RFB " || banner[7] != '.' || banner[11] != '\n' {
		return false
	}
	for _, index := range []int{4, 5, 6, 8, 9, 10} {
		if banner[index] < '0' || banner[index] > '9' {
			return false
		}
	}
	return true
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
	publishedPortSet bool
}

// UnmarshalJSON records whether published_port appeared on the wire. Plain
// services retain their historical null-or-absent contract, while the
// Computer trait forbids the member itself, including an explicit null.
func (s *JobSpec) UnmarshalJSON(data []byte) error {
	type wire JobSpec
	var decoded wire
	if err := decodeJSONStrict(data, &decoded); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		return err
	}
	*s = JobSpec(decoded)
	_, s.publishedPortSet = members["published_port"]
	return nil
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
	Image            OCIImageSpec     `json:"image"`
	Argv             []string         `json:"argv,omitempty"`
	WorkingDirectory *string          `json:"working_directory,omitempty"`
	Mounts           []OCIMount       `json:"mounts,omitempty"`
	Limits           *OCILimits       `json:"limits,omitempty"`
	Computer         *OCIComputerSpec `json:"computer,omitempty"`
	argvNull         bool
	workingDirNull   bool
	mountsNull       bool
	limitsNull       bool
	computerNull     bool
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
	s.computerNull = rawJSONNull(members["computer"])
	return nil
}

// ComputerDisplayProtocol is the closed display transport vocabulary for the
// OCI Computer trait.
type ComputerDisplayProtocol string

const ComputerDisplayProtocolRFBWebSocketV1 ComputerDisplayProtocol = "rfb-websocket-v1"

// OCIComputerSpec is the optional Computer trait on an OCI service Job. Its
// presence changes neither workload kind nor lifecycle class.
type OCIComputerSpec struct {
	Display   OCIComputerDisplaySpec `json:"display"`
	DiskBytes int64                  `json:"disk_bytes"`
}

func (s *OCIComputerSpec) UnmarshalJSON(data []byte) error {
	type wire struct {
		Display   OCIComputerDisplaySpec `json:"display"`
		DiskBytes json.RawMessage        `json:"disk_bytes"`
	}
	var decoded wire
	if err := decodeJSONStrict(data, &decoded); err != nil {
		return err
	}
	*s = OCIComputerSpec{Display: decoded.Display}
	if len(decoded.DiskBytes) > 0 {
		diskBytes, err := decodeJSONInt64(decoded.DiskBytes)
		if err != nil {
			return fmt.Errorf("disk_bytes: %w", err)
		}
		s.DiskBytes = diskBytes
	}
	return nil
}

type OCIComputerDisplaySpec struct {
	Protocol ComputerDisplayProtocol `json:"protocol"`
}

func (s *OCIComputerDisplaySpec) UnmarshalJSON(data []byte) error {
	type wire OCIComputerDisplaySpec
	var decoded wire
	if err := decodeJSONStrict(data, &decoded); err != nil {
		return err
	}
	*s = OCIComputerDisplaySpec(decoded)
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
	type wire struct {
		MemoryBytes   json.RawMessage `json:"memory_bytes"`
		CPUMillicores json.RawMessage `json:"cpu_millicores"`
	}
	var decoded wire
	if err := decodeJSONStrict(data, &decoded); err != nil {
		return err
	}
	*l = OCILimits{}
	if len(decoded.MemoryBytes) > 0 {
		if rawJSONNull(decoded.MemoryBytes) {
			l.memoryNull = true
		} else {
			memoryBytes, err := decodeJSONInt64(decoded.MemoryBytes)
			if err != nil {
				return fmt.Errorf("memory_bytes: %w", err)
			}
			l.MemoryBytes = &memoryBytes
		}
	}
	if len(decoded.CPUMillicores) > 0 {
		if rawJSONNull(decoded.CPUMillicores) {
			l.cpuNull = true
		} else {
			cpuMillicores, err := decodeJSONInt64(decoded.CPUMillicores)
			if err != nil {
				return fmt.Errorf("cpu_millicores: %w", err)
			}
			l.CPUMillicores = &cpuMillicores
		}
	}
	return nil
}

// decodeJSONInt64 accepts every JSON number that denotes a mathematically
// integral int64 value, independent of decimal or exponent notation.
func decodeJSONInt64(raw json.RawMessage) (int64, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return 0, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return 0, fmt.Errorf("json: multiple values")
		}
		return 0, err
	}
	number, ok := value.(json.Number)
	if !ok {
		return 0, fmt.Errorf("must be a JSON number")
	}
	rational, ok := new(big.Rat).SetString(number.String())
	if !ok || !rational.IsInt() || !rational.Num().IsInt64() {
		return 0, fmt.Errorf("must be an integral int64")
	}
	return rational.Num().Int64(), nil
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
	LogGapLoggerSourceIncomplete    LogGapReason = "logger_source_incomplete"
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
// outcome always names its structured initiator in TerminationCause. OOM,
// DiskExhausted, and LogEvidenceIncomplete are additive evidence and never
// replace the primary terminal arm.
type ProcessResult struct {
	SpawnError            *SpawnFailure    `json:"spawn_error,omitempty"`
	RuntimeFailure        *RuntimeFailure  `json:"runtime_failure,omitempty"`
	OutputError           string           `json:"output_error,omitempty"`
	ExitCode              *int             `json:"exit_code,omitempty"`
	Signal                string           `json:"signal,omitempty"`
	TerminationCause      TerminationCause `json:"termination_cause,omitempty"`
	OOM                   bool             `json:"oom,omitempty"`
	DiskExhausted         bool             `json:"disk_exhausted,omitempty"`
	LogEvidenceIncomplete bool             `json:"log_evidence_incomplete,omitempty"`
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
	Code                   SpawnFailureCode `json:"code"`
	Message                string           `json:"message"`
	NodeID                 string           `json:"node_id,omitempty"`
	RequestedBytes         int64            `json:"requested_bytes,omitempty"`
	ObservedAvailableBytes int64            `json:"observed_available_bytes,omitempty"`
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
	SpawnFailureInsufficientMemory         SpawnFailureCode = "insufficient_memory"
	SpawnFailureInsufficientDisk           SpawnFailureCode = "insufficient_disk"
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

// CapabilityReasonCode is the bounded, sanitized explanation for a node's
// missing execution capabilities. Detailed probe diagnostics remain local to
// the agent; this closed vocabulary is safe to retain in L1.
type CapabilityReasonCode string

const (
	CapabilityReasonOCIIntentDisabled         CapabilityReasonCode = "oci_intent_disabled"
	CapabilityReasonPrerequisiteMissing       CapabilityReasonCode = "prerequisite_missing"
	CapabilityReasonRuntimeVersionUnsupported CapabilityReasonCode = "runtime_version_unsupported"
	CapabilityReasonHelperUnreachable         CapabilityReasonCode = "helper_unreachable"
	CapabilityReasonHelperVersionMismatch     CapabilityReasonCode = "helper_version_mismatch"
	CapabilityReasonHelperHandshakeFailed     CapabilityReasonCode = "helper_handshake_failed"
	CapabilityReasonBootSweepFailed           CapabilityReasonCode = "boot_sweep_failed"
	CapabilityReasonProbeFailed               CapabilityReasonCode = "probe_failed"
	CapabilityReasonLimaStopped               CapabilityReasonCode = "lima_stopped"
	CapabilityReasonLimaBroken                CapabilityReasonCode = "lima_broken"
	CapabilityReasonLimaStartTimeout          CapabilityReasonCode = "lima_start_timeout"
	CapabilityReasonTemplateRestartRequired   CapabilityReasonCode = "template_restart_required"
	CapabilityReasonTemplateRecreateRequired  CapabilityReasonCode = "template_recreate_required"
	CapabilityReasonMountRootUnavailable      CapabilityReasonCode = "mount_root_unavailable"
	CapabilityReasonLocalPermissionDenied     CapabilityReasonCode = "local_permission_denied"
)

func (code CapabilityReasonCode) Valid() bool {
	switch code {
	case CapabilityReasonOCIIntentDisabled,
		CapabilityReasonPrerequisiteMissing,
		CapabilityReasonRuntimeVersionUnsupported,
		CapabilityReasonHelperUnreachable,
		CapabilityReasonHelperVersionMismatch,
		CapabilityReasonHelperHandshakeFailed,
		CapabilityReasonBootSweepFailed,
		CapabilityReasonProbeFailed,
		CapabilityReasonLimaStopped,
		CapabilityReasonLimaBroken,
		CapabilityReasonLimaStartTimeout,
		CapabilityReasonTemplateRestartRequired,
		CapabilityReasonTemplateRecreateRequired,
		CapabilityReasonMountRootUnavailable,
		CapabilityReasonLocalPermissionDenied:
		return true
	default:
		return false
	}
}

// ValidOCIRestriction reports whether code can explain an observation that
// has atomically withdrawn kind:oci during registration or recovery.
func (code CapabilityReasonCode) ValidOCIRestriction() bool {
	switch code {
	case CapabilityReasonOCIIntentDisabled,
		CapabilityReasonPrerequisiteMissing,
		CapabilityReasonRuntimeVersionUnsupported,
		CapabilityReasonHelperUnreachable,
		CapabilityReasonHelperVersionMismatch,
		CapabilityReasonHelperHandshakeFailed,
		CapabilityReasonBootSweepFailed,
		CapabilityReasonProbeFailed,
		CapabilityReasonLimaStopped,
		CapabilityReasonLimaBroken,
		CapabilityReasonLimaStartTimeout,
		CapabilityReasonTemplateRestartRequired,
		CapabilityReasonTemplateRecreateRequired,
		CapabilityReasonMountRootUnavailable,
		CapabilityReasonLocalPermissionDenied:
		return true
	default:
		return false
	}
}

// CapabilityObservation is one immutable, boot-scoped observation of the
// node's complete execution eligibility set. A higher Revision replaces the
// complete observation; the same revision is replayable only unchanged.
type CapabilityObservation struct {
	Revision            int64                `json:"capability_revision"`
	Capabilities        map[string]bool      `json:"capabilities"`
	ObservedAt          time.Time            `json:"capability_observed_at"`
	MissingCapabilities []string             `json:"missing_capabilities"`
	ReasonCode          CapabilityReasonCode `json:"capability_reason_code,omitempty"`
}

// NodeRegistration intentionally has no tags or capacity fields: operators own
// routing tags and class-scoped slot policy in the control plane. Capabilities
// are node-advertised execution facts such as kind:process, kind:oci,
// runtime_handler:<name>, and cgroup_v2; they must not carry max_oneshot_slots
// or max_service_slots.
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
	RootInstanceID       string               `json:"root_instance_id,omitempty"`
	OS                   string               `json:"os"`
	Architecture         string               `json:"architecture"`
	AgentVersion         string               `json:"agent_version"`
	Capabilities         map[string]bool      `json:"capabilities"`
	CapabilityRevision   int64                `json:"capability_revision"`
	CapabilityObservedAt time.Time            `json:"capability_observed_at"`
	MissingCapabilities  []string             `json:"missing_capabilities"`
	CapabilityReasonCode CapabilityReasonCode `json:"capability_reason_code,omitempty"`
	// SupersedeCapabilityRevision asks L1 to atomically assign stored N+1 to
	// this restrictive same-boot observation. It is reserved for a runtime
	// authority barrier that must revoke stale advertised capability before it
	// can learn the stored revision.
	SupersedeCapabilityRevision bool `json:"supersede_capability_revision,omitempty"`
}
