package contract

import (
	"encoding/json"
	"time"
)

const SchemaVersionV1 = 1

// Run execution environment names are part of the v0.1 process contract.
// Keep credentials in SensitiveEnv so public job projections can redact them.
const (
	EnvRunID           = "WEFTY_RUN_ID"
	EnvL3Endpoint      = "WEFTY_L3_ENDPOINT"
	EnvRunToken        = "WEFTY_RUN_TOKEN"
	EnvHandoffDir      = "WEFTY_HANDOFF_DIR"
	DefaultHandoffRoot = "/tmp/wefty/handoffs"

	// StableNodeTagPrefix reserves the routing tag used when a cold rerun
	// consumes node-local handoff files from an earlier execution.
	StableNodeTagPrefix = "wefty:node:"
)

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
	JobClassOneShot = "one-shot"
	JobClassService = "service"
	RestartAlways   = "always"
)

type ExecutionSpec struct {
	Executable       ExecutableSpec    `json:"executable"`
	Argv             []string          `json:"argv"`
	Env              map[string]string `json:"env,omitempty"`
	SensitiveEnv     map[string]string `json:"sensitive_env,omitempty"`
	WorkingDirectory string            `json:"working_directory"`
	HandoffDirectory string            `json:"handoff_directory,omitempty"`
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
}

type InlineScript struct {
	Content string `json:"content"`
	SHA256  string `json:"sha256"`
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
	OutputError      string           `json:"output_error,omitempty"`
	ExitCode         *int             `json:"exit_code,omitempty"`
	Signal           string           `json:"signal,omitempty"`
	TerminationCause TerminationCause `json:"termination_cause,omitempty"`
}

// SpawnFailure is a stable machine-readable pre-execution failure. Message is
// diagnostic only; policy must key exclusively on Code.
type SpawnFailure struct {
	Code    SpawnFailureCode `json:"code"`
	Message string           `json:"message"`
}

type SpawnFailureCode string

const (
	SpawnFailureUnsupportedClass          SpawnFailureCode = "unsupported_class"
	SpawnFailureUnsupportedKind           SpawnFailureCode = "unsupported_kind"
	SpawnFailureUnsupportedRuntimeHandler SpawnFailureCode = "unsupported_runtime_handler"
	SpawnFailureHandoffPreparation        SpawnFailureCode = "handoff_preparation_failed"
	SpawnFailureExecutableMaterialization SpawnFailureCode = "executable_materialization_failed"
	SpawnFailureWorkflowBridgeCreation    SpawnFailureCode = "workflow_bridge_creation_failed"
	SpawnFailureLogSinkSetup              SpawnFailureCode = "log_sink_setup_failed"
	SpawnFailureProcessRequest            SpawnFailureCode = "process_request_invalid"
	SpawnFailureProcessGroupSetup         SpawnFailureCode = "process_group_setup_failed"
	SpawnFailureProcessSpawn              SpawnFailureCode = "process_spawn_failed"
	SpawnFailureProcessWait               SpawnFailureCode = "process_wait_failed"
	SpawnFailurePublishedPortOccupied     SpawnFailureCode = "published_port_occupied"
	SpawnFailureStartupReadinessTimeout   SpawnFailureCode = "startup_readiness_timeout"
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
	NodeID        string          `json:"node_id"`
	BootSessionID string          `json:"boot_session_id"`
	OS            string          `json:"os"`
	Architecture  string          `json:"architecture"`
	AgentVersion  string          `json:"agent_version"`
	Capabilities  map[string]bool `json:"capabilities,omitempty"`
}
