package contract

import (
	"encoding/json"
	"time"
)

const SchemaVersionV1 = 1

// JobSpec is the versioned, transport-neutral description of a one-shot job.
// Kind is deliberately a string rather than a closed enum.
type JobSpec struct {
	SchemaVersion  int               `json:"schema_version"`
	DispatchKey    string            `json:"dispatch_key"`
	Kind           string            `json:"kind"`
	RuntimeHandler string            `json:"runtime_handler,omitempty"`
	RoutingTags    []string          `json:"routing_tags,omitempty"`
	Execution      ExecutionSpec     `json:"execution"`
	Limits         *JobLimits        `json:"limits,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
}

type ExecutionSpec struct {
	Executable       ExecutableSpec    `json:"executable"`
	Argv             []string          `json:"argv"`
	Env              map[string]string `json:"env,omitempty"`
	SensitiveEnv     map[string]string `json:"sensitive_env,omitempty"`
	WorkingDirectory string            `json:"working_directory"`
	HandoffDirectory string            `json:"handoff_directory"`
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

// LogEvent is ordered within one attempt and stream. Bytes contains raw log
// bytes and is base64 encoded by encoding/json.
type LogEvent struct {
	AttemptID string    `json:"attempt_id"`
	Stream    LogStream `json:"stream"`
	Sequence  uint64    `json:"sequence"`
	Timestamp time.Time `json:"timestamp"`
	Bytes     []byte    `json:"bytes"`
}

type LogStream string

const (
	LogStdout LogStream = "stdout"
	LogStderr LogStream = "stderr"
)

// NodeRegistration intentionally has no tags field. Claim eligibility uses
// authoritative tags obtained from Fabric identity/configuration.
type NodeRegistration struct {
	NodeID        string          `json:"node_id"`
	BootSessionID string          `json:"boot_session_id"`
	OS            string          `json:"os"`
	Architecture  string          `json:"architecture"`
	AgentVersion  string          `json:"agent_version"`
	Capabilities  map[string]bool `json:"capabilities,omitempty"`
}
