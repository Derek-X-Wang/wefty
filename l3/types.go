// Package l3 implements wefty's durable run ledger and L1 dispatch outbox.
package l3

import (
	"encoding/json"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

const (
	DefaultCallerPrincipalTag = "tag:wefty-client"
	DefaultL1Address          = "wefty://control-plane"
	DefaultL3Address          = "wefty://run-ledger"
	DefaultHandoffRoot        = contract.DefaultHandoffRoot
	DefaultRunTokenGrace      = 5 * time.Minute
)

// Clock supplies ledger timestamps so state projection can be tested without
// wall-clock sleeps.
type Clock interface {
	Now() time.Time
}

type ClockFunc func() time.Time

func (f ClockFunc) Now() time.Time { return f() }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// InlineScriptInput is the inline workflow accepted by POST /v1/runs. SHA256
// is supplied by the caller and verified against Content before persistence.
type InlineScriptInput struct {
	Content     string   `json:"content"`
	SHA256      string   `json:"sha256"`
	Interpreter []string `json:"interpreter,omitempty"`
	Mode        *uint32  `json:"mode,omitempty"`
}

// WorkflowVersionInput is the executable snapshot accepted when appending a
// saved workflow version. Versions are assigned monotonically by the ledger.
type WorkflowVersionInput struct {
	Content     string   `json:"content"`
	SHA256      string   `json:"sha256"`
	Interpreter []string `json:"interpreter,omitempty"`
	Mode        *uint32  `json:"mode,omitempty"`
}

type WorkflowVersion struct {
	WorkflowID  string    `json:"workflow_id"`
	Version     int       `json:"version"`
	WorkflowRef string    `json:"workflow_ref"`
	Content     string    `json:"content"`
	SHA256      string    `json:"sha256"`
	Interpreter []string  `json:"interpreter"`
	Mode        uint32    `json:"mode"`
	CreatedAt   time.Time `json:"created_at"`
}

type CreateRunRequest struct {
	WorkflowRef      string              `json:"workflow_ref,omitempty"`
	InlineScript     *InlineScriptInput  `json:"inline_script,omitempty"`
	Params           json.RawMessage     `json:"params"`
	Tags             []string            `json:"tags,omitempty"`
	Limits           *contract.RunLimits `json:"limits,omitempty"`
	EnvelopeSchema   json.RawMessage     `json:"envelope_schema,omitempty"`
	RequiredEnvelope bool                `json:"required_envelope,omitempty"`
	ParentRunID      string              `json:"parent_run_id,omitempty"`
}

type CreateRunInput struct {
	IdempotencyKey string
	Actor          string
	Request        CreateRunRequest
}

type CreateRerunInput struct {
	IdempotencyKey string
	Actor          string
	SourceRunID    string
}

type RunAccepted struct {
	RunID     string `json:"run_id"`
	StatusURL string `json:"status_url"`
	LogsURL   string `json:"logs_url"`
}

// RunExecution is the run-keyed diagnostic projection across the asynchronous
// L3 dispatch and L1 execution seam. Job is the redacted L1 projection and is
// absent until dispatch succeeds.
type RunExecution struct {
	RunID            string             `json:"run_id"`
	L1JobID          string             `json:"l1_job_id,omitempty"`
	DispatchAttempts int                `json:"dispatch_attempts"`
	DispatchError    *contract.APIError `json:"dispatch_error,omitempty"`
	Job              *l1.Job            `json:"job,omitempty"`
}

// TriggerProvenance is the immutable ledger row explaining why a run exists.
// Params is snapshotted separately from the run row so provenance remains
// self-contained for future trigger types.
type TriggerProvenance struct {
	RunID       string          `json:"run_id"`
	Actor       string          `json:"actor"`
	Source      string          `json:"source"`
	SourceRunID string          `json:"source_run_id,omitempty"`
	Params      json.RawMessage `json:"params"`
	CreatedAt   time.Time       `json:"created_at"`
}

// ProtocolRejection is the immutable audit record for a rejected in-run
// envelope or gate write. Rejected payloads never enter the accepted
// envelope/gate tables, but remain explainable after the run fails.
type ProtocolRejection struct {
	RejectionID    string          `json:"rejection_id"`
	RunID          string          `json:"run_id"`
	Kind           string          `json:"kind"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Body           json.RawMessage `json:"body"`
	Reason         string          `json:"reason"`
	CreatedAt      time.Time       `json:"created_at"`
}

type LineageEntry struct {
	RunID       string            `json:"run_id"`
	ParentRunID string            `json:"parent_run_id,omitempty"`
	Status      contract.RunState `json:"status"`
	Depth       int               `json:"depth"`
}

type RunLineage struct {
	RunID       string         `json:"run_id"`
	Ancestors   []LineageEntry `json:"ancestors"`
	Descendants []LineageEntry `json:"descendants"`
}

type StoreOptions struct {
	Clock         Clock
	RunTokenGrace time.Duration
}

// RunTokenScope is the authenticated authority carried by an opaque run
// token. AttemptID binds one minted credential to one dispatch execution.
type RunTokenScope struct {
	RunID     string
	AttemptID string
}
