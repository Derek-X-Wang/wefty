// Package l3 implements wefty's durable run ledger and L1 dispatch outbox.
package l3

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

const (
	DefaultCallerPrincipalTag        = "tag:wefty-client"
	DefaultL1Address                 = "wefty://control-plane"
	DefaultL3Address                 = "wefty://run-ledger"
	DefaultHandoffRoot               = contract.DefaultHandoffRoot
	DefaultRunTokenGrace             = 5 * time.Minute
	DefaultComputerSubmitMaxInflight = 20
	DefaultComputerRunPageLimit      = 100
	MaxComputerRunPageLimit          = 1000
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
	Content     string                 `json:"content,omitempty"`
	SHA256      string                 `json:"sha256,omitempty"`
	Interpreter []string               `json:"interpreter,omitempty"`
	Mode        *uint32                `json:"mode,omitempty"`
	Image       *contract.ImageProgram `json:"image,omitempty"`
}

type WorkflowVersion struct {
	WorkflowID  string                 `json:"workflow_id"`
	Version     int                    `json:"version"`
	WorkflowRef string                 `json:"workflow_ref"`
	Content     string                 `json:"content"`
	SHA256      string                 `json:"sha256"`
	Interpreter []string               `json:"interpreter,omitempty"`
	Mode        uint32                 `json:"mode,omitempty"`
	Image       *contract.ImageProgram `json:"image,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
}

// MarshalJSON keeps saved Workflow versions discriminated on the wire while
// preserving the existing flat script representation.
func (v WorkflowVersion) MarshalJSON() ([]byte, error) {
	type common struct {
		WorkflowID  string    `json:"workflow_id"`
		Version     int       `json:"version"`
		WorkflowRef string    `json:"workflow_ref"`
		CreatedAt   time.Time `json:"created_at"`
	}
	base := common{WorkflowID: v.WorkflowID, Version: v.Version, WorkflowRef: v.WorkflowRef, CreatedAt: v.CreatedAt}
	if v.Image != nil {
		return json.Marshal(struct {
			common
			Image *contract.ImageProgram `json:"image"`
		}{common: base, Image: v.Image})
	}
	return json.Marshal(struct {
		common
		Content     string   `json:"content"`
		SHA256      string   `json:"sha256"`
		Interpreter []string `json:"interpreter"`
		Mode        uint32   `json:"mode"`
	}{common: base, Content: v.Content, SHA256: v.SHA256, Interpreter: nonNilStrings(v.Interpreter), Mode: v.Mode})
}

type CreateRunRequest struct {
	WorkflowRef      string                 `json:"workflow_ref,omitempty"`
	InlineScript     *InlineScriptInput     `json:"inline_script,omitempty"`
	Image            *contract.ImageProgram `json:"image,omitempty"`
	Params           json.RawMessage        `json:"params"`
	Tags             []string               `json:"tags,omitempty"`
	Limits           *contract.RunLimits    `json:"limits,omitempty"`
	EnvelopeSchema   json.RawMessage        `json:"envelope_schema,omitempty"`
	RequiredEnvelope bool                   `json:"required_envelope,omitempty"`
	ParentRunID      string                 `json:"parent_run_id,omitempty"`
}

type CreateRunInput struct {
	IdempotencyKey string
	Actor          string
	Request        CreateRunRequest
	ComputerScope  *ComputerTokenScope
	// VerifyComputerScope is called after the immediate SQLite write transaction
	// begins and before any Run row is committed. The caller must re-prove the
	// exact live L1 attempt authority represented by ComputerScope.
	VerifyComputerScope func(context.Context, ComputerTokenScope) error
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

// AttemptImageEvidence is the accepted L1 observation needed to freeze a
// tag-only run before a cold rerun. It intentionally carries only identity,
// observation time, and provenance needed by the L3 snapshot ledger.
type AttemptImageEvidence struct {
	AttemptID          string
	SubmittedReference string
	TopLevelDigest     string
	PlatformDigest     *string
	ObservedAt         time.Time
}

// TriggerProvenance is the immutable ledger row explaining why a run exists.
// Params is snapshotted separately from the run row so provenance remains
// self-contained for future trigger types.
type TriggerProvenance struct {
	RunID                     string          `json:"run_id"`
	Actor                     string          `json:"actor"`
	Source                    string          `json:"source"`
	SourceRunID               string          `json:"source_run_id,omitempty"`
	ComputerID                string          `json:"computer_id,omitempty"`
	ComputerAttemptID         string          `json:"computer_attempt_id,omitempty"`
	ComputerStorageGeneration int64           `json:"computer_storage_generation,omitempty"`
	SubmitIntentRevision      int64           `json:"submit_intent_revision,omitempty"`
	Params                    json.RawMessage `json:"params"`
	CreatedAt                 time.Time       `json:"created_at"`
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
	Clock                       Clock
	RunTokenGrace               time.Duration
	ComputerAuthorityInstanceID string
}

// RunTokenScope is the authenticated authority carried by an opaque run
// token. AttemptID binds one minted credential to one dispatch execution.
type RunTokenScope struct {
	RunID     string
	AttemptID string
}

type ComputerPermission string

const (
	ComputerPermissionSubmit  ComputerPermission = "submit"
	ComputerPermissionObserve ComputerPermission = "observe"
)

// ComputerTokenScope is the complete L3-derived identity of one Computer
// submission pass. No request header or run body may supply these fields.
type ComputerTokenScope struct {
	ComputerID                string
	ComputerAttemptID         string
	ComputerStorageGeneration int64
	SubmitIntentRevision      int64
	HostNodeID                string
	L3AuthorityGeneration     int64
	GrantRevision             int64
	SubmitMaxInflight         int
	Permissions               []ComputerPermission
}

type ComputerTokenMintRequest struct {
	ComputerID        string `json:"computer_id"`
	ComputerAttemptID string `json:"computer_attempt_id"`
}

type ComputerTokenGrant struct {
	Token                     string               `json:"token"`
	ComputerID                string               `json:"computer_id"`
	ComputerAttemptID         string               `json:"computer_attempt_id"`
	ComputerStorageGeneration int64                `json:"computer_storage_generation"`
	SubmitIntentRevision      int64                `json:"submit_intent_revision"`
	HostNodeID                string               `json:"host_node_id"`
	L3AuthorityGeneration     int64                `json:"l3_authority_generation"`
	GrantRevision             int64                `json:"grant_revision"`
	SubmitMaxInflight         int                  `json:"submit_max_inflight"`
	Permissions               []ComputerPermission `json:"permissions"`
}

type ComputerTokenRevocationRequest struct {
	ComputerID           string `json:"computer_id"`
	SubmitIntentRevision int64  `json:"submit_intent_revision"`
	RevokeAll            bool   `json:"revoke_all,omitempty"`
	Reason               string `json:"reason"`
}

type ComputerAttemptTokenRevocationRequest struct {
	ComputerID        string `json:"computer_id"`
	ComputerAttemptID string `json:"computer_attempt_id"`
	Reason            string `json:"reason"`
}

type HostComputerTokenRevocationRequest struct {
	Reason string `json:"reason"`
}

type ComputerSelf struct {
	ComputerID                string               `json:"computer_id"`
	ComputerStorageGeneration int64                `json:"computer_storage_generation"`
	GrantRevision             int64                `json:"grant_revision"`
	Permissions               []ComputerPermission `json:"permissions"`
}

type ComputerRunPage struct {
	Runs       []contract.RunRecord `json:"runs"`
	NextCursor string               `json:"next_cursor,omitempty"`
}

// ComputerTokenScopeProof is the L1-authoritative fact L3 consumes before it
// mints a pass. The host Node and attempt are verified, never caller asserted.
type ComputerTokenScopeProof struct {
	ComputerID                string `json:"computer_id"`
	ComputerAttemptID         string `json:"computer_attempt_id"`
	ComputerStorageGeneration int64  `json:"computer_storage_generation"`
	SubmitIntentRevision      int64  `json:"submit_intent_revision"`
	HostNodeID                string `json:"host_node_id"`
	SubmitMaxInflight         int    `json:"submit_max_inflight"`
}
