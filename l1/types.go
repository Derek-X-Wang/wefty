// Package l1 implements wefty's SQLite-backed L1 control plane.
package l1

import (
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	DefaultClientPrincipalTag = "tag:wefty-client"
	DefaultAgentPrincipalTag  = "tag:wefty-agent"
	DefaultLeaseDuration      = 30 * time.Second
	DefaultNodeStaleAfter     = 45 * time.Second
	DefaultNodeDeadAfter      = 2 * time.Minute
	DefaultReconcileInterval  = time.Second
)

// Clock supplies all control-plane timestamps used by lease logic.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function into a Clock.
type ClockFunc func() time.Time

func (f ClockFunc) Now() time.Time { return f() }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Job is the L1 HTTP representation of a job.
type Job struct {
	JobID            string            `json:"job_id"`
	State            contract.JobState `json:"state"`
	Spec             contract.JobSpec  `json:"spec"`
	CurrentAttemptID string            `json:"current_attempt_id,omitempty"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
}

// AttemptLease is the authority returned by a successful claim or renewal.
type AttemptLease struct {
	AttemptID    string    `json:"attempt_id"`
	FencingToken string    `json:"fencing_token"`
	LeaseExpires time.Time `json:"lease_expires_at"`
}

// Claim is returned when an eligible queued job is won.
type Claim struct {
	Job   Job          `json:"job"`
	Lease AttemptLease `json:"lease"`
}

// Node is the registered node record returned by the agent protocol.
type Node struct {
	contract.NodeRegistration
	State             contract.NodeState `json:"state"`
	AuthoritativeTags []string           `json:"authoritative_tags"`
	LastHeartbeatAt   time.Time          `json:"last_heartbeat_at"`
}

// ProcessResult matches the M0 completion contract. Pointer fields preserve
// the distinction between an omitted exit code and exit code zero.
type ProcessResult struct {
	SpawnError string `json:"spawn_error,omitempty"`
	ExitCode   *int   `json:"exit_code,omitempty"`
	Signal     string `json:"signal,omitempty"`
}

type CompletionRequest struct {
	FencingToken       string        `json:"fencing_token"`
	IdempotencyKey     string        `json:"idempotency_key"`
	Result             ProcessResult `json:"result"`
	ProtocolOutputHash string        `json:"protocol_output_digest,omitempty"`
}

type ClaimRequest struct {
	NodeID        string `json:"node_id"`
	BootSessionID string `json:"boot_session_id"`
}

type RenewalRequest struct {
	FencingToken string `json:"fencing_token"`
}

type HeartbeatRequest struct {
	BootSessionID string `json:"boot_session_id"`
}

type DrainRequest struct {
	BootSessionID string `json:"boot_session_id"`
}

// ReconcileResult reports the durable transitions won by one reconciliation
// pass. Counts are useful for observability and deterministic tests.
type ReconcileResult struct {
	ExpiredAttempts int64 `json:"expired_attempts"`
	StaleNodes      int64 `json:"stale_nodes"`
	DeadNodes       int64 `json:"dead_nodes"`
}
