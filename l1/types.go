// Package l1 implements wefty's SQLite-backed L1 control plane.
package l1

import (
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	DefaultClientPrincipalTag                      = "tag:wefty-client"
	DefaultAgentPrincipalTag                       = "tag:wefty-agent"
	DefaultLeaseDuration                           = 30 * time.Second
	DefaultLateEvidenceWindow                      = 48 * time.Hour
	DefaultNodeStaleAfter                          = 45 * time.Second
	DefaultNodeDeadAfter                           = 2 * time.Minute
	DefaultReconcileInterval                       = time.Second
	DefaultServiceStabilityWindow                  = 2 * time.Minute
	DefaultServiceLogRetentionAge                  = 7 * 24 * time.Hour
	DefaultServiceLogRetentionBytes          int64 = 32 << 20
	DefaultServiceAttemptSummaries                 = 32
	DefaultMaxOneshotSlots                         = 4
	DefaultMaxServiceSlots                         = 2
	DefaultPrestartInfrastructureBudget            = 10 * time.Minute
	DefaultAdminBootstrapTTL                       = 10 * time.Minute
	DefaultComputerTakeoverAuditRetentionAge       = 90 * 24 * time.Hour
)

// Clock supplies all control-plane timestamps used by lease logic.
type Clock interface {
	Now() time.Time
}

type clockTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type clockTimerProvider interface {
	NewTimer(time.Duration) clockTimer
}

type systemClockTimer struct{ timer *time.Timer }

func (timer *systemClockTimer) C() <-chan time.Time { return timer.timer.C }
func (timer *systemClockTimer) Stop() bool          { return timer.timer.Stop() }

func newClockTimer(clock Clock, duration time.Duration) clockTimer {
	if provider, ok := clock.(clockTimerProvider); ok {
		return provider.NewTimer(duration)
	}
	return &systemClockTimer{timer: time.NewTimer(duration)}
}

// ClockFunc adapts a function into a Clock.
type ClockFunc func() time.Time

func (f ClockFunc) Now() time.Time { return f() }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Job is the L1 HTTP representation of a job.
type Job struct {
	JobID  string            `json:"job_id"`
	NodeID string            `json:"node_id,omitempty"`
	State  contract.JobState `json:"state"`
	Status string            `json:"status,omitempty"`
	// Spec is absent once removal has finalized because tombstones deliberately
	// retain no executable or environment bytes.
	Spec                contract.JobSpec `json:"spec,omitzero"`
	CurrentAttemptID    string           `json:"current_attempt_id,omitempty"`
	Attempts            []Attempt        `json:"attempts,omitempty"`
	UnschedulableReason string           `json:"unschedulable_reason,omitempty"`
	FailureReason       string           `json:"failure_reason,omitempty"`
	CreatedAt           time.Time        `json:"created_at"`
	UpdatedAt           time.Time        `json:"updated_at"`
	*ServiceJob
	Removal *ServiceRemoval `json:"removal,omitempty"`
}

// Attempt is the operator-safe execution summary for one retained attempt.
// It deliberately omits the fencing token, boot session, and authority
// generation: those grant or identify write authority rather than explain an
// execution. Result and LateResult are mutually exclusive by contract.
type Attempt struct {
	AttemptID      string                `json:"attempt_id"`
	NodeID         string                `json:"node_id"`
	State          contract.AttemptState `json:"state"`
	LeaseExpiresAt time.Time             `json:"lease_expires_at"`
	Result         *ProcessResult        `json:"result,omitempty"`
	LateResult     *LateResultEvidence   `json:"late_result,omitempty"`
	Image          *OCIImageEvidence     `json:"image,omitempty"`
	CreatedAt      time.Time             `json:"created_at"`
	UpdatedAt      time.Time             `json:"updated_at"`
}

// ServiceRemoval is the operator projection for an irreversible service
// removal. CleanupFence is intentionally absent: only the bound agent receives
// that authority through RemovalDirective.
type ServiceRemoval struct {
	RemovalDesiredState   contract.ServiceDesiredState `json:"desired_state"`
	RemovalBoundNodeID    string                       `json:"bound_node_id,omitempty"`
	RemovalGeneration     uint64                       `json:"removal_generation"`
	RemovalRequestedAt    time.Time                    `json:"removal_requested_at"`
	RemovalOutcome        ServiceRemovalOutcome        `json:"removal_outcome,omitempty"`
	RemovedAt             *time.Time                   `json:"removed_at,omitempty"`
	CleanupAcknowledgedAt *time.Time                   `json:"cleanup_acknowledged_at,omitempty"`
}

type ServiceRemovalOutcome string

const (
	ServiceRemovalVerified  ServiceRemovalOutcome = "verified_removed"
	ServiceRemovalForgotten ServiceRemovalOutcome = "force_forgotten"
)

// RemovalDirective is durable node-scoped cleanup authority. Unlike an
// attempt lease, it has no expiry; only the node's current boot session may
// act on it.
type RemovalDirective struct {
	JobID                      string                           `json:"job_id"`
	BoundNodeID                string                           `json:"bound_node_id"`
	Kind                       string                           `json:"kind"`
	RemovalGeneration          uint64                           `json:"removal_generation"`
	CleanupFence               string                           `json:"cleanup_fence"`
	RootInstanceID             string                           `json:"root_instance_id"`
	ComputerStorage            *ComputerStorageClaim            `json:"computer_storage,omitempty"`
	ComputerStorageGenerations *ComputerStorageGenerationClaims `json:"computer_storage_generations,omitempty"`
	ComputerBackupCopies       *ComputerBackupCopyClaims        `json:"computer_backup_copies,omitempty"`
	ComputerCustodyExports     *ComputerCustodyExportClaims     `json:"computer_custody_exports,omitempty"`
}

type ComputerBackupCopyClaims struct {
	Copies []ComputerBackupPruneDirective `json:"copies"`
}

type ComputerCustodyExportClaims struct {
	Exports []ComputerCustodyExportClaim `json:"exports"`
}

type ComputerCustodyExportClaim struct {
	ExportID                string `json:"export_id"`
	SourceStorageID         string `json:"source_storage_id"`
	SourceGeneration        int64  `json:"source_generation"`
	Status                  string `json:"status"`
	OperatorAttestedDeleted bool   `json:"operator_attested_deleted"`
}

type ComputerStorageGenerationClaims struct {
	Generations []ComputerStorageGenerationClaim `json:"generations"`
}

// ComputerStorageGenerationClaim is Storage identity and allocation truth for
// removal. IntentRevision is deliberately absent: it is mutable intent, not
// part of (computer_id, storage_id, generation) identity.
type ComputerStorageGenerationClaim struct {
	ComputerID        string `json:"computer_id"`
	StorageID         string `json:"storage_id"`
	StorageGeneration int64  `json:"storage_generation"`
	DiskBytes         int64  `json:"disk_bytes"`
}

type RemovalAcknowledgementRequest struct {
	NodeID            string `json:"node_id"`
	BootSessionID     string `json:"boot_session_id"`
	RemovalGeneration uint64 `json:"removal_generation"`
	CleanupFence      string `json:"cleanup_fence"`
	RootInstanceID    string `json:"root_instance_id"`
	IdempotencyKey    string `json:"idempotency_key"`
}

type ForceForgetRequest struct {
	Force bool `json:"force"`
}

// ServiceDesiredStateRequest is the single mutation behind operator start and
// stop adapters. DesiredState is intent; callers must continue polling the
// observed State/Status projection for completion.
type ServiceDesiredStateRequest struct {
	DesiredState contract.ServiceDesiredState `json:"desired_state"`
}

// ServiceRestartRequest gives an explicit restart its own durable replay key.
type ServiceRestartRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
}

// JobList is one stable creation/job-ID ordered page of service jobs.
type JobList struct {
	Jobs       []Job  `json:"jobs"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// AttemptLease is the authority returned by a successful claim or renewal.
type AttemptLease struct {
	AttemptID    string           `json:"attempt_id"`
	FencingToken string           `json:"fencing_token"`
	LeaseExpires time.Time        `json:"lease_expires_at"`
	LeaseTTL     time.Duration    `json:"lease_ttl"`
	Directive    AttemptDirective `json:"directive,omitempty"`
}

// AttemptDirective carries service intent to the currently fenced payload.
// The empty value means no lifecycle change is requested.
type AttemptDirective string

const (
	AttemptDirectiveStop    AttemptDirective = "stop"
	AttemptDirectiveRestart AttemptDirective = "restart"
)

// Claim is returned when an eligible queued job is won.
type Claim struct {
	Job              Job                   `json:"job"`
	Lease            AttemptLease          `json:"lease"`
	PrestartDeadline *time.Time            `json:"prestart_deadline,omitempty"`
	ComputerStorage  *ComputerStorageClaim `json:"computer_storage,omitempty"`
}

// ComputerStorageClaim is the exact durable Storage identity a claimed
// Computer projection may attach. It is absent for every ordinary Job.
type ComputerStorageClaim struct {
	ComputerID           string `json:"computer_id"`
	StorageID            string `json:"storage_id"`
	StorageGeneration    int64  `json:"storage_generation"`
	IntentRevision       int64  `json:"intent_revision"`
	SubmitEnabled        bool   `json:"submit_enabled"`
	SubmitIntentRevision int64  `json:"submit_intent_revision"`
	SubmitMaxInflight    int    `json:"submit_max_inflight"`
	SubmitPolicyRevision int64  `json:"submit_policy_revision"`
	DiskBytes            int64  `json:"disk_bytes"`
	// Chown authorizes one explicit, crash-resumable ownership migration for
	// the current reimage only. It is never inferred from an image mismatch.
	Chown bool `json:"chown,omitempty"`
}

// ComputerTokenScopeProof is returned only after L1 has verified the exact
// current Computer projection, attempt, Node, Storage generation, submission
// intent, and installed grant-policy revision.
type ComputerTokenScopeProof struct {
	ComputerID                string `json:"computer_id"`
	ComputerAttemptID         string `json:"computer_attempt_id"`
	ComputerStorageGeneration int64  `json:"computer_storage_generation"`
	SubmitIntentRevision      int64  `json:"submit_intent_revision"`
	HostNodeID                string `json:"host_node_id"`
	SubmitMaxInflight         int    `json:"submit_max_inflight"`
}

// Node is the node projection shared by the operator list and agent protocol.
type Node struct {
	contract.NodeRegistration
	State               contract.NodeState `json:"state"`
	AuthoritativeTags   []string           `json:"authoritative_tags"`
	MaxOneshotSlots     int                `json:"max_oneshot_slots"`
	MaxServiceSlots     int                `json:"max_service_slots"`
	OneshotOccupancy    int                `json:"oneshot_occupancy"`
	ServiceOccupancy    int                `json:"service_occupancy"`
	Overcommitted       bool               `json:"overcommitted"`
	AuthorityGeneration int64              `json:"authority_generation"`
	ClaimsEnabled       bool               `json:"claims_enabled"`
	IntentRevision      int64              `json:"intent_revision"`
	IntentReason        string             `json:"intent_reason"`
	IntentUpdatedAt     *time.Time         `json:"intent_updated_at"`
	IntentActor         string             `json:"intent_actor"`
	LastHeartbeatAt     time.Time          `json:"last_heartbeat_at"`
}

// HeartbeatResponse is the boot-session-scoped node channel. Directives stay
// off the operator-visible Node projection because they carry cleanup fences.
type HeartbeatResponse struct {
	Node
	RemovalDirectives       []RemovalDirective                  `json:"removal_directives"`
	StorageResetDirectives  []ComputerStorageResetDirective     `json:"storage_reset_directives"`
	StorageGrowDirectives   []ComputerStorageGrowDirective      `json:"storage_grow_directives"`
	ReimageDirectives       []ComputerReimagePreflightDirective `json:"reimage_preflight_directives"`
	BackupDirectives        []ComputerBackupDirective           `json:"backup_directives"`
	BackupPruneDirectives   []ComputerBackupPruneDirective      `json:"backup_prune_directives"`
	StorageCopyDirectives   []ComputerStorageCopyDirective      `json:"storage_copy_directives"`
	CustodyExportDirectives []ComputerCustodyExportDirective    `json:"custody_export_directives"`
	ComputerPolicy          *ComputerPolicySnapshot             `json:"computer_policy,omitempty"`
}

type ServiceBindingProofRequest struct {
	NodeID        string `json:"node_id"`
	BootSessionID string `json:"boot_session_id"`
}

type ServiceBindingProofResponse struct {
	Bound bool `json:"bound"`
}

type ServiceImageReconciliationFailureRequest struct {
	NodeID        string                `json:"node_id"`
	BootSessionID string                `json:"boot_session_id"`
	Failure       contract.SpawnFailure `json:"failure"`
}

// NodeList is the L1 client representation of the operator-visible fleet.
type NodeList struct {
	Nodes []Node `json:"nodes"`
}

// ProcessResult matches the M0 completion contract. Pointer fields preserve
// the distinction between an omitted exit code and exit code zero. OutputError
// makes incomplete durable output a terminal failure rather than false success;
// signal outcomes carry a structured termination initiator.
type ProcessResult struct {
	SpawnError            *contract.SpawnFailure    `json:"spawn_error,omitempty"`
	RuntimeFailure        *contract.RuntimeFailure  `json:"runtime_failure,omitempty"`
	OutputError           string                    `json:"output_error,omitempty"`
	ExitCode              *int                      `json:"exit_code,omitempty"`
	Signal                string                    `json:"signal,omitempty"`
	TerminationCause      contract.TerminationCause `json:"termination_cause,omitempty"`
	OOM                   bool                      `json:"oom,omitempty"`
	DiskExhausted         bool                      `json:"disk_exhausted,omitempty"`
	LogEvidenceIncomplete bool                      `json:"log_evidence_incomplete,omitempty"`
}

// OCIImageEvidence is the fenced image and runtime identity observed before
// an OCI attempt starts. ResolvedAt and StartedAt use the L1 clock.
type OCIImageEvidence struct {
	SubmittedReference     string      `json:"submitted_reference"`
	TopLevelDigest         string      `json:"top_level_digest"`
	TopLevelMediaType      string      `json:"top_level_media_type"`
	IndexDigest            *string     `json:"index_digest"`
	PlatformManifestDigest string      `json:"platform_manifest_digest"`
	Platform               OCIPlatform `json:"platform"`
	RuntimeHandler         string      `json:"runtime_handler"`
	Snapshotter            string      `json:"snapshotter"`
	ResolvedAt             time.Time   `json:"resolved_at"`
	StartedAt              *time.Time  `json:"started_at,omitempty"`
}

type OCIPlatform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	Variant      string `json:"variant,omitempty"`
}

type ImageObservationRequest struct {
	FencingToken           string      `json:"fencing_token"`
	SubmittedReference     string      `json:"submitted_reference"`
	TopLevelDigest         string      `json:"top_level_digest"`
	TopLevelMediaType      string      `json:"top_level_media_type"`
	IndexDigest            *string     `json:"index_digest,omitempty"`
	PlatformManifestDigest string      `json:"platform_manifest_digest"`
	Platform               OCIPlatform `json:"platform"`
	RuntimeHandler         string      `json:"runtime_handler"`
	Snapshotter            string      `json:"snapshotter"`
}

type StartedRequest struct {
	FencingToken string `json:"fencing_token"`
}

type CompletionRequest struct {
	FencingToken              string                    `json:"fencing_token"`
	IdempotencyKey            string                    `json:"idempotency_key"`
	Result                    ProcessResult             `json:"result"`
	RuntimeQuiescenceEvidence RuntimeQuiescenceEvidence `json:"runtime_quiescence_evidence,omitempty"`
	ProtocolOutputHash        string                    `json:"protocol_output_digest,omitempty"`
}

// RuntimeQuiescenceEvidence names the positive runtime authority that proved
// attempt resources absent before completion. An omitted value is not proof.
type RuntimeQuiescenceEvidence string

const (
	RuntimeQuiescenceAttempt           RuntimeQuiescenceEvidence = "attempt"
	RuntimeQuiescenceNoRuntime         RuntimeQuiescenceEvidence = "no_runtime_resources"
	RuntimeQuiescenceOCISweep          RuntimeQuiescenceEvidence = "oci_sweep"
	RuntimeQuiescencePriorBootGuardian RuntimeQuiescenceEvidence = "prior_boot_guardian"
	RuntimeQuiescencePriorBootOCISweep RuntimeQuiescenceEvidence = "prior_boot_oci_sweep"
)

// LateResultEvidence is the non-authoritative completion fact retained after
// an attempt is lost. Kind explicitly distinguishes a retained ProcessResult
// from an aggregate marker saying a report arrived outside the observation
// window; consumers never infer the arm from null fields.
type LateResultEvidence struct {
	Kind            LateResultEvidenceKind `json:"kind"`
	Result          *ProcessResult         `json:"result,omitempty"`
	Gap             *LateResultGap         `json:"gap,omitempty"`
	Late            bool                   `json:"late"`
	ObservedAt      time.Time              `json:"observed_at"`
	AuthorityLostAt time.Time              `json:"authority_lost_at"`
}

type LateResultEvidenceKind string

const (
	LateResultObservation LateResultEvidenceKind = "observation"
	LateResultGapKind     LateResultEvidenceKind = "gap"
)

type LateResultGap struct {
	Reason LateResultGapReason `json:"reason"`
}

type LateResultGapReason string

const LateResultGapObservationWindowExpired LateResultGapReason = "observation_window_expired"

type ClaimRequest struct {
	NodeID         string   `json:"node_id"`
	BootSessionID  string   `json:"boot_session_id"`
	Class          string   `json:"class"`
	ExcludedJobIDs []string `json:"excluded_job_ids,omitempty"`
}

type RenewalRequest struct {
	FencingToken string `json:"fencing_token"`
}

// PublicationRequest carries an absolute publication state for one service
// attempt. Computer display endpoints are private Fabric front doors and are
// present only while ready is true.
type PublicationRequest struct {
	FencingToken    string  `json:"fencing_token"`
	Ready           *bool   `json:"ready"`
	DisplayEndpoint *string `json:"display_endpoint,omitempty"`
}

// ComputerTakeoverAuditEventKind is the closed, immutable take-over evidence
// vocabulary. Ticket #178 emits admission/session events; the control events
// are reserved for the session-bound tenure implementation in #179.
type ComputerTakeoverAuditEventKind string

const (
	ComputerTakeoverAdmissionDenied ComputerTakeoverAuditEventKind = "admission_denied"
	ComputerTakeoverSessionOpen     ComputerTakeoverAuditEventKind = "session_open"
	ComputerTakeoverSessionClose    ComputerTakeoverAuditEventKind = "session_close"
	ComputerTakeoverControlAcquired ComputerTakeoverAuditEventKind = "control_acquired"
	ComputerTakeoverControlReleased ComputerTakeoverAuditEventKind = "control_released"
	ComputerTakeoverAdminOverrode   ComputerTakeoverAuditEventKind = "admin_overrode"
)

// ComputerAdmittedMode is the closed relay mode recorded by take-over audit.
// Admission can produce only view; #179 owns the transition to controller.
type ComputerAdmittedMode string

const (
	ComputerAdmittedView       ComputerAdmittedMode = "view"
	ComputerAdmittedController ComputerAdmittedMode = "controller"
)

// ComputerTakeoverReason is the closed reason vocabulary carried on the wire
// and stored in L1. It is evidence, never an arbitrary error message.
type ComputerTakeoverReason string

const (
	ComputerTakeoverIdentityUnavailable    ComputerTakeoverReason = "identity_unavailable"
	ComputerTakeoverInvalidRequestPath     ComputerTakeoverReason = "invalid_request_path"
	ComputerTakeoverInvalidSubprotocol     ComputerTakeoverReason = "invalid_subprotocol"
	ComputerTakeoverUnauthorizedIdentity   ComputerTakeoverReason = "unauthorized_identity"
	ComputerTakeoverAttemptAuthorityLost   ComputerTakeoverReason = "attempt_authority_lost"
	ComputerTakeoverRevoked                ComputerTakeoverReason = "revoked"
	ComputerTakeoverViewBackendUnavailable ComputerTakeoverReason = "view_backend_unavailable"
	ComputerTakeoverClientUpgradeFailed    ComputerTakeoverReason = "client_upgrade_failed"
	ComputerTakeoverClientClosed           ComputerTakeoverReason = "client_closed"
	ComputerTakeoverViewBackendClosed      ComputerTakeoverReason = "view_backend_closed"
	ComputerTakeoverRevalidationFailed     ComputerTakeoverReason = "revalidation_failed"
	ComputerTakeoverSessionCapExpired      ComputerTakeoverReason = "session_cap_expired"
	ComputerTakeoverExplicitRelease        ComputerTakeoverReason = "explicit_release"
	ComputerTakeoverControllerOverridden   ComputerTakeoverReason = "controller_overridden"
	ComputerTakeoverControlBackendClosed   ComputerTakeoverReason = "control_backend_closed"
	ComputerTakeoverControlBackendFailed   ComputerTakeoverReason = "control_backend_unavailable"
)

// ComputerTakeoverAuditEvent contains identity and policy evidence only. It
// deliberately has no fencing token, display bytes, input data, or endpoint.
type ComputerTakeoverAuditEvent struct {
	EventID        string                         `json:"event_id"`
	Kind           ComputerTakeoverAuditEventKind `json:"kind"`
	ComputerID     string                         `json:"computer_id"`
	JobID          string                         `json:"job_id"`
	AttemptID      string                         `json:"attempt_id"`
	SessionID      string                         `json:"session_id,omitempty"`
	FabricID       string                         `json:"fabric_id,omitempty"`
	UserID         string                         `json:"user_id,omitempty"`
	DeviceID       string                         `json:"device_id,omitempty"`
	AuthorizedRole ComputerGrantPermission        `json:"authorized_role,omitempty"`
	AdmittedMode   ComputerAdmittedMode           `json:"admitted_mode,omitempty"`
	PolicyRevision int64                          `json:"policy_revision,omitempty"`
	OccurredAt     time.Time                      `json:"occurred_at"`
	Reason         ComputerTakeoverReason         `json:"reason,omitempty"`
	EventCount     int64                          `json:"event_count,omitempty"`
	// AuthorityGeneration is derived by L1 from the fenced attempt and never
	// accepted from the uploader.
	AuthorityGeneration int64 `json:"authority_generation,omitempty"`
}

type ComputerTakeoverAuditRequest struct {
	// FencingToken authenticates the upload but is never copied into the
	// durable event or any operator response.
	FencingToken string                     `json:"fencing_token"`
	Event        ComputerTakeoverAuditEvent `json:"event"`
}

type ComputerTakeoverAuditReceipt struct {
	Event    ComputerTakeoverAuditEvent `json:"event"`
	Replayed bool                       `json:"replayed"`
}

type HeartbeatRequest struct {
	BootSessionID        string                        `json:"boot_session_id"`
	Capabilities         map[string]bool               `json:"capabilities"`
	CapabilityRevision   int64                         `json:"capability_revision"`
	CapabilityObservedAt time.Time                     `json:"capability_observed_at"`
	MissingCapabilities  []string                      `json:"missing_capabilities"`
	CapabilityReasonCode contract.CapabilityReasonCode `json:"capability_reason_code,omitempty"`
}

type DrainRequest struct {
	BootSessionID string `json:"boot_session_id"`
}

// NodeIntentRequest is an operator CAS over the durable claim-admission bit.
// IntentRevision is the revision observed in the node projection.
type NodeIntentRequest struct {
	ClaimsEnabled  bool   `json:"claims_enabled"`
	IntentRevision int64  `json:"intent_revision"`
	Reason         string `json:"reason"`
}

// ReconcileResult reports the durable transitions won by one reconciliation
// pass. Counts are useful for observability and deterministic tests.
type ReconcileResult struct {
	ExpiredAttempts                   int64 `json:"expired_attempts"`
	StaleNodes                        int64 `json:"stale_nodes"`
	DeadNodes                         int64 `json:"dead_nodes"`
	RestartStreakResets               int64 `json:"restart_streak_resets"`
	EvictedLogEvents                  int64 `json:"evicted_log_events"`
	EvictedLogBytes                   int64 `json:"evicted_log_bytes"`
	PrunedAttempts                    int64 `json:"pruned_attempts"`
	PrunedComputerTakeoverAuditEvents int64 `json:"pruned_computer_takeover_audit_events"`
	FinalizedRemovals                 int64 `json:"finalized_removals"`
}

// AppendLogsRequest is one provenance-authenticated, idempotent upload batch.
// Event identity is the tuple (attempt_id, stream, sequence); the reader cursor
// is intentionally absent because upload acknowledgements and polling position
// are separate protocol concepts.
type AppendLogsRequest struct {
	FencingToken string              `json:"fencing_token"`
	Events       []contract.LogEvent `json:"events"`
}

type AppendLogsResponse struct {
	Acknowledged map[contract.LogStream]uint64 `json:"acknowledged"`
}

type LogPage struct {
	Events     []contract.LogEvent   `json:"events"`
	NextCursor string                `json:"next_cursor,omitempty"`
	Truncation *ServiceLogTruncation `json:"truncation,omitempty"`
}

// ServiceLogTruncation is L1's aggregate declaration that retained service
// history was evicted. It is intentionally distinct from contract.LogGap,
// which declares evidence lost before L1 accepted it.
type ServiceLogTruncation struct {
	BoundKind             ServiceLogRetentionBound `json:"bound_kind"`
	EvictedEventCount     int64                    `json:"evicted_event_count"`
	EvictedByteCount      int64                    `json:"evicted_byte_count"`
	EvictedThroughOrdinal int64                    `json:"evicted_through_ordinal"`
	EarliestRetainedAt    *time.Time               `json:"earliest_retained_at"`
	UpdatedAt             time.Time                `json:"updated_at"`
}

type ServiceLogRetentionBound string

const (
	ServiceLogRetentionBytes ServiceLogRetentionBound = "bytes"
	ServiceLogRetentionAge   ServiceLogRetentionBound = "age"
)
