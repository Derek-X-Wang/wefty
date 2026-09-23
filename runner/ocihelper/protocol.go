// Package ocihelper defines the narrow, versioned protocol between an
// unprivileged node agent and the privileged OCI helper. It deliberately does
// not expose containerd types or a general network proxy.
package ocihelper

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	// ProtocolVersion is the only wire major accepted by this implementation.
	// The helper executable checksum pins the exact minor shape within this
	// major, so an in-place wire change still fails closed at session setup.
	ProtocolVersion = 2
	// ComputerProtocolVersion is the first protocol carrying the exact Computer
	// endpoint, control-state, and attachment semantics required for admission.
	ComputerProtocolVersion = 2
	InvocationArg           = "__wefty_oci_helper"
	// MaxFrameBytes bounds every decoded request, response, and stream event.
	MaxFrameBytes = 1 << 20
)

type Method string

const (
	MethodAcquireSession     Method = "AcquireSession"
	MethodHeartbeat          Method = "Heartbeat"
	MethodEnsureImage        Method = "EnsureImage"
	MethodReconcileImagePins Method = "ReconcileImagePins"
	MethodReleaseImagePin    Method = "ReleaseImagePin"
	MethodReleaseAttemptPin  Method = "ReleaseAttemptImagePin"
	MethodImageCacheStatus   Method = "ImageCacheStatus"
	MethodDoctorStatus       Method = "DoctorStatus"
	MethodRun                Method = "Run"
	MethodSignal             Method = "Signal"
	MethodWatch              Method = "Watch"
	MethodDelete             Method = "Delete"
	MethodDeleteVolume       Method = "DeleteManagedVolume"
	MethodInventoryRemoval   Method = "InventoryRemoval"
	MethodAttestRemoval      Method = "AttestRemoval"
	MethodResetStorage       Method = "ResetComputerStorage"
	MethodGrowStorage        Method = "GrowComputerStorage"
	MethodPreflightReimage   Method = "PreflightComputerReimage"
	MethodCreateBackup       Method = "CreateComputerBackup"
	MethodDeleteBackup       Method = "DeleteComputerBackupCopy"
	MethodCopyStorage        Method = "CopyComputerStorage"
	MethodExportCustody      Method = "ExportComputerCustody"
	MethodVerify             Method = "Verify"
	MethodSweep              Method = "Sweep"
	MethodDialAttemptPort    Method = "DialAttemptPort"
	MethodDialHostBridge     Method = "DialHostBridge"
	MethodSetComputerControl Method = "SetComputerControlState"
	MethodSetComputerToken   Method = "SetComputerToken"
	MethodListRunMailbox     Method = "ListRunMailbox"
	MethodReadRunMailbox     Method = "ReadRunMailbox"
	MethodRemoveRunMailbox   Method = "RemoveRunMailboxEntry"
	// MethodInventoryHandoffs is session-authorized rather than
	// attempt-authorized on purpose: it spans every handoff volume this node
	// still holds, and the attempts that produced them are long gone. There is
	// no attempt whose authority could stand for the node's retained results.
	MethodInventoryHandoffs Method = "InventoryHandoffVolumes"
)

// attemptPortBackendReady is emitted only after the helper has connected the
// authorized stream to the payload's exact attempt-local loopback port.
const attemptPortBackendReady byte = 1

// hostBridgeBackendReady is emitted only after the helper has accepted
// the guest side of the authorized host bridge. The client must consume this
// marker before dialing its host-side loopback bridge.
const hostBridgeBackendReady byte = 1

type ErrorCode string

const (
	CodeInvalidRequest                ErrorCode = "invalid_request"
	CodePeerUnauthenticated           ErrorCode = "peer_unauthenticated"
	CodeVersionMismatch               ErrorCode = "version_mismatch"
	CodeChecksumMismatch              ErrorCode = "checksum_mismatch"
	CodeSessionBusy                   ErrorCode = "session_busy"
	CodeSessionStale                  ErrorCode = "session_stale"
	CodeComputerStorageBusy           ErrorCode = "computer_storage_busy"
	CodeComputerStorageRetired        ErrorCode = "computer_storage_retired"
	CodeComputerStorageResumeDeferred ErrorCode = "computer_storage_resume_deferred"
	CodeComputerStorageQuarantined    ErrorCode = "computer_storage_quarantined"
	CodeComputerStorageGrowUncertain  ErrorCode = "computer_storage_grow_uncertain"
	CodeUnauthorizedAttempt           ErrorCode = "unauthorized_attempt"
	CodeAttemptOutsideSession         ErrorCode = "attempt_outside_session"
	CodeUnauthorizedPort              ErrorCode = "unauthorized_port"
	CodeUnauthorizedBridge            ErrorCode = "unauthorized_bridge"
	CodeOCISpecRejected               ErrorCode = "oci_spec_rejected"
	CodeImageUnavailable              ErrorCode = "image_unavailable"
	CodeInsufficientMemory            ErrorCode = "insufficient_memory"
	CodeInsufficientDisk              ErrorCode = "insufficient_disk"
	CodeEngineFailure                 ErrorCode = "engine_failure"
	// CodeDiagnosticFailure is a read-only observation failure. It is never
	// evidence that the helper session or runtime authority was lost.
	CodeDiagnosticFailure    ErrorCode = "diagnostic_failure"
	CodeUnsupportedOperation ErrorCode = "unsupported_operation"
	CodeSweepRequired        ErrorCode = "sweep_required"
	// CodeHandoffVolumeLive refuses a handoff-volume deletion because an
	// attempt of this node still owns the volume.
	//
	// It is a replayable refusal and not an engine failure: nothing was
	// detached, nothing was freed, and the identical request succeeds once the
	// owner finishes. The agent's budget eviction is the only caller that can
	// meet it -- it selects from an inventory snapshot, and an attempt may
	// claim a volume between that read and this call -- and the honest answer
	// to "give up a volume a run is writing into" is to refuse, not to take
	// the files out from under a running container.
	CodeHandoffVolumeLive ErrorCode = "handoff_volume_live"
	// CodeStartupBoundTripped refuses session admission because the startup
	// barrier already burned its consecutive-failure bound. This generation
	// never ran a startup sweep, so the refusal is cheap and repeatable, and
	// the caller learns the bound from the handshake instead of discovering a
	// closed connection.
	CodeStartupBoundTripped ErrorCode = "startup_bound_tripped"
)

// ErrorDetail is a closed, stable token that narrows one ErrorCode without
// splitting it into two. A code says what the caller may do; a detail says
// which of that code's causes produced this one, for the consumers that must
// tell them apart.
type ErrorDetail string

// DetailAdmissionContention marks the one `computer_storage_busy` refusal that
// is about the Node rather than about the resource: the helper gave up waiting
// for a Node-wide Computer disk admission mutex. Every other busy refusal --
// a live attempt owning the Storage generation, or fencing removal inventory
// -- is a fact about the requested resource and carries no detail, so a
// consumer can tell unrelated Node traffic from its own live attempt without a
// second code.
const DetailAdmissionContention ErrorDetail = "admission_contention"

// RPCError is safe to cross the private protocol. Engine failures may include
// bounded one-line mechanics detail in Message so native diagnostics retain the
// causal engine error; the closed EngineFailure fact remains policy authority.
type RPCError struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	// Detail narrows Code for a consumer that must distinguish its causes. It
	// is optional, closed, and never carries identifiers.
	Detail        ErrorDetail        `json:"detail,omitempty"`
	ImageFailure  *ImageFailureFact  `json:"image_failure,omitempty"`
	EngineFailure *EngineFailureFact `json:"engine_failure,omitempty"`
	MemoryFailure *MemoryFailureFact `json:"memory_failure,omitempty"`
	DiskFailure   *DiskFailureFact   `json:"disk_failure,omitempty"`
}

// EngineFailureFact is bounded mechanics evidence for a failed helper engine
// operation. It deliberately carries no containerd type, host path, or raw
// privileged error text.
type EngineFailureFact struct {
	Operation Method              `json:"operation"`
	Reason    EngineFailureReason `json:"reason"`
	// AttemptScoped is the helper's positive claim that this failure is bounded
	// by the one attempt it names: the helper reaped that attempt's runtime
	// resources successfully and its exclusive session remained live, so the
	// session capability the caller holds is still authoritative. Absent the
	// claim the failure remains runtime-loss evidence.
	AttemptScoped bool `json:"attempt_scoped,omitempty"`
}

// EngineFailureReason is the closed, sanitized mechanics vocabulary allowed
// to cross the helper boundary for an engine failure.
type EngineFailureReason string

const (
	EngineFailureDeadlineExceeded EngineFailureReason = "deadline_exceeded"
	EngineFailureCanceled         EngineFailureReason = "canceled"
	EngineFailurePermissionDenied EngineFailureReason = "permission_denied"
	EngineFailureRetentionBound   EngineFailureReason = "retention_bound_exceeded"
	EngineFailureEgressDNS        EngineFailureReason = "egress_dns_unavailable"
	EngineFailureEgressBoundary   EngineFailureReason = "egress_boundary_unproven"
	EngineFailureLoopDiscard      EngineFailureReason = "loop_discard_not_disabled"
	EngineFailureOperationFailed  EngineFailureReason = "operation_failed"
)

func (reason EngineFailureReason) valid() bool {
	switch reason {
	case EngineFailureDeadlineExceeded, EngineFailureCanceled, EngineFailurePermissionDenied, EngineFailureRetentionBound, EngineFailureEgressDNS, EngineFailureEgressBoundary, EngineFailureLoopDiscard, EngineFailureOperationFailed:
		return true
	default:
		return false
	}
}

func (reason *EngineFailureReason) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	decoded := EngineFailureReason(value)
	if !decoded.valid() {
		return fmt.Errorf("unknown engine failure reason %q", value)
	}
	*reason = decoded
	return nil
}

type MemoryFailureFact struct {
	RequestedBytes         int64 `json:"requested_bytes"`
	ObservedAvailableBytes int64 `json:"observed_available_bytes"`
}

type DiskFailureFact struct {
	RequestedBytes         int64 `json:"requested_bytes"`
	ObservedAvailableBytes int64 `json:"observed_available_bytes"`
}

type insufficientDiskError struct {
	RequestedBytes         int64
	ObservedAvailableBytes int64
	err                    error
}

type insufficientMemoryError struct {
	RequestedBytes         int64
	ObservedAvailableBytes int64
}

func (failure *insufficientMemoryError) Error() string {
	return "insufficient memory capacity for workload cap"
}

func (failure *insufficientDiskError) Error() string {
	return "insufficient disk for full Computer allocation"
}
func (failure *insufficientDiskError) Unwrap() error { return failure.err }

// ImageFailureFact is sanitized mechanics evidence. The helper reports only
// what it observed; retry and terminal classification remain agent policy.
type ImageFailureFact struct {
	Kind           ImageFailureKind `json:"kind"`
	Reason         string           `json:"reason,omitempty"`
	HTTPStatus     int              `json:"http_status,omitempty"`
	RetryAfter     time.Duration    `json:"retry_after,omitempty"`
	TopLevelDigest string           `json:"top_level_digest,omitempty"`
}

type ImageFailureKind string

const (
	ImageFailureHTTP              ImageFailureKind = "http_status"
	ImageFailureNetwork           ImageFailureKind = "network"
	ImageFailurePlatformMismatch  ImageFailureKind = "platform_mismatch"
	ImageFailureEngineLoss        ImageFailureKind = "engine_loss"
	ImageFailureResourceExhausted ImageFailureKind = "resource_exhausted"
	ImageFailureManifestRejected  ImageFailureKind = "manifest_rejected"
	ImageFailureUnavailable       ImageFailureKind = "unavailable"
)

func (err *RPCError) Error() string {
	if err == nil {
		return ""
	}
	if err.EngineFailure != nil {
		return fmt.Sprintf("oci helper %s: %s (operation=%s reason=%s)", err.Code, err.Message, err.EngineFailure.Operation, err.EngineFailure.Reason)
	}
	return fmt.Sprintf("oci helper %s: %s", err.Code, err.Message)
}

// RuntimeLossError is positive evidence that an operation lost the active
// helper session or its engine. Callers must not infer node-wide runtime loss
// from an arbitrary policy, validation, or cancellation error.
type RuntimeLossError struct{ Cause error }

func (err *RuntimeLossError) Error() string {
	if err == nil || err.Cause == nil {
		return "OCI helper runtime lost"
	}
	return "OCI helper runtime lost: " + err.Cause.Error()
}

func (err *RuntimeLossError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

type frame struct {
	Version           int             `json:"version"`
	Method            Method          `json:"method,omitempty"`
	SessionCapability string          `json:"session_capability,omitempty"`
	Body              json.RawMessage `json:"body,omitempty"`
	OK                bool            `json:"ok,omitempty"`
	Error             *RPCError       `json:"error,omitempty"`
}

type AcquireSessionRequest struct {
	NodeID                 string `json:"node_id"`
	BootSessionID          string `json:"boot_session_id"`
	ExpectedHelperChecksum string `json:"expected_helper_checksum,omitempty"`
}

type AcquireSessionResponse struct {
	ProtocolVersion       int           `json:"protocol_version"`
	HelperVersion         string        `json:"helper_version"`
	HelperChecksum        string        `json:"helper_checksum"`
	SessionCapability     string        `json:"session_capability"`
	HelperInstanceID      string        `json:"helper_instance_id"`
	SessionGeneration     uint64        `json:"session_generation"`
	HeartbeatTimeout      time.Duration `json:"heartbeat_timeout"`
	MaximumAttemptDeadman time.Duration `json:"maximum_attempt_deadman"`
	ReapTimeout           time.Duration `json:"reap_timeout"`
	StartupInProgress     bool          `json:"startup_in_progress"`
	// StartupBound reports this generation's startup-barrier bound. It is
	// tripped only on a generation that refused to run the startup sweep at
	// all, so the peer can name the bound in its own diagnostics instead of
	// inferring a wedge from a dropped connection.
	StartupBound StartupBoundFacts `json:"startup_bound"`
}

// StartupBoundFacts is the closed, non-privileged description of the helper's
// consecutive-startup-barrier-failure bound. It carries no error text.
type StartupBoundFacts struct {
	Tripped     bool                `json:"tripped"`
	Phase       StartupBarrierPhase `json:"phase,omitempty"`
	Consecutive int                 `json:"consecutive,omitempty"`
	Bound       int                 `json:"bound,omitempty"`
	Elapsed     time.Duration       `json:"elapsed,omitempty"`
	// NextAttemptAt is when the refusing generation re-attempts the barrier.
	// A tripped helper retries on its own at most once per startup-failure
	// window, so a caller can tell a helper that is waiting to recover from
	// one that needs a human.
	NextAttemptAt time.Time `json:"next_attempt_at,omitzero"`
}

// HelperSession identifies one opaque helper process/session generation
// without exposing its bearer capability.
type HelperSession struct {
	HelperInstanceID  string `json:"helper_instance_id"`
	SessionGeneration uint64 `json:"session_generation"`
}

type SessionIdentity struct {
	NodeID        string `json:"node_id"`
	BootSessionID string `json:"boot_session_id"`
}

// AttemptAuthority is the complete helper-side authorization tuple. Class is
// an immutable resource label and may only narrow admission for mechanics that
// are valid for that class.
type AttemptAuthority struct {
	NodeID            string `json:"node_id"`
	JobID             string `json:"job_id"`
	AttemptID         string `json:"attempt_id"`
	FencingToken      string `json:"fencing_token"`
	BootSessionID     string `json:"boot_session_id"`
	Class             string `json:"class"`
	RemovalGeneration string `json:"removal_generation"`
}

func (authority AttemptAuthority) validate() error {
	values := []struct {
		name  string
		value string
	}{
		{"node_id", authority.NodeID}, {"job_id", authority.JobID},
		{"attempt_id", authority.AttemptID}, {"fencing_token", authority.FencingToken},
		{"boot_session_id", authority.BootSessionID}, {"class", authority.Class},
		{"removal_generation", authority.RemovalGeneration},
	}
	for _, value := range values {
		if strings.TrimSpace(value.value) == "" {
			return fmt.Errorf("attempt authority requires %s", value.name)
		}
	}
	return nil
}

func (authority AttemptAuthority) key() string {
	return strings.Join([]string{
		authority.NodeID, authority.JobID, authority.AttemptID,
		authority.FencingToken, authority.BootSessionID,
		authority.Class, authority.RemovalGeneration,
	}, "\x00")
}

// ResourceIdentity deterministically names and labels runtime resources. The
// attempt tuple names ephemeral resources while the stable job ID names the
// service data volume that survives attempts. Digests keep operator-provided
// identifiers out of runtime names while labels retain the complete authority
// tuple for verification.
type ResourceIdentity struct {
	LeaseID                  string            `json:"lease_id"`
	SnapshotID               string            `json:"snapshot_id"`
	ContainerID              string            `json:"container_id"`
	TaskID                   string            `json:"task_id"`
	ShimID                   string            `json:"shim_id"`
	CgroupID                 string            `json:"cgroup_id"`
	LogSegmentDirectory      string            `json:"log_segment_directory"`
	HandoffVolumeDirectory   string            `json:"handoff_volume_directory"`
	ServiceVolumeDirectory   string            `json:"service_volume_directory"`
	ServiceVolumeOwnerRecord string            `json:"service_volume_owner_record"`
	Labels                   map[string]string `json:"labels"`
}

func DeterministicResourceIdentity(authority AttemptAuthority) (ResourceIdentity, error) {
	if err := authority.validate(); err != nil {
		return ResourceIdentity{}, err
	}
	digest := sha256.Sum256([]byte(authority.key()))
	suffix := hex.EncodeToString(digest[:16])
	var serviceVolumeDirectory, serviceVolumeOwnerRecord string
	if authority.Class == "service" {
		var err error
		serviceVolumeDirectory, err = DeterministicServiceVolumeDirectory(authority.JobID)
		if err != nil {
			return ResourceIdentity{}, err
		}
		serviceVolumeOwnerRecord = serviceVolumeDirectory + ".owner"
	}
	containerID := "wefty-container-" + suffix
	return ResourceIdentity{
		LeaseID: "wefty-lease-" + suffix, SnapshotID: "wefty-snapshot-" + suffix,
		ContainerID: containerID, TaskID: containerID,
		ShimID: containerID, CgroupID: "wefty-cgroup-" + suffix,
		LogSegmentDirectory:      "wefty-log-segments-" + suffix,
		HandoffVolumeDirectory:   handoffVolumeNamePrefix + suffix,
		ServiceVolumeDirectory:   serviceVolumeDirectory,
		ServiceVolumeOwnerRecord: serviceVolumeOwnerRecord,
		Labels: map[string]string{
			"io.wefty/node_id": authority.NodeID, "io.wefty/job_id": authority.JobID,
			"io.wefty/attempt_id": authority.AttemptID, "io.wefty/fencing_token": authority.FencingToken,
			"io.wefty/boot_session_id": authority.BootSessionID, "io.wefty/class": authority.Class,
			"io.wefty/removal_generation": authority.RemovalGeneration,
		},
	}, nil
}

// DeterministicServiceVolumeDirectory maps one stable service job identity to
// helper-owned guest-native storage shared by that job's attempts only.
func DeterministicServiceVolumeDirectory(jobID string) (string, error) {
	if jobID == "" || strings.TrimSpace(jobID) != jobID || len(jobID) > 255 || strings.IndexByte(jobID, 0) >= 0 {
		return "", errors.New("service data job ID must be bounded and non-empty")
	}
	digest := sha256.Sum256([]byte("service-data\x00" + jobID))
	return "wefty-service-volume-" + hex.EncodeToString(digest[:16]), nil
}

// DeterministicHandoffVolumeDirectory maps the opaque stable owner identity to
// a helper-owned name without exposing it in filesystem paths.
func DeterministicHandoffVolumeDirectory(ownerKey string) (string, error) {
	if ownerKey == "" || strings.TrimSpace(ownerKey) != ownerKey || len(ownerKey) > 255 || strings.IndexByte(ownerKey, 0) >= 0 {
		return "", errors.New("handoff owner key must be bounded and non-empty")
	}
	digest := sha256.Sum256([]byte("handoff\x00" + ownerKey))
	return handoffVolumeNamePrefix + hex.EncodeToString(digest[:16]), nil
}

// DeterministicComputerDiskName maps one non-transferable Storage generation
// to the stable helper-owned disk identity used by manifests and inventory.
func DeterministicComputerDiskName(storage ComputerStorageReference) (string, error) {
	if !boundedStorageID(storage.ComputerID) || !boundedStorageID(storage.StorageID) || storage.StorageGeneration < 1 {
		return "", errors.New("Computer disk requires a bounded durable Storage identity")
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{
		"computer-disk", storage.ComputerID, storage.StorageID, strconv.FormatInt(storage.StorageGeneration, 10),
	}, "\x00")))
	return "wefty-computer-disk-" + hex.EncodeToString(digest[:16]), nil
}

type DeadmanRenewal struct {
	Authority AttemptAuthority `json:"authority"`
	TTL       time.Duration    `json:"ttl"`
}

type HeartbeatRequest struct {
	Sequence        uint64           `json:"sequence"`
	RenewedAttempts []DeadmanRenewal `json:"renewed_attempts,omitempty"`
}

type EnsureImageRequest struct {
	Reference        string        `json:"reference"`
	Digest           string        `json:"digest"`
	Platform         OCIPlatform   `json:"platform"`
	Source           ImageSource   `json:"source,omitempty"`
	OperationTimeout time.Duration `json:"operation_timeout,omitempty"`
	Pin              *ImagePin     `json:"pin,omitempty"`
}

const DefaultImageCacheMaxBytes int64 = 16 << 30

// ImagePin is one exact cache hold. Attempt authority is boot-scoped while a
// service binding is keyed durably by JobID and reconstructed after boot.
type ImagePin struct {
	Authority AttemptAuthority `json:"authority"`
	Binding   bool             `json:"binding,omitempty"`
}

type BindingImagePin struct {
	JobID       string      `json:"job_id"`
	Reference   string      `json:"reference"`
	Digest      string      `json:"digest"`
	Platform    OCIPlatform `json:"platform"`
	Snapshotter string      `json:"snapshotter"`
}

type ReconcileImagePinsRequest struct {
	Bindings      []BindingImagePin `json:"bindings"`
	ProbeDigests  []string          `json:"probe_digests"`
	CacheMaxBytes int64             `json:"cache_max_bytes"`
}

type ReconcileImagePinsResponse struct {
	MissingDigests []string `json:"missing_digests,omitempty"`
}

type ReleaseImagePinRequest struct {
	JobID string `json:"job_id"`
}

type ReleaseAttemptImagePinRequest struct {
	Authority AttemptAuthority `json:"authority"`
}

type ImageCacheEviction struct {
	Digest    string    `json:"digest"`
	Reason    string    `json:"reason"`
	Bytes     int64     `json:"bytes"`
	EvictedAt time.Time `json:"evicted_at"`
}

type ImageCacheStatus struct {
	Bytes        int64               `json:"bytes"`
	CapBytes     int64               `json:"cap_bytes"`
	LastEviction *ImageCacheEviction `json:"last_eviction,omitempty"`
	LastError    string              `json:"last_error,omitempty"`
}

type DiagnosticReadOutcome string

const (
	DiagnosticReadOK     DiagnosticReadOutcome = "ok"
	DiagnosticReadFailed DiagnosticReadOutcome = "failed"

	DiagnosticErrorContainerdVersion = "containerd_version_unavailable"
	DiagnosticErrorRuncVersion       = "runc_version_unavailable"
	DiagnosticErrorCacheStatus       = "cache_status_unavailable"
	DiagnosticErrorCacheEviction     = "cache_eviction_failed"
	DiagnosticErrorMountRoots        = "mount_roots_unavailable"
	DiagnosticErrorComputerFirewall  = "computer_firewall_unavailable"

	DiagnosticErrorAttemptOwnershipQuarantines = "attempt_ownership_quarantines_unavailable"

	RuncVersionSourceConfiguredPath         = "configured_absolute_path"
	RuncVersionSourceContainerdInfo         = "containerd_runtime_info"
	RuncVersionSourceRuntimeHandlerPath     = "runtime_handler_binary"
	RuncVersionSourceRuntimeHandlerFeatures = "runtime_handler_features"
)

// DiagnosticReadReceipt states whether one helper-side read produced its
// fact. ErrorCode is a closed, sanitized local code; raw errors never cross
// the privileged helper boundary.
type DiagnosticReadReceipt struct {
	Outcome   DiagnosticReadOutcome `json:"outcome"`
	ErrorCode string                `json:"error_code,omitempty"`
}

// SessionInvalidationReceipt preserves the closed cause of the latest helper
// session invalidated by a rejected control heartbeat. It carries no session
// capability or raw privileged error text.
type SessionInvalidationReceipt struct {
	ObservedAt        time.Time `json:"observed_at"`
	SessionGeneration uint64    `json:"session_generation"`
	AttemptID         string    `json:"attempt_id,omitempty"`
	RejectionCode     ErrorCode `json:"rejection_code"`
}

// DoctorStatus is the helper's read-only mechanics snapshot. It contains no
// session capability, raw error, or mutation control and is safe to surface to
// the operator-only node doctor.
type DoctorStatus struct {
	RuntimePlatform         OCIPlatform                 `json:"runtime_platform"`
	ContainerdVersion       string                      `json:"containerd_version"`
	ContainerdRead          DiagnosticReadReceipt       `json:"containerd_read"`
	RuncVersion             string                      `json:"runc_version"`
	RuncVersionSource       string                      `json:"runc_version_source"`
	RuncRead                DiagnosticReadReceipt       `json:"runc_read"`
	AllowedMountRoots       []string                    `json:"allowed_mount_roots"`
	MountRootsRead          DiagnosticReadReceipt       `json:"mount_roots_read"`
	Cache                   ImageCacheStatus            `json:"cache"`
	CacheRead               DiagnosticReadReceipt       `json:"cache_read"`
	CacheLastErrorCode      string                      `json:"cache_last_error_code,omitempty"`
	ComputerFirewallPresent bool                        `json:"computer_firewall_present"`
	ComputerAttemptsLive    bool                        `json:"computer_attempts_live"`
	ComputerFirewallRead    DiagnosticReadReceipt       `json:"computer_firewall_read"`
	LastProfile             *ProfileReceipt             `json:"last_profile,omitempty"`
	LastAdmission           *ResourceAdmissionReceipt   `json:"last_admission,omitempty"`
	LastSessionInvalidation *SessionInvalidationReceipt `json:"last_session_invalidation,omitempty"`

	AttemptOwnershipQuarantines     []AttemptOwnershipQuarantine `json:"attempt_ownership_quarantines,omitempty"`
	AttemptOwnershipQuarantinesRead DiagnosticReadReceipt        `json:"attempt_ownership_quarantines_read"`
}

// AttemptOwnershipQuarantineReason is the closed set of typed causes for which
// a durable Attempt ownership record cannot be reconciled with the fenced
// authority that its own file name names.
type AttemptOwnershipQuarantineReason string

const (
	AttemptOwnershipQuarantineUnreadable        AttemptOwnershipQuarantineReason = "unreadable"
	AttemptOwnershipQuarantineInvalidRecord     AttemptOwnershipQuarantineReason = "invalid_record"
	AttemptOwnershipQuarantineAuthorityMismatch AttemptOwnershipQuarantineReason = "authority_mismatch"
)

// AttemptOwnershipQuarantineKind names this receipt shape on disk and on the
// doctor surface.
const AttemptOwnershipQuarantineKind = "attempt_ownership_unreconcilable"

// AttemptOwnershipQuarantine is the operator-visible receipt for one durable
// Attempt ownership record the boot sweep moved aside instead of wedging
// helper startup on. The record name is a deterministic digest, so the receipt
// carries no operator-provided identifier and no raw error text.
type AttemptOwnershipQuarantine struct {
	Kind          string                           `json:"kind"`
	ReceiptID     string                           `json:"receipt_id"`
	Record        string                           `json:"record"`
	Reason        AttemptOwnershipQuarantineReason `json:"reason"`
	QuarantinedAt time.Time                        `json:"quarantined_at"`
}

// ImageSource selects one closed delivery mechanism. Empty retains the wire-v1
// registry default for callers compiled before offline import landed.
type ImageSource string

const (
	ImageSourceRegistry ImageSource = "registry"
	ImageSourceArchive  ImageSource = "archive"
)

type EnsureImageResponse struct {
	TopLevelDigest string        `json:"top_level_digest"`
	PlatformDigest string        `json:"platform_digest"`
	Evidence       ImageEvidence `json:"evidence"`
}

type ImageEventKind string

const (
	ImageProgress ImageEventKind = "progress"
	ImageComplete ImageEventKind = "complete"
)

// EnsureImageEvent is a closed stream event. Exactly one of Progress or
// Result is populated according to Kind.
type EnsureImageEvent struct {
	Kind     ImageEventKind       `json:"kind"`
	Progress *ImageProgressEvent  `json:"progress,omitempty"`
	Result   *EnsureImageResponse `json:"result,omitempty"`
}

type ImageProgressEvent struct {
	Status         string `json:"status"`
	Completed      int64  `json:"completed,omitempty"`
	Total          int64  `json:"total,omitempty"`
	TopLevelDigest string `json:"top_level_digest,omitempty"`
}

type EnvironmentVariable struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type ManagedVolumeKind string

const (
	ManagedVolumeHandoff      ManagedVolumeKind = "handoff"
	ManagedVolumeServiceData  ManagedVolumeKind = "service_data"
	ManagedVolumeComputerDisk ManagedVolumeKind = "computer_disk"
	ManagedVolumeLogSegments  ManagedVolumeKind = "log_segments"
)

// ComputerStorageReference is the complete durable identity and allocation
// budget for one Computer Storage generation. It contains no host path.
type ComputerStorageReference struct {
	ComputerID        string `json:"computer_id"`
	StorageID         string `json:"storage_id"`
	StorageGeneration int64  `json:"storage_generation"`
	IntentRevision    int64  `json:"intent_revision"`
	DiskBytes         int64  `json:"disk_bytes"`
	Chown             bool   `json:"chown,omitempty"`
}

type ManagedVolumeDescriptor struct {
	Kind            ManagedVolumeKind         `json:"kind"`
	OwnerKey        string                    `json:"owner_key,omitempty"`
	ComputerStorage *ComputerStorageReference `json:"computer_storage,omitempty"`
	ReadOnly        bool                      `json:"read_only,omitempty"`
}

// Run mailbox. The helper seeds one run-scoped directory inside the handoff
// volume before the workload starts and then serves a bounded, attempt-scoped
// read of it, so an unprivileged agent can publish what the workload reported
// without the workload ever holding a credential. The layout and the file
// protocol are `docs/contracts/run-execution-context.md`; the helper enforces
// only confinement and these bounds and never parses an event.
const (
	// RunMailboxDirectoryName and the run-scoped child below it mirror the
	// agent's own layout so one contract describes both kinds.
	RunMailboxDirectoryName        = ".wefty"
	RunMailboxEventsDirectoryName  = "events"
	RunMailboxStagingDirectoryName = "tmp"
	RunMailboxParamsFileName       = "params.json"
	// MaxRunMailboxReadBytes bounds one served event. It matches the agent's
	// own per-event bound; a larger file is truncated, never refused, because
	// losing a verdict to a large payload is the worse failure.
	MaxRunMailboxReadBytes = 64 << 10
	// MaxRunMailboxListNames bounds one listing. The publisher needs a
	// complete listing to establish lexical order, so this is a single cap and
	// not a page size: at the name bound the whole response stays inside
	// MaxFrameBytes.
	MaxRunMailboxListNames = 4096
	// MaxRunMailboxHandoffFileBytes bounds one file served from the handoff
	// scope. It is larger than an event because a result document is, and
	// smaller than contract.MaxUploadedResultBytes because a helper response
	// must fit inside MaxFrameBytes once the payload is base64-encoded into
	// JSON. An OCI run whose result.json is larger than this is not truncated:
	// the agent reads it as oversize, keeps the file on the node, and uploads
	// nothing, because half a result document is worse than none.
	MaxRunMailboxHandoffFileBytes = 640 << 10
	// MaxRunMailboxNameBytes bounds one entry name, matching the agent's rule.
	MaxRunMailboxNameBytes = 128
	// MaxRunMailboxParamsBytes bounds the params document the helper seeds.
	MaxRunMailboxParamsBytes = 64 << 10
)

// RunMailboxSeed asks the helper to create one run-scoped mailbox inside this
// attempt's handoff volume and deliver its parameters. Params is the exact
// document written to params.json; the helper validates its size and that it
// is a JSON object, and never interprets it further.
type RunMailboxSeed struct {
	RunID  string `json:"run_id"`
	Params []byte `json:"params,omitempty"`
}

// ContainerDirectory is the mailbox path as the workload sees it. The helper
// mints WEFTY_RUN_DIR from this, so no guest path is ever supplied by a caller.
func (seed RunMailboxSeed) ContainerDirectory() string {
	return contract.OCIContainerHandoffDirectory + "/" + RunMailboxDirectoryName + "/" + seed.RunID
}

// RunMailboxScope selects which directory of an attempt's handoff volume the
// bounded read path addresses. It is an enum rather than a path because no path
// from the wire is ever joined into a filesystem path: the scope names one of a
// fixed set of descents the helper knows how to perform.
type RunMailboxScope string

const (
	// RunMailboxScopeEvents is the run mailbox's event directory, and the
	// default, so every caller written before the scope existed keeps its
	// meaning exactly.
	RunMailboxScopeEvents RunMailboxScope = ""
	// RunMailboxScopeHandoffFiles is the handoff volume's own root -- where a
	// run writes result.json. It is the same volume, the same owner key and
	// the same live-attempt authority as the events scope; only the last
	// descent differs. It grants the agent nothing the workload does not
	// already have: the container mounts this directory read-write.
	RunMailboxScopeHandoffFiles RunMailboxScope = "handoff_files"
)

func (scope RunMailboxScope) valid() bool {
	switch scope {
	case RunMailboxScopeEvents, RunMailboxScopeHandoffFiles:
		return true
	default:
		return false
	}
}

// RunMailboxReference names exactly one attempt's mailbox. Every field is
// checked: the authority must match a live attempt of this session, and the
// owner key must be the one that attempt's Run declared, so a live attempt
// cannot read a different run's handoff volume.
type RunMailboxReference struct {
	Authority AttemptAuthority `json:"authority"`
	OwnerKey  string           `json:"owner_key"`
	RunID     string           `json:"run_id"`
	// Scope selects the directory within the volume. Empty is the event
	// directory, which is what every pre-scope caller meant.
	Scope RunMailboxScope `json:"scope,omitempty"`
}

type ListRunMailboxRequest struct {
	RunMailboxReference
	Limit int `json:"limit,omitempty"`
}

type ListRunMailboxResponse struct {
	Names []string `json:"names,omitempty"`
	// Exhausted reports that the listing reached its cap, so the caller must
	// treat it as incomplete rather than as the whole directory.
	Exhausted bool `json:"exhausted,omitempty"`
}

type ReadRunMailboxRequest struct {
	RunMailboxReference
	Name  string `json:"name"`
	Limit int    `json:"limit,omitempty"`
}

type ReadRunMailboxResponse struct {
	Payload   []byte `json:"payload,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
	// Unusable is the helper's positive classification of an entry that can
	// never become an event: it is not a readable regular file, or it changed
	// identity while being opened. It is deliberately a field and not an
	// error, because the caller must be able to tell "this entry is junk, drop
	// it" apart from "I could not reach the helper", and a transport failure
	// that read as junk would delete a workload's only copy of its evidence.
	Unusable bool   `json:"unusable,omitempty"`
	Reason   string `json:"reason,omitempty"`
	// Absent reports that the entry is not there at all. It is separate from
	// Unusable for the same reason Unusable is separate from an error: an
	// entry that was never written and an entry the helper could not reach are
	// different answers, and the caller acts on them differently. It also
	// keeps this read path answering the way an agent-opened directory does,
	// where a missing name is simply a not-exist error.
	Absent bool `json:"absent,omitempty"`
}

type RemoveRunMailboxEntryRequest struct {
	RunMailboxReference
	Name string `json:"name"`
}

type RemoveRunMailboxEntryResponse struct {
	Removed bool `json:"removed,omitempty"`
	// Absent distinguishes an entry that was already gone from one this call
	// deleted, so a replayed retirement is not mistaken for a failure.
	Absent bool `json:"absent,omitempty"`
}

// ValidRunMailboxName is the single name rule for the mailbox, applied by the
// agent before it asks and by the helper before it opens anything. A name is
// one path component of bounded, unambiguous characters that cannot begin with
// a dot, so it can never name `.`, `..`, or the agent-only bookkeeping.
func ValidRunMailboxName(name string) bool {
	if name == "" || len(name) > MaxRunMailboxNameBytes || strings.HasPrefix(name, ".") {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.' || r == '_' || r == '-':
		default:
			return false
		}
	}
	return true
}

func (seed RunMailboxSeed) validate() error {
	if !ValidRunMailboxName(seed.RunID) {
		return errors.New("run mailbox run ID is not a bounded mailbox name")
	}
	if len(seed.Params) > MaxRunMailboxParamsBytes {
		return fmt.Errorf("run mailbox params exceed %d bytes", MaxRunMailboxParamsBytes)
	}
	if len(seed.Params) > 0 && !json.Valid(seed.Params) {
		return errors.New("run mailbox params are not valid JSON")
	}
	return nil
}

// validate checks everything about the reference that does not require session
// state. Attempt liveness and the owner-key match are the server's, because
// only the session knows what this attempt's Run actually declared.
func (reference RunMailboxReference) validate() error {
	if err := reference.Authority.validate(); err != nil {
		return err
	}
	if !reference.Scope.valid() {
		return fmt.Errorf("run mailbox scope %q is not a known scope", string(reference.Scope))
	}
	if !ValidRunMailboxName(reference.RunID) {
		return errors.New("run mailbox run ID is not a bounded mailbox name")
	}
	if _, err := DeterministicHandoffVolumeDirectory(reference.OwnerKey); err != nil {
		return err
	}
	return nil
}

func (request ListRunMailboxRequest) boundedLimit() int {
	if request.Limit <= 0 || request.Limit > MaxRunMailboxListNames {
		return MaxRunMailboxListNames
	}
	return request.Limit
}

func (request ReadRunMailboxRequest) boundedLimit() int {
	bound := MaxRunMailboxReadBytes
	if request.Scope == RunMailboxScopeHandoffFiles {
		bound = MaxRunMailboxHandoffFileBytes
	}
	if request.Limit <= 0 || request.Limit > bound {
		return bound
	}
	return request.Limit
}

type OperatorMount struct {
	NodePath      string `json:"node_path"`
	ContainerPath string `json:"container_path"`
	ReadOnly      bool   `json:"read_only,omitempty"`
}

// WorkloadInput contains only the closed, validated inputs from which the
// privileged helper constructs its runtime spec. A caller cannot supply OCI
// JSON, namespaces, privileges, devices, or other runtime mechanics.
type WorkloadInput struct {
	ImageReference       string                `json:"image_reference"`
	ImageDigest          string                `json:"image_digest"`
	Computer             bool                  `json:"computer,omitempty"`
	Argv                 []string              `json:"argv,omitempty"`
	WorkingDirectory     string                `json:"working_directory,omitempty"`
	Environment          []EnvironmentVariable `json:"environment,omitempty"`
	SensitiveEnvironment []EnvironmentVariable `json:"sensitive_environment,omitempty"`
	// L1Endpoint, L3Endpoint, AttemptToken, RunToken, and ComputerToken are
	// closed helper-minting inputs. Their values become reserved environment
	// only inside the privileged trust boundary; the two credentials are
	// deliberately separate fields so sensitive-only routing is explicit.
	L1Endpoint          string                `json:"l1_endpoint,omitempty"`
	L3Endpoint          string                `json:"l3_endpoint,omitempty"`
	AttemptToken        string                `json:"attempt_token,omitempty"`
	RunToken            string                `json:"run_token,omitempty"`
	ComputerToken       string                `json:"computer_token,omitempty"`
	ReservedEnvironment []EnvironmentVariable `json:"reserved_environment,omitempty"`
	// RunMailbox seeds this attempt's mailbox inside its handoff volume. It
	// requires a handoff managed volume, and WEFTY_RUN_DIR is minted from it
	// inside the trust boundary like every other reserved value.
	RunMailbox           *RunMailboxSeed           `json:"run_mailbox,omitempty"`
	ManagedVolumes       []ManagedVolumeDescriptor `json:"managed_volumes,omitempty"`
	OperatorMounts       []OperatorMount           `json:"operator_mounts,omitempty"`
	Limits               WorkloadLimits            `json:"limits,omitempty"`
	helperMintedReserved bool                      `json:"-"`
}

// WorkloadLimits are cgroup-v2 hard limits. Zero means the corresponding
// limit is absent; a negative value is always invalid.
type WorkloadLimits struct {
	MemoryBytes   int64 `json:"memory_bytes,omitempty"`
	CPUMillicores int64 `json:"cpu_millicores,omitempty"`
}

type RunRequest struct {
	Authority                  AttemptAuthority `json:"authority"`
	InitialDeadman             time.Duration    `json:"initial_deadman"`
	AllocateEndpoints          []string         `json:"allocate_endpoints,omitempty"`
	EnableHostBridgeFallback   bool             `json:"enable_host_bridge_fallback,omitempty"`
	ActivateHostBridgeFallback bool             `json:"activate_host_bridge_fallback,omitempty"`
	Workload                   WorkloadInput    `json:"workload"`
	// Resources is helper-derived after decoding. It cannot be supplied over
	// the wire, so the engine always receives the deterministic names and full
	// labels before it creates the lease or any dependent resource.
	Resources ResourceIdentity `json:"-"`
}

type RunResponse struct {
	Started            bool                     `json:"started"`
	StartedAt          time.Time                `json:"started_at"`
	Image              *ImageEvidence           `json:"image,omitempty"`
	Endpoints          map[string]uint16        `json:"endpoints,omitempty"`
	HostBridgeReady    bool                     `json:"host_bridge_ready,omitempty"`
	HostBridgeEndpoint string                   `json:"host_bridge_endpoint,omitempty"`
	BridgeCapability   string                   `json:"bridge_capability,omitempty"`
	Profile            ProfileReceipt           `json:"profile"`
	Admission          ResourceAdmissionReceipt `json:"admission"`
}

// ResourceAdmissionReceipt records the exact facts used for one atomic
// newcomer decision. MemAvailable is observational and never gates admission.
type ResourceAdmissionReceipt struct {
	ObservedAt                 time.Time        `json:"observed_at"`
	Admitted                   bool             `json:"admitted"`
	FailureCode                ErrorCode        `json:"failure_code,omitempty"`
	MemoryCapacityBytes        int64            `json:"memory_capacity_bytes"`
	MemoryReserveBytes         int64            `json:"memory_reserve_bytes"`
	MemoryCommittedBeforeBytes int64            `json:"memory_committed_before_bytes"`
	RequestedMemoryBytes       int64            `json:"requested_memory_bytes"`
	MemoryCommittedAfterBytes  int64            `json:"memory_committed_after_bytes"`
	DiskCommittedBeforeBytes   int64            `json:"disk_committed_before_bytes"`
	MemTotalBytes              int64            `json:"mem_total_bytes"`
	MemAvailableBytes          int64            `json:"mem_available_bytes"`
	RequestedDiskBytes         int64            `json:"requested_disk_bytes"`
	DiskCommittedAfterBytes    int64            `json:"disk_committed_after_bytes"`
	FilesystemAvailableBytes   int64            `json:"filesystem_available_bytes"`
	ComputerTmpfsCeilingBytes  int64            `json:"computer_tmpfs_ceiling_bytes"`
	Warnings                   []ProfileWarning `json:"warnings"`
}

type ProfileWarningCode string

const (
	ProfileWarningTmpfsCeilingExceedsMemory  ProfileWarningCode = "tmpfs_ceiling_exceeds_memory_limit"
	ProfileWarningTmpfsCombinedExceedsMemory ProfileWarningCode = "tmpfs_combined_ceiling_exceeds_memory_limit"
)

// ProfileWarning is a typed, non-admission warning. Tmpfs sizes are ceilings,
// not reservations; the memory cgroup remains the enforcement boundary.
type ProfileWarning struct {
	Code         ProfileWarningCode `json:"code"`
	Target       string             `json:"target,omitempty"`
	CeilingBytes int64              `json:"ceiling_bytes"`
	LimitBytes   int64              `json:"limit_bytes"`
}

// ComputerIPv6NATState records whether the defence-in-depth IPv6 NAT table is
// configured or safely unavailable while Computer IPv6 is disabled.
type ComputerIPv6NATState string

const (
	// ComputerIPv6NATConfigured means the helper installed the IPv6 NAT chain.
	ComputerIPv6NATConfigured ComputerIPv6NATState = "configured"
	// ComputerIPv6NATUnavailableIPv6Disabled means the kernel has no IPv6 NAT
	// table; IPv6 filter policy remains mandatory and Computer IPv6 is disabled.
	ComputerIPv6NATUnavailableIPv6Disabled ComputerIPv6NATState = "unavailable_ipv6_disabled"
)

// ProfileReceipt is assertion-derived from the exact runtime profile handed
// to containerd. ComputerTmpfsCeilingBytes covers /dev/shm, /tmp, and
// /var/tmp; ordinary baseline tmpfs mounts remain outside that product delta.
type ProfileReceipt struct {
	Computer                                     bool                 `json:"computer"`
	NetworkNamespacePresent                      bool                 `json:"network_namespace_present"`
	HelperNetworkNamespaceInode                  string               `json:"helper_network_namespace_inode,omitempty"`
	TaskNetworkNamespaceInode                    string               `json:"task_network_namespace_inode,omitempty"`
	HostAbstractSocketVisible                    bool                 `json:"host_abstract_socket_visible"`
	TargetAbstractSocketLive                     bool                 `json:"target_abstract_socket_live"`
	HostAbstractSocketObservedAfterEndpointReady bool                 `json:"host_abstract_socket_observed_after_endpoint_ready"`
	ComputerNetworkAddress                       string               `json:"computer_network_address,omitempty"`
	ComputerNetworkGateway                       string               `json:"computer_network_gateway,omitempty"`
	ComputerResolverAddress                      string               `json:"computer_resolver_address,omitempty"`
	ComputerDNSProxyUDP                          bool                 `json:"computer_dns_proxy_udp"`
	ComputerDNSProxyTCP                          bool                 `json:"computer_dns_proxy_tcp"`
	ComputerDNSUpstreamAddress                   string               `json:"computer_dns_upstream_address,omitempty"`
	ComputerDNSUpstreamSource                    string               `json:"computer_dns_upstream_source,omitempty"`
	ComputerDNSUpstreamReachable                 bool                 `json:"computer_dns_upstream_reachable"`
	ComputerIPv6NATState                         ComputerIPv6NATState `json:"computer_ipv6_nat_state,omitempty"`
	MemoryLimitBytes                             int64                `json:"memory_limit_bytes"`
	MemoryMaxBytes                               int64                `json:"memory_max_bytes"`
	MemoryOOMGroup                               bool                 `json:"memory_oom_group"`
	MemorySwapMaxBytes                           int64                `json:"memory_swap_max_bytes"`
	ComputerTmpfsCeilingBytes                    int64                `json:"computer_tmpfs_ceiling_bytes"`
	LargestTmpfsTarget                           string               `json:"largest_tmpfs_target,omitempty"`
	LargestTmpfsCeilingBytes                     int64                `json:"largest_tmpfs_ceiling_bytes"`
	Warnings                                     []ProfileWarning     `json:"warnings"`
}

// ImageEvidence is produced from the local immutable image selected by the
// privileged engine. It contains no registry policy or mutable tag decision.
type ImageEvidence struct {
	SubmittedReference     string      `json:"submitted_reference"`
	TopLevelDigest         string      `json:"top_level_digest"`
	TopLevelMediaType      string      `json:"top_level_media_type"`
	IndexDigest            *string     `json:"index_digest,omitempty"`
	PlatformManifestDigest string      `json:"platform_manifest_digest"`
	Platform               OCIPlatform `json:"platform"`
	RuntimeHandler         string      `json:"runtime_handler"`
	Snapshotter            string      `json:"snapshotter"`
}

type OCIPlatform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	Variant      string `json:"variant,omitempty"`
}

type SignalRequest struct {
	Authority AttemptAuthority `json:"authority"`
	Signal    Signal           `json:"signal"`
}

// SignalResponse distinguishes a signal delivered to a live task from the
// benign race where containerd had already reaped the task. Watch remains the
// authority for the terminal outcome in both cases.
type SignalResponse struct {
	AlreadyTerminated bool `json:"already_terminated"`
}

type SetComputerControlStateRequest struct {
	Authority    AttemptAuthority `json:"authority"`
	HumanDriving bool             `json:"human_driving"`
}

type SetComputerTokenRequest struct {
	Authority  AttemptAuthority `json:"authority"`
	Token      string           `json:"token,omitempty"`
	L3Endpoint string           `json:"l3_endpoint,omitempty"`
}

type Signal string

const (
	SignalTERM Signal = "TERM"
	SignalKILL Signal = "KILL"
)

type WatchRequest struct {
	Authority AttemptAuthority `json:"authority"`
}

type WatchResponse struct {
	ExitCode              *int   `json:"exit_code,omitempty"`
	Signal                Signal `json:"signal,omitempty"`
	TerminationCause      string `json:"termination_cause,omitempty"`
	OutOfMemory           bool   `json:"out_of_memory,omitempty"`
	DiskExhausted         bool   `json:"disk_exhausted,omitempty"`
	RuntimeFailure        string `json:"runtime_failure,omitempty"`
	LogEvidenceIncomplete bool   `json:"log_evidence_incomplete,omitempty"`
}

// LogFrame is one checksum-protected frame emitted from the shim-side
// binary-v2 logger. Sequence is independent per stream.
type LogFrame struct {
	Stream   string       `json:"stream"`
	Sequence uint64       `json:"sequence"`
	Bytes    []byte       `json:"bytes"`
	Checksum string       `json:"checksum"`
	Gap      *LogGapFrame `json:"gap,omitempty"`
}

// LogGapFrame is generated by the shim-side logger/tailer when a source frame
// cannot be delivered intact. Counts describe the exact discarded frame.
type LogGapFrame struct {
	ThroughSequence uint64 `json:"through_sequence"`
	LostEventCount  uint64 `json:"lost_event_count"`
	LostByteCount   uint64 `json:"lost_byte_count"`
	Reason          string `json:"reason"`
}

// LogSeal records the pipe-EOF boundary for one stream. An incomplete seal is
// additive log evidence and never replaces the task's real terminal result.
type LogSeal struct {
	Stream   string `json:"stream"`
	Complete bool   `json:"complete"`
	// Reason states what this stream observed, in free-form text.
	Reason string `json:"reason,omitempty"`
	// ReleaseReason names a cause that lives outside the stream: the attempt's
	// task was never released, so no stream could reach pipe EOF at all. It is
	// a closed vocabulary -- currently only TaskNeverStoppedSealReason -- so an
	// agent separates that cause from a stream's own incompleteness by
	// equality rather than by matching Reason text.
	ReleaseReason string `json:"release_reason,omitempty"`
}

type WatchEventKind string

const (
	WatchProgress WatchEventKind = "progress"
	WatchComplete WatchEventKind = "complete"
)

type WatchEvent struct {
	Kind   WatchEventKind `json:"kind"`
	Status string         `json:"status,omitempty"`
	Log    *LogFrame      `json:"log,omitempty"`
	Seal   *LogSeal       `json:"seal,omitempty"`
	Result *WatchResponse `json:"result,omitempty"`
}

type DeleteRequest struct {
	Authority AttemptAuthority `json:"authority"`
}

type DeleteResponse struct {
	Deleted bool `json:"deleted"`
}

// DeleteManagedVolumeRequest names durable helper-owned state independently
// from an attempt. OwnerKey is an opaque stable handoff-owner identity.
type DeleteManagedVolumeRequest struct {
	// StorageAbsent preserves frozen payload absence as a deletion precondition:
	// only an absent root or its exact refusal tombstone and lock may remain.
	StorageAbsent       bool                           `json:"storage_absent,omitempty"`
	Kind                ManagedVolumeKind              `json:"kind"`
	OwnerKey            string                         `json:"owner_key"`
	ComputerStorage     *ComputerStorageReference      `json:"computer_storage,omitempty"`
	Removal             *ManagedVolumeRemovalAuthority `json:"removal,omitempty"`
	QuarantineOnFailure bool                           `json:"quarantine_on_failure,omitempty"`
	FailureAttempts     int                            `json:"failure_attempts,omitempty"`
}

type ManagedVolumeRemovalAuthority struct {
	NodeID            string `json:"node_id"`
	BootSessionID     string `json:"boot_session_id"`
	JobID             string `json:"job_id"`
	PriorJobID        string `json:"prior_job_id,omitempty"`
	RemovalGeneration uint64 `json:"removal_generation"`
	CleanupFence      string `json:"cleanup_fence"`
}

type DeleteManagedVolumeResponse struct {
	Deleted    bool                            `json:"deleted"`
	Quarantine *ManagedVolumeQuarantineReceipt `json:"quarantine,omitempty"`
}

type ManagedVolumeQuarantineReceipt struct {
	Kind            string                        `json:"kind"`
	ReceiptID       string                        `json:"receipt_id"`
	VolumeKind      ManagedVolumeKind             `json:"volume_kind"`
	ComputerStorage ComputerStorageReference      `json:"computer_storage"`
	Removal         ManagedVolumeRemovalAuthority `json:"removal"`
	FailureReason   EngineFailureReason           `json:"failure_reason"`
	Attempts        int                           `json:"attempts"`
}

// RemovalResourceClass is the helper inventory registry used for compound
// post-delete proof. New runtime-owned classes extend this registry without
// changing the agent's removal state machine.
type RemovalResourceClass string

const (
	RemovalResourceLease                  RemovalResourceClass = "lease"
	RemovalResourceSnapshot               RemovalResourceClass = "snapshot"
	RemovalResourceContainer              RemovalResourceClass = "container"
	RemovalResourceTask                   RemovalResourceClass = "task"
	RemovalResourceShim                   RemovalResourceClass = "shim"
	RemovalResourceCgroup                 RemovalResourceClass = "cgroup"
	RemovalResourceLogSegments            RemovalResourceClass = "log_segments"
	RemovalResourceHandoffVolume          RemovalResourceClass = "handoff_volume"
	RemovalResourceServiceData            RemovalResourceClass = "service_data"
	RemovalResourceServiceDataRecord      RemovalResourceClass = "service_data_owner_record"
	RemovalResourceComputerDiskImage      RemovalResourceClass = "computer_disk_image"
	RemovalResourceComputerDiskAllocation RemovalResourceClass = "computer_disk_allocation"
	RemovalResourceComputerDiskQuota      RemovalResourceClass = "computer_disk_quota"
	RemovalResourceComputerDiskManifest   RemovalResourceClass = "computer_disk_manifest"
	RemovalResourceComputerDiskMount      RemovalResourceClass = "computer_disk_mount"
	RemovalResourceComputerDiskLoop       RemovalResourceClass = "computer_disk_loop"
	RemovalResourceComputerAttachment     RemovalResourceClass = "computer_attachment"
	RemovalResourceComputerResetManifest  RemovalResourceClass = "computer_reset_manifest"
	RemovalResourceComputerQuarantine     RemovalResourceClass = "computer_quarantine"
	RemovalResourceComputerNetworkLink    RemovalResourceClass = "computer_network_link"
	RemovalResourceComputerFirewallRule   RemovalResourceClass = "computer_firewall_rule"
)

type RemovalResource struct {
	Class RemovalResourceClass `json:"class"`
	ID    string               `json:"id"`
}

type RemovalAttemptManifest struct {
	Authority          AttemptAuthority                            `json:"authority"`
	HandoffVolume      string                                      `json:"handoff_volume,omitempty"`
	ComputerStorage    *ComputerStorageReference                   `json:"computer_storage,omitempty"`
	StorageOnly        bool                                        `json:"storage_only,omitempty"`
	StorageAbsent      bool                                        `json:"storage_absent,omitempty"`
	StoragePreparation *contract.ComputerStoragePreparationWitness `json:"storage_preparation,omitempty"`
	Resources          []RemovalResource                           `json:"resources"`
}

type InventoryRemovalRequest struct {
	Removal         ManagedVolumeRemovalAuthority `json:"removal"`
	RootInstanceID  string                        `json:"root_instance_id"`
	ComputerStorage *ComputerStorageReference     `json:"computer_storage,omitempty"`
}

type InventoryRemovalResponse struct {
	JobID             string                   `json:"job_id"`
	RemovalGeneration uint64                   `json:"removal_generation"`
	HelperSession     HelperSession            `json:"helper_session"`
	NoRuntimeAttempts bool                     `json:"no_runtime_attempts,omitempty"`
	NoStorageEvidence bool                     `json:"no_storage_evidence,omitempty"`
	Attempts          []RemovalAttemptManifest `json:"attempts"`
}

type AttestRemovalRequest struct {
	JobID             string                   `json:"job_id"`
	RemovalGeneration string                   `json:"removal_generation"`
	Attempts          []RemovalAttemptManifest `json:"attempts"`
}

type RemovalAssertion struct {
	Class  RemovalResourceClass `json:"class"`
	ID     string               `json:"id"`
	Absent bool                 `json:"absent"`
}

type AttestRemovalResponse struct {
	JobID             string             `json:"job_id"`
	RemovalGeneration string             `json:"removal_generation"`
	HelperSession     HelperSession      `json:"helper_session"`
	Assertions        []RemovalAssertion `json:"assertions"`
}

// ExpectedRemovalResources is the helper's closed manifest registry. Agent
// manifests are pinned against this projection in adapter tests so adding a
// resource class cannot silently diverge across the protocol boundary.
func ExpectedRemovalResources(identity ResourceIdentity, handoffVolume string, computerStorage *ComputerStorageReference) []RemovalResource {
	resources := []RemovalResource{
		{Class: RemovalResourceLease, ID: identity.LeaseID},
		{Class: RemovalResourceSnapshot, ID: identity.SnapshotID},
		{Class: RemovalResourceContainer, ID: identity.ContainerID},
		{Class: RemovalResourceTask, ID: identity.TaskID},
		{Class: RemovalResourceShim, ID: identity.ShimID},
		{Class: RemovalResourceCgroup, ID: identity.CgroupID},
		{Class: RemovalResourceLogSegments, ID: identity.LogSegmentDirectory},
		{Class: RemovalResourceHandoffVolume, ID: handoffVolume},
	}
	if computerStorage == nil {
		resources = append(resources,
			RemovalResource{Class: RemovalResourceServiceData, ID: identity.ServiceVolumeDirectory},
			RemovalResource{Class: RemovalResourceServiceDataRecord, ID: identity.ServiceVolumeOwnerRecord},
		)
	} else {
		name, err := DeterministicComputerDiskName(*computerStorage)
		if err == nil {
			resources = append(resources,
				RemovalResource{Class: RemovalResourceComputerDiskImage, ID: name},
				RemovalResource{Class: RemovalResourceComputerDiskAllocation, ID: name},
				RemovalResource{Class: RemovalResourceComputerDiskQuota, ID: name},
				RemovalResource{Class: RemovalResourceComputerDiskManifest, ID: name},
				RemovalResource{Class: RemovalResourceComputerDiskMount, ID: name},
				RemovalResource{Class: RemovalResourceComputerDiskLoop, ID: name},
				RemovalResource{Class: RemovalResourceComputerAttachment, ID: name},
				RemovalResource{Class: RemovalResourceComputerResetManifest, ID: name},
				RemovalResource{Class: RemovalResourceComputerQuarantine, ID: name},
			)
		}
	}
	result := resources[:0]
	for _, resource := range resources {
		if resource.ID != "" {
			result = append(result, resource)
		}
	}
	return result
}

func expectedRemovalResources(identity ResourceIdentity, storage ...*ComputerStorageReference) []RemovalResource {
	var computerStorage *ComputerStorageReference
	if len(storage) > 0 {
		computerStorage = storage[0]
	}
	return ExpectedRemovalResources(identity, "", computerStorage)
}

func expectedComputerStorageRemovalResources(storage *ComputerStorageReference) []RemovalResource {
	if storage == nil {
		return nil
	}
	name, err := DeterministicComputerDiskName(*storage)
	if err != nil {
		return nil
	}
	return []RemovalResource{
		{Class: RemovalResourceComputerDiskImage, ID: name},
		{Class: RemovalResourceComputerDiskAllocation, ID: name},
		{Class: RemovalResourceComputerDiskQuota, ID: name},
		{Class: RemovalResourceComputerDiskManifest, ID: name},
		{Class: RemovalResourceComputerDiskMount, ID: name},
		{Class: RemovalResourceComputerDiskLoop, ID: name},
		{Class: RemovalResourceComputerAttachment, ID: name},
		{Class: RemovalResourceComputerResetManifest, ID: name},
		{Class: RemovalResourceComputerQuarantine, ID: name},
	}
}

func validateAttestRemovalRequest(request AttestRemovalRequest, nodeID string) error {
	generation, err := strconv.ParseUint(request.RemovalGeneration, 10, 64)
	if strings.TrimSpace(request.JobID) == "" || err != nil || generation == 0 || len(request.Attempts) == 0 {
		return errors.New("removal attestation requires a job, positive generation, and reconstructed attempt inventory")
	}
	seenAttempts := make(map[string]struct{}, len(request.Attempts))
	for _, attempt := range request.Attempts {
		authority := attempt.Authority
		if err := authority.validate(); err != nil || authority.NodeID != nodeID || authority.JobID != request.JobID ||
			authority.Class != "service" || authority.RemovalGeneration != request.RemovalGeneration {
			return errors.New("removal attestation attempt authority does not match the active node, job, class, and generation")
		}
		if _, exists := seenAttempts[authority.key()]; exists {
			return errors.New("removal attestation repeats an attempt authority")
		}
		seenAttempts[authority.key()] = struct{}{}
		var identity ResourceIdentity
		if !attempt.StorageOnly {
			identity, err = DeterministicResourceIdentity(authority)
			if err != nil {
				return err
			}
		}
		if attempt.ComputerStorage != nil {
			if _, err := DeterministicComputerDiskName(*attempt.ComputerStorage); err != nil || attempt.ComputerStorage.DiskBytes <= 0 {
				return errors.New("removal attestation Computer Storage identity is incomplete")
			}
		}
		want := ExpectedRemovalResources(identity, attempt.HandoffVolume, attempt.ComputerStorage)
		if attempt.StorageOnly {
			if attempt.ComputerStorage == nil {
				return errors.New("storage-only removal attestation requires Computer Storage")
			}
			switch {
			case attempt.StorageAbsent && contract.ValidStorageAbsentRemovalAttemptID(authority.AttemptID, attempt.ComputerStorage.StorageGeneration):
				if attempt.StoragePreparation != nil {
					return errors.New("already-absent storage removal evidence cannot claim preparation authority")
				}
			case !attempt.StorageAbsent && contract.ValidStorageOnlyRemovalAttemptID(authority.AttemptID, attempt.ComputerStorage.StorageGeneration):
				if attempt.StoragePreparation == nil || !attempt.StoragePreparation.Valid() ||
					attempt.StoragePreparation.NodeID != authority.NodeID || attempt.StoragePreparation.JobID != authority.JobID ||
					attempt.StoragePreparation.ComputerID != attempt.ComputerStorage.ComputerID ||
					attempt.StoragePreparation.StorageID != attempt.ComputerStorage.StorageID ||
					attempt.StoragePreparation.StorageGeneration != attempt.ComputerStorage.StorageGeneration {
					return errors.New("storage-only removal attestation requires matching durable Computer Storage preparation evidence")
				}
			case !attempt.StorageAbsent && generation <= uint64(1<<63-1) && contract.ValidComputerStorageCleanupAttemptID(authority.AttemptID, int64(generation)):
				if attempt.StoragePreparation != nil {
					return errors.New("storage cleanup attestation cannot claim never-attached preparation evidence")
				}
			default:
				return errors.New("storage-only removal attestation requires typed prepared-removal or storage-cleanup authority")
			}
			want = expectedComputerStorageRemovalResources(attempt.ComputerStorage)
		}
		if len(attempt.Resources) != len(want) {
			return errors.New("removal attestation resource inventory is incomplete")
		}
		remaining := make(map[RemovalResource]struct{}, len(want))
		for _, resource := range want {
			remaining[resource] = struct{}{}
		}
		for _, resource := range attempt.Resources {
			if strings.TrimSpace(string(resource.Class)) == "" || strings.TrimSpace(resource.ID) == "" {
				return errors.New("removal attestation resource identity is incomplete")
			}
			if _, ok := remaining[resource]; !ok {
				return fmt.Errorf("removal attestation resource %s/%s does not match deterministic authority", resource.Class, resource.ID)
			}
			delete(remaining, resource)
		}
		if len(remaining) != 0 {
			return errors.New("removal attestation omitted deterministic resources")
		}
	}
	return nil
}

type ComputerStorageResetAuthority struct {
	NodeID           string `json:"node_id"`
	BootSessionID    string `json:"boot_session_id"`
	HelperGeneration uint64 `json:"helper_generation"`
	RootInstanceID   string `json:"root_instance_id"`
	JobID            string `json:"job_id"`
	PriorJobID       string `json:"prior_job_id"`
	IntentRevision   int64  `json:"intent_revision"`
	CleanupFence     string `json:"cleanup_fence"`
}

type ResetComputerStorageRequest struct {
	Storage       ComputerStorageReference      `json:"storage"`
	NewGeneration int64                         `json:"new_generation"`
	Authority     ComputerStorageResetAuthority `json:"authority"`
}

type ComputerStorageResetReceipt = contract.ComputerStorageResetReceipt

type ResetComputerStorageResponse struct {
	Verified bool                        `json:"verified"`
	Receipt  ComputerStorageResetReceipt `json:"receipt"`
}

type ComputerStorageGrowAuthority struct {
	NodeID            string `json:"node_id"`
	BootSessionID     string `json:"boot_session_id"`
	HelperGeneration  uint64 `json:"helper_generation"`
	RootInstanceID    string `json:"root_instance_id"`
	JobID             string `json:"job_id"`
	OperationRevision int64  `json:"operation_revision"`
	OperationFence    string `json:"operation_fence"`
}

type GrowComputerStorageRequest struct {
	Storage      ComputerStorageReference     `json:"storage"`
	NewDiskBytes int64                        `json:"new_disk_bytes"`
	Authority    ComputerStorageGrowAuthority `json:"authority"`
}

type ComputerStorageGrowReceipt = contract.ComputerStorageGrowReceipt

type GrowComputerStorageResponse struct {
	Receipt ComputerStorageGrowReceipt `json:"receipt"`
}

type ComputerReimagePreflightAuthority struct {
	NodeID            string `json:"node_id"`
	BootSessionID     string `json:"boot_session_id"`
	HelperGeneration  uint64 `json:"helper_generation"`
	RootInstanceID    string `json:"root_instance_id"`
	OldJobID          string `json:"old_job_id"`
	StagingJobID      string `json:"staging_job_id"`
	OperationRevision int64  `json:"operation_revision"`
	OperationFence    string `json:"operation_fence"`
}

type PreflightComputerReimageRequest struct {
	Storage     ComputerStorageReference          `json:"storage"`
	TargetImage EnsureImageRequest                `json:"target_image"`
	Chown       bool                              `json:"chown"`
	Authority   ComputerReimagePreflightAuthority `json:"authority"`
}

type ComputerReimagePreflightReceipt struct {
	Kind                      string `json:"kind"`
	ReceiptID                 string `json:"receipt_id"`
	ComputerID                string `json:"computer_id"`
	StorageID                 string `json:"storage_id"`
	StorageGeneration         int64  `json:"storage_generation"`
	OldJobID                  string `json:"old_job_id"`
	StagingJobID              string `json:"staging_job_id"`
	NodeID                    string `json:"node_id"`
	RootInstanceID            string `json:"root_instance_id"`
	OperationRevision         int64  `json:"operation_revision"`
	OperationFence            string `json:"operation_fence"`
	TargetDigest              string `json:"target_digest"`
	PlatformOS                string `json:"platform_os"`
	PlatformArchitecture      string `json:"platform_architecture"`
	ImageUID                  uint32 `json:"image_uid"`
	ImageGID                  uint32 `json:"image_gid"`
	DiskRootUID               uint32 `json:"disk_root_uid"`
	DiskRootGID               uint32 `json:"disk_root_gid"`
	StorageEvidenceKind       string `json:"storage_evidence_kind"`
	DetachmentReceiptID       string `json:"detachment_receipt_id"`
	DetachmentAttemptID       string `json:"detachment_attempt_id"`
	DetachmentFencingToken    string `json:"detachment_fencing_token"`
	ResetPreparationReceiptID string `json:"reset_preparation_receipt_id"`
	HelperGeneration          uint64 `json:"helper_generation"`
	FailureCode               string `json:"failure_code"`
	FailureStage              string `json:"failure_stage"`
	FailureReason             string `json:"failure_reason"`
}

type PreflightComputerReimageResponse struct {
	Receipt ComputerReimagePreflightReceipt `json:"receipt"`
}

type ComputerBackupAuthority struct {
	NodeID            string `json:"node_id"`
	BootSessionID     string `json:"boot_session_id"`
	HelperGeneration  uint64 `json:"helper_generation"`
	RootInstanceID    string `json:"root_instance_id"`
	JobID             string `json:"job_id"`
	PriorJobID        string `json:"prior_job_id"`
	OperationRevision int64  `json:"operation_revision"`
	CleanupFence      string `json:"cleanup_fence"`
}

type CreateComputerBackupRequest struct {
	BackupID  string                   `json:"backup_id"`
	CopyID    string                   `json:"copy_id"`
	Storage   ComputerStorageReference `json:"storage"`
	Authority ComputerBackupAuthority  `json:"authority"`
}

type ComputerBackupCopyReceipt = contract.ComputerBackupCopyReceipt

type CreateComputerBackupResponse struct {
	Receipt ComputerBackupCopyReceipt `json:"receipt"`
}

type DeleteComputerBackupCopyRequest struct {
	BackupID   string                   `json:"backup_id"`
	CopyID     string                   `json:"copy_id"`
	Storage    ComputerStorageReference `json:"storage"`
	Authority  ComputerBackupAuthority  `json:"authority"`
	Superseded bool                     `json:"superseded"`
}

type ComputerBackupCopyRemovalReceipt = contract.ComputerBackupCopyRemovalReceipt

type DeleteComputerBackupCopyResponse struct {
	Receipt ComputerBackupCopyRemovalReceipt `json:"receipt"`
}

type ComputerStorageCopyAuthority struct {
	NodeID            string `json:"node_id"`
	BootSessionID     string `json:"boot_session_id"`
	HelperGeneration  uint64 `json:"helper_generation"`
	RootInstanceID    string `json:"root_instance_id"`
	JobID             string `json:"job_id"`
	OperationRevision int64  `json:"operation_revision"`
	CleanupFence      string `json:"cleanup_fence"`
}

type CopyComputerStorageRequest struct {
	Operation        string                       `json:"operation"`
	BackupID         string                       `json:"backup_id"`
	CopyID           string                       `json:"copy_id"`
	SourceComputerID string                       `json:"source_computer_id"`
	SourceStorageID  string                       `json:"source_storage_id"`
	SourceGeneration int64                        `json:"source_generation"`
	SourceSize       int64                        `json:"source_size"`
	SourceDigest     string                       `json:"source_digest"`
	ExportID         string                       `json:"export_id,omitempty"`
	ExternalPath     string                       `json:"external_path,omitempty"`
	ManifestDigest   string                       `json:"manifest_digest,omitempty"`
	Destination      ComputerStorageReference     `json:"destination"`
	Authority        ComputerStorageCopyAuthority `json:"authority"`
}

type ComputerStorageCopyReceipt = contract.ComputerStorageCopyReceipt

type CopyComputerStorageResponse struct {
	Receipt ComputerStorageCopyReceipt `json:"receipt"`
}

type ComputerCustodyExportAuthority struct {
	NodeID            string `json:"node_id"`
	BootSessionID     string `json:"boot_session_id"`
	HelperGeneration  uint64 `json:"helper_generation"`
	RootInstanceID    string `json:"root_instance_id"`
	OperationRevision int64  `json:"operation_revision"`
	CustodyFence      string `json:"custody_fence"`
}

type ExportComputerCustodyRequest struct {
	ExportID     string                         `json:"export_id"`
	BackupID     string                         `json:"backup_id"`
	CopyID       string                         `json:"copy_id"`
	Storage      ComputerStorageReference       `json:"storage"`
	SourceSize   int64                          `json:"source_size"`
	SourceDigest string                         `json:"source_digest"`
	ExternalPath string                         `json:"external_path"`
	JobSpec      contract.JobSpec               `json:"job_spec"`
	JobSpecHash  string                         `json:"job_spec_hash"`
	Authority    ComputerCustodyExportAuthority `json:"authority"`
}

type ComputerCustodyExportReceipt = contract.ComputerCustodyExportReceipt

type ExportComputerCustodyResponse struct {
	Receipt ComputerCustodyExportReceipt `json:"receipt"`
}

type VerifyScope string

const (
	VerifyAttempt           VerifyScope = "attempt"
	VerifyNamespace         VerifyScope = "namespace"
	VerifyNamespaceReadOnly VerifyScope = "namespace_read_only"
)

type VerifyRequest struct {
	Scope     VerifyScope       `json:"scope"`
	Authority *AttemptAuthority `json:"authority,omitempty"`
}

type VerifyResponse struct {
	Absent            bool               `json:"absent"`
	Inventory         ResourceInventory  `json:"inventory"`
	RuntimeResidue    ResourceInventory  `json:"runtime_residue"`
	DurableRetained   ResourceInventory  `json:"durable_retained"`
	DurableRetentions []DurableRetention `json:"durable_retentions"`
}

type DurableRetentionOwner string

const DurableRetentionOwnerOCIHelper DurableRetentionOwner = "oci_helper"

type DurableRetentionReason string

const (
	DurableRetentionReasonLogSpoolSealing DurableRetentionReason = "log_spool_sealing"
	DurableRetentionReasonCgroupReaping   DurableRetentionReason = "cgroup_reaping"
)

type DurableRetentionState string

const (
	DurableRetentionStateUnsealed  DurableRetentionState = "unsealed"
	DurableRetentionStatePopulated DurableRetentionState = "populated"
)

// DurableRetention binds a projected-out namespace resource to the authority
// retaining it and a closed reason. The resource remains visible in
// DurableRetained; this record explains why it does not block admission.
type DurableRetention struct {
	Class      RemovalResourceClass   `json:"class"`
	ID         string                 `json:"id"`
	Owner      DurableRetentionOwner  `json:"owner"`
	Reason     DurableRetentionReason `json:"reason"`
	AttemptID  string                 `json:"attempt_id"`
	State      DurableRetentionState  `json:"state"`
	Bound      time.Duration          `json:"bound"`
	RecordedAt time.Time              `json:"recorded_at"`
	Deadline   time.Time              `json:"deadline"`
}

type SweepAction string

const (
	SweepActionRemoved                  SweepAction = "removed"
	SweepActionKillReaped               SweepAction = "kill_reaped"
	SweepActionRetained                 SweepAction = "retained"
	SweepActionRetentionBoundReaped     SweepAction = "retention_bound_reaped"
	SweepActionResumed                  SweepAction = "resumed"
	SweepActionResumeDeferred           SweepAction = "resume_deferred"
	SweepActionRolledBack               SweepAction = "rolled_back"
	SweepActionQuarantined              SweepAction = "quarantined"
	SweepActionQuarantinePayloadDropped SweepAction = "quarantine_payload_dropped"
	SweepActionQuarantineGCFailed       SweepAction = "quarantine_gc_failed"
	SweepActionQuarantineGCEscalated    SweepAction = "quarantine_gc_escalated"
	SweepActionPreenCorrected           SweepAction = "preen_corrected"
	SweepActionAllocationReasserted     SweepAction = "allocation_reasserted"
)

// SweepEvidence is assertion-derived mechanics evidence for a helper-owned
// lost Attempt resource. It does not grant ownership; the durable Attempt
// record and the helper's live-attempt registry do that before any mutation.
type SweepEvidence struct {
	Class             RemovalResourceClass                    `json:"class"`
	ID                string                                  `json:"id"`
	AttemptID         string                                  `json:"attempt_id"`
	Action            SweepAction                             `json:"action"`
	Method            string                                  `json:"method,omitempty"`
	PIDs              []int                                   `json:"pids,omitempty"`
	Duration          time.Duration                           `json:"duration"`
	GCEvidenceStorage ComputerDiskQuarantineGCEvidenceStorage `json:"gc_evidence_storage,omitempty"`
	GCStopReason      ComputerDiskQuarantineGCStopReason      `json:"gc_stop_reason,omitempty"`
}

// SweepRequest is intentionally empty: the boot barrier always sweeps the
// complete wefty namespace, so no caller-supplied selection policy exists.
type SweepRequest struct {
	// SweepEpoch is helper-derived before engine entry and never decoded from
	// the wire. Computer attachment receipts bind to this exact sweep.
	SweepEpoch string `json:"-"`
	// countComputerStorageRecoveryAttempt is helper-derived. Only the
	// agent-requested boot-barrier sweep consumes the durable recovery-attempt
	// budget; helper startup and in-session ReapSession sweeps do not advance it.
	countComputerStorageRecoveryAttempt bool
}

type SweepResponse struct {
	SweepEpoch            string                  `json:"sweep_epoch"`
	Removed               int                     `json:"removed"`
	PriorBootSessionsSeen []SessionIdentity       `json:"prior_boot_sessions_seen"`
	Inventory             ResourceInventory       `json:"inventory"`
	Attempts              []SweptAttemptAuthority `json:"attempts"`
	DurableRetentions     []DurableRetention      `json:"durable_retentions"`
	Evidence              []SweepEvidence         `json:"evidence"`
}

// ResourceInventory is the engine's complete, class-separated namespace
// observation. Empty slices are retained in receipts so every inventory class
// is explicitly verified, rather than inferred from a total count.
type ResourceInventory struct {
	Leases                     []string                                `json:"leases"`
	Snapshots                  []string                                `json:"snapshots"`
	Containers                 []string                                `json:"containers"`
	Tasks                      []string                                `json:"tasks"`
	Shims                      []string                                `json:"shims"`
	Cgroups                    []string                                `json:"cgroups"`
	LogSegments                []string                                `json:"log_segments"`
	ImageSpools                []string                                `json:"image_spools"`
	ManagedVolumes             []string                                `json:"managed_volumes"`
	ManagedVolumeRecords       []string                                `json:"managed_volume_records"`
	ComputerDiskImages         []string                                `json:"computer_disk_images"`
	ComputerDiskAllocations    []string                                `json:"computer_disk_allocations"`
	ComputerDiskQuotas         []string                                `json:"computer_disk_quotas"`
	ComputerDiskManifests      []string                                `json:"computer_disk_manifests"`
	ComputerDiskMounts         []string                                `json:"computer_disk_mounts"`
	ComputerDiskLoops          []string                                `json:"computer_disk_loops"`
	ComputerAttachments        []string                                `json:"computer_attachments"`
	ComputerResetManifests     []string                                `json:"computer_reset_manifests"`
	ComputerQuarantines        []string                                `json:"computer_quarantines"`
	ComputerStorageDeferred    []ComputerStorageRecoveryInventoryEntry `json:"computer_storage_deferred"`
	ComputerStorageQuarantined []ComputerStorageRecoveryInventoryEntry `json:"computer_storage_quarantined"`
	// ComputerDiskAnomalies are per-disk observations. They remain auditable
	// without turning one durable disk's accounting drift into node-wide helper failure.
	ComputerDiskAnomalies []string `json:"computer_disk_anomalies"`
	ComputerNetworkLinks  []string `json:"computer_network_links"`
	ComputerFirewallRules []string `json:"computer_firewall_rules"`
	// HandoffRetentionRecords are the helper-owned terminal receipts under
	// `handoffs-state/`, one per handoff volume. They are their own class
	// because the boot receipt's "observed = runtime residue union durable
	// retained" invariant has to keep holding over every durable thing the
	// helper writes: a receipt whose volume is retained is retained with it,
	// and a receipt left standing over a volume that is gone is residue to
	// remove, exactly as a service data owner record is.
	HandoffRetentionRecords []string `json:"handoff_retention_records"`
}

// RetainedHandoffVolume is one handoff volume this node still holds, as the
// helper sees it.
//
// There is no owner key here, in either direction. The agent derives every
// name it knows with DeterministicHandoffVolumeDirectory, so a name in this
// list that it cannot map back to one of its own runs is precisely the
// crash-residue signal -- and no identity the helper holds leaves the helper.
type RetainedHandoffVolume struct {
	Name string `json:"name"`
	// TerminalAt is when the helper observed this volume's last attempt
	// finish, read from the helper-owned receipt. TerminalKnown says whether
	// that receipt was there and bound to this directory. When it is false,
	// TerminalAt carries the directory's mtime as a labelled fallback: a
	// uid-0 workload owns its own handoff directory and can move that
	// timestamp, so it is a figure to report and to evict against, never one
	// to expire on.
	TerminalAt    time.Time `json:"terminal_at"`
	TerminalKnown bool      `json:"terminal_known"`
	// LogicalBytes is what this volume's regular files hold, counted once per
	// link inside the volume; DedupedBytes charges an inode to the first
	// volume of this pass that reached it, so a file two volumes hard-link is
	// counted once for the node. Symlinks are never followed.
	LogicalBytes int64 `json:"logical_bytes"`
	DedupedBytes int64 `json:"deduped_bytes"`
	// ChargedBytes is what this volume costs a node budget, under exactly the
	// rule the agent's own handoff root uses: every directory entry the pass
	// reached contributes the larger of its deduplicated logical bytes and
	// 4 KiB. One charging rule over both roots is the point -- an entry is an
	// inode and a directory slot whatever it holds, so a tree of a million
	// empty files is ~0 logical bytes and about 4 GiB here, and the node that
	// is really out of inodes can see it. It is reported rather than derived
	// from the two figures above, because no function of a volume total and an
	// entry count reproduces a per-entry floor.
	ChargedBytes int64 `json:"charged_bytes"`
	// Entries is every directory entry the pass reached, which is the inode
	// cost neither byte figure can show on its own.
	Entries int64 `json:"entries"`
	// Live says a live attempt of this session is still writing here. Its
	// results are not retained yet and it is nobody's eviction candidate.
	Live bool `json:"live"`
	// Truncated says the measurement stopped early -- budget, depth or
	// cancellation -- so the byte and entry figures are a floor rather than a
	// measurement. The terminal time above is never a floor.
	Truncated bool `json:"truncated,omitempty"`
	// Anomalies are the per-volume observations that must not fail a
	// node-wide call, the ComputerDiskAnomalies precedent. They are a closed
	// token vocabulary, deduplicated and capped, rather than free text,
	// because a response's size must not be a function of what a workload
	// wrote: a frame over MaxFrameBytes costs the node its session, not its
	// accounting. Empty means the volume was read whole.
	Anomalies []HandoffVolumeAnomaly `json:"anomalies,omitempty"`
}

// HandoffVolumeAnomaly is the closed vocabulary of per-volume observations.
//
// It is a fixed token set rather than free text for two reasons: a reader and
// a later consumer should name the same condition, and a response's size must
// not be a function of what a workload wrote. An earlier draft carried the
// walker's own error strings, repeated per failure, which made a node-wide
// read's frame unbounded.
type HandoffVolumeAnomaly string

const (
	// HandoffAnomalyNoReceipt: no helper-owned terminal time exists, so the
	// reported one is the directory's mtime, which the workload owns.
	HandoffAnomalyNoReceipt HandoffVolumeAnomaly = "no_receipt"
	// HandoffAnomalyReceiptUnreadable: the receipt is there and could not be
	// read.
	HandoffAnomalyReceiptUnreadable HandoffVolumeAnomaly = "receipt_unreadable"
	// HandoffAnomalyReceiptInvalid: the receipt is not a version-1 helper
	// receipt, or carries no terminal time.
	HandoffAnomalyReceiptInvalid HandoffVolumeAnomaly = "receipt_invalid"
	// HandoffAnomalyReceiptMismatched: the receipt names a different directory
	// than the one standing at this volume's name.
	HandoffAnomalyReceiptMismatched HandoffVolumeAnomaly = "receipt_identity_mismatch"
	// HandoffAnomalyVolumeUnreadable: the volume itself could not be opened or
	// enumerated as a helper-owned directory.
	HandoffAnomalyVolumeUnreadable HandoffVolumeAnomaly = "volume_unreadable"
	// HandoffAnomalyMeasurementTruncated: the pass stopped early -- budget,
	// depth, or cancellation -- so the figures are a floor.
	HandoffAnomalyMeasurementTruncated HandoffVolumeAnomaly = "measurement_truncated"
	// HandoffAnomalySubtreeReplaced: a directory stopped being the one this
	// pass had just observed, so it was counted rather than measured.
	HandoffAnomalySubtreeReplaced HandoffVolumeAnomaly = "subtree_replaced"
)

// HandoffVolumeLiveError refuses a handoff-volume deletion because an attempt
// still owns the volume.
//
// It is a refusal, not a failure: nothing was detached and nothing was freed,
// and the identical request succeeds once that attempt finishes. The caller it
// exists for is the agent's budget eviction, which selects from an inventory
// snapshot and can therefore arrive after a rerun has claimed the volume it
// chose -- and taking a mounted tree out from under a running container is not
// something a byte budget may do.
//
// It lives here rather than beside the engine that raises it because the
// server maps it to a wire code on every platform, including the ones that
// build no containerd engine at all.
type HandoffVolumeLiveError struct {
	// Name is the helper's own directory name for the volume. It is the
	// helper's to say: the caller named an owner key, and this is what that
	// key derives to here.
	Name string
}

func (err *HandoffVolumeLiveError) Error() string {
	return fmt.Sprintf("handoff volume %s is owned by a live attempt and is not the budget's to give up", err.Name)
}

func (err *HandoffVolumeLiveError) Code() ErrorCode { return CodeHandoffVolumeLive }

// InventoryHandoffVolumesRequest carries a page cursor and nothing else.
// Attempt authority is not merely unnecessary here, it is unrepresentable: a
// body that tries to attach one is refused as an unknown field.
type InventoryHandoffVolumesRequest struct {
	// After resumes a listing after this volume name, which is the `Next` a
	// previous response returned. Names are listed in sorted order, so a
	// cursor is a name and nothing about it has to be remembered by the
	// helper between calls.
	//
	// A flag alone could not make the tail of a large root readable: it said
	// the response stopped and gave no way to ask for the rest, so a node
	// could not know whether the published result it must give up first was
	// simply beyond the prefix.
	After string `json:"after,omitempty"`
}

type InventoryHandoffVolumesResponse struct {
	Volumes []RetainedHandoffVolume `json:"volumes"`
	// Exhausted says this page stopped before the end of the root -- its row
	// or byte budget ran out, or its caller was cancelled -- so these figures
	// are this page's, not the node's.
	Exhausted bool `json:"exhausted"`
	// Next is the cursor to pass as the following request's After. A page that
	// stopped early carries one; a page that finished does not. What says the
	// listing is complete is `exhausted=false`, never an empty cursor, because
	// a page that stopped before its first row has an empty cursor too.
	Next string `json:"next,omitempty"`
	// Restart says the root changed under this listing, so there is no
	// consistent page to return and the reader starts over from the first.
	// Stitching a page of one root to a page of another is how a volume
	// created behind the cursor becomes invisible to every call -- and to a
	// budget that gives published results up first, an invisible published
	// volume is an unpublished one destroyed.
	Restart bool `json:"restart,omitempty"`
	// Generation is the root's mutation counter for the scan this page came
	// from, so a reader can assert that every page it stitches together
	// describes the same root. On a restart it is the counter now.
	Generation uint64 `json:"generation"`
	// DetachedTrees counts the volumes an authorized deletion detached from
	// their names and whose bytes are not yet freed, after this call finished
	// what it could. They are in no volume's figures above -- a detached tree
	// is nobody's volume -- so a node budget that could not see them would be
	// reading a node emptier than it is.
	DetachedTrees int `json:"detached_trees,omitempty"`
}

// MaxInventoriedHandoffVolumes bounds one InventoryHandoffVolumes page the way
// the run mailbox listing is bounded: a node is handed a page and a cursor,
// never an unbounded frame.
const MaxInventoriedHandoffVolumes = 4096

// MaxHandoffInventoryCursorBytes bounds the page cursor a caller may send. A
// cursor is a volume name, and a name this root can hold is far shorter; the
// bound exists so an unbounded string cannot be spent on the comparison.
const MaxHandoffInventoryCursorBytes = 256

// handoffInventoryFrameHeadroom is what one InventoryHandoffVolumes response
// leaves below MaxFrameBytes for the reply envelope and framing.
//
// A frame the transport refuses is not a smaller answer: the server discards
// the write error and closes the connection, and the client reads that as a
// lost session. A row cap alone did not bound this -- 4096 receiptless volumes
// encode past MaxFrameBytes on their own -- so the budget is in encoded bytes.
// Accounting must never cost a node its runtime.
const handoffInventoryFrameHeadroom = 64 << 10

// HandoffRetentionRecordName maps one handoff volume directory to the
// helper-owned receipt that carries its terminal time. Both sides spell it
// here and nowhere else.
func HandoffRetentionRecordName(volume string) string {
	if volume == "" {
		return ""
	}
	return volume + handoffRetentionRecordSuffix
}

const (
	handoffRetentionRecordSuffix = ".retention"
	// handoffVolumeNamePrefix is what every scan of the handoff root matches
	// on. The retention receipts live in their own root rather than beside the
	// volumes precisely because a sibling carrying this prefix would be read
	// as a volume.
	handoffVolumeNamePrefix = "wefty-handoff-volume-"
	// handoffDetachedVolumePrefix names a volume between being detached from
	// its own name and having its bytes freed. It is dot-prefixed so no scan
	// that matches handoffVolumeNamePrefix can see it -- neither the
	// inventory, nor expiry, nor accounting repair.
	handoffDetachedVolumePrefix = ".removing-"
)

type ComputerDiskQuarantineGCEvidenceStorage string
type ComputerDiskQuarantineGCStopReason string

const (
	ComputerDiskQuarantineGCEvidencePrimary      ComputerDiskQuarantineGCEvidenceStorage = "primary_receipt"
	ComputerDiskQuarantineGCEvidenceMirror       ComputerDiskQuarantineGCEvidenceStorage = "mirror_receipt"
	ComputerDiskQuarantineGCEvidenceMemory       ComputerDiskQuarantineGCEvidenceStorage = "memory_only_both_writes_failed"
	ComputerDiskQuarantineGCStopFailureLimit     ComputerDiskQuarantineGCStopReason      = "failure_limit"
	ComputerDiskQuarantineGCStopUnrecordedWindow ComputerDiskQuarantineGCStopReason      = "unrecorded_retry_window_elapsed"
)

// ComputerStorageRecoveryInventoryEntry gives deferred and quarantined disk
// generations a typed operator surface. DiskName remains corroborating
// physical custody and Storage is the generation authority when readable; an
// operational record-read deferral leaves Storage zero rather than inventing
// identity from the directory name.
type ComputerStorageRecoveryInventoryEntry struct {
	Storage           ComputerStorageReference                `json:"storage"`
	DiskName          string                                  `json:"disk_name"`
	Operation         string                                  `json:"operation"`
	Reason            string                                  `json:"reason"`
	DeferredReason    string                                  `json:"deferred_reason,omitempty"`
	Attempts          int                                     `json:"attempts"`
	FirstDeferredAt   time.Time                               `json:"first_deferred_at,omitempty"`
	PayloadDroppedAt  *time.Time                              `json:"payload_dropped_at,omitempty"`
	GCFailures        int                                     `json:"gc_failures,omitempty"`
	GCFirstFailedAt   time.Time                               `json:"gc_first_failed_at,omitempty"`
	GCEscalatedAt     time.Time                               `json:"gc_escalated_at,omitempty"`
	GCLastFailure     string                                  `json:"gc_last_failure,omitempty"`
	GCEvidenceStorage ComputerDiskQuarantineGCEvidenceStorage `json:"gc_evidence_storage,omitempty"`
	GCRetryStoppedAt  time.Time                               `json:"gc_retry_stopped_at,omitempty"`
	GCRetryStopReason ComputerDiskQuarantineGCStopReason      `json:"gc_retry_stop_reason,omitempty"`
}

type ComputerStorageResumeDeferredError struct{ Storage ComputerStorageReference }

func (err *ComputerStorageResumeDeferredError) Error() string {
	return "Computer Storage recovery is resume_deferred"
}

type ComputerStorageQuarantinedError struct{ Storage ComputerStorageReference }

func (err *ComputerStorageQuarantinedError) Error() string {
	return "Computer Storage generation is quarantined"
}

func recoveryInventoryEntryKey(entry ComputerStorageRecoveryInventoryEntry) string {
	payloadDroppedAt := ""
	if entry.PayloadDroppedAt != nil {
		payloadDroppedAt = entry.PayloadDroppedAt.UTC().Format(time.RFC3339Nano)
	}
	gcFirstFailedAt := ""
	if !entry.GCFirstFailedAt.IsZero() {
		gcFirstFailedAt = entry.GCFirstFailedAt.UTC().Format(time.RFC3339Nano)
	}
	gcEscalatedAt := ""
	if !entry.GCEscalatedAt.IsZero() {
		gcEscalatedAt = entry.GCEscalatedAt.UTC().Format(time.RFC3339Nano)
	}
	gcRetryStoppedAt := ""
	if !entry.GCRetryStoppedAt.IsZero() {
		gcRetryStoppedAt = entry.GCRetryStoppedAt.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%020d\x00%s\x00%020d\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s", entry.DiskName, entry.Operation,
		entry.Reason, entry.FirstDeferredAt.UTC().Format(time.RFC3339Nano), entry.Attempts, payloadDroppedAt, entry.GCFailures,
		gcFirstFailedAt, gcEscalatedAt, entry.GCLastFailure, entry.GCEvidenceStorage, gcRetryStoppedAt, entry.GCRetryStopReason)
}

// SweptAttemptAuthority is the immutable removal-validation subset recovered
// from labels while sweeping. PriorBootSessionID is the owning boot observed
// on the swept resource; it is never rewritten to the acquiring boot.
type SweptAttemptAuthority struct {
	NodeID             string `json:"node_id"`
	JobID              string `json:"job_id"`
	RemovalGeneration  string `json:"removal_generation"`
	AttemptID          string `json:"attempt_id"`
	FencingToken       string `json:"fencing_token"`
	PriorBootSessionID string `json:"prior_boot_session_id"`
	Class              string `json:"class"`
}

// VerifiedSweepReceipt joins the engine's sweep inventory with an independent
// empty namespace observation and the exact helper session that performed it.
type BootBarrierTimelineReceipt struct {
	AdvertisedReapTimeout   time.Duration `json:"advertised_reap_timeout"`
	TakeoverBound           time.Duration `json:"takeover_bound"`
	VerifiedReadyBound      time.Duration `json:"verified_ready_bound"`
	HelperLossObservedAt    time.Time     `json:"helper_loss_observed_at,omitempty"`
	BarrierStartedAt        time.Time     `json:"barrier_started_at"`
	PrefaceCompletedAt      time.Time     `json:"preface_completed_at"`
	SessionAdmittedAt       time.Time     `json:"session_admitted_at"`
	VerifiedReadyAt         time.Time     `json:"verified_ready_at"`
	PrefacedDuringStartup   bool          `json:"prefaced_during_startup"`
	HandshakeElapsed        time.Duration `json:"handshake_elapsed"`
	SessionAdmissionElapsed time.Duration `json:"session_admission_elapsed"`
	SweepElapsed            time.Duration `json:"sweep_elapsed"`
	VerifyElapsed           time.Duration `json:"verify_elapsed"`
	VerifiedReadyElapsed    time.Duration `json:"verified_ready_elapsed"`
}

type VerifiedSweepReceipt struct {
	SweepEpoch                      string                     `json:"sweep_epoch"`
	HelperSession                   HelperSession              `json:"helper_session"`
	PriorBootSessionsSeen           []SessionIdentity          `json:"prior_boot_sessions_seen"`
	SweptInventory                  ResourceInventory          `json:"swept_inventory"`
	VerifiedAbsent                  bool                       `json:"verified_absent"`
	VerifiedInventory               ResourceInventory          `json:"verified_inventory"`
	VerifiedResidue                 ResourceInventory          `json:"verified_residue"`
	VerifiedRetained                ResourceInventory          `json:"verified_retained"`
	ComputerStorageDeferredCount    int                        `json:"computer_storage_deferred_count"`
	ComputerStorageQuarantinedCount int                        `json:"computer_storage_quarantined_count"`
	DurableRetentions               []DurableRetention         `json:"durable_retentions"`
	SweepEvidence                   []SweepEvidence            `json:"sweep_evidence"`
	Attempts                        []SweptAttemptAuthority    `json:"attempts"`
	BarrierTimeline                 BootBarrierTimelineReceipt `json:"barrier_timeline"`
}

type DialAttemptPortRequest struct {
	Authority AttemptAuthority `json:"authority"`
	Name      string           `json:"name"`
	// Port is helper-derived after authorization and never decoded from the wire.
	Port     uint16 `json:"-"`
	CgroupID string `json:"-"`
}

type DialHostBridgeRequest struct {
	Authority        AttemptAuthority `json:"authority"`
	BridgeCapability string           `json:"bridge_capability"`
}

func marshalBody(value any) (json.RawMessage, error) {
	if value == nil {
		return nil, nil
	}
	return json.Marshal(value)
}

func decodeBody(raw json.RawMessage, target any) error {
	if len(raw) == 0 {
		return errors.New("request body is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body contains trailing data")
	}
	return nil
}

func randomCapability() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}
