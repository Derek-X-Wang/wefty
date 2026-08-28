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
	"strings"
	"time"
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
	MethodRun                Method = "Run"
	MethodSignal             Method = "Signal"
	MethodWatch              Method = "Watch"
	MethodDelete             Method = "Delete"
	MethodDeleteVolume       Method = "DeleteManagedVolume"
	MethodVerify             Method = "Verify"
	MethodSweep              Method = "Sweep"
	MethodDialAttemptPort    Method = "DialAttemptPort"
	MethodDialHostBridge     Method = "DialHostBridge"
	MethodSetComputerControl Method = "SetComputerControlState"
)

// attemptPortBackendReady is emitted only after the helper has connected the
// authorized stream to the payload's exact attempt-local loopback port.
const attemptPortBackendReady byte = 1

type ErrorCode string

const (
	CodeInvalidRequest        ErrorCode = "invalid_request"
	CodePeerUnauthenticated   ErrorCode = "peer_unauthenticated"
	CodeVersionMismatch       ErrorCode = "version_mismatch"
	CodeChecksumMismatch      ErrorCode = "checksum_mismatch"
	CodeSessionBusy           ErrorCode = "session_busy"
	CodeSessionStale          ErrorCode = "session_stale"
	CodeUnauthorizedAttempt   ErrorCode = "unauthorized_attempt"
	CodeAttemptOutsideSession ErrorCode = "attempt_outside_session"
	CodeUnauthorizedPort      ErrorCode = "unauthorized_port"
	CodeUnauthorizedBridge    ErrorCode = "unauthorized_bridge"
	CodeOCISpecRejected       ErrorCode = "oci_spec_rejected"
	CodeImageUnavailable      ErrorCode = "image_unavailable"
	CodeInsufficientDisk      ErrorCode = "insufficient_disk"
	CodeEngineFailure         ErrorCode = "engine_failure"
	CodeUnsupportedOperation  ErrorCode = "unsupported_operation"
	CodeSweepRequired         ErrorCode = "sweep_required"
)

// RPCError is safe to cross the private protocol. Engine detail remains local.
type RPCError struct {
	Code         ErrorCode         `json:"code"`
	Message      string            `json:"message"`
	ImageFailure *ImageFailureFact `json:"image_failure,omitempty"`
	DiskFailure  *DiskFailureFact  `json:"disk_failure,omitempty"`
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

func (failure *insufficientDiskError) Error() string {
	return "insufficient disk for full Computer allocation"
}
func (failure *insufficientDiskError) Unwrap() error { return failure.err }

// ImageFailureFact is sanitized mechanics evidence. The helper reports only
// what it observed; retry and terminal classification remain agent policy.
type ImageFailureFact struct {
	Kind           ImageFailureKind `json:"kind"`
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
// carried only as an immutable resource label; it never selects mechanics.
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
		HandoffVolumeDirectory:   "wefty-handoff-volume-" + suffix,
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
	return "wefty-handoff-volume-" + hex.EncodeToString(digest[:16]), nil
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
	DiskBytes         int64  `json:"disk_bytes"`
}

type ManagedVolumeDescriptor struct {
	Kind            ManagedVolumeKind         `json:"kind"`
	OwnerKey        string                    `json:"owner_key,omitempty"`
	ComputerStorage *ComputerStorageReference `json:"computer_storage,omitempty"`
	ReadOnly        bool                      `json:"read_only,omitempty"`
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
	ImageReference       string                    `json:"image_reference"`
	ImageDigest          string                    `json:"image_digest"`
	Argv                 []string                  `json:"argv,omitempty"`
	WorkingDirectory     string                    `json:"working_directory,omitempty"`
	Environment          []EnvironmentVariable     `json:"environment,omitempty"`
	SensitiveEnvironment []EnvironmentVariable     `json:"sensitive_environment,omitempty"`
	ReservedEnvironment  []EnvironmentVariable     `json:"reserved_environment,omitempty"`
	ManagedVolumes       []ManagedVolumeDescriptor `json:"managed_volumes,omitempty"`
	OperatorMounts       []OperatorMount           `json:"operator_mounts,omitempty"`
	Limits               WorkloadLimits            `json:"limits,omitempty"`
}

// WorkloadLimits are cgroup-v2 hard limits. Zero means the corresponding
// limit is absent; a negative value is always invalid.
type WorkloadLimits struct {
	MemoryBytes   int64 `json:"memory_bytes,omitempty"`
	CPUMillicores int64 `json:"cpu_millicores,omitempty"`
}

type RunRequest struct {
	Authority                AttemptAuthority `json:"authority"`
	InitialDeadman           time.Duration    `json:"initial_deadman"`
	AllocateEndpoints        []string         `json:"allocate_endpoints,omitempty"`
	EnableHostBridgeFallback bool             `json:"enable_host_bridge_fallback,omitempty"`
	Workload                 WorkloadInput    `json:"workload"`
	// Resources is helper-derived after decoding. It cannot be supplied over
	// the wire, so the engine always receives the deterministic names and full
	// labels before it creates the lease or any dependent resource.
	Resources ResourceIdentity `json:"-"`
}

type RunResponse struct {
	Started          bool              `json:"started"`
	Image            *ImageEvidence    `json:"image,omitempty"`
	Endpoints        map[string]uint16 `json:"endpoints,omitempty"`
	HostBridgeReady  bool              `json:"host_bridge_ready,omitempty"`
	BridgeCapability string            `json:"bridge_capability,omitempty"`
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

type SetComputerControlStateRequest struct {
	Authority    AttemptAuthority `json:"authority"`
	HumanDriving bool             `json:"human_driving"`
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
	Reason   string `json:"reason,omitempty"`
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
	Kind            ManagedVolumeKind              `json:"kind"`
	OwnerKey        string                         `json:"owner_key"`
	ComputerStorage *ComputerStorageReference      `json:"computer_storage,omitempty"`
	Removal         *ManagedVolumeRemovalAuthority `json:"removal,omitempty"`
}

type ManagedVolumeRemovalAuthority struct {
	NodeID            string `json:"node_id"`
	BootSessionID     string `json:"boot_session_id"`
	JobID             string `json:"job_id"`
	RemovalGeneration uint64 `json:"removal_generation"`
	CleanupFence      string `json:"cleanup_fence"`
}

type DeleteManagedVolumeResponse struct {
	Deleted bool `json:"deleted"`
}

type VerifyScope string

const (
	VerifyAttempt   VerifyScope = "attempt"
	VerifyNamespace VerifyScope = "namespace"
)

type VerifyRequest struct {
	Scope     VerifyScope       `json:"scope"`
	Authority *AttemptAuthority `json:"authority,omitempty"`
}

type VerifyResponse struct {
	Absent    bool              `json:"absent"`
	Inventory ResourceInventory `json:"inventory"`
}

// SweepRequest is intentionally empty: the boot barrier always sweeps the
// complete wefty namespace, so no caller-supplied selection policy exists.
type SweepRequest struct {
	// SweepEpoch is helper-derived before engine entry and never decoded from
	// the wire. Computer attachment receipts bind to this exact sweep.
	SweepEpoch string `json:"-"`
}

type SweepResponse struct {
	SweepEpoch            string                  `json:"sweep_epoch"`
	Removed               int                     `json:"removed"`
	PriorBootSessionsSeen []string                `json:"prior_boot_sessions_seen"`
	Inventory             ResourceInventory       `json:"inventory"`
	Attempts              []SweptAttemptAuthority `json:"attempts"`
}

// ResourceInventory is the engine's complete, class-separated namespace
// observation. Empty slices are retained in receipts so every inventory class
// is explicitly verified, rather than inferred from a total count.
type ResourceInventory struct {
	Leases                  []string `json:"leases"`
	Snapshots               []string `json:"snapshots"`
	Containers              []string `json:"containers"`
	Tasks                   []string `json:"tasks"`
	Shims                   []string `json:"shims"`
	Cgroups                 []string `json:"cgroups"`
	LogSegments             []string `json:"log_segments"`
	ManagedVolumes          []string `json:"managed_volumes"`
	ManagedVolumeRecords    []string `json:"managed_volume_records"`
	ComputerDiskImages      []string `json:"computer_disk_images"`
	ComputerDiskAllocations []string `json:"computer_disk_allocations"`
	ComputerDiskQuotas      []string `json:"computer_disk_quotas"`
	ComputerDiskManifests   []string `json:"computer_disk_manifests"`
	ComputerDiskMounts      []string `json:"computer_disk_mounts"`
	ComputerDiskLoops       []string `json:"computer_disk_loops"`
	ComputerAttachments     []string `json:"computer_attachments"`
	// ComputerDiskAnomalies are per-disk observations. They remain auditable
	// without turning one durable disk's accounting drift into node-wide helper failure.
	ComputerDiskAnomalies []string `json:"computer_disk_anomalies"`
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
type VerifiedSweepReceipt struct {
	SweepEpoch            string                  `json:"sweep_epoch"`
	HelperSession         HelperSession           `json:"helper_session"`
	PriorBootSessionsSeen []string                `json:"prior_boot_sessions_seen"`
	SweptInventory        ResourceInventory       `json:"swept_inventory"`
	VerifiedInventory     ResourceInventory       `json:"verified_inventory"`
	Attempts              []SweptAttemptAuthority `json:"attempts"`
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

// SupportsComputerProtocol reports only the negotiated wire-semantic fact.
// Functional runtime probing and boot-scoped publication remain agent-owned.
func SupportsComputerProtocol(handshake AcquireSessionResponse) bool {
	return handshake.ProtocolVersion >= ComputerProtocolVersion
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
