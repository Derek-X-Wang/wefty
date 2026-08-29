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
	CodeInsufficientMemory    ErrorCode = "insufficient_memory"
	CodeInsufficientDisk      ErrorCode = "insufficient_disk"
	CodeEngineFailure         ErrorCode = "engine_failure"
	// CodeDiagnosticFailure is a read-only observation failure. It is never
	// evidence that the helper session or runtime authority was lost.
	CodeDiagnosticFailure    ErrorCode = "diagnostic_failure"
	CodeUnsupportedOperation ErrorCode = "unsupported_operation"
	CodeSweepRequired        ErrorCode = "sweep_required"
)

// RPCError is safe to cross the private protocol. Engine detail remains local.
type RPCError struct {
	Code          ErrorCode          `json:"code"`
	Message       string             `json:"message"`
	ImageFailure  *ImageFailureFact  `json:"image_failure,omitempty"`
	MemoryFailure *MemoryFailureFact `json:"memory_failure,omitempty"`
	DiskFailure   *DiskFailureFact   `json:"disk_failure,omitempty"`
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

	RuncVersionSourceConfiguredPath = "configured_absolute_path"
	RuncVersionSourceContainerdInfo = "containerd_runtime_info"
)

// DiagnosticReadReceipt states whether one helper-side read produced its
// fact. ErrorCode is a closed, sanitized local code; raw errors never cross
// the privileged helper boundary.
type DiagnosticReadReceipt struct {
	Outcome   DiagnosticReadOutcome `json:"outcome"`
	ErrorCode string                `json:"error_code,omitempty"`
}

// DoctorStatus is the helper's read-only mechanics snapshot. It contains no
// session capability, raw error, or mutation control and is safe to surface to
// the operator-only node doctor.
type DoctorStatus struct {
	RuntimePlatform    OCIPlatform               `json:"runtime_platform"`
	ContainerdVersion  string                    `json:"containerd_version"`
	ContainerdRead     DiagnosticReadReceipt     `json:"containerd_read"`
	RuncVersion        string                    `json:"runc_version"`
	RuncVersionSource  string                    `json:"runc_version_source"`
	RuncRead           DiagnosticReadReceipt     `json:"runc_read"`
	AllowedMountRoots  []string                  `json:"allowed_mount_roots"`
	MountRootsRead     DiagnosticReadReceipt     `json:"mount_roots_read"`
	Cache              ImageCacheStatus          `json:"cache"`
	CacheRead          DiagnosticReadReceipt     `json:"cache_read"`
	CacheLastErrorCode string                    `json:"cache_last_error_code,omitempty"`
	LastProfile        *ProfileReceipt           `json:"last_profile,omitempty"`
	LastAdmission      *ResourceAdmissionReceipt `json:"last_admission,omitempty"`
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
	// L3Endpoint, RunToken, and ComputerToken are closed helper-minting inputs. Their values
	// become reserved environment only inside the privileged trust boundary;
	// RunToken is deliberately separate so sensitive-only routing is explicit.
	L3Endpoint           string                    `json:"l3_endpoint,omitempty"`
	RunToken             string                    `json:"run_token,omitempty"`
	ComputerToken        string                    `json:"computer_token,omitempty"`
	ReservedEnvironment  []EnvironmentVariable     `json:"reserved_environment,omitempty"`
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

// ProfileReceipt is assertion-derived from the exact runtime profile handed
// to containerd. ComputerTmpfsCeilingBytes covers /dev/shm, /tmp, and
// /var/tmp; ordinary baseline tmpfs mounts remain outside that product delta.
type ProfileReceipt struct {
	Computer                  bool             `json:"computer"`
	MemoryLimitBytes          int64            `json:"memory_limit_bytes"`
	MemoryMaxBytes            int64            `json:"memory_max_bytes"`
	MemoryOOMGroup            bool             `json:"memory_oom_group"`
	MemorySwapMaxBytes        int64            `json:"memory_swap_max_bytes"`
	ComputerTmpfsCeilingBytes int64            `json:"computer_tmpfs_ceiling_bytes"`
	LargestTmpfsTarget        string           `json:"largest_tmpfs_target,omitempty"`
	LargestTmpfsCeilingBytes  int64            `json:"largest_tmpfs_ceiling_bytes"`
	Warnings                  []ProfileWarning `json:"warnings"`
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
)

type RemovalResource struct {
	Class RemovalResourceClass `json:"class"`
	ID    string               `json:"id"`
}

type RemovalAttemptManifest struct {
	Authority       AttemptAuthority          `json:"authority"`
	HandoffVolume   string                    `json:"handoff_volume,omitempty"`
	ComputerStorage *ComputerStorageReference `json:"computer_storage,omitempty"`
	StorageOnly     bool                      `json:"storage_only,omitempty"`
	Resources       []RemovalResource         `json:"resources"`
}

type InventoryRemovalRequest struct {
	Removal         ManagedVolumeRemovalAuthority `json:"removal"`
	ComputerStorage *ComputerStorageReference     `json:"computer_storage,omitempty"`
}

type InventoryRemovalResponse struct {
	JobID             string                   `json:"job_id"`
	RemovalGeneration uint64                   `json:"removal_generation"`
	HelperSession     HelperSession            `json:"helper_session"`
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
		identity, err := DeterministicResourceIdentity(authority)
		if err != nil {
			return err
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
	Kind                   string `json:"kind"`
	ReceiptID              string `json:"receipt_id"`
	ComputerID             string `json:"computer_id"`
	StorageID              string `json:"storage_id"`
	StorageGeneration      int64  `json:"storage_generation"`
	OldJobID               string `json:"old_job_id"`
	StagingJobID           string `json:"staging_job_id"`
	NodeID                 string `json:"node_id"`
	RootInstanceID         string `json:"root_instance_id"`
	OperationRevision      int64  `json:"operation_revision"`
	OperationFence         string `json:"operation_fence"`
	TargetDigest           string `json:"target_digest"`
	PlatformOS             string `json:"platform_os"`
	PlatformArchitecture   string `json:"platform_architecture"`
	ImageUID               uint32 `json:"image_uid"`
	ImageGID               uint32 `json:"image_gid"`
	DiskRootUID            uint32 `json:"disk_root_uid"`
	DiskRootGID            uint32 `json:"disk_root_gid"`
	DetachmentReceiptID    string `json:"detachment_receipt_id"`
	DetachmentAttemptID    string `json:"detachment_attempt_id"`
	DetachmentFencingToken string `json:"detachment_fencing_token"`
	HelperGeneration       uint64 `json:"helper_generation"`
	FailureCode            string `json:"failure_code"`
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
	ComputerResetManifests  []string `json:"computer_reset_manifests"`
	ComputerQuarantines     []string `json:"computer_quarantines"`
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
