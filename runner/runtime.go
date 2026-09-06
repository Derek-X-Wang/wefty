// Package runner defines the agent-owned seam between workload lifecycle
// policy and kind-specific execution mechanics.
package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// WorkflowBridgeBinding is a process-local listener binding. OCI on Lima can
// advertise the discovered guest-visible host gateway, or request the narrow
// helper fallback when binding that exact address fails.
type WorkflowBridgeBinding struct {
	Listener           net.Listener
	AdvertiseHost      string
	HostBridgeFallback bool
}

type WorkflowBridgeBinder interface {
	Bind(context.Context) (WorkflowBridgeBinding, error)
}

// IdlePolicy controls whether a workload is supervised for output inactivity.
// Its zero value preserves the one-shot hang guard.
type IdlePolicy uint8

const (
	MonitorIdle IdlePolicy = iota
	IgnoreIdle
)

// LifetimeBoundary describes the runtime ownership required by the agent. It
// is deliberately mechanical: workload class remains agent lifecycle policy
// and never selects or enters a runtime adapter.
type LifetimeBoundary uint8

const (
	CallerLifetime LifetimeBoundary = iota
	AgentBootLifetime
)

// AttemptAuthority is the runtime-neutral identity and fence for one attempt.
type AttemptAuthority struct {
	NodeID        string
	BootSessionID string
	JobID         string
	AttemptID     string
	FencingToken  string
	// WorkloadClass is carried only as immutable runtime-resource evidence. It
	// never selects the adapter or its mechanics.
	WorkloadClass     string
	RemovalGeneration string
}

// OCIImageObservation is the helper-observed identity that must be stored
// before authoritative OCI start can be acknowledged.
type OCIImageObservation struct {
	SubmittedReference     string
	TopLevelDigest         string
	TopLevelMediaType      string
	IndexDigest            *string
	PlatformManifestDigest string
	PlatformOS             string
	PlatformArchitecture   string
	PlatformVariant        string
	RuntimeHandler         string
	Snapshotter            string
}

// OCIImageBindingPin is the durable agent-local reconstruction record for a
// service binding's restart-critical image hold.
type OCIImageBindingPin struct {
	JobID                string
	Reference            string
	Digest               string
	PlatformOS           string
	PlatformArchitecture string
	PlatformVariant      string
	Snapshotter          string
}

type OCIImageBindingPinLedger interface {
	ListOCIImageBindingPins(context.Context) ([]OCIImageBindingPin, error)
	PutOCIImageBindingPin(context.Context, OCIImageBindingPin) (stored OCIImageBindingPin, created bool, err error)
	DeleteOCIImageBindingPin(context.Context, string) error
}

type OCIImageBindingProof func(context.Context, string) (bool, error)

type OCIImagePinReconciliationFailure struct {
	JobID   string
	Failure contract.SpawnFailure
}

// OCIImagePinRuntime is implemented by the OCI adapter without exposing
// helper/containerd types to the agent package.
type OCIImagePinRuntime interface {
	SetOCIImageBindingPinLedger(OCIImageBindingPinLedger)
	ReconcileOCIImagePins(context.Context, OCIImageBindingProof) ([]OCIImagePinReconciliationFailure, error)
	ReleaseOCIImageBindingPin(context.Context, string) error
}

// RuntimeGeneration identifies the adapter generation that observed a runtime
// loss without exposing kind-specific session types at the agent seam.
type RuntimeGeneration struct {
	InstanceID string
	Generation uint64
}

// RuntimeLossError carries positive adapter evidence that an operation lost
// the runtime generation whose resources it was trying to use or verify.
type RuntimeLossError struct {
	Generation RuntimeGeneration
	Err        error
}

func (err *RuntimeLossError) Error() string {
	if err == nil || err.Err == nil {
		return "workload runtime lost during operation"
	}
	return "workload runtime lost during operation: " + err.Err.Error()
}

func (err *RuntimeLossError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

// OCIObservationRefusal distinguishes an authoritative L1 protocol refusal
// from transport or server unavailability while persisting pre-Run evidence.
type OCIObservationRefusal struct{ Err error }

func (refusal *OCIObservationRefusal) Error() string { return refusal.Err.Error() }
func (refusal *OCIObservationRefusal) Unwrap() error { return refusal.Err }

// ManagedResources is an opaque agent-owned handle. Adapters must not infer
// workload class or depend on host paths hidden behind the handle.
type ManagedResources any

// ManagedVolumeKind names an agent-owned lifecycle requirement without
// exposing helper paths or runtime mechanics.
type ManagedVolumeKind string

const (
	ManagedVolumeHandoff      ManagedVolumeKind = "handoff"
	ManagedVolumeServiceData  ManagedVolumeKind = "service_data"
	ManagedVolumeComputerDisk ManagedVolumeKind = "computer_disk"
)

// ManagedVolume identifies durable runtime-managed state. OwnerKey is stable
// across attempts and reruns and is opaque to the runtime.
type ManagedVolume struct {
	Kind            ManagedVolumeKind
	OwnerKey        string
	ComputerStorage *ComputerStorage
}

// ComputerStorage identifies one non-transferable durable Storage generation.
// DiskBytes is the fully allocated budget declared by the immutable Job.
type ComputerStorage struct {
	ComputerID        string
	StorageID         string
	StorageGeneration int64
	IntentRevision    int64
	DiskBytes         int64
	Chown             bool
}

// RuntimeResourceManifest is the immutable, runtime-neutral inventory for one
// attempt. It contains only runtime-owned names and fenced identity; operator
// bind source paths are deliberately absent because removal must never traverse
// or delete them.
type RuntimeResourceManifest struct {
	Version                int                                         `json:"version"`
	RuntimeKind            string                                      `json:"runtime_kind"`
	NodeID                 string                                      `json:"node_id"`
	BootSessionID          string                                      `json:"boot_session_id"`
	JobID                  string                                      `json:"job_id"`
	AttemptID              string                                      `json:"attempt_id"`
	FencingToken           string                                      `json:"fencing_token"`
	WorkloadClass          string                                      `json:"workload_class"`
	RemovalGeneration      string                                      `json:"removal_generation"`
	LeaseID                string                                      `json:"lease_id"`
	TaskID                 string                                      `json:"task_id"`
	ContainerID            string                                      `json:"container_id"`
	SnapshotID             string                                      `json:"snapshot_id"`
	ShimID                 string                                      `json:"shim_id"`
	CgroupID               string                                      `json:"cgroup_id"`
	LogSegmentDirectory    string                                      `json:"log_segment_directory"`
	HandoffVolume          string                                      `json:"handoff_volume,omitempty"`
	ServiceDataVolume      string                                      `json:"service_data_volume,omitempty"`
	ServiceDataOwnerRecord string                                      `json:"service_data_owner_record,omitempty"`
	ComputerStorage        *ComputerStorage                            `json:"computer_storage,omitempty"`
	StoragePreparation     *contract.ComputerStoragePreparationWitness `json:"storage_preparation,omitempty"`
	// StorageOnly reuses the shared removal manifest for a reset predecessor
	// after successor publication. No synthetic runtime rows are invented.
	StorageOnly bool `json:"storage_only,omitempty"`
	// StorageAbsent is helper-observed evidence that an exact retired
	// generation root was already deleted before legacy reconstruction.
	StorageAbsent bool `json:"storage_absent,omitempty"`
}

// RuntimeRemovalManifestProvider freezes deterministic resource names before
// an adapter may create them. The agent persists the result before Run, then
// snapshots every attempt row when durable removal intent arrives.
type RuntimeRemovalManifestProvider interface {
	RemovalResourceManifest(Request) (RuntimeResourceManifest, error)
}

// RuntimeRemovalResourceClass names one independently inventoried residue
// class. The list is intentionally data-driven so kind-specific runtimes can
// extend their removal manifest without widening the agent lifecycle seam.
type RuntimeRemovalResourceClass string

const (
	RuntimeRemovalLease                  RuntimeRemovalResourceClass = "lease"
	RuntimeRemovalSnapshot               RuntimeRemovalResourceClass = "snapshot"
	RuntimeRemovalContainer              RuntimeRemovalResourceClass = "container"
	RuntimeRemovalTask                   RuntimeRemovalResourceClass = "task"
	RuntimeRemovalShim                   RuntimeRemovalResourceClass = "shim"
	RuntimeRemovalCgroup                 RuntimeRemovalResourceClass = "cgroup"
	RuntimeRemovalLogSegments            RuntimeRemovalResourceClass = "log_segments"
	RuntimeRemovalHandoffVolume          RuntimeRemovalResourceClass = "handoff_volume"
	RuntimeRemovalServiceData            RuntimeRemovalResourceClass = "service_data"
	RuntimeRemovalServiceDataRecord      RuntimeRemovalResourceClass = "service_data_owner_record"
	RuntimeRemovalComputerDiskImage      RuntimeRemovalResourceClass = "computer_disk_image"
	RuntimeRemovalComputerDiskAllocation RuntimeRemovalResourceClass = "computer_disk_allocation"
	RuntimeRemovalComputerDiskQuota      RuntimeRemovalResourceClass = "computer_disk_quota"
	RuntimeRemovalComputerDiskManifest   RuntimeRemovalResourceClass = "computer_disk_manifest"
	RuntimeRemovalComputerDiskMount      RuntimeRemovalResourceClass = "computer_disk_mount"
	RuntimeRemovalComputerDiskLoop       RuntimeRemovalResourceClass = "computer_disk_loop"
	RuntimeRemovalComputerAttachment     RuntimeRemovalResourceClass = "computer_attachment"
	RuntimeRemovalComputerResetManifest  RuntimeRemovalResourceClass = "computer_reset_manifest"
	RuntimeRemovalComputerQuarantine     RuntimeRemovalResourceClass = "computer_quarantine"
)

type RuntimeRemovalResource struct {
	Class RuntimeRemovalResourceClass `json:"class"`
	ID    string                      `json:"id"`
}

// RemovalResources projects the manifest into independently asserted rows.
// New runtime-owned classes extend this one projection and the helper's
// corresponding inventory registry; removal orchestration stays unchanged.
func (manifest RuntimeResourceManifest) RemovalResources() []RuntimeRemovalResource {
	resources := []RuntimeRemovalResource{}
	if !manifest.StorageOnly {
		resources = append(resources,
			RuntimeRemovalResource{Class: RuntimeRemovalLease, ID: manifest.LeaseID},
			RuntimeRemovalResource{Class: RuntimeRemovalSnapshot, ID: manifest.SnapshotID},
			RuntimeRemovalResource{Class: RuntimeRemovalContainer, ID: manifest.ContainerID},
			RuntimeRemovalResource{Class: RuntimeRemovalTask, ID: manifest.TaskID},
			RuntimeRemovalResource{Class: RuntimeRemovalShim, ID: manifest.ShimID},
			RuntimeRemovalResource{Class: RuntimeRemovalCgroup, ID: manifest.CgroupID},
			RuntimeRemovalResource{Class: RuntimeRemovalLogSegments, ID: manifest.LogSegmentDirectory},
			RuntimeRemovalResource{Class: RuntimeRemovalHandoffVolume, ID: manifest.HandoffVolume},
			RuntimeRemovalResource{Class: RuntimeRemovalServiceData, ID: manifest.ServiceDataVolume},
			RuntimeRemovalResource{Class: RuntimeRemovalServiceDataRecord, ID: manifest.ServiceDataOwnerRecord},
		)
	}
	if manifest.ComputerStorage != nil {
		name := deterministicComputerDiskRemovalName(*manifest.ComputerStorage)
		resources = append(resources,
			RuntimeRemovalResource{Class: RuntimeRemovalComputerDiskImage, ID: name},
			RuntimeRemovalResource{Class: RuntimeRemovalComputerDiskAllocation, ID: name},
			RuntimeRemovalResource{Class: RuntimeRemovalComputerDiskQuota, ID: name},
			RuntimeRemovalResource{Class: RuntimeRemovalComputerDiskManifest, ID: name},
			RuntimeRemovalResource{Class: RuntimeRemovalComputerDiskMount, ID: name},
			RuntimeRemovalResource{Class: RuntimeRemovalComputerDiskLoop, ID: name},
			RuntimeRemovalResource{Class: RuntimeRemovalComputerAttachment, ID: name},
			RuntimeRemovalResource{Class: RuntimeRemovalComputerResetManifest, ID: name},
			RuntimeRemovalResource{Class: RuntimeRemovalComputerQuarantine, ID: name},
		)
	}
	return slices.DeleteFunc(resources, func(resource RuntimeRemovalResource) bool { return resource.ID == "" })
}

func deterministicComputerDiskRemovalName(storage ComputerStorage) string {
	if strings.TrimSpace(storage.ComputerID) == "" || strings.TrimSpace(storage.StorageID) == "" || storage.StorageGeneration < 1 {
		return ""
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{
		"computer-disk", storage.ComputerID, storage.StorageID, strconv.FormatInt(storage.StorageGeneration, 10),
	}, "\x00")))
	return "wefty-computer-disk-" + hex.EncodeToString(digest[:16])
}

type RuntimeRemovalProofRequest struct {
	NodeID            string
	BootSessionID     string
	JobID             string
	RemovalGeneration uint64
	CleanupFence      string
	RootInstanceID    string
	ComputerStorage   *ComputerStorage
	Attempts          []RuntimeResourceManifest
}

type RuntimeRemovalAssertion struct {
	Class  RuntimeRemovalResourceClass `json:"class"`
	ID     string                      `json:"id"`
	Absent bool                        `json:"absent"`
}

// RuntimeRemovalAttestation is the assertion-derived post-delete receipt.
// Attempts may be helper-reconstructed for legacy services that predate the
// agent-local manifest ledger.
type RuntimeRemovalAttestation struct {
	Version           int                       `json:"version"`
	JobID             string                    `json:"job_id"`
	RemovalGeneration uint64                    `json:"removal_generation"`
	RuntimeInstanceID string                    `json:"runtime_instance_id"`
	RuntimeGeneration uint64                    `json:"runtime_generation"`
	Attempts          []RuntimeResourceManifest `json:"attempts"`
	Assertions        []RuntimeRemovalAssertion `json:"assertions"`
}

// RuntimeRemovalProofRuntime separates idempotent job-lifetime data deletion
// from the assertion-derived absence receipt so a crash at that production
// boundary safely retries deletion without inventing proof. It is optional
// because process workloads remain under managedroot rather than the OCI
// helper.
type RuntimeRemovalProofRuntime interface {
	ReconstructRuntimeRemoval(context.Context, RuntimeRemovalProofRequest) ([]RuntimeResourceManifest, error)
	DeleteRuntimeRemovalData(context.Context, RuntimeRemovalProofRequest) error
	AttestRuntimeRemoval(context.Context, RuntimeRemovalProofRequest) (RuntimeRemovalAttestation, error)
}

// AttemptEndpoint is an adapter-owned, exact-authority service endpoint. Its
// dial function never accepts an arbitrary address or port.
type AttemptEndpoint struct {
	Port uint16
	Dial func(context.Context) (net.Conn, error)
}

// OCILogSealObservation preserves the helper's per-stream EOF boundary for
// acceptance and diagnostics without treating it as an application log event.
type OCILogSealObservation struct {
	Stream   contract.LogStream
	Complete bool
	Reason   string
}

const (
	AttemptEndpointService = "service"
	AttemptEndpointView    = contract.ComputerDisplayEndpointView
	AttemptEndpointControl = contract.ComputerDisplayEndpointControl
)

// OCIComputerControlRuntime exposes the attempt-fenced guest driving signal
// without exposing helper protocol or filesystem mechanics to the agent.
type OCIComputerControlRuntime interface {
	SetComputerControlState(context.Context, AttemptAuthority, bool) error
	SetComputerSubmission(context.Context, AttemptAuthority, string, string) error
}

// Request contains the mechanics needed by any workload runtime. Kind and
// class are intentionally absent: kind selected the adapter already, while
// class-specific lifecycle policy was compiled into these fields by the agent.
type Request struct {
	Authority        AttemptAuthority
	RuntimeHandler   string
	Execution        contract.ExecutionSpec
	Limits           *contract.JobLimits
	ManagedResources ManagedResources
	ManagedVolumes   []ManagedVolume
	LifetimeBoundary LifetimeBoundary
	// TerminationGrace is the agent-compiled interval between TERM and KILL
	// for runtimes whose lifetime is bound to the agent boot. It is ignored
	// for caller-lifetime work.
	TerminationGrace time.Duration
	IdlePolicy       IdlePolicy
	CompletionSignal <-chan struct{}
	Started          func()
	// OCIImagePulling exposes the agent-local preparation phase. It never
	// mutates L1 state or starts payload/runtime clocks.
	OCIImagePulling func()
	// OCIImageReady returns the local observer to starting while the helper
	// constructs the runtime. L1 still remains Claimed.
	OCIImageReady func()
	// OCIHelperAdmitted reports that helper Run returned authoritative Started
	// evidence, every Started validation succeeded, and the exact attempt is now
	// eligible for deadman renewals in the named helper generation.
	OCIHelperAdmitted func(RuntimeGeneration) error
	// OCIRuntimeUnavailable reports helper/session or engine loss to the agent.
	// The agent performs recovery later under its finalization context. L1
	// transport failures must not call this hook.
	OCIRuntimeUnavailable func(RuntimeGeneration)
	// OCIImageResolved persists the helper-observed immutable identity before
	// runtime resource creation. A later attempt receives the same identity
	// and therefore never resolves the submitted tag again.
	OCIImageResolved func(context.Context, OCIImageObservation) error
	// OCIStarted replays helper-observed identity after task start and then
	// performs the fenced L1 Started mutation. The helper adapter must reap the
	// task when this callback fails.
	// OCIStartedAt carries the helper-captured task Start edge before any L1
	// round trip so lifecycle clocks retain the original budget.
	OCIStartedAt func(time.Time)
	OCIStarted   func(context.Context, OCIImageObservation) error
	// OCILogSealObserved reports the helper's terminal record for each stream.
	// ProcessResult.LogEvidenceIncomplete remains the durable aggregate fact.
	OCILogSealObserved func(OCILogSealObservation)
	OCIImageDeadline   time.Time
	InitialDeadman     time.Duration
	// HostBridgeDial dials the host-local run bridge and never accepts an
	// arbitrary target. Computers always use it so their private network
	// namespace does not widen the orchestrator channel; ordinary OCI uses it
	// only for Lima's bind-failure reverse-tunnel path.
	HostBridgeDial func(context.Context) (net.Conn, error)
	// HostBridgeFallbackActive selects the helper listener as the start-time
	// endpoint. Computer attempts always select their private-namespace bridge;
	// ordinary OCI uses it only for the constrained Lima fallback.
	HostBridgeFallbackActive bool
	// HostBridgeEndpointReady reports the helper-owned guest loopback endpoint.
	// It does not make the endpoint visible to the workload; the Computer
	// authority lifecycle publishes it separately.
	HostBridgeEndpointReady func(string) error
	// AttemptEndpoints asks an OCI runtime to allocate a bounded named endpoint
	// set. AttemptEndpointReady transfers each exact-authority dialer to the
	// local service front-door seam without exposing guest networking.
	AttemptEndpoints     []string
	AttemptEndpointReady func(string, AttemptEndpoint) error
	// TODO(#147): replace this process TCP backend with an adapter-supplied,
	// opaque dial endpoint.
	ServiceAddress   string
	ReadinessChanged func(startupSatisfied, ready bool)
}

// Result is the runtime-neutral structured outcome of one workload.
type Result struct {
	Outcome contract.ProcessResult
}

// Admission is the adapter-prepared request and its owned cleanup. Preflight
// runs before the agent acquires managed resources, ports, bridges, or sinks.
type Admission struct {
	Request Request
	Release func()
}

// ReapRequest identifies the runtime resources that must be absent before an
// attempt may finish or a removal may be acknowledged.
type ReapRequest struct {
	Authority        AttemptAuthority
	ManagedResources ManagedResources
}

// ReapEvidence names the authority behind a positive quiescence receipt.
type ReapEvidence string

const (
	ReapEvidenceAttempt           ReapEvidence = "attempt"
	ReapEvidenceNoRuntime         ReapEvidence = "no_runtime_resources"
	ReapEvidenceOCISweep          ReapEvidence = "oci_sweep"
	ReapEvidencePriorBootGuardian ReapEvidence = "prior_boot_guardian"
	ReapEvidencePriorBootOCISweep ReapEvidence = "prior_boot_oci_sweep"
	ReapEvidenceOCIRuntimeSweep   ReapEvidence = "oci_runtime_sweep"
)

// ReapReceipt is positive evidence that runtime-owned workload state is gone.
type ReapReceipt struct {
	RuntimeQuiesced  bool
	Evidence         ReapEvidence
	BootSessionID    string
	SweepEpoch       string
	HelperGeneration uint64
}

// ErrPriorBootEvidenceUnavailable reports that the removal intent's exact
// kind-specific runtime cannot supply prior-boot quiescence evidence.
var ErrPriorBootEvidenceUnavailable = errors.New("prior-boot runtime evidence is unavailable")

// PriorBootReapRequest asks a runtime to verify the boot boundary for a
// service whose attempt was owned by an earlier agent boot.
type PriorBootReapRequest struct {
	NodeID               string
	JobID                string
	PriorBootSessionID   string
	CurrentBootSessionID string
	AttemptID            string
	FencingToken         string
	WorkloadClass        string
	RemovalGeneration    string
}

// PriorBootReaper is optional because not every runtime has boot-barrier
// evidence. Process derives it from Guardian disconnect; OCI will derive it
// from the helper boot sweep.
type PriorBootReaper interface {
	ReapPriorBoot(context.Context, PriorBootReapRequest) (ReapReceipt, error)
}

// ManagedVolumeFinalizer deletes durable runtime-managed state only after the
// agent has received authoritative acceptance of a successful completion.
type ManagedVolumeFinalizer interface {
	FinalizeManagedVolumes(context.Context, ManagedVolumeFinalizationRequest) error
}

type ManagedVolumeFinalizationRequest struct {
	Authority AttemptAuthority
	Volumes   []ManagedVolume
	Removal   *ManagedVolumeRemovalAuthority
}

type ManagedVolumeRemovalAuthority struct {
	NodeID            string
	BootSessionID     string
	JobID             string
	PriorJobID        string
	RemovalGeneration uint64
	CleanupFence      string
}

type ManagedVolumeQuarantineReceipt struct {
	Kind            string
	ReceiptID       string
	VolumeKind      ManagedVolumeKind
	ComputerStorage ComputerStorage
	Removal         ManagedVolumeRemovalAuthority
	FailureReason   string
	Attempts        int
}

type ManagedVolumeCleanupQuarantinedError struct {
	Receipt ManagedVolumeQuarantineReceipt
}

func (failure *ManagedVolumeCleanupQuarantinedError) Error() string {
	if failure == nil {
		return "managed volume cleanup was quarantined"
	}
	return fmt.Sprintf("managed volume cleanup was quarantined after %d attempts (receipt %s)", failure.Receipt.Attempts, failure.Receipt.ReceiptID)
}

// ComputerStorageResetter prepares one detached Computer Storage successor.
// It fences the predecessor and fully allocates/verifies the successor without
// deleting either generation; L1 publishes before shared removal retires N.
type ComputerStorageResetter interface {
	ResetComputerStorage(context.Context, ComputerStorageResetRequest) (ComputerStorageResetReceipt, error)
}

type ComputerStorageResetRequest struct {
	Storage        ComputerStorage
	NewGeneration  int64
	NodeID         string
	BootSessionID  string
	RootInstanceID string
	JobID          string
	PriorJobID     string
	IntentRevision int64
	CleanupFence   string
}

type ComputerStorageResetReceipt = contract.ComputerStorageResetReceipt

type ComputerStorageGrower interface {
	GrowComputerStorage(context.Context, ComputerStorageGrowRequest) (ComputerStorageGrowReceipt, error)
}

type ComputerStorageGrowRequest struct {
	Storage           ComputerStorage
	NewDiskBytes      int64
	NodeID            string
	BootSessionID     string
	RootInstanceID    string
	JobID             string
	OperationRevision int64
	OperationFence    string
}

type ComputerStorageGrowReceipt = contract.ComputerStorageGrowReceipt

// ComputerReimagePreflighter verifies the target image and detached disk
// together before L1 may transfer projection authority.
type ComputerReimagePreflighter interface {
	PreflightComputerReimage(context.Context, ComputerReimagePreflightRequest) (ComputerReimagePreflightReceipt, error)
}

type ComputerReimagePreflightRequest struct {
	Storage           ComputerStorage
	OldJobID          string
	StagingJobID      string
	NodeID            string
	BootSessionID     string
	RootInstanceID    string
	OperationRevision int64
	OperationFence    string
	TargetReference   string
	TargetDigest      string
	Chown             bool
}

type ComputerReimagePreflightReceipt struct {
	Kind                      string
	ReceiptID                 string
	ComputerID                string
	StorageID                 string
	StorageGeneration         int64
	OldJobID                  string
	StagingJobID              string
	NodeID                    string
	RootInstanceID            string
	OperationRevision         int64
	OperationFence            string
	TargetDigest              string
	PlatformOS                string
	PlatformArchitecture      string
	ImageUID                  uint32
	ImageGID                  uint32
	DiskRootUID               uint32
	DiskRootGID               uint32
	StorageEvidenceKind       string
	DetachmentReceiptID       string
	DetachmentAttemptID       string
	DetachmentFencingToken    string
	ResetPreparationReceiptID string
	HelperGeneration          uint64
	FailureCode               string
	FailureStage              string
	FailureReason             string
}

// ComputerBackupper owns the physical source-node copy mechanics behind the
// runtime-neutral agent seam. L1 owns cap, intent, resume, and pruning policy.
type ComputerBackupper interface {
	CreateComputerBackup(context.Context, ComputerBackupRequest) (ComputerBackupCopyReceipt, error)
	DeleteComputerBackupCopy(context.Context, ComputerBackupCopyRemovalRequest) (ComputerBackupCopyRemovalReceipt, error)
}

type ComputerBackupRequest struct {
	BackupID          string
	CopyID            string
	Storage           ComputerStorage
	NodeID            string
	BootSessionID     string
	RootInstanceID    string
	JobID             string
	PriorJobID        string
	OperationRevision int64
	CleanupFence      string
}

type ComputerBackupCopyReceipt = contract.ComputerBackupCopyReceipt

type ComputerBackupCopyRemovalRequest struct {
	BackupID          string
	CopyID            string
	Storage           ComputerStorage
	NodeID            string
	BootSessionID     string
	RootInstanceID    string
	OperationRevision int64
	CleanupFence      string
	Superseded        bool
}

type ComputerBackupCopyRemovalReceipt = contract.ComputerBackupCopyRemovalReceipt

// ComputerStorageCopier materializes one detached restore or clone destination
// from an already-published source-node Backup copy. L1 owns publication,
// provenance, authority rotation, and predecessor retirement.
type ComputerStorageCopier interface {
	CopyComputerStorage(context.Context, ComputerStorageCopyRequest) (ComputerStorageCopyReceipt, error)
}

type ComputerStorageCopyRequest struct {
	Operation         string
	BackupID          string
	CopyID            string
	SourceComputerID  string
	SourceStorageID   string
	SourceGeneration  int64
	SourceSize        int64
	SourceDigest      string
	ExportID          string
	ExternalPath      string
	ManifestDigest    string
	Destination       ComputerStorage
	NodeID            string
	BootSessionID     string
	RootInstanceID    string
	JobID             string
	OperationRevision int64
	CleanupFence      string
}

type ComputerStorageCopyReceipt = contract.ComputerStorageCopyReceipt

const (
	ComputerStoragePreparationInterrupted    = "computer_storage_preparation_interrupted"
	ComputerStoragePreparationResumeDeferred = "computer_storage_resume_deferred"
	ComputerStoragePreparationQuarantined    = "computer_storage_quarantined"
)

// ComputerStoragePreparationOutcome is helper-authored evidence that a
// detached destination cannot currently be prepared. Unlike an ordinary copy
// error, these closed outcomes are safe for the agent to persist at L1.
type ComputerStoragePreparationOutcome struct {
	Code             string
	Storage          ComputerStorage
	HelperGeneration uint64
	SweepEpoch       string
	DiskName         string
	Operation        string
	Reason           string
	DeferredReason   string
	Attempts         int
	FirstDeferredAt  *time.Time
	PayloadDroppedAt string
	RecordedAt       time.Time
}

type ComputerStoragePreparationError struct {
	Outcome ComputerStoragePreparationOutcome
}

func (err *ComputerStoragePreparationError) Error() string {
	return "Computer Storage preparation reported " + err.Outcome.Code
}

// ComputerCustodyExporter transfers one already-published Backup copy beyond
// the managed root. L1 records the permanent custody event before calling it.
type ComputerCustodyExporter interface {
	ExportComputerCustody(context.Context, ComputerCustodyExportRequest) (ComputerCustodyExportReceipt, error)
}

type ComputerCustodyExportRequest struct {
	ExportID          string
	BackupID          string
	CopyID            string
	Storage           ComputerStorage
	SourceSize        int64
	SourceDigest      string
	ExternalPath      string
	NodeID            string
	BootSessionID     string
	RootInstanceID    string
	OperationRevision int64
	CustodyFence      string
	JobSpec           contract.JobSpec
	JobSpecHash       string
}

type ComputerCustodyExportReceipt = contract.ComputerCustodyExportReceipt

// OutputSink receives raw output events. Calls may be concurrent across
// streams, so implementations must provide any required synchronization.
type OutputSink interface {
	WriteOutput(context.Context, contract.LogEvent) error
}

// OutputSinkFunc adapts a function into an OutputSink.
type OutputSinkFunc func(context.Context, contract.LogEvent) error

func (function OutputSinkFunc) WriteOutput(ctx context.Context, event contract.LogEvent) error {
	return function(ctx, event)
}

// WorkloadRuntime executes one open workload kind and positively verifies
// that its runtime-owned resources have been reaped.
type WorkloadRuntime interface {
	Preflight(context.Context, Request) (Admission, Result, error)
	Run(context.Context, Request, OutputSink) (Result, error)
	ReapAndVerify(context.Context, ReapRequest) (ReapReceipt, error)
}
