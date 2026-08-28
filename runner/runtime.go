// Package runner defines the agent-owned seam between workload lifecycle
// policy and kind-specific execution mechanics.
package runner

import (
	"context"
	"errors"
	"net"
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

// RuntimeLossError carries positive adapter evidence that a reap operation
// lost the runtime generation whose resources it was trying to verify.
type RuntimeLossError struct {
	Generation RuntimeGeneration
	Err        error
}

func (err *RuntimeLossError) Error() string {
	if err == nil || err.Err == nil {
		return "workload runtime lost during quiescence verification"
	}
	return "workload runtime lost during quiescence verification: " + err.Err.Error()
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
	DiskBytes         int64
}

// RuntimeResourceManifest is the immutable, runtime-neutral inventory for one
// attempt. It contains only runtime-owned names and fenced identity; operator
// bind source paths are deliberately absent because removal must never traverse
// or delete them.
type RuntimeResourceManifest struct {
	Version                int              `json:"version"`
	RuntimeKind            string           `json:"runtime_kind"`
	NodeID                 string           `json:"node_id"`
	BootSessionID          string           `json:"boot_session_id"`
	JobID                  string           `json:"job_id"`
	AttemptID              string           `json:"attempt_id"`
	FencingToken           string           `json:"fencing_token"`
	WorkloadClass          string           `json:"workload_class"`
	RemovalGeneration      string           `json:"removal_generation"`
	LeaseID                string           `json:"lease_id"`
	TaskID                 string           `json:"task_id"`
	ContainerID            string           `json:"container_id"`
	SnapshotID             string           `json:"snapshot_id"`
	ShimID                 string           `json:"shim_id"`
	CgroupID               string           `json:"cgroup_id"`
	LogSegmentDirectory    string           `json:"log_segment_directory"`
	HandoffVolume          string           `json:"handoff_volume,omitempty"`
	ServiceDataVolume      string           `json:"service_data_volume,omitempty"`
	ServiceDataOwnerRecord string           `json:"service_data_owner_record,omitempty"`
	ComputerStorage        *ComputerStorage `json:"computer_storage,omitempty"`
}

// RuntimeRemovalManifestProvider freezes deterministic resource names before
// an adapter may create them. The agent persists the result before Run, then
// snapshots every attempt row when durable removal intent arrives.
type RuntimeRemovalManifestProvider interface {
	RemovalResourceManifest(Request) (RuntimeResourceManifest, error)
}

// AttemptEndpoint is an adapter-owned, exact-authority service endpoint. Its
// dial function never accepts an arbitrary address or port.
type AttemptEndpoint struct {
	Port uint16
	Dial func(context.Context) (net.Conn, error)
}

const (
	AttemptEndpointService = "service"
	AttemptEndpointView    = "view"
	AttemptEndpointControl = "control"
)

// OCIComputerControlRuntime exposes the attempt-fenced guest driving signal
// without exposing helper protocol or filesystem mechanics to the agent.
type OCIComputerControlRuntime interface {
	SetComputerControlState(context.Context, AttemptAuthority, bool) error
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
	OCIStarted       func(context.Context, OCIImageObservation) error
	OCIImageDeadline time.Time
	InitialDeadman   time.Duration
	// HostBridgeDial is set only for Lima's bind-failure reverse-tunnel path.
	// It dials the host-local run bridge and never accepts an arbitrary target.
	HostBridgeDial func(context.Context) (net.Conn, error)
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

// ErrPriorBootEvidenceUnavailable lets the agent try the other kind-specific
// reaper without treating absence of OCI sweep evidence as positive proof.
var ErrPriorBootEvidenceUnavailable = errors.New("prior-boot runtime evidence is unavailable")

// PriorBootReapRequest asks a runtime to verify the boot boundary for a
// service whose attempt was owned by an earlier agent boot.
type PriorBootReapRequest struct {
	NodeID               string
	JobID                string
	PriorBootSessionID   string
	CurrentBootSessionID string
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
	RemovalGeneration uint64
	CleanupFence      string
}

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
