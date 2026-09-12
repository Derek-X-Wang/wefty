// Package ocicontrol defines the operator-only node-local control surface.
// It deliberately carries no Fabric address or credential: singular node
// commands resolve this socket before Fabric is initialized.
package ocicontrol

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const (
	InstalledConfigVersion = 1
	RunbookPath            = "docs/runbooks/oci-node.md"
)

type ConvergenceClass string

const (
	ConvergenceUnchanged        ConvergenceClass = "unchanged"
	ConvergenceLiveSafe         ConvergenceClass = "live_safe"
	ConvergenceRestartRequired  ConvergenceClass = "restart_required"
	ConvergenceRecreateRequired ConvergenceClass = "recreate_required"
)

func (class ConvergenceClass) Valid() bool {
	switch class {
	case ConvergenceUnchanged, ConvergenceLiveSafe, ConvergenceRestartRequired, ConvergenceRecreateRequired:
		return true
	default:
		return false
	}
}

type InstalledConfig struct {
	Version       int    `json:"version"`
	ControlSocket string `json:"control_socket"`
}

type SetupRequest struct {
	VMMemory     string `json:"vm_memory,omitempty"`
	VMCPUs       int    `json:"vm_cpus,omitempty"`
	VMDisk       string `json:"vm_disk,omitempty"`
	ApplyRestart bool   `json:"apply_restart,omitempty"`
	Recreate     bool   `json:"recreate,omitempty"`
}

type SetupResponse struct {
	Configured        bool                          `json:"configured"`
	Intent            lima.OCIIntent                `json:"intent"`
	Convergence       ConvergenceClass              `json:"convergence"`
	ProbePreloaded    bool                          `json:"probe_preloaded"`
	RestartApplied    bool                          `json:"restart_applied,omitempty"`
	RecreateApplied   bool                          `json:"recreate_applied,omitempty"`
	ReasonCode        contract.CapabilityReasonCode `json:"reason_code,omitempty"`
	MissingCapability string                        `json:"missing_capability,omitempty"`
	Runbook           string                        `json:"runbook,omitempty"`
}

type IntentMutationRequest struct {
	ExpectedRevision uint64 `json:"expected_revision"`
}

type IntentResponse struct {
	Intent              lima.OCIIntent `json:"intent"`
	CapabilityPublished bool           `json:"capability_published"`
	RuntimeQuiesced     bool           `json:"runtime_quiesced,omitempty"`
}

type LoadImageResponse struct {
	TopLevelDigest string                  `json:"top_level_digest"`
	PlatformDigest string                  `json:"platform_digest"`
	Evidence       ocihelper.ImageEvidence `json:"evidence"`
}

// RemovalsResponseVersion is bumped only when an existing field's meaning
// changes; new optional fields do not move it.
const RemovalsResponseVersion = 1

// RemovalRecord is a read-only projection of one durable runtime removal the
// agent is carrying. It exists because the removal proof -- the frozen
// resource manifest, the phase it has reached, the quiescence receipt, and the
// per-resource absence attestation -- was durable and complete but readable
// only from Go. An attended acceptance run, or an operator asking why a job is
// still removal_pending, had no way to see any of it.
//
// The record is the agent's own durable state rendered verbatim; reading it
// starts nothing, retries nothing, and changes no phase. A removal disappears
// from this surface once L1 has acknowledged its cleanup, because that is when
// the agent releases the record.
// RemovalQuiescence is the wire projection of the runtime's positive
// quiescence receipt. The receipt itself is deliberately left untagged: it is
// the agent's durable spool encoding, and tagging it to suit this operator
// document would rewrite bytes already on disk. Projecting it here also keeps
// the control surface's shape its own, so a change to the internal receipt
// cannot silently reshape what operators and acceptance receipts read.
type RemovalQuiescence struct {
	RuntimeQuiesced  bool   `json:"runtime_quiesced"`
	Evidence         string `json:"evidence,omitempty"`
	BootSessionID    string `json:"boot_session_id,omitempty"`
	SweepEpoch       string `json:"sweep_epoch,omitempty"`
	HelperGeneration uint64 `json:"helper_generation,omitempty"`
}

// QuiescenceProjection renders a runtime reap receipt on the wire.
func QuiescenceProjection(receipt workloadrunner.ReapReceipt) RemovalQuiescence {
	return RemovalQuiescence{
		RuntimeQuiesced: receipt.RuntimeQuiesced, Evidence: string(receipt.Evidence),
		BootSessionID: receipt.BootSessionID, SweepEpoch: receipt.SweepEpoch,
		HelperGeneration: receipt.HelperGeneration,
	}
}

type RemovalRecord struct {
	JobID             string                                    `json:"job_id"`
	RemovalGeneration uint64                                    `json:"removal_generation"`
	CleanupFence      string                                    `json:"cleanup_fence"`
	RootInstanceID    string                                    `json:"root_instance_id"`
	Phase             string                                    `json:"phase"`
	PreparedAt        time.Time                                 `json:"prepared_at"`
	QuiescedAt        *time.Time                                `json:"quiesced_at,omitempty"`
	AttestedAt        *time.Time                                `json:"attested_at,omitempty"`
	CompletedAt       *time.Time                                `json:"completed_at,omitempty"`
	RuntimeQuiescence RemovalQuiescence                         `json:"runtime_quiescence"`
	ResourceManifests []workloadrunner.RuntimeResourceManifest  `json:"resource_manifests"`
	Attestation       *workloadrunner.RuntimeRemovalAttestation `json:"absence_attestation,omitempty"`
}

type RemovalsResponse struct {
	Version  int             `json:"version"`
	Removals []RemovalRecord `json:"removals"`
}

const (
	ErrorInvalidRequest           contract.ErrorCode = contract.ErrorInvalidRequest
	ErrorIntentConflict           contract.ErrorCode = contract.ErrorStaleIntentRevision
	ErrorRuntimeUnavailable       contract.ErrorCode = "oci_runtime_unavailable"
	ErrorRuntimeQuiescenceFailed  contract.ErrorCode = "oci_runtime_quiescence_failed"
	ErrorSetupRequired            contract.ErrorCode = "oci_setup_required"
	ErrorTemplateRestartRequired  contract.ErrorCode = "template_restart_required"
	ErrorTemplateRecreateRequired contract.ErrorCode = "template_recreate_required"
	ErrorInternal                 contract.ErrorCode = contract.ErrorInternal
)

type ControlError struct {
	Code    contract.ErrorCode
	Status  int
	Message string
	Cause   error
}

func (err *ControlError) Error() string { return err.Message }
func (err *ControlError) Unwrap() error { return err.Cause }

func controlError(code contract.ErrorCode, status int, message string, cause error) error {
	return &ControlError{Code: code, Status: status, Message: message, Cause: cause}
}

func invalidRequest(message string, cause error) error {
	return controlError(ErrorInvalidRequest, http.StatusBadRequest, message, cause)
}

func runtimeUnavailable(message string, cause error) error {
	return controlError(ErrorRuntimeUnavailable, http.StatusServiceUnavailable, message, cause)
}

type Service interface {
	Doctor(context.Context) (DoctorResponse, error)
	Intent(context.Context) (lima.OCIIntent, error)
	Setup(context.Context, SetupRequest) (SetupResponse, error)
	Start(context.Context, IntentMutationRequest) (IntentResponse, error)
	Stop(context.Context, IntentMutationRequest) (IntentResponse, error)
	LoadImage(context.Context, io.Reader) (LoadImageResponse, error)
	Removals(context.Context) (RemovalsResponse, error)
}

type ServiceFuncs struct {
	DoctorFunc    func(context.Context) (DoctorResponse, error)
	IntentFunc    func(context.Context) (lima.OCIIntent, error)
	SetupFunc     func(context.Context, SetupRequest) (SetupResponse, error)
	StartFunc     func(context.Context, IntentMutationRequest) (IntentResponse, error)
	StopFunc      func(context.Context, IntentMutationRequest) (IntentResponse, error)
	LoadImageFunc func(context.Context, io.Reader) (LoadImageResponse, error)
	RemovalsFunc  func(context.Context) (RemovalsResponse, error)
}

func (service ServiceFuncs) Doctor(ctx context.Context) (DoctorResponse, error) {
	if service.DoctorFunc == nil {
		return DoctorResponse{}, runtimeUnavailable("OCI doctor is unavailable", nil)
	}
	return service.DoctorFunc(ctx)
}

func (service ServiceFuncs) Intent(ctx context.Context) (lima.OCIIntent, error) {
	if service.IntentFunc == nil {
		return lima.OCIIntent{}, runtimeUnavailable("OCI intent control is unavailable", nil)
	}
	return service.IntentFunc(ctx)
}

func (service ServiceFuncs) Setup(ctx context.Context, request SetupRequest) (SetupResponse, error) {
	if service.SetupFunc == nil {
		return SetupResponse{}, controlError(ErrorSetupRequired, http.StatusPreconditionFailed, "OCI setup control is unavailable", nil)
	}
	return service.SetupFunc(ctx, request)
}

func (service ServiceFuncs) Start(ctx context.Context, request IntentMutationRequest) (IntentResponse, error) {
	if service.StartFunc == nil {
		return IntentResponse{}, runtimeUnavailable("OCI start control is unavailable", nil)
	}
	return service.StartFunc(ctx, request)
}

func (service ServiceFuncs) Stop(ctx context.Context, request IntentMutationRequest) (IntentResponse, error) {
	if service.StopFunc == nil {
		return IntentResponse{}, runtimeUnavailable("OCI stop control is unavailable", nil)
	}
	return service.StopFunc(ctx, request)
}

func (service ServiceFuncs) LoadImage(ctx context.Context, archive io.Reader) (LoadImageResponse, error) {
	if service.LoadImageFunc == nil {
		return LoadImageResponse{}, runtimeUnavailable("OCI image loading is unavailable", nil)
	}
	return service.LoadImageFunc(ctx, archive)
}

func (service ServiceFuncs) Removals(ctx context.Context) (RemovalsResponse, error) {
	if service.RemovalsFunc == nil {
		return RemovalsResponse{}, runtimeUnavailable("OCI removal inspection is unavailable", nil)
	}
	return service.RemovalsFunc(ctx)
}

type Clock interface {
	Now() time.Time
}

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }
