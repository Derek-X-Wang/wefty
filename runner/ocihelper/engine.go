package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

const DefaultContainerdAddress = "/run/containerd/containerd.sock"

// NativeEngineConfig contains only host-side helper configuration. The agent
// never supplies these values over the helper protocol.
type NativeEngineConfig struct {
	Address                string
	LoggerExecutable       string
	RuntimeRoot            string
	ContainerdStateRoot    string
	CgroupRoot             string
	ResolverPath           string
	HostsPath              string
	AllowedMountRoots      []string
	RuncExecutable         string
	HostMountRoot          string
	GuestMountRoot         string
	AttemptPortMin         uint16
	AttemptPortMax         uint16
	AttemptPortBindTimeout time.Duration
	LogSealTimeout         time.Duration
	HandoffRetention       time.Duration
	MemoryCapacityBytes    int64
	MemoryReserveBytes     int64
	Clock                  Clock
}

// GuardianReaper preserves the helper deadman's signal initiator when the
// concrete engine can obtain a real task signal-delivery acknowledgement.
type GuardianReaper interface {
	ReapAttemptAsGuardian(context.Context, AttemptAuthority) error
}

type ManagedVolumeEngine interface {
	DeleteManagedVolume(context.Context, DeleteManagedVolumeRequest) (DeleteManagedVolumeResponse, error)
}

type ComputerControlEngine interface {
	SetComputerControlState(context.Context, SetComputerControlStateRequest) error
	SetComputerToken(context.Context, SetComputerTokenRequest) error
}

type RemovalProofEngine interface {
	AttestRemoval(context.Context, AttestRemovalRequest) (AttestRemovalResponse, error)
}

type RemovalInventoryEngine interface {
	InventoryRemoval(context.Context, InventoryRemovalRequest) (InventoryRemovalResponse, error)
}

type ComputerStorageResetEngine interface {
	ResetComputerStorage(context.Context, ResetComputerStorageRequest) (ResetComputerStorageResponse, error)
}

type ComputerStorageGrowEngine interface {
	GrowComputerStorage(context.Context, GrowComputerStorageRequest) (GrowComputerStorageResponse, error)
}

type ComputerReimagePreflightEngine interface {
	PreflightComputerReimage(context.Context, PreflightComputerReimageRequest) (PreflightComputerReimageResponse, error)
}

type ComputerBackupEngine interface {
	CreateComputerBackup(context.Context, CreateComputerBackupRequest) (CreateComputerBackupResponse, error)
	DeleteComputerBackupCopy(context.Context, DeleteComputerBackupCopyRequest) (DeleteComputerBackupCopyResponse, error)
}

type ComputerStorageCopyEngine interface {
	CopyComputerStorage(context.Context, CopyComputerStorageRequest) (CopyComputerStorageResponse, error)
}

type ComputerCustodyExportEngine interface {
	ExportComputerCustody(context.Context, ExportComputerCustodyRequest) (ExportComputerCustodyResponse, error)
}

// Engine is the helper-internal mechanics seam. No containerd request or type
// crosses RPC.
type Engine interface {
	EnsureImage(context.Context, EnsureImageRequest, io.Reader, func(EnsureImageEvent) error) error
	Run(context.Context, RunRequest) (RunResponse, error)
	Signal(context.Context, SignalRequest) error
	Watch(context.Context, WatchRequest, func(WatchEvent) error) error
	Delete(context.Context, DeleteRequest) (DeleteResponse, error)
	Verify(context.Context, VerifyRequest) (VerifyResponse, error)
	Sweep(context.Context, SweepRequest) (SweepResponse, error)
	DialAttemptPort(context.Context, DialAttemptPortRequest, io.ReadWriteCloser) error
	DialHostBridge(context.Context, DialHostBridgeRequest, io.ReadWriteCloser) error
	ReapAttempt(context.Context, AttemptAuthority) error
	ReapSession(context.Context, SessionIdentity) error
}

type ImageCacheEngine interface {
	ReconcileImagePins(context.Context, ReconcileImagePinsRequest) (ReconcileImagePinsResponse, error)
	ReleaseImagePin(context.Context, ReleaseImagePinRequest) error
	ReleaseAttemptImagePin(context.Context, ReleaseAttemptImagePinRequest) error
	ImageCacheStatus(context.Context) (ImageCacheStatus, error)
}

// DoctorEngine reads already-configured runtime mechanics. Implementations
// must not pull images, create tasks, sweep resources, or alter cache policy.
type DoctorEngine interface {
	DoctorStatus(context.Context) (DoctorStatus, error)
}

// UnavailableEngine keeps the private helper mode fail-closed on unsupported
// hosts. Tests also use it when no real engine is required.
type UnavailableEngine struct{}

var errEngineUnavailable = errors.New("OCI engine adapter is not installed")

// ImageUnavailableError separates missing/corrupt/unpacked local content from
// loss of the containerd engine.
type ImageUnavailableError struct{ err error }

func (failure *ImageUnavailableError) Error() string {
	return fmt.Sprintf("OCI image unavailable: %v", failure.err)
}
func (failure *ImageUnavailableError) Unwrap() error { return failure.err }

// ImageMechanicsError carries only a sanitized observation across the helper
// boundary. It deliberately contains no retry or L1 classification policy.
type ImageMechanicsError struct {
	Fact ImageFailureFact
	err  error
}

func (failure *ImageMechanicsError) Error() string {
	return fmt.Sprintf("OCI image delivery mechanics %s: %v", failure.Fact.Kind, failure.err)
}
func (failure *ImageMechanicsError) Unwrap() error { return failure.err }

func NewImageMechanicsError(fact ImageFailureFact, err error) error {
	return &ImageMechanicsError{Fact: fact, err: err}
}

func imageMechanicsError(kind ImageFailureKind, digest string, err error) error {
	fact := ImageFailureFact{Kind: kind, TopLevelDigest: digest}
	var inspection *ociArchiveInspectionError
	if errors.As(err, &inspection) {
		fact.Reason = inspection.Error()
	}
	return &ImageMechanicsError{Fact: fact, err: err}
}

func (UnavailableEngine) EnsureImage(context.Context, EnsureImageRequest, io.Reader, func(EnsureImageEvent) error) error {
	return &ImageMechanicsError{Fact: ImageFailureFact{Kind: ImageFailureEngineLoss}, err: errEngineUnavailable}
}
func (UnavailableEngine) ReconcileImagePins(context.Context, ReconcileImagePinsRequest) (ReconcileImagePinsResponse, error) {
	return ReconcileImagePinsResponse{}, errEngineUnavailable
}
func (UnavailableEngine) ReleaseImagePin(context.Context, ReleaseImagePinRequest) error {
	return errEngineUnavailable
}
func (UnavailableEngine) ReleaseAttemptImagePin(context.Context, ReleaseAttemptImagePinRequest) error {
	return errEngineUnavailable
}
func (UnavailableEngine) ImageCacheStatus(context.Context) (ImageCacheStatus, error) {
	return ImageCacheStatus{}, errEngineUnavailable
}
func (UnavailableEngine) Run(context.Context, RunRequest) (RunResponse, error) {
	return RunResponse{}, errEngineUnavailable
}
func (UnavailableEngine) Signal(context.Context, SignalRequest) error { return errEngineUnavailable }
func (UnavailableEngine) Watch(context.Context, WatchRequest, func(WatchEvent) error) error {
	return errEngineUnavailable
}
func (UnavailableEngine) Delete(context.Context, DeleteRequest) (DeleteResponse, error) {
	return DeleteResponse{}, errEngineUnavailable
}
func (UnavailableEngine) Verify(context.Context, VerifyRequest) (VerifyResponse, error) {
	return VerifyResponse{}, errEngineUnavailable
}
func (UnavailableEngine) Sweep(context.Context, SweepRequest) (SweepResponse, error) {
	return SweepResponse{}, errEngineUnavailable
}
func (UnavailableEngine) DialAttemptPort(context.Context, DialAttemptPortRequest, io.ReadWriteCloser) error {
	return errEngineUnavailable
}
func (UnavailableEngine) DialHostBridge(context.Context, DialHostBridgeRequest, io.ReadWriteCloser) error {
	return errEngineUnavailable
}
func (UnavailableEngine) ReapAttempt(context.Context, AttemptAuthority) error { return nil }
func (UnavailableEngine) ReapSession(context.Context, SessionIdentity) error  { return nil }
