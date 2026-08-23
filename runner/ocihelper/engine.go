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
	HostMountRoot          string
	GuestMountRoot         string
	AttemptPortMin         uint16
	AttemptPortMax         uint16
	AttemptPortBindTimeout time.Duration
	LogSealTimeout         time.Duration
}

// GuardianReaper preserves the helper deadman's signal initiator when the
// concrete engine can obtain a real task signal-delivery acknowledgement.
type GuardianReaper interface {
	ReapAttemptAsGuardian(context.Context, AttemptAuthority) error
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
	return &ImageMechanicsError{Fact: ImageFailureFact{Kind: kind, TopLevelDigest: digest}, err: err}
}

func (UnavailableEngine) EnsureImage(context.Context, EnsureImageRequest, io.Reader, func(EnsureImageEvent) error) error {
	return &ImageMechanicsError{Fact: ImageFailureFact{Kind: ImageFailureEngineLoss}, err: errEngineUnavailable}
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
