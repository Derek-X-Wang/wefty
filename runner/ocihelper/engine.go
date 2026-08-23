package ocihelper

import (
	"context"
	"errors"
	"io"
)

// Engine is the helper-internal mechanics seam. A containerd adapter will
// implement it in a later ticket; no containerd request or type crosses RPC.
type Engine interface {
	EnsureImage(context.Context, EnsureImageRequest, func(EnsureImageEvent) error) error
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

// UnavailableEngine keeps the private helper mode fail-closed until the native
// containerd adapter lands. Tests supply a fake through RunInvocation.
type UnavailableEngine struct{}

var errEngineUnavailable = errors.New("OCI engine adapter is not installed")

func (UnavailableEngine) EnsureImage(context.Context, EnsureImageRequest, func(EnsureImageEvent) error) error {
	return errEngineUnavailable
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
