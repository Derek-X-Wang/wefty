package ocihelper

import (
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
)

// TestPerResourceRefusalsAreNotRuntimeLoss pins the narrow set of
// engine refusals that are scoped to the one durable resource they name.
// Deleting a Backup copy is authorized on its own, deletes only that copy, and
// is accepted by the caller only against an independently verified
// positive-absence receipt -- so a refusal there says nothing about whether the
// exclusive helper session still holds namespace authority.
//
// Reading it as node-wide loss is what turned one immutable Backup copy into a
// node that dropped kind:oci and could place nothing, while the removal holding
// that copy had already been declared stalled and had released its Slot (#513).
// Creating a Backup copy is the same shape for the same one copy; reading its
// refusal as loss let a Backup of a never-started clone tear the session down
// on every redispatch (#558).
func TestPerResourceRefusalsAreNotRuntimeLoss(t *testing.T) {
	scoped := []Method{MethodDeleteVolume, MethodDeleteBackup, MethodCreateBackup}
	for _, operation := range scoped {
		refusal := &RPCError{
			Code: CodeEngineFailure, Message: "OCI engine operation failed",
			EngineFailure: &EngineFailureFact{Operation: operation, Reason: EngineFailurePermissionDenied},
		}
		if rpcErrorProvesRuntimeLoss(refusal) {
			t.Fatalf("%s refusal would invalidate the exclusive helper session: %+v", operation, refusal)
		}
	}

	// The default stands: an engine failure that names no independently
	// verified per-resource operation is still runtime-loss evidence.
	wholeNamespace := &RPCError{
		Code: CodeEngineFailure, Message: "OCI engine operation failed",
		EngineFailure: &EngineFailureFact{Operation: MethodSweep, Reason: EngineFailureOperationFailed},
	}
	if !rpcErrorProvesRuntimeLoss(wholeNamespace) {
		t.Fatalf("a swept-namespace engine failure stopped proving runtime loss: %+v", wholeNamespace)
	}
}

// A connection_limit refusal is the helper answering, on a fresh connection,
// that it has no slot for one request. It must never read as runtime loss --
// a bare close in its place did, and reaped every OCI workload on the Node
// (#597) -- while a real transport EOF still does.
func TestConnectionLimitRefusalIsNotRuntimeLoss(t *testing.T) {
	refusal := &RPCError{Code: CodeConnectionLimit, Message: "OCI helper connection limit is full; retry this operation"}
	if rpcErrorProvesRuntimeLoss(refusal) {
		t.Fatalf("connection_limit refusal proves runtime loss: %+v", refusal)
	}
	client, stop := startTestServer(t, newFakeEngine(), ServerConfig{HeartbeatTimeout: 5 * time.Second})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.markOperationFailure(t.Context(), refusal); !IsConnectionLimitRefusal(err) {
		t.Fatalf("connection_limit refusal was rewritten to %v", err)
	}
	if err := session.HealthError(); err != nil {
		t.Fatalf("connection_limit refusal marked the session lost: %v", err)
	}
	eof := fmt.Errorf("receive OCI helper response: %w", io.EOF)
	var loss *RuntimeLossError
	if err := session.markOperationFailure(t.Context(), eof); !errors.As(err, &loss) {
		t.Fatalf("transport EOF = %v, want runtime loss", err)
	}
	if session.HealthError() == nil {
		t.Fatal("transport EOF left the session healthy")
	}
}
