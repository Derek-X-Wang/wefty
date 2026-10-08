package ocihelper

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestBootBarrierRefusedSweepRPCIsUnreachable(t *testing.T) {
	client, stop := startTestServer(t, newFakeEngine(), ServerConfig{})
	defer stop()
	directory, err := os.MkdirTemp("/tmp", "wefty-refused-rpc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	refusedPath := filepath.Join(directory, "helper.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: refusedPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	dials := 0
	originalDial := client.Dial
	client.Dial = func(ctx context.Context) (net.Conn, error) {
		dials++
		if dials == 1 {
			return originalDial(ctx)
		}
		return (&net.Dialer{}).DialContext(ctx, "unix", refusedPath)
	}
	barrier, err := NewBootBarrier(client, testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err = barrier.Ensure(ctx)
	var loss *RuntimeLossError
	var dial *HelperDialError
	if dials != 2 || !errors.Is(err, syscall.ECONNREFUSED) || !errors.As(err, &loss) || !errors.As(err, &dial) || barrier.Ready() {
		t.Fatalf("did not reach refused sweep RPC after session admission: dials=%d err=%v ready=%t", dials, err, barrier.Ready())
	}
	if reason := barrier.CapabilityReasonCode(); reason != contract.CapabilityReasonHelperUnreachable {
		t.Fatalf("refused sweep RPC reason = %q, want helper_unreachable: %v", reason, err)
	}
}

func TestBootBarrierInvalidAdmissionIsHandshakeFailed(t *testing.T) {
	preface := validAcquireResponse("helper-invalid-admission", time.Second)
	client := scriptedAcquireClient(t, []scriptedAcquireAttempt{{handshake: preface, admission: &preface}})
	barrier, err := NewBootBarrier(client, testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err = barrier.Ensure(ctx)
	var handshake *HelperHandshakeError
	if !errors.As(err, &handshake) || barrier.Ready() || client.scriptedDials() != 1 {
		t.Fatalf("invalid admission outcome = err=%v ready=%t dials=%d", err, barrier.Ready(), client.scriptedDials())
	}
	if reason := barrier.CapabilityReasonCode(); reason != contract.CapabilityReasonHelperHandshakeFailed {
		t.Fatalf("invalid admission reason = %q, want helper_handshake_failed: %v", reason, err)
	}
}
