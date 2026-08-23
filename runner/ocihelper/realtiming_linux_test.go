//go:build service_acceptance_realtiming && linux

package ocihelper

import (
	"context"
	"testing"
	"time"
)

func TestLinuxRealtimeDeadmanThenReusedBootSweep(t *testing.T) {
	socketPath := startRealtimeHelperChild(t)
	client := NewUnixClient(socketPath, "realtime-fake-checksum")
	client.HeartbeatInterval = 100 * time.Millisecond
	request := AcquireSessionRequest{NodeID: "linux-realtime-node", BootSessionID: "reused-linux-boot"}
	barrier, err := NewBootBarrier(client, request)
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := barrier.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	authority := AttemptAuthority{
		NodeID: request.NodeID, JobID: "deadman-job", AttemptID: "deadman-attempt",
		FencingToken: "deadman-fence", BootSessionID: request.BootSessionID,
		Class: "one-shot", RemovalGeneration: "deadman-removal",
	}
	deadmanStarted := time.Now()
	if _, err := session.Run(ctx, testRunRequest(authority, time.Second)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	err = session.Signal(ctx, SignalRequest{Authority: authority, Signal: SignalTERM})
	assertRPCCode(t, err, CodeUnauthorizedAttempt)
	if elapsed := time.Since(deadmanStarted); elapsed > 4*time.Second {
		t.Fatalf("attempt deadman reap took %s, want at most 4s", elapsed)
	}

	barrier.Invalidate()
	sweepStarted := time.Now()
	if err := barrier.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(sweepStarted); elapsed > 3*time.Second {
		t.Fatalf("reused-boot takeover and sweep took %s, want at most 3s", elapsed)
	}
}
