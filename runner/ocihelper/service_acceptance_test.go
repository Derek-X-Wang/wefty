//go:build service_acceptance && (darwin || linux)

package ocihelper

import (
	"testing"
	"time"
)

func TestServiceAcceptanceHelperAuthorityFailsClosed(t *testing.T) {
	engine := newFakeEngine()
	client, stop := startTestServer(t, engine, ServerConfig{
		HeartbeatTimeout: time.Second, MaximumAttemptDeadman: 2 * time.Second,
	})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	authority := testAuthority()
	requireSweep(t, session)
	engine.setRunResponse(RunResponse{Started: true, StartedAt: testStartedAt(), Endpoints: map[string]uint16{"service": 42101}, HostBridgeReady: true, HostBridgeEndpoint: "http://127.0.0.1:42102/l3"})
	request := testRunRequest(authority, 2*time.Second)
	request.AllocateEndpoints = []string{"service"}
	request.EnableHostBridgeFallback = true
	run, err := session.Run(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if run.BridgeCapability == "" {
		t.Fatal("Mac fallback did not receive attempt-scoped authority")
	}
	_, err = session.DialAttemptPort(t.Context(), DialAttemptPortRequest{Authority: authority, Name: "missing"})
	assertRPCCode(t, err, CodeUnauthorizedPort)
	_, err = session.DialHostBridge(t.Context(), DialHostBridgeRequest{Authority: authority, BridgeCapability: "not-authorized"})
	assertRPCCode(t, err, CodeUnauthorizedBridge)

	// Keep the control connection open but intentionally blackhole heartbeats.
	waitFor(t, 3*time.Second, func() bool { return engine.sessionReapCount() == 1 }, "blackholed helper session reap")
	err = session.Watch(t.Context(), WatchRequest{Authority: authority}, nil)
	assertRPCCode(t, err, CodeSessionStale)
}

func TestServiceAcceptanceComputerControlStateFailsClosed(t *testing.T) {
	engine := newFakeEngine()
	engine.setRunResponse(RunResponse{Started: true, StartedAt: testStartedAt(), Endpoints: map[string]uint16{"view": 42111, "control": 42112}})
	client, stop := startTestServer(t, engine, ServerConfig{
		HeartbeatTimeout: 2 * time.Second, MaximumAttemptDeadman: 3 * time.Second,
	})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	authority := testAuthority()
	authority.Class = "service"
	request := testRunRequest(authority, 2*time.Second)
	request.Workload.Computer = true
	request.Workload.Limits.MemoryBytes = 1 << 30
	request.Workload.ManagedVolumes = testComputerManagedVolumes()
	request.AllocateEndpoints = []string{"view", "control"}
	if _, err := session.Run(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := session.SetComputerControlState(t.Context(), SetComputerControlStateRequest{Authority: authority, HumanDriving: true}); err != nil {
		t.Fatal(err)
	}
	stale := authority
	stale.FencingToken = "stale"
	if err := session.SetComputerControlState(t.Context(), SetComputerControlStateRequest{Authority: stale}); err == nil {
		t.Fatal("stale authority changed the Computer driving signal")
	}
	if _, err := session.Delete(t.Context(), DeleteRequest{Authority: authority}); err != nil {
		t.Fatal(err)
	}
	if err := session.SetComputerControlState(t.Context(), SetComputerControlStateRequest{Authority: authority}); err == nil {
		t.Fatal("reaped authority changed the Computer driving signal")
	}
}
