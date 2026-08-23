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
	engine.setRunResponse(RunResponse{Started: true, AttemptPort: 42101, HostBridgeReady: true})
	request := testRunRequest(authority, 2*time.Second)
	request.EnableHostBridgeFallback = true
	run, err := session.Run(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if run.BridgeCapability == "" {
		t.Fatal("Mac fallback did not receive attempt-scoped authority")
	}
	_, err = session.DialAttemptPort(t.Context(), DialAttemptPortRequest{Authority: authority, Port: 42102})
	assertRPCCode(t, err, CodeUnauthorizedPort)
	_, err = session.DialHostBridge(t.Context(), DialHostBridgeRequest{Authority: authority, BridgeCapability: "not-authorized"})
	assertRPCCode(t, err, CodeUnauthorizedBridge)

	// Keep the control connection open but intentionally blackhole heartbeats.
	waitFor(t, 3*time.Second, func() bool { return engine.sessionReapCount() == 1 }, "blackholed helper session reap")
	err = session.Watch(t.Context(), WatchRequest{Authority: authority}, nil)
	assertRPCCode(t, err, CodeSessionStale)
}
