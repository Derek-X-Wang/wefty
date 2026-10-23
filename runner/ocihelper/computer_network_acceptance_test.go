//go:build service_acceptance

package ocihelper

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestComputerNetworkSettingCrossesHelperSession(t *testing.T) {
	e := newFakeEngine()
	e.runResponse.Endpoints = map[string]uint16{"view": 42011, "control": 42012}
	client, stop := startTestServer(t, e, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	a := testAuthority()
	a.Class = "service"
	run := testRunRequest(a, time.Minute)
	run.Workload.Computer = true
	run.Workload.Limits.MemoryBytes = 1 << 30
	run.Workload.ManagedVolumes = testComputerManagedVolumes()
	run.AllocateEndpoints = []string{"view", "control"}
	run.EnableHostBridgeFallback, run.ActivateHostBridgeFallback = true, true
	setComputerNetworkForTest(t, &run.Workload, "filtered")
	_, err = session.Run(context.Background(), run)
	var rpc *RPCError
	if !errors.As(err, &rpc) || rpc.Code != CodeInvalidRequest {
		t.Fatalf("unsupported network was not refused before admission: %v", err)
	}
	setComputerNetworkForTest(t, &run.Workload, "none")
	if _, err := session.Run(t.Context(), run); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	got := e.lastRunRequest.Workload.ComputerNetwork
	e.mu.Unlock()
	if got != "none" {
		t.Fatalf("helper lost Computer network setting: %q", got)
	}
}
