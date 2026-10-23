//go:build service_acceptance

package ocihelper

import (
	"context"
	"testing"
	"time"
)

// Exercise the real session transport and transition logic together. The fake
// replaces only the privileged cgroup files; the Linux Seat lane proves them.
type acceptanceMemoryEngine struct {
	*fakeEngine
	seats    *computerSeats
	runtime  *fakeComputerMemory
	reserved *int64
	clock    Clock
}

func (e *acceptanceMemoryEngine) SetComputerMemoryLimit(ctx context.Context, r SetComputerMemoryLimitRequest) (SetComputerMemoryLimitResponse, error) {
	return changeComputerMemoryLimit(ctx, r, e.seats, 233<<20, e.reserved, 2<<30, e.runtime, e.clock, func() error { return nil })
}
func TestComputerMemoryRoomHeldThroughSessionAcceptance(t *testing.T) {
	seats, runtime, reserved, clock := memoryLimitFixture(t)
	*reserved = 2 << 30
	runtime.limit = 2 << 30
	runtime.usage = 1500 << 20
	e := &acceptanceMemoryEngine{newFakeEngine(), seats, runtime, reserved, clock}
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
	run.Workload.Limits.MemoryBytes = 2 << 30
	run.Workload.ManagedVolumes = testComputerManagedVolumes()
	run.AllocateEndpoints = []string{"view", "control"}
	run.EnableHostBridgeFallback = true
	run.ActivateHostBridgeFallback = true
	if _, err := session.Run(t.Context(), run); err != nil {
		t.Fatal(err)
	}
	req := SetComputerMemoryLimitRequest{a, 1 << 30, seats.generation}
	held, err := session.SetComputerMemoryLimit(t.Context(), req)
	if err != nil || held.ReservedMemoryBytes/(1<<30) != 2 || held.HeldMemoryBytes/(1<<30) != 1 || held.Reason != "memory_in_use" {
		t.Fatalf("held Room through wire: %+v %v", held, err)
	}
	runtime.usage = 128 << 20
	shrunk, err := session.SetComputerMemoryLimit(t.Context(), req)
	if err != nil || shrunk.ReservedMemoryBytes/(1<<30) != 1 || shrunk.HeldMemoryBytes != 0 || shrunk.MemoryLimitBytes != runtime.limit {
		t.Fatalf("returned Room through wire: %+v %v", shrunk, err)
	}
}
