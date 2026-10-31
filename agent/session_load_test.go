//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Exercise the production claim path, including actual claim HTTP requests.
func loadAdmissionSession(t *testing.T) (*agentSession, *manualClock, *atomic.Int32, *atomic.Bool) {
	t.Helper()
	network := plain.NewNetwork()
	listener, err := network.NewFabric(fabric.Identity{NodeID: "control-plane"}).Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	calls := &atomic.Int32{}
	empty := &atomic.Bool{}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/agent/jobs/claim" {
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		n := calls.Add(1)
		if empty.Load() {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var request l1.ClaimRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(l1.Claim{Job: l1.Job{JobID: fmt.Sprintf("job-%d", n), Spec: contract.JobSpec{Class: request.Class, Kind: contract.JobKindProcess}}})
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	client, err := newClient(network.NewFabric(fabric.Identity{NodeID: "agent"}), "wefty://control-plane", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	clock := newManualClock(time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC))
	registration := contract.NodeRegistration{NodeID: "node", BootSessionID: "boot", Capabilities: map[string]bool{"kind:process": true}}
	session := newAgentSession(client, registration, newCapabilityState(registration.Capabilities, nil, clock, 0, nil), time.Second, time.Second, clock, newLifecycleObserver(clock), nil, 4, 2)
	t.Cleanup(session.close)
	session.oneshotLoadLimit = 18
	session.readLoad = func() (float64, error) { return 1, nil }
	return session, clock, calls, empty
}

func TestOneshotLoadAdmissionSendsNoClaimWhenHolding(t *testing.T) {
	for _, tc := range []struct {
		name        string
		occupied    int
		load, limit float64
		readErr     error
		want        bool
	}{
		{name: "at limit", occupied: 1, load: 18, limit: 18},
		{name: "above limit", occupied: 1, load: 25, limit: 18},
		{name: "below limit", occupied: 1, load: 17, limit: 18, want: true},
		{name: "first run", load: 25, limit: 18, want: true},
		{name: "disabled", occupied: 1, load: 25, limit: 0, want: true},
		{name: "unreadable", occupied: 1, limit: 18, readErr: errors.New("unreadable"), want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session, _, calls, _ := loadAdmissionSession(t)
			session.oneshotLoadLimit = tc.limit
			session.readLoad = func() (float64, error) { return tc.load, tc.readErr }
			gate := session.gates[workloadClassOneShot]
			for range tc.occupied {
				gate.acquireReserved()
			}
			claim, admitted, err := session.claim(t.Context(), gate, contract.JobClassOneShot, false)
			if err != nil || admitted != tc.want || (claim != nil) != tc.want || (calls.Load() != 0) != tc.want {
				t.Fatalf("claim=%v admitted=%t calls=%d err=%v", claim, admitted, calls.Load(), err)
			}
		})
	}
}

func TestOneshotClaimsAreSpacedWithInjectedClock(t *testing.T) {
	session, clock, calls, _ := loadAdmissionSession(t)
	gate := session.gates[workloadClassOneShot]
	claim := func(want bool) {
		t.Helper()
		before := calls.Load()
		got, admitted, err := session.claim(t.Context(), gate, contract.JobClassOneShot, false)
		if err != nil || admitted != want || (got != nil) != want || calls.Load()-before != boolCount(want) {
			t.Fatalf("claim=%v admitted=%t calls=%d err=%v", got, admitted, calls.Load()-before, err)
		}
	}
	claim(true)
	claim(false) // Another loop cannot burst immediately after the first claim.
	clock.Advance(29 * time.Second)
	claim(false)
	clock.Advance(time.Second)
	claim(true)
	claim(false)
	clock.Advance(30 * time.Second)
	claim(true)
	// An empty node starts at once, even if a run recently finished.
	for gate.occupancy().Occupied > 0 {
		gate.release()
	}
	session.readLoad = func() (float64, error) { return 99, nil }
	claim(true)
	// A zero limit turns the whole rule off: high load and no spacing.
	session.oneshotLoadLimit = 0
	claim(true)
	claim(true)
}

func boolCount(value bool) int32 {
	if value {
		return 1
	}
	return 0
}

func TestLoadAdmissionLeavesServicesAndEmptyPollsAlone(t *testing.T) {
	session, clock, calls, empty := loadAdmissionSession(t)
	session.readLoad = func() (float64, error) { return 99, nil }
	session.lastOneshotClaim = clock.Now()
	session.gates[workloadClassOneShot].acquireReserved()
	for range 2 {
		got, admitted, err := session.claim(t.Context(), session.gates[workloadClassService], contract.JobClassService, false)
		if err != nil || !admitted || got == nil {
			t.Fatalf("service claim=%v admitted=%t err=%v", got, admitted, err)
		}
	}
	if calls.Load() != 2 {
		t.Fatal(calls.Load())
	}
	clock.Advance(30 * time.Second)
	session.readLoad = func() (float64, error) { return 1, nil }
	empty.Store(true)
	gate := session.gates[workloadClassOneShot]
	for range 2 {
		got, admitted, err := session.claim(t.Context(), gate, contract.JobClassOneShot, false)
		if err != nil || !admitted || got != nil {
			t.Fatalf("empty poll=%v %t %v", got, admitted, err)
		}
	}
	empty.Store(false)
	got, _, err := session.claim(t.Context(), gate, contract.JobClassOneShot, false)
	if err != nil || got == nil {
		t.Fatalf("empty polls consumed spacing: %v %v", got, err)
	}
}

func TestUnreadableLoadLogsOnlyOnce(t *testing.T) {
	session, clock, _, empty := loadAdmissionSession(t)
	session.gates[workloadClassOneShot].acquireReserved()
	empty.Store(true)
	logs := 0
	session.logf = func(string, ...any) { logs++ }
	session.readLoad = func() (float64, error) { return 0, errors.New("unavailable") }
	for range 2 {
		_, admitted, err := session.claim(t.Context(), session.gates[workloadClassOneShot], contract.JobClassOneShot, false)
		if err != nil || !admitted {
			t.Fatal(admitted, err)
		}
		clock.Advance(30 * time.Second)
	}
	if logs != 1 {
		t.Fatalf("load errors logged %d times", logs)
	}
}

// Existing lifecycle integration tests exercise capacity, drain and intent,
// independently of the load catch-up window. Advance this clock between starts
// while the real session clock continues to renew their actual L1 leases.
func injectOneshotAdmissionClock(t *testing.T, nodeAgent *Agent) *manualClock {
	t.Helper()
	clock := newManualClock(time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC))
	nodeAgent.session.oneshotClaimClock = clock
	nodeAgent.session.readLoad = func() (float64, error) { return 0, nil }
	return clock
}

func TestAgentLoadAdmissionUsesConfiguredReader(t *testing.T) {
	network := plain.NewNetwork()
	store, stop := startFailureServerWithPolicies(t, network, nil, map[string]l1.NodePolicy{
		"node-1": {Tags: []string{"linux"}, MaxOneshotSlots: 2, MaxServiceSlots: 1},
	})
	defer stop()
	createAgentTestJob(t, store, "configured-load-first")
	queued := createAgentTestJob(t, store, "configured-load-second")
	runner := newSlotRefillRunner()
	var load atomic.Int64
	load.Store(1)
	read := make(chan struct{}, 16)
	// Keep this below every CPU-based default so dropping the configured limit
	// changes the claim decision and makes this test fail.
	limit := 0.5
	nodeAgent, err := New(Config{
		Fabric:              network.NewFabric(fabric.Identity{NodeID: "agent", Tags: []string{l1.DefaultAgentPrincipalTag}}),
		ControlPlaneAddress: "wefty://control-plane", NodeID: "node-1", BootSessionID: "boot", Version: "test",
		Capabilities: map[string]bool{"kind:process": true}, WorkloadRuntimes: testRuntimeSet(runner), LogSpoolDirectory: t.TempDir(),
		MaxOneshotSlots: 2, OneshotLoadLimit: &limit, ClaimInterval: time.Second, HeartbeatInterval: time.Second, RenewalInterval: time.Second,
		ReadLoad: func() (float64, error) {
			select {
			case read <- struct{}{}:
			default:
			}
			return float64(load.Load()), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	admissionClock := newManualClock(time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC))
	nodeAgent.session.oneshotClaimClock = admissionClock
	var calls atomic.Int32
	base := nodeAgent.session.client.httpClient.Transport
	nodeAgent.session.client.httpClient.Transport = completionDirectiveRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/v1/agent/jobs/claim" {
			body, err := r.GetBody()
			if err != nil {
				return nil, err
			}
			var request l1.ClaimRequest
			err = json.NewDecoder(body).Decode(&request)
			_ = body.Close()
			if err != nil {
				return nil, err
			}
			if request.Class == contract.JobClassOneShot {
				calls.Add(1)
			}
		}
		return base.RoundTrip(r)
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testwait.Bound):
			t.Error("agent did not stop")
		}
	}()
	runner.waitStarted(t) // The first run starts without a load read.
	admissionClock.Advance(30 * time.Second)
	// A second read proves the prior high-load check returned without a claim.
	// If it claimed, both slots would be occupied and no further read would occur.
	for range 2 {
		select {
		case <-read:
		case <-time.After(testwait.Bound):
			t.Fatal("configured reader and limit did not keep the next claim waiting")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("high load sent %d one-shot requests, want only the first", calls.Load())
	}
	job, err := store.GetJob(t.Context(), queued.JobID)
	if err != nil || job.State != contract.JobQueued {
		t.Fatalf("queued state=%s err=%v", job.State, err)
	}
	load.Store(0)
	runner.waitStarted(t)
	if calls.Load() != 2 {
		t.Fatalf("below-limit requests=%d, want 2", calls.Load())
	}
}
