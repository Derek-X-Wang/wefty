//go:build darwin || linux

package agent

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestPrimaryGenerationRejoinKeepsLeasedResident(t *testing.T) {
	for _, path := range []string{"heartbeat", "claim"} {
		for _, restartable := range []bool{false, true} {
			name := path + "/ordinary"
			if restartable {
				name = path + "/restartable"
			}
			t.Run(name, func(t *testing.T) {
				clock := newManualClock(time.Now().UTC())
				var expire, trouble atomic.Bool
				var registrations, registrationCalls atomic.Int32
				a, store, _, _ := newResidentRejoinFixture(t, clock, func(a *Agent, store *l1.Store) {
					base := a.session.client.httpClient.Transport
					a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
						if trouble.Load() {
							if response := holdOtherRejoinRequests(r, path); response != nil {
								return response, nil
							}
						}
						if isResidentRejoinRequest(r, path) && expire.CompareAndSwap(true, false) {
							expireNodeKeepingResidentLease(t, store, clock)
						}
						if r.URL.Path == "/v1/agent/nodes/register" && registrationCalls.Add(1) == 2 {
							// A brief registration outage must preserve the resident too.
							recorder := httptest.NewRecorder()
							recorder.WriteHeader(http.StatusServiceUnavailable)
							return recorder.Result(), nil
						}
						response, err := base.RoundTrip(r)
						if err == nil && r.URL.Path == "/v1/agent/nodes/register" && response.StatusCode == http.StatusOK {
							if registrations.Add(1) >= 2 {
								trouble.Store(false)
							}
						}
						return response, err
					})
				})
				job, ready, release := retirementReviewJob(t, store, "rejoin-resident", restartable, nil)
				retirementWait(t, "resident payload ready", func() bool { _, err := os.Stat(ready); return err == nil })
				trouble.Store(true)
				expire.Store(true)
				retirementWait(t, "same primary re-registered", func() bool { return registrations.Load() >= 2 })
				current, err := store.GetJob(t.Context(), job.JobID)
				if err != nil || current.State != contract.JobRunning {
					t.Fatalf("rejoin canceled a resident with a valid Lease: state=%s err=%v", current.State, err)
				}
				attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
				if err != nil || len(attempts) != 1 || !attempts[0].LeaseExpiresAt.After(clock.Now()) {
					t.Fatalf("resident Lease=%+v %v", attempts, err)
				}
				if a.Status().State == LifecycleQuarantined {
					t.Fatal("primary quarantined during rejoin")
				}
				finishRejoinResident(t, store, job, release)
			})
		}
	}
}

func TestPrimaryGenerationReadRefusalRejoinKeepsLeasedResident(t *testing.T) {
	for _, restartable := range []bool{false, true} {
		t.Run(fmt.Sprintf("restartable=%t", restartable), func(t *testing.T) {
			clock := newManualClock(time.Now().UTC())
			var refuse atomic.Bool
			var registrations, refusals, logged atomic.Int32
			a, store, _, _ := newResidentRejoinFixture(t, clock, func(a *Agent, store *l1.Store) {
				a.session.logf = func(format string, args ...any) {
					line := fmt.Sprintf(format, args...)
					if strings.HasPrefix(line, "agent: heartbeat failed:") && strings.Contains(line, string(contract.ErrorNodeNotRegistered)) {
						logged.Add(1)
					}
					t.Log(line)
				}
				base := a.session.client.httpClient.Transport
				a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == "/v1/agent/nodes/node/generations" && refuse.Load() {
						refusals.Add(1)
						return generationReadRefusal(t, r, store, contract.ErrorNodeNotRegistered), nil
					}
					response, err := base.RoundTrip(r)
					if err == nil && r.URL.Path == "/v1/agent/nodes/register" && response.StatusCode == http.StatusOK {
						if registrations.Add(1) == 2 {
							refuse.Store(false)
						}
					}
					return response, err
				})
			})
			job, ready, release := retirementReviewJob(t, store, "inventory-rejoin", restartable, nil)
			retirementWait(t, "resident payload ready", func() bool { _, err := os.Stat(ready); return err == nil })
			refuse.Store(true)
			retirementWait(t, "primary re-registered after inventory refusal", func() bool { return registrations.Load() == 2 })
			if refusals.Load() < 2 {
				t.Fatal("rejoin did not re-observe the generation before registration")
			}
			if logged.Load() != 1 {
				t.Fatalf("inventory refusal logs=%d, want one across rejoin observations", logged.Load())
			}
			if status := a.Status(); status.State == LifecycleQuarantined {
				t.Fatal("primary quarantined during inventory rejoin")
			}
			finishRejoinResident(t, store, job, release)
		})
	}
}

func TestPrimaryGenerationRejoinConvergesToLateRetirement(t *testing.T) {
	for _, latchLocally := range []bool{false, true} {
		name := "registration_refused"
		if latchLocally {
			name = "retirement_latched_during_registration"
		}
		t.Run(name, func(t *testing.T) {
			clock := newManualClock(time.Now().UTC())
			var expire, trouble atomic.Bool
			var registrationCalls atomic.Int32
			promoted := make(chan struct{})
			a, store, done, _ := newResidentRejoinFixture(t, clock, func(a *Agent, store *l1.Store) {
				base := a.session.client.httpClient.Transport
				a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
					if trouble.Load() {
						if response := holdOtherRejoinRequests(r, "heartbeat"); response != nil {
							return response, nil
						}
					}
					if isResidentRejoinRequest(r, "heartbeat") && expire.CompareAndSwap(true, false) {
						expireNodeKeepingResidentLease(t, store, clock)
					}
					if r.URL.Path == "/v1/agent/nodes/register" && registrationCalls.Add(1) == 2 {
						// All the caller's primary observations have already returned.
						// Promote immediately before L1's transactional register guard.
						promoteRetirementFixture(t, store, "node", "retirement-agent")
						if latchLocally {
							if retiring, err := a.session.observeGeneration(r.Context()); err != nil || !retiring {
								t.Errorf("latch retirement=%t %v", retiring, err)
							}
						}
						close(promoted)
					}
					return base.RoundTrip(r)
				})
			})
			job, ready, release := retirementReviewJob(t, store, "late-retirement", true, nil)
			retirementWait(t, "resident payload ready", func() bool { _, err := os.Stat(ready); return err == nil })
			trouble.Store(true)
			expire.Store(true)
			reviewWait(t, "promotion during registration", promoted)
			deadline := time.Now().Add(3 * time.Second)
			for a.Status().State != LifecycleRetiring && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if status := a.Status(); status.State != LifecycleRetiring {
				t.Fatalf("late promotion did not enter retirement: state=%s", status.State)
			}
			current, err := store.GetJob(t.Context(), job.JobID)
			if err != nil || current.State != contract.JobRunning {
				t.Fatalf("late promotion canceled resident: state=%s err=%v", current.State, err)
			}
			clock.Advance(20 * time.Second)
			retirementWait(t, "retiring resident still renews", func() bool {
				attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
				return err == nil && len(attempts) == 1 && attempts[0].LeaseExpiresAt.After(clock.Now().Add(50*time.Second))
			})
			finishRejoinResident(t, store, job, release)
			reviewRetirementExit(t, a, done)
			if registrationCalls.Load() != 2 {
				t.Fatalf("retiree retried registration: %d calls", registrationCalls.Load())
			}
			inventory, err := store.NodeGenerations(t.Context(), "retirement-agent", "node")
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			for _, generation := range inventory.Generations {
				if generation.AgentGenerationID == "generation" {
					closed = generation.Role == l1.NodeGenerationClosed
				}
			}
			if !closed {
				t.Fatal("L1 did not record the generation as closed")
			}
			node, err := store.GetNode(t.Context(), "node")
			if err != nil || node.BootSessionID != "next-boot" {
				t.Fatalf("successor projection changed: boot=%s err=%v", node.BootSessionID, err)
			}
		})
	}
}

func isResidentRejoinRequest(r *http.Request, path string) bool {
	if path == "claim" {
		return r.URL.Path == "/v1/agent/jobs/claim"
	}
	return r.URL.Path == "/v1/agent/nodes/node/heartbeat"
}

// Isolate the refusal source without blocking Attempt renewals or generation
// observations. Empty claim polls and transient Node heartbeats are safe here.
func holdOtherRejoinRequests(r *http.Request, path string) *http.Response {
	recorder := httptest.NewRecorder()
	if path == "heartbeat" && r.URL.Path == "/v1/agent/jobs/claim" {
		recorder.WriteHeader(http.StatusNoContent)
	} else if path == "claim" && r.URL.Path == "/v1/agent/nodes/node/heartbeat" {
		recorder.WriteHeader(http.StatusServiceUnavailable)
	} else {
		return nil
	}
	return recorder.Result()
}

func expireNodeKeepingResidentLease(t *testing.T, store *l1.Store, clock *manualClock) {
	t.Helper()
	// Advance across Node expiry while the production Attempt loop renews each
	// time. Each step stays inside the fixture's 60-second Attempt Lease.
	for range 3 {
		clock.Advance(40 * time.Second)
		retirementWait(t, "resident renewal while Node heartbeats are withheld", func() bool {
			node, err := store.GetNode(t.Context(), "node")
			return err == nil && len(node.ActiveAttempts) == 1 && node.ActiveAttempts[0].LeaseExpiresAt.After(clock.Now().Add(50*time.Second))
		})
	}
	if _, err := store.Reconcile(t.Context()); err != nil {
		t.Error(err)
	}
	node, err := store.GetNode(t.Context(), "node")
	if err != nil || node.State != contract.NodeDead {
		t.Errorf("dead Node state=%s err=%v", node.State, err)
	}
}

func finishRejoinResident(t *testing.T, store *l1.Store, job l1.Job, release string) {
	t.Helper()
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "resident completed with its real exit", func() bool {
		attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
		return err == nil && len(attempts) == 1 && attempts[0].Result != nil && attempts[0].Result.ExitCode != nil && *attempts[0].Result.ExitCode == 17
	})
	logs, err := store.GetJobLogs(t.Context(), job.JobID, "", l1.MaxLogPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	for _, event := range logs.Events {
		output.Write(event.Bytes)
	}
	if !strings.Contains(output.String(), "tail") {
		t.Fatalf("resident output was lost: %q", output.String())
	}
}

// Keep real process execution, Guardian, renewal, evidence and retirement, but
// inject L1's HTTP transport and boot identity so no loopback listener is needed.
func newResidentRejoinFixture(t *testing.T, clock *manualClock, hook func(*Agent, *l1.Store)) (*Agent, *l1.Store, <-chan error, context.CancelFunc) {
	t.Helper()
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "resident.sqlite"), l1.StoreOptions{Clock: clock, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	participant := &mutableWhoIsFabric{identity: fabric.Identity{NodeID: "retirement-agent", FabricID: "plain-rejoin-resident", Tags: []string{l1.DefaultAgentPrincipalTag}}}
	server, err := l1.NewServer(participant, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"node": {Tags: []string{"linux"}, MaxOneshotSlots: 3}}})
	if err != nil {
		t.Fatal(err)
	}
	managed := temporaryManagedRoot(t)
	zero := 0.0
	a, err := New(Config{Fabric: participant, ControlPlaneAddress: "wefty://control-plane", NodeID: "node", BootSessionID: "boot", AgentGenerationID: "generation", OS: "darwin", Version: "test",
		Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true, contract.CapabilityExecCaptureBundle: true},
		HandoffRoot:  t.TempDir(), GuardianExecutable: agentBinaryPath, LogSpoolDirectory: t.TempDir(), ManagedRootDirectory: managed, MaxOneshotSlots: 3, OneshotLoadLimit: &zero,
		HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 10 * time.Millisecond, RenewalInterval: 100 * time.Millisecond, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	resource := a.managedResource.(*processManagedResource)
	resource.root, err = managedroot.Open(managedroot.Config{Root: managed, NodeID: "node", BootSessionID: "boot", HostBootIdentity: rootHostIdentity})
	if err != nil {
		t.Fatal(err)
	}
	a.session.client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, r)
		return recorder.Result(), nil
	})
	// These payloads do not call a control-plane bridge. Omit its loopback
	// listener; the session Client above still uses the real L1 handler.
	a.fabric = nil
	if hook != nil {
		hook(a, store)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testwait.Bound):
			t.Error("agent did not stop")
		}
	})
	select {
	case <-a.Registered():
	case <-time.After(testwait.Bound):
		t.Fatal("registration timed out")
	}
	return a, store, done, cancel
}
