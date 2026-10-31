//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Exercise the real session supervisor and L1 HTTP/store boundary without a
// listener or a host boot lookup, neither of which is available in a sandbox.
func newGenerationRejoinFixture(t *testing.T, heartbeatInterval, claimInterval time.Duration, logf func(string, ...any), beforeRequest func(*http.Request, *l1.Store, *manualClock) *http.Response) (*agentSession, *l1.Store, *atomic.Int32) {
	t.Helper()
	clock := newManualClock(time.Now().UTC())
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "rejoin.sqlite"), l1.StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	participant := &mutableWhoIsFabric{identity: fabric.Identity{NodeID: "agent", FabricID: "plain-generation-rejoin", Tags: []string{l1.DefaultAgentPrincipalTag, l1.DefaultClientPrincipalTag}}}
	server, err := l1.NewServer(participant, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"node": {MaxOneshotSlots: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	client, err := newClient(participant, "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	registrations := &atomic.Int32{}
	client.httpClient.Transport = captureRoundTrip(func(request *http.Request) (*http.Response, error) {
		if beforeRequest != nil {
			if response := beforeRequest(request, store, clock); response != nil {
				return response, nil
			}
		}
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		if request.URL.Path == "/v1/agent/nodes/register" && recorder.Code == http.StatusOK {
			registrations.Add(1)
		}
		return recorder.Result(), nil
	})
	registration := contract.NodeRegistration{NodeID: "node", BootSessionID: "boot", AgentGenerationID: "generation", OS: "darwin", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true}}
	if logf == nil {
		logf = t.Logf
	}
	capabilities := newCapabilityState(registration.Capabilities, nil, systemClock{}, 0, nil)
	session := newAgentSession(client, registration, capabilities, heartbeatInterval, claimInterval, systemClock{}, newLifecycleObserver(systemClock{}), logf, 1, 0)
	// Host load must not suppress the claim refusal under test.
	session.oneshotLoadLimit = 0
	t.Cleanup(session.close)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- session.run(ctx, func(context.Context, l1.Claim, time.Time) (errorDestination, error) {
			return errorDestinationUnclassified, fmt.Errorf("unexpected resident attempt")
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("session shutdown: %v", err)
			}
		case <-time.After(testwait.Bound):
			t.Error("session did not stop")
		}
	})
	retirementWait(t, "initial registration", func() bool { return registrations.Load() == 1 })
	return session, store, registrations
}

func TestPrimaryGenerationRejoinsAfterNodeDeadHeartbeat(t *testing.T) {
	assertPrimaryGenerationRejoinsAfterNodeDead(t, true)
}

// Produce refusals through L1's real authorization and inventory handlers.
// Other RPCs keep their original identity and store, including Attempt renewals.
func generationReadRefusal(t *testing.T, request *http.Request, store *l1.Store, code contract.ErrorCode) *http.Response {
	t.Helper()
	identity := fabric.Identity{NodeID: "agent", Tags: []string{l1.DefaultAgentPrincipalTag}}
	request = request.Clone(request.Context())
	request.Method = http.MethodGet
	switch code {
	case contract.ErrorNodeNotRegistered:
		request.URL.Path = "/v1/agent/nodes/missing/generations"
	case contract.ErrorIdentityBound:
		identity.NodeID = "other-agent"
	case contract.ErrorPrincipalForbidden:
		identity.Tags = nil
	default:
		t.Fatalf("unsupported real inventory refusal: %s", code)
	}
	server, err := l1.NewServer(&mutableWhoIsFabric{identity: identity}, store, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	var response contract.ErrorResponse
	// Agent loops keep reading while the fixture shuts down; a canceled
	// request has no refusal to check.
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); request.Context().Err() == nil && (err != nil || response.Error.Code != code) {
		t.Errorf("inventory refusal=%s, want %s: %v", recorder.Body.String(), code, err)
	}
	return recorder.Result()
}

func TestPrimaryGenerationReadRefusalQuarantines(t *testing.T) {
	for _, code := range []contract.ErrorCode{contract.ErrorIdentityBound, contract.ErrorPrincipalForbidden} {
		for _, path := range []string{"heartbeat", "heartbeat_reobservation", "claim", "rejoin", "rejoin_after_register"} {
			t.Run(string(code)+"/"+path, func(t *testing.T) {
				var refuse atomic.Bool
				var reads, logged atomic.Int32
				heartbeatInterval, claimInterval := time.Hour, 10*time.Millisecond
				if strings.HasPrefix(path, "heartbeat") {
					heartbeatInterval, claimInterval = 10*time.Millisecond, time.Hour
				}
				session, _, registrations := newGenerationRejoinFixture(t, heartbeatInterval, claimInterval, func(format string, args ...any) {
					line := fmt.Sprintf(format, args...)
					if strings.HasPrefix(line, "agent: heartbeat failed:") {
						logged.Add(1)
					}
				}, func(r *http.Request, store *l1.Store, clock *manualClock) *http.Response {
					if !refuse.Load() {
						return nil
					}
					if r.URL.Path == "/v1/agent/jobs/claim" || (r.URL.Path == "/v1/agent/nodes/node/heartbeat" && path == "heartbeat_reobservation") {
						if path == "claim" {
							recorder := httptest.NewRecorder()
							recorder.WriteHeader(http.StatusServiceUnavailable)
							return recorder.Result()
						}
						// Expire through L1 to reach the inventory read after a
						// refused claim or heartbeat.
						clock.Advance(l1.DefaultNodeDeadAfter)
						if _, err := store.Reconcile(r.Context()); err != nil && r.Context().Err() == nil {
							t.Error(err)
						}
					}
					if r.URL.Path == "/v1/agent/nodes/register" && path == "rejoin_after_register" {
						recorder := httptest.NewRecorder()
						recorder.WriteHeader(http.StatusServiceUnavailable)
						return recorder.Result()
					}
					if r.URL.Path == "/v1/agent/nodes/node/generations" {
						read := reads.Add(1)
						if (path == "rejoin" || path == "heartbeat_reobservation") && read == 1 || path == "rejoin_after_register" && read <= 2 {
							return nil
						}
						return generationReadRefusal(t, r, store, code)
					}
					return nil
				})
				refuse.Store(true)
				retirementWait(t, "inventory refusal quarantines the session", func() bool {
					return session.observer.snapshot(ClassOccupancy{}, ClassOccupancy{}).State == LifecycleQuarantined
				})
				status := session.observer.snapshot(ClassOccupancy{}, ClassOccupancy{})
				if status.LastSemanticError == nil || status.LastSemanticError.Code != code {
					t.Fatalf("quarantine reason=%+v, want %s", status.LastSemanticError, code)
				}
				if registrations.Load() != 1 || logged.Load() != 1 {
					t.Fatalf("registrations=%d refusal logs=%d, want one each", registrations.Load(), logged.Load())
				}
			})
		}
	}
}

func TestPrimaryGenerationReadTransientFailureRetries(t *testing.T) {
	var fail atomic.Bool
	var failures, recoveredHeartbeats atomic.Int32
	session, _, registrations := newGenerationRejoinFixture(t, 10*time.Millisecond, time.Hour, nil, func(r *http.Request, _ *l1.Store, _ *manualClock) *http.Response {
		if r.URL.Path == "/v1/agent/nodes/node/generations" && fail.Load() {
			failures.Add(1)
			recorder := httptest.NewRecorder()
			recorder.WriteHeader(http.StatusServiceUnavailable)
			return recorder.Result()
		}
		if r.URL.Path == "/v1/agent/nodes/node/heartbeat" && failures.Load() >= 2 {
			recoveredHeartbeats.Add(1)
		}
		return nil
	})
	fail.Store(true)
	retirementWait(t, "transient inventory failure retries", func() bool { return failures.Load() >= 2 })
	if registrations.Load() != 1 {
		t.Fatal("transient inventory failure re-registered the generation")
	}
	fail.Store(false)
	retirementWait(t, "heartbeat recovers after inventory becomes available", func() bool {
		return recoveredHeartbeats.Load() > 0 && session.observer.snapshot(ClassOccupancy{}, ClassOccupancy{}).State == LifecycleReady
	})
	if registrations.Load() != 1 {
		t.Fatal("inventory recovery re-registered the generation")
	}
}

func TestGenerationReadRefusalReobservesRetirementBeforeRejoin(t *testing.T) {
	var promote atomic.Bool
	var retiringHeartbeats atomic.Int32
	session, store, registrations := newGenerationRejoinFixture(t, 10*time.Millisecond, time.Hour, nil, func(r *http.Request, store *l1.Store, _ *manualClock) *http.Response {
		if r.URL.Path == "/v1/agent/nodes/node/generations" && promote.CompareAndSwap(true, false) {
			promoteRetirementFixture(t, store, "node", "agent")
			return generationReadRefusal(t, r, store, contract.ErrorNodeNotRegistered)
		}
		if strings.HasSuffix(r.URL.Path, "/generations/generation/heartbeat") {
			retiringHeartbeats.Add(1)
			return generationReadRefusal(t, r, store, contract.ErrorNodeNotRegistered)
		}
		return nil
	})
	promote.Store(true)
	retirementWait(t, "retiring heartbeat retries", func() bool { return retiringHeartbeats.Load() >= 2 })
	if registrations.Load() != 1 || !session.isRetiring() {
		t.Fatalf("retirement fence lost: registrations=%d retiring=%t", registrations.Load(), session.isRetiring())
	}
	node, err := store.GetNode(t.Context(), "node")
	if err != nil || node.BootSessionID != "next-boot" || node.AgentGenerationID != "next-generation" {
		t.Fatalf("successor projection changed: %+v %v", node, err)
	}
}

func TestPrimaryGenerationRejoinsAfterNodeDeadClaim(t *testing.T) {
	assertPrimaryGenerationRejoinsAfterNodeDead(t, false)
}

func TestPrimaryGenerationRejoinCoalescesConcurrentRefusals(t *testing.T) {
	const claimCallers = 4
	for _, code := range []contract.ErrorCode{contract.ErrorNodeDead, contract.ErrorNodeNotRegistered} {
		for _, scenario := range []struct {
			leader  int
			newLoss bool
		}{{0, false}, {claimCallers, false}, {0, true}, {claimCallers, true}} {
			t.Run(fmt.Sprintf("%s/leader=%d/after_rejoin_refusal=%t", code, scenario.leader, scenario.newLoss), func(t *testing.T) {
				leader := scenario.leader
				type callerKey struct{}
				var active, followers, registering atomic.Bool
				var rejoins, observations atomic.Int32
				var callers [claimCallers + 1]atomic.Int32
				firstRegister := make(chan struct{})
				allObserved := make(chan struct{})
				firstRejoined := make(chan struct{})
				recovered := make(chan int, len(callers))
				session, _, registrations := newGenerationRejoinFixture(t, 10*time.Millisecond, time.Hour, func(format string, args ...any) {
					if fmt.Sprintf(format, args...) == "agent: rejoined node node" && rejoins.Add(1) == 1 {
						close(firstRejoined)
					}
				}, func(r *http.Request, store *l1.Store, _ *manualClock) *http.Response {
					if !active.Load() {
						return nil
					}
					if r.URL.Path == "/v1/agent/nodes/register" && registering.CompareAndSwap(false, true) {
						close(firstRegister)
						select {
						case <-allObserved:
						case <-r.Context().Done():
						}
					}
					caller, tagged := r.Context().Value(callerKey{}).(int)
					if !tagged {
						// The fixture's heartbeat joins the explicit claim callers.
						caller = claimCallers
					}
					isRPC := r.URL.Path == "/v1/agent/nodes/node/heartbeat" || tagged && r.URL.Path == "/v1/agent/jobs/claim"
					if isRPC && (caller == leader || followers.Load()) && callers[caller].CompareAndSwap(0, 1) {
						if code == contract.ErrorNodeNotRegistered {
							return generationReadRefusal(t, r, store, code)
						}
						recorder := httptest.NewRecorder()
						recorder.WriteHeader(http.StatusConflict)
						_ = json.NewEncoder(recorder).Encode(contract.ErrorResponse{Error: contract.APIError{Code: code, Message: "node is dead"}})
						return recorder.Result()
					}
					if r.URL.Path == "/v1/agent/nodes/node/generations" && callers[caller].CompareAndSwap(1, 2) && observations.Add(1) == int32(len(callers)) {
						// The leader is already registering while other callers queue.
						// Delay the final retirement check until that rejoin completes:
						// its refusal still belongs to the same node-session loss.
						close(allObserved)
						select {
						case <-firstRejoined:
						case <-r.Context().Done():
						}
						if scenario.newLoss {
							return generationReadRefusal(t, r, store, contract.ErrorNodeNotRegistered)
						}
					}
					if isRPC && callers[caller].CompareAndSwap(2, 3) {
						recovered <- caller
					}
					return nil
				})
				ctx, cancel := context.WithCancel(t.Context())
				done := make(chan destinationError, claimCallers)
				started := 0
				t.Cleanup(func() {
					cancel()
					for range started {
						select {
						case result := <-done:
							if result.err != nil {
								t.Errorf("claim caller stopped: %v", result.err)
							}
						case <-time.After(testwait.Bound):
							t.Error("claim caller did not stop")
						}
					}
				})
				startClaim := func(caller int) {
					started++
					go func() {
						done <- session.claimClassLoop(context.WithValue(ctx, callerKey{}, caller), make(chan struct{}), workloadClassOneShot, contract.JobClassOneShot,
							func(context.Context, l1.Claim, time.Time) (errorDestination, error) {
								return errorDestinationUnclassified, fmt.Errorf("unexpected resident attempt")
							})
					}()
				}
				active.Store(true)
				if leader < claimCallers {
					startClaim(leader)
				}
				select {
				case <-firstRegister:
				case <-time.After(testwait.Bound):
					t.Fatal("leader did not attempt a rejoin")
				}
				followers.Store(true)
				for caller := range claimCallers {
					if caller != leader {
						startClaim(caller)
					}
				}
				for range callers {
					select {
					case <-recovered:
					case <-time.After(testwait.Bound):
						t.Fatal("not all refused callers recovered")
					}
				}
				wantRejoins := int32(1)
				if scenario.newLoss {
					wantRejoins++
				}
				if registrations.Load() != wantRejoins+1 || rejoins.Load() != wantRejoins {
					t.Fatalf("registrations=%d rejoin logs=%d, want %d registrations and %d logs", registrations.Load(), rejoins.Load(), wantRejoins+1, wantRejoins)
				}
			})
		}
	}
}

func TestPrimaryGenerationRejoinsAfterSubsequentRefusal(t *testing.T) {
	for _, code := range []contract.ErrorCode{contract.ErrorNodeDead, contract.ErrorNodeNotRegistered} {
		for _, heartbeat := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/heartbeat=%t", code, heartbeat), func(t *testing.T) {
				endpoint := "/v1/agent/jobs/claim"
				heartbeatInterval, claimInterval := time.Hour, 10*time.Millisecond
				if heartbeat {
					endpoint = "/v1/agent/nodes/node/heartbeat"
					heartbeatInterval, claimInterval = claimInterval, heartbeatInterval
				}
				var refuse atomic.Bool
				var rejoins atomic.Int32
				session, _, registrations := newGenerationRejoinFixture(t, heartbeatInterval, claimInterval, func(format string, args ...any) {
					if fmt.Sprintf(format, args...) == "agent: rejoined node node" {
						rejoins.Add(1)
					}
				}, func(r *http.Request, store *l1.Store, clock *manualClock) *http.Response {
					if r.URL.Path != endpoint || !refuse.CompareAndSwap(true, false) {
						return nil
					}
					if code == contract.ErrorNodeNotRegistered {
						return generationReadRefusal(t, r, store, code)
					}
					clock.Advance(l1.DefaultNodeDeadAfter)
					if _, err := store.Reconcile(r.Context()); err != nil && r.Context().Err() == nil {
						t.Error(err)
					}
					return nil
				})
				for loss := int32(1); loss <= 2; loss++ {
					// This refusal arrives only after the previous rejoin completed.
					refuse.Store(true)
					retirementWait(t, "a new refusal causes a new rejoin", func() bool {
						return registrations.Load() == loss+1 && rejoins.Load() == loss
					})
				}
				if state := session.observer.snapshot(ClassOccupancy{}, ClassOccupancy{}).State; state != LifecycleReady {
					t.Fatalf("session state=%s, want Ready", state)
				}
			})
		}
	}
}

func assertPrimaryGenerationRejoinsAfterNodeDead(t *testing.T, heartbeat bool) {
	t.Helper()
	endpoint := "/v1/agent/nodes/node/heartbeat"
	heartbeatInterval, claimInterval := 10*time.Millisecond, time.Hour
	if !heartbeat {
		endpoint = "/v1/agent/jobs/claim"
		heartbeatInterval, claimInterval = time.Hour, 10*time.Millisecond
	}
	var expire atomic.Bool
	var heartbeatErrors, rejoins atomic.Int32
	logf := func(format string, args ...any) {
		line := fmt.Sprintf(format, args...)
		if strings.HasPrefix(line, "agent: heartbeat failed:") && strings.Contains(line, "node_dead") {
			heartbeatErrors.Add(1)
		}
		if line == "agent: rejoined node node" {
			rejoins.Add(1)
		}
	}
	refused := make(chan struct{}, 1)
	session, _, registrations := newGenerationRejoinFixture(t, heartbeatInterval, claimInterval, logf, func(request *http.Request, store *l1.Store, clock *manualClock) *http.Response {
		if request.URL.Path != endpoint || !expire.CompareAndSwap(true, false) {
			return nil
		}
		// Expire only L1's clock, while the actual request is in flight.
		clock.Advance(l1.DefaultNodeDeadAfter)
		if _, err := store.Reconcile(request.Context()); err != nil {
			t.Error(err)
		}
		node, err := store.GetNode(request.Context(), "node")
		if err != nil || node.State != contract.NodeDead {
			t.Errorf("Node did not become dead: %+v %v", node, err)
		}
		refused <- struct{}{}
		return nil
	})
	expire.Store(true)
	select {
	case <-refused:
	case <-time.After(testwait.Bound):
		t.Fatal("request did not reach the dead Node")
	}
	deadline := time.After(2 * time.Second)
	for registrations.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("primary generation did not register again after node_dead: registrations=%d", registrations.Load())
		case <-time.After(time.Millisecond):
		}
	}
	var nodes l1.NodeList
	if err := session.client.request(t.Context(), http.MethodGet, "/v1/nodes", nil, &nodes); err != nil {
		t.Fatal(err)
	}
	if len(nodes.Nodes) != 1 {
		t.Fatalf("status Nodes=%+v", nodes.Nodes)
	}
	node := nodes.Nodes[0]
	if node.MaxOneshotSlots <= node.OneshotOccupancy || node.State != contract.NodeAlive || !node.ClaimsEnabled || !node.Capabilities["kind:process"] || node.AgentGenerationID != "generation" || node.BootSessionID != "boot" {
		t.Fatalf("status readiness did not recover: %+v", node)
	}
	if len(node.Generations) != 1 || node.Generations[0].Role != l1.NodeGenerationPrimary || node.Generations[0].ObservationError != "" {
		t.Fatalf("generation session is unhealthy: %+v", node.Generations)
	}
	retirementWait(t, "agent Ready after rejoin", func() bool {
		return session.observer.snapshot(ClassOccupancy{}, ClassOccupancy{}).State == LifecycleReady && rejoins.Load() == 1
	})
	if heartbeat && heartbeatErrors.Load() != 1 {
		t.Fatalf("heartbeat node_dead log lines=%d, want one", heartbeatErrors.Load())
	}
}

func TestRetiringGenerationDoesNotRejoinAfterNodeDeadHeartbeat(t *testing.T) {
	for _, race := range []bool{false, true} {
		t.Run(fmt.Sprintf("promotion_races_heartbeat=%t", race), func(t *testing.T) {
			var promote atomic.Bool
			var heartbeatErrors atomic.Int32
			logf := func(format string, args ...any) {
				if strings.Contains(fmt.Sprintf(format, args...), "agent: heartbeat failed:") {
					heartbeatErrors.Add(1)
				}
			}
			refused := make(chan struct{}, 10)
			session, store, registrations := newGenerationRejoinFixture(t, 10*time.Millisecond, time.Hour, logf, func(request *http.Request, store *l1.Store, _ *manualClock) *http.Response {
				if request.URL.Path == "/v1/agent/nodes/node/heartbeat" && promote.CompareAndSwap(true, false) {
					promoteRetirementFixture(t, store, "node", "agent")
				}
				if strings.HasSuffix(request.URL.Path, "/generations/generation/heartbeat") {
					recorder := httptest.NewRecorder()
					recorder.WriteHeader(http.StatusConflict)
					_ = json.NewEncoder(recorder).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorNodeDead, Message: "node is dead"}})
					select {
					case refused <- struct{}{}:
					default:
					}
					return recorder.Result()
				}
				return nil
			})
			if race {
				promote.Store(true)
			} else {
				promoteRetirementFixture(t, store, "node", "agent")
				if retiring, err := session.observeGeneration(t.Context()); err != nil || !retiring {
					t.Fatalf("observe retirement: %t %v", retiring, err)
				}
			}
			// Two refused retiring heartbeats prove that the loop stays alive
			// through the same error without returning to registration.
			for range 2 {
				select {
				case <-refused:
				case <-time.After(testwait.Bound):
					t.Fatal("retiring heartbeat retries stopped")
				}
			}
			loggedErrors := heartbeatErrors.Load()
			if loggedErrors != 1 {
				t.Fatalf("heartbeat error log lines=%d, want one across retries", loggedErrors)
			}
			if registrations.Load() != 1 {
				t.Fatalf("retiree registered again: %d registrations", registrations.Load())
			}
			node, err := store.GetNode(t.Context(), "node")
			if err != nil || node.BootSessionID != "next-boot" || node.AgentGenerationID != "next-generation" {
				t.Fatalf("retiree replaced successor: %+v %v", node, err)
			}
			if status := session.observer.snapshot(ClassOccupancy{}, ClassOccupancy{}); status.State != LifecycleRetiring {
				t.Fatalf("retiree lifecycle=%s", status.State)
			}
		})
	}
}
