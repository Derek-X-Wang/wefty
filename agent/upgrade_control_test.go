//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/internal/upgradecompat"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func newUpgradeControlStore(t *testing.T) (agentinstall.Store, agentinstall.Generation) {
	t.Helper()
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"data", "LaunchAgents"} {
		if err = os.Mkdir(filepath.Join(home, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	source := filepath.Join(home, "source")
	if err = os.WriteFile(source, []byte("test binary"), 0700); err != nil {
		t.Fatal(err)
	}
	store := agentinstall.Store{Data: filepath.Join(home, "data"), LaunchAgents: filepath.Join(home, "LaunchAgents")}
	g, err := store.Initialize("node", source, "test", "darwin:11111111-1111-1111-1111-111111111111", []int{1})
	if err != nil {
		t.Fatal(err)
	}
	return store, g
}

func TestUpgradeControlPersistsProofAndReplaysSameOperation(t *testing.T) {
	store, g := newUpgradeControlStore(t)
	var err error
	clock := systemClock{}
	var admissionOpen atomic.Bool
	client := newRoundTripClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/generations"):
			_ = json.NewEncoder(w).Encode(l1.NodeGenerationInventory{NodeID: "node", Generations: []l1.NodeGeneration{{AgentGenerationID: g.ID, BootSessionID: "boot", Role: l1.NodeGenerationPrimary}}})
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			_ = json.NewEncoder(w).Encode(l1.HeartbeatResponse{Node: l1.Node{NodeRegistration: contract.NodeRegistration{NodeID: "node", AgentGenerationID: g.ID, BootSessionID: "boot"}, ClaimsEnabled: admissionOpen.Load(), IntentRevision: 1, IntentReason: "maintenance"}})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	s := newAgentSession(client, contract.NodeRegistration{NodeID: "node", AgentGenerationID: g.ID, BootSessionID: "boot"}, nil, time.Second, time.Millisecond, clock, newLifecycleObserver(clock), nil, 1, 1)
	a := &Agent{session: s}
	payloadContext, payloadCancel := context.WithCancelCause(t.Context())
	defer payloadCancel(nil)
	s.resident["one-shot"] = &residentAttempt{attemptID: "kept", class: contract.JobClassOneShot, kind: contract.JobKindProcess, cancel: payloadCancel}
	s.residentJobID["one-shot"] = struct{}{}
	s.retirementAttempts["kept"] = &retirementAttempt{jobID: "one-shot", class: contract.JobClassOneShot, kind: contract.JobKindProcess}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { defer close(done); done <- a.ServeUpgradeControl(ctx, store) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testwait.Bound):
			t.Error("control did not stop")
		}
	})
	proof := UpgradeServiceQuiescence{IntentRevision: 1, IntentReason: "maintenance", OperationID: "upgrade", NodeID: "node", AgentGenerationID: g.ID, BootSessionID: "boot", Services: []l1.NodeActiveAttempt{}}
	request := agentinstall.ControlRequest{ID: "upgrade", RequestID: "first", NodeID: "node", GenerationID: g.ID, BootSessionID: "boot", Action: "quiesce_services"}
	bad := proof
	bad.Services = []l1.NodeActiveAttempt{{JobID: "unowned", AttemptID: "unowned", BootSessionID: "boot", Class: contract.JobClassService, Kind: contract.JobKindProcess}}
	request.Payload, _ = json.Marshal(bad)
	if err = store.RequestControl(request); err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "refused control response", func() bool {
		result, err := store.ReadControlResult(g.ID)
		return err == nil && result.Request.RequestID == "first" && result.Error != ""
	})
	request.RequestID = "retry"
	request.Payload, _ = json.Marshal(proof)
	if err = store.RequestControl(request); err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "positive durable control response", func() bool {
		result, err := store.ReadControlResult(g.ID)
		if err != nil || result.Request.RequestID != "retry" || result.Error != "" {
			return false
		}
		var accepted UpgradeServiceQuiescence
		return json.Unmarshal(result.Payload, &accepted) == nil && accepted.OperationID == proof.OperationID && accepted.AgentGenerationID == g.ID && len(accepted.Services) == 0
	})
	if payloadContext.Err() != nil {
		t.Fatal("installed control canceled a one-shot")
	}
	// A request file is not authority to enter another boot's runtime.
	request.RequestID = "wrong-boot"
	request.BootSessionID = "other-boot"
	if err = store.RequestControl(request); err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "stale boot refusal", func() bool {
		result, err := store.ReadControlResult(g.ID)
		return err == nil && result.Request.RequestID == "wrong-boot" && strings.Contains(result.Error, "identity mismatch")
	})
	request.RequestID = "after-stale"
	request.BootSessionID = "boot"
	if err = store.RequestControl(request); err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "control available after stale boot", func() bool {
		result, err := store.ReadControlResult(g.ID)
		return err == nil && result.Request.RequestID == "after-stale" && result.Error == ""
	})

	// A resumed operation is terminal, regardless of the transport request ID.
	resume := request
	resume.RequestID, resume.Action, resume.Payload = "resume", "resume_services", nil
	if err = store.RequestControl(resume); err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "resume response", func() bool {
		result, err := store.ReadControlResult(g.ID)
		return err == nil && result.Request.RequestID == "resume" && result.Error == ""
	})
	admissionOpen.Store(false)
	for _, requestID := range []string{"after-stale", "changed-replay"} {
		request.RequestID = requestID
		if err = store.RequestControl(request); err != nil {
			t.Fatal(err)
		}
		retirementWait(t, "replay response", func() bool {
			result, err := store.ReadControlResult(g.ID)
			return err == nil && result.Request.RequestID == requestID
		})
		result, err := store.ReadControlResult(g.ID)
		if err != nil || result.Error == "" {
			t.Fatalf("resumed quiescence replay accepted: %+v %v", result, err)
		}
		s.claimMu.Lock()
		pending := s.upgradeServiceOperation
		s.claimMu.Unlock()
		if pending != "" {
			t.Fatal("replay closed service admission", pending)
		}
	}
	admissionOpen.Store(true)
	// A fresh operation still needs the current, closed admission fence.
	request.ID = "new-upgrade"
	request.RequestID = "open-admission"
	proof.OperationID = request.ID
	request.Payload, _ = json.Marshal(proof)
	if err = store.RequestControl(request); err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "open admission refusal", func() bool {
		result, err := store.ReadControlResult(g.ID)
		return err == nil && result.Request.RequestID == request.RequestID
	})
	result, err := store.ReadControlResult(g.ID)
	if err != nil || result.Error == "" {
		t.Fatalf("new quiescence accepted with open admission: %+v %v", result, err)
	}

	admissionOpen.Store(false)
	proof.IntentRevision = 0
	request.RequestID = "stale-revision"
	request.Payload, _ = json.Marshal(proof)
	if err = store.RequestControl(request); err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "stale intent refusal", func() bool {
		result, err := store.ReadControlResult(g.ID)
		return err == nil && result.Request.RequestID == request.RequestID
	})
	result, err = store.ReadControlResult(g.ID)
	if err != nil || result.Error == "" {
		t.Fatalf("new quiescence accepted with stale intent: %+v %v", result, err)
	}
	close(s.retiringRequested)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testwait.Bound):
		t.Fatal("retiree kept polling installation control")
	}
	if payloadContext.Err() != nil {
		t.Fatal("control retirement canceled a one-shot")
	}
}

func TestUpgradeRetirementAcknowledgmentPrecedesIdleExit(t *testing.T) {
	ack := make(chan l1.NodeGeneration, 1)
	_, store, done, _ := newRetirementFixtureWithHook(t, func(a *Agent, _ *l1.Store) {
		a.session.retirementAcknowledged = func(g l1.NodeGeneration) error { ack <- g; return nil }
	})
	promoteRetirementFixture(t, store, "node", "retirement-agent")
	select {
	case g := <-ack:
		if g.Role != l1.NodeGenerationRetiring || g.BootSessionID != "boot" || g.PromotionOperationID != "promotion" {
			t.Fatal("wrong retirement ack", g)
		}
	case <-time.After(testwait.Bound):
		t.Fatal("missing retirement ack")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testwait.Bound):
		t.Fatal("idle retiree did not finish")
	}
}

func TestUpgradeControlAdmissionFenceRealL1(t *testing.T) {
	installed, g := newUpgradeControlStore(t)
	network := plain.NewNetwork()
	authority, stop := startFailureServerWithPoliciesAndLease(t, network, nil, nil, 2*time.Second)
	t.Cleanup(stop)
	identity := fabric.Identity{NodeID: "installed-control-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}
	client, err := newClient(network.NewFabric(identity), "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	clock := systemClock{}
	capabilities := newCapabilityState(map[string]bool{"kind:process": true, "upgrade:retirement:v1": true}, nil, clock, time.Second, nil)
	observation := capabilities.snapshot()
	registration := contract.NodeRegistration{NodeID: "node", AgentGenerationID: g.ID, BootSessionID: "boot", OS: "darwin", Architecture: "arm64", AgentVersion: "test", Capabilities: observation.Capabilities, CapabilityRevision: observation.Revision, CapabilityObservedAt: observation.ObservedAt}
	node, err := authority.RegisterNode(t.Context(), identity, registration, l1.DefaultNodePolicy(), true)
	if err != nil {
		t.Fatal(err)
	}
	node, err = authority.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{IntentRevision: node.IntentRevision, ClaimsEnabled: false, Reason: "upgrade"})
	if err != nil {
		t.Fatal(err)
	}
	session := newAgentSession(client, registration, capabilities, time.Second, time.Millisecond, clock, newLifecycleObserver(clock), nil, 1, 1)
	a := &Agent{session: session}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- a.ServeUpgradeControl(ctx, installed) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testwait.Bound):
			t.Error("control adapter did not join")
		}
	})
	proof := UpgradeServiceQuiescence{OperationID: "operation", NodeID: "node", AgentGenerationID: g.ID, BootSessionID: "boot", IntentRevision: node.IntentRevision, IntentReason: node.IntentReason, Services: []l1.NodeActiveAttempt{}}
	payload, _ := json.Marshal(proof)
	request := agentinstall.ControlRequest{ID: proof.OperationID, RequestID: "quiesce", NodeID: "node", GenerationID: g.ID, BootSessionID: "boot", Action: "quiesce_services", Payload: payload}
	exchange := func(r agentinstall.ControlRequest, wantError bool) {
		t.Helper()
		if err := installed.RequestControl(r); err != nil {
			t.Fatal(err)
		}
		retirementWait(t, "real L1 control result", func() bool {
			result, err := installed.ReadControlResult(g.ID)
			return err == nil && result.Request.RequestID == r.RequestID
		})
		result, err := installed.ReadControlResult(g.ID)
		if err != nil || (result.Error != "") != wantError {
			t.Fatalf("result=%+v error=%v wantError=%t", result, err, wantError)
		}
	}
	exchange(request, false)
	resume := request
	resume.Action, resume.RequestID, resume.Payload = "resume_services", "resume", nil
	exchange(resume, false)
	exchange(request, true)
	request.RequestID = "changed-replay"
	exchange(request, true)
	node, err = authority.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{IntentRevision: node.IntentRevision, ClaimsEnabled: true, Reason: "operator resumed"})
	if err != nil {
		t.Fatal(err)
	}
	proof.OperationID = "next-operation"
	proof.IntentRevision, proof.IntentReason = node.IntentRevision, node.IntentReason
	request.ID, request.RequestID = proof.OperationID, "new-with-open-admission"
	request.Payload, _ = json.Marshal(proof)
	exchange(request, true)
	session.claimMu.Lock()
	defer session.claimMu.Unlock()
	if session.upgradeServiceOperation != "" {
		t.Fatal("replay or stale request closed local admission")
	}
}

func TestUpgradeControlRejectsForgedUnlistedActions(t *testing.T) {
	store, g := newUpgradeControlStore(t)
	clock := systemClock{}
	session := newAgentSession(nil, contract.NodeRegistration{NodeID: "node", AgentGenerationID: g.ID, BootSessionID: "boot"}, nil, time.Second, time.Millisecond, clock, newLifecycleObserver(clock), nil, 1, 1)
	a := &Agent{session: session}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- a.ServeUpgradeControl(ctx, store) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	for _, action := range []string{"answer_seat_approval", "POST /v1/agent/seat-approvals/seat/answer", "close_generation"} {
		request := agentinstall.ControlRequest{ID: "operation", RequestID: action, NodeID: "node", GenerationID: g.ID, BootSessionID: "boot", Action: action, Payload: []byte(`{"approved":true}`)}
		if err := store.RequestControl(request); err == nil {
			t.Fatal("unlisted action accepted by writer", action)
		}
		// The OS account can forge a private record. The runtime must still
		// reject an unlisted operation without sending any node request.
		body, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(store.Root(), g.ID, "control-request.json"), body, 0600); err != nil {
			t.Fatal(err)
		}
		retirementWait(t, "unlisted action refusal", func() bool {
			result, err := store.ReadControlResult(g.ID)
			return err == nil && result.Request.RequestID == action && strings.Contains(result.Error, "unknown installed control action") && result.APIError != nil && result.APIError.Code == contract.ErrorConflict && result.StatusCode == http.StatusConflict && result.APIError.Details["reason"] == string(upgradecompat.InvalidInventory)
		})
	}
}

func TestUpgradeControlServiceBarrierResultsRetainErrorType(t *testing.T) {
	for _, reason := range []string{"unowned", "omitted", "reap", "reap-timeout", "evidence-L1", "heartbeat-decode", "evidence-persistence", "evidence-persistence-stop", "evidence-transport", "heartbeat-empty"} {
		t.Run(reason, func(t *testing.T) {
			store, g := newUpgradeControlStore(t)
			client := newRoundTripClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if reason == "evidence-transport" && strings.HasSuffix(r.URL.Path, "/logs") {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
					return
				}
				if reason == "heartbeat-empty" {
					_, _ = w.Write([]byte(`{}`))
					return
				}
				if reason == "heartbeat-decode" {
					_, _ = w.Write([]byte("invalid JSON"))
					return
				}
				_ = json.NewEncoder(w).Encode(l1.HeartbeatResponse{Node: l1.Node{NodeRegistration: contract.NodeRegistration{NodeID: "node", AgentGenerationID: g.ID, BootSessionID: "boot"}, IntentRevision: 1, IntentReason: "upgrade"}})
			}))
			clock := systemClock{}
			s := newAgentSession(client, contract.NodeRegistration{NodeID: "node", AgentGenerationID: g.ID, BootSessionID: "boot"}, nil, time.Second, time.Millisecond, clock, newLifecycleObserver(clock), nil, 1, 1)
			proof := UpgradeServiceQuiescence{OperationID: "operation", NodeID: "node", AgentGenerationID: g.ID, BootSessionID: "boot", IntentRevision: 1, IntentReason: "upgrade", Services: []l1.NodeActiveAttempt{}}
			switch reason {
			case "unowned":
				proof.Services = []l1.NodeActiveAttempt{{JobID: "service", AttemptID: "attempt", Class: contract.JobClassService, Kind: contract.JobKindProcess, BootSessionID: "boot"}}
			case "omitted":
				s.retirementAttempts["attempt"] = &retirementAttempt{jobID: "service", class: contract.JobClassService, kind: contract.JobKindProcess}
				s.residentJobID["service"] = struct{}{}
			case "reap", "reap-timeout", "evidence-L1", "evidence-persistence", "evidence-persistence-stop", "evidence-transport":
				record := &retirementAttempt{jobID: "service", class: contract.JobClassService, kind: contract.JobKindProcess, finished: true}
				if reason == "reap-timeout" {
					record.reapErr = context.DeadlineExceeded
				}
				if reason == "evidence-L1" {
					record.reaped = true
					if reason == "evidence-persistence-stop" {
						record.completionLease = &upgradeServiceLease{stopCause: &ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: contract.ErrorConflict, Message: "prior lease refusal"}}}
					}
					record.checkEvidence = func(context.Context) (bool, error) {
						return false, &ProtocolError{StatusCode: http.StatusForbidden, APIError: contract.APIError{Code: contract.ErrorForbidden, Message: "L1 refused evidence"}}
					}
				}
				if strings.HasPrefix(reason, "evidence-persistence") || reason == "evidence-transport" {
					// Exercise real recovery, SQL persistence and log transport,
					// not only a synthetic UpgradeServiceQuiescenceError.
					outbox, err := newEvidenceOutbox(t.TempDir(), "node", 1024, clock, 8, time.Hour, time.Millisecond)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = outbox.Close() })
					claim := spoolTestClaim("attempt")
					claim.Job.JobID = "service"
					claim.Job.Spec.Class, claim.Job.Spec.Kind = contract.JobClassService, contract.JobKindProcess
					request := completionRequest(claim, agentTerminatedResult(contract.ProcessResult{}), workloadrunner.ReapEvidenceAttempt)
					lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, outbox: outbox, clock: clock, completionRetry: time.Millisecond})
					lifecycle.completion.Store(&request)
					if strings.HasPrefix(reason, "evidence-persistence") {
						outbox.spool.db.SetMaxOpenConns(1)
						if _, err = outbox.spool.db.ExecContext(t.Context(), "PRAGMA query_only=ON"); err != nil {
							t.Fatal(err)
						}
					} else {
						if err = outbox.ensureAttempt(t.Context(), claim); err != nil {
							t.Fatal(err)
						}
						if err = outbox.storeDurableCompletion(t.Context(), claim.Lease.AttemptID, durableCompletionOf(request), time.Now()); err != nil {
							t.Fatal(err)
						}
						if err = outbox.spool.append(t.Context(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "retained bytes")); err != nil {
							t.Fatal(err)
						}
					}
					record.reaped = true
					if reason == "evidence-persistence-stop" {
						record.completionLease = &upgradeServiceLease{stopCause: &ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: contract.ErrorConflict, Message: "prior lease refusal"}}}
					}
					record.checkEvidence = func(context.Context) (bool, error) { return false, nil }
					record.recoverEvidence = func(ctx context.Context) error { return lifecycle.recoverUpgradeServiceEvidence(ctx, claim) }
					s.upgradeServiceOperation = proof.OperationID
					s.upgradeServiceIntent = l1.NodeIntentRequest{IntentRevision: proof.IntentRevision, Reason: proof.IntentReason}
				}
				s.retirementAttempts["attempt"] = record
			}
			a := &Agent{session: s}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- a.ServeUpgradeControl(ctx, store) }()
			t.Cleanup(func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			})
			body, _ := json.Marshal(proof)
			r := agentinstall.ControlRequest{ID: proof.OperationID, RequestID: reason, NodeID: "node", GenerationID: g.ID, BootSessionID: "boot", Action: "quiesce_services", Payload: body}
			if err := store.RequestControl(r); err != nil {
				t.Fatal(err)
			}
			retirementWait(t, "barrier refusal result", func() bool {
				result, err := store.ReadControlReply(r)
				return err == nil && result.Request.RequestID == reason && result.Error != ""
			})
			result, err := store.ReadControlReply(r)
			if err != nil {
				t.Fatal(err)
			}
			switch reason {
			case "heartbeat-decode", "heartbeat-empty", "reap-timeout", "evidence-persistence", "evidence-persistence-stop", "evidence-transport":
				if result.APIError != nil {
					t.Fatal("operational failure became compatibility decision", result)
				}
				if strings.HasPrefix(reason, "evidence-persistence") || reason == "evidence-transport" {
					if result.ServiceFailure == nil || result.ServiceFailure.AttemptID != "attempt" {
						t.Fatal("lost structured service diagnostic", result)
					}
					fragments := []string{"service", "attempt", "evidence"}
					if strings.HasPrefix(reason, "evidence-persistence") {
						fragments = []string{"service", "attempt", "persistence", "store log spool attempt", "readonly"}
					} else {
						fragments = append(fragments, "EOF")
					}
					for _, fragment := range fragments {
						if !strings.Contains(result.Error, fragment) {
							t.Error("lost diagnostic", fragment, result.Error)
						}
					}
				}
			case "evidence-L1":
				if result.APIError == nil || result.APIError.Code != contract.ErrorForbidden || result.StatusCode != http.StatusForbidden {
					t.Fatal("typed L1 cause changed", result)
				}
			default:
				if result.APIError == nil || result.APIError.Code != contract.ErrorConflict || result.StatusCode != http.StatusConflict || result.APIError.Details["reason"] != string(upgradecompat.InvalidInventory) {
					t.Fatal("untyped barrier refusal", result)
				}
				if reason == "reap" && (result.ServiceFailure == nil || result.ServiceFailure.AttemptID != "attempt" || result.ServiceFailure.Phase != "reap") {
					t.Fatal("lost barrier failure details", result)
				}
			}
		})
	}
}

func TestUpgradeControlStagedAbortUsesCurrentPrimaryAndExactOperation(t *testing.T) {
	store, primary := newUpgradeControlStore(t)
	boot := "darwin:11111111-1111-1111-1111-111111111111"
	candidate, err := store.PrepareCandidate(primary.Executable, "next", boot, []int{1}, "operation")
	if err != nil {
		t.Fatal(err)
	}
	var closes atomic.Int32
	client := newRoundTripClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			_ = json.NewEncoder(w).Encode(l1.HeartbeatResponse{Node: l1.Node{NodeRegistration: contract.NodeRegistration{NodeID: "node", AgentGenerationID: primary.ID, BootSessionID: "primary-boot"}, AuthorityGeneration: 1, IntentRevision: 2, IntentReason: "closed"}})
		case strings.HasSuffix(r.URL.Path, "/close"):
			var request l1.CloseNodeGenerationRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if request.Abort == nil || request.Abort.OperationID != "operation" || request.BootSessionID != candidate.CandidateBootSessionID {
				t.Error("changed abort request")
			}
			closes.Add(1)
			_ = json.NewEncoder(w).Encode(l1.NodeGenerationInventory{NodeID: "node"})
		case strings.HasSuffix(r.URL.Path, "/generations"):
			_ = json.NewEncoder(w).Encode(l1.NodeGenerationInventory{NodeID: "node", Generations: []l1.NodeGeneration{{AgentGenerationID: primary.ID, BootSessionID: "primary-boot", Role: l1.NodeGenerationPrimary}, {AgentGenerationID: candidate.ID, BootSessionID: candidate.CandidateBootSessionID, Role: l1.NodeGenerationClosed, PromotionOperationID: "operation"}}})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
		}
	}))
	clock := systemClock{}
	session := newAgentSession(client, contract.NodeRegistration{NodeID: "node", AgentGenerationID: primary.ID, BootSessionID: "primary-boot", SystemBootID: boot}, nil, time.Second, time.Second, clock, newLifecycleObserver(clock), nil, 1, 1)
	a := &Agent{session: session}
	payload := agentinstall.StagedAbortControl{CandidateGenerationID: candidate.ID, Request: l1.CloseNodeGenerationRequest{BootSessionID: candidate.CandidateBootSessionID, PromotionOperationID: "operation", ResidentAttempts: []l1.ResidentAttempt{}, RuntimesReaped: true, EvidenceSettled: true, Abort: &l1.StagedGenerationAbort{OperationID: "operation", PrimaryGenerationID: primary.ID, PrimaryBootSessionID: "primary-boot", AuthorityGeneration: 1, IntentRevision: 2, IntentReason: "closed"}}}
	raw, _ := json.Marshal(payload)
	control := agentinstall.ControlRequest{ID: "different", Payload: raw}
	if _, err = a.abortStaged(t.Context(), store, control); err == nil || closes.Load() != 0 {
		t.Fatal("changed operation reached L1", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- a.ServeUpgradeControl(ctx, store) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	send := func(action string, payload json.RawMessage) {
		t.Helper()
		request := agentinstall.ControlRequest{ID: "operation", RequestID: action, NodeID: "node", GenerationID: primary.ID, BootSessionID: "primary-boot", Action: action, Payload: payload}
		if err := store.RequestControl(request); err != nil {
			t.Fatal(err)
		}
		retirementWait(t, "recovery adapter reply", func() bool {
			reply, err := store.ReadControlReply(request)
			return err == nil && reply.Request.RequestID == request.RequestID
		})
		reply, err := store.ReadControlReply(request)
		if err != nil || reply.Error != "" {
			t.Fatalf("%s: %+v %v", action, reply, err)
		}
	}
	proof := UpgradeServiceQuiescence{OperationID: "operation", NodeID: "node", AgentGenerationID: primary.ID, BootSessionID: "primary-boot", IntentRevision: 2, IntentReason: "closed", Services: []l1.NodeActiveAttempt{}}
	proofRaw, _ := json.Marshal(proof)
	send("quiesce_services", proofRaw)
	send("abort_staged_generation", raw)
	send("resume_services", nil)
	if closes.Load() != 1 {
		t.Fatal("staged close repeated or bypassed")
	}
	session.claimMu.Lock()
	resumed := session.upgradeServiceResumed["operation"] && session.upgradeServiceOperation == ""
	session.claimMu.Unlock()
	if !resumed {
		t.Fatal("real resume adapter did not release the recovery gate")
	}
}
