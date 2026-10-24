//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func TestUpgradeServicesBarrierKeepsOneShotsAndWaitsForEvidence(t *testing.T) {
	clock := systemClock{}
	s := newAgentSession(nil, contract.NodeRegistration{NodeID: "node", AgentGenerationID: "generation", BootSessionID: "boot"}, nil, time.Second, time.Millisecond, clock, newLifecycleObserver(clock), nil, 2, 2)
	a := &Agent{session: s}
	request := UpgradeServiceQuiescence{OperationID: "activation", NodeID: "node", AgentGenerationID: "generation", BootSessionID: "boot", Services: []l1.NodeActiveAttempt{}}
	var payloads []context.Context
	var evidence atomic.Bool
	checkedPending := make(chan struct{}, 1)
	for i, id := range []string{"ordinary", "restartable", "always", "never"} {
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)
		payloads = append(payloads, ctx)
		class := contract.JobClassOneShot
		if i >= 2 {
			class = contract.JobClassService
			request.Services = append(request.Services, l1.NodeActiveAttempt{JobID: id, AttemptID: id, BootSessionID: "boot", Class: class, Kind: contract.JobKindProcess})
		}
		s.residentJobID[id] = struct{}{}
		s.resident[id] = &residentAttempt{attemptID: id, class: class, kind: contract.JobKindProcess, cancel: cancel, restartable: i == 1}
		s.retirementAttempts[id] = &retirementAttempt{jobID: id, class: class, kind: contract.JobKindProcess, checkEvidence: func(context.Context) (bool, error) {
			if !evidence.Load() {
				select {
				case checkedPending <- struct{}{}:
				default:
				}
				return false, nil
			}
			return true, nil
		}}
	}
	done := make(chan error, 1)
	go func() { _, err := a.QuiesceUpgradeServices(t.Context(), request); done <- err }()
	for _, ctx := range payloads[2:] {
		select {
		case <-ctx.Done():
			if !errors.Is(context.Cause(ctx), errUpgradeServiceQuiescence) {
				t.Fatal(context.Cause(ctx))
			}
		case <-time.After(5 * time.Second):
			t.Fatal("service not canceled")
		}
	}
	for _, ctx := range payloads[:2] {
		if ctx.Err() != nil {
			t.Fatal("one-shot canceled by service barrier")
		}
	}
	select {
	case err := <-done:
		t.Fatalf("barrier returned before reap: %v", err)
	default:
	}
	for _, id := range []string{"always", "never"} {
		s.recordRuntimeReap(id, workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil)
		s.claimMu.Lock()
		delete(s.resident, id)
		delete(s.residentJobID, id)
		s.claimMu.Unlock()
	}
	select {
	case <-checkedPending:
	case <-time.After(5 * time.Second):
		t.Fatal("service evidence was never checked")
	}
	select {
	case err := <-done:
		t.Fatalf("barrier returned before evidence delivery: %v", err)
	default:
	}
	evidence.Store(true)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("settled services kept barrier alive")
	}
	if _, err := a.QuiesceUpgradeServices(t.Context(), request); err != nil {
		t.Fatal("replay:", err)
	}
	changed := request
	changed.Services = request.Services[:1]
	if _, err := a.QuiesceUpgradeServices(t.Context(), changed); err == nil {
		t.Fatal("changed replay accepted")
	}
	// Admission is closed before touching capabilities or sending another RPC.
	if c, sent, err := s.claim(t.Context(), s.gates[workloadClassService], contract.JobClassService, false); err != nil || sent || c != nil {
		t.Fatalf("service admitted after quiescence: %+v %v %v", c, sent, err)
	}
	for _, ctx := range payloads[:2] {
		if ctx.Err() != nil {
			t.Fatal("replay canceled one-shot")
		}
	}
}

func TestUpgradeServicesRejectsLegacyAndIncompleteInventory(t *testing.T) {
	clock := systemClock{}
	s := newAgentSession(nil, contract.NodeRegistration{NodeID: "node", BootSessionID: "boot"}, nil, time.Second, time.Millisecond, clock, newLifecycleObserver(clock), nil, 1, 1)
	a := &Agent{session: s}
	request := UpgradeServiceQuiescence{OperationID: "upgrade", NodeID: "node", BootSessionID: "boot"}
	if _, err := a.QuiesceUpgradeServices(t.Context(), request); err == nil || s.upgradeServiceOperation != "" {
		t.Fatal("legacy session enrolled or changed")
	}
	s.registration.AgentGenerationID = "generation"
	request.AgentGenerationID = "generation"
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	s.resident["service"] = &residentAttempt{attemptID: "attempt", class: contract.JobClassService, kind: contract.JobKindProcess, cancel: cancel}
	s.residentJobID["service"] = struct{}{}
	s.retirementAttempts["attempt"] = &retirementAttempt{jobID: "service", class: contract.JobClassService, kind: contract.JobKindProcess}
	if _, err := a.QuiesceUpgradeServices(t.Context(), request); err == nil || ctx.Err() != nil || s.upgradeServiceOperation != "" {
		t.Fatal("partial inventory canceled work or closed gate")
	}
}

func TestUpgradeServicesResumeRequiresSettledPrimary(t *testing.T) {
	var primary atomic.Bool
	primary.Store(true)
	client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		role := l1.NodeGenerationRetiring
		if primary.Load() {
			role = l1.NodeGenerationPrimary
		}
		_ = json.NewEncoder(w).Encode(l1.NodeGenerationInventory{NodeID: "node", Generations: []l1.NodeGeneration{{AgentGenerationID: "generation", BootSessionID: "boot", Role: role}}})
	}), time.Second)
	defer stop()
	defer client.Close()
	clock := systemClock{}
	s := newAgentSession(client, contract.NodeRegistration{NodeID: "node", AgentGenerationID: "generation", BootSessionID: "boot"}, nil, time.Second, time.Millisecond, clock, newLifecycleObserver(clock), nil, 1, 1)
	a := &Agent{session: s}
	s.upgradeServiceOperation = "activation"
	if err := a.ResumeUpgradeServiceClaims(t.Context(), "activation"); err == nil || s.upgradeServiceOperation == "" {
		t.Fatal("unsettled services resumed")
	}
	s.upgradeServicesSettled = true
	if err := a.ResumeUpgradeServiceClaims(t.Context(), "different"); err == nil {
		t.Fatal("wrong operation resumed")
	}
	primary.Store(false)
	if err := a.ResumeUpgradeServiceClaims(t.Context(), "activation"); err == nil {
		t.Fatal("retiring generation resumed")
	}
	primary.Store(true)
	if err := a.ResumeUpgradeServiceClaims(t.Context(), "activation"); err != nil || s.upgradeServiceOperation != "" {
		t.Fatalf("pre-promotion recovery failed: %v", err)
	}
	if err := a.ResumeUpgradeServiceClaims(t.Context(), "activation"); err != nil {
		t.Fatal("resume lost-reply replay:", err)
	}
}

func TestUpgradeServicesRemovalSettlesOnlyAfterCleanupAcknowledged(t *testing.T) {
	for _, fail := range []string{"purge", "resource", "ack", "finish", ""} {
		t.Run(fail, func(t *testing.T) {
			clock := systemClock{}
			s := newAgentSession(nil, contract.NodeRegistration{NodeID: "node", AgentGenerationID: "generation", BootSessionID: "boot"}, nil, time.Second, time.Millisecond, clock, newLifecycleObserver(clock), nil, 1, 1)
			service := &retirementAttempt{jobID: "removed", class: contract.JobClassService, kind: contract.JobKindProcess, finished: true, reaped: true, checkEvidence: func(context.Context) (bool, error) { return false, nil }}
			oneShot := &retirementAttempt{jobID: "removed", class: contract.JobClassOneShot, kind: contract.JobKindProcess, finished: true}
			s.retirementAttempts["service"] = service
			s.retirementAttempts["one-shot"] = oneShot
			step := func(name string) error {
				if name == fail {
					return errors.New("injected " + name)
				}
				return nil
			}
			controller := &removalController{
				purgeJob:       func(context.Context, string) error { return step("purge") },
				removeResource: func(context.Context, localRemoval) error { return step("resource") },
				ackRemoval:     func(context.Context, localRemoval) error { return step("ack") },
				finishRemoval:  func(context.Context, localRemoval) error { return step("finish") },
				clearReap:      s.completeServiceRemoval,
			}
			err := controller.completeLocalRemoval(t.Context(), localRemoval{jobID: "removed", kind: contract.JobKindProcess, processTreeReaped: true}, nil, nil)
			if fail != "" {
				if err == nil || service.evidenceSettled || service.checkEvidence == nil {
					t.Fatal("failed removal manufactured settlement", err)
				}
				return
			}
			if err != nil || !service.reaped || !service.evidenceSettled || service.checkEvidence != nil {
				t.Fatal("completed removal kept obsolete service evidence pending", err)
			}
			if oneShot.reaped || oneShot.evidenceSettled {
				t.Fatal("service removal touched one-shot proof")
			}
			request := UpgradeServiceQuiescence{OperationID: "activation", NodeID: "node", AgentGenerationID: "generation", BootSessionID: "boot", Services: []l1.NodeActiveAttempt{{JobID: "removed", AttemptID: "service", BootSessionID: "boot", Class: contract.JobClassService, Kind: contract.JobKindProcess}}}
			if _, err := (&Agent{session: s}).QuiesceUpgradeServices(t.Context(), request); err != nil {
				t.Fatal("removed service kept quiescence pending", err)
			}
		})
	}
}
