package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/internal/upgradecompat"
	"github.com/Derek-X-Wang/wefty/l1"
)

type rollingControlFixture struct {
	*rollingFixture
	runtime      *agent.Agent
	op           upgradeHost
	profile      connprofile.Profile
	node         l1.Node
	candidate    agentinstall.Generation
	registration contract.NodeRegistration
	request      l1.PromoteNodeGenerationRequest
}

func newRollingControlFixture(t *testing.T, beforeReply func()) *rollingControlFixture {
	t.Helper()
	return newRollingControlFixtureWithStore(t, beforeReply, nil)
}
func newRollingControlFixtureWithStore(t *testing.T, beforeReply func(), configure func(*rollingFixture)) *rollingControlFixture {
	t.Helper()
	f := newRollingFixture(t)
	if configure != nil {
		configure(f)
	}
	c := &rollingControlFixture{rollingFixture: f}
	c.runtime = exposeRollingL1WithPromotionReply(t, f, beforeReply)
	var err error
	c.profile, err = connprofile.Read(f.paths.profile)
	mustRolling(t, err)
	op := upgradeOperations(f.host)
	op.generations = nil
	op.stageGeneration = nil
	op.promoteGeneration = nil
	op.generationControl = nil
	host := f.host
	host.upgrade = &op
	c.op = upgradeOperations(host)
	node, err := f.h.store.GetNode(t.Context(), f.node.NodeID)
	mustRolling(t, err)
	c.node, err = f.h.store.SetNodeClaimsByOperator(t.Context(), node.NodeID, "operator", l1.NodeIntentRequest{ClaimsEnabled: false, IntentRevision: node.IntentRevision, Reason: "upgrade"})
	mustRolling(t, err)
	proof := agent.UpgradeServiceQuiescence{OperationID: "control-fences", NodeID: node.NodeID, AgentGenerationID: f.primary.ID, BootSessionID: node.BootSessionID, IntentRevision: c.node.IntentRevision, IntentReason: c.node.IntentReason, Services: []l1.NodeActiveAttempt{}}
	body, _ := json.Marshal(proof)
	_, err = c.exchange(t, "quiesce_services", "quiesce", body)
	mustRolling(t, err)
	c.candidate, err = f.install.PrepareCandidate(filepath.Join(f.paths.bin, "wefty-agent"), "candidate", rollingTestBoot, []int{1})
	mustRolling(t, err)
	services, err := setupServices(f.paths, c.profile, map[string]string{}, f.loginPATH)
	mustRolling(t, err)
	_, err = f.install.LoadCandidate(t.Context(), c.candidate.ID, services[2])
	mustRolling(t, err)
	f.holdReady(t, c.candidate, c.candidate.CandidateBootSessionID)
	_, err = f.install.BindReady(t.Context(), c.candidate.ID, agentinstall.Binding{PID: os.Getpid(), BootSessionID: c.candidate.CandidateBootSessionID, SystemBootID: rollingTestBoot}, c.candidate.CreatedAt)
	mustRolling(t, err)
	receipt := upgradeRollingReceipt{Version: 1, NodeID: node.NodeID, OperationID: "control-fences"}
	mustRolling(t, writeUpgradeRolling(f.paths, &receipt, "candidate_staging"))
	c.registration = c.node.NodeRegistration
	c.registration.AgentGenerationID, c.registration.BootSessionID, c.registration.AgentVersion = c.candidate.ID, c.candidate.CandidateBootSessionID, c.candidate.Version
	return c
}
func (c *rollingControlFixture) exchange(t *testing.T, action, id string, body json.RawMessage) (json.RawMessage, error) {
	t.Helper()
	return c.op.generationControl(t.Context(), c.install, agentinstall.ControlRequest{ID: "control-fences", RequestID: id, Action: action, NodeID: c.rollingFixture.node.NodeID, GenerationID: c.primary.ID, BootSessionID: c.rollingFixture.node.BootSessionID, Payload: body})
}
func (c *rollingControlFixture) preparePromotion(t *testing.T) {
	t.Helper()
	in, err := c.op.stageGeneration(t.Context(), c.profile, c.registration)
	mustRolling(t, err)
	catalog, err := c.install.Read()
	mustRolling(t, err)
	_, err = c.install.BeginPromotion(c.candidate.ID, rollingTestBoot, "control-fences", catalog.Revision, 1, agentinstall.PromotionEvidence{IntentRevision: c.node.IntentRevision, L1CatalogRevision: in.CatalogRevision, Residents: map[string][]agentinstall.ResidentAttempt{c.primary.ID: {}, c.candidate.ID: {}}})
	mustRolling(t, err)
	c.request = l1.PromoteNodeGenerationRequest{OperationID: "control-fences", ExpectedPrimaryGenerationID: c.primary.ID, ExpectedPrimaryBootSessionID: c.node.BootSessionID, ExpectedAuthorityGeneration: c.node.AuthorityGeneration, CandidateGenerationID: c.candidate.ID, CandidateBootSessionID: c.candidate.CandidateBootSessionID, ExpectedCatalogRevision: in.CatalogRevision, CatalogRevision: in.CatalogRevision + 1, IntentRevision: c.node.IntentRevision, SelectedProtocol: 1, RetiringGenerationIDs: []string{}, ResidentAttempts: []l1.ResidentAttempt{}}
}
func (c *rollingControlFixture) changeIntent(t *testing.T) {
	t.Helper()
	node, err := c.h.store.GetNode(t.Context(), c.node.NodeID)
	mustRolling(t, err)
	node, err = c.h.store.SetNodeClaimsByOperator(t.Context(), node.NodeID, "operator", l1.NodeIntentRequest{ClaimsEnabled: true, IntentRevision: node.IntentRevision, Reason: "operator reopen"})
	mustRolling(t, err)
	c.node, err = c.h.store.SetNodeClaimsByOperator(t.Context(), node.NodeID, "operator", l1.NodeIntentRequest{ClaimsEnabled: false, IntentRevision: node.IntentRevision, Reason: "operator maintenance"})
	mustRolling(t, err)
}

func TestUpgradeControlPromotionPublicationFailureFencesRetirement(t *testing.T) {
	committed := make(chan struct{})
	pathReady := make(chan struct{})
	var resultPath string
	var publicationErr error
	c := newRollingControlFixture(t, func() {
		<-pathReady
		publicationErr = os.Remove(resultPath)
		if publicationErr == nil {
			publicationErr = os.Mkdir(resultPath, 0700)
		}
		close(committed)
	})
	c.preparePromotion(t)
	resultPath = filepath.Join(c.install.Root(), c.primary.ID, "control-result.json")
	close(pathReady)
	body, _ := json.Marshal(c.request)
	mustRolling(t, c.install.RequestControl(agentinstall.ControlRequest{ID: "control-fences", RequestID: "commit", Action: "promote_generation", NodeID: c.node.NodeID, GenerationID: c.primary.ID, BootSessionID: c.request.ExpectedPrimaryBootSessionID, Payload: body}))
	select {
	case <-committed:
	case <-time.After(testwait.Bound):
		t.Fatal("promotion not committed")
	}
	mustRolling(t, publicationErr)
	// A directory at the result path forces real reply publication to fail.
	time.Sleep(300 * time.Millisecond)
	retired := make(chan error, 1)
	retireCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() { _, err := c.runtime.Retire(retireCtx); retired <- err }()
	select {
	case err := <-retired:
		t.Errorf("retirement passed failed reply publication: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	if _, err := c.install.ReadRetirement(c.primary.ID); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("retirement ack exists before reply: %v", err)
	}
	mustRolling(t, os.Remove(resultPath))
	deadline := time.Now().Add(4 * time.Second)
	for {
		reply, err := c.install.ReadControlResult(c.primary.ID)
		if err == nil && reply.Request.RequestID == "commit" && reply.Error == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("promotion publication was not retried: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case err := <-retired:
		mustRolling(t, err)
	case <-time.After(testwait.Bound):
		t.Fatal("retirement did not resume after durable reply")
	}
}

func TestUpgradeControlBarrierRetainsAdmissionIntent(t *testing.T) {
	for _, action := range []string{"stage_generation", "promote_generation"} {
		t.Run(action, func(t *testing.T) {
			c := newRollingControlFixture(t, nil)
			if action == "promote_generation" {
				c.preparePromotion(t)
			}
			c.changeIntent(t)
			var body []byte
			if action == "stage_generation" {
				body, _ = json.Marshal(c.registration)
			} else {
				// Same-account edits to the activation receipt cannot rebind the barrier.
				catalog, err := c.install.Read()
				mustRolling(t, err)
				activation, err := c.install.ReadActivationReceipt(catalog.PendingReceipt)
				mustRolling(t, err)
				activation.IntentRevision = c.node.IntentRevision
				b, _ := json.Marshal(activation)
				mustRolling(t, os.WriteFile(filepath.Join(c.install.Root(), "activation-"+catalog.PendingReceipt+".json"), b, 0600))
				c.request.IntentRevision = c.node.IntentRevision
				body, _ = json.Marshal(c.request)
			}
			for _, id := range []string{"original-transport", "changed-transport"} {
				_, err := c.exchange(t, action, id, body)
				if err == nil {
					t.Fatalf("old settled barrier reused under changed intent (%s)", id)
				}
				if !strings.Contains(err.Error(), "admission") {
					t.Fatalf("wrong refusal: %v", err)
				}
			}
		})
	}
}

func TestUpgradeControlPromotionReplayPreservesSuccess(t *testing.T) {
	c := newRollingControlFixture(t, nil)
	c.preparePromotion(t)
	body, _ := json.Marshal(c.request)
	first, err := c.exchange(t, "promote_generation", "first", body)
	mustRolling(t, err)
	for _, id := range []string{"first", "fresh"} {
		// Transport identity does not change the semantic promotion request.
		replay, err := c.exchange(t, "promote_generation", id, body)
		mustRolling(t, err)
		if !reflect.DeepEqual([]byte(first), []byte(replay)) {
			t.Fatal("promotion replay changed successful result")
		}
	}
	for _, id := range []string{"fresh", "changed-payload"} {
		changed := c.request
		changed.CatalogRevision++
		b, _ := json.Marshal(changed)
		if _, err := c.exchange(t, "promote_generation", id, b); err == nil {
			t.Fatal("changed promotion replay accepted")
		}
		// A rejected replay must not erase the last successful reply.
		recorded, err := c.install.ReadControlResult(c.primary.ID)
		mustRolling(t, err)
		if recorded.Error != "" || !reflect.DeepEqual([]byte(recorded.Payload), []byte(first)) {
			t.Fatal("successful promotion evidence overwritten", recorded.Error)
		}
	}
}

func TestUpgradeControlAdmissionRaceKeepsConflictExit(t *testing.T) {
	c := newRollingControlFixture(t, nil)
	c.preparePromotion(t)
	c.changeIntent(t)
	_, err := c.op.promoteGeneration(t.Context(), c.profile, c.request)
	var protocol *apiResponseError
	if commandExitCode(err) != exitConflict || !errors.As(err, &protocol) || protocol.APIError.Code != contract.ErrorConflict || protocol.APIError.Details["reason"] != string(upgradecompat.InvalidInventory) {
		t.Fatalf("admission race lost typed gate conflict: exit=%d err=%#v", commandExitCode(err), err)
	}
}

func TestUpgradeControlQuiescenceIntentRetryKeepsConflictExit(t *testing.T) {
	c := newRollingControlFixture(t, nil)
	c.changeIntent(t)
	// The retry intentionally supplies the current intent. The adapter heartbeat
	// accepts it; the original settled barrier must reject it as a typed conflict.
	proof := agent.UpgradeServiceQuiescence{OperationID: "control-fences", NodeID: c.node.NodeID, AgentGenerationID: c.primary.ID, BootSessionID: c.node.BootSessionID, IntentRevision: c.node.IntentRevision, IntentReason: c.node.IntentReason, Services: []l1.NodeActiveAttempt{}}
	body, _ := json.Marshal(proof)
	for _, id := range []string{"quiesce", "new-intent-retry"} {
		_, err := c.exchange(t, "quiesce_services", id, body)
		var protocol *apiResponseError
		if commandExitCode(err) != exitConflict || !errors.As(err, &protocol) || protocol.StatusCode != 409 || protocol.APIError.Code != contract.ErrorConflict || protocol.APIError.Details["reason"] != string(upgradecompat.InvalidInventory) {
			t.Errorf("quiescence retry %s lost typed conflict: exit=%d err=%#v", id, commandExitCode(err), err)
		}
	}
}

func TestUpgradeControlFenceRefusalsKeepConflictExit(t *testing.T) {
	c := newRollingControlFixture(t, nil)
	proof := agent.UpgradeServiceQuiescence{OperationID: "control-fences", NodeID: c.node.NodeID, AgentGenerationID: c.primary.ID, BootSessionID: c.node.BootSessionID, IntentRevision: c.node.IntentRevision, IntentReason: c.node.IntentReason, Services: []l1.NodeActiveAttempt{}}
	body, _ := json.Marshal(proof)
	request := agentinstall.ControlRequest{ID: proof.OperationID, NodeID: c.node.NodeID, GenerationID: c.primary.ID, BootSessionID: c.node.BootSessionID, Action: "quiesce_services", Payload: body}
	for _, name := range []string{"node", "boot", "operation", "inner-identity", "pending-operation", "inventory", "resume-authority", "quiesce-payload", "stage-payload", "missing-activation"} {
		t.Run(name, func(t *testing.T) {
			r, p := request, proof
			r.RequestID = name
			switch name {
			case "node":
				r.NodeID = "other-node"
			case "boot":
				r.BootSessionID = "other-boot"
			case "operation":
				p.OperationID = "other-operation"
			case "inner-identity":
				p.AgentGenerationID = "other-generation"
			case "pending-operation":
				r.ID = "another-operation"
				p.OperationID = r.ID
			case "inventory":
				p.Services = []l1.NodeActiveAttempt{{JobID: "unowned", AttemptID: "unowned", Class: contract.JobClassService, Kind: contract.JobKindProcess, BootSessionID: c.node.BootSessionID}}
			case "resume-authority":
				r.Action = "resume_services"
				r.ID = "another-operation"
			case "quiesce-payload", "stage-payload":
				if name == "stage-payload" {
					r.Action = "stage_generation"
				}
			case "missing-activation":
				r.Action = "promote_generation"
			}
			r.Payload, _ = json.Marshal(p)
			if name == "quiesce-payload" || name == "stage-payload" {
				r.Payload = []byte(`[]`)
			}
			if name == "missing-activation" {
				r.Payload = []byte(`{"operation_id":"control-fences"}`)
			}
			_, err := c.op.generationControl(t.Context(), c.install, r)
			assertUpgradeControlCLIConflict(t, err)
		})
	}
	for _, name := range []string{"empty-operation", "unknown-action", "invalid-generation", "unknown-generation"} {
		t.Run(name, func(t *testing.T) {
			r := request
			switch name {
			case "empty-operation":
				r.ID = ""
			case "unknown-action":
				r.Action = "not_an_action"
			case "invalid-generation":
				r.GenerationID = "invalid"
			case "unknown-generation":
				r.GenerationID = strings.Repeat("f", 32)
			}
			_, err := c.op.generationControl(t.Context(), c.install, r)
			assertUpgradeControlCLIConflict(t, err)
		})
	}
	// The lifetime proof is part of the ready-process fence, not an internal CLI
	// failure. Dropping only this temporary lock must refuse stage with exit 5.
	c.lifetimes[c.candidate.ID].Close()
	_, err := c.op.stageGeneration(t.Context(), c.profile, c.registration)
	assertUpgradeControlCLIConflict(t, err)
}
func assertUpgradeControlCLIConflict(t *testing.T, err error) {
	t.Helper()
	var protocol *apiResponseError
	if commandExitCode(err) != exitConflict || !errors.As(err, &protocol) || protocol.StatusCode != 409 || protocol.APIError.Code != contract.ErrorConflict || protocol.APIError.Details["reason"] != string(upgradecompat.InvalidInventory) {
		t.Errorf("adapter refusal lost typed conflict: exit=%d err=%#v", commandExitCode(err), err)
	}
}

func TestUpgradeControlStorageFailureIsNotGateConflict(t *testing.T) {
	c := newRollingControlFixture(t, nil)
	store := c.install
	// Use actual missing storage, rather than an aliased/invalid ownership path.
	store.Data = filepath.Join(c.install.Data, "missing-storage")
	_, err := c.op.generationControl(t.Context(), store, agentinstall.ControlRequest{ID: "operation", RequestID: "storage-failure", NodeID: c.node.NodeID, GenerationID: c.primary.ID, BootSessionID: c.node.BootSessionID, Action: "generation_inventory"})
	var protocol *apiResponseError
	if err == nil || commandExitCode(err) != exitFailure || errors.As(err, &protocol) || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("storage failure became gate refusal: exit=%d err=%#v", commandExitCode(err), err)
	}
}

func TestUpgradeRollingClientAdapterFencesKeepConflictExit(t *testing.T) {
	for _, name := range []string{"profile-owner", "missing-operation", "foreign-inventory"} {
		t.Run(name, func(t *testing.T) {
			f := newRollingFixture(t)
			p, err := connprofile.Read(f.paths.profile)
			mustRolling(t, err)
			op := upgradeOperations(f.host)
			op.generations = nil
			op.stageGeneration = nil
			op.generationControl = func(context.Context, agentinstall.Store, agentinstall.ControlRequest) (json.RawMessage, error) {
				return []byte(`{"node_id":"another-node","generations":[]}`), nil
			}
			host := f.host
			host.upgrade = &op
			op = upgradeOperations(host)
			if name == "profile-owner" {
				p.Identities.Node = "another-node"
			}
			if name == "missing-operation" {
				// Explicit invalid operation differs from a failed receipt read.
				mustRolling(t, writeUpgradeRolling(f.paths, &upgradeRollingReceipt{Version: 1, NodeID: f.node.NodeID}, "candidate_staging"))
				_, err = op.stageGeneration(t.Context(), p, f.node.NodeRegistration)
			} else {
				_, err = op.generations(t.Context(), p)
			}
			assertUpgradeControlCLIConflict(t, err)
		})
	}
}

func assertUpgradeControlOperational(t *testing.T, err error, fragments ...string) {
	t.Helper()
	var protocol *apiResponseError
	if err == nil || commandExitCode(err) != exitFailure || errors.As(err, &protocol) {
		t.Errorf("operational failure misclassified: exit=%d err=%#v", commandExitCode(err), err)
	}
	for _, fragment := range fragments {
		if err == nil || !strings.Contains(err.Error(), fragment) {
			t.Errorf("lost failure diagnostic %q: %v", fragment, err)
		}
	}
}

func TestUpgradeControlReceiptReadFailureRemainsOperational(t *testing.T) {
	c := newRollingControlFixture(t, nil)
	c.preparePromotion(t)
	catalog, err := c.install.Read()
	mustRolling(t, err)
	path := filepath.Join(c.install.Root(), "activation-"+catalog.PendingReceipt+".json")
	mustRolling(t, os.Remove(path))
	_, err = c.op.promoteGeneration(t.Context(), c.profile, c.request)
	assertUpgradeControlOperational(t, err, "activation-", "no such file")
	result, readErr := c.install.ReadControlResult(c.primary.ID)
	mustRolling(t, readErr)
	if result.APIError != nil {
		t.Error("I/O failure was published as a refusal", result.APIError)
	}
}

func TestUpgradeControlServiceManagerFailureRemainsOperational(t *testing.T) {
	var fail atomic.Bool
	c := newRollingControlFixtureWithStore(t, nil, func(f *rollingFixture) {
		run := f.install.Manager.Run
		f.install.Manager.Run = func(ctx context.Context, command string, args ...string) ([]byte, error) {
			if fail.Load() {
				return nil, &os.PathError{Op: "manager-state", Path: "temporary-fake-manager", Err: syscall.EIO}
			}
			return run(ctx, command, args...)
		}
	})
	fail.Store(true)
	_, err := c.op.stageGeneration(t.Context(), c.profile, c.registration)
	assertUpgradeControlOperational(t, err, "manager-state", "input/output error")
	result, readErr := c.install.ReadControlResult(c.primary.ID)
	mustRolling(t, readErr)
	if result.APIError != nil {
		t.Error("service-manager error published as conflict", result.APIError)
	}
}

func TestUpgradeRollingCatalogReadFailureRemainsOperational(t *testing.T) {
	f := newRollingFixture(t)
	p, err := connprofile.Read(f.paths.profile)
	mustRolling(t, err)
	op := upgradeOperations(f.host)
	op.stageGeneration = nil
	host := f.host
	host.upgrade = &op
	op = upgradeOperations(host)
	mustRolling(t, os.Remove(filepath.Join(f.install.Root(), "catalog.json")))
	_, err = op.stageGeneration(t.Context(), p, f.node.NodeRegistration)
	assertUpgradeControlOperational(t, err, "catalog.json", "no such file")
}

func TestUpgradeRollingMalformedPrivateReplyRemainsOperational(t *testing.T) {
	f := newRollingFixture(t)
	p, err := connprofile.Read(f.paths.profile)
	mustRolling(t, err)
	op := upgradeOperations(f.host)
	op.generations = nil
	op.generationControl = func(ctx context.Context, store agentinstall.Store, r agentinstall.ControlRequest) (json.RawMessage, error) {
		// The private transport and CLI reader are real; only the response
		// producer is a fake adapter emitting a malformed successful result.
		r.RequestID = "malformed-reply"
		mustRolling(t, store.PublishControl(agent.NewUpgradeControlResult(r, []byte(`[]`), nil)))
		return requestGenerationControl(ctx, store, r)
	}
	host := f.host
	host.upgrade = &op
	op = upgradeOperations(host)
	_, err = op.generations(t.Context(), p)
	assertUpgradeControlOperational(t, err, "unmarshal")
}

func TestUpgradeControlBarrierOperationalCausesReachCLI(t *testing.T) {
	for _, phase := range []string{"persistence", "evidence"} {
		t.Run(phase, func(t *testing.T) {
			f := newRollingFixture(t)
			var cause error = &os.PathError{Op: "write-completion", Path: "temporary-spool", Err: syscall.EIO}
			if phase == "evidence" {
				cause = fmt.Errorf("transport read failed: %w", io.ErrUnexpectedEOF)
			}
			failure := &agent.UpgradeServiceQuiescenceError{JobID: "service-job", AttemptID: "service-attempt", Phase: phase, Cause: cause}
			r := agentinstall.ControlRequest{ID: "operation", RequestID: phase, NodeID: f.node.NodeID, GenerationID: f.primary.ID, BootSessionID: f.node.BootSessionID, Action: "quiesce_services"}
			// This is the same production encoder used after the actual service
			// barrier caller; agent tests separately exercise that recovery caller.
			result := agent.NewUpgradeControlResult(r, nil, failure)
			mustRolling(t, f.install.PublishControl(result))
			result, readErr := f.install.ReadControlResult(f.primary.ID)
			mustRolling(t, readErr)
			if result.ServiceFailure == nil || result.ServiceFailure.AttemptID != "service-attempt" || result.ServiceFailure.Phase != phase {
				t.Fatal("lost structured barrier diagnostics", result)
			}
			_, err := requestGenerationControl(t.Context(), f.install, r)
			assertUpgradeControlOperational(t, err, "service-job", "service-attempt", phase, cause.Error())
		})
	}
}

func TestUpgradeControlMalformedRecordedReplyRemainsOperational(t *testing.T) {
	c := newRollingControlFixture(t, nil)
	c.preparePromotion(t)
	body, _ := json.Marshal(c.request)
	r := agentinstall.ControlRequest{ID: c.request.OperationID, RequestID: "corrupt-recorded-reply", NodeID: c.node.NodeID, GenerationID: c.primary.ID, BootSessionID: c.node.BootSessionID, Action: "promote_generation", Payload: body}
	mustRolling(t, c.install.PublishControl(agent.NewUpgradeControlResult(r, []byte(`[]`), nil)))
	_, err := c.op.promoteGeneration(t.Context(), c.profile, c.request)
	assertUpgradeControlOperational(t, err, "unmarshal")
	request, err := c.install.ReadControl(c.primary.ID)
	mustRolling(t, err)
	result, err := c.install.ReadControlReply(request)
	mustRolling(t, err)
	if result.APIError != nil {
		t.Error("malformed recorded reply became a refusal", result)
	}
}
