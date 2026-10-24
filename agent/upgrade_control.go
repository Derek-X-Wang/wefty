package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/upgradecompat"
	"github.com/Derek-X-Wang/wefty/l1"
)

// ServeUpgradeControl handles only owner-private generation requests. It starts
// before readiness is published and never treats a file as execution authority.
func (a *Agent) ServeUpgradeControl(ctx context.Context, store agentinstall.Store) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var handled *agentinstall.ControlRequest
	var pending *agentinstall.ControlResult
	replyFenced := false
	publish := func() error {
		if err := store.PublishControl(*pending); err != nil {
			return err
		}
		if replyFenced {
			a.session.retirementMu.Lock()
			close(a.session.upgradePromotionReply)
			a.session.upgradePromotionReply = nil
			a.session.retirementMu.Unlock()
			replyFenced = false
		}
		request := pending.Request
		handled = &request
		pending = nil
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-a.session.retiringRequested:
			return nil
		case <-ticker.C:
		}
		if pending != nil {
			// Retry only publication, never a possibly committed L1 mutation.
			// No mutex is held across a failed write or between retries.
			_ = publish()
			continue
		}
		r, err := store.ReadControl(a.session.registration.AgentGenerationID)
		var notPublished *agentinstall.RecordPendingError
		if errors.As(err, &notPublished) {
			continue
		}
		if err != nil {
			return err
		}
		if handled != nil && reflect.DeepEqual(*handled, r) {
			continue
		}
		if r.GenerationID != a.session.registration.AgentGenerationID || r.ID == "" {
			return upgradeControlConflict("agent: installed control identity mismatch")
		}
		result := agentinstall.ControlResult{Request: r}
		if r.NodeID != a.session.registration.NodeID || r.BootSessionID != a.session.registration.BootSessionID {
			result = NewUpgradeControlResult(r, nil, upgradeControlConflict("agent: installed control identity mismatch"))
			if err := store.PublishControl(result); err != nil {
				return err
			}
			handled = &r
			continue
		}
		bounded, cancel := context.WithTimeout(ctx, 2*time.Minute)
		// Install the reply fence before L1 can demote this process. It is
		// cleared only after durable publication, including on failed writes.
		if r.Action == "promote_generation" {
			err = lockMutexContext(bounded, &a.session.retirementMu)
			if err == nil {
				if !a.session.isRetiring() {
					a.session.upgradePromotionReply = make(chan struct{})
					replyFenced = true
				}
				a.session.retirementMu.Unlock()
			}
		}
		if err == nil {
			switch r.Action {
			case "quiesce_services":
				var request UpgradeServiceQuiescence
				if err = json.Unmarshal(r.Payload, &request); err != nil {
					err = upgradeControlConflict(fmt.Sprintf("agent: invalid quiescence payload: %v", err))
				} else if request.OperationID != r.ID {
					err = upgradeControlConflict("agent: installed control operation mismatch")
				}
				if err == nil {
					// Owner-private transport is not admission authority. Observe the
					// exact current primary boot and closed L1 intent before accepting
					// a new or replayed barrier; a stale file cannot fence services.
					var heartbeat l1.HeartbeatResponse
					heartbeat, err = a.session.heartbeat(bounded)
					if err == nil && (heartbeat.NodeID == "" || heartbeat.AgentGenerationID == "" || heartbeat.BootSessionID == "") {
						err = errors.New("agent: malformed generation heartbeat reply")
					} else if err == nil && (heartbeat.NodeID != r.NodeID || heartbeat.AgentGenerationID != r.GenerationID || heartbeat.BootSessionID != r.BootSessionID || heartbeat.ClaimsEnabled || heartbeat.IntentRevision != request.IntentRevision || heartbeat.IntentReason != request.IntentReason) {
						err = upgradeControlConflict("agent: upgrade service admission fence changed")
					}
				}
				if err == nil {
					var proof UpgradeServiceQuiescence
					proof, err = a.QuiesceUpgradeServices(bounded, request)
					if err == nil {
						result.Payload, err = json.Marshal(proof)
					}
				}
			case "generation_inventory", "stage_generation", "promote_generation":
				var inventory l1.NodeGenerationInventory
				inventory, err = a.controlUpgradeGenerations(bounded, store, r)
				if err == nil {
					result.Payload, err = json.Marshal(inventory)
				}
			case "resume_services":
				err = a.ResumeUpgradeServiceClaims(bounded, r.ID)
			default:
				err = upgradeControlConflict("agent: unknown installed control action")
			}
		}
		cancel()
		result = NewUpgradeControlResult(r, result.Payload, err)
		pending = &result
		if err := publish(); err != nil && r.Action != "promote_generation" {
			return err
		}
	}
}

// Generation operations execute with this agent's existing node participant.
// The private adapter exposes no URL, method, arbitrary body or Seat approval
// operation. L1 still enforces node identity and every promotion fence.
func (a *Agent) controlUpgradeGenerations(ctx context.Context, store agentinstall.Store, r agentinstall.ControlRequest) (l1.NodeGenerationInventory, error) {
	s := a.session
	// Replay the successful operation before requiring current-primary status:
	// the first call has already demoted this boot. Changed requests receive a
	// separate refusal; PublishControl preserves the successful evidence.
	if r.Action == "promote_generation" {
		previous, err := store.ReadControlResult(r.GenerationID)
		if err == nil && previous.SuccessfulPromotion() {
			if !agentinstall.SamePromotionControl(previous.Request, r) {
				return l1.NodeGenerationInventory{}, upgradeControlConflict("agent: promotion operation identity reused with a changed request")
			}
			var result l1.NodeGenerationInventory
			err = json.Unmarshal(previous.Payload, &result)
			if err == nil && result.NodeID == "" {
				err = errors.New("agent: malformed recorded promotion reply")
			}
			return result, err
		}
		var notPublished *agentinstall.RecordPendingError
		if err != nil && !errors.As(err, &notPublished) {
			return l1.NodeGenerationInventory{}, err
		}
	}
	inventory, err := s.client.NodeGenerations(ctx, s.registration.NodeID)
	if err != nil {
		return inventory, err
	}
	if inventory.NodeID == "" {
		return inventory, errors.New("agent: malformed generation inventory reply")
	}
	if inventory.NodeID != s.registration.NodeID {
		return inventory, upgradeControlConflict("agent: generation inventory belongs to another node")
	}
	if !slices.ContainsFunc(inventory.Generations, func(g l1.NodeGeneration) bool {
		return g.Role == l1.NodeGenerationPrimary && g.AgentGenerationID == s.registration.AgentGenerationID && g.BootSessionID == s.registration.BootSessionID
	}) {
		return inventory, upgradeControlConflict("agent: generation control requires the current primary")
	}
	if r.Action == "generation_inventory" {
		return inventory, nil
	}
	if err := lockMutexContext(ctx, &s.upgradeServiceMu); err != nil {
		return inventory, err
	}
	defer s.upgradeServiceMu.Unlock()
	s.claimMu.Lock()
	quiesced := s.upgradeServiceOperation == r.ID && s.upgradeServicesSettled && !s.isRetiring()
	s.claimMu.Unlock()
	if !quiesced {
		return inventory, upgradeControlConflict("agent: generation mutation requires this operation's settled service barrier")
	}
	node, err := s.heartbeat(ctx)
	if err != nil {
		return inventory, err
	}
	if node.NodeID == "" || node.AgentGenerationID == "" || node.BootSessionID == "" {
		return inventory, errors.New("agent: malformed generation heartbeat reply")
	}
	if node.AgentGenerationID != s.registration.AgentGenerationID || node.BootSessionID != s.registration.BootSessionID || node.ClaimsEnabled || node.IntentRevision != s.upgradeServiceIntent.IntentRevision || node.IntentReason != s.upgradeServiceIntent.Reason {
		return inventory, upgradeControlConflict("agent: generation admission fence changed")
	}
	catalog, err := store.Read()
	if err != nil {
		return inventory, err
	}
	if catalog.NodeID != s.registration.NodeID || catalog.Primary != s.registration.AgentGenerationID {
		return inventory, upgradeControlConflict("agent: local generation primary changed")
	}
	switch r.Action {
	case "stage_generation":
		var registration contract.NodeRegistration
		if err := json.Unmarshal(r.Payload, &registration); err != nil {
			return inventory, upgradeControlConflict(fmt.Sprintf("agent: invalid stage payload: %v", err))
		}
		candidate, owned := catalog.Generations[registration.AgentGenerationID]
		if registration.NodeID != s.registration.NodeID || !owned || candidate.Role != "" || candidate.Process == nil || candidate.Process.BootSessionID != registration.BootSessionID || candidate.Version != registration.AgentVersion || catalog.PendingReceipt != "" {
			return inventory, upgradeControlConflict("agent: stage requires the exact ready local candidate")
		}
		if err := store.VerifyRunning(ctx, candidate.ID, candidate.Process.SystemBootID); err != nil {
			return inventory, err
		}
		return s.client.StageNodeGeneration(ctx, s.registration.NodeID, registration)
	case "promote_generation":
		var request l1.PromoteNodeGenerationRequest
		if err := json.Unmarshal(r.Payload, &request); err != nil {
			return inventory, upgradeControlConflict(fmt.Sprintf("agent: invalid promotion payload: %v", err))
		}
		if catalog.PendingReceipt == "" {
			return inventory, upgradeControlConflict("agent: promotion requires an active installation receipt")
		}
		activation, err := store.ReadActivationReceipt(catalog.PendingReceipt)
		if err != nil {
			return inventory, err
		}
		if request.OperationID != r.ID || activation.L1OperationID != r.ID || request.ExpectedPrimaryGenerationID != s.registration.AgentGenerationID || request.ExpectedPrimaryBootSessionID != s.registration.BootSessionID || request.ExpectedAuthorityGeneration != node.AuthorityGeneration || request.IntentRevision != node.IntentRevision || activation.ExpectedPrimary != request.ExpectedPrimaryGenerationID || activation.Candidate != request.CandidateGenerationID || activation.IntentRevision != request.IntentRevision || activation.L1CatalogRevision != request.ExpectedCatalogRevision || activation.Protocol != int(request.SelectedProtocol) {
			return inventory, upgradeControlConflict("agent: promotion differs from the active installation receipt or L1 fence")
		}
		return s.client.PromoteNodeGeneration(ctx, s.registration.NodeID, request)
	}
	return inventory, upgradeControlConflict("agent: unknown generation operation")
}

// Adapter fence refusals have the same conflict semantics as L1 mutation
// refusals. Keep the compatibility reason across the private reply hop.
func upgradeControlConflict(message string) *ProtocolError {
	return &ProtocolError{StatusCode: http.StatusConflict, APIError: contract.APIError{Code: contract.ErrorConflict, Message: message, Details: map[string]any{"reason": string(upgradecompat.InvalidInventory)}}}
}

func setUpgradeControlError(result *agentinstall.ControlResult, err error) {
	if err == nil {
		return
	}
	result.Error = err.Error()
	var barrier *UpgradeServiceQuiescenceError
	if errors.As(err, &barrier) {
		result.ServiceFailure = &agentinstall.ControlServiceFailure{JobID: barrier.JobID, AttemptID: barrier.AttemptID, Phase: barrier.Phase, Pending: barrier.Pending}
	}
	protocol := upgradeControlProtocolCause(err)
	if protocol != nil {
		result.APIError = &protocol.APIError
		result.StatusCode = protocol.StatusCode
	}
}

// A protocol verdict may survive transparent wrappers, but an operational
// branch in a joined error must not be hidden by another branch's refusal.
func upgradeControlProtocolCause(err error) *ProtocolError {
	switch e := err.(type) {
	case *ProtocolError:
		return e
	case *agentinstall.VerificationRefusal:
		return upgradeControlConflict(e.Error())
	case *agentinstall.ControlRequestRefusal:
		return upgradeControlConflict(e.Error())
	case interface{ Unwrap() []error }:
		var verdict *ProtocolError
		for _, cause := range e.Unwrap() {
			next := upgradeControlProtocolCause(cause)
			if next == nil || (verdict != nil && !reflect.DeepEqual(verdict, next)) {
				return nil
			}
			verdict = next
		}
		return verdict
	case interface{ Unwrap() error }:
		return upgradeControlProtocolCause(e.Unwrap())
	default:
		return nil
	}
}

// NewUpgradeControlResult encodes the closed private reply. It grants no node
// authority and is also used by transport adapters that publish these records.
func NewUpgradeControlResult(request agentinstall.ControlRequest, payload json.RawMessage, err error) agentinstall.ControlResult {
	result := agentinstall.ControlResult{Request: request, Payload: payload}
	setUpgradeControlError(&result, err)
	return result
}
