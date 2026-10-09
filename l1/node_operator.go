package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

const (
	nodeVerbDrain     = "drain"
	nodeVerbSetClaims = "set-claims"
)

func validateNodeIntentRequest(verb, actor string, request NodeIntentRequest) error {
	if request.IntentRevision < 0 {
		return protocolError(contract.ErrorInvalidRequest, "intent_revision must be non-negative")
	}
	if strings.TrimSpace(request.Reason) == "" || strings.TrimSpace(actor) == "" {
		return protocolError(contract.ErrorInvalidRequest, "intent reason and actor are required")
	}
	if verb == nodeVerbDrain && request.ClaimsEnabled {
		return protocolError(contract.ErrorInvalidRequest, "drain requires claims_enabled=false")
	}
	return nil
}

// nodeIntentActor keeps the authenticated identity and deployment's principal
// policy together. Decisions must not infer authority from a display actor name.
type nodeIntentActor struct {
	Identity           fabric.Identity
	ClientPrincipalTag string
}

func (s *Server) nodeIntentActor(r *http.Request) nodeIntentActor {
	return nodeIntentActor{Identity: identityFromRequest(r), ClientPrincipalTag: s.clientPrincipalTag}
}

// nodeIntentDecision is shared by advertised actions and the enforcing write.
// Liveness, capacity, capabilities and resident attempts do not forbid an
// authorized operator intent write, including a disable on a dead Node.
func nodeIntentDecision(node Node, verb string, actor nodeIntentActor, request NodeIntentRequest) error {
	if err := taggedIdentityDecision(actor.Identity, actor.ClientPrincipalTag); err != nil {
		return err
	}
	if err := validateNodeIntentRequest(verb, actor.Identity.NodeID, request); err != nil {
		return err
	}
	if request.IntentRevision != node.IntentRevision {
		return protocolErrorWithDetails(contract.ErrorStaleIntentRevision, map[string]any{
			"node_id": node.NodeID, "expected_revision": node.IntentRevision, "observed_revision": request.IntentRevision,
		}, "node %q intent revision has changed", node.NodeID)
	}
	return nil
}

func nodeAllowedActions(node Node, actor nodeIntentActor) []contract.AllowedAction {
	actions := make([]contract.AllowedAction, 0, 2)
	for _, verb := range []string{nodeVerbDrain, nodeVerbSetClaims} {
		request := NodeIntentRequest{IntentRevision: node.IntentRevision, Reason: "required operator input"}
		action := contract.AllowedAction{Verb: verb, Requires: map[string]any{"intent_revision": node.IntentRevision},
			Inputs: []contract.ActionInput{{Name: "reason", Type: "string", Required: true}}}
		if verb == nodeVerbDrain {
			action.Requires["claims_enabled"] = false
		} else {
			action.Inputs = append(action.Inputs, contract.ActionInput{Name: "claims_enabled", Type: "boolean", Required: true})
		}
		action.RefusedBecause = apiErrorFromDecision(nodeIntentDecision(node, verb, actor, request))
		actions = append(actions, action)
	}
	return actions
}

// Node actions are request-specific; store snapshots never advertise authority.
func (s *Server) projectNodeForCaller(r *http.Request, node Node) Node {
	node.AllowedActions = nodeAllowedActions(node, s.nodeIntentActor(r))
	return node
}

func conditionJSON(code, scope string, now time.Time, details map[string]any) []byte {
	if details == nil {
		details = map[string]any{}
	}
	// All callers supply JSON-compatible facts (strings, revisions and booleans).
	data, err := json.Marshal(contract.Condition{Code: code, Scope: scope, Since: canonicalTime(now), Details: details})
	if err != nil {
		panic(err)
	}
	return data
}

func recordNodeCondition(ctx context.Context, tx *sql.Tx, nodeID, code, scope string, now time.Time, details map[string]any) error {
	_, err := tx.ExecContext(ctx, "UPDATE nodes SET last_condition_json=? WHERE node_id=?", conditionJSON(code, scope, now, details), nodeID)
	return err
}

// GetNode reads the same factual snapshot as the fleet listing. The route
// computes allowed actions for its authenticated caller.
func (s *Store) GetNode(ctx context.Context, nodeID string) (Node, error) {
	var node Node
	err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		var err error
		node, err = reads.node(ctx, nodeID)
		if errors.Is(err, sql.ErrNoRows) {
			return protocolError(contract.ErrorNotFound, "node %q was not found", nodeID)
		}
		if err != nil {
			return internalError(err, "read node")
		}
		node = s.nodeLiveness().project(node, reads.now())
		return nil
	})
	return node, err
}

// nodeLiveness projects only display state. The cached Node and nodeState
// remain recorded facts for enforcing decisions such as reconfiguration abort.
type nodeLiveness struct {
	staleAfter time.Duration
	deadAfter  time.Duration
}

func (s *Store) nodeLiveness() nodeLiveness {
	return nodeLiveness{staleAfter: s.nodeStaleAfter, deadAfter: s.nodeDeadAfter}
}

func (l nodeLiveness) project(node Node, now time.Time) Node {
	switch node.State {
	case contract.NodeAlive, contract.NodeStale, contract.NodeDraining:
		if !node.LastHeartbeatAt.Add(l.deadAfter).After(now) {
			node.State = contract.NodeDead
		} else if node.State == contract.NodeAlive && !node.LastHeartbeatAt.Add(l.staleAfter).After(now) {
			node.State = contract.NodeStale
		}
	}
	return node
}
