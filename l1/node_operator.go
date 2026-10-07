package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
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

// nodeIntentDecision is shared by the advertised actions and the transaction
// that enforces them. Liveness, capacity, capabilities and resident attempts do
// not forbid an operator intent write, including a disable on a dead Node.
func nodeIntentDecision(node Node, verb, actor string, request NodeIntentRequest) error {
	if err := validateNodeIntentRequest(verb, actor, request); err != nil {
		return err
	}
	if request.IntentRevision != node.IntentRevision {
		return protocolErrorWithDetails(contract.ErrorStaleIntentRevision, map[string]any{
			"node_id": node.NodeID, "current_revision": node.IntentRevision, "provided_revision": request.IntentRevision,
		}, "node %q intent revision has changed", node.NodeID)
	}
	return nil
}

func nodeAllowedActions(node Node) []contract.AllowedAction {
	actions := make([]contract.AllowedAction, 0, 2)
	for _, verb := range []string{nodeVerbDrain, nodeVerbSetClaims} {
		request := NodeIntentRequest{IntentRevision: node.IntentRevision, Reason: "required operator input"}
		action := contract.AllowedAction{Verb: verb, Requires: map[string]any{"revision": node.IntentRevision, "reason": true}}
		if verb == nodeVerbSetClaims {
			action.Requires["claims_enabled"] = true
		}
		if err := nodeIntentDecision(node, verb, "authenticated operator", request); err != nil {
			var refusal *Error
			if errors.As(err, &refusal) {
				action.RefusedBecause = &contract.APIError{Code: refusal.Code, Message: refusal.Message, Details: refusal.Details}
			}
		}
		actions = append(actions, action)
	}
	return actions
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

// GetNode reads the same projection as the fleet listing in one snapshot.
func (s *Store) GetNode(ctx context.Context, nodeID string) (Node, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Node{}, internalError(err, "begin node read")
	}
	defer tx.Rollback()
	node, err := getNode(ctx, tx, nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return Node{}, protocolError(contract.ErrorNotFound, "node %q was not found", nodeID)
	}
	if err != nil {
		return Node{}, internalError(err, "read node")
	}
	if err := tx.Commit(); err != nil {
		return Node{}, internalError(err, "commit node read")
	}
	return node, nil
}
