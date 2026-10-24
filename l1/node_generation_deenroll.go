package l1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/Derek-X-Wang/wefty/contract"
)

// DeEnrollNodeGenerationRequest is an inverse enrollment, never a promotion or
// force-reset. Only the authenticated primary may attest local reap/evidence.
type DeEnrollNodeGenerationRequest struct {
	OperationID          string `json:"operation_id"`
	PrimaryGenerationID  string `json:"primary_generation_id"`
	PrimaryBootSessionID string `json:"primary_boot_session_id"`
	AuthorityGeneration  int64  `json:"authority_generation"`
	CatalogRevision      int64  `json:"catalog_revision"`
	IntentRevision       int64  `json:"intent_revision"`
	IntentReason         string `json:"intent_reason"`
	RuntimesReaped       bool   `json:"runtimes_reaped"`
	EvidenceSettled      bool   `json:"evidence_settled"`
}

// DeEnrollmentRequestSHA256 binds the canonical decoded request, including both
// evidence flags. Node identity is separately bound by the response details.
func DeEnrollmentRequestSHA256(r DeEnrollNodeGenerationRequest) string {
	payload, _ := json.Marshal(r)
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// IsDeEnrollmentNotCommitted accepts only the operation-specific proof emitted
// after the ledger recorded a terminal rejection of this exact request.
func IsDeEnrollmentNotCommitted(e contract.APIError, node string, r DeEnrollNodeGenerationRequest) bool {
	return node != "" && r.OperationID != "" && e.Code == contract.ErrorDeEnrollmentNotCommitted && !e.Retryable &&
		e.Details["node_id"] == node && e.Details["operation_id"] == r.OperationID &&
		e.Details["request_sha256"] == DeEnrollmentRequestSHA256(r)
}

type deEnrollmentOutcome struct {
	Refusal *contract.APIError `json:"de_enrollment_refusal,omitempty"`
}

func (s *Store) DeEnrollNodeGeneration(ctx context.Context, identity, nodeID string, r DeEnrollNodeGenerationRequest) (NodeGenerationInventory, error) {
	if r.OperationID == "" || r.PrimaryGenerationID == "" || r.PrimaryBootSessionID == "" || r.AuthorityGeneration <= 0 || r.IntentRevision <= 0 || r.IntentReason != "wefty maintenance "+r.OperationID || !r.RuntimesReaped || !r.EvidenceSettled {
		return NodeGenerationInventory{}, stagedAbortConflict("de-enrollment requires exact operation, positive reap and settled evidence")
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return NodeGenerationInventory{}, err
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return NodeGenerationInventory{}, err
	}
	defer write.rollback()
	tx := write.tx
	if err = requireGenerationIdentity(ctx, tx, nodeID, identity); err != nil {
		return NodeGenerationInventory{}, err
	}
	var prior, result []byte
	key := "de-enroll:" + r.OperationID
	err = tx.QueryRowContext(ctx, `SELECT request_json,result_json FROM node_generation_promotions WHERE node_id=? AND operation_id=?`, nodeID, key).Scan(&prior, &result)
	if err == nil {
		if string(prior) != string(payload) {
			return NodeGenerationInventory{}, stagedAbortConflict("de-enrollment operation payload changed")
		}
		var outcome deEnrollmentOutcome
		if err = json.Unmarshal(result, &outcome); err != nil {
			return NodeGenerationInventory{}, err
		}
		if outcome.Refusal != nil {
			if !IsDeEnrollmentNotCommitted(*outcome.Refusal, nodeID, r) {
				return NodeGenerationInventory{}, errors.New("invalid stored de-enrollment refusal")
			}
			return NodeGenerationInventory{}, &Error{Code: outcome.Refusal.Code, Message: outcome.Refusal.Message, Details: outcome.Refusal.Details}
		}
		var in NodeGenerationInventory
		err = json.Unmarshal(result, &in)
		return in, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return NodeGenerationInventory{}, err
	}
	// A terminal negative ledger entry prevents a delayed duplicate from
	// committing after this refusal, even if the failed fence later clears.
	reject := func(message string) (NodeGenerationInventory, error) {
		refusal := contract.APIError{Code: contract.ErrorDeEnrollmentNotCommitted, Message: message, Details: map[string]any{"node_id": nodeID, "operation_id": r.OperationID, "request_sha256": DeEnrollmentRequestSHA256(r)}}
		result, err := json.Marshal(deEnrollmentOutcome{Refusal: &refusal})
		if err != nil {
			return NodeGenerationInventory{}, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO node_generation_promotions(node_id,operation_id,request_json,result_json) VALUES(?,?,?,?)`, nodeID, key, payload, result); err != nil {
			return NodeGenerationInventory{}, err
		}
		if err = tx.Commit(); err != nil {
			return NodeGenerationInventory{}, err
		}
		return NodeGenerationInventory{}, &Error{Code: refusal.Code, Message: refusal.Message, Details: refusal.Details}
	}
	in, err := readNodeGenerations(ctx, tx, nodeID)
	if err != nil {
		return in, err
	}
	primary := false
	for _, g := range in.Generations {
		switch g.Role {
		case NodeGenerationPrimary:
			primary = g.AgentGenerationID == r.PrimaryGenerationID && g.BootSessionID == r.PrimaryBootSessionID && g.AuthorityGeneration == r.AuthorityGeneration
		case NodeGenerationClosed:
			if g.ClosedAt == nil {
				return reject("de-enrollment requires positive closed-generation evidence")
			}
		default:
			return reject("de-enrollment requires no retiring or staged generations")
		}
	}
	var boot, reason string
	var authority, intent int64
	var claims, pending bool
	if err = tx.QueryRowContext(ctx, `SELECT boot_session_id,authority_generation,intent_revision,claims_enabled,intent_reason FROM nodes WHERE node_id=?`, nodeID).Scan(&boot, &authority, &intent, &claims, &reason); err != nil {
		return in, err
	}
	if !primary || in.CatalogRevision != r.CatalogRevision || boot != r.PrimaryBootSessionID || authority != r.AuthorityGeneration || claims || intent != r.IntentRevision || reason != r.IntentReason {
		return reject("de-enrollment primary, catalog or admission fence changed")
	}
	// L1 verifies all live and unsettled Attempts, including previous boots.
	// The primary's separate attestation covers local outbox/mailbox evidence.
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM attempts WHERE node_id=? AND (state IN ('claimed','running','awaiting-input') OR (state<>'lost' AND completion_key IS NULL)))`, nodeID).Scan(&pending)
	if err != nil {
		return in, err
	}
	if pending {
		return reject("de-enrollment requires every Attempt and its completion to settle")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE node_generations SET role='closed',closed_ns=? WHERE node_id=? AND generation_id=?`, write.at.UnixNano(), nodeID, r.PrimaryGenerationID); err != nil {
		return in, err
	}
	// Revoke the old boot before legacy registration becomes available. Merely
	// clearing the primary role must never grant the old process legacy claims.
	if _, err = tx.ExecContext(ctx, `UPDATE nodes SET boot_session_id=?,authority_generation=authority_generation+1 WHERE node_id=?`, "de-enrolled:"+r.OperationID, nodeID); err != nil {
		return in, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE node_generation_catalog SET catalog_revision=catalog_revision+1 WHERE node_id=?`, nodeID); err != nil {
		return in, err
	}
	in, err = readNodeGenerations(ctx, tx, nodeID)
	if err != nil {
		return in, err
	}
	result, err = json.Marshal(in)
	if err != nil {
		return in, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO node_generation_promotions(node_id,operation_id,request_json,result_json) VALUES(?,?,?,?)`, nodeID, key, payload, result); err != nil {
		return in, err
	}
	return in, tx.Commit()
}
