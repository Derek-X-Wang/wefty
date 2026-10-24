package agent

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

const generationInventoryRefreshInterval = 5 * time.Second

// One bounded inventory request runs independently of replay, followed by a
// full refresh interval even on failure. Recovery wakes and worker completions
// cannot amplify polling. Unknown owners stay protected until positive closure
// is cached; own and already-known-closed evidence never waits for this RPC.
func (outbox *evidenceOutbox) runGenerationInventoryRefresh(ctx context.Context, client *Client) {
	for ctx.Err() == nil {
		refreshContext, cancel := context.WithTimeout(ctx, DefaultOperationTimeout)
		err := outbox.refreshClosedGenerations(refreshContext, client, outbox.nodeID)
		cancel()
		if err == nil {
			outbox.scheduleRecovery()
		}
		timer := outbox.clock.NewTimer(generationInventoryRefreshInterval)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-timer.C():
		}
	}
}

// Unknown generations are live for collection purposes. Neither a missing PID,
// local map nor inventory omission proves closure. L1 closure is monotonic.
func (spool *logSpool) generationPredicate(alias string) string {
	owner := alias + ".owner_generation"
	own := "'" + strings.ReplaceAll(spool.generationID, "'", "''") + "'"
	if spool.generationID == "" {
		return owner + "=''"
	}
	return "(" + owner + "=" + own + " OR EXISTS (SELECT 1 FROM closed_agent_generations g WHERE g.generation_id=" + owner + "))"
}

func (outbox *evidenceOutbox) refreshClosedGenerations(ctx context.Context, client *Client, nodeID string) error {
	if outbox.spool.generationID == "" {
		return nil
	}
	inventory, err := client.NodeGenerations(ctx, nodeID)
	if err != nil {
		return err
	}
	for _, generation := range inventory.Generations {
		if generation.Role != l1.NodeGenerationClosed || generation.ClosedAt == nil {
			continue
		}
		if _, err := outbox.spool.db.ExecContext(ctx, `INSERT OR IGNORE INTO closed_agent_generations(generation_id) VALUES(?)`, generation.AgentGenerationID); err != nil {
			return err
		}
	}
	return nil
}

// settleRetirementEvidence acknowledges custody in the shared FULL-synchronous
// SQLite store. The source never removes a row on this acknowledgment. Only a
// proven-closed owner's evidence becomes eligible for historical O/R replay.
// L1 independently refuses closure while first completion still needs live E.
func (outbox *evidenceOutbox) settleRetirementEvidence(client *Client, nodeID string) func(context.Context, l1.NodeGeneration, []l1.ResidentAttempt) (bool, error) {
	return func(ctx context.Context, source l1.NodeGeneration, pending []l1.ResidentAttempt) (bool, error) {
		// The session's resident list cannot certify unrelated older evidence
		// from this generation. Keep it alive until normal recovery delivers
		// that backlog; do not invent an identity for a custody receipt.
		accounted := make(map[string]bool, len(pending))
		for _, attempt := range pending {
			accounted[attempt.AttemptID] = true
		}
		rows, err := outbox.spool.db.QueryContext(ctx, `SELECT attempt_id FROM spool_attempts
WHERE owner_generation=? AND (result_json IS NOT NULL OR incomplete_json IS NOT NULL
 OR EXISTS (SELECT 1 FROM spool_events e WHERE e.attempt_id=spool_attempts.attempt_id))`, source.AgentGenerationID)
		if err != nil {
			return false, err
		}
		unaccounted := false
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return false, err
			}
			unaccounted = unaccounted || !accounted[id]
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return false, err
		}
		if unaccounted {
			return false, nil
		}
		if len(pending) == 0 {
			return true, nil
		}
		inventory, err := client.NodeGenerations(ctx, nodeID)
		if err != nil {
			return false, err
		}
		primary := ""
		for _, generation := range inventory.Generations {
			if generation.Role == l1.NodeGenerationPrimary {
				primary = generation.AgentGenerationID
			}
		}
		if primary == "" || primary == source.AgentGenerationID {
			return false, nil
		}
		outbox.ownershipMu.RLock()
		revision := outbox.lateRevision
		outbox.ownershipMu.RUnlock()
		for _, attempt := range pending {
			if outbox.attemptIsLive(attempt.AttemptID) {
				return false, nil
			}
			outbox.ownershipMu.RLock()
			late := outbox.lateEvents[attempt.AttemptID] != 0
			outbox.ownershipMu.RUnlock()
			if late {
				return false, nil
			}
			settled, err := outbox.spool.acknowledgeEvidenceCustody(ctx, source.AgentGenerationID, primary, attempt, outbox.clock.Now().UnixNano())
			if err != nil || !settled {
				return false, err
			}
		}
		outbox.ownershipMu.RLock()
		unchanged := outbox.lateRevision == revision
		outbox.ownershipMu.RUnlock()
		return unchanged, nil
	}
}

// The digest binds exact stored evidence and immutable Attempt authority. Bytes
// remain in the same transactionally protected store, not in a receipt-only copy.
func (spool *logSpool) acknowledgeEvidenceCustody(ctx context.Context, owner, custodian string, attempt l1.ResidentAttempt, now int64) (bool, error) {
	tx, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var accepts bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM generation_evidence_custodians WHERE generation_id=?)`, custodian).Scan(&accepts); err != nil {
		return false, err
	}
	if !accepts {
		return false, nil
	}
	var storedOwner, job, fence string
	var result, incomplete []byte
	var finished sql.NullInt64
	var published bool
	err = tx.QueryRowContext(ctx, `SELECT owner_generation,job_id,fencing_token,result_json,finished_ns,incomplete_json,publication_settled FROM spool_attempts WHERE attempt_id=?`, attempt.AttemptID).Scan(&storedOwner, &job, &fence, &result, &finished, &incomplete, &published)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if owner == "" || custodian == "" || owner == custodian || storedOwner != owner || job != attempt.JobID || fence != attempt.FencingToken || !published {
		return false, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT stream,sequence,timestamp_ns,bytes,gap_json FROM spool_events WHERE attempt_id=? ORDER BY ordinal`, attempt.AttemptID)
	if err != nil {
		return false, err
	}
	type event struct {
		Stream              string
		Sequence, Timestamp int64
		Bytes, Gap          []byte
	}
	events := []event{}
	for rows.Next() {
		var e event
		if err := rows.Scan(&e.Stream, &e.Sequence, &e.Timestamp, &e.Bytes, &e.Gap); err != nil {
			rows.Close()
			return false, err
		}
		events = append(events, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	document, err := json.Marshal(struct {
		Attempt            l1.ResidentAttempt
		Result, Incomplete []byte
		Finished           sql.NullInt64
		Events             []event
	}{attempt, result, incomplete, finished, events})
	if err != nil {
		return false, err
	}
	identity, err := json.Marshal(attempt)
	if err != nil {
		return false, err
	}
	digest := sha256.Sum256(document)
	_, err = tx.ExecContext(ctx, `INSERT INTO generation_evidence_custody(attempt_id,owner_generation,custodian_generation,identity_json,digest,acknowledged_ns) VALUES(?,?,?,?,?,?) ON CONFLICT(attempt_id) DO UPDATE SET custodian_generation=excluded.custodian_generation,identity_json=excluded.identity_json,digest=excluded.digest,acknowledged_ns=excluded.acknowledged_ns`, attempt.AttemptID, owner, custodian, identity, hex.EncodeToString(digest[:]), now)
	if err != nil {
		return false, fmt.Errorf("acknowledge generation evidence custody: %w", err)
	}
	return true, tx.Commit()
}

// Recovery from separate processes shares a per-Attempt lease. Selection alone
// is not ownership; the source can still be finishing shutdown after L1 closes.
func (outbox *evidenceOutbox) recoverOwnedAttempt(ctx context.Context, client *Client, attempt logSpoolAttempt) error {
	lease, err := acquireNodeLock(outbox.spool.directory, outbox.nodeID+":evidence:"+attempt.attemptID)
	if err != nil {
		return err
	}
	defer lease.Close()
	return outbox.recoverAttempt(ctx, client, attempt)
}

// EvidenceCustodyReceipt identifies durable storage only. It confers no live
// execution, renewal, publication or first-completion authority.
type EvidenceCustodyReceipt struct {
	OwnerGeneration     string             `json:"owner_generation"`
	CustodianGeneration string             `json:"custodian_generation"`
	Attempt             l1.ResidentAttempt `json:"attempt"`
	Digest              string             `json:"sha256"`
	AcknowledgedAt      time.Time          `json:"acknowledged_at"`
}

// EvidenceCustodyReceipts lets installation cleanup bind the still-retained
// receipts to its own proof. An empty answer is not evidence of delivery/reap.
func (a *Agent) EvidenceCustodyReceipts(ctx context.Context, generationID string) ([]EvidenceCustodyReceipt, error) {
	rows, err := a.logSpool.db.QueryContext(ctx, `SELECT owner_generation,custodian_generation,identity_json,digest,acknowledged_ns FROM generation_evidence_custody WHERE owner_generation=? ORDER BY attempt_id`, generationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	receipts := []EvidenceCustodyReceipt{}
	for rows.Next() {
		var receipt EvidenceCustodyReceipt
		var identity []byte
		var acknowledged int64
		if err := rows.Scan(&receipt.OwnerGeneration, &receipt.CustodianGeneration, &identity, &receipt.Digest, &acknowledged); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(identity, &receipt.Attempt); err != nil {
			return nil, err
		}
		receipt.AcknowledgedAt = time.Unix(0, acknowledged).UTC()
		receipts = append(receipts, receipt)
	}
	return receipts, rows.Err()
}
