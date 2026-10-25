package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

var copyIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,200}$`)
var copyReasonPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

func ValidSeatCopyID(id string) bool { return copyIDPattern.MatchString(id) }

type SeatCopyOutcomeReceipt struct {
	CopyID    string `json:"copy_id"`
	Direction string `json:"direction"`
	Path      string `json:"path"`
	SizeBytes int64  `json:"size_bytes"`
	Outcome   string `json:"outcome"`
	Reason    string `json:"reason,omitempty"`
}

func validCopyReceipt(r SeatCopyOutcomeReceipt, node bool) bool {
	if !ValidSeatCopyID(r.CopyID) || r.Path == "" || len(r.Path) > 4096 || strings.ContainsRune(r.Path, 0) || r.SizeBytes < 0 || (r.Direction != "in" && r.Direction != "out" && r.Direction != "sync") {
		return false
	}
	if r.Outcome == "failed" {
		return copyReasonPattern.MatchString(r.Reason)
	}
	if r.Reason != "" {
		return false
	}
	if node {
		if r.Outcome == "sending" {
			return r.SizeBytes == 0
		}
		return (r.Direction == "in" || r.Direction == "sync") && r.Outcome == "completed" || r.Direction == "out" && r.Outcome == "sent"
	}
	return r.Direction == "out" && r.Outcome == "completed"
}
func (s *Store) ReportSeatCopyOutcome(ctx context.Context, id, credential string, r SeatCopyOutcomeReceipt) error {
	if credential == "" {
		return protocolError(contract.ErrorForbidden, "Seat credential is required")
	}
	if r.Direction != "out" || !validCopyReceipt(r, false) {
		return protocolError(contract.ErrorInvalidRequest, "invalid copy outcome")
	}
	if _, err := s.GetSeatLogs(ctx, "", id, credential, "", 1); err != nil {
		return err
	}
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer w.rollback()
	// Check the live boundary in the same transaction as the update. Retained
	// ended credentials can read logs, but cannot mutate copy evidence.
	if _, err = checkSeatCopyCredential(ctx, w.tx, id, credential, true); err != nil {
		return err
	}
	var raw []byte
	if err = w.tx.QueryRowContext(ctx, `SELECT event_json FROM seat_log_events WHERE seat_id=? AND event_id=?`, id, "copy:"+r.CopyID).Scan(&raw); errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorNotFound, "node copy event not found")
	} else if err != nil {
		return err
	}
	var old SeatLogEvent
	if err = json.Unmarshal(raw, &old); err != nil {
		return err
	}
	if old.NodeOutcome == "" {
		return protocolError(contract.ErrorNotFound, "node copy event not found")
	}
	if old.Kind != SeatEventCopyOut || old.Path != r.Path || old.CopyID != r.CopyID || old.SizeBytes == nil || *old.SizeBytes != r.SizeBytes {
		return protocolError(contract.ErrorIdempotencyConflict, "copy ID has different metadata")
	}
	if old.NodeOutcome != "sent" {
		return protocolError(contract.ErrorConflict, "copy-out has no sent receipt")
	}
	if old.Outcome != "sent" {
		if old.Outcome == r.Outcome && old.Reason == r.Reason {
			return nil
		}
		return protocolError(contract.ErrorIdempotencyConflict, "copy ID has different local outcome")
	}
	old.Outcome, old.Reason = r.Outcome, r.Reason
	old.OccurredAt = w.at
	payload, err := json.Marshal(old)
	if err != nil {
		return err
	}
	// Move the same event to a fresh cursor position so a reader that saw sent
	// also sees the final local outcome. Its immutable node evidence stays put.
	if _, err = w.tx.ExecContext(ctx, `UPDATE seat_log_events SET ordinal=(SELECT COALESCE(MAX(ordinal),0)+1 FROM seat_log_events),event_json=? WHERE seat_id=? AND event_id=?`, payload, id, old.EventID); err != nil {
		return err
	}
	return w.commit()
}

// Only the node creates copy events. Local publication receipts never append
// and never change the immutable node direction, path, size or result.
func (s *Store) appendSeatCopyOutcome(ctx context.Context, id string, r SeatCopyOutcomeReceipt, syncCounts ...*SeatSyncCounts) (bool, error) {
	kind := SeatEventCopyIn
	if r.Direction == "out" {
		kind = SeatEventCopyOut
	}
	incoming := SeatLogEvent{SeatID: id, EventID: "copy:" + r.CopyID, Kind: kind, CopyID: r.CopyID, Path: r.Path, SizeBytes: &r.SizeBytes, Outcome: r.Outcome, NodeOutcome: r.Outcome, Reason: r.Reason}
	if r.Direction == "sync" {
		incoming.Direction = "sync"
		if len(syncCounts) > 0 {
			incoming.Sync = syncCounts[0]
		}
	}
	err := s.AppendSeatEvent(ctx, incoming)
	if errorCodeForSeatLifecycle(err) != contract.ErrorIdempotencyConflict {
		return r.Outcome == "sending", err
	}
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return false, err
	}
	defer w.rollback()
	var raw []byte
	if err := w.tx.QueryRowContext(ctx, `SELECT event_json FROM seat_log_events WHERE seat_id=? AND event_id=?`, id, incoming.EventID).Scan(&raw); err != nil {
		return false, err
	}
	var old SeatLogEvent
	if err := json.Unmarshal(raw, &old); err != nil {
		return false, err
	}
	if old.Direction != incoming.Direction || old.Kind != incoming.Kind || old.CopyID != r.CopyID || old.Path != r.Path || old.SizeBytes == nil || old.NodeOutcome == "" {
		return false, protocolError(contract.ErrorIdempotencyConflict, "copy ID has different node evidence")
	}
	if r.Outcome == "sending" {
		// A delayed start retry never resurrects a finished operation's hold.
		return old.NodeOutcome == "sending", nil
	}
	if old.NodeOutcome != "sending" {
		if !equalSeatSyncCounts(old.Sync, incoming.Sync) || *old.SizeBytes != r.SizeBytes || old.NodeOutcome != r.Outcome || (r.Outcome == "failed" && old.Reason != r.Reason) {
			return false, protocolError(contract.ErrorIdempotencyConflict, "copy ID has different node evidence")
		}
		return false, nil
	}
	old.NodeOutcome, old.Outcome, old.Reason = r.Outcome, r.Outcome, r.Reason
	old.SizeBytes, old.OccurredAt = &r.SizeBytes, w.at
	old.Sync = incoming.Sync
	payload, err := json.Marshal(old)
	if err != nil {
		return false, err
	}
	if _, err = w.tx.ExecContext(ctx, `UPDATE seat_log_events SET ordinal=(SELECT COALESCE(MAX(ordinal),0)+1 FROM seat_log_events),event_json=? WHERE seat_id=? AND event_id=?`, payload, id, old.EventID); err != nil {
		return false, err
	}
	return false, w.commit()
}
