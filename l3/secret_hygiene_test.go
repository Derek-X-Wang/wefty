package l3

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

// The ledger zeroes what it deletes or clears, as L1 does (#52).
func TestLedgerOpensWithSecureDelete(t *testing.T) {
	s, _, _ := recoveryStore(t)
	var secureDelete int
	if err := s.db.QueryRow(`PRAGMA secure_delete`).Scan(&secureDelete); err != nil {
		t.Fatal(err)
	}
	if secureDelete != 1 {
		t.Fatalf("secure_delete = %d, want 1", secureDelete)
	}
}

// L1 accepted the job, L3 crashed before recording the dispatch, and the run
// then failed on its own protocol write. It is never dispatched again, so its
// staged run-token bearer must not stay in the outbox (#52).
func TestTerminalRunDropsItsStagedTokenDelivery(t *testing.T) {
	s, _, _ := recoveryStore(t)
	ctx := context.Background()
	record, _, err := s.CreateRun(ctx, CreateRunInput{IdempotencyKey: "staged-token", Actor: "test", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.ensureRunToken(ctx, record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.beginDispatch(ctx, record.RunID); err != nil {
		t.Fatal(err)
	}
	var staged sql.NullString
	if err := s.db.QueryRow(`SELECT token_delivery FROM dispatch_outbox WHERE run_id=?`, record.RunID).Scan(&staged); err != nil {
		t.Fatal(err)
	}
	if staged.String != token {
		t.Fatalf("fixture error: staged delivery = %v, want the minted bearer", staged)
	}
	if err := s.rejectProtocolWrite(ctx, record.RunID, "envelope", "rejected", []byte(`{}`), "hash", "invalid envelope", errors.New("invalid envelope")); err != nil {
		t.Fatal(err)
	}
	var status contract.RunState
	if err := s.db.QueryRow(`SELECT r.status, o.token_delivery FROM runs r JOIN dispatch_outbox o ON o.run_id=r.run_id WHERE r.run_id=?`, record.RunID).
		Scan(&status, &staged); err != nil {
		t.Fatal(err)
	}
	if status != contract.RunFailed || staged.Valid {
		t.Fatalf("failed run status=%s staged delivery=%v, want failed with no bearer", status, staged)
	}
}
