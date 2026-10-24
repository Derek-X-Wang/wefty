package l1

import (
	"context"
	"strings"
)

func (s *Store) migrateSeatCalls(ctx context.Context) error {
	if err := s.ensureColumn(ctx, "computer_seat_identities", "ever_live", "INTEGER NOT NULL DEFAULT 0 CHECK(ever_live IN (0,1))"); err != nil {
		return err
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer write.rollback()
	// The original request table had SQLite's implicit NO ACTION node edge.
	var schema string
	if err := write.tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name='seat_request_calls'`).Scan(&schema); err != nil {
		return err
	}
	if !strings.Contains(schema, "ON DELETE CASCADE") {
		create := strings.Split(seatCallSchema, "CREATE INDEX")[0]
		create = strings.Replace(create, "seat_request_calls", "seat_request_calls_upgrade", 1)
		if _, err := write.tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS seat_call_scrub_final_insert; DROP TRIGGER IF EXISTS seat_call_scrub_final_update; DROP TRIGGER IF EXISTS seat_call_prune_final;`+create+`; INSERT INTO seat_request_calls_upgrade(seat_id,node_id,boot_session_id,caller_identity,input_json,credential,created_ns,start_finished) SELECT seat_id,node_id,boot_session_id,caller_identity,input_json,credential,created_ns,start_finished FROM seat_request_calls; DROP TABLE seat_request_calls; ALTER TABLE seat_request_calls_upgrade RENAME TO seat_request_calls; CREATE INDEX seat_request_calls_node ON seat_request_calls(node_id,created_ns,seat_id);`); err != nil {
			return err
		}
	}
	// The permanent Seat identity outlives prunable call receipts. Preserve the
	// once-Live Computer guard before any receipt can be pruned.
	_, err = write.tx.ExecContext(ctx, `
 UPDATE computer_seat_identities SET ever_live=1 WHERE seat_id IN (SELECT seat_id FROM seat_request_calls WHERE start_finished=1);
 UPDATE seat_request_calls SET credential='' WHERE seat_id IN (SELECT seat_id FROM seat_approvals WHERE state='refused' OR lifecycle_state IN ('ending','ended','refused'));
 CREATE TRIGGER IF NOT EXISTS seat_call_scrub_final_insert AFTER INSERT ON seat_approvals WHEN NEW.state='refused' BEGIN
  UPDATE seat_request_calls SET credential='' WHERE seat_id=NEW.seat_id;
 END;
 CREATE TRIGGER IF NOT EXISTS seat_call_scrub_final_update AFTER UPDATE OF state,lifecycle_state ON seat_approvals WHEN NEW.state='refused' OR NEW.lifecycle_state IN ('ending','ended','refused') BEGIN
  UPDATE seat_request_calls SET credential='' WHERE seat_id=NEW.seat_id;
 END;
 CREATE TRIGGER IF NOT EXISTS seat_call_prune_final AFTER DELETE ON seat_approvals WHEN OLD.state='refused' OR OLD.lifecycle_state IN ('ended','refused') BEGIN
  DELETE FROM seat_request_calls WHERE seat_id=OLD.seat_id;
 END;
 `)
	if err != nil {
		return err
	}
	return write.commit()
}
