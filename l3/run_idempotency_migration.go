package l3

import (
	"context"
	"fmt"
	"strings"
)

// SQLite cannot drop the old column-level UNIQUE constraint. Rebuild only
// runs, on one connection with foreign keys disabled outside the transaction;
// never rename the old table, which would retarget every child foreign key.
// The table replacement, provenance backfill and index restoration commit
// together. Missing provenance refuses the upgrade rather than inventing an
// actor, dropping a run or releasing its existing key.
func (s *Store) migrateRunActorKeys(ctx context.Context) (err error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var scoped int
	if err := conn.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('runs') WHERE name='actor'`).Scan(&scoped); err != nil {
		return err
	}
	if scoped != 0 {
		return nil
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys=OFF`); err != nil {
		return err
	}
	defer func() {
		if _, restoreErr := conn.ExecContext(context.Background(), `PRAGMA foreign_keys=ON`); restoreErr != nil {
			err = fmt.Errorf("restore foreign key enforcement (migration error: %v): %w", err, restoreErr)
		}
	}()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Recheck under the write lock: another opener may have migrated while
	// this connection was waiting to begin its transaction.
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('runs') WHERE name='actor'`).Scan(&scoped); err != nil {
		return err
	}
	if scoped != 0 {
		return nil
	}
	var missing int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM runs r LEFT JOIN run_triggers t USING(run_id) WHERE t.actor IS NULL OR trim(t.actor)=''`).Scan(&missing); err != nil {
		return err
	}
	if missing != 0 {
		return fmt.Errorf("%d existing runs lack recorded actor provenance", missing)
	}

	// Preserve all existing run indexes/triggers, including partial recovery
	// indexes. Autoindexes are recreated by the new table's constraints.
	rows, err := tx.QueryContext(ctx, `SELECT sql FROM sqlite_schema WHERE tbl_name='runs' AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type,name`)
	if err != nil {
		return err
	}
	var schemaObjects []string
	for rows.Next() {
		var statement string
		if err := rows.Scan(&statement); err != nil {
			rows.Close()
			return err
		}
		schemaObjects = append(schemaObjects, statement)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// All previous additive migrations have run before this replacement. Use
	// the actual legacy columns to retain their values, including timestamps,
	// projection state and crash-recovery diagnostics, without defaulting them.
	rows, err = tx.QueryContext(ctx, `SELECT name FROM pragma_table_info('runs') ORDER BY cid`)
	if err != nil {
		return err
	}
	var columns, selections []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return err
		}
		quoted := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		columns = append(columns, quoted)
		selections = append(selections, "r."+quoted)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	replacement := strings.Replace(runTableSchema, "CREATE TABLE IF NOT EXISTS runs (", "CREATE TABLE runs_actor_scoped (", 1)
	if _, err := tx.ExecContext(ctx, replacement); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO runs_actor_scoped (`+strings.Join(columns, ",")+`,actor) SELECT `+strings.Join(selections, ",")+`,t.actor FROM runs r JOIN run_triggers t USING(run_id)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE runs`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE runs_actor_scoped RENAME TO runs`); err != nil {
		return err
	}
	for _, statement := range schemaObjects {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	rows, err = tx.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return err
	}
	invalid := rows.Next()
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if invalid {
		return fmt.Errorf("run table replacement failed foreign key validation")
	}
	return tx.Commit()
}
