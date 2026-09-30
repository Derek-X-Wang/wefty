package l1

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// A one-shot job's secrets do not outlive its execution (wefty #52). Once a
// one-shot is terminal -- succeeded or failed, which v1 never requeues -- its
// stored specification loses the three things that only an execution needed:
//
//   - execution.sensitive_env, which carries the plaintext WEFTY_RUN_TOKEN of
//     every L3 run;
//   - execution.executable.inline_base64, the script bytes (L3 keeps its own
//     program snapshot, which is what a rerun is built from); the sha256 stays
//     as the record of what ran;
//   - the run_params_json label, the parameter document L3 already keeps on
//     the run.
//
// Everything else stays, so the job remains a parseable, permanent record.
// jobs.secrets_scrubbed_ns records when it happened, and the public projection
// reports it as secrets_scrubbed_at, so an absent inline_base64 is never
// mistaken for a job that had none.
//
// The dispatch-key replay check compares jobs.request_hash, which is computed
// once from the submitted request and never recomputed from spec_json, so an
// identical replay of a scrubbed job still returns it.
//
// The scrub is a trigger rather than a call at each terminal transition, so a
// one-shot cannot reach succeeded or failed by any path -- completion, lease
// expiry, the claim path's pre-start budget expiry, one added later -- and
// keep its secrets: the state change and the scrub are one statement's
// effects and commit together.
const scrubbedOneShotSpecSQL = `CAST(json_remove(CAST(spec_json AS TEXT),
	'$.execution.sensitive_env',
	'$.execution.executable.inline_base64',
	'$.labels.run_params_json') AS BLOB)`

const terminalOneShotSecretScrubSchema = `
CREATE TABLE IF NOT EXISTS secret_scrub_state (
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  generation INTEGER NOT NULL CHECK(generation >= 0)
);
INSERT OR IGNORE INTO secret_scrub_state(singleton, generation) VALUES(1, 0);
CREATE INDEX IF NOT EXISTS jobs_secrets_unscrubbed ON jobs(state) WHERE secrets_scrubbed_ns IS NULL;
CREATE TRIGGER IF NOT EXISTS jobs_scrub_terminal_one_shot_secrets
AFTER UPDATE OF state ON jobs
WHEN NEW.state IN ('succeeded', 'failed') AND NEW.secrets_scrubbed_ns IS NULL
	AND json_extract(NEW.spec_json, '$.class') = 'one-shot'
BEGIN
	UPDATE jobs SET spec_json=` + scrubbedOneShotSpecSQL + `, secrets_scrubbed_ns=NEW.updated_ns
		WHERE job_id=NEW.job_id;
	UPDATE secret_scrub_state SET generation=generation+1 WHERE singleton=1;
END;`

// secretScrubBackfillBatch bounds one sweep's backfill. A database written
// before the trigger existed is scrubbed over successive reconcile ticks
// rather than in one long write transaction at startup.
const secretScrubBackfillBatch = 256

// secretWALCheckpointWait is the fixed lock wait of the handle the sweep
// truncates the WAL on (Store.checkpointDB). A TRUNCATE checkpoint waits,
// through that wait, for every reader to leave the WAL and holds off new
// writers meanwhile, so this bounds what a held reader costs the writers and
// the reconcile loop. The driver does not interrupt the wait when a context
// ends, which is why the bound is the handle's and not a deadline's. A
// truncation that cannot finish within it is deferred to the next tick.
const secretWALCheckpointWait = 250 * time.Millisecond

// SecretScrubSweep reports one pass of SweepScrubbedSecrets.
type SecretScrubSweep struct {
	// Backfilled counts terminal one-shots scrubbed by this pass rather than
	// by the trigger, i.e. rows written before the trigger existed.
	Backfilled int64
	// TruncatedWAL is true when this pass truncated the WAL because a scrub
	// had committed since the last truncation.
	TruncatedWAL bool
	// TruncationDeferred is true when a truncation was due but a reader or
	// another checkpoint held the WAL past secretWALCheckpointWait. Nothing
	// is recorded, so the next pass tries again.
	TruncationDeferred bool
}

func (s *Store) initializeSecretScrub(ctx context.Context) error {
	if err := s.ensureColumn(ctx, "jobs", "secrets_scrubbed_ns", "INTEGER"); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, terminalOneShotSecretScrubSchema); err != nil {
		return fmt.Errorf("l1: install terminal one-shot secret scrub: %w", err)
	}
	return nil
}

// SweepScrubbedSecrets finishes what the terminal transaction started. It
// scrubs a bounded batch of terminal one-shots that predate the trigger, then
// truncates the WAL if any scrub has committed since the last truncation: the
// replaced page images, secret bytes included, otherwise stay in the WAL file
// until SQLite happens to overwrite them. secure_delete already zeroes the
// freed bytes in the main database file.
//
// Truncation is batched here, off the completion path, because a TRUNCATE
// checkpoint must wait for every reader to leave the WAL and blocks new
// writers while it waits; doing it inside each completion would put that wait
// on the agent's completion call. The L1 server runs this after every
// reconcile tick, so a scrubbed secret leaves the WAL within about one tick
// unless a reader holds the WAL for longer; a deferred truncation is retried
// on the next tick.
func (s *Store) SweepScrubbedSecrets(ctx context.Context) (SecretScrubSweep, error) {
	s.secretWALMu.Lock()
	defer s.secretWALMu.Unlock()
	var sweep SecretScrubSweep
	backfilled, err := s.backfillTerminalOneShotSecrets(ctx)
	if err != nil {
		return sweep, err
	}
	sweep.Backfilled = backfilled
	var generation int64
	if err := s.db.QueryRowContext(ctx, `SELECT generation FROM secret_scrub_state WHERE singleton=1`).Scan(&generation); err != nil {
		return sweep, internalError(err, "read secret scrub generation")
	}
	if s.secretWALTruncated && generation == s.secretWALGeneration {
		return sweep, nil
	}
	// The generation was read before the checkpoint, so every scrub it counts
	// had committed into the WAL this checkpoint empties. A scrub committing
	// after the read advances the generation past it and is caught next pass.
	truncated, err := s.truncateSecretWAL(ctx)
	if err != nil {
		return sweep, err
	}
	if !truncated {
		sweep.TruncationDeferred = true
		return sweep, nil
	}
	s.secretWALGeneration = generation
	s.secretWALTruncated = true
	sweep.TruncatedWAL = true
	return sweep, nil
}

// truncateSecretWAL makes one TRUNCATE checkpoint attempt on the short-wait
// handle. It reports false, without error, when SQLite could not finish
// within that handle's wait: the checkpoint answers busy, or refuses with
// SQLITE_BUSY because another checkpoint is running.
func (s *Store) truncateSecretWAL(ctx context.Context) (bool, error) {
	var busy, logFrames, checkpointedFrames int
	err := s.checkpointDB.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &checkpointedFrames)
	var sqliteErr sqliteErrorCoder
	if errors.As(err, &sqliteErr) && sqliteErr.Code()&0xff == sqliteBusyPrimaryCode {
		return false, nil
	}
	if err != nil {
		return false, internalError(err, "truncate secret-bearing SQLite WAL")
	}
	return busy == 0, nil
}

func (s *Store) backfillTerminalOneShotSecrets(ctx context.Context) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, internalError(err, "begin terminal one-shot secret backfill")
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE jobs SET spec_json=`+scrubbedOneShotSpecSQL+`, secrets_scrubbed_ns=?
		WHERE job_id IN (
			SELECT job_id FROM jobs
			WHERE secrets_scrubbed_ns IS NULL AND state IN ('succeeded', 'failed')
				AND json_extract(spec_json, '$.class')='one-shot'
			ORDER BY job_id LIMIT ?
		)`, canonicalTime(s.clock.Now()).UnixNano(), secretScrubBackfillBatch)
	if err != nil {
		return 0, internalError(err, "scrub terminal one-shot secrets")
	}
	scrubbed, err := result.RowsAffected()
	if err != nil {
		return 0, internalError(err, "read terminal one-shot secret backfill")
	}
	if scrubbed == 0 {
		return 0, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE secret_scrub_state SET generation=generation+1 WHERE singleton=1`); err != nil {
		return 0, internalError(err, "advance secret scrub generation")
	}
	if err := tx.Commit(); err != nil {
		return 0, internalError(err, "commit terminal one-shot secret backfill")
	}
	return scrubbed, nil
}
