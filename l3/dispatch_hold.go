package l3

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const dispatchHoldSchema = `
CREATE TABLE IF NOT EXISTS dispatch_hold (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 generation INTEGER NOT NULL DEFAULT 0,
 since_ns INTEGER,
 reason TEXT NOT NULL DEFAULT '',
 next_probe_ns INTEGER NOT NULL DEFAULT 0,
 failures INTEGER NOT NULL DEFAULT 0,
 probe_id TEXT NOT NULL DEFAULT '',
 probe_until_ns INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO dispatch_hold(singleton) VALUES(1);
CREATE TABLE IF NOT EXISTS dispatch_retry (
 run_id TEXT PRIMARY KEY REFERENCES runs(run_id),
 failures INTEGER NOT NULL,
 retry_ns INTEGER NOT NULL
);
`

// DispatchHealth reports availability of ledger dispatch independently of
// submission acceptance. Health is readable while L1 admission is unavailable.
type DispatchHealth struct {
	DispatchHold *contract.DispatchHold `json:"dispatch_hold,omitempty"`
}

func (s *Store) DispatchHealth(ctx context.Context) (DispatchHealth, error) {
	hold, err := readDispatchHold(ctx, s.db)
	return DispatchHealth{DispatchHold: hold}, err
}

type holdReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readDispatchHold(ctx context.Context, q holdReader) (*contract.DispatchHold, error) {
	var h contract.DispatchHold
	var since sql.NullInt64
	var next int64
	if err := q.QueryRowContext(ctx, `SELECT generation,since_ns,reason,next_probe_ns FROM dispatch_hold WHERE singleton=1`).Scan(&h.Generation, &since, &h.Reason, &next); err != nil {
		return nil, internalError(err, "read dispatch hold")
	}
	if !since.Valid {
		return nil, nil
	}
	h.Since = time.Unix(0, since.Int64).UTC()
	h.NextProbeAt = time.Unix(0, next).UTC()
	return &h, nil
}
func runDispatchHold(ctx context.Context, q holdReader, runID string) (*contract.DispatchHold, error) {
	var waiting bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runs r JOIN dispatch_outbox o ON o.run_id=r.run_id
 WHERE r.run_id=? AND r.status IN ('pending','dispatching') AND o.dispatched_ns IS NULL
 AND NOT EXISTS(SELECT 1 FROM run_cancellations c WHERE c.run_id=r.run_id))`, runID).Scan(&waiting)
	if err != nil {
		return nil, internalError(err, "read dispatch wait")
	}
	if !waiting {
		return nil, nil
	}
	return readDispatchHold(ctx, q)
}

// Every affirmative refusal advances the generation. Onset and the existing
// probe schedule survive repeated observations, and an in-flight reservation
// survives too: no other pass can start a second probe beside it.
func (s *Store) holdDispatch(ctx context.Context, reason string) error {
	now := s.recoveryNow().UnixNano()
	_, err := s.db.ExecContext(ctx, `UPDATE dispatch_hold SET generation=generation+1,
 since_ns=COALESCE(since_ns,?), reason=?,
 next_probe_ns=CASE WHEN since_ns IS NULL THEN ? ELSE next_probe_ns END,
 failures=CASE WHEN since_ns IS NULL THEN 0 ELSE failures END WHERE singleton=1`, now, reason, now+int64(time.Second))
	if err != nil {
		return internalError(err, "persist dispatch hold")
	}
	return nil
}
func (s *Store) observeL1Error(ctx context.Context, err error) error {
	a := classifyL1Answer(err)
	if a.Kind == l1LedgerNotAdmitted {
		if holdErr := s.holdDispatch(ctx, a.Reason); holdErr != nil {
			return errors.Join(proxyL1Error(err), holdErr)
		}
		return proxyL1Error(err)
	}
	return proxyL1Error(err)
}

// A reservation expires if a process dies; its unique ID fences late replies
// after expiry, while its generation fences a newer admission refusal.
type dispatchProbe struct {
	generation int64
	id, key    string
}

func (s *Store) reserveDispatchProbe(ctx context.Context, budget time.Duration) (*dispatchProbe, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, internalError(err, "reserve admission probe")
	}
	defer tx.Rollback()
	var generation int64
	var since sql.NullInt64
	var next, until int64
	if err := tx.QueryRowContext(ctx, `SELECT generation,since_ns,next_probe_ns,probe_until_ns FROM dispatch_hold WHERE singleton=1`).Scan(&generation, &since, &next, &until); err != nil {
		return nil, internalError(err, "read admission probe")
	}
	now := s.recoveryNow()
	if !since.Valid || next > now.UnixNano() || until > now.UnixNano() {
		return nil, nil
	}
	id := newToken()
	p := &dispatchProbe{generation: generation, id: id, key: "l3-admission-" + id}
	if _, err := tx.ExecContext(ctx, `UPDATE dispatch_hold SET probe_id=?,probe_until_ns=? WHERE singleton=1`, id, now.Add(budget+time.Second).UnixNano()); err != nil {
		return nil, internalError(err, "write admission probe")
	}
	if err := tx.Commit(); err != nil {
		return nil, internalError(err, "commit admission probe")
	}
	return p, nil
}
func (s *Store) finishDispatchProbe(ctx context.Context, p *dispatchProbe, admitted bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "finish admission probe")
	}
	defer tx.Rollback()
	var generation int64
	var id string
	var failures int
	if err := tx.QueryRowContext(ctx, `SELECT generation,probe_id,failures FROM dispatch_hold WHERE singleton=1`).Scan(&generation, &id, &failures); err != nil {
		return internalError(err, "read admission probe result")
	}
	if id != p.id {
		return nil
	}
	if admitted && generation == p.generation {
		_, err = tx.ExecContext(ctx, `UPDATE dispatch_hold SET since_ns=NULL,reason='',failures=0,next_probe_ns=0,probe_id='',probe_until_ns=0 WHERE singleton=1`)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE dispatch_hold SET failures=failures+1,next_probe_ns=?,probe_id='',probe_until_ns=0 WHERE singleton=1`, s.recoveryNow().Add(dispatchRetryBackoff(failures)).UnixNano())
	}
	if err != nil {
		return internalError(err, "write admission probe result")
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit admission probe result")
	}
	return nil
}
func dispatchRetryBackoff(failures int) time.Duration {
	if failures > 5 {
		return time.Minute
	}
	if failures < 0 {
		failures = 0
	}
	return time.Second * time.Duration(1<<failures)
}

func (r *Reconciler) probeDispatchHold(ctx context.Context) error {
	if r.lookup == nil {
		return nil
	}
	p, err := r.store.reserveDispatchProbe(ctx, r.budget)
	if err != nil || p == nil {
		return err
	}
	remote, cancel := context.WithTimeout(ctx, r.budget)
	defer cancel()
	job, err := r.lookup.LookupJobByDispatchKey(remote, p.key)
	admitted := isMissingDispatch(err, p.key) || (err == nil && job.JobID != "" && job.Spec.DispatchKey == p.key)
	if err == nil && !admitted {
		err = fmt.Errorf("invalid admission probe response")
	}
	// Probe refusal keeps the existing generation and onset; it is evidence of
	// the same hold, not a new refusal from an independent operation.
	finishErr := r.store.finishDispatchProbe(ctx, p, admitted)
	if admitted {
		return finishErr
	}
	return errors.Join(proxyL1Error(err), finishErr)
}
