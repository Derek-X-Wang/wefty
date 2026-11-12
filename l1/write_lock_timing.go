package l1

import (
	"context"
	"runtime"
	"strings"
	"time"
)

// defaultWriteLockLogThreshold is how long a write transaction may wait for,
// or hold, SQLite's single write lock before L1 logs it by name (#1524). It
// is below the main pool's five-second busy wait, so a waiter that gives up
// and the holder it waited on both appear.
const defaultWriteLockLogThreshold = 2 * time.Second

// writeLockTiming measures one write transaction. The DSN's immediate
// transactions take the write lock at BEGIN, so the wait is BeginTx (pool
// checkout included) and the hold runs from BEGIN to commit or rollback,
// including the commit's fsync. One goroutine owns a transaction, and only
// its first end is recorded: a deferred rollback after commit is ignored.
// When the transaction's context has ended and it did not commit,
// database/sql may have rolled it back on its own, possibly finishing after the
// owner returns. Nothing here observes that release, so every such transaction
// is logged as event=l1_write_lock_canceled with release=unknown and the hold
// up to the owner's end, whatever its timing. The check reads the context at
// the end, so it cannot race a callback.
type writeLockTiming struct {
	store   *Store
	name    string
	pool    string
	started time.Time
	waited  time.Duration
	locked  time.Time
	ended   bool
	ctx     context.Context
}

// startWriteLockTiming names the transaction after the function skip frames
// above it, for example "l1.(*Store).RenewLease".
func (s *Store) startWriteLockTiming(pool string, skip int) *writeLockTiming {
	name := "unknown"
	if pc, _, _, ok := runtime.Caller(skip); ok {
		if function := runtime.FuncForPC(pc); function != nil {
			name = function.Name()
			name = name[strings.LastIndex(name, "/")+1:]
		}
	}
	return &writeLockTiming{store: s, name: name, pool: pool, started: time.Now()}
}

// acquired starts the hold. ctx is the transaction's context, or nil when no
// transaction is bound to one.
func (t *writeLockTiming) acquired(ctx context.Context) {
	t.locked = time.Now()
	t.waited = t.locked.Sub(t.started)
	t.ctx = ctx
}

func (t *writeLockTiming) beginFailed() {
	t.ended = true
	t.waited = time.Since(t.started)
	t.log("begin_failed", 0)
}

func (t *writeLockTiming) end(outcome string) {
	if t == nil || t.ended {
		return
	}
	t.ended = true
	held := time.Since(t.locked)
	// A commit that succeeded released the lock itself, even if the context
	// ended during it.
	if t.ctx != nil && t.ctx.Err() != nil && outcome != "commit" {
		t.logCanceled(outcome, held)
		return
	}
	t.log(outcome, held)
}

func (t *writeLockTiming) threshold() time.Duration {
	if t.store.writeLockLogThreshold > 0 {
		return t.store.writeLockLogThreshold
	}
	return defaultWriteLockLogThreshold
}

func (t *writeLockTiming) log(outcome string, held time.Duration) {
	threshold := t.threshold()
	if t.waited < threshold && held < threshold {
		return
	}
	t.store.logf("event=l1_write_lock_slow tx=%s pool=%s outcome=%s wait_ms=%d hold_ms=%d threshold_ms=%d",
		t.name, t.pool, outcome, t.waited.Milliseconds(), held.Milliseconds(), threshold.Milliseconds())
}

func (t *writeLockTiming) logCanceled(outcome string, heldToOwnerEnd time.Duration) {
	t.store.logf("event=l1_write_lock_canceled tx=%s pool=%s outcome=%s release=unknown wait_ms=%d hold_ms=%d",
		t.name, t.pool, outcome, t.waited.Milliseconds(), heldToOwnerEnd.Milliseconds())
}
