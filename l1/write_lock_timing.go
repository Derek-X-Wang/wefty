package l1

import (
	"context"
	"runtime"
	"strings"
	"sync/atomic"
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
// When the transaction's context ends first, database/sql rolls it back on its
// own, after any in-flight statement returns. The owner cannot see when that
// rollback finished, so such a line says outcome=canceled, keeps hold_ms as
// the upper bound (up to the owner's end), and adds canceled_after_ms, when
// the context ended. A slow holder is never hidden by an early cancellation.
type writeLockTiming struct {
	store      *Store
	name       string
	pool       string
	started    time.Time
	waited     time.Duration
	locked     time.Time
	ended      bool
	canceledAt atomic.Int64
	stopCancel func() bool
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
	if ctx != nil {
		t.stopCancel = context.AfterFunc(ctx, func() { t.canceledAt.Store(time.Now().UnixNano()) })
	}
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
	if t.stopCancel != nil {
		t.stopCancel()
	}
	held := time.Since(t.locked)
	// A commit that succeeded released the lock itself, even if the context
	// ended during it.
	if canceled := t.canceledAt.Load(); canceled != 0 && outcome != "commit" {
		t.logCanceled(held, time.Unix(0, canceled).Sub(t.locked))
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
	if t.store.logf == nil || t.waited < threshold && held < threshold {
		return
	}
	t.store.logf("event=l1_write_lock_slow tx=%s pool=%s outcome=%s wait_ms=%d hold_ms=%d threshold_ms=%d",
		t.name, t.pool, outcome, t.waited.Milliseconds(), held.Milliseconds(), threshold.Milliseconds())
}

func (t *writeLockTiming) logCanceled(heldAtMost, canceledAfter time.Duration) {
	threshold := t.threshold()
	if t.store.logf == nil || t.waited < threshold && heldAtMost < threshold {
		return
	}
	t.store.logf("event=l1_write_lock_slow tx=%s pool=%s outcome=canceled wait_ms=%d hold_ms=%d canceled_after_ms=%d threshold_ms=%d",
		t.name, t.pool, t.waited.Milliseconds(), heldAtMost.Milliseconds(), canceledAfter.Milliseconds(), threshold.Milliseconds())
}
