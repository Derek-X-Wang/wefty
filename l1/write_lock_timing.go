package l1

import (
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
type writeLockTiming struct {
	store   *Store
	name    string
	pool    string
	started time.Time
	waited  time.Duration
	locked  time.Time
	ended   bool
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

func (t *writeLockTiming) acquired() {
	t.locked = time.Now()
	t.waited = t.locked.Sub(t.started)
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
	t.log(outcome, time.Since(t.locked))
}

func (t *writeLockTiming) log(outcome string, held time.Duration) {
	threshold := t.store.writeLockLogThreshold
	if threshold <= 0 {
		threshold = defaultWriteLockLogThreshold
	}
	if t.store.logf == nil || t.waited < threshold && held < threshold {
		return
	}
	t.store.logf("event=l1_write_lock_slow tx=%s pool=%s outcome=%s wait_ms=%d hold_ms=%d threshold_ms=%d",
		t.name, t.pool, outcome, t.waited.Milliseconds(), held.Milliseconds(), threshold.Milliseconds())
}
