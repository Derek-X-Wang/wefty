package l1

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
)

type writeLockLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *writeLockLog) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

// slow returns the slow write-lock lines for one transaction name.
func (l *writeLockLog) slow(name string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var found []string
	for _, line := range l.lines {
		if strings.HasPrefix(line, "event=l1_write_lock_slow tx="+name+" ") {
			found = append(found, line)
		}
	}
	return found
}

func writeLockField(t *testing.T, line, field string) time.Duration {
	t.Helper()
	match := regexp.MustCompile(` ` + field + `=(\d+)`).FindStringSubmatch(line)
	if match == nil {
		t.Fatalf("%s missing from %q", field, line)
	}
	ms, _ := strconv.Atoi(match[1])
	return time.Duration(ms) * time.Millisecond
}

// Named package functions, not closures, so the logged names are stable.
func holdWriteLockForTest(s *Store, held chan<- struct{}, hold time.Duration) error {
	write, err := s.beginWriteTransaction(context.Background(), nil)
	if err != nil {
		return err
	}
	defer write.rollback()
	close(held)
	time.Sleep(hold)
	return write.commit()
}

func waitWriteLockForTest(s *Store) error {
	write, err := s.beginWriteTransaction(context.Background(), nil)
	if err != nil {
		return err
	}
	defer write.rollback()
	return write.commit()
}

// #1524: a holder and the writer it blocked are both logged by name, with the
// lock hold and the lock wait, so a stall names what held SQLite's write lock.
func TestSlowWriteLockLogsHolderAndWaiter(t *testing.T) {
	s, _ := snapshotStore(t)
	log := &writeLockLog{}
	s.logf = log.logf
	s.writeLockLogThreshold = 100 * time.Millisecond
	held := make(chan struct{})
	holder := make(chan error, 1)
	go func() { holder <- holdWriteLockForTest(s, held, 400*time.Millisecond) }()
	<-held
	if err := waitWriteLockForTest(s); err != nil {
		t.Fatal(err)
	}
	if err := <-holder; err != nil {
		t.Fatal(err)
	}

	holds := log.slow("l1.holdWriteLockForTest")
	if len(holds) != 1 || !strings.Contains(holds[0], " pool=main outcome=commit ") || writeLockField(t, holds[0], "hold_ms") < 400*time.Millisecond {
		t.Fatalf("holder lines = %q (all %q)", holds, log.lines)
	}
	waits := log.slow("l1.waitWriteLockForTest")
	if len(waits) != 1 || !strings.Contains(waits[0], " outcome=commit ") || writeLockField(t, waits[0], "wait_ms") < 200*time.Millisecond {
		t.Fatalf("waiter lines = %q (all %q)", waits, log.lines)
	}

	// A quick transaction stays quiet at the default threshold, and the
	// deferred rollback after commit records nothing more.
	log.lines = nil
	s.writeLockLogThreshold = 0
	if err := waitWriteLockForTest(s); err != nil {
		t.Fatal(err)
	}
	if len(log.lines) != 0 {
		t.Fatalf("quick transaction logged %q", log.lines)
	}
}

// #1632: Reconcile samples its wall time after it holds the write lock, so
// authority_lost_at cannot precede the moment the pass could act.
func TestReconcileStampsAuthorityLossAfterWriteLock(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-node-1", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
	h.submit(client, "lock-wait-expiry", []string{"linux"})
	claim := claimJob(t, h, agent, "node-1")
	h.clock.Advance(30 * time.Second)

	// Another writer holds the lock while Reconcile waits; L1's clock moves on.
	blocker, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		result, err := h.store.Reconcile(context.Background())
		if err == nil && result.ExpiredAttempts != 1 {
			err = fmt.Errorf("expired attempts = %d, want 1", result.ExpiredAttempts)
		}
		done <- err
	}()
	time.Sleep(300 * time.Millisecond)
	h.clock.Advance(3 * time.Second)
	unblocked := h.clock.Now()
	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var lostNS int64
	if err := h.store.db.QueryRow(`SELECT updated_ns FROM attempts WHERE attempt_id=?`, claim.Lease.AttemptID).Scan(&lostNS); err != nil {
		t.Fatal(err)
	}
	if lost := time.Unix(0, lostNS).UTC(); !lost.Equal(unblocked) {
		t.Fatalf("authority lost at %s, want %s (after the lock wait)", lost, unblocked)
	}
}
