package l1

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestAttemptLeaseComputerTokenScopeIgnoresWallStep(t *testing.T) {
	h, computer, claim := liveComputerTokenScope(t, "wall-step-scope")
	h.stopServer()
	// The fixture claimed on its injected timeline. Keep that boot and elapsed
	// origin, then move wall time independently of elapsed time.
	h.store.clock = &seatBootTestClock{fakeClock: h.clock, boot: "injected-clock", elapsed: time.Duration(h.clock.Now().UnixNano())}
	before, err := h.store.ProveComputerTokenScope(t.Context(), computer.ComputerID, claim.Lease.AttemptID, "fabric-computer-node", "")
	if err != nil {
		t.Fatal(err)
	}
	h.clock.Advance(2 * time.Hour)
	after, err := h.store.ProveComputerTokenScope(t.Context(), computer.ComputerID, claim.Lease.AttemptID, "fabric-computer-node", "")
	if err != nil || after.ComputerAttemptID != before.ComputerAttemptID {
		t.Fatalf("same-boot Computer scope after wall step=%+v err=%v", after, err)
	}
}

func TestAttemptLeaseSeatListIgnoresWallStep(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	clock := &seatBootTestClock{fakeClock: f.clock, boot: "seat-list-boot", elapsed: time.Hour}
	f.store.clock = clock
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	before, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(2 * time.Hour)
	after, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil || after.Revision != before.Revision {
		t.Fatalf("same-boot Seat list after wall step=%+v err=%v", after, err)
	}
}

func TestAttemptLeaseMissingClockRefusesAuthority(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	for _, ctx := range []context.Context{t.Context(), context.WithValue(t.Context(), attemptClockKey{}, attemptClock{})} {
		for _, state := range []contract.AttemptState{contract.AttemptRunning, contract.AttemptSucceeded} {
			if _, err := attemptLeaseExpired(ctx, now, attemptAuthority{state: state, leaseExpires: now.Add(time.Hour)}); err == nil {
				t.Errorf("missing transaction clock silently used wall time for %s", state)
			}
		}
		if currentAttemptClock(ctx).sqlBoot() != nil {
			t.Error("missing transaction clock enabled SQL lease authority")
		}
	}
}

func TestAttemptLeaseDeadlineRefusesClockError(t *testing.T) {
	ctx := context.WithValue(t.Context(), attemptClockKey{}, attemptClock{boot: "boot", ns: int64(time.Hour), err: errors.New("elapsed clock unavailable")})
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	if boot, ns := attemptLeaseDeadline(ctx, now, now.Add(time.Minute)); boot != "" || ns != 0 {
		t.Fatalf("failed clock minted deadline: boot=%q ns=%d", boot, ns)
	}
}

// Identity can fail after opening without making the independent elapsed
// source fail. BootTime models the old combined source for the regression.
type identityFailureClock struct {
	*seatBootTestClock
	identityCalls int
	failIdentity  bool
}

func (c *identityFailureClock) BootIdentity() (string, error) {
	c.identityCalls++
	if c.failIdentity {
		return "", errors.New("boot identity source unavailable")
	}
	return c.boot, nil
}
func (c *identityFailureClock) BootElapsed() (time.Duration, error) { return c.elapsed, nil }
func (c *identityFailureClock) BootTime() (string, time.Duration, error) {
	boot, err := c.BootIdentity()
	return boot, c.elapsed, err
}

func TestAttemptLeaseBootIdentityCachedAtOpen(t *testing.T) {
	f := newAttemptClockFixture(t)
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	clock := &identityFailureClock{seatBootTestClock: f.clock}
	f.options.Clock = clock
	var err error
	f.store, err = OpenStore(f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	if clock.identityCalls != 1 {
		t.Errorf("OpenStore identity reads=%d, want one", clock.identityCalls)
	}
	clock.failIdentity = true
	f.clock.Advance(2 * time.Hour)
	f.clock.elapsed += time.Second
	if _, err := f.store.ResolveAttemptCredential(t.Context(), f.claim.AttemptToken, "agent-lease-node"); err != nil {
		t.Fatalf("cached identity lost credential authority: %v", err)
	}
	if _, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken); err != nil {
		t.Fatalf("cached identity refused renewal: %v", err)
	}
	f.reconcile(t, 0)
	if _, _, err := f.store.CreateJob(t.Context(), validJobSpec("cached-identity-claim", nil)); err != nil {
		t.Fatal(err)
	}
	if claim, err := f.store.ClaimJob(t.Context(), "agent-lease-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassOneShot); err != nil || claim == nil {
		t.Fatalf("cached identity refused claim: %+v %v", claim, err)
	}
	// Seat and Attempt timers must share the Store's one cached identity.
	if boot, ns, err := seatTime(f.store.clock); err != nil || boot != f.clock.boot || ns != int64(f.clock.elapsed) {
		t.Fatalf("Seat clock differs from cached Attempt source: %q %d %v", boot, ns, err)
	}
	if clock.identityCalls != 1 {
		t.Errorf("transactions reread boot identity: calls=%d", clock.identityCalls)
	}
}

func TestAttemptLeaseBootIdentityFailureRefusesOpen(t *testing.T) {
	clock := &identityFailureClock{seatBootTestClock: &seatBootTestClock{fakeClock: &fakeClock{now: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}, boot: "boot", elapsed: time.Hour}, failIdentity: true}
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), StoreOptions{Clock: clock})
	if store != nil {
		store.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "boot identity") {
		t.Fatalf("OpenStore must report boot identity failure: %v", err)
	}
}

func TestAttemptLeaseCancelSettlementWithoutBootClock(t *testing.T) {
	f := newAttemptClockFixture(t)
	// Upgrade-shape fixture: a cancellation already committed before the
	// elapsed source became unavailable must still settle at its wall cap.
	if _, err := f.store.db.ExecContext(t.Context(), `UPDATE jobs SET outcome='canceled',cancel_settle_by_ns=? WHERE job_id=?`, f.clock.Now().Add(CancelSettlementTimeout).UnixNano(), f.job.JobID); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(CancelSettlementTimeout)
	f.store.clock = failedSeatClock{f.clock}
	f.reconcile(t, 1)
}

// Enumerate the constructors, including the AI agent/L3 door, so a new door
// cannot accidentally obtain lease authority through an unpinned wall clock.
func TestAttemptLeaseReadBoundaryClockEntryPoints(t *testing.T) {
	expected := map[string]bool{
		"withReadSnapshot": false, "withAgentReadSnapshot": false,
		"beginWriteTransaction": false, "beginSettlementWriteTransaction": false,
		"NodeGenerationClosureReady": false,
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var constructs bool
			var pinnedAt token.Pos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				id, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				if id.Name == "withAttemptClock" {
					pinnedAt = call.Pos()
				}
				if id.Name == "newDatabaseReads" && fn.Name.Name != "transactionReads" {
					constructs = true
					if pinnedAt == token.NoPos || pinnedAt > call.Pos() {
						t.Errorf("%s: %s constructs reads without pinning the Attempt clock", name, fn.Name.Name)
					}
				}
				return true
			})
			if _, ok := expected[fn.Name.Name]; ok {
				expected[fn.Name.Name] = true
				if pinnedAt == token.NoPos {
					t.Errorf("%s: %s does not pin the Attempt clock", name, fn.Name.Name)
				}
			} else if constructs {
				t.Errorf("unreviewed read constructor: %s:%s", name, fn.Name.Name)
			}
		}
	}
	for name, found := range expected {
		if !found {
			t.Errorf("entry point disappeared without updating the clock audit: %s", name)
		}
	}
}
