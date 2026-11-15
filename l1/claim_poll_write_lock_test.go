package l1

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// claimPollFixture is one L1 with one claimed one-shot Attempt and pollers
// extra registered Nodes, so every further claim poll finds nothing (#1686).
// A fake clock never measures sleep; the store's real clock does, so its
// polls also show how often a sleep-anchor update sends one to the write path.
type claimPollFixture struct {
	store *Store
	log   *writeLockLog
	nodes []Node
	job   Job
	claim *Claim
}

func newClaimPollFixture(tb testing.TB, pollers int, fake bool) *claimPollFixture {
	tb.Helper()
	f := &claimPollFixture{log: &writeLockLog{}}
	options := StoreOptions{LeaseDuration: time.Minute, NodeStaleAfter: 12 * time.Hour, NodeDeadAfter: 24 * time.Hour}
	observed := time.Now()
	if fake {
		wall := &fakeClock{now: time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)}
		options.Clock = &seatBootTestClock{fakeClock: wall, boot: "l1-boot", elapsed: time.Hour}
		observed = wall.Now()
	}
	store, err := openTestStore(tb, filepath.Join(tb.TempDir(), "l1.sqlite"), options)
	if err != nil {
		tb.Fatal(err)
	}
	f.store = store
	tb.Cleanup(func() {
		if err := store.Close(); err != nil {
			tb.Error(err)
		}
	})
	for i := range pollers + 1 {
		id := fmt.Sprintf("poll-node-%d", i)
		node, err := store.RegisterNode(context.Background(), fabric.Identity{NodeID: "agent-" + id}, contract.NodeRegistration{
			NodeID: id, BootSessionID: "boot-" + id, RootInstanceID: "root-" + id,
			OS: "linux", Architecture: "arm64", AgentVersion: "test",
			Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1, CapabilityObservedAt: observed, MissingCapabilities: []string{},
		}, DefaultNodePolicy(), true)
		if err != nil {
			tb.Fatal(err)
		}
		f.nodes = append(f.nodes, node)
	}
	f.job, _, err = store.CreateJob(context.Background(), validJobSpec("claim-poll", nil))
	if err != nil {
		tb.Fatal(err)
	}
	f.claim, err = store.ClaimJob(context.Background(), "agent-poll-node-0", f.nodes[0].NodeID, f.nodes[0].BootSessionID, contract.JobClassOneShot)
	if err != nil || f.claim == nil {
		tb.Fatalf("claim=%+v err=%v", f.claim, err)
	}
	// Log every write transaction from here on, so the lines count them.
	store.setLogf(f.log.logf)
	store.writeLockLogThreshold = time.Nanosecond
	return f
}

func (f *claimPollFixture) poll(node Node) (ClaimPollResponse, error) {
	return f.store.ClaimJobPoll(context.Background(), "agent-"+node.NodeID, node.NodeID, node.BootSessionID, contract.JobClassOneShot)
}

func (f *claimPollFixture) renew() error {
	_, err := f.store.RenewLease(context.Background(), "agent-poll-node-0", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken)
	return err
}

// #1686: a claim poll that finds nothing to claim must not take SQLite's
// write lock, so it cannot delay a lease renewal.
func TestEmptyClaimPollTakesNoWriteLock(t *testing.T) {
	f := newClaimPollFixture(t, 2, true)
	for _, node := range f.nodes {
		reply, err := f.poll(node)
		if err != nil || reply.Claim != nil {
			t.Fatalf("poll %s: claim=%+v err=%v", node.NodeID, reply.Claim, err)
		}
		if claim, err := f.store.ClaimJob(context.Background(), "agent-"+node.NodeID, node.NodeID, node.BootSessionID, contract.JobClassService); err != nil || claim != nil {
			t.Fatalf("service claim %s: claim=%+v err=%v", node.NodeID, claim, err)
		}
	}
	if writes := f.log.slow("l1.(*Store).claimJob"); len(writes) != 0 {
		t.Fatalf("empty claim polls took the write lock %d times: %q", len(writes), writes)
	}
}

// BenchmarkEmptyClaimPollRenewal measures how long lease renewals take while
// many Nodes poll for work that is not there (#1686). Each Node polls one
// class every 5 ms, the rate of the #1654 test, so both claim paths serve the
// same poll load. Each op is one renewal.
//
//	go test ./l1 -run '^$' -bench EmptyClaimPollRenewal -benchtime 300x
func BenchmarkEmptyClaimPollRenewal(b *testing.B) {
	for _, clock := range []string{"fake", "real"} {
		for _, pollers := range []int{0, 16} {
			b.Run(fmt.Sprintf("clock=%s/pollers=%d", clock, pollers), func(b *testing.B) {
				benchmarkEmptyClaimPollRenewal(b, pollers, clock == "fake")
			})
		}
	}
}

func benchmarkEmptyClaimPollRenewal(b *testing.B, pollers int, fake bool) {
	const interval = 5 * time.Millisecond
	f := newClaimPollFixture(b, pollers, fake)
	stop := make(chan struct{})
	var polls atomic.Int64
	var wg sync.WaitGroup
	for _, node := range f.nodes[1:] {
		wg.Go(func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
				if _, err := f.poll(node); err != nil {
					b.Error(err)
					return
				}
				polls.Add(1)
			}
		})
	}
	latencies := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for range b.N {
		started := time.Now()
		if err := f.renew(); err != nil {
			b.Fatal(err)
		}
		latencies = append(latencies, time.Since(started))
		time.Sleep(time.Millisecond)
	}
	b.StopTimer()
	close(stop)
	wg.Wait()

	slices.Sort(latencies)
	at := func(q float64) float64 {
		return float64(latencies[int(q*float64(len(latencies)-1))].Microseconds()) / 1000
	}
	var renewWaits int
	for _, line := range f.log.slow("l1.(*Store).RenewLease") {
		if !strings.Contains(line, " wait_ms=0 ") {
			renewWaits++
		}
	}
	b.ReportMetric(float64(polls.Load())/float64(b.N), "polls/op")
	b.ReportMetric(float64(len(f.log.slow("l1.(*Store).claimJob")))/float64(b.N), "claim-write-locks/op")
	b.ReportMetric(float64(renewWaits)/float64(b.N), "renew-waits≥1ms/op")
	b.ReportMetric(at(0.5), "renew-p50-ms")
	b.ReportMetric(at(0.99), "renew-p99-ms")
	b.ReportMetric(at(1), "renew-max-ms")
}
