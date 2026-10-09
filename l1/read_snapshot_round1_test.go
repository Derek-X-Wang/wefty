package l1

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestReadSnapshotTargetOverrunReturnsAnswer(t *testing.T) {
	s, _ := snapshotStore(t)
	var logs bytes.Buffer
	s.logf = func(format string, args ...any) { fmt.Fprintf(&logs, format, args...) }
	answer := ""
	err := s.withReadSnapshot(t.Context(), nil, func(ctx context.Context, reads readModel) error {
		time.Sleep(125 * time.Millisecond)
		if _, err := reads.node(ctx, "absent"); !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		answer = "correct"
		return nil
	})
	if err != nil || answer != "correct" {
		t.Fatalf("slow correct answer=%q err=%v", answer, err)
	}
	counter, ok := any(s).(interface{ readSnapshotOverrunCount() uint64 })
	if !ok || counter.readSnapshotOverrunCount() != 1 || !strings.Contains(logs.String(), "read_snapshot_target_overrun") {
		t.Fatalf("overrun was not counted and logged: %s", logs.String())
	}
}

func assertSnapshotUnavailable(t *testing.T, err error, reason string) {
	t.Helper()
	api := apiErrorFromDecision(err)
	if api == nil || api.Code != contract.ErrorUnavailable || !api.Retryable || api.Details["reason"] != reason {
		t.Fatalf("snapshot expiry must be typed retryable unavailable (%s): %#v err=%v", reason, api, err)
	}
	response := httptest.NewRecorder()
	writeError(response, err)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestReadSnapshotHardExpiryRefusesPartialAnswer(t *testing.T) {
	s, _ := snapshotStore(t)
	started := time.Now()
	var q queryer
	err := s.withReadSnapshot(t.Context(), nil, func(ctx context.Context, reads readModel) error {
		q = reads.(*databaseReads).q
		<-ctx.Done()
		// A projector that swallows its last error must still not return 200.
		return nil
	})
	assertSnapshotUnavailable(t, err, "read_snapshot_expired")
	elapsed := time.Since(started)
	if elapsed < 175*time.Millisecond || elapsed > time.Second {
		t.Fatalf("hard expiry=%s; want approximately 200ms", elapsed)
	}
	if err := q.QueryRowContext(t.Context(), "SELECT 1").Scan(new(int)); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("expired transaction retained its SQLite read lock: %v", err)
	}
}

func TestReadSnapshotAdmissionBounded(t *testing.T) {
	s, _ := snapshotStore(t)
	release := make(chan struct{})
	entered := make(chan struct{}, readSnapshotLimit)
	done := make(chan error, readSnapshotLimit)
	for range readSnapshotLimit {
		go func() {
			ctx := t.Context()
			done <- s.withReadSnapshot(ctx, nil, func(context.Context, readModel) error {
				entered <- struct{}{}
				<-release
				return nil
			})
		}()
	}
	for range readSnapshotLimit {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("readers did not enter")
		}
	}
	defer func() {
		close(release)
		for range readSnapshotLimit {
			<-done
		}
	}()
	outer, cancel := context.WithTimeout(t.Context(), 600*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := s.withReadSnapshot(outer, nil, func(context.Context, readModel) error { t.Error("admitted without capacity"); return nil })
	assertSnapshotUnavailable(t, err, "read_snapshot_admission_expired")
	if time.Since(start) > 300*time.Millisecond {
		t.Fatalf("admission held until client timeout: %s", time.Since(start))
	}
}

// Keep behaviour probes compilable on the original code for exact red checks.
func decisionReadProbe(w *writeTransaction) readModel {
	if fresh, ok := any(w).(interface{ decisionReads() readModel }); ok {
		return fresh.decisionReads()
	}
	return any(w).(readModel)
}
func TestWriteDecisionFreshMemoAndPinnedClock(t *testing.T) {
	s, clock := snapshotStore(t)
	w, err := s.beginWriteTransaction(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.rollback()
	first := decisionReadProbe(w)
	if _, err := first.nodeState(t.Context(), "inside"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if _, err := w.tx.ExecContext(t.Context(), "INSERT INTO nodes(node_id, identity_node_id, boot_session_id, os, architecture, agent_version, capabilities_json, missing_capabilities_json, state, max_oneshot_slots, max_service_slots, last_heartbeat_ns) VALUES('inside','fabric-inside','boot','linux','amd64','test','{}','[]','alive',1,1,0)"); err != nil {
		t.Fatal(err)
	}
	clock.at.Add(int64(time.Hour))
	second := decisionReadProbe(w)
	state, err := second.nodeState(t.Context(), "inside")
	if err != nil || state != contract.NodeAlive {
		t.Fatalf("next decision hid same-write insertion: state=%s err=%v", state, err)
	}
	if first.now().IsZero() || second.now() != first.now() || second.now().UnixNano() == clock.at.Load() {
		t.Fatal("write clock was zero or resampled")
	}
}
func TestTransactionReadsRejectsZeroClock(t *testing.T) {
	s, _ := snapshotStore(t)
	fn := reflect.ValueOf(transactionReads)
	arguments := []reflect.Value{reflect.ValueOf(s.db)}
	if fn.Type().NumIn() == 2 {
		arguments = append(arguments, reflect.ValueOf(time.Time{}))
	}
	defer func() {
		if recover() == nil {
			t.Error("zero write clock was silently accepted")
		}
	}()
	fn.Call(arguments)
}

func TestReadSnapshotSeparatePoolCannotBeStarvedByLegacyReads(t *testing.T) {
	s, _ := snapshotStore(t)
	var held []*sql.Conn
	for range s.db.Stats().MaxOpenConnections {
		conn, err := s.db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
	}
	defer func() {
		for _, conn := range held {
			_ = conn.Close()
		}
	}()
	if err := s.withReadSnapshot(t.Context(), nil, func(context.Context, readModel) error { return nil }); err != nil {
		t.Fatalf("legacy pool starved snapshot checkout: %v", err)
	}
}
func TestTransactionReadsCarriesPinnedClock(t *testing.T) {
	s, clock := snapshotStore(t)
	at := clock.Now()
	fn := reflect.ValueOf(transactionReads)
	args := []reflect.Value{reflect.ValueOf(s.db)}
	if fn.Type().NumIn() == 2 {
		args = append(args, reflect.ValueOf(at))
	}
	reads := fn.Call(args)[0].Interface().(readModel)
	if reads.now() != at {
		t.Fatalf("decision clock=%s want=%s", reads.now(), at)
	}
}

type projectionNodeCounter struct {
	queryer
	nodes int
}

func (q *projectionNodeCounter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if strings.Contains(query, "FROM nodes") || strings.Contains(query, "FROM node_tags") {
		q.nodes++
	}
	return q.queryer.QueryContext(ctx, query, args...)
}
func (q *projectionNodeCounter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if strings.Contains(query, "FROM nodes") {
		q.nodes++
	}
	return q.queryer.QueryRowContext(ctx, query, args...)
}
func TestReadSnapshotProjectorMemoizesNodeFacts(t *testing.T) {
	for _, state := range []string{"claimed", "capability-missing"} {
		t.Run(state, func(t *testing.T) {
			h, _, original, _ := jobProjectionFixture(t, state)
			if err := h.store.withReadSnapshot(t.Context(), nil, func(ctx context.Context, reads readModel) error {
				impl := reads.(*databaseReads)
				counter := &projectionNodeCounter{queryer: impl.q}
				impl.q = counter
				job, err := reads.job(ctx, original.JobID)
				if err != nil {
					return err
				}
				if _, err := projectJobWithReads(ctx, reads, job, projectJobStatusPart); err != nil {
					return err
				}
				queries := counter.nodes
				if queries == 0 {
					t.Fatal("fixture did not read node facts")
				}
				job, err = reads.job(ctx, original.JobID)
				if err != nil {
					return err
				}
				if _, err := projectJobWithReads(ctx, reads, job, projectJobStatusPart); err != nil {
					return err
				}
				if counter.nodes != queries {
					t.Fatalf("projector re-read node facts: before=%d after=%d", queries, counter.nodes)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
