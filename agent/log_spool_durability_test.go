package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/l1"
)

// TestTheLogSpoolCarriesTheDurabilityPragmas: the spool deletes rows L1
// acknowledged and never re-sends them, so a commit it reported must survive
// power loss. On darwin that takes F_FULLFSYNC for commits and checkpoints
// (#599); elsewhere nothing is added. Both handles carry the pragmas; only the
// FULL handle's commits sync, and output appends use the NORMAL one.
func TestTheLogSpoolCarriesTheDurabilityPragmas(t *testing.T) {
	durable.EnableSQLiteFullFsyncForTests()
	t.Cleanup(durable.DisableSQLiteFullFsyncForTests)
	want := 0
	if runtime.GOOS == "darwin" {
		want = 1
	}
	spool, err := openLogSpool(t.TempDir(), "node-durability", 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	for _, handle := range []struct {
		name        string
		db          *sql.DB
		synchronous int
	}{
		{name: "FULL handle", db: spool.db, synchronous: 2},
		{name: "append handle", db: spool.appendDB, synchronous: 1},
	} {
		for pragma, expected := range map[string]int{
			"fullfsync": want, "checkpoint_fullfsync": want, "synchronous": handle.synchronous,
		} {
			var value int
			if err := handle.db.QueryRowContext(t.Context(), "PRAGMA "+pragma).Scan(&value); err != nil {
				t.Fatalf("%s: read PRAGMA %s: %v", handle.name, pragma, err)
			}
			if value != expected {
				t.Fatalf("%s: PRAGMA %s = %d on %s, want %d", handle.name, pragma, value, runtime.GOOS, expected)
			}
		}
	}
}

// TestOutputAppendsDoNotPayAFullSyncEach: on the production DSN a full sync
// costs about 4 ms on darwin, so 500 appends each paying one would take about
// 2 s. They go through the NORMAL handle and must take well under that.
func TestOutputAppendsDoNotPayAFullSyncEach(t *testing.T) {
	durable.EnableSQLiteFullFsyncForTests()
	t.Cleanup(durable.DisableSQLiteFullFsyncForTests)
	spool, err := openLogSpool(t.TempDir(), "node-append-cost", 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	claim := spoolTestClaim("append-cost")
	if err := spool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	const appends = 500
	started := time.Now()
	for sequence := range appends {
		event := spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, uint64(sequence), fmt.Sprintf("line %d\n", sequence))
		if err := spool.append(t.Context(), event); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(started)
	t.Logf("%d output appends on the production DSN took %s", appends, elapsed)
	if runtime.GOOS != "darwin" {
		t.Skip("the full-sync cost this bounds is darwin's F_FULLFSYNC; elsewhere fsync is cheap and the timing proves nothing")
	}
	if elapsed >= time.Second {
		t.Fatalf("%d output appends took %s; each is paying a full sync", appends, elapsed)
	}
}

// TestEveryUploadBatchIsCoveredByAFullCommitBeforeItIsSent: output appends do
// not sync, so before a batch leaves the agent a FULL commit must cover it --
// L1 never holds an event the spool could lose (#599). Both paths that send
// spool events to L1, the live sink and evidence recovery, are checked, and
// the barrier costs one commit per batch, not one per event.
func TestEveryUploadBatchIsCoveredByAFullCommitBeforeItIsSent(t *testing.T) {
	for _, path := range []string{"live sink", "evidence recovery"} {
		t.Run(path, func(t *testing.T) {
			const batchSize, events = 10, 30
			var outbox *evidenceOutbox
			var requests, uncovered atomic.Int64
			handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if !strings.HasSuffix(request.URL.Path, "/logs") {
					http.NotFound(w, request)
					return
				}
				var appendRequest l1.AppendLogsRequest
				if err := json.NewDecoder(request.Body).Decode(&appendRequest); err != nil {
					t.Error(err)
					return
				}
				// This request is batch n; n barriers must already have committed.
				if sent := requests.Add(1); outbox.spool.durabilityBarriers.Load() < sent {
					uncovered.Add(1)
				}
				acknowledged := make(map[contract.LogStream]uint64)
				for _, event := range appendRequest.Events {
					acknowledged[event.Stream] = eventEndSequence(event)
				}
				_ = json.NewEncoder(w).Encode(l1.AppendLogsResponse{Acknowledged: acknowledged})
			})
			client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
			defer stopServer()
			defer client.Close()
			var err error
			outbox, err = newEvidenceOutbox(t.TempDir(), "barrier-node", 1<<20, systemClock{}, batchSize, time.Hour, time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			defer outbox.Close()
			claim := spoolTestClaim("barrier-attempt")
			if err := outbox.ensureAttempt(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			for sequence := range events {
				if err := outbox.spool.append(t.Context(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, uint64(sequence), "line")); err != nil {
					t.Fatal(err)
				}
			}
			if barriers := outbox.spool.durabilityBarriers.Load(); barriers != 0 {
				t.Fatalf("appending alone committed %d barriers; appends must not pay a full sync", barriers)
			}

			switch path {
			case "live sink":
				sink, err := outbox.newLogSink(t.Context(), client, claim)
				if err != nil {
					t.Fatal(err)
				}
				defer sink.Close()
				if err := sink.uploadAvailableContext(t.Context(), true); err != nil {
					t.Fatal(err)
				}
			case "evidence recovery":
				attempts, err := outbox.spool.pendingAttempts(t.Context())
				if err != nil || len(attempts) != 1 {
					t.Fatalf("pending attempts = %#v, %v", attempts, err)
				}
				if err := outbox.recoverLogs(t.Context(), client, attempts[0]); err != nil {
					t.Fatal(err)
				}
			}

			if sent := requests.Load(); sent != events/batchSize {
				t.Fatalf("%d upload requests, want %d", sent, events/batchSize)
			}
			if count := uncovered.Load(); count != 0 {
				t.Fatalf("%d upload batches left the agent before a FULL commit covered them", count)
			}
			if barriers := outbox.spool.durabilityBarriers.Load(); barriers != events/batchSize {
				t.Fatalf("%d barrier commits for %d batches of %d events; want one per batch", barriers, events/batchSize, batchSize)
			}
			var commits int64
			if err := outbox.spool.db.QueryRowContext(t.Context(), "SELECT commits FROM spool_durability_barrier WHERE id=1").Scan(&commits); err != nil || commits != events/batchSize {
				t.Fatalf("barrier row = %d, %v; want %d commits on the FULL handle", commits, err, events/batchSize)
			}
			// A batch an earlier barrier covered -- a retry -- pays nothing more.
			if err := outbox.spool.append(t.Context(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, events, "tail")); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := outbox.spool.pending(t.Context(), claim.Lease.AttemptID, batchSize); err != nil {
					t.Fatal(err)
				}
			}
			if barriers := outbox.spool.durabilityBarriers.Load(); barriers != events/batchSize+1 {
				t.Fatalf("re-reading a covered batch committed another barrier: %d, want %d", barriers, events/batchSize+1)
			}
		})
	}
}
