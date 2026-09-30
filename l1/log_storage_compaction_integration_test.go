package l1

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func assertNoJobLogJSONLTable(t *testing.T, store *Store) {
	t.Helper()
	var tables int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name='job_log_jsonl'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatal("job_log_jsonl still exists")
	}
}

// #52 S3: job_log_jsonl kept one empty row per job and nothing read it; the
// JSONL export is derived from log_events. A new database never has the
// table, creating jobs writes nothing there, and an upgraded database drops
// it and its rows on open without disturbing the jobs or their export.
func TestJobLogJSONLTableIsRetired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jsonl.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertNoJobLogJSONLTable(t, store)
	job, err := submitDirect(store, validJobSpec("jsonl-retired", nil))
	if err != nil {
		t.Fatal(err)
	}
	insertDetachedLogRows(t, store, job.JobID, "jsonl-attempt", contract.AttemptRunning, "a", "b")
	assertNoJobLogJSONLTable(t, store)
	want, err := store.RawJobLogJSONL(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// The database as an earlier L1 left it: the table and one row per job.
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Exec(`CREATE TABLE job_log_jsonl (
		job_id TEXT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE CASCADE,
		jsonl BLOB NOT NULL);
		INSERT INTO job_log_jsonl(job_id, jsonl) SELECT job_id, X'' FROM jobs;`)
	closeErr := database.Close()
	if err != nil || closeErr != nil {
		t.Fatal(errors.Join(err, closeErr))
	}

	store, err = OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	assertNoJobLogJSONLTable(t, store)
	if _, err := store.GetJob(context.Background(), job.JobID); err != nil {
		t.Fatalf("job after retiring job_log_jsonl: %v", err)
	}
	got, err := store.RawJobLogJSONL(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("JSONL export after retiring job_log_jsonl = %q, want %q", got, want)
	}
	if _, err := submitDirect(store, validJobSpec("jsonl-retired-after-upgrade", nil)); err != nil {
		t.Fatalf("submit after retiring job_log_jsonl: %v", err)
	}
	assertNoJobLogJSONLTable(t, store)
}

// storedDocuments returns each row's event_json and payload, in order.
func storedDocuments(t *testing.T, store *Store, jobID string) (documents, payloads [][]byte) {
	t.Helper()
	rows, err := store.db.Query(`SELECT event_json, bytes FROM log_events WHERE job_id=? ORDER BY ordinal`, jobID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var document, payload []byte
		if err := rows.Scan(&document, &payload); err != nil {
			t.Fatal(err)
		}
		documents = append(documents, document)
		payloads = append(payloads, payload)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return documents, payloads
}

// legacyDocument is the event_json an L1 before #52 stored: the whole
// canonical event, payload included.
func legacyDocument(t *testing.T, event contract.LogEvent) []byte {
	t.Helper()
	event.Timestamp = event.Timestamp.UTC().Round(0)
	document, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return document
}

func compactAllLogEventDocuments(t *testing.T, store *Store) int64 {
	t.Helper()
	var total int64
	for pass := 0; ; pass++ {
		if pass > 10000 {
			t.Fatal("log event document migration never finished")
		}
		compaction, err := store.CompactLogEventDocuments(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		total += compaction.Rewritten
		if compaction.Done {
			return total
		}
	}
}

// #52 S3: L1 stored each log payload twice, raw and again Base64-encoded in
// event_json. It is now stored once. Every read -- log pages, the JSONL
// export, replay comparison -- returns byte for byte what it did before, for
// rows written either way and for rows the migration rewrote.
func TestLogEventDocumentsStoreThePayloadOnceAndReadBackUnchanged(t *testing.T) {
	// The background loop would run the migration itself; this test counts it.
	h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{}, map[string]NodePolicy{
		"worker": DefaultNodePolicy("worker"),
	}, true, time.Hour)
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "worker")
	job := h.submit(client, "log-document-compaction", nil)
	claim := claimOneshot(t, h, agent, node, job.JobID)
	attemptID := claim.Lease.AttemptID
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, attemptID)

	binary := logEvent(attemptID, contract.LogStdout, 0, []byte{0xff, 0xfe, 0x00, 0x80, '"', '\\', 0xc3, 0x28})
	binary.Timestamp = time.Date(2026, 8, 9, 12, 0, 0, 123456789, time.FixedZone("east", 8*3600))
	quoted := logEvent(attemptID, contract.LogStdout, 1, []byte(`{"bytes":"not a key","gap":null}`+"\n"))
	single := logEvent(attemptID, contract.LogStdout, 2, []byte{0})
	gap := contract.LogEvent{AttemptID: attemptID, Stream: contract.LogStderr, Sequence: 0,
		Timestamp: time.Date(2026, 8, 9, 10, 0, 5, 0, time.UTC),
		Gap:       &contract.LogGap{ThroughSequence: 2, LostEventCount: 3, LostByteCount: 42, Reason: contract.LogGapSpoolEviction}}
	large := logEvent(attemptID, contract.LogStderr, 3, bytes.Repeat([]byte("large payload\x01"), 4096))
	events := []contract.LogEvent{binary, quoted, single, gap, large}
	appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, events)
	assertUsageCountersMatchLogEvents(t, h.store)

	var wantJSONL []byte
	for _, event := range events {
		wantJSONL = append(append(wantJSONL, legacyDocument(t, event)...), '\n')
	}
	read := func(label string) ([]byte, []byte) {
		t.Helper()
		status, _, body := h.do(client, http.MethodGet, "/v1/jobs/"+job.JobID+"/logs?limit=1000", nil)
		if status != http.StatusOK {
			t.Fatalf("%s: get logs status = %d body=%s", label, status, body)
		}
		jsonl, err := h.store.RawJobLogJSONL(context.Background(), job.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(jsonl, wantJSONL) {
			t.Fatalf("%s: JSONL export =\n%s\nwant\n%s", label, jsonl, wantJSONL)
		}
		return body, jsonl
	}
	replay := func(label string) {
		t.Helper()
		appendLogsExpectingOK(t, h, agent, path, claim.Lease.FencingToken, events)
		changed := binary
		changed.Bytes = []byte{0xff, 0xfe, 0x00, 0x81}
		status, _, body := h.do(agent, http.MethodPost, path, AppendLogsRequest{FencingToken: claim.Lease.FencingToken,
			Events: []contract.LogEvent{changed}})
		if status != http.StatusConflict || !bytes.Contains(body, []byte(contract.ErrorIdempotencyConflict)) {
			t.Fatalf("%s: changed replay status = %d body=%s, want idempotency_conflict", label, status, body)
		}
	}

	// Written compact: no row carries its payload in the document.
	documents, payloads := storedDocuments(t, h.store, job.JobID)
	var documentBytes int
	for index, document := range documents {
		if bytes.Contains(document, []byte(`"bytes":`)) {
			t.Fatalf("stored document %d still carries its payload: %s", index, document)
		}
		if !bytes.Equal(logEventDocument(document, payloads[index]), legacyDocument(t, events[index])) {
			t.Fatalf("document %d does not rebuild the wire event", index)
		}
		documentBytes += len(document)
	}
	if documentBytes > 1024 {
		t.Fatalf("stored documents hold %d bytes, want only the non-payload fields", documentBytes)
	}
	compactPage, compactJSONL := read("compact")
	var page LogPage
	if err := json.Unmarshal(compactPage, &page); err != nil {
		t.Fatal(err)
	}
	for index, event := range page.Events {
		if !bytes.Equal(legacyDocument(t, event), legacyDocument(t, events[index])) {
			t.Fatalf("page event %d = %#v, want %#v", index, event, events[index])
		}
	}
	replay("compact")

	// The same rows as an L1 before #52 wrote them read back identically.
	for _, event := range events {
		if _, err := h.store.db.Exec(`UPDATE log_events SET event_json=? WHERE job_id=? AND stream=? AND sequence=?`,
			legacyDocument(t, event), job.JobID, event.Stream, event.Sequence); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.store.db.Exec(`DELETE FROM l1_data_migrations WHERE name=?`, logEventDocumentMigration); err != nil {
		t.Fatal(err)
	}
	legacyPage, legacyJSONL := read("legacy")
	if !bytes.Equal(legacyPage, compactPage) || !bytes.Equal(legacyJSONL, compactJSONL) {
		t.Fatalf("legacy rows read differently:\n%s\nvs compact\n%s", legacyPage, compactPage)
	}
	replay("legacy")

	// The migration rewrites them to the compact document, and the reads
	// still do not change.
	if rewritten := compactAllLogEventDocuments(t, h.store); rewritten != 4 {
		t.Fatalf("migration rewrote %d rows, want the 4 raw events (the gap has no payload)", rewritten)
	}
	migrated, _ := storedDocuments(t, h.store, job.JobID)
	for index := range migrated {
		if !bytes.Equal(migrated[index], documents[index]) {
			t.Fatalf("migrated document %d = %s, want %s", index, migrated[index], documents[index])
		}
	}
	migratedPage, migratedJSONL := read("migrated")
	if !bytes.Equal(migratedPage, compactPage) || !bytes.Equal(migratedJSONL, compactJSONL) {
		t.Fatalf("migrated rows read differently:\n%s\nvs compact\n%s", migratedPage, compactPage)
	}
	replay("migrated")
	assertUsageCountersMatchLogEvents(t, h.store)
}

// insertLegacyLogEvents writes count stdout events for one attempt exactly as
// an L1 before #52 stored them, the payload also inside event_json.
func insertLegacyLogEvents(t *testing.T, store *Store, jobID, attemptID string, first, count, payloadBytes int) {
	t.Helper()
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for sequence := first; sequence < first+count; sequence++ {
		payload := bytes.Repeat([]byte{byte(sequence), 0xff, 'x'}, payloadBytes/3+1)[:payloadBytes]
		event := logEvent(attemptID, contract.LogStdout, uint64(sequence), payload)
		event.Timestamp = time.Unix(1_700_000_000, int64(sequence)).UTC()
		if _, err := tx.Exec(`INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
			VALUES(?, ?, 'stdout', ?, ?, ?, ?, ?)`, jobID, attemptID, sequence, sequence, event.Timestamp.UnixNano(),
			payload, legacyDocument(t, event)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func legacyDocumentCount(t *testing.T, store *Store) int {
	t.Helper()
	var legacy int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM log_events WHERE instr(event_json, '"bytes":') > 0`).Scan(&legacy); err != nil {
		t.Fatal(err)
	}
	return legacy
}

func usedPages(t *testing.T, store *Store) int64 {
	t.Helper()
	var pages, free int64
	if err := store.db.QueryRow(`PRAGMA page_count`).Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`PRAGMA freelist_count`).Scan(&free); err != nil {
		t.Fatal(err)
	}
	return pages - free
}

// #52 S3: an upgraded database is migrated in bounded batches from the
// reconcile loop, each batch atomic with its resume point, so a restart
// between batches resumes where it stopped, and the stored log data roughly
// halves. A new database has nothing to migrate and never runs it.
func TestLogEventDocumentMigrationIsBoundedAndResumable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migration.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var markers int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM l1_data_migrations WHERE name=?`, logEventDocumentMigration).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	if markers != 1 {
		t.Fatalf("new database migration markers = %d, want 1", markers)
	}
	job, err := submitDirect(store, validJobSpec("document-migration", nil))
	if err != nil {
		t.Fatal(err)
	}
	insertDetachedLogRows(t, store, job.JobID, "legacy-attempt", contract.AttemptRunning)
	const legacyRows = 2*logEventDocumentMigrationRows + 100
	const payloadBytes = 3000
	insertLegacyLogEvents(t, store, job.JobID, "legacy-attempt", 0, legacyRows, payloadBytes)
	// A row whose document is not the shape L1 wrote is left alone and still read.
	insertDetachedLogRows(t, store, job.JobID, "odd-attempt", contract.AttemptRunning, "odd")
	if _, err := store.db.Exec(`DELETE FROM l1_data_migrations WHERE name=?`, logEventDocumentMigration); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	beforePages := usedPages(t, store)
	wantJSONL, err := store.RawJobLogJSONL(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	wantPage, err := store.GetJobLogs(context.Background(), job.JobID, "", MaxLogPageLimit)
	if err != nil {
		t.Fatal(err)
	}

	first, err := store.CompactLogEventDocuments(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Rewritten != logEventDocumentMigrationRows || first.Done {
		t.Fatalf("first batch = %+v, want %d rows and not done", first, logEventDocumentMigrationRows)
	}
	if got := legacyDocumentCount(t, store); got != legacyRows-logEventDocumentMigrationRows {
		t.Fatalf("legacy documents after one batch = %d, want %d", got, legacyRows-logEventDocumentMigrationRows)
	}
	// Rows written after the migration started are compact and past its
	// ceiling; it never reads them.
	var ceiling int64
	if err := store.db.QueryRow(`SELECT ceiling_ordinal FROM l1_data_migration_cursors WHERE name=?`, logEventDocumentMigration).Scan(&ceiling); err != nil {
		t.Fatal(err)
	}
	var maxOrdinal int64
	if err := store.db.QueryRow(`SELECT MAX(ordinal) FROM log_events`).Scan(&maxOrdinal); err != nil {
		t.Fatal(err)
	}
	if ceiling != maxOrdinal {
		t.Fatalf("migration ceiling = %d, want %d", ceiling, maxOrdinal)
	}

	// A restart between batches: the committed batch and its resume point
	// survive, and the migration continues from there.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if got := legacyDocumentCount(t, store); got != legacyRows-logEventDocumentMigrationRows {
		t.Fatalf("legacy documents after restart = %d, want %d", got, legacyRows-logEventDocumentMigrationRows)
	}
	// A crash inside a batch rolls the whole batch back with its resume
	// point; replaying it from the last committed point rewrites nothing
	// twice, because a compact document is never rewritten.
	if _, err := store.db.Exec(`UPDATE l1_data_migration_cursors SET through_ordinal=0 WHERE name=?`, logEventDocumentMigration); err != nil {
		t.Fatal(err)
	}
	rest := compactAllLogEventDocuments(t, store)
	if rest != legacyRows-logEventDocumentMigrationRows {
		t.Fatalf("remaining batches rewrote %d rows, want %d", rest, legacyRows-logEventDocumentMigrationRows)
	}
	if got := legacyDocumentCount(t, store); got != 0 {
		t.Fatalf("legacy documents after migration = %d, want 0", got)
	}
	var cursors int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM l1_data_migration_cursors`).Scan(&cursors); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM l1_data_migrations WHERE name=?`, logEventDocumentMigration).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	if cursors != 0 || markers != 1 {
		t.Fatalf("after migration cursors=%d markers=%d, want 0 and 1", cursors, markers)
	}
	if again, err := store.CompactLogEventDocuments(context.Background()); err != nil || again.Rewritten != 0 || !again.Done {
		t.Fatalf("pass after migration = %+v, %v", again, err)
	}

	gotJSONL, err := store.RawJobLogJSONL(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotJSONL, wantJSONL) {
		t.Fatal("JSONL export changed across the migration")
	}
	gotPage, err := store.GetJobLogs(context.Background(), job.JobID, "", MaxLogPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	wantEncoded, _ := json.Marshal(wantPage)
	gotEncoded, _ := json.Marshal(gotPage)
	if !bytes.Equal(gotEncoded, wantEncoded) {
		t.Fatal("log page changed across the migration")
	}
	assertUsageCountersMatchLogEvents(t, store)

	if _, err := store.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	afterPages := usedPages(t, store)
	t.Logf("log-heavy fixture (%d x %d-byte events): used pages %d before, %d after (%.0f%%)",
		legacyRows, payloadBytes, beforePages, afterPages, 100*float64(afterPages)/float64(beforePages))
	if float64(afterPages) > 0.6*float64(beforePages) {
		t.Fatalf("used pages %d -> %d, want the log data roughly halved", beforePages, afterPages)
	}
}

func TestLogEventDocumentReconstruction(t *testing.T) {
	for _, event := range []contract.LogEvent{
		{AttemptID: "a", Stream: contract.LogStdout, Sequence: 7, Timestamp: time.Unix(1, 2).UTC(), Bytes: []byte{0xff, 0, '"'}},
		{AttemptID: `a"b\`, Stream: contract.LogStderr, Sequence: 0, Timestamp: time.Unix(0, 1).UTC(), Bytes: []byte("x")},
	} {
		full, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		stored, err := storedLogEventDocument(event)
		if err != nil {
			t.Fatal(err)
		}
		if got := logEventDocument(stored, event.Bytes); !bytes.Equal(got, full) {
			t.Fatalf("rebuilt %s, want %s", got, full)
		}
		if got := logEventDocument(full, event.Bytes); !bytes.Equal(got, full) {
			t.Fatalf("legacy document read as %s, want %s", got, full)
		}
		compact, ok := compactLegacyLogEventDocument(full, event.Bytes)
		if !ok || !bytes.Equal(compact, stored) {
			t.Fatalf("compacted %s (%t), want %s", compact, ok, stored)
		}
		if _, ok := compactLegacyLogEventDocument(stored, event.Bytes); ok {
			t.Fatal("a compact document was compacted again")
		}
		if _, ok := compactLegacyLogEventDocument(full, []byte("other")); ok {
			t.Fatal("a document was compacted against another payload")
		}
	}
	if got := logEventDocument([]byte(`{}`), []byte("p")); string(got) != `{"bytes":"cA=="}` {
		t.Fatalf("empty document rebuilt as %s", got)
	}
}

// The reconcile loop runs one migration batch per tick, logs what it
// rewrote, retries a failed batch on the next tick, and stops asking once
// the migration reports done.
func TestServerRunsLogEventDocumentMigrationUntilDone(t *testing.T) {
	var calls int
	var logged []string
	answers := []struct {
		compaction LogEventCompaction
		err        error
	}{
		{LogEventCompaction{Rewritten: 512}, nil},
		{LogEventCompaction{}, errors.New("database is locked")},
		{LogEventCompaction{Rewritten: 7, Done: true}, nil},
	}
	server := &Server{
		compactLogEvents: func(context.Context) (LogEventCompaction, error) {
			answer := answers[calls]
			calls++
			return answer.compaction, answer.err
		},
		logf: func(format string, args ...any) { logged = append(logged, fmt.Sprintf(format, args...)) },
	}
	for tick := 0; tick < 5; tick++ {
		server.compactLogEventDocuments(context.Background())
	}
	if calls != 3 {
		t.Fatalf("migration batches = %d, want 3 (stop once done)", calls)
	}
	want := []string{
		"event=l1_log_event_documents_compacted rows=512 done=false",
		"event=l1_log_event_document_migration_failed action=retry_next_tick error=database is locked",
		"event=l1_log_event_documents_compacted rows=7 done=true",
	}
	if fmt.Sprint(logged) != fmt.Sprint(want) {
		t.Fatalf("logged %q, want %q", logged, want)
	}
}
