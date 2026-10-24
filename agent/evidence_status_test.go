package agent

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestPendingEvidenceReadsLegacySpoolWithoutMigration(t *testing.T) {
	directory := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(directory, spoolFileName("node")))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE spool_attempts (job_id TEXT,attempt_id TEXT,fencing_token TEXT,class TEXT,kind TEXT,created_ns INTEGER,incomplete_json BLOB,result_json BLOB,completion_disposition TEXT);
 CREATE TABLE spool_events (attempt_id TEXT);
 CREATE TABLE spool_removals (job_id TEXT);
 INSERT INTO spool_attempts VALUES ('job','attempt','fence','one-shot','process',0,NULL,NULL,NULL);
 INSERT INTO spool_events VALUES ('attempt');`)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := PendingEvidence(t.Context(), directory, "node")
	if err != nil || pending != 1 {
		t.Fatalf("legacy pending evidence = %d, %v", pending, err)
	}
	if _, err := db.Exec(`DELETE FROM spool_events`); err != nil {
		t.Fatal(err)
	}
	pending, err = PendingEvidence(t.Context(), directory, "node")
	if err != nil || pending != 0 {
		t.Fatalf("legacy drained evidence = %d, %v", pending, err)
	}
	var ownershipColumns int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('spool_attempts') WHERE name='owner_generation'`).Scan(&ownershipColumns); err != nil || ownershipColumns != 0 {
		t.Fatalf("inspection migrated legacy schema: %d, %v", ownershipColumns, err)
	}
}

func TestPendingEvidenceCountsEveryGenerationWithoutGrantingRecovery(t *testing.T) {
	directory := t.TempDir()
	spool := openTestLogSpool(t, directory, "node", 1024)
	defer spool.Close()
	for _, owner := range []string{"one", "two", "three"} {
		spool.generationID = owner
		claim := spoolTestClaim(owner)
		if err := spool.ensureAttempt(t.Context(), claim); err != nil {
			t.Fatal(err)
		}
		if err := spool.append(t.Context(), spoolTestEvent(owner, contract.LogStdout, 0, "pending")); err != nil {
			t.Fatal(err)
		}
	}
	count, err := PendingEvidence(t.Context(), directory, "node")
	if err != nil || count != 3 {
		t.Fatalf("node-wide pending evidence = %d, %v", count, err)
	}
	own, err := spool.pendingAttempts(t.Context())
	if err != nil || len(own) != 1 || own[0].attemptID != "three" {
		t.Fatalf("inspection granted foreign recovery: %+v, %v", own, err)
	}
}
