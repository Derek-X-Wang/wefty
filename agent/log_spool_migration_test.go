package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	_ "modernc.org/sqlite"
)

func TestMigrateCompletionReceiptDispositions(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "receipts.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE spool_completion_receipts (
  attempt_id TEXT PRIMARY KEY,
  disposition TEXT NOT NULL CHECK (disposition IN ('delivered', 'suppressed')),
  reason TEXT NOT NULL,
  observed_ns INTEGER NOT NULL
);
INSERT INTO spool_completion_receipts VALUES('old', 'suppressed', 'service_intent_stop', 1);`); err != nil {
		t.Fatal(err)
	}
	if err := migrateCompletionReceiptDispositions(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	var disposition, reason string
	if err := db.QueryRow(`SELECT disposition, reason FROM spool_completion_receipts WHERE attempt_id='old'`).Scan(&disposition, &reason); err != nil || disposition != "suppressed" || reason != "service_intent_stop" {
		t.Fatalf("migrated old receipt=%q/%q err=%v", disposition, reason, err)
	}
	if _, err := db.Exec(`INSERT INTO spool_completion_receipts(attempt_id, disposition, reason, observed_ns, intent_revision)
VALUES('new', 'withheld', 'intent_authority_unavailable', 2, 7)`); err != nil {
		t.Fatalf("migrated receipt schema rejected withheld disposition: %v", err)
	}
}

func TestLogSpoolJoinsLegacySuppressionToRetainedPayload(t *testing.T) {
	directory := t.TempDir()
	nodeID := "legacy-suppression-node"
	db, err := sql.Open("sqlite", filepath.Join(directory, spoolFileName(nodeID)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE spool_attempts (
  attempt_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, fencing_token TEXT NOT NULL,
  class TEXT NOT NULL, created_ns INTEGER NOT NULL, result_json BLOB, finished_ns INTEGER, incomplete_json BLOB
);
CREATE TABLE spool_completion_receipts (
  attempt_id TEXT PRIMARY KEY,
  disposition TEXT NOT NULL CHECK (disposition IN ('delivered', 'suppressed')),
  reason TEXT NOT NULL, observed_ns INTEGER NOT NULL
);`); err != nil {
		t.Fatal(err)
	}
	exitCode := 7
	payload, err := json.Marshal(durableCompletion{Result: l1.ProcessResult{ExitCode: &exitCode}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO spool_attempts VALUES('attempt','job','fence',?,1,?,2,NULL);
INSERT INTO spool_completion_receipts VALUES('attempt','suppressed','service_intent_stop',3);`, contract.JobClassService, payload); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	spool, err := openLogSpool(directory, nodeID, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	disposition, reason, revision, err := spool.completionDisposition(t.Context(), "attempt")
	if err != nil || disposition != "suppressed" || reason != "service_intent_stop" || revision != 0 {
		t.Fatalf("joined disposition=%q/%q revision=%d err=%v", disposition, reason, revision, err)
	}
	if attempts, err := spool.pendingAttempts(t.Context()); err != nil || len(attempts) != 0 {
		t.Fatalf("legacy suppressed payload became replayable=%+v err=%v", attempts, err)
	}
}

func TestLogSpoolMigratesPreRemovalProofSchema(t *testing.T) {
	directory := t.TempDir()
	nodeID := "migration-node"
	path := filepath.Join(directory, spoolFileName(nodeID))
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	const oldSchema = `
CREATE TABLE spool_removals (
  job_id TEXT PRIMARY KEY,
  removal_generation INTEGER NOT NULL,
  cleanup_fence TEXT NOT NULL,
  root_instance_id TEXT NOT NULL,
  started_ns INTEGER NOT NULL
);
CREATE TABLE runtime_removal_manifests (
  job_id TEXT PRIMARY KEY,
  removal_generation INTEGER NOT NULL,
  cleanup_fence TEXT NOT NULL,
  root_instance_id TEXT NOT NULL,
  manifest_json BLOB NOT NULL,
  runtime_quiescence_json BLOB,
  phase TEXT NOT NULL,
  prepared_ns INTEGER NOT NULL,
  quiesced_ns INTEGER,
  completed_ns INTEGER
);
CREATE TABLE runtime_service_manifests (
  job_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL UNIQUE,
  removal_generation TEXT NOT NULL,
  manifest_json BLOB NOT NULL,
  created_ns INTEGER NOT NULL
);
CREATE TABLE oci_binding_pins (
  job_id TEXT PRIMARY KEY,
  reference TEXT NOT NULL,
  digest TEXT NOT NULL,
  platform_os TEXT NOT NULL,
  platform_architecture TEXT NOT NULL,
  platform_variant TEXT NOT NULL,
  snapshotter TEXT NOT NULL,
  updated_ns INTEGER NOT NULL
);`
	if _, err := db.Exec(oldSchema); err != nil {
		t.Fatal(err)
	}
	removal := testRuntimeRemoval("upgrade-oci")
	manifest := runtimeRemovalManifest{Version: 1, JobID: removal.jobID, RemovalGeneration: removal.generation,
		Attempts: []workloadrunner.RuntimeResourceManifest{testRuntimeResourceManifest(removal.jobID, "attempt")}}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	receipt := workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt, BootSessionID: "boot"}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 28, 2, 0, 0, 0, time.UTC).UnixNano()
	if _, err := db.Exec(`INSERT INTO spool_removals(job_id, removal_generation, cleanup_fence, root_instance_id, started_ns)
VALUES(?, ?, ?, ?, ?)`, removal.jobID, removal.generation, removal.cleanupFence, removal.rootInstanceID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO runtime_removal_manifests(job_id, removal_generation, cleanup_fence, root_instance_id,
manifest_json, runtime_quiescence_json, phase, prepared_ns, quiesced_ns, completed_ns)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, removal.jobID, removal.generation, removal.cleanupFence, removal.rootInstanceID,
		manifestJSON, receiptJSON, runtimeRemovalComplete, now, now, now); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	spool, err := openLogSpool(directory, nodeID, 1024)
	if err != nil {
		t.Fatalf("open migrated spool: %v", err)
	}
	defer spool.Close()
	intent, found, err := spool.removalIntent(t.Context(), removal.jobID)
	if err != nil || !found || intent.kind != contract.JobKindOCI {
		t.Fatalf("migrated removal intent = %+v err=%v", intent, err)
	}
	record, found, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found || record.phase != runtimeRemovalQuarantined || record.completedAt != nil || record.attestedAt != nil {
		t.Fatalf("migrated runtime removal = %+v found=%t err=%v", record, found, err)
	}
}

func TestLogSpoolMigratesLegacyAttemptKind(t *testing.T) {
	tests := []struct {
		name, class, evidence, want string
	}{
		{name: "manifest derived OCI", class: contract.JobClassService, evidence: "manifest", want: contract.JobKindOCI},
		{name: "pin derived OCI", class: contract.JobClassService, evidence: "pin", want: contract.JobKindOCI},
		{name: "plain process", class: contract.JobClassOneShot, want: contract.JobKindProcess},
		{name: "unclassifiable service fails closed", class: contract.JobClassService, want: "unclassified"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			nodeID := "attempt-kind-node"
			db, err := sql.Open("sqlite", filepath.Join(directory, spoolFileName(nodeID)))
			if err != nil {
				t.Fatal(err)
			}
			const oldSchema = `
CREATE TABLE spool_attempts (
  attempt_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, fencing_token TEXT NOT NULL,
  class TEXT NOT NULL, created_ns INTEGER NOT NULL, result_json BLOB, finished_ns INTEGER, incomplete_json BLOB
);
CREATE TABLE runtime_attempt_manifests (
  attempt_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, runtime_kind TEXT NOT NULL,
  removal_generation TEXT NOT NULL, manifest_json BLOB NOT NULL, created_ns INTEGER NOT NULL
);
CREATE TABLE oci_binding_pins (
  job_id TEXT PRIMARY KEY, reference TEXT NOT NULL, digest TEXT NOT NULL,
  platform_os TEXT NOT NULL, platform_architecture TEXT NOT NULL, platform_variant TEXT NOT NULL,
  snapshotter TEXT NOT NULL, updated_ns INTEGER NOT NULL
);`
			if _, err := db.Exec(oldSchema); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO spool_attempts(attempt_id, job_id, fencing_token, class, created_ns)
VALUES('attempt', 'job', 'fence', ?, 1)`, test.class); err != nil {
				t.Fatal(err)
			}
			switch test.evidence {
			case "manifest":
				_, err = db.Exec(`INSERT INTO runtime_attempt_manifests VALUES('attempt','job',?,'1','{}',1)`, contract.JobKindOCI)
			case "pin":
				_, err = db.Exec(`INSERT INTO oci_binding_pins VALUES('job','ref','sha256:digest','linux','amd64','','overlayfs',1)`)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			spool, err := openLogSpool(directory, nodeID, 1024)
			if err != nil {
				t.Fatal(err)
			}
			defer spool.Close()
			var got string
			if err := spool.db.QueryRowContext(t.Context(), `SELECT kind FROM spool_attempts WHERE attempt_id='attempt'`).Scan(&got); err != nil || got != test.want {
				t.Fatalf("migrated kind=%q want=%q err=%v", got, test.want, err)
			}
		})
	}
}
