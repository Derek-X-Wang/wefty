//go:build (service_acceptance || service_acceptance_realtiming) && (darwin || linux)

package serviceacceptance

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

type slotFailureJob struct {
	Class string `json:"class"`
	Index int    `json:"index"`
	JobID string `json:"job_id"`
}
type slotFailureEvidence struct {
	harness *acceptanceHarness
	stage   string
	jobs    []slotFailureJob
}

func newSlotFailureEvidence() *slotFailureEvidence {
	e := &slotFailureEvidence{}
	for i := range 3 {
		e.jobs = append(e.jobs, slotFailureJob{Class: "service", Index: i})
	}
	for i := range 5 {
		e.jobs = append(e.jobs, slotFailureJob{Class: "oneshot", Index: i})
	}
	return e
}

// A defer (not Cleanup) captures before harness Cleanup stops processes and
// removes databases. No reads/polling run on the successful test path.
func (e *slotFailureEvidence) log(t *testing.T) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Log("slot failure capture panic; original test failure retained")
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if e.harness == nil {
		t.Logf("slot failure evidence: stage=%s harness unavailable", e.stage)
		return
	}
	secrets := []string{}
	for _, spec := range e.harness.specs {
		for _, value := range spec.Execution.SensitiveEnv {
			if value != "" {
				secrets = append(secrets, value)
			}
		}
	}
	data := e.capture(ctx, e.harness.l1Database, e.harness.spoolDirectory, e.harness.agent.outputString(), secrets)
	encoded, err := json.Marshal(data)
	if err != nil {
		t.Log("slot failure capture encoding error; original failure retained")
		return
	}
	t.Logf("[slot-failure-evidence] %s", encoded)
}

func (e *slotFailureEvidence) capture(ctx context.Context, l1Path, spoolDir, stderr string, secrets []string) map[string]any {
	started := time.Now()
	result := map[string]any{"stage": e.stage, "jobs": e.jobs, "captured_at": started.UTC(), "renewal_note": "lease_expires_ns is durable; updated_ns may include other transitions, no dedicated last-renewal field", "cross_database_snapshot": "L1 and spool are separate observations"}
	ids := []any{}
	marks := []string{}
	for _, job := range e.jobs {
		if job.JobID != "" {
			ids = append(ids, job.JobID)
			marks = append(marks, "?")
		}
	}
	if len(ids) == 0 {
		result["capture_error"] = "no submitted job identities available"
		result["agent_stderr"] = slotRedact(stderr, secrets)
		return result
	}
	filter := strings.Join(marks, ",")
	type query struct {
		name, sql string
		args      []any
	}
	queries := []query{
		{"jobs", `SELECT j.job_id,j.state,j.current_attempt_id,j.updated_ns,s.desired_state,s.bound_node_id,s.lease_loss_count,s.lifetime_restart_count FROM jobs j LEFT JOIN service_jobs s ON s.job_id=j.job_id WHERE j.job_id IN (` + filter + `)`, ids},
		{"attempts", `SELECT job_id,attempt_id,node_id,state,fencing_token,boot_session_id,authority_generation,lease_expires_ns,started_ns,created_ns,updated_ns,result_json,late_result_json FROM attempts WHERE job_id IN (` + filter + `) ORDER BY created_ns LIMIT 129`, ids},
		{"nodes", `SELECT n.node_id,n.state,n.boot_session_id,n.authority_generation,n.last_heartbeat_ns,n.max_service_slots,n.max_oneshot_slots,(SELECT COUNT(*) FROM service_jobs s JOIN jobs j ON j.job_id=s.job_id WHERE s.bound_node_id=n.node_id AND ((j.state='queued' AND s.desired_state='running') OR j.state IN ('claimed','running','stopping','removal_pending','agent_cleaned'))) AS service_occupancy,(SELECT COUNT(*) FROM attempts a WHERE a.node_id=n.node_id AND a.state IN ('claimed','running','awaiting-input') AND NOT EXISTS(SELECT 1 FROM service_jobs s WHERE s.job_id=a.job_id)) AS oneshot_occupancy FROM nodes n LIMIT 129`, nil},
	}
	read := func(path, prefix string, queries []query) {
		dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}).String()
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			result[prefix+"_error"] = err.Error()
			return
		}
		defer db.Close()
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			result[prefix+"_error"] = err.Error()
			return
		}
		defer tx.Rollback()
		for _, q := range queries {
			rows, err := tx.QueryContext(ctx, q.sql, q.args...)
			if err != nil {
				result[prefix+"_"+q.name+"_error"] = err.Error()
				continue
			}
			records, readErr := slotReadRows(rows, &secrets)
			if closeErr := rows.Close(); readErr == nil {
				readErr = closeErr
			}
			result[prefix+"_"+q.name] = records
			if readErr != nil {
				result[prefix+"_"+q.name+"_error"] = readErr.Error()
			}
		}
		if err := tx.Commit(); err != nil {
			result[prefix+"_commit_error"] = err.Error()
		}
	}
	read(l1Path, "l1", queries)
	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		result["spool_error"] = err.Error()
	} else {
		paths := []string{}
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) == ".sqlite" {
				paths = append(paths, filepath.Join(spoolDir, entry.Name()))
			}
		}
		if len(paths) != 1 {
			result["spool_error"] = fmt.Sprintf("expected one spool database, found %d", len(paths))
		} else {
			read(paths[0], "spool", []query{
				{"attempts", `SELECT job_id,attempt_id,fencing_token,class,kind,created_ns,finished_ns,result_json,completion_disposition,completion_reason FROM spool_attempts WHERE job_id IN (` + filter + `) LIMIT 129`, ids},
				{"receipts", `SELECT job_id,attempt_id,disposition,reason,observed_ns,finished_ns,terminal_audit_json FROM spool_completion_receipts WHERE job_id IN (` + filter + `) LIMIT 129`, ids},
			})
		}
	}
	result["agent_stderr"] = stderr
	result["capture_elapsed_ns"] = time.Since(started).Nanoseconds()
	if err := ctx.Err(); err != nil {
		result["capture_context_error"] = err.Error()
	}
	// Redact after all authority values have been collected, including earlier
	// error/stderr strings. Truncation happens after redaction, not mid-secret.
	return slotRedactTree(result, secrets).(map[string]any)
}

func slotReadRows(rows *sql.Rows, secrets *[]string) ([]map[string]any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	records := []map[string]any{}
	for rows.Next() {
		if len(records) == 128 {
			return records, fmt.Errorf("snapshot truncated at 128 rows")
		}
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return records, err
		}
		record := map[string]any{}
		for i, name := range columns {
			value := values[i]
			if b, ok := value.([]byte); ok {
				value = string(b)
			}
			switch name {
			case "fencing_token", "boot_session_id":
				if value != nil {
					raw := fmt.Sprint(value)
					*secrets = append(*secrets, raw)
					record[name+"_sha256"] = fmt.Sprintf("%x", sha256.Sum256([]byte(raw)))
				}
			case "result_json", "late_result_json", "terminal_audit_json":
				record[name] = slotResultArm(value)
			default:
				record[name] = value
			}
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

// Curate result fields: never dump arbitrary runtime/spawn messages or specs.
func slotResultArm(value any) any {
	if value == nil {
		return nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(fmt.Sprint(value)), &raw); err != nil {
		return map[string]any{"decode_error": "invalid durable result JSON"}
	}
	result := map[string]any{}
	for _, name := range []string{"exit_code", "signal", "termination_cause", "oom", "disk_exhausted", "log_evidence_incomplete"} {
		if data, ok := raw[name]; ok {
			var v any
			if json.Unmarshal(data, &v) == nil {
				result[name] = v
			}
		}
	}
	for _, name := range []string{"spawn_error", "runtime_failure", "output_error"} {
		if data, ok := raw[name]; ok && string(data) != "null" && string(data) != "\"\"" {
			result[name+"_present"] = true
			result[name+"_sha256"] = fmt.Sprintf("%x", sha256.Sum256(data))
			text := string(data)
			for _, cause := range []string{"context canceled", "context deadline exceeded", "lease expired", "sql: database is closed"} {
				if strings.Contains(text, cause) {
					result[name+"_cause"] = cause
				}
			}
		}
	}
	return result
}

var slotCredentialPattern = regexp.MustCompile(`(?i)(bearer\s+|(?:token|password|secret|credential|fencing_token)["']?\s*[:=]\s*["']?)[^\s,"']+`)

func slotRedact(text string, secrets []string) string {
	for _, secret := range secrets {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "[REDACTED]")
		}
	}
	text = slotCredentialPattern.ReplaceAllString(text, "[REDACTED-CREDENTIAL]")
	if len(text) > 65536 {
		text = "[truncated]" + text[len(text)-65536:]
	}
	return text
}
func slotRedactTree(value any, secrets []string) any {
	switch v := value.(type) {
	case string:
		return slotRedact(v, secrets)
	case map[string]any:
		for key, item := range v {
			v[key] = slotRedactTree(item, secrets)
		}
		return v
	case []map[string]any:
		for _, item := range v {
			slotRedactTree(item, secrets)
		}
		return v
	default:
		return value
	}
}

func TestSlotFailureEvidenceCapturesDurableArmAndRedacts(t *testing.T) {
	directory := t.TempDir()
	l1Path := filepath.Join(directory, "l1.sqlite")
	spoolDir := filepath.Join(directory, "spool")
	if err := os.Mkdir(spoolDir, 0700); err != nil {
		t.Fatal(err)
	}
	secret := "known-sensitive-env-value"
	token := "live-fencing-authority"
	boot := "private-boot-session"
	setup := func(path string, queries []string) {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		for _, q := range queries {
			if _, err := db.Exec(q); err != nil {
				t.Fatal(err)
			}
		}
	}
	setup(l1Path, []string{
		`CREATE TABLE jobs(job_id TEXT,state TEXT,current_attempt_id TEXT,updated_ns INTEGER)`,
		`CREATE TABLE service_jobs(job_id TEXT,desired_state TEXT,bound_node_id TEXT,lease_loss_count INTEGER,lifetime_restart_count INTEGER)`,
		`CREATE TABLE attempts(job_id TEXT,attempt_id TEXT,node_id TEXT,state TEXT,fencing_token TEXT,boot_session_id TEXT,authority_generation INTEGER,lease_expires_ns INTEGER,started_ns INTEGER,created_ns INTEGER,updated_ns INTEGER,result_json BLOB,late_result_json BLOB)`,
		`CREATE TABLE nodes(node_id TEXT,state TEXT,boot_session_id TEXT,authority_generation INTEGER,last_heartbeat_ns INTEGER,max_service_slots INTEGER,max_oneshot_slots INTEGER)`,
		`INSERT INTO jobs VALUES('oneshot-job','failed','attempt-1',101)`,
		`INSERT INTO attempts VALUES('oneshot-job','attempt-1','node','failed','live-fencing-authority','private-boot-session',1,100,50,40,101,'{"output_error":"context canceled known-sensitive-env-value"}',NULL)`,
		`INSERT INTO nodes VALUES('node','alive','private-boot-session',1,99,2,4)`,
	})
	setup(filepath.Join(spoolDir, "node.sqlite"), []string{
		`CREATE TABLE spool_attempts(job_id TEXT,attempt_id TEXT,fencing_token TEXT,class TEXT,kind TEXT,created_ns INTEGER,finished_ns INTEGER,result_json BLOB,completion_disposition TEXT,completion_reason TEXT)`,
		`CREATE TABLE spool_completion_receipts(job_id TEXT,attempt_id TEXT,disposition TEXT,reason TEXT,observed_ns INTEGER,finished_ns INTEGER,terminal_audit_json BLOB)`,
		`INSERT INTO spool_attempts VALUES('oneshot-job','attempt-1','live-fencing-authority','oneshot','process',40,101,'{"output_error":"context canceled known-sensitive-env-value"}',NULL,NULL)`,
	})
	evidence := newSlotFailureEvidence()
	evidence.stage = "await-oneshot-2-succeeded"
	evidence.jobs[5].JobID = "oneshot-job"
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	data := evidence.capture(ctx, l1Path, spoolDir, "stderr "+secret+" "+token+" "+boot+" password=unlisted-credential", []string{secret})
	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, forbidden := range []string{secret, token, boot, "unlisted-credential"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("capture leaked %q", forbidden)
		}
	}
	for _, required := range []string{`"stage":"await-oneshot-2-succeeded"`, `"index":2,"job_id":"oneshot-job"`, `"state":"failed"`, `"output_error_present":true`, `"output_error_cause":"context canceled"`, `"lease_expires_ns":100`, `"service_occupancy":0`, `"oneshot_occupancy":0`, `"agent_stderr":"stderr [REDACTED] [REDACTED] [REDACTED] [REDACTED-CREDENTIAL]"`} {
		if !strings.Contains(text, required) {
			t.Fatalf("capture missing %s: %s", required, text)
		}
	}
	for key := range data {
		if strings.HasSuffix(key, "_error") {
			t.Fatalf("unexpected capture error %s=%v", key, data[key])
		}
	}
	// Query-only capture must not replace a failed durable result or mutate state.
	db, err := sql.Open("sqlite", l1Path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var state string
	if err := db.QueryRow("SELECT state FROM jobs").Scan(&state); err != nil || state != "failed" {
		t.Fatalf("state=%s err=%v", state, err)
	}
}

func TestSlotFailureEvidenceReportsReadErrors(t *testing.T) {
	evidence := newSlotFailureEvidence()
	evidence.jobs[3].JobID = "missing-job"
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	directory := t.TempDir()
	path := filepath.Join(directory, "absent.sqlite")
	data := evidence.capture(ctx, path, directory, "", nil)
	if _, ok := data["l1_error"]; !ok {
		t.Fatalf("missing canceled-read error: %+v", data)
	}
	if _, ok := data["l1_attempts"]; ok {
		t.Fatalf("failed read became an empty observation: %+v", data)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("read-only capture created database: %v", err)
	}
}
