package l1

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

const (
	scrubRunTokenSecret = "run-token-secret-5c1f0e"
	scrubParamSecret    = "param-secret-8d2a41"
	scrubScriptSecret   = "script-secret-3b7e92"
)

// secretBearingOneShot is what L3 dispatches for an inline-script run: the
// run token in SensitiveEnv, the script bytes inline, and the parameter
// document as the run_params_json label.
func secretBearingOneShot(dispatchKey string) contract.JobSpec {
	return secretBearingOneShotWith(dispatchKey, scrubRunTokenSecret, scrubParamSecret, scrubScriptSecret)
}

func secretBearingOneShotWith(dispatchKey, runToken, param, scriptWord string) contract.JobSpec {
	script := []byte("#!/bin/sh\necho " + scriptWord + "\n")
	digest := sha256.Sum256(script)
	return contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   dispatchKey,
		Kind:          contract.JobKindProcess,
		Class:         contract.JobClassOneShot,
		RoutingTags:   []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{
				InlineBase64: base64.StdEncoding.EncodeToString(script), SHA256: hex.EncodeToString(digest[:]),
				Interpreter: []string{"/bin/sh"}, Mode: 0o700,
			},
			Argv:             []string{"wefty-inline-" + dispatchKey},
			Env:              map[string]string{contract.EnvRunID: "run-" + dispatchKey},
			SensitiveEnv:     map[string]string{contract.EnvRunToken: runToken},
			WorkingDirectory: "/tmp",
			HandoffDirectory: "/tmp/handoff",
		},
		Labels: map[string]string{
			contract.LabelRunID:     "run-" + dispatchKey,
			contract.LabelRunParams: `{"api_key":"` + param + `"}`,
		},
	}
}

func scrubSecrets(spec contract.JobSpec) [][]byte {
	var params struct {
		APIKey string `json:"api_key"`
	}
	if err := json.Unmarshal([]byte(spec.Labels[contract.LabelRunParams]), &params); err != nil || params.APIKey == "" {
		panic(fmt.Sprintf("fixture run_params_json = %q: %v", spec.Labels[contract.LabelRunParams], err))
	}
	return [][]byte{
		[]byte(spec.Execution.SensitiveEnv[contract.EnvRunToken]), []byte(params.APIKey),
		[]byte(spec.Execution.Executable.InlineBase64),
	}
}

func storedSpecJSON(t *testing.T, store *Store, jobID string) ([]byte, sql.NullInt64) {
	t.Helper()
	var spec []byte
	var scrubbedNS sql.NullInt64
	if err := store.db.QueryRow(`SELECT spec_json, secrets_scrubbed_ns FROM jobs WHERE job_id=?`, jobID).
		Scan(&spec, &scrubbedNS); err != nil {
		t.Fatal(err)
	}
	return spec, scrubbedNS
}

func assertSpecHoldsSecrets(t *testing.T, store *Store, jobID string, secrets [][]byte) {
	t.Helper()
	spec, scrubbedNS := storedSpecJSON(t, store, jobID)
	for _, secret := range secrets {
		if !bytes.Contains(spec, secret) {
			t.Fatalf("live job lost %q before it was terminal: %s", secret, spec)
		}
	}
	if scrubbedNS.Valid {
		t.Fatalf("live job carries secrets_scrubbed_ns=%d", scrubbedNS.Int64)
	}
}

// assertScrubbedSpec checks the permanent record: no secret, still parseable,
// and still carrying the non-secret facts of what ran.
func assertScrubbedSpec(t *testing.T, store *Store, jobID string, secrets [][]byte) contract.JobSpec {
	t.Helper()
	raw, scrubbedNS := storedSpecJSON(t, store, jobID)
	for _, secret := range secrets {
		if bytes.Contains(raw, secret) {
			t.Fatalf("terminal one-shot spec retained %q: %s", secret, raw)
		}
	}
	for _, member := range []string{"sensitive_env", "inline_base64", contract.LabelRunParams} {
		if bytes.Contains(raw, []byte(member)) {
			t.Fatalf("terminal one-shot spec retained member %q: %s", member, raw)
		}
	}
	if !scrubbedNS.Valid {
		t.Fatal("terminal one-shot has no secrets_scrubbed_ns")
	}
	var spec contract.JobSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("scrubbed spec no longer parses: %v: %s", err, raw)
	}
	if spec.Class != contract.JobClassOneShot || spec.Labels[contract.LabelRunID] == "" {
		t.Fatalf("scrubbed spec lost its non-secret record: %s", raw)
	}
	return spec
}

func databaseFiles(t *testing.T, store *Store) []byte {
	t.Helper()
	var sequence int
	var name, path string
	if err := store.db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	var all []byte
	for _, file := range []string{path, path + "-wal"} {
		payload, err := os.ReadFile(file)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		all = append(all, payload...)
	}
	return all
}

func TestTerminalOneShotLosesItsSecretsButKeepsItsRecordAndReplays(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "run-ledger", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
	spec := secretBearingOneShot("scrub-on-success")
	secrets := scrubSecrets(spec)

	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("submit status = %d body=%s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(body, []byte(scrubRunTokenSecret)) || bytes.Contains(body, []byte(scrubParamSecret)) {
		t.Fatalf("submit response exposed a secret: %s", body)
	}
	assertSpecHoldsSecrets(t, h.store, job.JobID, secrets)

	claim := claimJob(t, h, agent, "node-1")
	if claim.Job.Spec.Execution.SensitiveEnv[contract.EnvRunToken] != scrubRunTokenSecret ||
		claim.Job.Spec.Labels[contract.LabelRunParams] == "" || claim.Job.Spec.Execution.Executable.InlineBase64 == "" {
		t.Fatalf("claim did not deliver what the execution needs: %#v", claim.Job.Spec)
	}
	exitCode := 0
	completion := CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion:" + claim.Lease.AttemptID,
		Result: ProcessResult{ExitCode: &exitCode},
	}
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	status, _, body = h.do(agent, http.MethodPost, path, completion)
	if status != http.StatusOK {
		t.Fatalf("completion status = %d body=%s", status, body)
	}
	var completed Job
	if err := json.Unmarshal(body, &completed); err != nil {
		t.Fatal(err)
	}
	if completed.State != contract.JobSucceeded || completed.SecretsScrubbedAt == nil {
		t.Fatalf("completion answered %+v, want succeeded and scrubbed", completed)
	}
	for _, secret := range secrets {
		if bytes.Contains(body, secret) {
			t.Fatalf("completion response carried %q: %s", secret, body)
		}
	}
	scrubbed := assertScrubbedSpec(t, h.store, job.JobID, secrets)
	if scrubbed.Execution.Executable.SHA256 != spec.Execution.Executable.SHA256 ||
		len(scrubbed.Execution.Executable.Interpreter) != 1 || scrubbed.Execution.Env[contract.EnvRunID] == "" {
		t.Fatalf("scrubbed spec dropped non-secret provenance: %#v", scrubbed.Execution)
	}

	// The public record says the spec was scrubbed, and when.
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+job.JobID, nil)
	if status != http.StatusOK {
		t.Fatalf("get status = %d body=%s", status, body)
	}
	var read Job
	if err := json.Unmarshal(body, &read); err != nil {
		t.Fatal(err)
	}
	if read.SecretsScrubbedAt == nil || !read.SecretsScrubbedAt.Equal(completed.UpdatedAt) {
		t.Fatalf("secrets_scrubbed_at = %v, want the terminal transition time %v", read.SecretsScrubbedAt, completed.UpdatedAt)
	}

	// Dispatch-key replay compares the hash L1 stored at submit, so the
	// identical request still resolves to the stored job.
	status, headers, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusOK || headers.Get("Idempotent-Replay") != "true" {
		t.Fatalf("dispatch-key replay status = %d replay=%q body=%s", status, headers.Get("Idempotent-Replay"), body)
	}
	var replayed Job
	if err := json.Unmarshal(body, &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.JobID != job.JobID || replayed.State != contract.JobSucceeded {
		t.Fatalf("dispatch-key replay = %+v, want stored job %s", replayed, job.JobID)
	}
	different := secretBearingOneShot("scrub-on-success")
	different.Execution.SensitiveEnv[contract.EnvRunToken] = "another-token"
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs", different)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorDispatchKeyConflict)

	// Completion replay (#553) still answers already-recorded.
	status, headers, body = h.do(agent, http.MethodPost, path, completion)
	if status != http.StatusOK || headers.Get("Idempotent-Replay") != "true" {
		t.Fatalf("completion replay status = %d replay=%q body=%s", status, headers.Get("Idempotent-Replay"), body)
	}

	// After the sweep's WAL truncation no secret byte is left in the
	// database file or its WAL.
	if _, err := h.store.SweepScrubbedSecrets(context.Background()); err != nil {
		t.Fatal(err)
	}
	files := databaseFiles(t, h.store)
	for _, secret := range secrets {
		if bytes.Contains(files, secret) {
			t.Fatalf("database files still hold %q after the sweep", secret)
		}
	}
}

// Every terminal path is covered, not only completion: lease expiry through
// reconciliation fails the one-shot and scrubs it in the same transaction.
func TestLeaseExpiredOneShotIsScrubbed(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
	spec := secretBearingOneShot("scrub-on-lease-loss")
	job, _, err := h.store.CreateJobAs(context.Background(), spec, runLedgerOrigin)
	if err != nil {
		t.Fatal(err)
	}
	claimJob(t, h, agent, "node-1")
	assertSpecHoldsSecrets(t, h.store, job.JobID, scrubSecrets(spec))
	h.clock.Advance(time.Hour)
	if _, err := h.store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	failed, err := h.store.GetJob(context.Background(), job.JobID)
	if err != nil || failed.State != contract.JobFailed {
		t.Fatalf("expired one-shot = %+v err %v, want failed", failed, err)
	}
	assertScrubbedSpec(t, h.store, job.JobID, scrubSecrets(spec))
}

// A one-shot that still has a retry keeps what its next attempt needs. OCI
// pre-start runtime loss requeues it; only exhausting the budget, on the
// claim path, makes it terminal and scrubs it.
func TestRequeuedOneShotKeepsSecretsUntilItsLastAttempt(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{
		PrestartInfrastructureBudget: 2 * time.Second,
		Jitter:                       func(delay time.Duration) time.Duration { return delay },
	}, map[string]NodePolicy{"node-1": DefaultNodePolicy()})
	registerOCIFixtureNode(t, h)
	spec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "scrub-oci-requeue", Kind: contract.JobKindOCI,
		Class: contract.JobClassOneShot, RuntimeHandler: "io.containerd.runc.v2",
		Execution: contract.ExecutionSpec{
			SensitiveEnv: map[string]string{contract.EnvRunToken: scrubRunTokenSecret},
			OCI:          &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest"}},
		},
		Labels: map[string]string{
			contract.LabelRunID:     "run-scrub-oci-requeue",
			contract.LabelRunParams: `{"api_key":"` + scrubParamSecret + `"}`,
		},
	}
	secrets := [][]byte{[]byte(scrubRunTokenSecret), []byte(scrubParamSecret)}
	job, _, err := h.store.CreateJobAs(context.Background(), spec, runLedgerOrigin)
	if err != nil {
		t.Fatal(err)
	}
	claim := claimOCIFixture(t, h, contract.JobClassOneShot)
	requeued, err := h.store.CompleteAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "runtime-loss",
		Result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureRuntimeUnavailable, Message: "helper unavailable"}},
	})
	if err != nil || requeued.State != contract.JobQueued || requeued.SecretsScrubbedAt != nil {
		t.Fatalf("runtime loss = %+v err %v, want queued and unscrubbed", requeued, err)
	}
	assertSpecHoldsSecrets(t, h.store, job.JobID, secrets)

	h.clock.Advance(3 * time.Second)
	if claim, err := h.store.ClaimJob(context.Background(), "agent", "node-1", "boot-node-1", contract.JobClassOneShot); err != nil || claim != nil {
		t.Fatalf("claim after the budget = %#v err %v", claim, err)
	}
	assertScrubbedSpec(t, h.store, job.JobID, secrets)
}

// Services keep their spec until removal, which scrubs it as before; the
// terminal rule is for one-shots only.
func TestFailedServiceKeepsItsSpecUntilRemoval(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"service"}})
	spec := removalServiceSpec("scrub-not-a-service", []string{"service"})
	spec.Execution.SensitiveEnv = map[string]string{"SERVICE_SECRET": scrubRunTokenSecret}
	job, _, err := h.store.CreateJob(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE jobs SET state=? WHERE job_id=?`, contract.JobFailed, job.JobID); err != nil {
		t.Fatal(err)
	}
	sweep, err := h.store.SweepScrubbedSecrets(context.Background())
	if err != nil || sweep.Backfilled != 0 {
		t.Fatalf("sweep = %+v err %v, want no service backfilled", sweep, err)
	}
	assertSpecHoldsSecrets(t, h.store, job.JobID, [][]byte{[]byte(scrubRunTokenSecret)})
}

// A database written before the scrub existed holds terminal one-shots with
// their secrets. Opening it installs the rule, and the sweep scrubs them.
func TestSweepBackfillsTerminalOneShotsFromBeforeTheScrub(t *testing.T) {
	path := t.TempDir() + "/legacy.sqlite"
	clock := &fakeClock{now: time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)}
	store, err := OpenStore(path, StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	spec := secretBearingOneShot("scrub-backfill")
	job, _, err := store.CreateJobAs(context.Background(), spec, runLedgerOrigin)
	if err != nil {
		t.Fatal(err)
	}
	liveSpec := secretBearingOneShotWith("scrub-backfill-live", "live-token-2f9c", "live-param-7a1d", "live-script-0e4b")
	live, _, err := store.CreateJobAs(context.Background(), liveSpec, runLedgerOrigin)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// L1 has opened its database with secure_delete since #49; the legacy
	// writer matches, so the only secret bytes left on disk are the rows'.
	legacy, err := sql.Open("sqlite", sqliteDSN(path, sqliteBusyTimeout))
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`DROP TRIGGER jobs_scrub_terminal_one_shot_secrets`,
		`DROP INDEX jobs_secrets_unscrubbed`,
		`DROP TABLE secret_scrub_state`,
		`ALTER TABLE jobs DROP COLUMN secrets_scrubbed_ns`,
		`UPDATE jobs SET state='succeeded' WHERE job_id='` + job.JobID + `'`,
	} {
		if _, err := legacy.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	migrated, err := OpenStore(path, StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	assertSpecHoldsSecrets(t, migrated, job.JobID, scrubSecrets(spec))
	if files := databaseFiles(t, migrated); !bytes.Contains(files, []byte(scrubRunTokenSecret)) {
		t.Fatal("fixture error: the legacy database files do not hold the secret the sweep must remove")
	}
	sweep, err := migrated.SweepScrubbedSecrets(context.Background())
	if err != nil || sweep.Backfilled != 1 || !sweep.TruncatedWAL {
		t.Fatalf("backfill sweep = %+v err %v, want one job scrubbed and the WAL truncated", sweep, err)
	}
	assertScrubbedSpec(t, migrated, job.JobID, scrubSecrets(spec))
	files := databaseFiles(t, migrated)
	for _, secret := range scrubSecrets(spec) {
		if bytes.Contains(files, secret) {
			t.Fatalf("database files still hold %q after the backfill sweep", secret)
		}
	}
	// The live job's secrets are untouched, in the row and on disk.
	assertSpecHoldsSecrets(t, migrated, live.JobID, scrubSecrets(liveSpec))
	if !bytes.Contains(files, []byte("live-token-2f9c")) {
		t.Fatal("the sweep removed a live job's secret from disk")
	}
	if again, err := migrated.SweepScrubbedSecrets(context.Background()); err != nil || again.Backfilled != 0 || again.TruncatedWAL {
		t.Fatalf("second sweep = %+v err %v, want nothing to do", again, err)
	}
	replayed, wasReplay, err := migrated.CreateJobAs(context.Background(), spec, runLedgerOrigin)
	if err != nil || !wasReplay || replayed.JobID != job.JobID {
		t.Fatalf("replay of backfilled job = %+v replay=%v err %v", replayed, wasReplay, err)
	}
}
