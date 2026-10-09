package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// The observer commits a writer after the real production statement, before
// the next read. Red runs keep this inert seam on the original implementation.
func observeOnce(t *testing.T, ctx context.Context, point string, change func()) (context.Context, *bool) {
	t.Helper()
	fired := false
	return context.WithValue(ctx, readObservationKey{}, func(at string) {
		if at == point && !fired {
			fired = true
			change()
		}
	}), &fired
}
func jobReadRequest(ctx context.Context, method, path, id string) *http.Request {
	ctx = context.WithValue(ctx, identityContextKey{}, fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	r := httptest.NewRequest(method, path, nil).WithContext(ctx)
	r.SetPathValue("job_id", id)
	return r
}
func assertCurrentAttemptPresent(t *testing.T, job Job) {
	t.Helper()
	if job.CurrentAttemptID == "" {
		return
	}
	for _, attempt := range job.Attempts {
		if attempt.AttemptID == job.CurrentAttemptID {
			return
		}
	}
	t.Fatalf("current attempt %s missing from attempts: %+v", job.CurrentAttemptID, job.Attempts)
}
func TestJobReadSnapshotRemovalBetweenReads(t *testing.T) {
	for _, listing := range []bool{false, true} {
		t.Run(fmt.Sprint("listing=", listing), func(t *testing.T) {
			h, _, job, claim := jobProjectionFixture(t, "never-interruption")
			// Unbound removal commits its tombstone and deletes the Job and attempts.
			if _, err := h.store.db.Exec("UPDATE service_jobs SET bound_node_id=NULL WHERE job_id=?", job.JobID); err != nil {
				t.Fatal(err)
			}
			ctx, fired := observeOnce(t, t.Context(), "job", func() {
				commitReadTestRemoval(t, h.store, job.JobID)
			})
			// The injected writer's fsync is not projection cost.
			ctx = context.WithValue(ctx, readSnapshotHardLimitContextKey{}, 10*time.Second)
			w := httptest.NewRecorder()
			r := jobReadRequest(ctx, http.MethodGet, "/v1/jobs?class=service", job.JobID)
			if listing {
				h.server.listJobs(w, r)
			} else {
				h.server.getJob(w, r)
			}
			if !*fired {
				t.Fatal("writer did not commit between reads")
			}
			if w.Code != http.StatusOK {
				t.Fatalf("removal returned %d %s", w.Code, w.Body.String())
			}
			got := Job{}
			if listing {
				var page JobList
				if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
					t.Fatal(err)
				}
				if len(page.Jobs) != 1 {
					t.Fatalf("page=%+v", page)
				}
				got = page.Jobs[0]
			} else {
				got = decodeJob(t, w.Body.Bytes())
			}
			if got.State != contract.JobFailed || got.Removal != nil || got.CurrentAttemptID != claim.Lease.AttemptID {
				t.Fatalf("mixed removal view: %+v", got)
			}
			assertCurrentAttemptPresent(t, got)
			current, err := h.store.GetJob(t.Context(), job.JobID)
			if err != nil || current.Removal == nil {
				t.Fatalf("removal did not commit: %+v %v", current, err)
			}
		})
	}
}
func TestJobReadSnapshotClaimBetweenReads(t *testing.T) {
	for _, listing := range []bool{false, true} {
		t.Run(fmt.Sprint("listing=", listing), func(t *testing.T) {
			h, _, agent, node, job, old := neverFixture(t, "process")
			if _, err := h.store.db.Exec("UPDATE attempts SET state='lost' WHERE attempt_id=?", old.Lease.AttemptID); err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.db.Exec("UPDATE jobs SET state='queued' WHERE job_id=?", job.JobID); err != nil {
				t.Fatal(err)
			}
			point := "attempts"
			if listing {
				point = "job"
			}
			var claimed Claim
			ctx, fired := observeOnce(t, t.Context(), point, func() { claimed = claimClass(t, h, agent, node, contract.JobClassService) })
			// The injected writer's fsync is not projection cost.
			ctx = context.WithValue(ctx, readSnapshotHardLimitContextKey{}, 10*time.Second)
			w := httptest.NewRecorder()
			r := jobReadRequest(ctx, http.MethodGet, "/v1/jobs?class=service", job.JobID)
			if listing {
				h.server.listJobs(w, r)
			} else {
				h.server.getJob(w, r)
			}
			if !*fired {
				t.Fatal("claim boundary not reached")
			}
			if claimed.Lease.AttemptID == old.Lease.AttemptID {
				t.Fatal("claim was not new")
			}
			if w.Code != http.StatusOK {
				t.Fatalf("claim returned %d %s", w.Code, w.Body.String())
			}
			got := Job{}
			if listing {
				var page JobList
				json.Unmarshal(w.Body.Bytes(), &page)
				if len(page.Jobs) != 1 {
					t.Fatalf("page=%+v", page)
				}
				got = page.Jobs[0]
			} else {
				got = decodeJob(t, w.Body.Bytes())
			}
			if got.State != contract.JobQueued || got.CurrentAttemptID != old.Lease.AttemptID || len(got.Attempts) != 1 {
				t.Fatalf("mixed claim view: %+v", got)
			}
			assertCurrentAttemptPresent(t, got)
			status, _, raw := h.do(h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}}), http.MethodGet, "/v1/jobs/"+job.JobID+"?class=service", nil)
			if status != http.StatusOK {
				t.Fatalf("next read=%d %s", status, raw)
			}
			next := decodeJob(t, raw)
			assertCurrentAttemptPresent(t, next)
			if next.CurrentAttemptID != claimed.Lease.AttemptID {
				t.Fatal("next snapshot missed committed claim")
			}
		})
	}
}
func TestJobReadSnapshotLogsBetweenPageAndMarker(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(fmt.Sprint("remove=", remove), func(t *testing.T) {
			h, _, agent, _, job, claim := neverFixture(t, "process")
			path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)
			h.store.serviceLogRetentionBytes = 6
			appendRetentionLogs(t, h, agent, path, claim.Lease.FencingToken, []contract.LogEvent{logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("older")), logEvent(claim.Lease.AttemptID, contract.LogStdout, 1, []byte("before"))})
			before, err := h.store.GetJobLogs(t.Context(), job.JobID, "", 10)
			if err != nil || before.Truncation == nil {
				t.Fatalf("fixture marker missing: %+v %v", before, err)
			}
			ctx, fired := observeOnce(t, t.Context(), "log_page", func() {
				if remove {
					if _, err := h.store.db.Exec("UPDATE service_jobs SET bound_node_id=NULL WHERE job_id=?", job.JobID); err != nil {
						t.Fatal(err)
					}
					commitReadTestRemoval(t, h.store, job.JobID)
				} else {
					h.store.serviceLogRetentionBytes = 1
					tx, err := h.store.db.BeginTx(t.Context(), nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					if _, err := h.store.enforceJobLogByteRetention(t.Context(), tx, job.JobID, h.clock.Now(), appendEvictionBudget()); err != nil {
						t.Fatal(err)
					}
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				}
			})
			// The injected writer's fsync is not projection cost.
			ctx = context.WithValue(ctx, readSnapshotHardLimitContextKey{}, 10*time.Second)
			page, err := h.store.GetJobLogs(ctx, job.JobID, "", 10)
			if err != nil {
				t.Fatal(err)
			}
			if !*fired {
				t.Fatal("log writer did not commit")
			}
			if !reflect.DeepEqual(page, before) {
				t.Fatalf("mixed log page and marker: %+v", page)
			}
			next, err := h.store.GetJobLogs(t.Context(), job.JobID, "", 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(next.Events) != 0 || !remove && next.Truncation == nil {
				t.Fatalf("retention/removal not committed: %+v", next)
			}
		})
	}
}
func TestJobReadSnapshotWriterHeldHTTP(t *testing.T) {
	h, client, service, _ := jobProjectionFixture(t, "claimed")
	one := h.submit(client, "writer-held-one", nil)
	tx, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// Exhaust legacy admission with the writer held. Views must use reserved
	// read capacity even when no main-pool connection is available.
	previousMaximum := h.store.db.Stats().MaxOpenConnections
	h.store.db.SetMaxOpenConns(1)
	defer h.store.db.SetMaxOpenConns(previousMaximum)
	if _, err := tx.Exec("UPDATE jobs SET updated_ns=updated_ns WHERE job_id=?", service.JobID); err != nil {
		t.Fatal(err)
	}
	// The writer stays held until every request answers; the timeout is only
	// a deadlock watchdog, not a machine-speed assertion.
	for _, path := range []string{"/v1/jobs/" + one.JobID, "/v1/jobs/" + service.JobID + "?class=service", "/v1/jobs?class=service", "/v1/jobs/" + service.JobID + "/children"} {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://control-plane"+path, nil)
		res, err := client.Do(req)
		if err != nil {
			cancel()
			t.Fatalf("writer held blocked %s: %v", path, err)
		}
		res.Body.Close()
		cancel()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("writer held %s status=%d", path, res.StatusCode)
		}
	}
}
func TestJobReadSnapshotCredentialRevalidation(t *testing.T) {
	for _, route := range []string{"detail", "listing", "children"} {
		t.Run(route, func(t *testing.T) {
			h, client, agent, node := credentialHarness(t)
			parent := h.submit(client, "read-parent", []string{"linux"})
			claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
			scope, err := h.store.ResolveAttemptCredential(t.Context(), claim.AttemptToken, node.NodeID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.db.Exec("UPDATE attempts SET lease_expires_ns=0 WHERE attempt_id=?", claim.Lease.AttemptID); err != nil {
				t.Fatal(err)
			}
			ctx := context.WithValue(t.Context(), attemptCredentialContextKey{}, scope)
			w := httptest.NewRecorder()
			r := jobReadRequest(ctx, http.MethodGet, "/v1/jobs", parent.JobID)
			switch route {
			case "detail":
				h.server.getAttemptScopedJob(w, r)
			case "listing":
				h.server.listJobs(w, r)
			case "children":
				h.server.listAttemptScopedChildJobs(w, r)
			}
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("stale authority survived snapshot: %d %s", w.Code, w.Body.String())
			}
		})
	}
}
func TestJobReadSnapshotPostChangeFailureCommitStands(t *testing.T) {
	h, _, job, _ := jobProjectionFixture(t, "claimed")
	if err := h.store.readDB.Close(); err != nil {
		t.Fatal(err)
	}
	r := jobReadRequest(t.Context(), http.MethodPost, "/v1/jobs/"+job.JobID+"/desired-state?class=service", job.JobID)
	r.Body = http.NoBody
	r = httptest.NewRequest(http.MethodPost, r.URL.String(), bytes.NewBufferString(`{"desired_state":"stopped"}`)).WithContext(r.Context())
	r.SetPathValue("job_id", job.JobID)
	w := httptest.NewRecorder()
	h.server.setServiceDesiredState(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-change failure=%d %s", w.Code, w.Body.String())
	}
	var envelope contract.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code != contract.ErrorUnavailable || envelope.Error.Retryable || envelope.Error.Details["read_reason"] != string(contract.ErrorInternal) || envelope.Error.Details["reason"] != "read_snapshot_post_change_failed" || envelope.Error.Details["mutation_applied"] != true {
		t.Fatalf("undefined post-change failure: %s", w.Body.String())
	}
	var desired string
	if err := h.store.db.QueryRow("SELECT desired_state FROM service_jobs WHERE job_id=?", job.JobID).Scan(&desired); err != nil || desired != "stopped" {
		t.Fatalf("commit did not stand: %s %v", desired, err)
	}
}

// Commit the production unbound-removal primitives at the interleaving, then
// checkpoint only after the reader exits. Awaiting TRUNCATE inside the reader
// would measure its 250 ms wait instead of the coherent response.
func commitReadTestRemoval(t *testing.T, s *Store, id string) {
	t.Helper()
	tx, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var key, hash string
	var spec []byte
	var created int64
	if err := tx.QueryRow("SELECT dispatch_key, request_hash, spec_json, created_ns FROM jobs WHERE job_id=?", id).Scan(&key, &hash, &spec, &created); err != nil {
		t.Fatal(err)
	}
	now := canonicalTime(s.clock.Now())
	clean, err := scrubSensitiveSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := scrubServiceControllerState(t.Context(), tx, id, clean, now); err != nil {
		t.Fatal(err)
	}
	if err := markSecretWALTruncationDue(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	tombstone := serviceTombstoneRow{jobID: id, dispatchKeyHash: hashDispatchKey(key), requestHash: hash, createdAt: time.Unix(0, created).UTC(), removalRequestedAt: now, removedAt: now, outcome: ServiceRemovalVerified, removalGeneration: InitialServiceRemovalGeneration}
	if err := insertServiceTombstone(t.Context(), tx, tombstone); err != nil {
		t.Fatal(err)
	}
	if err := deleteServiceRows(t.Context(), tx, id); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.truncateRemovalSecretWAL(context.Background()); err != nil {
			t.Error(err)
		}
	})
}

func TestJobReadSnapshotLedgerLookupBetweenReads(t *testing.T) {
	h, client, _, _ := credentialHarness(t)
	spec := validJobSpec("read-ledger-key", nil)
	job, _, err := h.store.CreateJobAs(t.Context(), spec, JobOrigin{OriginatingSubmitter: "ledger", SubmittedByRunLedger: true})
	if err != nil {
		t.Fatal(err)
	}
	_ = client
	ctx, fired := observeOnce(t, t.Context(), "ledger_key", func() {
		if _, err := h.store.db.Exec("DELETE FROM jobs WHERE job_id=?", job.JobID); err != nil {
			t.Fatal(err)
		}
	})
	// The injected writer's fsync is not projection cost.
	ctx = context.WithValue(ctx, readSnapshotHardLimitContextKey{}, 10*time.Second)
	got, err := h.store.LookupRunLedgerJob(ctx, spec.DispatchKey)
	if !*fired {
		t.Fatal("lookup deletion did not commit")
	}
	if err != nil || got.JobID != job.JobID {
		t.Fatalf("lookup stitched key and newer row: %+v %v", got, err)
	}
	_, err = h.store.LookupRunLedgerJob(t.Context(), spec.DispatchKey)
	if errorCode(err) != contract.ErrorNotFound {
		t.Fatalf("next lookup=%v", err)
	}
}

func TestJobReadSnapshotResultBetweenReads(t *testing.T) {
	h, _, agent, node, job, old := neverFixture(t, "process")
	if _, err := h.store.db.Exec(`INSERT INTO job_results(job_id,attempt_id,document,sha256,skip_reason,uploaded_ns) VALUES(?,?,?,?,'',?)`, job.JobID, old.Lease.AttemptID, []byte("old-result"), fmt.Sprintf("%064d", 1), h.clock.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec("UPDATE attempts SET state='lost' WHERE attempt_id=?", old.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec("UPDATE jobs SET state='queued' WHERE job_id=?", job.JobID); err != nil {
		t.Fatal(err)
	}
	ctx, fired := observeOnce(t, t.Context(), "job", func() { h.clock.Advance(time.Millisecond); claimClass(t, h, agent, node, contract.JobClassService) })
	// The injected writer's fsync is not projection cost.
	ctx = context.WithValue(ctx, readSnapshotHardLimitContextKey{}, 10*time.Second)
	w := httptest.NewRecorder()
	h.server.getJobResult(w, jobReadRequest(ctx, http.MethodGet, "/v1/jobs/"+job.JobID+"/result?class=service", job.JobID))
	if !*fired {
		t.Fatal("result interleaving not reached")
	}
	if w.Code != http.StatusOK {
		t.Fatalf("result changed moment: %d %s", w.Code, w.Body.String())
	}
	var got JobResult
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.AttemptID != old.Lease.AttemptID || string(got.Document) != "old-result" {
		t.Fatalf("mixed result: %+v", got)
	}
	_, err := h.store.GetJobResult(t.Context(), job.JobID)
	if errorCode(err) != contract.ErrorNotFound {
		t.Fatalf("new attempt inherited result: %v", err)
	}
}

func TestJobReadSnapshotPageClock(t *testing.T) {
	h, _, _, _ := jobProjectionFixture(t, "claimed")
	h.stopServer()
	calls := 0
	at := h.clock.Now()
	h.store.clock = ClockFunc(func() time.Time { calls++; return at })
	page, err := h.store.listReadableJobs(t.Context(), jobListFilters{}, "", 10)
	if err != nil || len(page.Jobs) != 1 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	if calls != 1 {
		t.Fatalf("page sampled %d clocks; want one", calls)
	}
}
