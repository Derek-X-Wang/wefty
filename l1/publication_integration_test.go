package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type publicationFixture struct {
	h      *integrationHarness
	client *http.Client
	agent  *http.Client
	node   Node
	job    Job
	claim  Claim
}

func newPublicationFixture(t *testing.T, port *int) publicationFixture {
	t.Helper()
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "node-1")

	spec := validJobSpec("publication-contract", []string{"linux"})
	spec.Class = contract.JobClassService
	spec.Execution.HandoffDirectory = ""
	spec.Restart = contract.RestartAlways
	spec.PublishedPort = port
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("create service status = %d body=%s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	if port != nil && (job.Ready == nil || *job.Ready) {
		t.Fatalf("new portful service ready = %v, want false", job.Ready)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatal(err)
	}
	if _, exposed := wire["published_attempt_id"]; exposed {
		t.Fatalf("create response exposed internal publication marker: %s", body)
	}
	claim := claimClass(t, h, agent, node, contract.JobClassService)
	return publicationFixture{h: h, client: client, agent: agent, node: node, job: job, claim: claim}
}

func boolPointer(value bool) *bool { return &value }

func (fixture publicationFixture) path() string {
	return fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/publication", fixture.job.JobID, fixture.claim.Lease.AttemptID)
}

func (fixture publicationFixture) request(ready bool) PublicationRequest {
	return PublicationRequest{FencingToken: fixture.claim.Lease.FencingToken, Ready: boolPointer(ready)}
}

func TestAttemptPublicationFencedMutationAndTrueNoOps(t *testing.T) {
	assertAttemptPublicationFencedMutationAndTrueNoOps(t)
}

func assertAttemptPublicationFencedMutationAndTrueNoOps(t *testing.T) {
	t.Helper()
	port := 8080
	fixture := newPublicationFixture(t, &port)

	status, _, body := fixture.h.do(fixture.agent, http.MethodPost, fixture.path(), fixture.request(true))
	if status != http.StatusMethodNotAllowed {
		t.Fatalf("publication POST status = %d body=%s, want 405", status, body)
	}
	status, _, body = fixture.h.do(fixture.agent, http.MethodPut, fixture.path(), map[string]any{
		"fencing_token": fixture.claim.Lease.FencingToken,
	})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	status, _, body = fixture.h.do(fixture.agent, http.MethodPut, fixture.path(), map[string]any{
		"fencing_token":  fixture.claim.Lease.FencingToken,
		"ready":          true,
		"published_port": 9090,
	})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	status, _, body = fixture.h.do(fixture.agent, http.MethodPut, fixture.path(), PublicationRequest{
		FencingToken: fixture.claim.Lease.FencingToken + "-stale", Ready: boolPointer(true),
	})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorStaleFence)

	otherAgent := fixture.h.client(fabric.Identity{NodeID: "node-2", Tags: []string{DefaultAgentPrincipalTag}})
	status, _, body = fixture.h.do(otherAgent, http.MethodPut, fixture.path(), fixture.request(true))
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorAttemptNotOwned)

	if _, err := fixture.h.store.db.Exec(`CREATE TABLE publication_update_audit (updates INTEGER NOT NULL);
		INSERT INTO publication_update_audit(updates) VALUES(0);
		CREATE TRIGGER audit_service_publication_update AFTER UPDATE ON service_jobs
		BEGIN UPDATE publication_update_audit SET updates=updates+1; END;`); err != nil {
		t.Fatal(err)
	}
	fixture.h.clock.Advance(time.Second)
	status, _, body = fixture.h.do(fixture.agent, http.MethodPut, fixture.path(), fixture.request(true))
	if status != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", status, body)
	}
	var published Job
	if err := json.Unmarshal(body, &published); err != nil {
		t.Fatal(err)
	}
	if published.Ready == nil || !*published.Ready {
		t.Fatalf("published ready = %v, want true", published.Ready)
	}
	if published.PublishedPort == nil || *published.PublishedPort != port ||
		published.Spec.PublishedPort == nil || *published.Spec.PublishedPort != port {
		t.Fatalf("published ports = projection %v spec %v, want immutable %d", published.PublishedPort, published.Spec.PublishedPort, port)
	}
	var marker sql.NullString
	var healthySince, jobUpdated int64
	if err := fixture.h.store.db.QueryRow(`SELECT service_jobs.published_attempt_id,
		service_jobs.healthy_since_ns, jobs.updated_ns
		FROM service_jobs JOIN jobs ON jobs.job_id=service_jobs.job_id
		WHERE service_jobs.job_id=?`, fixture.job.JobID).Scan(&marker, &healthySince, &jobUpdated); err != nil {
		t.Fatal(err)
	}
	if !marker.Valid || marker.String != fixture.claim.Lease.AttemptID || healthySince != fixture.h.clock.Now().UnixNano() {
		t.Fatalf("stored publication = marker %v healthy %d, want %q/%d", marker, healthySince, fixture.claim.Lease.AttemptID, fixture.h.clock.Now().UnixNano())
	}

	var writesBefore int
	if err := fixture.h.store.db.QueryRow("SELECT updates FROM publication_update_audit").Scan(&writesBefore); err != nil {
		t.Fatal(err)
	}
	fixture.h.clock.Advance(time.Second)
	status, _, body = fixture.h.do(fixture.agent, http.MethodPut, fixture.path(), fixture.request(true))
	if status != http.StatusOK {
		t.Fatalf("same-state publish status = %d body=%s", status, body)
	}
	assertPublicationStorageUnchanged(t, fixture.h, fixture.job.JobID, writesBefore, jobUpdated, healthySince)

	status, _, body = fixture.h.do(fixture.agent, http.MethodPut, fixture.path(), fixture.request(false))
	if status != http.StatusOK {
		t.Fatalf("withdraw status = %d body=%s", status, body)
	}
	var withdrawn Job
	if err := json.Unmarshal(body, &withdrawn); err != nil {
		t.Fatal(err)
	}
	if withdrawn.Ready == nil || *withdrawn.Ready {
		t.Fatalf("withdrawn ready = %v, want false", withdrawn.Ready)
	}
	var withdrawnUpdated int64
	if err := fixture.h.store.db.QueryRow(`SELECT jobs.updated_ns FROM jobs WHERE job_id=?`, fixture.job.JobID).Scan(&withdrawnUpdated); err != nil {
		t.Fatal(err)
	}
	if withdrawnUpdated != fixture.h.clock.Now().UnixNano() || withdrawnUpdated == jobUpdated {
		t.Fatalf("withdraw updated_ns = %d, want changed to %d", withdrawnUpdated, fixture.h.clock.Now().UnixNano())
	}
	if err := fixture.h.store.db.QueryRow("SELECT updates FROM publication_update_audit").Scan(&writesBefore); err != nil {
		t.Fatal(err)
	}
	fixture.h.clock.Advance(time.Second)
	status, _, body = fixture.h.do(fixture.agent, http.MethodPut, fixture.path(), fixture.request(false))
	if status != http.StatusOK {
		t.Fatalf("same-state withdraw status = %d body=%s", status, body)
	}
	assertPublicationStorageUnchanged(t, fixture.h, fixture.job.JobID, writesBefore, withdrawnUpdated, 0)
}

func assertPublicationStorageUnchanged(t *testing.T, h *integrationHarness, jobID string, wantWrites int, wantUpdated, wantHealthy int64) {
	t.Helper()
	var writes int
	if err := h.store.db.QueryRow("SELECT updates FROM publication_update_audit").Scan(&writes); err != nil {
		t.Fatal(err)
	}
	var marker sql.NullString
	var healthy sql.NullInt64
	var updated int64
	if err := h.store.db.QueryRow(`SELECT service_jobs.published_attempt_id,
		service_jobs.healthy_since_ns, jobs.updated_ns
		FROM service_jobs JOIN jobs ON jobs.job_id=service_jobs.job_id
		WHERE service_jobs.job_id=?`, jobID).Scan(&marker, &healthy, &updated); err != nil {
		t.Fatal(err)
	}
	if writes != wantWrites || updated != wantUpdated {
		t.Fatalf("same-state write count/updated_ns = %d/%d, want %d/%d", writes, updated, wantWrites, wantUpdated)
	}
	if wantHealthy == 0 {
		if healthy.Valid || marker.Valid {
			t.Fatalf("withdrawn same-state storage = marker %v healthy %v, want both NULL", marker, healthy)
		}
	} else if !healthy.Valid || healthy.Int64 != wantHealthy || !marker.Valid {
		t.Fatalf("published same-state storage = marker %v healthy %v, want marker and %d", marker, healthy, wantHealthy)
	}
}

func TestAttemptPublicationRejectsPortlessAndOmitsReadiness(t *testing.T) {
	assertAttemptPublicationRejectsPortlessAndOmitsReadiness(t)
}

func assertAttemptPublicationRejectsPortlessAndOmitsReadiness(t *testing.T) {
	t.Helper()
	fixture := newPublicationFixture(t, nil)
	status, _, body := fixture.h.do(fixture.client, http.MethodGet, "/v1/jobs/"+fixture.job.JobID, nil)
	if status != http.StatusOK {
		t.Fatalf("get portless service status = %d body=%s", status, body)
	}
	var portless Job
	if err := json.Unmarshal(body, &portless); err != nil {
		t.Fatal(err)
	}
	if portless.PublishedPort != nil || portless.Ready != nil {
		t.Fatalf("portless publication projection = port %v ready %v, want nil/nil", portless.PublishedPort, portless.Ready)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatal(err)
	}
	if _, exists := wire["ready"]; exists {
		t.Fatalf("portless service emitted ready instead of omitting it: %s", body)
	}

	var updatedBefore int64
	if err := fixture.h.store.db.QueryRow("SELECT updated_ns FROM jobs WHERE job_id=?", fixture.job.JobID).Scan(&updatedBefore); err != nil {
		t.Fatal(err)
	}
	fixture.h.clock.Advance(time.Second)
	status, _, body = fixture.h.do(fixture.agent, http.MethodPut, fixture.path(), fixture.request(true))
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorConflict)
	var updatedAfter int64
	var marker sql.NullString
	if err := fixture.h.store.db.QueryRow(`SELECT jobs.updated_ns, service_jobs.published_attempt_id
		FROM jobs JOIN service_jobs ON service_jobs.job_id=jobs.job_id WHERE jobs.job_id=?`, fixture.job.JobID).
		Scan(&updatedAfter, &marker); err != nil {
		t.Fatal(err)
	}
	if updatedAfter != updatedBefore || marker.Valid {
		t.Fatalf("portless rejection mutated updated/marker = %d/%v, want %d/NULL", updatedAfter, marker, updatedBefore)
	}
}

func TestAttemptPublicationClearsAcrossLifecycleAndRejectsTerminalReplay(t *testing.T) {
	assertAttemptPublicationClearsAcrossLifecycleAndRejectsTerminalReplay(t)
}

func assertAttemptPublicationClearsAcrossLifecycleAndRejectsTerminalReplay(t *testing.T) {
	t.Helper()
	t.Run("completion and stale replay", func(t *testing.T) {
		port := 8081
		fixture := newPublicationFixture(t, &port)
		publishFixture(t, fixture)
		exitCode := 1
		completionPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", fixture.job.JobID, fixture.claim.Lease.AttemptID)
		status, _, body := fixture.h.do(fixture.agent, http.MethodPost, completionPath, CompletionRequest{
			FencingToken:   fixture.claim.Lease.FencingToken,
			IdempotencyKey: "publication-completion",
			Result:         ProcessResult{ExitCode: &exitCode},
		})
		if status != http.StatusOK {
			t.Fatalf("complete published attempt status = %d body=%s", status, body)
		}
		assertUnpublishedJob(t, fixture.h, fixture.client, fixture.job.JobID)

		status, _, body = fixture.h.do(fixture.agent, http.MethodPut, fixture.path(), fixture.request(true))
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorConflict)
		assertUnpublishedJob(t, fixture.h, fixture.client, fixture.job.JobID)

		fixture.h.clock.Advance(30 * time.Second)
		freshClaim := claimClass(t, fixture.h, fixture.agent, fixture.node, contract.JobClassService)
		status, _, body = fixture.h.do(fixture.agent, http.MethodPut, fixture.path(), PublicationRequest{
			FencingToken: fixture.claim.Lease.FencingToken + "-also-stale", Ready: boolPointer(true),
		})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorAttemptMismatch)
		freshFixture := fixture
		freshFixture.claim = freshClaim
		publishFixture(t, freshFixture)
		tx, err := fixture.h.store.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := transitionServiceJob(context.Background(), tx, fixture.job.JobID, contract.ServiceDesiredStopped, contract.JobStopping, fixture.h.clock.Now()); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		assertUnpublishedJob(t, fixture.h, fixture.client, fixture.job.JobID)
	})

	t.Run("lease expiry", func(t *testing.T) {
		port := 8082
		fixture := newPublicationFixture(t, &port)
		publishFixture(t, fixture)
		fixture.h.clock.Advance(30 * time.Second)
		status, _, body := fixture.h.do(fixture.agent, http.MethodPut, fixture.path(), fixture.request(true))
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorLeaseExpired)
		assertUnpublishedJob(t, fixture.h, fixture.client, fixture.job.JobID)
	})
}

func publishFixture(t *testing.T, fixture publicationFixture) Job {
	t.Helper()
	fixture.h.clock.Advance(time.Millisecond)
	status, _, body := fixture.h.do(fixture.agent, http.MethodPut, fixture.path(), fixture.request(true))
	if status != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	if job.Ready == nil || !*job.Ready {
		t.Fatalf("published ready = %v, want true", job.Ready)
	}
	return job
}

func assertUnpublishedJob(t *testing.T, h *integrationHarness, client *http.Client, jobID string) {
	t.Helper()
	status, _, body := h.do(client, http.MethodGet, "/v1/jobs/"+jobID, nil)
	if status != http.StatusOK {
		t.Fatalf("get unpublished job status = %d body=%s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	if job.Ready == nil || *job.Ready {
		t.Fatalf("unpublished ready = %v, want false", job.Ready)
	}
	var marker sql.NullString
	if err := h.store.db.QueryRow("SELECT published_attempt_id FROM service_jobs WHERE job_id=?", jobID).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	if marker.Valid {
		t.Fatalf("unpublished marker = %q, want NULL", marker.String)
	}
}

func TestOperatorReadyRequiresCurrentActiveAuthority(t *testing.T) {
	assertOperatorReadyRequiresCurrentActiveAuthority(t)
}

func assertOperatorReadyRequiresCurrentActiveAuthority(t *testing.T) {
	t.Helper()
	tests := []struct {
		name   string
		mutate func(*testing.T, publicationFixture)
	}{
		{name: "current attempt", mutate: func(t *testing.T, fixture publicationFixture) {
			_, err := fixture.h.store.db.Exec("UPDATE jobs SET current_attempt_id=NULL WHERE job_id=?", fixture.job.JobID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "active attempt", mutate: func(t *testing.T, fixture publicationFixture) {
			_, err := fixture.h.store.db.Exec("UPDATE attempts SET state=? WHERE attempt_id=?", contract.AttemptFailed, fixture.claim.Lease.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unexpired lease", mutate: func(t *testing.T, fixture publicationFixture) {
			_, err := fixture.h.store.db.Exec("UPDATE attempts SET lease_expires_ns=? WHERE attempt_id=?", fixture.h.clock.Now().UnixNano(), fixture.claim.Lease.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "current authority generation", mutate: func(t *testing.T, fixture publicationFixture) {
			_, err := fixture.h.store.db.Exec("UPDATE nodes SET authority_generation=authority_generation+1 WHERE node_id=?", "node-1")
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "active job", mutate: func(t *testing.T, fixture publicationFixture) {
			_, err := fixture.h.store.db.Exec("UPDATE jobs SET state=? WHERE job_id=?", contract.JobQueued, fixture.job.JobID)
			if err != nil {
				t.Fatal(err)
			}
		}},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			port := 8100 + index
			fixture := newPublicationFixture(t, &port)
			publishFixture(t, fixture)
			test.mutate(t, fixture)
			status, _, body := fixture.h.do(fixture.client, http.MethodGet, "/v1/jobs/"+fixture.job.JobID, nil)
			if status != http.StatusOK {
				t.Fatalf("get job status = %d body=%s", status, body)
			}
			var job Job
			if err := json.Unmarshal(body, &job); err != nil {
				t.Fatal(err)
			}
			if job.Ready == nil || *job.Ready {
				t.Fatalf("computed ready after losing %s = %v, want false", test.name, job.Ready)
			}
		})
	}
}
