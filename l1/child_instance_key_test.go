package l1

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestChildInstanceKeySurvivesParentAttemptSupersession(t *testing.T) {
	for _, class := range []string{contract.JobClassOneShot, contract.JobClassService} {
		t.Run(class, func(t *testing.T) {
			h, client, agent, node := credentialHarness(t)
			parent := submitRestartService(t, h, client, "key-retry-parent", []string{"linux"}, nil)
			first := claimClass(t, h, agent, node, contract.JobClassService)
			spec := validJobSpec("key-earlier-child", []string{"child-only"})
			if class == contract.JobClassService {
				spec = removalServiceSpec(spec.DispatchKey, spec.RoutingTags)
			}
			request := instanceKeyRequest(t, spec, "shared-across-attempts")
			status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", first.AttemptToken, request)
			if status != http.StatusCreated {
				t.Fatalf("first child=%d %s", status, body)
			}
			holder := decodeJob(t, body)
			h.clock.Advance(2 * time.Minute)
			if _, err := h.store.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			h.clock.Advance(10 * time.Minute)
			node = h.register(agent, "node-1")
			if _, err := h.store.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			second := claimClass(t, h, agent, node, contract.JobClassService)
			if second.Job.JobID != parent.JobID || second.Lease.AttemptID == first.Lease.AttemptID {
				t.Fatalf("retry=%+v", second)
			}
			status, body = h.credentialRequest(agent, http.MethodPost, "/v1/jobs", second.AttemptToken, request)
			if status != http.StatusOK || decodeJob(t, body).JobID != holder.JobID {
				t.Fatalf("retry dispatch replay=%d %s", status, body)
			}
			request["dispatch_key"] = "key-later-child"
			status, body = h.credentialRequest(agent, http.MethodPost, "/v1/jobs", second.AttemptToken, request)
			assertAPIError(t, status, body, http.StatusConflict, contract.ErrorInstanceKeyConflict)
			var refusal contract.ErrorResponse
			if err := json.Unmarshal(body, &refusal); err != nil || refusal.Error.Details["job_id"] != holder.JobID {
				t.Fatalf("later attempt holder=%s %v", body, err)
			}
			status, body = h.credentialRequest(agent, http.MethodPost, "/v1/jobs", first.AttemptToken, request)
			assertAPIError(t, status, body, http.StatusUnauthorized, contract.ErrorUnauthorized)
		})
	}
}

// Resolve while live, then change authority before CreateJobAs opens its write
// transaction. A held key must not disclose its holder to that stale scope.
func TestChildInstanceKeyRevalidatesBeforeConflict(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	h.submit(client, "key-revalidation-parent", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
	scope, err := h.store.ResolveAttemptCredential(t.Context(), claim.AttemptToken, node.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	spec := keyedSpec(t, validJobSpec("key-revalidation-child", nil), "held")
	holder, _, err := h.store.CreateJobAs(t.Context(), spec, JobOrigin{Parent: &scope})
	if err != nil {
		t.Fatal(err)
	}
	h.clock.Advance(2 * time.Minute) // No reconcile: the transaction must enforce expiry itself.
	spec.DispatchKey = "key-stale-child"
	_, _, err = h.store.CreateJobAs(t.Context(), spec, JobOrigin{Parent: &scope})
	if errorCode(err) != contract.ErrorUnauthorized {
		t.Fatalf("stale scope=%v, want unauthorized", err)
	}
	page, err := h.store.ListChildJobs(t.Context(), scope.JobID, "", DefaultJobPageLimit)
	if err != nil || len(page.Jobs) != 1 || page.Jobs[0].JobID != holder.JobID {
		t.Fatalf("stale credential created a child: %+v %v", page, err)
	}
}

func TestChildInstanceKeyExpiryWhileWaitingForCreationTransaction(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	h.submit(client, "key-wait-parent", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
	scope, err := h.store.ResolveAttemptCredential(t.Context(), claim.AttemptToken, node.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	spec := keyedSpec(t, validJobSpec("key-wait-child", nil), "wait-key")
	if _, _, err := h.store.CreateJobAs(t.Context(), spec, JobOrigin{Parent: &scope}); err != nil {
		t.Fatal(err)
	}
	writer, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	spec.DispatchKey = "key-expired-while-waiting"
	done := make(chan error, 1)
	go func() {
		_, _, err := h.store.CreateJobAs(t.Context(), spec, JobOrigin{Parent: &scope})
		done <- err
	}()
	// The second checked-out connection is blocked in BEGIN IMMEDIATE, behind
	// writer. This establishes the interleaving without a timing-only sleep.
	deadline := time.Now().Add(time.Second)
	for h.store.db.Stats().InUse < 2 {
		if time.Now().After(deadline) {
			t.Fatal("child creation did not wait for the writer")
		}
		time.Sleep(time.Millisecond)
	}
	h.clock.Advance(2 * time.Minute)
	if err := writer.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; errorCode(err) != contract.ErrorUnauthorized {
		t.Fatalf("credential expired while waiting: %v, want unauthorized", err)
	}
}

func TestChildInstanceKeyConflictDisclosureUsesReplayScope(t *testing.T) {
	for _, corruptColumn := range []string{"parent_job_id", "originating_submitter"} {
		t.Run(corruptColumn, func(t *testing.T) {
			h, client, agent, node := credentialHarness(t)
			h.submit(client, "key-disclosure-parent", []string{"linux"})
			claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
			scope, err := h.store.ResolveAttemptCredential(t.Context(), claim.AttemptToken, node.NodeID)
			if err != nil {
				t.Fatal(err)
			}
			origin := JobOrigin{Parent: &scope}
			spec := keyedSpec(t, validJobSpec("key-disclosure-child", nil), "private")
			holder, _, err := h.store.CreateJobAs(t.Context(), spec, origin)
			if err != nil {
				t.Fatal(err)
			}
			// Simulate inconsistent recovery data; the namespace alone is never
			// sufficient authority to disclose the holder, including in fallback.
			if _, err := h.store.db.Exec("UPDATE jobs SET "+corruptColumn+"=? WHERE job_id=?", "out-of-scope", holder.JobID); err != nil {
				t.Fatal(err)
			}
			spec.DispatchKey = "key-disclosure-conflict"
			_, _, err = h.store.CreateJobAs(t.Context(), spec, origin)
			assertHidden := func(err error) {
				t.Helper()
				var refusal *Error
				if !errors.As(err, &refusal) || refusal.Code != contract.ErrorInstanceKeyConflict || refusal.Details["instance_key"] != "private" {
					t.Fatalf("conflict=%v", err)
				}
				if _, disclosed := refusal.Details["job_id"]; disclosed {
					t.Fatalf("out-of-scope holder disclosed: %+v", refusal)
				}
			}
			assertHidden(err)
			_, _, err = h.store.readConcurrentSubmit(t.Context(), spec, "unused", origin, errors.New("concurrent insert lost"))
			assertHidden(err)
			spec.DispatchKey = holder.Spec.DispatchKey
			_, _, err = h.store.CreateJobAs(t.Context(), spec, origin)
			if errorCode(err) != contract.ErrorDispatchKeyConflict {
				t.Fatalf("out-of-scope dispatch replay=%v", err)
			}
		})
	}
}
