package l1

import (
	"bytes"
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

// Wire requests keep this regression runnable against the pre-key contract.
func instanceKeyRequest(t *testing.T, spec contract.JobSpec, key any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	var request map[string]any
	if err := json.Unmarshal(raw, &request); err != nil {
		t.Fatal(err)
	}
	request["instance_key"] = key
	return request
}

func TestInstanceKeyClientContract(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "app-a", Tags: []string{DefaultClientPrincipalTag}})
	spec := validJobSpec("instance-first", nil)
	request := instanceKeyRequest(t, spec, "Agent:One")
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", request)
	if status != http.StatusCreated {
		t.Fatalf("keyed submission = %d %s", status, body)
	}
	var first Job
	if err := json.Unmarshal(body, &first); err != nil {
		t.Fatal(err)
	}
	status, headers, body := h.do(client, http.MethodPost, "/v1/jobs", request)
	if status != http.StatusOK || headers.Get("Idempotent-Replay") != "true" {
		t.Fatalf("replay = %d %s", status, body)
	}
	request["dispatch_key"] = "instance-second"
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs", request)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorCode("instance_key_conflict"))
	var refusal contract.ErrorResponse
	if err := json.Unmarshal(body, &refusal); err != nil {
		t.Fatal(err)
	}
	if refusal.Error.Details["job_id"] != first.JobID {
		t.Fatalf("holder = %+v, want %s", refusal, first.JobID)
	}
	other := h.client(fabric.Identity{NodeID: "app-b", Tags: []string{DefaultClientPrincipalTag}})
	status, _, body = h.do(other, http.MethodPost, "/v1/jobs", request)
	if status != http.StatusCreated {
		t.Fatalf("other namespace = %d %s", status, body)
	}
	// Key comparison is exact, including case.
	request["dispatch_key"] = "instance-case"
	request["instance_key"] = "agent:one"
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs", request)
	if status != http.StatusCreated {
		t.Fatalf("case-distinct key = %d %s", status, body)
	}
}

func keyedSpec(t *testing.T, spec contract.JobSpec, key string) contract.JobSpec {
	t.Helper()
	raw, err := json.Marshal(instanceKeyRequest(t, spec, key))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	return spec
}

func assertInstanceConflict(t *testing.T, err error, holder string) {
	t.Helper()
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != contract.ErrorCode("instance_key_conflict") || refusal.Details["job_id"] != holder {
		t.Fatalf("instance conflict = %#v (%v), want holder %s", refusal, err, holder)
	}
}

func TestInstanceKeyConcurrentCreation(t *testing.T) {
	for _, sameDispatch := range []bool{false, true} {
		t.Run(fmt.Sprintf("same-dispatch-%t", sameDispatch), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "l1.sqlite")
			first, err := OpenStore(path, StoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			second, err := OpenStore(path, StoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			origin := JobOrigin{OriginatingSubmitter: "concurrent-app"}
			specs := make([]contract.JobSpec, 16)
			for i := range specs {
				dispatch := fmt.Sprintf("concurrent-%d", i)
				if sameDispatch {
					dispatch = "same-dispatch"
				}
				spec := validJobSpec(dispatch, nil)
				// Different classes compete in the same unique index.
				if !sameDispatch && i%2 == 0 {
					spec = removalServiceSpec(dispatch, nil)
				}
				specs[i] = keyedSpec(t, spec, "one-instance")
			}
			type result struct {
				job    Job
				replay bool
				err    error
			}
			results := make(chan result, len(specs))
			start := make(chan struct{})
			for i, spec := range specs {
				store := first
				if i%2 == 0 {
					store = second
				}
				go func() {
					<-start
					job, replay, err := store.CreateJobAs(t.Context(), spec, origin)
					results <- result{job, replay, err}
				}()
			}
			close(start)
			all := make([]result, 0, len(specs))
			holder := ""
			created := 0
			for range specs {
				r := <-results
				all = append(all, r)
				if r.err == nil && !r.replay {
					created++
					holder = r.job.JobID
				}
			}
			if created != 1 {
				t.Fatalf("created %d jobs: %+v", created, all)
			}
			for _, r := range all {
				if r.err == nil {
					if r.job.JobID != holder {
						t.Fatalf("returned a second job: %+v", r)
					}
				} else {
					if sameDispatch {
						t.Fatalf("identical replay failed: %v", r.err)
					}
					assertInstanceConflict(t, r.err, holder)
				}
			}
			var count int
			if err := first.db.QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("persisted jobs=%d %v", count, err)
			}
		})
	}
}

func TestInstanceKeyUpgradeAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "l1.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	legacy, _, err := store.CreateJobAs(t.Context(), validJobSpec("legacy-unkeyed", nil), JobOrigin{OriginatingSubmitter: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	// Rewind only the additive key schema, preserving all old rows and CHECKs.
	if _, err := db.Exec(`DROP INDEX IF EXISTS jobs_live_instance_key`); err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"instance_key", "instance_class", "instance_namespace"} {
		var exists int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('jobs') WHERE name=?`, column).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != 0 {
			if _, err := db.Exec(`ALTER TABLE jobs DROP COLUMN ` + column); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	upgraded, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if old, err := upgraded.GetJob(t.Context(), legacy.JobID); err != nil || old.Spec.DispatchKey != "legacy-unkeyed" {
		t.Fatalf("legacy row = %+v %v", old, err)
	}
	spec := keyedSpec(t, removalServiceSpec("upgraded-key", nil), "persisted")
	origin := JobOrigin{OriginatingSubmitter: "app"}
	holder, _, err := upgraded.CreateJobAs(t.Context(), spec, origin)
	if err != nil {
		t.Fatal(err)
	}
	// A raw insertion bypasses application preflight and must still lose inside SQL.
	_, err = upgraded.db.Exec(`INSERT INTO jobs(job_id,dispatch_key,request_hash,spec_json,state,originating_submitter,created_ns,updated_ns,instance_namespace,instance_key,instance_class)
 SELECT 'bypass-job','bypass-dispatch',request_hash,spec_json,state,originating_submitter,created_ns,updated_ns,instance_namespace,instance_key,instance_class FROM jobs WHERE job_id=?`, holder.JobID)
	if err == nil {
		t.Fatal("raw insertion bypassed instance-key uniqueness")
	}
	if err := upgraded.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	spec.DispatchKey = "after-reopen"
	_, _, err = reopened.CreateJobAs(t.Context(), spec, origin)
	assertInstanceConflict(t, err, holder.JobID)
}

func TestInstanceKeyTerminalReleaseAndPolicyStop(t *testing.T) {
	for _, outcome := range []string{"succeeded", "failed", "canceled"} {
		t.Run(outcome, func(t *testing.T) {
			h, client, agent, node := credentialHarness(t)
			request := instanceKeyRequest(t, validJobSpec("terminal-first", []string{"linux"}), "terminal")
			status, _, body := h.do(client, http.MethodPost, "/v1/jobs", request)
			if status != http.StatusCreated {
				t.Fatalf("create=%d %s", status, body)
			}
			holder := decodeJob(t, body)
			conflict := instanceKeyRequest(t, validJobSpec("terminal-next", []string{"linux"}), "terminal")
			if outcome == "canceled" {
				status, _, body = h.do(client, http.MethodPost, "/v1/jobs/"+holder.JobID+"/cancel", nil)
				if status != http.StatusOK || decodeJob(t, body).Outcome != contract.JobOutcomeCanceled {
					t.Fatalf("cancel=%d %s", status, body)
				}
			} else {
				claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
				status, _, body = h.do(client, http.MethodPost, "/v1/jobs", conflict)
				assertAPIError(t, status, body, http.StatusConflict, contract.ErrorCode("instance_key_conflict"))
				status, _, body = h.do(agent, http.MethodPost, fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/lease", holder.JobID, claim.Lease.AttemptID), RenewalRequest{FencingToken: claim.Lease.FencingToken})
				if status != http.StatusOK {
					t.Fatalf("start=%d %s", status, body)
				}
				status, _, body = h.do(client, http.MethodPost, "/v1/jobs", conflict)
				assertAPIError(t, status, body, http.StatusConflict, contract.ErrorCode("instance_key_conflict"))
				exit := 0
				if outcome == "failed" {
					exit = 7
				}
				status, _, body = h.do(agent, http.MethodPost, fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", holder.JobID, claim.Lease.AttemptID), CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "finish", Result: ProcessResult{ExitCode: &exit}})
				if status != http.StatusOK || string(decodeJob(t, body).State) != outcome {
					t.Fatalf("complete=%d %s", status, body)
				}
			}
			status, _, body = h.do(client, http.MethodPost, "/v1/jobs", conflict)
			if status != http.StatusCreated {
				t.Fatalf("terminal release=%d %s", status, body)
			}
			// Replay of the old dispatch wins even while a new job owns the key.
			status, headers, body := h.do(client, http.MethodPost, "/v1/jobs", request)
			if status != http.StatusOK || headers.Get("Idempotent-Replay") != "true" || decodeJob(t, body).JobID != holder.JobID {
				t.Fatalf("terminal replay=%d %s", status, body)
			}
		})
	}
	t.Run("policy-stopped-service", func(t *testing.T) {
		h, client, agent, node := credentialHarness(t)
		spec := removalServiceSpec("policy-holder", []string{"linux"})
		spec.Restart = "on-failure"
		request := instanceKeyRequest(t, spec, "policy")
		status, _, body := h.do(client, http.MethodPost, "/v1/jobs", request)
		if status != http.StatusCreated {
			t.Fatalf("submit=%d %s", status, body)
		}
		holder := decodeJob(t, body)
		claim := claimRestartService(t, h, agent, node)
		zero := 0
		status, _, body = h.do(agent, http.MethodPost, fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", holder.JobID, claim.Lease.AttemptID), CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "self-stop", Result: ProcessResult{ExitCode: &zero}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt})
		if status != http.StatusOK || decodeJob(t, body).State != contract.JobStopped || len(policyStopJSON(t, decodeJob(t, body))) == 0 {
			t.Fatalf("policy stop=%d %s", status, body)
		}
		request["dispatch_key"] = "policy-second"
		status, _, body = h.do(client, http.MethodPost, "/v1/jobs", request)
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorCode("instance_key_conflict"))
	})
}

func TestInstanceKeyAttemptCredentialRefusalAndDispatchPrecedence(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	parent := h.submit(client, "key-parent", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
	holderRequest := instanceKeyRequest(t, validJobSpec("root-key-holder", nil), "private-key")
	status, _, holderBody := h.do(client, http.MethodPost, "/v1/jobs", holderRequest)
	if status != http.StatusCreated {
		t.Fatalf("root holder=%d %s", status, holderBody)
	}
	holder := decodeJob(t, holderBody)
	request := instanceKeyRequest(t, validJobSpec("key-child", nil), "private-key")
	status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken, request)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorCode("instance_key_not_supported"))
	if bytes.Contains(body, []byte(parent.JobID)) || bytes.Contains(body, []byte(holder.JobID)) {
		t.Fatalf("refusal exposed a holder: %s", body)
	}
	// A different-body replay takes dispatch-key precedence over the unsupported key.
	status, body = h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken, validJobSpec("unkeyed-child", nil))
	if status != http.StatusCreated {
		t.Fatalf("unkeyed child=%d %s", status, body)
	}
	request["dispatch_key"] = "unkeyed-child"
	status, body = h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken, request)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorDispatchKeyConflict)
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs", request)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorDispatchKeyConflict)
}

func TestInstanceKeyServiceRemovalFinalizationAndTombstones(t *testing.T) {
	for _, ending := range []string{"verified", "forgotten", "stalled"} {
		t.Run(ending, func(t *testing.T) {
			h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{}, map[string]NodePolicy{"node-1": DefaultNodePolicy("service")}, false, time.Hour)
			clientIdentity := fabric.Identity{NodeID: "app", Tags: []string{DefaultClientPrincipalTag}}
			agentIdentity := fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}}
			client := h.client(clientIdentity)
			agent := h.client(agentIdentity)
			node := h.registerWithCapabilities(agent, "node-1", map[string]bool{"kind:process": true, "kind:oci": true, "runtime_handler:io.containerd.runc.v2": true})
			spec := removalServiceSpec("remove-key-first", []string{"service"})
			if ending == "stalled" {
				spec = stallOCIServiceSpec("remove-key-first")
			}
			request := instanceKeyRequest(t, spec, "removal-key")
			status, _, body := h.do(client, http.MethodPost, "/v1/jobs", request)
			if status != http.StatusCreated {
				t.Fatalf("create=%d %s", status, body)
			}
			holder := decodeJob(t, body)
			claimRestartService(t, h, agent, node)
			next := instanceKeyRequest(t, removalServiceSpec("remove-key-next", nil), "removal-key")
			assertHeld := func() {
				t.Helper()
				status, _, body := h.do(client, http.MethodPost, "/v1/jobs", next)
				assertAPIError(t, status, body, http.StatusConflict, contract.ErrorCode("instance_key_conflict"))
			}
			// A stopped service still owns its key and can be explicitly restarted.
			if ending == "verified" {
				status, _, body = h.do(client, http.MethodPut, serviceMutationPath(holder.JobID, "desired-state"), ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredStopped})
				if status != http.StatusAccepted {
					t.Fatalf("stop=%d %s", status, body)
				}
				assertHeld()
			}
			status, _, body = h.do(client, http.MethodPost, serviceMutationPath(holder.JobID, "remove"), nil)
			if status != http.StatusAccepted {
				t.Fatalf("remove=%d %s", status, body)
			}
			assertHeld()
			directives, err := h.store.ListNodeRemovalDirectives(t.Context(), "agent", node.NodeID, node.BootSessionID)
			if err != nil || len(directives) != 1 {
				t.Fatalf("directives=%+v %v", directives, err)
			}
			directive := directives[0]
			if ending == "forgotten" {
				status, _, body = h.do(client, http.MethodPost, serviceMutationPath(holder.JobID, "forget"), ForceForgetRequest{Force: true})
				if status != http.StatusOK || decodeJob(t, body).State != contract.JobForgottenCleanupUnverified {
					t.Fatalf("forget=%d %s", status, body)
				}
				assertHeld()
			}
			if ending == "stalled" {
				fixture := removalStallHarness{h: h, client: clientIdentity, agent: agentIdentity, node: node, job: holder, directive: directive}
				fixture.advancePastStallBound(t)
				stalled, err := h.store.AcknowledgeServiceRemoval(t.Context(), "agent", holder.JobID, fixture.declaration("key-stall"))
				if err != nil || stalled.State != contract.JobStalledCleanupUnverified {
					t.Fatalf("stall=%+v %v", stalled, err)
				}
				assertHeld()
			}
			ack := RemovalAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, RemovalGeneration: directive.RemovalGeneration, CleanupFence: directive.CleanupFence, RootInstanceID: directive.RootInstanceID, IdempotencyKey: "cleanup-key-holder"}
			cleaned, err := h.store.AcknowledgeServiceRemoval(t.Context(), "agent", holder.JobID, ack)
			if err != nil {
				t.Fatal(err)
			}
			if ending == "verified" && cleaned.State != contract.JobAgentCleaned {
				t.Fatalf("cleaned=%+v", cleaned)
			}
			assertHeld() // Agent-cleaned and acknowledged waiver/stall still keep the ordinary row.
			finalizeOrObserveRemoval(t, h.store, holder.JobID, func(j Job) bool { return j.Removal != nil && j.Removal.RemovedAt != nil })
			status, _, body = h.do(client, http.MethodPost, "/v1/jobs", next)
			if status != http.StatusCreated {
				t.Fatalf("finalization release=%d %s", status, body)
			}
			// Tombstone replay takes precedence over the replacement holder.
			status, headers, body := h.do(client, http.MethodPost, "/v1/jobs", request)
			if status != http.StatusOK || headers.Get("Idempotent-Replay") != "true" || decodeJob(t, body).JobID != holder.JobID {
				t.Fatalf("tombstone replay=%d %s", status, body)
			}
			request["instance_key"] = "changed-request"
			status, _, body = h.do(client, http.MethodPost, "/v1/jobs", request)
			assertAPIError(t, status, body, http.StatusConflict, contract.ErrorDispatchKeyConflict)
		})
	}
}

func TestInstanceKeyServiceKeepsReservationInEveryState(t *testing.T) {
	// Focused store regression: intermediate removal and failed states are not
	// all controllable through HTTP without an agent or a finalizer racing them.
	path := filepath.Join(t.TempDir(), "l1.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	origin := JobOrigin{OriginatingSubmitter: "app"}
	spec := keyedSpec(t, removalServiceSpec("state-holder", nil), "every-state")
	holder, _, err := store.CreateJobAs(t.Context(), spec, origin)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []contract.JobState{contract.JobQueued, contract.JobClaimed, contract.JobRunning, contract.JobStopping, contract.JobStopped, contract.JobFailed, contract.JobRemovalPending, contract.JobAgentCleaned, contract.JobForgottenCleanupUnverified, contract.JobStalledCleanupUnverified} {
		if _, err := store.db.Exec(`UPDATE jobs SET state=? WHERE job_id=?`, state, holder.JobID); err != nil {
			t.Fatal(err)
		}
		duplicate := spec
		duplicate.DispatchKey = "state-duplicate-" + string(state)
		_, _, err := store.CreateJobAs(t.Context(), duplicate, origin)
		assertInstanceConflict(t, err, holder.JobID)
	}
}

func TestInstanceKeyComputerRefused(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "app", Tags: []string{DefaultClientPrincipalTag}})
	spec := computerCapabilityJobSpec("computer-key")
	request := instanceKeyRequest(t, spec, "excluded")
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", request)
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	status, _, body = h.do(client, http.MethodPost, "/v1/computers", map[string]any{"name": "key-excluded", "spec": request})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	if !bytes.Contains(body, []byte("instance_key is forbidden for Computers")) {
		t.Fatalf("Computer refusal did not name the key: %s", body)
	}
}
