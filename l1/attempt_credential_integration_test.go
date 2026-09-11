package l1

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// credentialRequest is the in-job caller's shape: an ordinary request over the
// holding node's Fabric connection, plus the bearer.
func (h *integrationHarness) credentialRequest(client *http.Client, method, path, token string, body any) (int, []byte) {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			h.t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, "http://control-plane.invalid"+path, reader)
	if err != nil {
		h.t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(request)
	if err != nil {
		h.t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	return response.StatusCode, responseBody
}

func credentialHarness(t *testing.T) (*integrationHarness, *http.Client, *http.Client, Node) {
	t.Helper()
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "submitter", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "node-1")
	return h, client, agent, node
}

func decodeJob(t *testing.T, body []byte) Job {
	t.Helper()
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatalf("decode job: %v body=%s", err, body)
	}
	return job
}

// Acceptance (a), L1 half: a one-shot job submitted directly to L1 with no L3
// spawns a child and reads it back with nothing but the injected credential.
func TestAttemptCredentialSpawnsAndReadsOwnChildren(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	parent := h.submit(client, "credential-parent", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
	if claim.Job.JobID != parent.JobID {
		t.Fatalf("claimed job = %q, want %q", claim.Job.JobID, parent.JobID)
	}
	if claim.AttemptToken == "" {
		t.Fatal("claim carried no attempt credential")
	}

	status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken,
		validJobSpec("credential-child", []string{"linux"}))
	if status != http.StatusCreated {
		t.Fatalf("child submit status = %d body=%s", status, body)
	}
	child := decodeJob(t, body)
	if child.ParentJobID != parent.JobID || child.ParentAttemptID != claim.Lease.AttemptID {
		t.Fatalf("child parentage = job:%q attempt:%q, want job:%q attempt:%q",
			child.ParentJobID, child.ParentAttemptID, parent.JobID, claim.Lease.AttemptID)
	}
	if child.OriginatingSubmitter != "submitter" || child.SpawnDepth != 1 {
		t.Fatalf("child delegation = submitter:%q depth:%d, want submitter:%q depth:1",
			child.OriginatingSubmitter, child.SpawnDepth, "submitter")
	}

	status, body = h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+child.JobID, claim.AttemptToken, nil)
	if status != http.StatusOK {
		t.Fatalf("child read status = %d body=%s", status, body)
	}
	if read := decodeJob(t, body); read.JobID != child.JobID {
		t.Fatalf("child read = %q, want %q", read.JobID, child.JobID)
	}

	status, body = h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+parent.JobID, claim.AttemptToken, nil)
	if status != http.StatusOK {
		t.Fatalf("own job read status = %d body=%s", status, body)
	}

	status, body = h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+parent.JobID+"/children", claim.AttemptToken, nil)
	if status != http.StatusOK {
		t.Fatalf("children status = %d body=%s", status, body)
	}
	var page JobList
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 1 || page.Jobs[0].JobID != child.JobID {
		t.Fatalf("children = %#v, want only %q", page.Jobs, child.JobID)
	}
}

// A one-shot credential dies with its lease and nothing revives it.
func TestAttemptCredentialIsRefusedAfterLeaseLoss(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	h.submit(client, "credential-lease-loss", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)

	status, body := h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+claim.Job.JobID, claim.AttemptToken, nil)
	if status != http.StatusOK {
		t.Fatalf("read while live status = %d body=%s", status, body)
	}

	// Lose the lease, then let the ordinary reconcile pass observe it.
	h.clock.Advance(2 * time.Minute)
	if _, err := h.store.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, probe := range []struct {
		name   string
		method string
		path   string
		body   any
	}{
		{"read own job", http.MethodGet, "/v1/jobs/" + claim.Job.JobID, nil},
		{"list children", http.MethodGet, "/v1/jobs/" + claim.Job.JobID + "/children", nil},
		{"submit child", http.MethodPost, "/v1/jobs", validJobSpec("credential-after-loss", []string{"linux"})},
	} {
		status, body := h.credentialRequest(agent, probe.method, probe.path, claim.AttemptToken, probe.body)
		if status != http.StatusUnauthorized {
			t.Fatalf("%s after lease loss status = %d body=%s, want %d", probe.name, status, body, http.StatusUnauthorized)
		}
	}
}

// Children are job-level, so the next attempt of a retried service job
// inherits them while the superseded attempt's credential stops working.
func TestAttemptCredentialSupersessionKeepsJobLevelChildren(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	service := submitRestartService(t, h, client, "credential-retried-service", []string{"linux"}, nil)
	first := claimClass(t, h, agent, node, contract.JobClassService)
	if first.Job.JobID != service.JobID {
		t.Fatalf("claimed job = %q, want the service %q", first.Job.JobID, service.JobID)
	}

	status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", first.AttemptToken,
		validJobSpec("credential-first-child", []string{"linux"}))
	if status != http.StatusCreated {
		t.Fatalf("first child status = %d body=%s", status, body)
	}
	child := decodeJob(t, body)

	// Lose the lease; a desired-running service requeues and is claimed again.
	// Let the restart backoff elapse before refreshing the node, so the node is
	// alive at claim time rather than freshly starved of heartbeats.
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
	if second.Job.JobID != service.JobID || second.Lease.AttemptID == first.Lease.AttemptID {
		t.Fatalf("retry claim = job:%q attempt:%q, want a fresh attempt of %q",
			second.Job.JobID, second.Lease.AttemptID, service.JobID)
	}
	if second.AttemptToken == first.AttemptToken || second.AttemptToken == "" {
		t.Fatal("a fresh claim did not mint a fresh attempt credential")
	}

	status, body = h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+service.JobID+"?class=service",
		first.AttemptToken, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("superseded credential status = %d body=%s, want %d", status, body, http.StatusUnauthorized)
	}

	status, body = h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+service.JobID+"/children",
		second.AttemptToken, nil)
	if status != http.StatusOK {
		t.Fatalf("retried children status = %d body=%s", status, body)
	}
	var page JobList
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 1 || page.Jobs[0].JobID != child.JobID {
		t.Fatalf("retried attempt children = %#v, want the earlier attempt's child %q", page.Jobs, child.JobID)
	}
}

// The credential dies with the attempt's authority, not merely with the
// wall-clock lease. These two predicates have no wall-clock component at all.
func TestAttemptCredentialIsRefusedAfterAuthorityLossWithALiveLease(t *testing.T) {
	t.Run("replaced node registration", func(t *testing.T) {
		h, client, agent, node := credentialHarness(t)
		h.submit(client, "credential-replaced-session", []string{"linux"})
		claim := claimClass(t, h, agent, node, contract.JobClassOneShot)

		// A fresh boot session for the same stable node, while the lease is
		// still comfortably live.
		registration := contract.NodeRegistration{
			NodeID: node.NodeID, BootSessionID: "boot-replacement", RootInstanceID: "root-" + node.NodeID,
			OS: "linux", Architecture: "arm64", AgentVersion: "test",
			Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1,
			CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
		}
		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration)
		if status != http.StatusOK {
			t.Fatalf("re-register status = %d body=%s", status, body)
		}
		if !claim.Lease.LeaseExpires.After(h.clock.Now()) {
			t.Fatal("test needs the original lease to still be unexpired")
		}

		status, body = h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+claim.Job.JobID, claim.AttemptToken, nil)
		if status != http.StatusUnauthorized {
			t.Fatalf("replaced-session credential status = %d body=%s, want %d", status, body, http.StatusUnauthorized)
		}
	})

	t.Run("terminal attempt", func(t *testing.T) {
		h, client, agent, node := credentialHarness(t)
		h.submit(client, "credential-terminal-attempt", []string{"linux"})
		claim := claimClass(t, h, agent, node, contract.JobClassOneShot)

		exitCode := 0
		if _, err := h.store.CompleteAttempt(t.Context(), node.NodeID, claim.Job.JobID, claim.Lease.AttemptID,
			CompletionRequest{
				FencingToken: claim.Lease.FencingToken, IdempotencyKey: "credential-terminal-completion",
				Result: ProcessResult{ExitCode: &exitCode},
			}); err != nil {
			t.Fatal(err)
		}
		if !claim.Lease.LeaseExpires.After(h.clock.Now()) {
			t.Fatal("test needs the lease to still be unexpired after completion")
		}

		status, body := h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+claim.Job.JobID, claim.AttemptToken, nil)
		if status != http.StatusUnauthorized {
			t.Fatalf("terminal-attempt credential status = %d body=%s, want %d", status, body, http.StatusUnauthorized)
		}
	})
}

// Each liveness predicate is pinned directly, so a future refactor cannot drop
// one and still look correct through the routes above.
func TestAttemptCredentialAuthorityPredicates(t *testing.T) {
	now := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	scope := AttemptCredentialScope{AttemptID: "attempt-1", JobID: "job-1", NodeID: "node-1"}
	live := func() attemptAuthority {
		return attemptAuthority{
			attemptID: "attempt-1", jobID: "job-1", identityNodeID: "node-1",
			bootSessionID: "boot-1", currentBootSessionID: "boot-1",
			authorityGeneration: 3, currentAuthorityGeneration: 3,
			state:          contract.AttemptRunning,
			currentAttempt: sql.NullString{String: "attempt-1", Valid: true},
			leaseExpires:   now.Add(30 * time.Second),
		}
	}
	if err := validateAttemptCredentialAuthority("node-1", scope, live(), now.UnixNano()); err != nil {
		t.Fatalf("live attempt = %v, want authorized", err)
	}

	for _, probe := range []struct {
		name    string
		mutate  func(*attemptAuthority)
		node    string
		wantErr contract.ErrorCode
	}{
		{"another node", func(*attemptAuthority) {}, "node-2", contract.ErrorForbidden},
		{"different job", func(a *attemptAuthority) { a.jobID = "job-2" }, "node-1", contract.ErrorUnauthorized},
		{"superseded attempt", func(a *attemptAuthority) {
			a.currentAttempt = sql.NullString{String: "attempt-2", Valid: true}
		}, "node-1", contract.ErrorUnauthorized},
		{"no current attempt", func(a *attemptAuthority) {
			a.currentAttempt = sql.NullString{}
		}, "node-1", contract.ErrorUnauthorized},
		{"replaced boot session", func(a *attemptAuthority) { a.currentBootSessionID = "boot-2" }, "node-1", contract.ErrorUnauthorized},
		{"advanced authority generation", func(a *attemptAuthority) { a.currentAuthorityGeneration = 4 }, "node-1", contract.ErrorUnauthorized},
		{"terminal attempt with a live lease", func(a *attemptAuthority) {
			a.state = contract.AttemptSucceeded
		}, "node-1", contract.ErrorUnauthorized},
		{"lost attempt with a live lease", func(a *attemptAuthority) {
			a.state = contract.AttemptLost
		}, "node-1", contract.ErrorUnauthorized},
		{"expired lease", func(a *attemptAuthority) { a.leaseExpires = now }, "node-1", contract.ErrorUnauthorized},
	} {
		authority := live()
		probe.mutate(&authority)
		err := validateAttemptCredentialAuthority(probe.node, scope, authority, now.UnixNano())
		if errorCode(err) != probe.wantErr {
			t.Errorf("%s = %v (code %s), want %s", probe.name, err, errorCode(err), probe.wantErr)
		}
	}
}

// Authorization happens before CreateJobAs opens its transaction, so the
// window between them must not be able to persist a child. The store is
// driven directly with an already-resolved scope, which is exactly the state
// that window produces; there is no "pause mid-transaction" seam in the
// harness, so this reproduces the end state rather than the interleaving.
func TestAttemptCredentialRevalidatesInsideTheWriteTransaction(t *testing.T) {
	for _, probe := range []struct {
		name          string
		loseAuthority func(*integrationHarness, Node, *Claim)
	}{
		{"attempt completed after authorization", func(h *integrationHarness, node Node, claim *Claim) {
			exitCode := 0
			if _, err := h.store.CompleteAttempt(t.Context(), node.NodeID, claim.Job.JobID, claim.Lease.AttemptID,
				CompletionRequest{
					FencingToken: claim.Lease.FencingToken, IdempotencyKey: "toctou-completion",
					Result: ProcessResult{ExitCode: &exitCode},
				}); err != nil {
				t.Fatal(err)
			}
		}},
		{"lease lost after authorization", func(h *integrationHarness, _ Node, _ *Claim) {
			h.clock.Advance(2 * time.Minute)
			if _, err := h.store.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(probe.name, func(t *testing.T) {
			h, client, agent, node := credentialHarness(t)
			h.submit(client, "toctou-parent", []string{"linux"})
			claim := claimClass(t, h, agent, node, contract.JobClassOneShot)

			// Resolve the credential while it is still live: this is what the
			// HTTP layer hands to CreateJobAs.
			scope, err := h.store.ResolveAttemptCredential(t.Context(), claim.AttemptToken, node.NodeID)
			if err != nil {
				t.Fatalf("resolve live credential: %v", err)
			}
			probe.loseAuthority(h, node, &claim)

			_, _, err = h.store.CreateJobAs(t.Context(),
				validJobSpec("toctou-child", []string{"linux"}), JobOrigin{Parent: &scope})
			if errorCode(err) != contract.ErrorUnauthorized {
				t.Fatalf("create with a stale credential = %v (code %s), want %s",
					err, errorCode(err), contract.ErrorUnauthorized)
			}
			// The refusal must happen before any row is written.
			page, err := h.store.ListChildJobs(t.Context(), claim.Job.JobID, "", DefaultJobPageLimit)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Jobs) != 0 {
				t.Fatalf("a child was persisted despite lost authority: %#v", page.Jobs)
			}
			var consumed int
			if err := h.store.db.QueryRowContext(t.Context(),
				"SELECT COUNT(*) FROM jobs WHERE dispatch_key=?", "toctou-child").Scan(&consumed); err != nil {
				t.Fatal(err)
			}
			if consumed != 0 {
				t.Fatal("the refused child's dispatch key was consumed")
			}
		})
	}
}

// Parentage alone is not identity. A child always inherits its root submitter,
// so a row whose submitter diverges is not one this credential may replay.
func TestReplayScopeRequiresBothParentAndSubmitter(t *testing.T) {
	parent := AttemptCredentialScope{JobID: "job-parent", OriginatingSubmitter: "submitter-a"}
	for _, probe := range []struct {
		name     string
		origin   JobOrigin
		replayed Job
		want     bool
	}{
		{"client principal is unrestricted", JobOrigin{}, Job{ParentJobID: "anything"}, true},
		{"own child", JobOrigin{Parent: &parent},
			Job{ParentJobID: "job-parent", OriginatingSubmitter: "submitter-a"}, true},
		{"same parent, different submitter", JobOrigin{Parent: &parent},
			Job{ParentJobID: "job-parent", OriginatingSubmitter: "submitter-b"}, false},
		{"another parent's child", JobOrigin{Parent: &parent},
			Job{ParentJobID: "job-other", OriginatingSubmitter: "submitter-a"}, false},
		{"a root job", JobOrigin{Parent: &parent},
			Job{ParentJobID: "", OriginatingSubmitter: "submitter-a"}, false},
	} {
		if got := replayWithinScope(probe.origin, probe.replayed); got != probe.want {
			t.Errorf("%s = %t, want %t", probe.name, got, probe.want)
		}
	}
}

// Reads obey the ordinary class-selector rule, not a credential-specific one.
func TestAttemptCredentialReadsFollowTheClassSelectorRule(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	service := submitRestartService(t, h, client, "class-rule-service", []string{"linux"}, nil)
	claim := claimClass(t, h, agent, node, contract.JobClassService)
	if claim.Job.JobID != service.JobID {
		t.Fatalf("claimed %q, want the service %q", claim.Job.JobID, service.JobID)
	}
	status, body := h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+service.JobID, claim.AttemptToken, nil)
	if status != http.StatusBadRequest {
		t.Fatalf("unscoped service read status = %d body=%s, want %d", status, body, http.StatusBadRequest)
	}
	status, body = h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+service.JobID+"?class=service", claim.AttemptToken, nil)
	if status != http.StatusOK {
		t.Fatalf("class-scoped service read status = %d body=%s", status, body)
	}

	// A one-shot child is the mirror image: no selector, and a selector is a
	// not-found rather than a different job.
	status, body = h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken,
		validJobSpec("class-rule-child", []string{"linux"}))
	if status != http.StatusCreated {
		t.Fatalf("child submit status = %d body=%s", status, body)
	}
	child := decodeJob(t, body)
	status, body = h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+child.JobID, claim.AttemptToken, nil)
	if status != http.StatusOK {
		t.Fatalf("unscoped one-shot child read status = %d body=%s", status, body)
	}
	status, body = h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+child.JobID+"?class=service", claim.AttemptToken, nil)
	if status != http.StatusNotFound {
		t.Fatalf("class-scoped one-shot child read status = %d body=%s, want %d", status, body, http.StatusNotFound)
	}
}

// One attempt's credential is authority over that attempt's job alone.
func TestAttemptCredentialCannotActOnAnotherJob(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}, "node-2": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "submitter", Tags: []string{DefaultClientPrincipalTag}})
	agentOne := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	agentTwo := h.client(fabric.Identity{NodeID: "node-2", Tags: []string{DefaultAgentPrincipalTag}})
	nodeOne := h.register(agentOne, "node-1")
	nodeTwo := h.register(agentTwo, "node-2")
	h.submit(client, "credential-job-a", []string{"linux"})
	claimA := claimClass(t, h, agentOne, nodeOne, contract.JobClassOneShot)
	h.submit(client, "credential-job-b", []string{"linux"})
	claimB := claimClass(t, h, agentTwo, nodeTwo, contract.JobClassOneShot)

	foreignStatus, foreignBody := h.credentialRequest(agentOne, http.MethodGet, "/v1/jobs/"+claimB.Job.JobID, claimA.AttemptToken, nil)
	if foreignStatus != http.StatusForbidden {
		t.Fatalf("cross-job read status = %d body=%s, want %d", foreignStatus, foreignBody, http.StatusForbidden)
	}
	status, body := h.credentialRequest(agentOne, http.MethodGet,
		"/v1/jobs/"+claimB.Job.JobID+"/children", claimA.AttemptToken, nil)
	if status != http.StatusForbidden {
		t.Fatalf("cross-job children status = %d body=%s, want %d", status, body, http.StatusForbidden)
	}

	// An absent job answers byte-for-byte like a foreign one, so the read route
	// is not an existence probe over the whole job collection.
	absentStatus, absentBody := h.credentialRequest(agentOne, http.MethodGet, "/v1/jobs/job_does_not_exist", claimA.AttemptToken, nil)
	if absentStatus != foreignStatus || !bytes.Equal(absentBody, foreignBody) {
		t.Fatalf("absent job answered %d %s but a foreign job answered %d %s; they must be identical",
			absentStatus, absentBody, foreignStatus, foreignBody)
	}

	// A leaked bearer replayed from another node is refused even though the
	// attempt it names is perfectly live.
	status, body = h.credentialRequest(agentTwo, http.MethodGet, "/v1/jobs/"+claimA.Job.JobID, claimA.AttemptToken, nil)
	if status != http.StatusForbidden {
		t.Fatalf("replay from another node status = %d body=%s, want %d", status, body, http.StatusForbidden)
	}
	// A client principal cannot borrow the in-job surface either.
	status, body = h.credentialRequest(client, http.MethodGet, "/v1/jobs/"+claimA.Job.JobID, claimA.AttemptToken, nil)
	if status != http.StatusForbidden {
		t.Fatalf("client principal with credential status = %d body=%s, want %d", status, body, http.StatusForbidden)
	}
}

// Dispatch-key replay must not become a side door around the credential's
// scope, nor an oracle for which keys exist.
func TestAttemptCredentialReplayStaysInsideItsOwnParent(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}, "node-2": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "submitter", Tags: []string{DefaultClientPrincipalTag}})
	agentOne := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	agentTwo := h.client(fabric.Identity{NodeID: "node-2", Tags: []string{DefaultAgentPrincipalTag}})
	nodeOne := h.register(agentOne, "node-1")
	nodeTwo := h.register(agentTwo, "node-2")
	h.submit(client, "replay-parent-a", []string{"linux"})
	claimA := claimClass(t, h, agentOne, nodeOne, contract.JobClassOneShot)
	foreign := h.submit(client, "replay-foreign-root", []string{"linux"})
	claimB := claimClass(t, h, agentTwo, nodeTwo, contract.JobClassOneShot)
	if claimB.Job.JobID != foreign.JobID {
		t.Fatalf("second claim = %q, want the foreign root %q", claimB.Job.JobID, foreign.JobID)
	}

	// A's credential spawns its own child and may replay that key forever: a
	// retried attempt resubmitting the same work must stay idempotent.
	ownKey := "replay-own-child"
	status, body := h.credentialRequest(agentOne, http.MethodPost, "/v1/jobs", claimA.AttemptToken,
		validJobSpec(ownKey, []string{"linux"}))
	if status != http.StatusCreated {
		t.Fatalf("own child status = %d body=%s", status, body)
	}
	child := decodeJob(t, body)
	status, body = h.credentialRequest(agentOne, http.MethodPost, "/v1/jobs", claimA.AttemptToken,
		validJobSpec(ownKey, []string{"linux"}))
	if status != http.StatusOK {
		t.Fatalf("own-child replay status = %d body=%s, want %d", status, body, http.StatusOK)
	}
	if replayed := decodeJob(t, body); replayed.JobID != child.JobID || replayed.ParentJobID != claimA.Job.JobID {
		t.Fatalf("own-child replay = %#v, want the original child %q", replayed, child.JobID)
	}

	// Every key outside A's own children is a conflict, and the answer does not
	// depend on whether the canonical request happens to match.
	for _, probe := range []struct {
		name string
		spec contract.JobSpec
	}{
		{"foreign root, matching request", validJobSpec("replay-foreign-root", []string{"linux"})},
		{"foreign root, different request", func() contract.JobSpec {
			spec := validJobSpec("replay-foreign-root", []string{"linux"})
			spec.Execution.Argv = []string{"echo", "different"}
			return spec
		}()},
		{"another parent's child, matching request", func() contract.JobSpec {
			// B spawns a child, then A tries to replay B's child key.
			status, body := h.credentialRequest(agentTwo, http.MethodPost, "/v1/jobs", claimB.AttemptToken,
				validJobSpec("replay-foreign-child", []string{"linux"}))
			if status != http.StatusCreated {
				t.Fatalf("foreign child status = %d body=%s", status, body)
			}
			return validJobSpec("replay-foreign-child", []string{"linux"})
		}()},
		{"never-used key with a colliding request", validJobSpec("replay-unused-key", []string{"linux"})},
	} {
		status, body := h.credentialRequest(agentOne, http.MethodPost, "/v1/jobs", claimA.AttemptToken, probe.spec)
		if probe.name == "never-used key with a colliding request" {
			// The control: an unused key is a plain creation, so the refusals
			// above are about scope rather than about every submission failing.
			if status != http.StatusCreated {
				t.Fatalf("%s status = %d body=%s, want %d", probe.name, status, body, http.StatusCreated)
			}
			continue
		}
		if status != http.StatusConflict {
			t.Fatalf("%s status = %d body=%s, want %d", probe.name, status, body, http.StatusConflict)
		}
		if !bytes.Contains(body, []byte(contract.ErrorDispatchKeyConflict)) {
			t.Fatalf("%s body = %s, want %s", probe.name, body, contract.ErrorDispatchKeyConflict)
		}
		if bytes.Contains(body, []byte("\"job_id\"")) || bytes.Contains(body, []byte("\"spec\"")) {
			t.Fatalf("%s leaked a job projection: %s", probe.name, body)
		}
	}

	// The foreign job is untouched and still invisible through the read route.
	status, body = h.credentialRequest(agentOne, http.MethodGet, "/v1/jobs/"+foreign.JobID, claimA.AttemptToken, nil)
	if status != http.StatusForbidden {
		t.Fatalf("foreign read status = %d body=%s, want %d", status, body, http.StatusForbidden)
	}
}

// The credential reaches exactly three routes. Everything else on the job
// collection is refused without consulting the store.
func TestAttemptCredentialReachesNoOperatorRoute(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	job := h.submit(client, "credential-scope", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)

	for _, probe := range []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/jobs/" + job.JobID + "/remove"},
		{http.MethodPost, "/v1/jobs/" + job.JobID + "/restart"},
		{http.MethodPost, "/v1/jobs/" + job.JobID + "/forget"},
		{http.MethodPut, "/v1/jobs/" + job.JobID + "/desired-state"},
		{http.MethodGet, "/v1/jobs/" + job.JobID + "/logs"},
		{http.MethodGet, "/v1/jobs"},
	} {
		status, body := h.credentialRequest(agent, probe.method, probe.path, claim.AttemptToken, nil)
		if status != http.StatusForbidden {
			t.Fatalf("%s %s status = %d body=%s, want %d", probe.method, probe.path, status, body, http.StatusForbidden)
		}
		if !bytes.Contains(body, []byte(contract.ErrorPrincipalForbidden)) {
			t.Fatalf("%s %s body = %s, want %s", probe.method, probe.path, body, contract.ErrorPrincipalForbidden)
		}
	}
}

// The cap counts parent links, so a chain cannot grow without bound even
// though v1 never cascades cancellation.
func TestAttemptCredentialSpawnDepthIsCapped(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	h.submit(client, "spawn-depth-root", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)

	// Walk a real credential up through every depth to the cap, then prove the
	// next hop is refused. The scope stays the one L1 actually resolved, so the
	// in-transaction revalidation is satisfied at every step; only the depth
	// moves, which is the value the cap is about. Building a literal eight-deep
	// ancestry through claims would test the scheduler, not the cap.
	scope, err := h.store.ResolveAttemptCredential(t.Context(), claim.AttemptToken, node.NodeID)
	if err != nil {
		t.Fatalf("resolve live credential: %v", err)
	}
	if scope.SpawnDepth != 0 {
		t.Fatalf("root attempt credential depth = %d, want 0", scope.SpawnDepth)
	}
	for depth := 1; depth <= MaxSpawnDepth; depth++ {
		parent := scope
		parent.SpawnDepth = depth - 1
		child, _, err := h.store.CreateJobAs(t.Context(),
			validJobSpec("spawn-depth-"+strings.Repeat("x", depth), []string{"linux"}), JobOrigin{Parent: &parent})
		if err != nil {
			t.Fatalf("create job at depth %d: %v", depth, err)
		}
		if child.SpawnDepth != depth {
			t.Fatalf("job depth = %d, want %d", child.SpawnDepth, depth)
		}
		if child.OriginatingSubmitter != "submitter" {
			t.Fatalf("job at depth %d submitter = %q, want the inherited root submitter", depth, child.OriginatingSubmitter)
		}
	}
	atCap := scope
	atCap.SpawnDepth = MaxSpawnDepth
	_, _, err = h.store.CreateJobAs(t.Context(),
		validJobSpec("spawn-depth-over-cap", []string{"linux"}), JobOrigin{Parent: &atCap})
	if errorCode(err) != contract.ErrorSpawnDepthExceeded {
		t.Fatalf("submit past the cap = %v, want %s", err, contract.ErrorSpawnDepthExceeded)
	}

	// The live credential is at depth zero, so it is still free to spawn.
	status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken,
		validJobSpec("spawn-depth-first-hop", []string{"linux"}))
	if status != http.StatusCreated {
		t.Fatalf("first hop status = %d body=%s", status, body)
	}
}

// The refusal's wire shape is published, so pin it: HTTP 409 and not
// retryable, like the other job-creation conflicts.
func TestSpawnDepthExceededIsANonRetryableConflictOnTheWire(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	h.submit(client, "spawn-depth-wire-root", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)

	// Stand this credential at the cap. Its depth is read from the credential
	// row, so moving it there exercises the real HTTP refusal rather than a
	// store-level shortcut, without building an eight-deep chain again.
	if _, err := h.store.db.ExecContext(t.Context(),
		"UPDATE attempt_credentials SET spawn_depth=? WHERE attempt_id=?", MaxSpawnDepth, claim.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}

	status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken,
		validJobSpec("spawn-depth-wire-child", []string{"linux"}))
	if status != http.StatusConflict {
		t.Fatalf("over-cap submit status = %d body=%s, want %d", status, body, http.StatusConflict)
	}
	var response contract.ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode error response: %v body=%s", err, body)
	}
	if response.Error.Code != contract.ErrorSpawnDepthExceeded || response.Error.Retryable {
		t.Fatalf("over-cap error = %#v, want %s and retryable=false", response.Error, contract.ErrorSpawnDepthExceeded)
	}
}

// A public job projection never carries an environment secret, so the
// credential the agent injects cannot come back out through a job read.
func TestAttemptCredentialIsAbsentFromPublicJobProjections(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	h.submit(client, "credential-redaction", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)

	childSpec := validJobSpec("credential-redaction-child", []string{"linux"})
	childSpec.Execution.SensitiveEnv = map[string]string{contract.EnvAttemptToken: claim.AttemptToken}
	status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken, childSpec)
	if status != http.StatusCreated {
		t.Fatalf("child submit status = %d body=%s", status, body)
	}
	if bytes.Contains(body, []byte(claim.AttemptToken)) {
		t.Fatalf("child creation response leaked the attempt credential: %s", body)
	}
	child := decodeJob(t, body)

	for _, path := range []string{
		"/v1/jobs/" + child.JobID,
		"/v1/jobs/" + claim.Job.JobID + "/children",
	} {
		status, body := h.credentialRequest(agent, http.MethodGet, path, claim.AttemptToken, nil)
		if status != http.StatusOK {
			t.Fatalf("%s status = %d body=%s", path, status, body)
		}
		if bytes.Contains(body, []byte(claim.AttemptToken)) {
			t.Fatalf("%s leaked the attempt credential: %s", path, body)
		}
		if bytes.Contains(body, []byte("sensitive_env")) {
			t.Fatalf("%s exposed the sensitive environment: %s", path, body)
		}
	}
}

// An unknown bearer is refused before anything is read, and a root job records
// the client principal that actually submitted it.
func TestAttemptCredentialRefusesUnknownBearerAndRecordsRootSubmitter(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	root := h.submit(client, "credential-root-submitter", []string{"linux"})
	if root.OriginatingSubmitter != "submitter" || root.SpawnDepth != 0 || root.ParentJobID != "" {
		t.Fatalf("root job origin = submitter:%q depth:%d parent:%q",
			root.OriginatingSubmitter, root.SpawnDepth, root.ParentJobID)
	}
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
	_ = claim

	status, body := h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+root.JobID, "not-a-real-bearer", nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("unknown bearer status = %d body=%s, want %d", status, body, http.StatusUnauthorized)
	}
	if !bytes.Contains(body, []byte(contract.ErrorUnauthorized)) {
		t.Fatalf("unknown bearer body = %s, want %s", body, contract.ErrorUnauthorized)
	}
}

// A client principal keeps the same child listing, which is what makes this an
// L1 contract feature rather than an in-job convenience (ADR-0006).
func TestChildJobListingIsAvailableToClientPrincipals(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	parent := h.submit(client, "client-children-parent", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
	status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken,
		validJobSpec("client-children-child", []string{"linux"}))
	if status != http.StatusCreated {
		t.Fatalf("child submit status = %d body=%s", status, body)
	}
	child := decodeJob(t, body)

	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+parent.JobID+"/children", nil)
	if status != http.StatusOK {
		t.Fatalf("client children status = %d body=%s", status, body)
	}
	var page JobList
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 1 || page.Jobs[0].JobID != child.JobID {
		t.Fatalf("client children = %#v, want %q", page.Jobs, child.JobID)
	}
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/job_missing/children", nil)
	if status != http.StatusNotFound {
		t.Fatalf("children of an unknown job status = %d body=%s, want %d", status, body, http.StatusNotFound)
	}
}
