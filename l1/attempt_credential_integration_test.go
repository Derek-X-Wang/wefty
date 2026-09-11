package l1

import (
	"bytes"
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

	status, body := h.credentialRequest(agentOne, http.MethodGet, "/v1/jobs/"+claimB.Job.JobID, claimA.AttemptToken, nil)
	if status != http.StatusForbidden {
		t.Fatalf("cross-job read status = %d body=%s, want %d", status, body, http.StatusForbidden)
	}
	status, body = h.credentialRequest(agentOne, http.MethodGet,
		"/v1/jobs/"+claimB.Job.JobID+"/children", claimA.AttemptToken, nil)
	if status != http.StatusForbidden {
		t.Fatalf("cross-job children status = %d body=%s, want %d", status, body, http.StatusForbidden)
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

	// Grow the chain to exactly the cap through the store, then prove the next
	// hop is refused on the wire.
	scope := AttemptCredentialScope{
		AttemptID: claim.Lease.AttemptID, JobID: claim.Job.JobID,
		NodeID: node.NodeID, OriginatingSubmitter: "submitter",
	}
	for depth := 1; depth <= MaxSpawnDepth; depth++ {
		parent := scope
		parent.SpawnDepth = depth - 1
		child, _, err := h.store.CreateJobAs(t.Context(),
			validJobSpec("spawn-depth-"+strings.Repeat("x", depth), []string{"linux"}), JobOrigin{Parent: &parent})
		if err != nil {
			t.Fatalf("create chain job at depth %d: %v", depth, err)
		}
		if child.SpawnDepth != depth {
			t.Fatalf("chain job depth = %d, want %d", child.SpawnDepth, depth)
		}
		scope.JobID = child.JobID
	}
	atCap := scope
	atCap.SpawnDepth = MaxSpawnDepth
	_, _, err := h.store.CreateJobAs(t.Context(),
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
