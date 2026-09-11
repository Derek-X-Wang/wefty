package agent

import (
	"context"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// capturingRunner records the execution environment the attempt actually
// received, which is the only place the delivery contract is observable.
type capturingRunner struct{ execution contract.ExecutionSpec }

func (r *capturingRunner) Run(_ context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	r.execution = request.Execution
	if request.Started != nil {
		request.Started()
	}
	code := 0
	return contract.ProcessResult{ExitCode: &code}, nil
}

func credentialAgent(t *testing.T, runner *capturingRunner) *Agent {
	t.Helper()
	return &Agent{
		runtimes:         testRuntimeSet(runner),
		fabric:           plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "delivery-node"}),
		controlPlaneAddr: "wefty://control-plane",
	}
}

func oneShotClaim(token string) l1.Claim {
	return l1.Claim{
		Job: l1.Job{JobID: "job-delivery", Spec: contract.JobSpec{
			Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
		}},
		Lease:        l1.AttemptLease{AttemptID: "attempt-delivery"},
		AttemptToken: token,
	}
}

// Acceptance (a), agent half: a one-shot process attempt with no L3 in sight
// still receives both halves of the in-job contract.
func TestOneShotAttemptReceivesAttemptCredentialWithoutL3(t *testing.T) {
	runner := &capturingRunner{}
	claim := oneShotClaim("attempt-bearer-value")
	if _, err := credentialAgent(t, runner).runWorkload(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	endpoint := runner.execution.Env[contract.EnvL1Endpoint]
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") || !strings.HasSuffix(endpoint, "/l1") {
		t.Fatalf("%s = %q, want an attempt-local /l1 bridge URL", contract.EnvL1Endpoint, endpoint)
	}
	if got := runner.execution.SensitiveEnv[contract.EnvAttemptToken]; got != "attempt-bearer-value" {
		t.Fatalf("%s = %q, want the claimed bearer", contract.EnvAttemptToken, got)
	}
	// The credential must travel sensitively so log redaction and public job
	// projections both cover it without a second rule.
	if _, public := runner.execution.Env[contract.EnvAttemptToken]; public {
		t.Fatalf("attempt credential appeared in the public environment: %#v", runner.execution.Env)
	}
	// No L3 dispatched this job, so no run context may be invented for it.
	if endpoint, present := runner.execution.Env[contract.EnvL3Endpoint]; present {
		t.Fatalf("%s = %q with no L3 dispatch", contract.EnvL3Endpoint, endpoint)
	}
}

// Reserved names carry attempt-local truth. A submitter that supplies them is
// overwritten, never trusted.
func TestSubmittedAttemptCredentialValuesAreReplaced(t *testing.T) {
	runner := &capturingRunner{}
	claim := oneShotClaim("real-bearer")
	claim.Job.Spec.Execution.Env = map[string]string{
		contract.EnvL1Endpoint:   "http://attacker.invalid/l1",
		contract.EnvAttemptToken: "forged-bearer",
	}
	claim.Job.Spec.Execution.SensitiveEnv = map[string]string{contract.EnvAttemptToken: "forged-bearer"}
	if _, err := credentialAgent(t, runner).runWorkload(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if got := runner.execution.Env[contract.EnvL1Endpoint]; strings.Contains(got, "attacker.invalid") {
		t.Fatalf("%s = %q, want the attempt-local bridge", contract.EnvL1Endpoint, got)
	}
	if got := runner.execution.SensitiveEnv[contract.EnvAttemptToken]; got != "real-bearer" {
		t.Fatalf("%s = %q, want the claimed bearer", contract.EnvAttemptToken, got)
	}
	if _, forged := runner.execution.Env[contract.EnvAttemptToken]; forged {
		t.Fatal("a submitted attempt credential survived in the public environment")
	}
}

// v1 delivers to one-shot attempts only. L1 still mints for every claim, so
// enabling service delivery later changes no authority rule.
func TestServiceAttemptReceivesNoAttemptCredentialYet(t *testing.T) {
	runner := &capturingRunner{}
	claim := oneShotClaim("service-bearer")
	claim.Job.Spec.Class = contract.JobClassService
	agent := credentialAgent(t, runner)
	// EvalSymlinks because the managed-root guardrails refuse a symlink
	// anywhere in the ancestry, and macOS TempDir sits under /var -> /private/var.
	resolvedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	managedResource, err := initializeManagedResource(resolvedRoot, "delivery-node", "delivery-boot")
	if err != nil {
		t.Fatal(err)
	}
	agent.managedResource = managedResource
	if _, err := agent.runWorkload(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if _, present := runner.execution.Env[contract.EnvL1Endpoint]; present {
		t.Fatalf("service attempt received %s", contract.EnvL1Endpoint)
	}
	if _, present := runner.execution.SensitiveEnv[contract.EnvAttemptToken]; present {
		t.Fatalf("service attempt received %s", contract.EnvAttemptToken)
	}
}

// The bridge is transport only. Anything outside the published three routes is
// refused before it can reach the agent's authenticated Fabric connection.
func TestControlPlaneBridgeAllowsOnlyTheAttemptCredentialRoutes(t *testing.T) {
	for _, probe := range []struct {
		method  string
		path    string
		allowed bool
	}{
		{http.MethodPost, "/v1/jobs", true},
		{http.MethodGet, "/v1/jobs/job_1", true},
		{http.MethodGet, "/v1/jobs/job_1/children", true},
		{http.MethodGet, "/v1/jobs", false},
		{http.MethodPost, "/v1/jobs/job_1/remove", false},
		{http.MethodDelete, "/v1/jobs/job_1", false},
		{http.MethodGet, "/v1/jobs/job_1/logs", false},
		{http.MethodGet, "/v1/nodes", false},
		{http.MethodPost, "/v1/computers", false},
		{http.MethodGet, "/v1/jobs//children", false},
		{http.MethodGet, "/v1/jobs/../nodes", false},
	} {
		if got := bridgeRouteAllowed(attemptCredentialBridgeRoutes, probe.method, probe.path); got != probe.allowed {
			t.Errorf("%s %s allowed = %t, want %t", probe.method, probe.path, got, probe.allowed)
		}
	}
}

// The bridge now exists for every one-shot attempt, which must not hand an
// L1-only workload a loopback door to the run ledger it could not reach before.
// Reachability is not authority — L3 still demands a run token — but an attempt
// with no run context has no business dialling the ledger at all.
func TestBridgeOmitsTheRunLedgerSurfaceForAnL1OnlyAttempt(t *testing.T) {
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "suppress-node"})
	agent := &Agent{fabric: participant, controlPlaneAddr: "wefty://control-plane", runLedgerAddr: "wefty://run-ledger"}

	l1Only, err := agent.startWorkflowBridge(t.Context(), contract.JobKindProcess, contract.ExecutionSpec{})
	if err != nil || l1Only == nil {
		t.Fatalf("L1-only bridge = (%v, %v), want a bridge", l1Only, err)
	}
	defer l1Only.close()
	if l1Only.l1Endpoint == "" {
		t.Fatal("L1-only attempt received no attempt-credential surface")
	}
	if l1Only.l3Endpoint != "" {
		t.Fatalf("L1-only attempt was given a run-ledger endpoint %q", l1Only.l3Endpoint)
	}
	if status := bridgeProbeStatus(t, l1Only, "/l3/v1/runs"); status != http.StatusNotFound {
		t.Fatalf("L1-only /l3 probe = %d, want %d", status, http.StatusNotFound)
	}

	// An L3-dispatched attempt keeps both surfaces.
	dispatched, err := agent.startWorkflowBridge(t.Context(), contract.JobKindProcess, contract.ExecutionSpec{
		Env: map[string]string{contract.EnvL3Endpoint: "http://placeholder.invalid/l3"},
	})
	if err != nil || dispatched == nil {
		t.Fatalf("L3-dispatched bridge = (%v, %v), want a bridge", dispatched, err)
	}
	defer dispatched.close()
	if dispatched.l3Endpoint == "" || dispatched.l1Endpoint == "" {
		t.Fatalf("L3-dispatched bridge endpoints l1=%q l3=%q, want both", dispatched.l1Endpoint, dispatched.l3Endpoint)
	}
}

func bridgeProbeStatus(t *testing.T, bridge *workflowBridge, path string) int {
	t.Helper()
	connection, err := bridge.dial(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) { return connection, nil },
	}}
	response, err := client.Get("http://bridge.invalid" + path)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

// A short line must not wait behind a long credential: the redactor withholds
// only a genuine partial prefix, so live tailing survives the injection.
func TestRedactionWithholdsOnlyGenuineSecretPrefixes(t *testing.T) {
	secret := "wefty-attempt-credential-that-is-quite-long"
	sink := &recordingOutputSink{}
	redacting := newRedactingOutputSink(sink, map[string]string{contract.EnvAttemptToken: secret})
	write := func(payload string) {
		t.Helper()
		if err := redacting.WriteOutput(t.Context(), contract.LogEvent{
			AttemptID: "attempt-tail", Stream: contract.LogStdout, Bytes: []byte(payload),
		}); err != nil {
			t.Fatal(err)
		}
	}

	write("first\n")
	if len(sink.events) != 1 || string(sink.events[0].Bytes) != "first\n" {
		t.Fatalf("prompt line = %#v, want an immediate first\\n", sink.events)
	}

	// A split secret is still caught: the head is withheld until the tail lands.
	write(secret[:10])
	if len(sink.events) != 1 {
		t.Fatalf("partial secret was emitted early: %#v", sink.events)
	}
	write(secret[10:] + " done\n")
	if err := redacting.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	var combined strings.Builder
	for _, event := range sink.events {
		combined.Write(event.Bytes)
	}
	if got, want := combined.String(), "first\n[REDACTED] done\n"; got != want {
		t.Fatalf("redacted stream = %q, want %q", got, want)
	}
}

type recordingOutputSink struct{ events []contract.LogEvent }

func (s *recordingOutputSink) WriteOutput(_ context.Context, event contract.LogEvent) error {
	s.events = append(s.events, event)
	return nil
}
