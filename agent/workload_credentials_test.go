//go:build darwin || linux

package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

const undeliveredAttemptBearer = "wattempt_never_delivered_secret"

// dispatchedRun is one run carried through a real L3 — real store, real
// reconciler, real token minting — so the job spec under test is the one
// production builds rather than a hand-written fixture.
type dispatchedRun struct {
	store   *l3.Store
	fabric  fabric.Fabric
	runID   string
	spec    contract.JobSpec
	handoff string
}

func dispatchRunDeclaring(t *testing.T, dispatchAuthority bool) *dispatchedRun {
	t.Helper()
	network := plain.NewNetwork()
	ledgerFabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent-node"})
	store, err := l3.OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	jobs := &capturingJobClient{}
	reconciler, err := l3.NewReconciler(store, jobs, l3.ReconcilerConfig{Interval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	server, err := l3.NewServer(ledgerFabric, store, l3.ServerConfig{Jobs: jobs})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := ledgerFabric.Listen("tcp", l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	serveContext, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveContext, listener) }()
	t.Cleanup(func() {
		cancelServe()
		if err := <-serveDone; err != nil {
			t.Errorf("serve L3: %v", err)
		}
	})

	content := "exit 0\n"
	digest := sha256.Sum256([]byte(content))
	record, _, err := store.CreateRun(context.Background(), l3.CreateRunInput{
		IdempotencyKey: "credential-delivery",
		Actor:          "tester",
		Request: l3.CreateRunRequest{
			InlineScript: &l3.InlineScriptInput{
				Content: content, SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{"/bin/sh"},
			},
			Params:            json.RawMessage(`{}`),
			DispatchAuthority: dispatchAuthority,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if record.DispatchAuthority != dispatchAuthority {
		t.Fatalf("run record dispatch_authority = %t, want %t", record.DispatchAuthority, dispatchAuthority)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("dispatch the run: %v", err)
	}
	jobs.mu.Lock()
	spec := jobs.spec
	jobs.mu.Unlock()
	// The run token reaches the node on every dispatch: the mailbox publisher
	// holds it on the run's behalf. Whether it goes any further is the
	// behaviour under test.
	if spec.Execution.SensitiveEnv[contract.EnvRunToken] == "" {
		t.Fatal("dispatch delivered no run token to the node agent")
	}
	if got := contract.DeclaresDispatchAuthority(spec.Labels); got != dispatchAuthority {
		t.Fatalf("dispatched job declares dispatch authority = %t, want %t", got, dispatchAuthority)
	}
	handoff := filepath.Join(t.TempDir(), "handoff")
	if err := os.MkdirAll(handoff, 0o700); err != nil {
		t.Fatal(err)
	}
	spec.Execution.HandoffDirectory = handoff
	spec.Execution.Env[contract.EnvHandoffDir] = handoff
	return &dispatchedRun{store: store, fabric: agentFabric, runID: record.RunID, spec: spec, handoff: handoff}
}

func (d *dispatchedRun) claim() l1.Claim {
	return l1.Claim{
		Job:          l1.Job{JobID: "job-" + d.runID, Spec: d.spec, State: contract.JobClaimed},
		Lease:        l1.AttemptLease{AttemptID: "attempt-" + d.runID},
		AttemptToken: undeliveredAttemptBearer,
	}
}

// node builds an agent whose Fabric reaches this run's L3, so the attempt's
// loopback bridge proxies to the real ledger exactly as it does in production.
func (d *dispatchedRun) node(runner processrunner.Executor) *Agent {
	return &Agent{
		registration:     contract.NodeRegistration{NodeID: "agent-node"},
		runtimes:         testRuntimeSet(runner),
		fabric:           d.fabric,
		controlPlaneAddr: "wefty://control-plane",
		runLedgerAddr:    l3.DefaultL3Address,
		runLedger:        newFabricRunLedgerAppender(d.fabric, l3.DefaultL3Address),
		mailboxPoll:      time.Hour,
	}
}

func (d *dispatchedRun) runToken(t *testing.T) string {
	t.Helper()
	token := d.spec.Execution.SensitiveEnv[contract.EnvRunToken]
	if token == "" {
		t.Fatal("dispatched job carries no run token")
	}
	return token
}

// probingRunner stands in for the workload: it records the environment it was
// given and may act inside the attempt, while the bridge and the mailbox are
// still alive.
type probingRunner struct {
	execution contract.ExecutionSpec
	probe     func(context.Context, contract.ExecutionSpec, processrunner.OutputSink)
}

func (r *probingRunner) Run(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
	r.execution = request.Execution
	if request.Started != nil {
		request.Started()
	}
	if r.probe != nil {
		r.probe(ctx, request.Execution, sink)
	}
	code := 0
	return contract.ProcessResult{ExitCode: &code}, nil
}

// A run that did not declare dispatch authority is the new default: it reports
// through its mailbox and holds nothing a hostile subprocess could steal.
func TestADefaultRunDeliversNoCredentialToTheWorkload(t *testing.T) {
	run := dispatchRunDeclaring(t, false)
	runner := &probingRunner{}
	if _, err := run.node(runner).runWorkload(t.Context(), run.claim()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{contract.EnvRunToken, contract.EnvAttemptToken} {
		if value := runner.execution.SensitiveEnv[name]; value != "" {
			t.Fatalf("%s reached the workload's sensitive environment", name)
		}
		if value := runner.execution.Env[name]; value != "" {
			t.Fatalf("%s reached the workload's public environment", name)
		}
	}
	// It is not left mute: the mailbox is prepared and is how it reports.
	directory := runner.execution.Env[contract.EnvRunDir]
	if directory == "" {
		t.Fatal("a credential-free job received no run mailbox either")
	}
	if info, err := os.Stat(filepath.Join(directory, runMailboxEventsDirectoryName)); err != nil || !info.IsDir() {
		t.Fatalf("mailbox events directory = (%v, %v)", info, err)
	}
	// The endpoints stay. They are transport, not authority, and withholding
	// them would only hide the refusal the next test asserts.
	for _, name := range []string{contract.EnvL3Endpoint, contract.EnvL1Endpoint} {
		if !strings.HasPrefix(runner.execution.Env[name], "http://127.0.0.1:") {
			t.Fatalf("%s = %q, want the attempt-local bridge", name, runner.execution.Env[name])
		}
	}
}

// Declaring dispatch authority at submit delivers both credentials exactly as
// they were delivered before the default changed.
func TestADispatchingRunStillReceivesBothCredentials(t *testing.T) {
	run := dispatchRunDeclaring(t, true)
	runner := &probingRunner{}
	if _, err := run.node(runner).runWorkload(t.Context(), run.claim()); err != nil {
		t.Fatal(err)
	}
	if got := runner.execution.SensitiveEnv[contract.EnvRunToken]; got != run.runToken(t) {
		t.Fatalf("%s = %q, want the dispatched run token", contract.EnvRunToken, got)
	}
	if got := runner.execution.SensitiveEnv[contract.EnvAttemptToken]; got != undeliveredAttemptBearer {
		t.Fatalf("%s = %q, want the claimed bearer", contract.EnvAttemptToken, got)
	}
	// Both stay sensitive, so redaction and public job projections still cover
	// them without a second rule.
	for _, name := range []string{contract.EnvRunToken, contract.EnvAttemptToken} {
		if _, public := runner.execution.Env[name]; public {
			t.Fatalf("%s appeared in the public environment: %#v", name, runner.execution.Env)
		}
	}
	if runner.execution.Env[contract.EnvRunDir] == "" {
		t.Fatal("a dispatching run lost its mailbox")
	}
}

// Withholding the credential is the whole point only if the ledger actually
// refuses the workload. The probe runs inside the attempt, against the real L3
// behind the attempt-local bridge, using only what the job environment offers.
func TestAWorkloadWithoutTheRunTokenCannotAppendToL3(t *testing.T) {
	run := dispatchRunDeclaring(t, false)
	envelope := []byte(`{"schema_version":1,"envelope_id":"env_probe","run_id":"` + run.runID +
		`","step_id":"probe","status":"succeeded","summary":"forged","idempotency_key":"probe"}`)
	var anonymousStatus, forgedStatus int
	runner := &probingRunner{probe: func(ctx context.Context, execution contract.ExecutionSpec, _ processrunner.OutputSink) {
		endpoint := execution.Env[contract.EnvL3Endpoint] + "/v1/runs/" + run.runID + "/envelopes"
		anonymousStatus = postEnvelope(t, ctx, endpoint, "", envelope)
		// The only run-shaped string the job holds is its own run ID. It is
		// not a credential, and presenting it must not become one.
		forgedStatus = postEnvelope(t, ctx, endpoint, execution.Env[contract.EnvRunID], envelope)
	}}
	if _, err := run.node(runner).runWorkload(t.Context(), run.claim()); err != nil {
		t.Fatal(err)
	}
	// No bearer at all: the workload inherits no Fabric privilege from the
	// agent that proxied its request.
	if anonymousStatus != http.StatusForbidden {
		t.Fatalf("unauthenticated append status = %d, want %d", anonymousStatus, http.StatusForbidden)
	}
	if forgedStatus != http.StatusUnauthorized {
		t.Fatalf("forged-bearer append status = %d, want %d", forgedStatus, http.StatusUnauthorized)
	}
	envelopes, err := run.store.ListEnvelopes(context.Background(), run.runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(envelopes) != 0 {
		t.Fatalf("the ledger accepted %d envelopes from a credential-free job", len(envelopes))
	}
}

func postEnvelope(t *testing.T, ctx context.Context, endpoint, bearer string, body []byte) int {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("post envelope: %v", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	return response.StatusCode
}

// A credential the workload never received is still a credential: if it turns
// up in the job's output by any other route, the log sink must not see it.
func TestTheRedactorStillMasksUndeliveredTokens(t *testing.T) {
	for _, testCase := range []struct {
		name string
		// withMailbox separates the two sources the redactor draws on. Without
		// a mailbox the run token is named only by the withheld set, so this is
		// the case that would regress silently.
		withMailbox bool
	}{
		{name: "mailbox publishes for the run", withMailbox: true},
		{name: "no mailbox holds the run token", withMailbox: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			run := dispatchRunDeclaring(t, false)
			token := run.runToken(t)
			var output bytes.Buffer
			runner := &probingRunner{probe: func(ctx context.Context, _ contract.ExecutionSpec, sink processrunner.OutputSink) {
				line := "run=" + token + " attempt=" + undeliveredAttemptBearer + "\n"
				if err := sink.WriteOutput(ctx, contract.LogEvent{Stream: contract.LogStdout, Bytes: []byte(line)}); err != nil {
					t.Errorf("write workload output: %v", err)
				}
			}}
			node := run.node(runner)
			if !testCase.withMailbox {
				node.runLedger = nil
			}
			node.outputSinkFactory = func(l1.Claim) processrunner.OutputSink {
				return processrunner.OutputSinkFunc(func(_ context.Context, event contract.LogEvent) error {
					if event.Stream == contract.LogStdout {
						_, _ = output.Write(event.Bytes)
					}
					return nil
				})
			}
			if _, err := node.runWorkload(t.Context(), run.claim()); err != nil {
				t.Fatal(err)
			}
			if got := output.String(); got != "run=[REDACTED] attempt=[REDACTED]\n" {
				t.Fatalf("workload output = %q, want both undelivered credentials masked", got)
			}
		})
	}
}
