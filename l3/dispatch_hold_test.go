package l3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestLedgerAnswerClassifier(t *testing.T) {
	tests := []struct {
		name, method, path string
		status             int
		code, reason       string
		complete, retry    bool
		want               l1AnswerKind
	}{
		{"identity", "POST", "/v1/jobs", 503, "unavailable", "identity_unverifiable", true, true, l1LedgerNotAdmitted},
		{"old identity", "POST", "/v1/jobs", 401, "unauthorized", "", true, false, l1LedgerNotAdmitted},
		{"old principal", "GET", "/v1/jobs/j", 403, "principal_forbidden", "", true, false, l1LedgerNotAdmitted},
		{"old principal 401", "GET", "/v1/jobs/j", 401, "principal_forbidden", "", true, false, l1LedgerNotAdmitted},
		{"583 root", "POST", "/v1/jobs", 403, "run_identity_not_entitled", "", true, false, l1LedgerNotAdmitted},
		{"not a submission", "POST", "/v1/jobs/j/cancel", 403, "run_identity_not_entitled", "", true, false, l1WorkRefused},
		{"lookup gate", "GET", "/v1/dispatch-keys/k/job", 403, "forbidden", "run_ledger_not_admitted", true, false, l1LedgerNotAdmitted},
		{"computer gate", "POST", "/v1/computers/c/token-scope-proof", 403, "forbidden", "run_ledger_not_admitted", true, false, l1LedgerNotAdmitted},
		{"host gate", "POST", "/v1/host-boot-session-proof", 403, "forbidden", "run_ledger_not_admitted", true, false, l1LedgerNotAdmitted},
		{"computer resource", "POST", "/v1/computers/c/token-scope-proof", 403, "forbidden", "", true, false, l1WorkRefused},
		{"host resource", "POST", "/v1/host-boot-session-proof", 403, "forbidden", "", true, false, l1WorkRefused},
		{"unscoped forbidden", "GET", "/v1/jobs/j", 403, "forbidden", "", true, false, l1WorkRefused},
		{"job absent", "GET", "/v1/jobs/j", 404, "not_found", "", true, false, l1AuthoritativeAbsence},
		{"dispatch absent", "GET", "/v1/dispatch-keys/k/job", 404, "not_found", "", true, false, l1AuthoritativeAbsence},
		{"mux job", "GET", "/v1/jobs/j", 404, "not_found", "no_route", true, false, l1ProtocolViolation},
		{"mux lookup", "GET", "/v1/dispatch-keys/k/job", 404, "not_found", "no_route", true, false, l1ProtocolViolation},
		{"mux submission", "POST", "/v1/jobs", 404, "not_found", "no_route", true, false, l1ProtocolViolation},
		{"cancel resource", "POST", "/v1/jobs/j/cancel", 404, "not_found", "", true, false, l1WorkRefused},
		{"unknown", "POST", "/v1/jobs", 409, "future_code", "", true, false, l1ProtocolViolation},
		{"partial auth", "POST", "/v1/jobs", 401, "unauthorized", "", false, false, l1ProtocolViolation},
		{"partial absence", "GET", "/v1/jobs/j", 404, "not_found", "", false, false, l1ProtocolViolation},
		{"wrong status", "GET", "/v1/jobs/j", 503, "not_found", "", true, false, l1Transient},
		{"wrong refusal status", "POST", "/v1/jobs", 401, "invalid_request", "", true, false, l1ProtocolViolation},
		{"capacity", "POST", "/v1/jobs", 409, "capacity_exhausted", "", true, true, l1Transient},
		{"unavailable resource", "GET", "/v1/jobs/j", 503, "unavailable", "maintenance", true, true, l1Transient},
		{"submission refused", "POST", "/v1/jobs", 400, "invalid_request", "", true, false, l1WorkRefused},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"error":{"code":%q,"message":"answer","retryable":%t,"details":{"reason":%q}}}`, tt.code, tt.retry, tt.reason)
			if !tt.complete {
				body = fmt.Sprintf(`{"error":{"code":%q}}`, tt.code)
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tt.status); io.WriteString(w, body) }))
			defer srv.Close()
			c := &L1Client{client: srv.Client(), operationTimeout: time.Second}
			// Redirect the test client's transport to a real local HTTP listener.
			c.client.Transport = &http.Transport{Proxy: http.ProxyURL(mustTestURL(t, srv.URL))}
			defer c.CloseIdleConnections()
			err := c.do(context.Background(), tt.method, tt.path, nil, &l1.Job{}, 200)
			a := classifyL1Answer(err)
			if a.Kind != tt.want || a.Method != tt.method || a.Target != tt.path || a.Status != tt.status {
				t.Fatalf("answer=%+v want kind=%d", a, tt.want)
			}
			// These are the existing production-called consequences; no_route must
			// never become destructive absence, and unknown dispatch errors must retry.
			if authoritativeL1NotFound(err, tt.method, tt.path) != (tt.want == l1AuthoritativeAbsence) {
				t.Fatalf("unsafe absence: %v", err)
			}
			if (tt.method != "POST" || tt.path != "/v1/jobs") && !retryableDispatchError(err) {
				t.Fatalf("another operation's refusal would fail submission: %v", err)
			}
			if tt.method == "POST" && tt.path == "/v1/jobs" && retryableDispatchError(err) != (tt.want != l1WorkRefused) {
				t.Fatalf("unsafe submission consequence: %v", err)
			}
		})
	}
}

func mustTestURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

type holdIdentityFabric struct {
	fabric.Fabric
	failure atomic.Int32
}

func (f *holdIdentityFabric) WhoIs(ctx context.Context, addr string) (fabric.Identity, error) {
	switch f.failure.Load() {
	case 1:
		return fabric.Identity{}, errors.New("WhoIs service temporarily unavailable")
	case 2:
		return fabric.Identity{}, fmt.Errorf("wrapped: %w", fabric.ErrIdentityNotFound)
	}
	return f.Fabric.WhoIs(ctx, addr)
}

// Both control planes are real HTTP servers over plain Fabric. The atomic
// handler swap models correcting RunLedgerNodeID without changing L1's data.
type holdHTTPHarness struct {
	*integrationHarness
	identity                   *holdIdentityFabric
	ledgerIdentity             *holdIdentityFabric
	clock                      *mutableClock
	seedProbe                  atomic.Bool
	probeSeed                  dispatchIntent
	configured                 atomic.Bool
	submits, probes            atomic.Int32
	probeEntered, probeRelease chan struct{}
}

func newHoldHTTPHarness(t *testing.T) *holdHTTPHarness {
	ctx := context.Background()
	network := plain.NewNetwork()
	control := &holdIdentityFabric{Fabric: network.NewFabric(fabric.Identity{NodeID: "control-plane"})}
	ledger := &holdIdentityFabric{Fabric: network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})}
	authority, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	good, err := l1.NewServer(control, authority, l1.ServerConfig{RunLedgerNodeID: "run-ledger"})
	if err != nil {
		t.Fatal(err)
	}
	bad, err := l1.NewServer(control, authority, l1.ServerConfig{RunLedgerNodeID: "wrong-ledger"})
	if err != nil {
		t.Fatal(err)
	}
	store, path, clock := recoveryStore(t)
	client, err := NewL1Client(ledger, DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(ledger, store, ServerConfig{Jobs: client, Logs: client, Results: client, ComputerGrants: client, HostBootSessions: client})
	if err != nil {
		t.Fatal(err)
	}
	h := &holdHTTPHarness{integrationHarness: &integrationHarness{t: t, network: network, l1Store: authority, l3Store: store, l3Path: path, l1Client: client, l3Server: srv, callerUser: "alice"}, identity: control, ledgerIdentity: ledger, clock: clock}
	h.configured.Store(true)
	listener, err := control.Listen("tcp", DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/jobs" {
			h.submits.Add(1)
		}
		if strings.HasPrefix(r.URL.Path, "/v1/dispatch-keys/") {
			h.probes.Add(1)
			if h.probeEntered != nil && strings.HasPrefix(r.URL.Path, "/v1/dispatch-keys/l3-admission-") {
				close(h.probeEntered)
				<-h.probeRelease
			}
		}
		if h.seedProbe.Load() && strings.HasPrefix(r.URL.Path, "/v1/dispatch-keys/l3-admission-") {
			key, _ := url.PathUnescape(strings.TrimSuffix(strings.TrimPrefix(r.URL.EscapedPath(), "/v1/dispatch-keys/"), "/job"))
			seed := h.probeSeed
			seed.DispatchKey = key
			if _, _, err := authority.CreateJobAs(r.Context(), seed.jobSpec("probe-fixture-token"), l1.JobOrigin{OriginatingSubmitter: "run-ledger", SubmittedByRunLedger: true}); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
		}
		if h.configured.Load() {
			good.Handler().ServeHTTP(w, r)
		} else {
			bad.Handler().ServeHTTP(w, r)
		}
	})}
	done := make(chan error, 1)
	go func() { done <- httpServer.Serve(listener) }()
	l3Listener, err := ledger.Listen("tcp", DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	l3Done := serveL3(runCtx, srv, l3Listener)
	h.caller = h.client(fabric.Identity{NodeID: "caller", UserID: "alice", Tags: []string{DefaultCallerPrincipalTag}}, DefaultL3Address)
	t.Cleanup(func() {
		cancel()
		if err := <-l3Done; err != nil {
			t.Error(err)
		}
		httpServer.Close()
		if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
		client.CloseIdleConnections()
		for _, c := range h.clients {
			c.CloseIdleConnections()
		}
		h.l3Store.Close()
		authority.Close()
	})
	return h
}
func (h *holdHTTPHarness) reconcile() error {
	r, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		h.t.Fatal(err)
	}
	return r.ReconcileOnce(context.Background())
}
func assertHeld(t *testing.T, s *Store, id string) {
	t.Helper()
	r, err := s.GetRun(context.Background(), id)
	if err != nil || r.DispatchHold == nil || (r.Status != contract.RunPending && r.Status != contract.RunDispatching) {
		t.Fatalf("Run not held: %+v %v", r, err)
	}
}
func stagedBearer(t *testing.T, s *Store, id string) string {
	t.Helper()
	var token string
	if err := s.db.QueryRow(`SELECT token_delivery FROM dispatch_outbox WHERE run_id=?`, id).Scan(&token); err != nil || token == "" {
		t.Fatalf("missing staged bearer: %v", err)
	}
	return token
}

func TestLedgerHoldIdentityRecoveryAndBearer(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("lookup200=%t", existing), func(t *testing.T) {
			h := newHoldHTTPHarness(t)
			ctx := context.Background()
			run := h.submit(inlineRunRequest("exit 0\n"), "identity-hold")
			before, err := h.l3Store.ensureRunToken(ctx, run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			h.identity.failure.Store(1)
			if err := h.reconcile(); err == nil {
				t.Fatal("outage not reported")
			}
			assertHeld(t, h.l3Store, run.RunID)
			// Let the per-Run retry become due, so this checks the hold's own
			// transactional guard rather than passing on the retry predicate.
			h.clock.now = h.clock.now.Add(time.Second)
			if _, err := h.l3Store.beginDispatch(ctx, run.RunID); !errors.Is(err, errDispatchAbandoned) {
				t.Fatalf("transactional hold guard bypassed: %v", err)
			}
			if stagedBearer(t, h.l3Store, run.RunID) != before {
				t.Fatal("held token rotated")
			}
			if existing {
				// Seed a real L1 job for this probe's key; the admission response is then
				// L1's own validated 200, rather than its resource-level 404.
				req := inlineRunRequest("exit 0\n")
				h.probeSeed = dispatchIntent{RunID: run.RunID, Content: []byte(req.InlineScript.Content), SHA256: req.InlineScript.SHA256, Interpreter: []string{"/bin/sh"}, Mode: 0755, Tags: []string{}, Params: []byte(`{}`)}
				h.seedProbe.Store(true)
			}
			h.identity.failure.Store(0)
			h.clock.now = h.clock.now.Add(time.Second)
			// Capture the exact outbound bearer when admission clears.
			transport := h.l1Client.client.Transport
			var sent string
			h.l1Client.client.Transport = recoveryRoundTripper(func(req *http.Request) (*http.Response, error) {
				if req.Method == "POST" && req.URL.Path == "/v1/jobs" {
					body, _ := io.ReadAll(req.Body)
					req.Body = io.NopCloser(strings.NewReader(string(body)))
					var spec contract.JobSpec
					json.Unmarshal(body, &spec)
					sent = spec.Execution.SensitiveEnv["WEFTY_RUN_TOKEN"]
				}
				return transport.RoundTrip(req)
			})
			if err := h.reconcile(); err != nil {
				t.Fatal(err)
			}
			record, err := h.l3Store.GetRun(ctx, run.RunID)
			if err != nil || record.Status != contract.RunQueued || record.DispatchHold != nil || sent != before {
				t.Fatalf("not recovered: %+v sent unchanged=%t err=%v", record, sent == before, err)
			}
		})
	}
}

func TestLedgerHoldMisconfigurationReplayRestartAndVisibility(t *testing.T) {
	h := newHoldHTTPHarness(t)
	ctx := context.Background()
	// Lose the acknowledgement of an accepted submit. Its immutable request,
	// including staged token, can replay even after configuration is broken.
	refused := h.submit(inlineRunRequest("exit 0\n"), "first-refusal")
	h.clock.now = h.clock.now.Add(time.Second)
	first := h.submit(inlineRunRequest("exit 0\n"), "replayed")
	intents, _ := h.l3Store.pendingDispatches(ctx)
	token, err := h.l3Store.beginDispatch(ctx, first.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var seed dispatchIntent
	for _, intent := range intents {
		if intent.RunID == first.RunID {
			seed = intent
		}
	}
	seeded, err := h.l1Client.SubmitJob(ctx, seed.jobSpec(token))
	if err != nil {
		t.Fatal(err)
	}
	h.configured.Store(false)
	runs := []string{first.RunID, refused.RunID}
	for i := 0; i < 6; i++ {
		h.clock.now = h.clock.now.Add(time.Second)
		runs = append(runs, h.submit(inlineRunRequest("exit 0\n"), fmt.Sprintf("held-%d", i)).RunID)
	}
	if err := h.reconcile(); err == nil {
		t.Fatal("misconfiguration not reported")
	}
	for _, id := range runs[1:] {
		assertHeld(t, h.l3Store, id)
	}
	// The first refusal holds the ledger before the older ambiguous
	// submit can replay. Its staged bearer is still exact while it waits.
	assertHeld(t, h.l3Store, first.RunID)
	if stagedBearer(t, h.l3Store, first.RunID) != token {
		t.Fatal("held replay token rotated")
	}
	beforeReplay, _ := h.l3Store.DispatchHealth(ctx)
	replayJob, err := h.l1Client.SubmitJob(ctx, seed.jobSpec(token))
	if err != nil || replayJob.JobID != seeded.JobID {
		t.Fatalf("misconfigured held submit replay: %+v %v", replayJob, err)
	}
	// Recording a replay acknowledgement is the same production seam the
	// reconciler calls for an in-flight submit that finishes during a hold.
	if err := h.l3Store.completeDispatch(ctx, first.RunID, replayJob.JobID); err != nil {
		t.Fatal(err)
	}
	afterReplay, _ := h.l3Store.DispatchHealth(ctx)
	if afterReplay.DispatchHold == nil || *afterReplay.DispatchHold != *beforeReplay.DispatchHold {
		t.Fatal("submit replay released or reset admission")
	}
	if h.submits.Load() != 3 {
		t.Fatalf("expected seed, replay, first refusal; submits=%d", h.submits.Load())
	}
	// The successful submit replay has not proved admission.
	for i := 0; i < 5; i++ {
		_ = h.reconcile()
	}
	if h.probes.Load() != 0 || h.submits.Load() != 3 {
		t.Fatal("hold made per-Run requests before its probe")
	}
	health, _ := h.l3Store.DispatchHealth(ctx)
	if health.DispatchHold == nil {
		t.Fatal("successful replay released admission")
	}
	hold := *health.DispatchHold
	status, _, body := h.do(h.caller, "POST", "/v1/runs", inlineRunRequest("exit 0\n"), http.Header{"Idempotency-Key": {"first-refusal"}})
	var replay RunAccepted
	if status != 200 || json.Unmarshal(body, &replay) != nil {
		t.Fatalf("Run replay: %d %s", status, body)
	}
	if replay.RunID != runs[1] {
		t.Fatal("Run replay changed identity")
	}
	health, _ = h.l3Store.DispatchHealth(ctx)
	if *health.DispatchHold != hold {
		t.Fatal("Run replay reset hold")
	}
	var count int
	h.l3Store.db.QueryRow(`SELECT COUNT(*) FROM dispatch_outbox`).Scan(&count)
	if count != len(runs) {
		t.Fatal("replay appended outbox")
	}
	for _, path := range []string{"/v1/runs/" + runs[1], "/v1/runs/" + runs[1] + "/execution", "/v1/runs", "/v1/health"} {
		status, _, body := h.do(h.caller, "GET", path, nil, nil)
		if status != 200 || !strings.Contains(string(body), `"dispatch_hold"`) || !strings.Contains(string(body), hold.Reason) {
			t.Fatalf("invisible hold: %s %d %s", path, status, body)
		}
	}
	h.clock.now = h.clock.now.Add(time.Second)
	_ = h.reconcile()
	if h.probes.Load() != 1 || h.submits.Load() != 3 {
		t.Fatalf("not one shared probe: probes=%d submit=%d", h.probes.Load(), h.submits.Load())
	}
	// Reconcile at the normal one-second cadence through a longer outage.
	// There is one ledger probe per due time, rather than one request per Run
	// on every tick; the schedule reaches its one-minute cap.
	heldBearer := stagedBearer(t, h.l3Store, runs[1])
	for i := 0; i < 125; i++ {
		h.clock.now = h.clock.now.Add(time.Second)
		before := h.probes.Load()
		_ = h.reconcile()
		if h.probes.Load()-before > 1 {
			t.Fatal("more than one probe in one tick")
		}
		if h.probes.Load() > before && h.probes.Load() >= 7 {
			state, _ := h.l3Store.DispatchHealth(ctx)
			if state.DispatchHold == nil || state.DispatchHold.NextProbeAt.Sub(h.clock.now) != time.Minute {
				t.Fatal("probe backoff did not reach the one-minute cap")
			}
		}
	}
	if h.probes.Load() > 9 || h.submits.Load() != 3 {
		t.Fatalf("misconfigured ledger flooded L1: probes=%d submits=%d", h.probes.Load(), h.submits.Load())
	}
	health, _ = h.l3Store.DispatchHealth(ctx)
	if health.DispatchHold == nil || health.DispatchHold.NextProbeAt.Sub(h.clock.now) > time.Minute {
		t.Fatal("probe schedule exceeds one minute")
	}
	if stagedBearer(t, h.l3Store, runs[1]) != heldBearer {
		t.Fatal("long hold rotated bearer")
	}
	if err := h.l3Store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(h.l3Path, StoreOptions{Clock: h.clock})
	if err != nil {
		t.Fatal(err)
	}
	h.l3Store = reopened
	h.l3Server.store = reopened
	health, _ = reopened.DispatchHealth(ctx)
	if health.DispatchHold == nil || !health.DispatchHold.Since.Equal(hold.Since) {
		t.Fatal("restart lost hold")
	}
	// Cancel wins after the hold read but before probe release and beginDispatch.
	h.probeEntered = make(chan struct{})
	h.probeRelease = make(chan struct{})
	h.configured.Store(true)
	// Advance no more than the admission backoff cap after the fix.
	h.clock.now = h.clock.now.Add(time.Minute)
	finished := make(chan error, 1)
	go func() { finished <- h.reconcile() }()
	<-h.probeEntered
	status, _, body = h.do(h.caller, "POST", "/v1/runs/"+runs[2]+"/cancel", nil, nil)
	if status != 200 {
		t.Fatalf("local held cancel: %d %s", status, body)
	}
	close(h.probeRelease)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	canceled, _ := h.l3Store.GetRun(ctx, runs[2])
	if canceled.Status != contract.RunFailed || canceled.L1JobID != "" || canceled.DispatchHold != nil {
		t.Fatalf("cancel lost race: %+v", canceled)
	}
	for _, id := range append([]string{runs[0], runs[1]}, runs[3:]...) {
		record, _ := h.l3Store.GetRun(ctx, id)
		if record.Status != contract.RunQueued || record.DispatchHold != nil {
			t.Fatalf("waiting Run not released within minute: %+v", record)
		}
	}
}

func TestLedgerIdentityProxyRoutesAndFrontDoor(t *testing.T) {
	for _, failure := range []string{"WhoIs", "old unauthorized", "old principal", "not admitted"} {
		t.Run(failure, func(t *testing.T) {
			h := newHoldHTTPHarness(t)
			run := h.submit(inlineRunRequest("exit 0\n"), "proxy")
			if err := h.reconcile(); err != nil {
				t.Fatal(err)
			}
			if failure == "WhoIs" {
				h.identity.failure.Store(1)
			} else if failure == "not admitted" {
				h.configured.Store(false)
			} else {
				transport := h.l1Client.client.Transport
				h.l1Client.client.Transport = recoveryRoundTripper(func(req *http.Request) (*http.Response, error) {
					code, status := "unauthorized", 401
					if failure == "old principal" {
						code, status = "principal_forbidden", 403
					}
					body := fmt.Sprintf(`{"error":{"code":%q,"message":"old identity failure","retryable":false}}`, code)
					return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
				})
				defer func() { h.l1Client.client.Transport = transport }()
			}
			host := h.client(fabric.Identity{NodeID: "host"}, DefaultL3Address)
			paths := []struct {
				client       *http.Client
				method, path string
				body         any
			}{
				{h.caller, "GET", "/v1/runs/" + run.RunID + "/execution", nil},
				{h.caller, "GET", "/v1/runs/" + run.RunID + "/logs", nil},
				{h.caller, "GET", "/v1/runs/" + run.RunID + "/result", nil},
				{host, "POST", "/v1/computer-token/mint", ComputerTokenMintRequest{ComputerID: "missing", ComputerAttemptID: "attempt"}},
				{host, "POST", "/v1/computer-token/revoke-host", HostComputerTokenRevocationRequest{StableNodeID: "host", BootSessionID: "boot", Reason: "startup"}},
			}
			for _, p := range paths {
				// Misconfiguration affects ledger-only proofs; ordinary job reads continue
				// to authenticate the same client principal and are not identity failures.
				if failure == "not admitted" && p.method == "GET" {
					continue
				}
				status, _, body := h.do(p.client, p.method, p.path, p.body, nil)
				var envelope contract.ErrorResponse
				json.Unmarshal(body, &envelope)
				if status != 503 || envelope.Error.Code != contract.ErrorUnavailable || !envelope.Error.Retryable {
					t.Fatalf("identity proxy %s: %d %s", p.path, status, body)
				}
			}
			// verifyComputerScope must not relabel ledger identity as invalid scope.
			err := h.l3Server.verifyComputerScope(context.Background(), ComputerTokenScope{ComputerID: "missing", ComputerAttemptID: "attempt"}, "host")
			code, retry := errorDetails(err)
			if code != contract.ErrorUnavailable || !retry {
				t.Fatalf("scope identity mislabeled: %v", err)
			}
		})
	}
	h := newHoldHTTPHarness(t)
	for _, n := range []int32{1, 2} {
		h.ledgerIdentity.failure.Store(n)
		status, _, body := h.do(h.caller, "GET", "/v1/runs", nil, nil)
		var envelope contract.ErrorResponse
		json.Unmarshal(body, &envelope)
		if n == 1 {
			if status != 503 || envelope.Error.Code != contract.ErrorUnavailable || envelope.Error.Details["reason"] != "identity_unverifiable" || !envelope.Error.Retryable {
				t.Fatalf("front door: %d %s", status, body)
			}
		} else if status != 401 || envelope.Error.Code != contract.ErrorUnauthorized {
			t.Fatalf("identity absence: %d %s", status, body)
		}
	}
}

func TestLedgerBadComputerProofDoesNotHoldHealthyDispatch(t *testing.T) {
	h := newHoldHTTPHarness(t)
	host := h.client(fabric.Identity{NodeID: "host"}, DefaultL3Address)
	// Real L1 refuses the missing resource; no ledger-wide admission evidence.
	status, _, body := h.do(host, "POST", "/v1/computer-token/mint", ComputerTokenMintRequest{ComputerID: "missing", ComputerAttemptID: "attempt"}, nil)
	if status != 403 {
		t.Fatalf("bad resource proof: %d %s", status, body)
	}
	health, err := h.l3Store.DispatchHealth(context.Background())
	if err != nil || health.DispatchHold != nil {
		t.Fatalf("resource refusal held ledger: %+v %v", health, err)
	}
	for i := 0; i < 3; i++ {
		run := h.submit(inlineRunRequest("exit 0\n"), fmt.Sprintf("healthy-%d", i))
		if err := h.reconcile(); err != nil {
			t.Fatal(err)
		}
		record, _ := h.l3Store.GetRun(context.Background(), run.RunID)
		if record.Status != contract.RunQueued {
			t.Fatalf("healthy dispatch stalled: %+v", record)
		}
	}
	h.identity.failure.Store(1)
	blocked := h.submit(inlineRunRequest("exit 0\n"), "ledger-refusal-after-resource")
	_ = h.reconcile()
	assertHeld(t, h.l3Store, blocked.RunID)
}

func TestLedgerProbeSingleReservationAndGenerationFence(t *testing.T) {
	h := newHoldHTTPHarness(t)
	ctx := context.Background()
	h.identity.failure.Store(1)
	run := h.submit(inlineRunRequest("exit 0\n"), "reservation")
	_ = h.reconcile()
	assertHeld(t, h.l3Store, run.RunID)
	h.identity.failure.Store(0)
	h.clock.now = h.clock.now.Add(time.Second)
	h.probeEntered = make(chan struct{})
	h.probeRelease = make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- h.reconcile() }()
	<-h.probeEntered
	// A second reconciler sees the persisted reservation while the HTTP call is
	// in flight and cannot probe or submit alongside it.
	if err := h.reconcile(); err != nil {
		t.Fatal(err)
	}
	if h.probes.Load() != 1 || h.submits.Load() != 1 {
		t.Fatalf("duplicate reservation: probes=%d submits=%d", h.probes.Load(), h.submits.Load())
	}
	// A new independent refusal advances the generation before the late 404.
	if err := h.l3Store.holdDispatch(ctx, "principal_forbidden"); err != nil {
		t.Fatal(err)
	}
	close(h.probeRelease)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	assertHeld(t, h.l3Store, run.RunID)
	health, _ := h.l3Store.DispatchHealth(ctx)
	if health.DispatchHold.Reason != "principal_forbidden" {
		t.Fatalf("late success cleared newer generation: %+v", health)
	}
}

func TestLedgerProbeRejectsMalformedRedirectAndNoRoute(t *testing.T) {
	for _, answer := range []string{"no_route", "partial", "redirect", "malformed 200", "partial bound 200", "wrong key 200"} {
		t.Run(answer, func(t *testing.T) {
			h := newHoldHTTPHarness(t)
			h.identity.failure.Store(1)
			run := h.submit(inlineRunRequest("exit 0\n"), "invalid-probe")
			_ = h.reconcile()
			h.clock.now = h.clock.now.Add(time.Second)
			h.identity.failure.Store(0)
			calls := 0
			listener, err := h.network.NewFabric(fabric.Identity{NodeID: "test-proxy"}).Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				switch answer {
				case "no_route":
					w.WriteHeader(404)
					io.WriteString(w, `{"error":{"code":"not_found","message":"mux","retryable":false,"details":{"reason":"no_route"}}}`)
				case "partial":
					w.WriteHeader(404)
					io.WriteString(w, `{"error":{"code":"not_found"}}`)
				case "redirect":
					w.Header().Set("Location", "/v1/dispatch-keys/final/job")
					w.WriteHeader(302)
				case "malformed 200":
					io.WriteString(w, `{"job_id":"incomplete"}`)
				case "partial bound 200":
					key, _ := url.PathUnescape(strings.TrimSuffix(strings.TrimPrefix(r.URL.EscapedPath(), "/v1/dispatch-keys/"), "/job"))
					fmt.Fprintf(w, `{"job_id":"j","state":"queued","spec":{"dispatch_key":%q}}`, key)
				case "wrong key 200":
					io.WriteString(w, `{"job_id":"j","state":"queued","spec":{"dispatch_key":"wrong"}}`)
				}
			})}
			done := make(chan error, 1)
			go func() { done <- srv.Serve(listener) }()
			defer func() { srv.Close(); <-done }()
			ledger := h.network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
			invalid, err := NewL1Client(ledger, listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer invalid.CloseIdleConnections()
			r, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{DispatchLookup: invalid})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.ReconcileOnce(context.Background()); err == nil {
				t.Fatal("invalid probe not reported")
			}
			assertHeld(t, h.l3Store, run.RunID)
			if calls != 1 {
				t.Fatalf("redirect followed: %d requests", calls)
			}
		})
	}
}

func TestLedgerTransientDispatchBackoff(t *testing.T) {
	h := newHoldHTTPHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("exit 0\n"), "retry")
	calls := 0
	transport := h.l1Client.client.Transport
	h.l1Client.client.Transport = recoveryRoundTripper(func(req *http.Request) (*http.Response, error) {
		if req.Method == "POST" {
			calls++
			return nil, errors.New("transport down")
		}
		return transport.RoundTrip(req)
	})
	_ = h.reconcile()
	token := stagedBearer(t, h.l3Store, run.RunID)
	_ = h.reconcile()
	if calls != 1 {
		t.Fatal("transient dispatch retries on every pass")
	}
	for i := 0; i < 9; i++ {
		var due int64
		if err := h.l3Store.db.QueryRow(`SELECT retry_ns FROM dispatch_retry WHERE run_id=?`, run.RunID).Scan(&due); err != nil {
			t.Fatal(err)
		}
		delay := time.Unix(0, due).Sub(h.clock.now)
		if delay <= 0 || delay > time.Minute {
			t.Fatalf("bad retry cap %s", delay)
		}
		h.clock.now = time.Unix(0, due)
		_ = h.reconcile()
		if calls != i+2 {
			t.Fatalf("due submission not retried: %d", calls)
		}
		if stagedBearer(t, h.l3Store, run.RunID) != token {
			t.Fatal("retry rotated token")
		}
	}
	h.l1Client.client.Transport = transport
	h.clock.now = h.clock.now.Add(time.Minute)
	if err := h.reconcile(); err != nil {
		t.Fatal(err)
	}
	record, _ := h.l3Store.GetRun(ctx, run.RunID)
	if record.Status != contract.RunQueued {
		t.Fatalf("backoff didn't recover: %+v", record)
	}
}

func TestLedgerAbandonedProbeResumesAfterRestart(t *testing.T) {
	h := newHoldHTTPHarness(t)
	ctx := context.Background()
	h.identity.failure.Store(1)
	run := h.submit(inlineRunRequest("exit 0\n"), "abandoned-probe")
	_ = h.reconcile()
	assertHeld(t, h.l3Store, run.RunID)
	h.clock.now = h.clock.now.Add(time.Second)
	old, err := h.l3Store.reserveDispatchProbe(ctx, DefaultDispatchRecoveryBudget)
	if err != nil || old == nil {
		t.Fatalf("no reservation: %+v %v", old, err)
	}
	reopened, err := OpenStore(h.l3Path, StoreOptions{Clock: h.clock})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if p, err := reopened.reserveDispatchProbe(ctx, DefaultDispatchRecoveryBudget); err != nil || p != nil {
		t.Fatalf("restart duplicated live probe: %+v %v", p, err)
	}
	h.clock.now = h.clock.now.Add(DefaultDispatchRecoveryBudget + time.Second)
	next, err := reopened.reserveDispatchProbe(ctx, DefaultDispatchRecoveryBudget)
	if err != nil || next == nil || next.id == old.id {
		t.Fatalf("restart didn't resume expired probe: %+v %v", next, err)
	}
	if err := h.l3Store.finishDispatchProbe(ctx, old, true); err != nil {
		t.Fatal(err)
	}
	assertHeld(t, reopened, run.RunID)
	if err := reopened.finishDispatchProbe(ctx, next, false); err != nil {
		t.Fatal(err)
	}
	h.identity.failure.Store(0)
	h.clock.now = h.clock.now.Add(time.Minute)
	if err := h.reconcile(); err != nil {
		t.Fatal(err)
	}
	record, _ := h.l3Store.GetRun(ctx, run.RunID)
	if record.Status != contract.RunQueued {
		t.Fatalf("restarted probe didn't dispatch: %+v", record)
	}
}

func TestLedgerHoldReadsDoNotReserveSQLiteWriter(t *testing.T) {
	h := newHoldHTTPHarness(t)
	h.identity.failure.Store(1)
	run := h.submit(inlineRunRequest("exit 0\n"), "read-lock")
	_ = h.reconcile()
	assertHeld(t, h.l3Store, run.RunID)
	// A reserved immediate writer must not prevent the new diagnostic reads.
	tx, err := h.l3Store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if p, err := h.l3Store.GetRunExecution(ctx, run.RunID); err != nil || p.DispatchHold == nil {
		t.Fatalf("execution read reserved writer: %+v %v", p, err)
	}
	if health, err := h.l3Store.DispatchHealth(ctx); err != nil || health.DispatchHold == nil {
		t.Fatalf("health read reserved writer: %+v %v", health, err)
	}
	if page, err := h.l3Store.ListRuns(ctx, RunListFilter{}); err != nil || len(page.Runs) != 1 || page.Runs[0].DispatchHold == nil {
		t.Fatalf("list read reserved writer: %+v %v", page, err)
	}
}
