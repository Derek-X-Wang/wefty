//go:build darwin || linux

package agent

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const preAdmissionTraceCapacity = 512

// Entries contain only curated values, never buffers or arbitrary error messages.
type preAdmissionFailureEntry struct {
	at     time.Time
	phase  string
	values string
}
type preAdmissionFailureTrace struct {
	mu                       sync.Mutex
	start                    time.Time
	entries                  [preAdmissionTraceCapacity]preAdmissionFailureEntry
	next, total, connections int
	firstError               *preAdmissionFailureEntry
}

func newPreAdmissionFailureTrace() *preAdmissionFailureTrace {
	return &preAdmissionFailureTrace{start: time.Now()}
}
func preAdmissionErrorCode(err error) string {
	if err == nil {
		return "none"
	}
	var loss *ocihelper.RuntimeLossError
	if errors.As(err, &loss) {
		return "runtime_loss/" + preAdmissionErrorCode(loss.Cause)
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, io.EOF) {
		return "eof"
	}
	if errors.Is(err, net.ErrClosed) {
		return "closed"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "transport_timeout"
	}
	return "other_error"
}
func preAdmissionSafeValue(value any) string {
	switch v := value.(type) {
	case nil:
		return "none"
	case error:
		return preAdmissionErrorCode(v)
	case bool:
		return fmt.Sprint(v)
	case int:
		return fmt.Sprint(v)
	case uint64:
		return fmt.Sprint(v)
	case time.Duration:
		return v.String()
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case contract.RuntimeFailureCode:
		if v == contract.RuntimeFailureUnavailable {
			return string(v)
		}
		return "unrecognized_runtime_code"
	case contract.SpawnFailureCode:
		switch v {
		case contract.SpawnFailureUnsupportedClass, contract.SpawnFailureUnsupportedKind, contract.SpawnFailureUnsupportedRuntimeHandler, contract.SpawnFailureManagedResourcePreparation, contract.SpawnFailureHandoffPreparation, contract.SpawnFailureExecutableMaterialization, contract.SpawnFailureWorkflowBridgeCreation, contract.SpawnFailureLogSinkSetup, contract.SpawnFailureProcessRequest, contract.SpawnFailureProcessGroupSetup, contract.SpawnFailureProcessSpawn, contract.SpawnFailureProcessWait, contract.SpawnFailurePublishedPortOccupied, contract.SpawnFailurePublishedListener, contract.SpawnFailureStartupReadinessTimeout, contract.SpawnFailureRuntimeUnavailable, contract.SpawnFailureImageUnavailable, contract.SpawnFailureImageNotFound, contract.SpawnFailureImageManifestInvalid, contract.SpawnFailureImagePlatformUnsupported, contract.SpawnFailureOCISpecRejected, contract.SpawnFailureInsufficientMemory, contract.SpawnFailureInsufficientDisk, contract.SpawnFailureReconfigurationAborted, contract.SpawnFailureReimagePreflight, contract.SpawnFailurePassUnavailable:
			return string(v)
		}
		return "other_spawn_code"
	case contract.AttemptState:
		switch v {
		case contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput, contract.AttemptSucceeded, contract.AttemptFailed, contract.AttemptLost:
			return string(v)
		}
		return "other_attempt_state"
	case contract.TerminationCause:
		switch v {
		case "", contract.TerminationCauseSpontaneous, contract.TerminationCauseAgent, contract.TerminationCauseGuardian:
			return string(v)
		}
		return "unrecognized_termination"
	case string:
		return fmt.Sprintf("fingerprint:%x", sha256.Sum256([]byte(v)))
	default:
		return "unreported_type"
	}
}
func (p *preAdmissionFailureTrace) add(phase string, values ...any) {
	safe := make([]string, len(values))
	failed := false
	for i, v := range values {
		safe[i] = preAdmissionSafeValue(v)
		if err, ok := v.(error); ok && err != nil {
			failed = true
		}
	}
	e := preAdmissionFailureEntry{time.Now(), phase, strings.Join(safe, " ")}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.entries[p.next] = e
	p.next = (p.next + 1) % len(p.entries)
	p.total++
	if failed && p.firstError == nil {
		saved := e
		p.firstError = &saved
	}
}
func (p *preAdmissionFailureTrace) emit(logf func(string, ...any)) {
	p.mu.Lock()
	n := min(p.total, len(p.entries))
	start := 0
	if p.total > len(p.entries) {
		start = p.next
	}
	entries := make([]preAdmissionFailureEntry, n)
	for i := range entries {
		entries[i] = p.entries[(start+i)%len(p.entries)]
	}
	total := p.total
	var first *preAdmissionFailureEntry
	if p.firstError != nil {
		copy := *p.firstError
		first = &copy
	}
	p.mu.Unlock()
	logf("pre-admission evidence retained=%d overwritten=%d", n, total-n)
	if first != nil {
		logf("pre-admission first-error +%s phase=%s %s", first.at.Sub(p.start), first.phase, first.values)
	}
	for _, e := range entries {
		logf("pre-admission +%s phase=%s %s", e.at.Sub(p.start), e.phase, e.values)
	}
}
func (p *preAdmissionFailureTrace) serverLogf(format string, args ...any) {
	const allowed = "OCI helper session invalidated reason=heartbeat_rejected session_generation=%d attempt_id=%q rejection_code=%s"
	if format != allowed || len(args) != 3 {
		return
	}
	generation, _ := args[0].(uint64)
	code := "other_rejection"
	if v, ok := args[2].(ocihelper.ErrorCode); ok && v == ocihelper.CodeUnauthorizedAttempt {
		code = "unauthorized_attempt"
	}
	// No caller-supplied message, attempt string, or capability is rendered.
	p.add("server heartbeat rejected/"+code, generation)
}
func (p *preAdmissionFailureTrace) attempt(a l1.Attempt) {
	p.add("durable attempt identity/state/lease/updated", a.AttemptID, a.State, a.LeaseExpiresAt, a.UpdatedAt)
	if a.Result == nil {
		p.add("durable result absent")
		return
	}
	r := a.Result
	exit := 0
	if r.ExitCode != nil {
		exit = *r.ExitCode
	}
	p.add("result exit-present/exit/termination/incomplete/output-present", r.ExitCode != nil, exit, r.TerminationCause, r.LogEvidenceIncomplete, r.OutputError != "")
	if r.SpawnError != nil {
		p.add("result spawn code", r.SpawnError.Code)
	}
	if r.RuntimeFailure != nil {
		p.add("result runtime code", r.RuntimeFailure.Code)
	}
}

type preAdmissionFailureConn struct {
	net.Conn
	trace                       *preAdmissionFailureTrace
	id                          int
	mu                          sync.Mutex
	readDeadline, writeDeadline time.Time
}

func (c *preAdmissionFailureConn) Read(b []byte) (int, error) {
	start := time.Now()
	c.mu.Lock()
	deadline := c.readDeadline
	c.mu.Unlock()
	n, err := c.Conn.Read(b)
	c.trace.add("connection read", c.id, time.Since(start), deadline, n, err)
	return n, err
}
func (c *preAdmissionFailureConn) Write(b []byte) (int, error) {
	start := time.Now()
	c.mu.Lock()
	deadline := c.writeDeadline
	c.mu.Unlock()
	n, err := c.Conn.Write(b)
	c.trace.add("connection write", c.id, time.Since(start), deadline, n, err)
	return n, err
}
func (c *preAdmissionFailureConn) Close() error {
	c.trace.add("connection close entry", c.id)
	err := c.Conn.Close()
	c.trace.add("connection close exit", c.id, err)
	return err
}
func (c *preAdmissionFailureConn) SetDeadline(d time.Time) error {
	err := c.Conn.SetDeadline(d)
	if err == nil {
		c.mu.Lock()
		c.readDeadline = d
		c.writeDeadline = d
		c.mu.Unlock()
	}
	c.trace.add("connection deadline", c.id, d, err)
	return err
}
func (c *preAdmissionFailureConn) SetReadDeadline(d time.Time) error {
	err := c.Conn.SetReadDeadline(d)
	if err == nil {
		c.mu.Lock()
		c.readDeadline = d
		c.mu.Unlock()
	}
	c.trace.add("connection read deadline", c.id, d, err)
	return err
}
func (c *preAdmissionFailureConn) SetWriteDeadline(d time.Time) error {
	err := c.Conn.SetWriteDeadline(d)
	if err == nil {
		c.mu.Lock()
		c.writeDeadline = d
		c.mu.Unlock()
	}
	c.trace.add("connection write deadline", c.id, d, err)
	return err
}

type preAdmissionFailureEngine struct {
	*preAdmissionRenewalEngine
	trace *preAdmissionFailureTrace
}

func (e preAdmissionFailureEngine) Run(ctx context.Context, r ocihelper.RunRequest) (ocihelper.RunResponse, error) {
	deadline, _ := ctx.Deadline()
	e.trace.add("engine Run entry", r.Authority.AttemptID, r.InitialDeadman, deadline)
	v, err := e.preAdmissionRenewalEngine.Run(ctx, r)
	e.trace.add("engine Run exit", err, context.Cause(ctx))
	return v, err
}
func (e preAdmissionFailureEngine) Watch(ctx context.Context, r ocihelper.WatchRequest, emit func(ocihelper.WatchEvent) error) error {
	deadline, _ := ctx.Deadline()
	e.trace.add("engine Watch entry", r.Authority.AttemptID, deadline)
	err := e.preAdmissionRenewalEngine.Watch(ctx, r, emit)
	e.trace.add("engine Watch exit", err, context.Cause(ctx))
	return err
}
func (e preAdmissionFailureEngine) ReapAttempt(ctx context.Context, a ocihelper.AttemptAuthority) error {
	deadline, _ := ctx.Deadline()
	e.trace.add("engine ReapAttempt entry", a.AttemptID, deadline)
	err := e.preAdmissionRenewalEngine.ReapAttempt(ctx, a)
	e.trace.add("engine ReapAttempt exit", err, context.Cause(ctx))
	return err
}
func (e preAdmissionFailureEngine) ReapSession(ctx context.Context, s ocihelper.SessionIdentity) (ocihelper.SweepResponse, error) {
	deadline, _ := ctx.Deadline()
	e.trace.add("engine ReapSession entry", deadline)
	v, err := e.preAdmissionRenewalEngine.ReapSession(ctx, s)
	e.trace.add("engine ReapSession exit", err, context.Cause(ctx))
	return v, err
}
func startPreAdmissionFailureHelper(t *testing.T, engine ocihelper.Engine, now func() time.Time, trace *preAdmissionFailureTrace) (*ocihelper.BootBarrier, func()) {
	t.Helper()
	directory, err := os.MkdirTemp("", "wefty-332-")
	if err != nil {
		trace.add("helper setup error", err)
		trace.emit(t.Logf)
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "helper.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		trace.add("helper setup error", err)
		trace.emit(t.Logf)
		t.Fatal(err)
	}
	server, err := ocihelper.NewServer(engine, ocihelper.ServerConfig{
		HelperChecksum: "checksum-test", AllowedUIDs: []uint32{uint32(os.Getuid())},
		HeartbeatTimeout: time.Second, MaximumAttemptDeadman: 5 * time.Second, Logf: trace.serverLogf,
	})
	if err != nil {
		_ = listener.Close()
		trace.add("helper setup error", err)
		trace.emit(t.Logf)
		t.Fatal(err)
	}
	serveContext, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveContext, listener) }()
	client := ocihelper.NewUnixClient(path, "checksum-test")
	client.HeartbeatInterval = 20 * time.Millisecond
	client.Now = now
	originalDial := client.Dial
	client.Dial = func(ctx context.Context) (net.Conn, error) {
		conn, err := originalDial(ctx)
		trace.add("dial returned", err, context.Cause(ctx))
		if err != nil {
			return nil, err
		}
		trace.mu.Lock()
		trace.connections++
		id := trace.connections
		trace.mu.Unlock()
		return &preAdmissionFailureConn{Conn: conn, trace: trace, id: id}, nil
	}
	barrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{
		NodeID: "pre-admission-node", BootSessionID: "pre-admission-boot",
	})
	if err != nil {
		cancelServe()
		_ = listener.Close()
		<-serveDone
		trace.add("helper setup error", err)
		trace.emit(t.Logf)
		t.Fatal(err)
	}
	return barrier, func() {
		_ = barrier.Close()
		cancelServe()
		_ = listener.Close()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("serve helper: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("helper server did not stop")
			<-serveDone
		}
	}
}

func TestPreAdmissionFailureEvidenceRetentionAndPrivacy(t *testing.T) {
	trace := newPreAdmissionFailureTrace()
	secret := "secret-capability-and-spec-payload"
	deadline := time.Unix(20000, 0)
	trace.add("connection read", 1, time.Millisecond, deadline, 0, fmt.Errorf("%s: %w", secret, io.EOF))
	for i := 0; i < preAdmissionTraceCapacity+3; i++ {
		trace.add("retention control", i)
	}
	trace.serverLogf(secret, secret)
	trace.serverLogf("OCI helper session invalidated reason=heartbeat_rejected session_generation=%d attempt_id=%q rejection_code=%s", uint64(7), secret, ocihelper.CodeUnauthorizedAttempt)
	trace.attempt(l1.Attempt{AttemptID: secret, State: contract.AttemptFailed, Result: &l1.ProcessResult{
		RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: secret}, OutputError: secret,
		SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureRuntimeUnavailable, Message: secret},
	}})
	var output strings.Builder
	trace.emit(func(format string, args ...any) { fmt.Fprintf(&output, format+"\n", args...) })
	text := output.String()
	for _, want := range []string{"retained=512", "first-error", "phase=connection read 1 1ms", deadline.Format(time.RFC3339Nano), "eof", "heartbeat rejected/unauthorized_attempt", "runtime_unavailable", "result runtime code", "result spawn code", "result exit-present/exit/termination/incomplete/output-present false 0"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in evidence", want)
		}
	}
	if strings.Contains(text, secret) {
		t.Fatal("diagnostics exposed raw payload, error or server text")
	}
	if strings.Contains(text, "phase=retention control 0\n") {
		t.Fatal("ring failed to overwrite oldest event")
	}
	if got := strings.Count(text, "pre-admission +"); got != preAdmissionTraceCapacity {
		t.Fatalf("retained entries=%d", got)
	}
	if strings.Count(text, "pre-admission first-error") != 1 {
		t.Fatal("first error was lost or duplicated")
	}
	if got := preAdmissionSafeValue(contract.RuntimeFailureCode(secret)); got != "unrecognized_runtime_code" {
		t.Fatal("unknown runtime code not excluded")
	}
	if got := preAdmissionSafeValue(contract.SpawnFailureCode(secret)); got != "other_spawn_code" {
		t.Fatal("unknown spawn code not excluded")
	}
}
