package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// The run mailbox is the job-owned directory in which a workload writes its
// envelopes, phases, gate results and result files, and from which this agent
// publishes them to the ledger under authority the workload never holds. The
// workload writes plain text; building protocol JSON that satisfies the v1
// schemas is the agent's job, which is what lets a bash job report without
// hand-rolling either HTTP or JSON escaping.
const (
	runMailboxDirectoryName          = ".wefty"
	runMailboxEventsDirectoryName    = "events"
	runMailboxStagingDirectoryName   = "tmp"
	runMailboxPublishedDirectoryName = ".published"
	runMailboxParamsFileName         = "params.json"
	runMailboxProtocolHeader         = "wefty-protocol"
	runMailboxProtocolVersion        = "1"
	runMailboxExtensionNamespace     = "dev.wefty.mailbox"
)

// Bounds. A mailbox is a workload-writable directory, so every one of these is
// enforced by the agent and never by the producer.
const (
	// MaxRunMailboxEventBytes bounds one event file. A larger file is
	// truncated with an explicit marker rather than dropped: losing the tail
	// of a failing gate's output is recoverable, losing the verdict is not.
	MaxRunMailboxEventBytes = 64 << 10
	// MaxRunMailboxEvents bounds how many events one attempt may publish.
	MaxRunMailboxEvents = 1024
	// MaxRunMailboxTotalBytes bounds the published payload for one attempt.
	MaxRunMailboxTotalBytes = 1 << 20
	// MaxRunMailboxParamsBytes bounds the params document the agent writes.
	MaxRunMailboxParamsBytes = 64 << 10
	// MaxRunMailboxRejections bounds how many malformed events are reported
	// into the ledger before the agent only logs them.
	MaxRunMailboxRejections = 8
	// DefaultRunMailboxPollInterval is how often a running attempt's mailbox
	// is swept. Phase timestamps are therefore accurate to this interval, not
	// to the instant the workload wrote the file.
	DefaultRunMailboxPollInterval = 500 * time.Millisecond

	runMailboxTruncationNotice = "\n[truncated by the node agent: run mailbox event exceeded the size bound]"
)

// runLedgerAppender publishes one protocol document to L3 on a run's behalf.
// The agent holds the run token; the workload does not.
type runLedgerAppender interface {
	appendRunDocument(ctx context.Context, runToken, runID, collection string, body []byte) error
}

const (
	runLedgerEnvelopeCollection = "envelopes"
	runLedgerGateCollection     = "gates"
)

// runLedgerRejection is L3 refusing a document on its own merits. Retrying it
// unchanged cannot succeed, so the event is retired instead of republished.
type runLedgerRejection struct {
	statusCode int
	body       string
}

func (e *runLedgerRejection) Error() string {
	return fmt.Sprintf("run ledger rejected the document with HTTP %d: %s", e.statusCode, e.body)
}

type httpRunLedgerAppender struct {
	client *http.Client
}

// newFabricRunLedgerAppender dials L3 over the agent's own authenticated
// Fabric connection — the same transport the attempt-local bridge proxies —
// and supplies run authorization from the run token the agent holds.
func newFabricRunLedgerAppender(participant fabric.Fabric, address string) runLedgerAppender {
	if participant == nil || strings.TrimSpace(address) == "" {
		return nil
	}
	return &httpRunLedgerAppender{client: &http.Client{
		Transport: workflowBridgeTransport(participant, address),
		Timeout:   30 * time.Second,
	}}
}

func (a *httpRunLedgerAppender) appendRunDocument(ctx context.Context, runToken, runID, collection string, body []byte) error {
	endpoint := "http://wefty.invalid/v1/runs/" + url.PathEscape(runID) + "/" + collection
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Host = "wefty.invalid"
	request.Header.Set("Authorization", "Bearer "+runToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := a.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
	switch {
	case response.StatusCode == http.StatusOK || response.StatusCode == http.StatusCreated:
		return nil
	case response.StatusCode >= 400 && response.StatusCode < 500:
		return &runLedgerRejection{statusCode: response.StatusCode, body: string(bytes.TrimSpace(payload))}
	default:
		return fmt.Errorf("run ledger returned HTTP %d: %s", response.StatusCode, bytes.TrimSpace(payload))
	}
}

// runMailboxEvent is one parsed protocol file. Field values are the workload's
// own claims about its own run; nothing here carries authority.
type runMailboxEvent struct {
	kind        string
	name        string
	step        string
	status      string
	outcome     string
	summary     string
	key         string
	payloadJSON bool
	createdAt   time.Time
	body        []byte
	truncated   bool
}

const (
	runMailboxKindEnvelope = "envelope"
	runMailboxKindPhase    = "phase"
	runMailboxKindGate     = "gate"
	runMailboxKindResult   = "result"
)

var (
	runMailboxKinds          = []string{runMailboxKindEnvelope, runMailboxKindPhase, runMailboxKindGate, runMailboxKindResult}
	runMailboxStatuses       = []string{string(contract.EnvelopeSucceeded), string(contract.EnvelopeFailed), string(contract.EnvelopePartial)}
	runMailboxPhaseStatuses  = []string{"started", "ended"}
	runMailboxGateOutcomes   = []string{string(contract.GatePass), string(contract.GateFail), string(contract.GateError), string(contract.GateSkipped)}
	runMailboxPayloadFormats = []string{"text", "json"}
)

// parseRunMailboxEvent reads the line-oriented header block, the `--`
// separator and the raw payload. The payload is never escaped by the producer:
// that is the point of the format.
func parseRunMailboxEvent(raw []byte) (runMailboxEvent, error) {
	headerBlock, body, separated := bytes.Cut(raw, []byte("\n--\n"))
	if !separated {
		trimmed, hadTrailer := bytes.CutSuffix(raw, []byte("\n--\n"))
		if !hadTrailer {
			trimmed, hadTrailer = bytes.CutSuffix(raw, []byte("\n--"))
		}
		if !hadTrailer {
			return runMailboxEvent{}, errors.New("run mailbox event has no \"--\" payload separator")
		}
		headerBlock, body = trimmed, nil
	}
	event := runMailboxEvent{body: body}
	seen := map[string]bool{}
	headers := 0
	for _, line := range strings.Split(string(headerBlock), "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return runMailboxEvent{}, fmt.Errorf("run mailbox header %q is not \"name: value\"", line)
		}
		name = strings.ToLower(strings.TrimSpace(name))
		value = strings.TrimSpace(value)
		headers++
		if headers == 1 && name != runMailboxProtocolHeader {
			return runMailboxEvent{}, fmt.Errorf("run mailbox event must start with %q", runMailboxProtocolHeader+": "+runMailboxProtocolVersion)
		}
		if seen[name] {
			return runMailboxEvent{}, fmt.Errorf("run mailbox header %q appears twice", name)
		}
		seen[name] = true
		switch name {
		case runMailboxProtocolHeader:
			if value != runMailboxProtocolVersion {
				return runMailboxEvent{}, fmt.Errorf("run mailbox protocol version %q is unsupported", value)
			}
		case "kind":
			event.kind = value
		case "name":
			event.name = value
		case "step":
			event.step = value
		case "status":
			event.status = value
		case "outcome":
			event.outcome = value
		case "summary":
			event.summary = value
		case "key":
			event.key = value
		case "payload":
			event.payloadJSON = value == "json"
			if !slices.Contains(runMailboxPayloadFormats, value) {
				return runMailboxEvent{}, fmt.Errorf("run mailbox payload format %q is unsupported", value)
			}
		case "created-at":
			parsed, err := time.Parse(time.RFC3339, value)
			if err != nil {
				return runMailboxEvent{}, fmt.Errorf("run mailbox created-at %q is not RFC3339", value)
			}
			event.createdAt = parsed.UTC()
		default:
			return runMailboxEvent{}, fmt.Errorf("run mailbox header %q is not part of the protocol", name)
		}
	}
	if !slices.Contains(runMailboxKinds, event.kind) {
		return runMailboxEvent{}, fmt.Errorf("run mailbox kind %q is not one of %s", event.kind, strings.Join(runMailboxKinds, ", "))
	}
	if event.name == "" {
		event.name = event.step
	}
	if event.step == "" {
		event.step = event.name
	}
	switch event.kind {
	case runMailboxKindGate:
		if event.name == "" {
			return runMailboxEvent{}, errors.New("run mailbox gate requires a name")
		}
		if !slices.Contains(runMailboxGateOutcomes, event.outcome) {
			return runMailboxEvent{}, fmt.Errorf("run mailbox gate outcome %q is not one of %s", event.outcome, strings.Join(runMailboxGateOutcomes, ", "))
		}
	case runMailboxKindPhase:
		if event.name == "" {
			return runMailboxEvent{}, errors.New("run mailbox phase requires a name")
		}
		if event.status == "" {
			event.status = "started"
		}
		if !slices.Contains(runMailboxPhaseStatuses, event.status) {
			return runMailboxEvent{}, fmt.Errorf("run mailbox phase status %q is not one of %s", event.status, strings.Join(runMailboxPhaseStatuses, ", "))
		}
	default:
		if event.step == "" {
			event.step = event.kind
		}
		if event.status == "" {
			event.status = string(contract.EnvelopeSucceeded)
		}
		if !slices.Contains(runMailboxStatuses, event.status) {
			return runMailboxEvent{}, fmt.Errorf("run mailbox status %q is not one of %s", event.status, strings.Join(runMailboxStatuses, ", "))
		}
	}
	if event.outcome != "" && event.kind != runMailboxKindGate {
		return runMailboxEvent{}, fmt.Errorf("run mailbox kind %q does not carry an outcome", event.kind)
	}
	return event, nil
}

// runMailbox owns one attempt's mailbox directory and publishes from it.
type runMailbox struct {
	directory string
	events    string
	staging   string
	published string

	runID     string
	attemptID string
	runToken  string

	appender runLedgerAppender
	poll     time.Duration
	logf     func(string, ...any)

	mu             sync.Mutex
	publishedCount int
	publishedBytes int64
	rejections     int
	boundReported  bool

	stopOnce sync.Once
	cancel   context.CancelFunc
	finished chan struct{}

	// retireCheckpoint is a test-only seam that withholds the cursor rename, so
	// an agent lost between a successful publication and its bookkeeping can be
	// exercised; production construction always leaves it nil.
	retireCheckpoint func() bool
}

// runMailboxAvailable reports whether this attempt can have a mailbox the
// agent is able to read. An OCI handoff volume is helper-owned and guest-side,
// and the helper protocol exposes no read method, so OCI publication waits for
// that seam rather than pretending to work.
func runMailboxAvailable(spec contract.JobSpec) bool {
	return usesAgentHandoffLifecycle(spec) &&
		strings.TrimSpace(spec.Execution.Env[contract.EnvL3Endpoint]) != "" &&
		strings.TrimSpace(spec.Execution.Env[contract.EnvRunID]) != "" &&
		strings.TrimSpace(spec.Execution.SensitiveEnv[contract.EnvRunToken]) != ""
}

// prepareRunMailbox creates the directory layout and materializes the params
// file. It never fails an attempt for a params problem: a job that cannot read
// its params fails on its own terms, with its own diagnostics.
func prepareRunMailbox(spec contract.JobSpec, attemptID string, appender runLedgerAppender, poll time.Duration, logf func(string, ...any)) (*runMailbox, error) {
	directory := filepath.Join(filepath.Clean(spec.Execution.HandoffDirectory), runMailboxDirectoryName)
	mailbox := &runMailbox{
		directory: directory,
		events:    filepath.Join(directory, runMailboxEventsDirectoryName),
		staging:   filepath.Join(directory, runMailboxStagingDirectoryName),
		published: filepath.Join(directory, runMailboxPublishedDirectoryName),
		runID:     strings.TrimSpace(spec.Execution.Env[contract.EnvRunID]),
		attemptID: attemptID,
		runToken:  strings.TrimSpace(spec.Execution.SensitiveEnv[contract.EnvRunToken]),
		appender:  appender,
		poll:      durationOrDefault(poll, DefaultRunMailboxPollInterval),
		logf:      logf,
		finished:  make(chan struct{}),
	}
	for _, path := range []string{mailbox.directory, mailbox.events, mailbox.staging, mailbox.published} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return nil, fmt.Errorf("create run mailbox directory %q: %w", path, err)
		}
		if err := os.Chmod(path, 0o700); err != nil {
			return nil, fmt.Errorf("set run mailbox directory permissions on %q: %w", path, err)
		}
	}
	if err := mailbox.writeParams(spec.Execution.Env[contract.EnvRunParamsJSON]); err != nil {
		return nil, err
	}
	return mailbox, nil
}

func (m *runMailbox) writeParams(raw string) error {
	document := []byte("{}\n")
	if trimmed := strings.TrimSpace(raw); trimmed != "" {
		switch {
		case len(trimmed) > MaxRunMailboxParamsBytes:
			m.log("agent: run %s params exceed %d bytes and were not delivered", m.runID, MaxRunMailboxParamsBytes)
		case !json.Valid([]byte(trimmed)) || !strings.HasPrefix(trimmed, "{"):
			m.log("agent: run %s params are not a JSON object and were not delivered", m.runID)
		default:
			document = append([]byte(trimmed), '\n')
		}
	}
	path := filepath.Join(m.directory, runMailboxParamsFileName)
	if err := os.WriteFile(path, document, 0o600); err != nil {
		return fmt.Errorf("write run mailbox params: %w", err)
	}
	return os.Chmod(path, 0o600)
}

// secrets are the values the agent holds on the run's behalf and must keep out
// of captured output even once they stop travelling in the job environment.
func (m *runMailbox) secrets() []string {
	if m == nil || m.runToken == "" {
		return nil
	}
	return []string{m.runToken}
}

// start begins streaming publication. Publishing only at attempt completion
// would leave a running run with no observable phase at all, which is the
// whole point of the live surface.
func (m *runMailbox) start(ctx context.Context) {
	pollContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	m.cancel = cancel
	go func() {
		defer close(m.finished)
		ticker := time.NewTicker(m.poll)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-pollContext.Done():
				return
			case <-ticker.C:
				m.sweep(pollContext)
			}
		}
	}()
}

// finalize stops the poller and performs the last sweep. It must complete
// before the handoff lifecycle can remove the directory, or a successful run
// would publish nothing.
func (m *runMailbox) finalize(ctx context.Context) {
	if m == nil {
		return
	}
	m.stopOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
			<-m.finished
		} else {
			close(m.finished)
		}
	})
	m.sweep(ctx)
}

// sweep publishes every complete event file in lexical order and retires it by
// renaming it under the published cursor. A rename that never happens costs a
// republish, not a duplicate: the document is byte-identical, so L3 replays it.
func (m *runMailbox) sweep(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries, err := os.ReadDir(m.events)
	if err != nil {
		if !os.IsNotExist(err) {
			m.log("agent: read run mailbox events for run %s: %v", m.runID, err)
		}
		return
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		names = append(names, entry.Name())
	}
	slices.Sort(names)
	for _, name := range names {
		if ctx.Err() != nil {
			return
		}
		if err := m.publishEvent(ctx, name); err != nil {
			// A transport failure keeps the event for the next sweep.
			return
		}
	}
}

func (m *runMailbox) publishEvent(ctx context.Context, name string) error {
	if !validRunMailboxEventName(name) {
		m.log("agent: run %s mailbox event name %q is not acceptable", m.runID, name)
		m.retire(name)
		return nil
	}
	if m.publishedCount >= MaxRunMailboxEvents || m.publishedBytes >= MaxRunMailboxTotalBytes {
		if !m.boundReported {
			m.boundReported = true
			m.log("agent: run %s reached the run mailbox publication bound; later events are discarded", m.runID)
		}
		m.retire(name)
		return nil
	}
	path := filepath.Join(m.events, name)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		m.log("agent: stat run mailbox event %q: %v", name, err)
		return nil
	}
	raw, truncated, err := readBoundedFile(path, MaxRunMailboxEventBytes)
	if err != nil {
		m.log("agent: read run mailbox event %q: %v", name, err)
		return nil
	}
	event, err := parseRunMailboxEvent(raw)
	if err != nil {
		m.reject(ctx, name, err)
		m.retire(name)
		return nil
	}
	event.truncated = truncated
	if truncated {
		event.body = append(event.body, runMailboxTruncationNotice...)
	}
	if event.createdAt.IsZero() {
		event.createdAt = info.ModTime().UTC()
	}
	collection, document, err := m.document(event, name)
	if err != nil {
		m.reject(ctx, name, err)
		m.retire(name)
		return nil
	}
	if err := m.appender.appendRunDocument(ctx, m.runToken, m.runID, collection, document); err != nil {
		var rejection *runLedgerRejection
		if errors.As(err, &rejection) {
			m.log("agent: run %s mailbox event %q was refused: %v", m.runID, name, err)
			m.retire(name)
			return nil
		}
		m.log("agent: publish run %s mailbox event %q: %v", m.runID, name, err)
		return err
	}
	m.publishedCount++
	m.publishedBytes += int64(len(document))
	m.retire(name)
	return nil
}

// reject reports a malformed event into the ledger so it is visible where the
// rest of the run's evidence is, not only in the node's log.
func (m *runMailbox) reject(ctx context.Context, name string, cause error) {
	if m.rejections >= MaxRunMailboxRejections {
		m.log("agent: run %s mailbox event %q rejected: %v", m.runID, name, cause)
		return
	}
	m.rejections++
	envelope := contract.Envelope{
		SchemaVersion:  contract.SchemaVersionV1,
		EnvelopeID:     m.documentID("rejected." + name),
		IdempotencyKey: m.idempotencyKey("rejected." + name),
		RunID:          m.runID,
		StepID:         "mailbox",
		AttemptID:      m.attemptID,
		Status:         contract.EnvelopeFailed,
		Summary:        fmt.Sprintf("run mailbox event %q was rejected: %v", name, cause),
		CreatedAt:      time.Unix(0, 0).UTC(),
	}
	document, err := json.Marshal(envelope)
	if err != nil {
		return
	}
	if err := m.appender.appendRunDocument(ctx, m.runToken, m.runID, runLedgerEnvelopeCollection, document); err != nil {
		m.log("agent: report rejected run %s mailbox event %q: %v", m.runID, name, err)
	}
}

func (m *runMailbox) retire(name string) {
	if m.retireCheckpoint != nil && !m.retireCheckpoint() {
		return
	}
	source := filepath.Join(m.events, name)
	if err := os.Rename(source, filepath.Join(m.published, name)); err != nil && !os.IsNotExist(err) {
		m.log("agent: retire run mailbox event %q: %v", name, err)
	}
}

// document builds the protocol JSON. Every field an id constraint applies to is
// derived here, never taken raw from the workload.
func (m *runMailbox) document(event runMailboxEvent, name string) (string, []byte, error) {
	identity := event.key
	if identity == "" {
		identity = name
	}
	switch event.kind {
	case runMailboxKindGate:
		gate := contract.GateResult{
			SchemaVersion:  contract.SchemaVersionV1,
			GateID:         m.documentID("gate." + identity),
			IdempotencyKey: m.idempotencyKey("gate." + identity),
			RunID:          m.runID,
			StepID:         boundedIdentifier(event.step),
			AttemptID:      m.attemptID,
			Name:           boundedIdentifier(event.name),
			Outcome:        contract.GateOutcome(event.outcome),
			EvaluatedAt:    event.createdAt,
		}
		if value := strings.TrimRight(string(event.body), "\n"); value != "" {
			kind := "text"
			if event.payloadJSON {
				kind = "application/json"
			}
			gate.Evidence = []contract.Evidence{{Kind: kind, Value: value}}
		}
		document, err := json.Marshal(gate)
		return runLedgerGateCollection, document, err
	default:
		status := contract.EnvelopeStatus(event.status)
		step := boundedIdentifier(event.step)
		summary := event.summary
		if event.kind == runMailboxKindPhase {
			step = boundedIdentifier("phase:" + event.name)
			status = contract.EnvelopePartial
			if event.status == "ended" {
				status = contract.EnvelopeSucceeded
			}
			if summary == "" {
				summary = "phase " + event.name + " " + event.status
			}
		}
		if summary == "" {
			summary = event.kind + " " + step
		}
		extensions, err := m.extensions(event)
		if err != nil {
			return "", nil, err
		}
		envelope := contract.Envelope{
			SchemaVersion:  contract.SchemaVersionV1,
			EnvelopeID:     m.documentID(event.kind + "." + identity),
			IdempotencyKey: m.idempotencyKey(event.kind + "." + identity),
			RunID:          m.runID,
			StepID:         step,
			AttemptID:      m.attemptID,
			Status:         status,
			Summary:        boundedSummary(summary),
			Extensions:     extensions,
			CreatedAt:      event.createdAt,
		}
		document, err := json.Marshal(envelope)
		return runLedgerEnvelopeCollection, document, err
	}
}

// extensions always nests the payload under one namespace, so a workload can
// never shape the envelope's own extension object.
func (m *runMailbox) extensions(event runMailboxEvent) (json.RawMessage, error) {
	payload := bytes.TrimSpace(event.body)
	if len(payload) == 0 && event.name == "" && !event.truncated {
		return nil, nil
	}
	inner := map[string]any{}
	if event.name != "" {
		inner["name"] = event.name
	}
	if event.truncated {
		inner["truncated"] = true
	}
	if len(payload) > 0 {
		if event.payloadJSON {
			var decoded any
			if err := json.Unmarshal(payload, &decoded); err != nil {
				return nil, fmt.Errorf("run mailbox json payload: %w", err)
			}
			inner["payload"] = decoded
		} else {
			inner["detail"] = string(bytes.TrimRight(event.body, "\n"))
		}
	}
	if len(inner) == 0 {
		return nil, nil
	}
	return json.Marshal(map[string]any{runMailboxExtensionNamespace: inner})
}

// idempotencyKey is stable for one file across every republish, which is what
// makes a repeated sweep — or a sweep after an agent restart — a replay in L3
// rather than a second document.
func (m *runMailbox) idempotencyKey(suffix string) string {
	return boundedIdentifier("mailbox." + m.attemptID + "." + suffix)
}

func (m *runMailbox) documentID(suffix string) string {
	return boundedIdentifier(m.runID + "." + "mailbox." + m.attemptID + "." + suffix)
}

func (m *runMailbox) log(format string, args ...any) {
	if m != nil && m.logf != nil {
		m.logf(format, args...)
	}
}

// boundedIdentifier keeps every derived id inside the v1 schema's 255-byte
// bound without losing uniqueness.
func boundedIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unnamed"
	}
	if len(value) <= 255 {
		return value
	}
	digest := sha256.Sum256([]byte(value))
	return value[:255-65] + "." + hex.EncodeToString(digest[:])[:64]
}

func boundedSummary(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "\n", " "))
	if value == "" {
		return "run mailbox event"
	}
	if len(value) > 2048 {
		return value[:2048]
	}
	return value
}

func validRunMailboxEventName(name string) bool {
	if name == "" || len(name) > 128 || strings.HasPrefix(name, ".") {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.' || r == '_' || r == '-':
		default:
			return false
		}
	}
	return true
}

// readBoundedFile reads at most limit bytes and reports whether the file had
// more. The tail is dropped, never the event.
func readBoundedFile(path string, limit int) ([]byte, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return nil, false, err
	}
	if len(payload) > limit {
		return payload[:limit], true, nil
	}
	return payload, false, nil
}
