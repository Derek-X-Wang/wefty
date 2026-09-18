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
	"sync/atomic"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// The run mailbox is the job-owned directory in which a workload writes its
// envelopes, steps, gate results and result files, and from which this agent
// publishes them without requiring the workload to hold or use a credential
// (the agent publishes with the run token it holds). The
// workload writes plain text; building protocol JSON that satisfies the v1
// schemas is the agent's job, which is what lets a bash workflow report without
// hand-rolling either HTTP or JSON escaping.
//
// A process workload runs under the agent's own OS identity, so directory
// permissions are not a boundary here. Publication is bounded and best-effort
// under a shared identity, not tamper-proof. Directory-relative operations,
// bounded reads and non-blocking opens limit accidental redirection and stalls;
// the credential used to append is held by the agent.
//
// An OCI workload's mailbox is not this process's to open: it lives in a
// helper-owned handoff volume and is read through the helper. Everything in
// this file is the same for both, because the publisher sees only a mailboxFS.
// What differs is in run_mailbox_remote.go, and each difference is a
// tightening: the bookkeeping moves out of the workload's reach entirely, and
// every read is authorized against the live attempt.
const (
	runMailboxDirectoryName          = ".wefty"
	runMailboxEventsDirectoryName    = "events"
	runMailboxStagingDirectoryName   = "tmp"
	runMailboxPublishedDirectoryName = ".published"
	runMailboxParamsFileName         = "params.json"
	runMailboxStateFileName          = "state.json"
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
	// MaxRunMailboxParamsBytes bounds the params document the agent delivers.
	MaxRunMailboxParamsBytes = 64 << 10
	// MaxRunMailboxRejections bounds how many malformed events are reported
	// into the ledger before the agent only logs them.
	MaxRunMailboxRejections = 8
	// MaxRunMailboxScanEntries bounds one sweep's directory enumeration, so a
	// workload cannot hold the agent in a single scan indefinitely.
	MaxRunMailboxScanEntries = 4096
	// MaxRunMailboxEventNameBytes bounds an event file name.
	MaxRunMailboxEventNameBytes = 128
	// MaxRunMailboxStateBytes bounds the durable bookkeeping. It is sized for
	// the largest state the bounds above can produce — every pending event
	// named at the name bound — so a legitimate state is never mistaken for a
	// corrupt one.
	MaxRunMailboxStateBytes = 4 << 20
	// Bound refusal reasons so terminal markers fit in the state budget,
	// including JSON escaping at the event and rejection limits.
	runMailboxRefusalReasonBytes = 256
	// DefaultRunMailboxPollInterval is how often a running attempt's mailbox
	// is swept. Step timestamps are therefore accurate to this interval, not
	// to the instant the workload wrote the file.
	DefaultRunMailboxPollInterval = 500 * time.Millisecond
	// runMailboxFinalPublishAttempts bounds the final sweep's retries. A
	// transport failure that outlasts them retains the handoff instead of
	// retrying forever inside finalization.
	runMailboxFinalPublishAttempts = 3
	runMailboxFinalPublishBackoff  = 100 * time.Millisecond

	// A finalization expiry gets one short join budget, matching the poll interval.
	runMailboxFenceJoinTimeout = DefaultRunMailboxPollInterval
	// Allow a small backwards wall-clock correction across agent restarts.
	runMailboxObservationClockTolerance = 5 * time.Second

	runMailboxTruncationNotice = "\n[truncated by the node agent: run mailbox event exceeded the size bound]"
)

// runLedgerAppender publishes one protocol document to L3 on a run's behalf.
// The publisher uses the agent-held run token; workload token delivery is unchanged.
type runLedgerAppender interface {
	appendRunDocument(ctx context.Context, runToken, runID, collection string, body []byte) error
}

const (
	runLedgerEnvelopeCollection = "envelopes"
	runLedgerGateCollection     = "gates"
)

// runLedgerRejection is L3 refusing a document on its own merits. Retrying it
// unchanged cannot succeed, so the source is retained with a terminal marker.
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
	runMailboxKindStep     = "step"
	runMailboxKindGate     = "gate"
	runMailboxKindResult   = "result"
)

var (
	runMailboxKinds          = []string{runMailboxKindEnvelope, runMailboxKindStep, runMailboxKindGate, runMailboxKindResult}
	runMailboxStatuses       = []string{string(contract.EnvelopeSucceeded), string(contract.EnvelopeFailed), string(contract.EnvelopePartial)}
	runMailboxStepStatuses   = []string{"started", "ended"}
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
			if !slices.Contains(runMailboxPayloadFormats, value) {
				return runMailboxEvent{}, fmt.Errorf("run mailbox payload format %q is unsupported", value)
			}
			event.payloadJSON = value == "json"
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
	case runMailboxKindStep:
		if event.name == "" {
			return runMailboxEvent{}, errors.New("run mailbox step requires a name")
		}
		if event.status == "" {
			event.status = "started"
		}
		if !slices.Contains(runMailboxStepStatuses, event.status) {
			return runMailboxEvent{}, fmt.Errorf("run mailbox step status %q is not one of %s", event.status, strings.Join(runMailboxStepStatuses, ", "))
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

// runMailboxState is the agent's durable bookkeeping for one attempt. It makes
// a republished document byte-identical to the first one — the timestamp is
// pinned at first observation rather than read from the file at publish time —
// and reserves the publication budget before an append can leave the agent.
// It is advisory: the workload shares the agent's OS identity.
type runMailboxState struct {
	AttemptID  string                          `json:"attempt_id"`
	Count      int                             `json:"count"`
	Rejections int                             `json:"rejections"`
	Bytes      int64                           `json:"bytes"`
	Events     map[string]runMailboxEventState `json:"events"`
}

type runMailboxEventState struct {
	ObservedAt time.Time `json:"observed_at"`
	// ChargedBytes reserves one event and its bytes before the first append.
	// Replays use the reservation, including after a lost append response.
	ChargedBytes      int64              `json:"charged_bytes,omitempty"`
	RejectionReserved bool               `json:"rejection_reserved,omitempty"`
	Refused           *runMailboxRefusal `json:"refused,omitempty"`
}

type runMailboxRefusal struct {
	Status int    `json:"status"`
	Reason string `json:"reason"`
}

// runMailbox owns one attempt's mailbox directory and publishes from it.
type runMailbox struct {
	// directory is the absolute path handed to the workload. Every agent-side
	// operation goes through these opened directories instead. They are opened
	// once, identity-checked once, and never re-resolved by name: a workload
	// that replaces events/ with a FIFO or a symlink after preparation changes
	// nothing the agent looks at.
	directory string
	root      *os.Root
	published *os.Root
	// fs is the publisher's only view of the events directory, so the rules
	// above this field hold whatever backs it.
	fs mailboxFS
	// remote reports that the mailbox is read through a runtime rather than
	// opened by this process. Its only consequence above this file is when the
	// final drain runs: a remote read is authorized against the live attempt,
	// so it must happen before the runtime is reaped, not after.
	remote bool

	runID     string
	attemptID string
	runToken  string

	appender runLedgerAppender
	poll     time.Duration
	clock    Clock
	logf     func(string, ...any)

	// The bounds are fields rather than direct constant reads so a test can
	// prove each one without writing a thousand files.
	maxEvents int
	maxBytes  int64
	maxScan   int

	mu      sync.Mutex
	state   runMailboxState
	bounded bool
	// corrupt latches when the durable bookkeeping cannot be trusted. The
	// mailbox then publishes nothing further and reports incompleteness, so
	// evidence is retained rather than silently re-accounted or re-published.
	corrupt bool

	// appendMu joins admission and completion with fence. Cancellation never
	// needs this mutex, so an in-flight transport can always be interrupted.
	appendMu           sync.Mutex
	publicationContext context.Context
	cancelPublication  context.CancelFunc
	appendCheckpoint   func(context.Context) // test seam, under appendMu
	appendJoinOnce     sync.Once
	appendsJoined      chan struct{}

	fenced     atomic.Bool
	incomplete atomic.Bool

	// Once finalization detaches, its cleanup worker alone closes the roots.
	detached     atomic.Bool
	detachedDone chan struct{}
	closeOnce    sync.Once

	startOnce sync.Once
	stopOnce  sync.Once
	cancel    context.CancelFunc
	finished  chan struct{}

	// retireCheckpoint is a test-only seam that withholds retirement, so
	// an agent lost between a successful publication and its bookkeeping can be
	// exercised; production construction always leaves it nil.
	retireCheckpoint func() bool
}

// runMailboxAvailable reports whether this attempt can have a mailbox the agent
// is able to open itself. An OCI handoff volume is helper-owned and guest-side,
// so it has its own eligibility rule and its own read path;
// see remoteRunMailboxAvailable.
func runMailboxAvailable(spec contract.JobSpec) bool {
	return usesAgentHandoffLifecycle(spec) &&
		strings.TrimSpace(spec.Execution.Env[contract.EnvL3Endpoint]) != "" &&
		validRunMailboxSegment(strings.TrimSpace(spec.Execution.Env[contract.EnvRunID])) &&
		strings.TrimSpace(spec.Execution.SensitiveEnv[contract.EnvRunToken]) != ""
}

// prepareRunMailbox creates the run-scoped directory layout and materializes
// the params file. Scoping by run ID is what keeps a cold rerun — which reuses
// the same handoff directory — from inheriting and republishing the previous
// run's events as its own.
func prepareRunMailbox(spec contract.JobSpec, attemptID string, appender runLedgerAppender, poll time.Duration, clock Clock, logf func(string, ...any)) (*runMailbox, error) {
	if clock == nil {
		clock = systemClock{}
	}
	runID := strings.TrimSpace(spec.Execution.Env[contract.EnvRunID])
	handoff := filepath.Clean(spec.Execution.HandoffDirectory)
	handoffRoot, err := os.OpenRoot(handoff)
	if err != nil {
		return nil, fmt.Errorf("open handoff directory %q: %w", handoff, err)
	}
	defer handoffRoot.Close()
	mailboxRoot, err := openRunMailboxDirectory(handoffRoot, runMailboxDirectoryName, runID)
	if err != nil {
		return nil, err
	}
	for _, name := range []string{runMailboxEventsDirectoryName, runMailboxStagingDirectoryName, runMailboxPublishedDirectoryName} {
		if err := ensureMailboxSubdirectory(mailboxRoot, name); err != nil {
			mailboxRoot.Close()
			return nil, err
		}
	}
	eventsRoot, err := mailboxRoot.OpenRoot(runMailboxEventsDirectoryName)
	if err != nil {
		mailboxRoot.Close()
		return nil, fmt.Errorf("open run mailbox events directory: %w", err)
	}
	publishedRoot, err := mailboxRoot.OpenRoot(runMailboxPublishedDirectoryName)
	if err != nil {
		eventsRoot.Close()
		mailboxRoot.Close()
		return nil, fmt.Errorf("open run mailbox cursor directory: %w", err)
	}
	mailbox := newRunMailbox(runMailboxSettings{
		directory: filepath.Join(handoff, runMailboxDirectoryName, runID),
		runID:     runID,
		attemptID: attemptID,
		runToken:  strings.TrimSpace(spec.Execution.SensitiveEnv[contract.EnvRunToken]),
		root:      mailboxRoot,
		published: publishedRoot,
		fs:        newOSRootMailboxFS(eventsRoot),
		appender:  appender,
		poll:      poll,
		clock:     clock,
		logf:      logf,
	})
	mailbox.loadState()
	if err := mailbox.writeParams(spec.Labels[contract.LabelRunParams]); err != nil {
		mailbox.close()
		return nil, err
	}
	return mailbox, nil
}

// runMailboxSettings is what a prepared mailbox needs whatever backs it. The
// bounds and the publication context are the same for every kind: only the
// storage the publisher reads, and where its bookkeeping lives, differ.
type runMailboxSettings struct {
	directory string
	runID     string
	attemptID string
	runToken  string
	root      *os.Root
	published *os.Root
	fs        mailboxFS
	appender  runLedgerAppender
	poll      time.Duration
	clock     Clock
	logf      func(string, ...any)
	remote    bool
}

func newRunMailbox(settings runMailboxSettings) *runMailbox {
	clock := settings.clock
	if clock == nil {
		clock = systemClock{}
	}
	mailbox := &runMailbox{
		directory: settings.directory,
		root:      settings.root,
		published: settings.published,
		fs:        settings.fs,
		runID:     settings.runID,
		attemptID: settings.attemptID,
		runToken:  settings.runToken,
		appender:  settings.appender,
		poll:      durationOrDefault(settings.poll, DefaultRunMailboxPollInterval),
		clock:     clock,
		logf:      settings.logf,
		remote:    settings.remote,
		maxEvents: MaxRunMailboxEvents,
		maxBytes:  MaxRunMailboxTotalBytes,
		maxScan:   MaxRunMailboxScanEntries,
		finished:  make(chan struct{}),

		appendsJoined: make(chan struct{}),
		detachedDone:  make(chan struct{}),
	}
	mailbox.publicationContext, mailbox.cancelPublication = context.WithCancel(context.Background())
	return mailbox
}

// openRunMailboxDirectory walks into the mailbox one component at a time,
// refusing anything that is not already a real directory. MkdirAll would
// happily follow a symlink a previous attempt's workload left behind.
func openRunMailboxDirectory(parent *os.Root, segments ...string) (*os.Root, error) {
	current := parent
	opened := false
	for _, segment := range segments {
		if err := ensureMailboxSubdirectory(current, segment); err != nil {
			if opened {
				current.Close()
			}
			return nil, err
		}
		next, err := current.OpenRoot(segment)
		if opened {
			current.Close()
		}
		if err != nil {
			return nil, fmt.Errorf("open run mailbox directory %q: %w", segment, err)
		}
		current, opened = next, true
	}
	return current, nil
}

func ensureMailboxSubdirectory(root *os.Root, name string) error {
	if err := root.Mkdir(name, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create run mailbox directory %q: %w", name, err)
	}
	info, err := root.Lstat(name)
	if err != nil {
		return fmt.Errorf("inspect run mailbox directory %q: %w", name, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("run mailbox path %q is not a directory", name)
	}
	return nil
}

// loadState reads advisory, workload-writable bookkeeping. Validate and clamp
// every field before use; invalid state stops publication and retains evidence.
func (m *runMailbox) loadState() {
	m.state = runMailboxState{AttemptID: m.attemptID, Events: map[string]runMailboxEventState{}}
	info, err := m.published.Lstat(runMailboxStateFileName)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil || !info.Mode().IsRegular() {
		m.markCorrupt(fmt.Errorf("run mailbox state is not a regular file: %v", err))
		return
	}
	payload, truncated, err := readBoundedRegularFile(m.published, runMailboxStateFileName, MaxRunMailboxStateBytes)
	if err != nil || truncated {
		m.markCorrupt(fmt.Errorf("read run mailbox state: %v (truncated=%t)", err, truncated))
		return
	}
	var stored runMailboxState
	if err := json.Unmarshal(payload, &stored); err != nil {
		m.markCorrupt(fmt.Errorf("decode run mailbox state: %w", err))
		return
	}
	if stored.AttemptID != m.attemptID {
		// A different attempt of the same run left this behind. That is a
		// legitimate retry, not tampering, and its accounting is not this
		// attempt's to continue.
		return
	}
	if stored.Events == nil {
		stored.Events = map[string]runMailboxEventState{}
	}
	m.state = stored
	invalid := false
	clampInt := func(value, upper int) int {
		bounded := min(max(value, 0), upper)
		invalid = invalid || bounded != value
		return bounded
	}
	clampBytes := func(value int64) int64 {
		bounded := min(max(value, 0), m.maxBytes)
		invalid = invalid || bounded != value
		return bounded
	}
	m.state.Count = clampInt(stored.Count, m.maxEvents)
	m.state.Bytes = clampBytes(stored.Bytes)
	m.state.Rejections = clampInt(stored.Rejections, MaxRunMailboxRejections)
	if len(stored.Events) > MaxRunMailboxScanEntries {
		invalid = true
		m.state.Events = map[string]runMailboxEventState{}
	}
	var chargedCount, reservedRejections int
	var chargedBytes int64
	now := m.clock.Now().UTC().Truncate(time.Second)
	earliest := time.Unix(0, 0).UTC()
	for name, event := range m.state.Events {
		if !validRunMailboxEventName(name) {
			invalid = true
			delete(m.state.Events, name)
			continue
		}
		if event.ObservedAt.Before(earliest) || event.ObservedAt.After(now.Add(runMailboxObservationClockTolerance)) || event.ObservedAt.Nanosecond() != 0 {
			invalid = true
			event.ObservedAt = now
		}
		event.ChargedBytes = clampBytes(event.ChargedBytes)
		if event.ChargedBytes > 0 {
			chargedCount++
			chargedBytes += event.ChargedBytes
		}
		if event.RejectionReserved {
			reservedRejections++
		}
		if event.Refused != nil {
			m.incomplete.Store(true)
			if event.Refused.Status < 400 || event.Refused.Status >= 500 || len(event.Refused.Reason) > runMailboxRefusalReasonBytes || (event.ChargedBytes == 0 && !event.RejectionReserved) {
				invalid = true
			}
		}
		m.state.Events[name] = event
	}
	if chargedCount > m.state.Count || chargedBytes > m.state.Bytes || reservedRejections > m.state.Rejections {
		invalid = true
	}
	if invalid {
		m.markCorrupt(errors.New("run mailbox state contains out-of-range or inconsistent fields"))
	}
}

// persistState commits the bookkeeping before the action it records is
// irreversible. A failure fails closed for the same reason corruption does.
func (m *runMailbox) persistState() error {
	payload, err := json.Marshal(m.state)
	if err != nil {
		m.markCorrupt(fmt.Errorf("encode run mailbox state: %w", err))
		return err
	}
	staging := runMailboxStateFileName + ".tmp"
	if err := writeRegularFile(m.published, staging, payload); err != nil {
		m.markCorrupt(fmt.Errorf("persist run mailbox state: %w", err))
		return err
	}
	if err := m.published.Rename(staging, runMailboxStateFileName); err != nil {
		m.markCorrupt(fmt.Errorf("commit run mailbox state: %w", err))
		return err
	}
	return nil
}

func (m *runMailbox) markCorrupt(cause error) {
	if m.corrupt {
		return
	}
	m.corrupt = true
	m.incomplete.Store(true)
	m.log("agent: run %s mailbox bookkeeping is not trustworthy, publication stops and evidence is retained: %v", m.runID, cause)
}

// writeParams delivers the submitted parameters as a file. They travel from L3
// on an agent-only dispatch label, never in the workload environment, so the
// job reads its own parameters without a credential and without a name it
// could confuse with the reserved execution context.
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
	staging := runMailboxStagingDirectoryName + "/" + runMailboxParamsFileName
	if err := writeRegularFile(m.root, staging, document); err != nil {
		return fmt.Errorf("write run mailbox params: %w", err)
	}
	// Rename replaces whatever occupies the name without following it, so a
	// symlink planted there is destroyed rather than written through.
	if err := m.root.Rename(staging, runMailboxParamsFileName); err != nil {
		return fmt.Errorf("commit run mailbox params: %w", err)
	}
	return nil
}

// secrets are the values the agent holds on the run's behalf and must keep out
// of captured output even once they stop travelling in the job environment.
func (m *runMailbox) secrets() []string {
	if m == nil || m.runToken == "" {
		return nil
	}
	return []string{m.runToken}
}

func (m *runMailbox) close() {
	if m == nil || m.detached.Load() {
		return
	}
	m.closeRoots()
}

func (m *runMailbox) closeRoots() {
	m.closeOnce.Do(func() {
		m.cancelPublication()
		if m.fs != nil {
			_ = m.fs.close()
		}
		for _, root := range []*os.Root{m.published, m.root} {
			if root != nil {
				root.Close()
			}
		}
	})
}

// start begins streaming publication. Publishing only at attempt completion
// would leave a running run with no observable step at all, which is the whole
// point of the live surface. The poll context is a child of the attempt's, so
// losing the attempt cancels an in-flight publication rather than letting it
// land after the lease is gone.
func (m *runMailbox) start(ctx context.Context) {
	pollContext, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	go func() {
		defer close(m.finished)
		ticker := time.NewTicker(m.poll)
		defer ticker.Stop()
		for {
			select {
			case <-pollContext.Done():
				return
			case <-ticker.C:
				m.sweep(pollContext)
			}
		}
	}()
}

// startIfRemote begins streaming publication for a mailbox served by the
// runtime, once its attempt is admitted and a read can actually be authorized.
// It is safe to call when there is no mailbox, and safe to call twice.
func (m *runMailbox) startIfRemote(ctx context.Context) {
	if !m.readsThroughRuntime() {
		return
	}
	m.startOnce.Do(func() { m.start(ctx) })
}

// fence stops publication immediately and permanently. Authority loss is not a
// reason to finish reporting: an attempt that no longer holds its lease must
// not write anything further on the run's behalf.
func (m *runMailbox) fence(cause error) {
	if m == nil {
		return
	}
	m.stopPublication(cause)
	// An expired finalization has already transferred the join and roots to
	// cleanup. Teardown must not rejoin that worker without a bound.
	if m.detached.Load() {
		return
	}
	// All callers share a join, including callers already waiting when an
	// expired finalization transfers ownership to cleanup. Later appends
	// observe the permanent fence under appendMu.
	m.appendJoinOnce.Do(func() {
		go func() {
			m.appendMu.Lock()
			m.appendMu.Unlock()
			close(m.appendsJoined)
		}()
	})
	select {
	case <-m.appendsJoined:
	case <-m.detachedDone:
	}
}

func (m *runMailbox) stopPublication(cause error) {
	first := !m.fenced.Swap(true)
	m.cancelPublication()
	if first {
		// An ordinary teardown fences with no cause at all. Saying "<nil>"
		// reads as a lost error; saying so plainly reads as what it is.
		reason := "attempt teardown"
		if cause != nil {
			reason = cause.Error()
		}
		m.log("agent: run %s mailbox publication fenced: %s", m.runID, reason)
	}
}

// appendDocument is the sole append admission point, for events and rejection
// reports alike. Final sweeps may outlive execution cancellation, but their
// appends always belong to the mailbox's cancelable publication context.
func (m *runMailbox) appendDocument(ctx context.Context, collection string, document []byte) error {
	m.appendMu.Lock()
	defer m.appendMu.Unlock()
	if m.fenced.Load() || m.publicationContext.Err() != nil {
		return errors.New("run mailbox publication is fenced")
	}
	appendContext, cancel := context.WithCancel(m.publicationContext)
	defer cancel()
	if deadline, ok := ctx.Deadline(); ok {
		var cancelDeadline context.CancelFunc
		appendContext, cancelDeadline = context.WithDeadline(appendContext, deadline)
		defer cancelDeadline()
	}
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	if m.appendCheckpoint != nil {
		m.appendCheckpoint(appendContext)
	}
	if ctx.Err() != nil {
		cancel()
	}
	if err := appendContext.Err(); err != nil {
		return err
	}
	return m.appender.appendRunDocument(appendContext, m.runToken, m.runID, collection, document)
}

// finalize uses the existing lifecycle budget even when called without a
// deadline. Its join and filesystem work are also bounded by that deadline;
// expiry cancels publication and allows a short, separately bounded join.
// If that expires too, cleanup owns the roots until all workers unwind.
func (m *runMailbox) finalize(ctx context.Context) {
	if m == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, DefaultFinalizationTimeout)
	defer cancel()
	m.stopOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
		} else {
			close(m.finished)
		}
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			return
		case <-m.finished:
		}
		if !m.fenced.Load() {
			for attempt := range runMailboxFinalPublishAttempts {
				if ctx.Err() != nil {
					return
				}
				if attempt > 0 {
					timer := time.NewTimer(runMailboxFinalPublishBackoff * time.Duration(1<<(attempt-1)))
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
				if m.sweep(ctx) {
					break
				}
			}
		}
		if ctx.Err() == nil && m.pending() {
			m.incomplete.Store(true)
		}
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	if ctx.Err() != nil {
		m.finishExpired(ctx.Err(), done)
	}
	if m.incomplete.Load() {
		m.log("agent: run %s mailbox has unpublished evidence; retaining the handoff directory", m.runID)
	}
}

// finishExpired cancels synchronously, then joins appends, both workers and
// the pending check under a second bound. No lock or filesystem call on the
// caller's path can extend that bound. The ownership decision is handed back
// even if the join finishes at the same instant the timer fires.
func (m *runMailbox) finishExpired(cause error, done <-chan struct{}) {
	m.stopPublication(cause)
	// A sweep holds mu across appends and state writes. Preserve incomplete
	// evidence even if that admitted work finishes during the short join.
	if m.mu.TryLock() {
		m.mu.Unlock()
	} else {
		m.incomplete.Store(true)
	}
	joined := make(chan bool, 1)
	ownership := make(chan bool, 1)
	go func() {
		m.appendMu.Lock()
		m.appendMu.Unlock()
		<-m.finished
		<-done
		joined <- m.pending() // includes the state lock and any filesystem I/O
		if <-ownership {
			m.closeRoots()
		}
	}()
	timer := time.NewTimer(runMailboxFenceJoinTimeout)
	defer timer.Stop()
	select {
	case pending := <-joined:
		if pending {
			m.incomplete.Store(true)
		}
		ownership <- false
	case <-timer.C:
		transferred := m.detached.CompareAndSwap(false, true)
		m.incomplete.Store(true)
		ownership <- true
		if transferred {
			close(m.detachedDone)
		}
		m.log("agent: run %s mailbox_finalization_join_timeout: roots transferred to cleanup; retaining handoff", m.runID)
	}
}

// handoffFiles exposes the bounded read of the handoff volume's own root when
// the backing implementation has one. Only a helper-backed mailbox does: a
// process attempt's agent already holds a verified handle on that directory.
func (m *runMailbox) handoffFiles() (handoffFileReader, bool) {
	if m == nil || m.fs == nil {
		return nil, false
	}
	reader, ok := m.fs.(handoffFileReader)
	return reader, ok
}

// readsThroughRuntime reports a mailbox whose evidence is only reachable while
// its attempt is still live at the runtime.
func (m *runMailbox) readsThroughRuntime() bool {
	return m != nil && m.remote
}

// publicationIncomplete reports that evidence written by this attempt did not
// reach the ledger. A successful run must then keep its handoff directory: the
// files are the only remaining copy.
func (m *runMailbox) publicationIncomplete() bool {
	return m != nil && m.incomplete.Load()
}

// pending reports whether evidence may still be unpublished. An exhausted scan
// listing counts as pending: there may be more entries than the cap allows, and
// deleting a handoff on the strength of an incomplete look is exactly the
// mistake that loses a run's only copy.
func (m *runMailbox) pending() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.corrupt {
		return true
	}
	names, exhausted, err := m.scanEvents()
	return err != nil || len(names) > 0 || exhausted
}

// sweep publishes every complete event file in lexical order and retires each
// one once the ledger has it. A retirement that never happens costs a
// republish, not a duplicate: the document is byte-identical, so L3 replays it.
// It reports whether the events directory was drained without a failure.
func (m *runMailbox) sweep(ctx context.Context) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx.Err() != nil || m.fenced.Load() || m.corrupt {
		return false
	}
	names, exhausted, err := m.scanEvents()
	if err != nil {
		m.log("agent: read run %s mailbox events: %v", m.runID, err)
		return false
	}
	if exhausted {
		// Publishing a partial directory listing could violate lexical order.
		m.incomplete.Store(true)
		return false
	}
	drained := true
	for _, name := range names {
		if ctx.Err() != nil || m.fenced.Load() || m.corrupt {
			return false
		}
		if err := m.publishEvent(ctx, name); err != nil {
			// Keep the event and continue: unremovable junk must not hide valid
			// later events in this complete, lexically sorted listing.
			if !errors.Is(err, errRunMailboxEntryNotDrained) {
				return false
			}
			drained = false
		}
	}
	return drained
}

// scanEvents reads the whole listing up to a hard cap, then sorts it globally.
// Hitting the cap is incomplete even if exactly that many entries exist; no
// partial listing is published and no page cursor depends on directory order.
// Sorting is the publisher's, not the implementation's: lexical publication
// order must hold whichever mailboxFS enumerated the names.
func (m *runMailbox) scanEvents() ([]string, bool, error) {
	names, exhausted, err := m.fs.list(m.maxScan)
	if err != nil {
		return nil, false, err
	}
	slices.Sort(names)
	return names, exhausted, nil
}

func (m *runMailbox) publishEvent(ctx context.Context, name string) error {
	if m.state.Events[name].Refused != nil {
		return errRunMailboxEntryNotDrained
	}
	if !validRunMailboxEventName(name) {
		m.log("agent: run %s mailbox event name %q is not acceptable", m.runID, name)
		return m.discard(name)
	}
	raw, truncated, err := m.readEvent(name)
	if err != nil {
		if errors.Is(err, errRunMailboxEntryUnusable) {
			// The implementation looked at the entry and says it can never
			// become an event, so removing it loses nothing. This sentinel is
			// the only thing that reaches discard. In particular a bare
			// os.ErrNotExist is NOT enough: a missing helper socket is an
			// ENOENT about the transport, not about the entry, and acting on
			// it would delete a run's only copy of its evidence. An
			// implementation that can genuinely tell an entry vanished says so
			// with this sentinel itself.
			m.log("agent: run %s mailbox entry %q is not a publishable event: %v", m.runID, name, err)
			return m.discard(name)
		}
		// Anything else -- a transport failure, an expired deadline, lost
		// authority, an unclassifiable I/O error -- says nothing about the
		// entry. Keep it, stop this sweep so lexical order is preserved, and
		// let the next one retry; if the failure outlasts finalization the
		// entry is still pending, which latches publicationIncomplete and
		// retains the handoff.
		m.log("agent: read run %s mailbox event %q: %v", m.runID, name, err)
		return err
	}
	// The observation timestamp is persisted before the document is built, so
	// every later republication of this file produces the same bytes.
	observation, known := m.state.Events[name]
	if !known {
		observation = runMailboxEventState{ObservedAt: m.clock.Now().UTC().Truncate(time.Second)}
		m.state.Events[name] = observation
		if err := m.persistState(); err != nil {
			return err
		}
	}
	event, err := parseRunMailboxEvent(raw)
	if err != nil {
		if rejectErr := m.reject(ctx, name, err); rejectErr != nil {
			return rejectErr
		}
		return m.retire(name)
	}
	if truncated {
		event.truncated = true
		// A truncated payload is no longer the JSON it claimed to be; keeping
		// it as marked text preserves the evidence instead of failing to
		// decode it.
		event.payloadJSON = false
		event.body = append(event.body, runMailboxTruncationNotice...)
	}
	if event.createdAt.IsZero() {
		event.createdAt = observation.ObservedAt
	}
	collection, document, err := m.document(event, name)
	if err != nil {
		if rejectErr := m.reject(ctx, name, err); rejectErr != nil {
			return rejectErr
		}
		return m.retire(name)
	}
	if observation.ChargedBytes == 0 {
		if m.state.Count >= m.maxEvents || m.state.Bytes+int64(len(document)) > m.maxBytes {
			if !m.bounded {
				m.bounded = true
				m.log("agent: run %s reached the run mailbox publication bound; later events are discarded", m.runID)
			}
			return m.retire(name)
		}
		// Reserve before sending: a lost response can replay without charging
		// twice, and a crash never resets the admitted document budget.
		observation.ChargedBytes = int64(len(document))
		m.state.Count++
		m.state.Bytes += observation.ChargedBytes
		m.state.Events[name] = observation
		if err := m.persistState(); err != nil {
			return err
		}
	} else if observation.ChargedBytes != int64(len(document)) {
		err := errors.New("run mailbox event no longer matches its reserved byte budget")
		m.markCorrupt(err)
		return err
	}
	if err := m.appendDocument(ctx, collection, document); err != nil {
		var rejection *runLedgerRejection
		if errors.As(err, &rejection) {
			m.log("agent: run %s mailbox event %q was refused: %v", m.runID, name, err)
			return m.recordRefusal(name, rejection)
		}
		m.log("agent: publish run %s mailbox event %q: %v", m.runID, name, err)
		return err
	}
	return m.retire(name)
}

// reject reports a malformed event into the ledger so it is visible where the
// rest of the run's evidence is, not only in the node's log. A rejection that
// cannot be delivered leaves the event where it is: retiring it would destroy
// the only remaining record of what the workload wrote.
func (m *runMailbox) reject(ctx context.Context, name string, cause error) error {
	observation := m.state.Events[name]
	if !observation.RejectionReserved && m.state.Rejections >= MaxRunMailboxRejections {
		m.log("agent: run %s mailbox event %q rejected after rejection-report budget exhausted; retiring: %v", m.runID, name, cause)
		return nil
	}
	envelope := contract.Envelope{
		SchemaVersion:  contract.SchemaVersionV1,
		EnvelopeID:     m.documentID("rejected." + name),
		IdempotencyKey: m.idempotencyKey("rejected." + name),
		RunID:          m.runID,
		StepID:         "mailbox",
		Status:         contract.EnvelopeFailed,
		Summary:        boundedSummary(fmt.Sprintf("run mailbox event %q was rejected: %v", name, cause)),
		CreatedAt:      m.state.Events[name].ObservedAt,
	}
	document, err := json.Marshal(envelope)
	if err != nil {
		return nil
	}
	if !observation.RejectionReserved {
		observation.RejectionReserved = true
		m.state.Events[name] = observation
		m.state.Rejections++
		if err := m.persistState(); err != nil {
			return err
		}
	}
	if err := m.appendDocument(ctx, runLedgerEnvelopeCollection, document); err != nil {
		var rejection *runLedgerRejection
		if errors.As(err, &rejection) {
			m.log("agent: run %s rejection report for %q was refused: %v", m.runID, name, err)
			return m.recordRefusal(name, rejection)
		}
		m.log("agent: report rejected run %s mailbox event %q: %v", m.runID, name, err)
		return err
	}
	return nil
}

// recordRefusal retains the source and a terminal marker so later sweeps and
// reopenings skip it while still publishing later events in lexical order.
func (m *runMailbox) recordRefusal(name string, rejection *runLedgerRejection) error {
	m.incomplete.Store(true)
	observation := m.state.Events[name]
	observation.Refused = &runMailboxRefusal{
		Status: rejection.statusCode,
		Reason: strings.ToValidUTF8(rejection.body[:min(len(rejection.body), runMailboxRefusalReasonBytes)], "?"),
	}
	m.state.Events[name] = observation
	if err := m.persistState(); err != nil {
		return err
	}
	return errRunMailboxEntryNotDrained
}

// retire removes an event the agent is finished with and forgets its
// bookkeeping. The ledger holds accepted documents; exhausted rejection
// reports are logged instead.
func (m *runMailbox) retire(name string) error {
	if m.retireCheckpoint != nil && !m.retireCheckpoint() {
		return nil
	}
	if err := m.fs.remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		m.log("agent: retire run %s mailbox event %q: %v", m.runID, name, err)
		return err
	}
	if _, known := m.state.Events[name]; known {
		delete(m.state.Events, name)
		return m.persistState()
	}
	return nil
}

var errRunMailboxEntryNotDrained = errors.New("run mailbox entry not drained")

// discard removes only files, symlinks and empty directories.
func (m *runMailbox) discard(name string) error {
	if err := m.fs.remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		m.log("agent: discard run %s mailbox entry %q: %v", m.runID, name, err)
		return fmt.Errorf("%w: %v", errRunMailboxEntryNotDrained, err)
	}
	return nil
}

// document builds the protocol JSON. Every field an id constraint applies to is
// derived here, never taken raw from the workload. attempt_id is deliberately
// absent: only L3 knows which attempt its run token is bound to, and it binds
// the field itself from the authenticated scope.
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
		if event.kind == runMailboxKindStep {
			status = contract.EnvelopePartial
			if event.status == "ended" {
				status = contract.EnvelopeSucceeded
			}
			if summary == "" {
				summary = "step " + event.name + " " + event.status
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
// rather than a second document. It is scoped to the attempt so a retried
// attempt's evidence is additive rather than conflicting.
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

// readEvent reads one event under the size bound. Whether the entry is a
// readable regular file is the implementation's to prove, and an entry that is
// not one is an error here rather than bytes.
func (m *runMailbox) readEvent(name string) ([]byte, bool, error) {
	return m.fs.read(name, MaxRunMailboxEventBytes)
}

func readBoundedRegularFile(root *os.Root, name string, limit int) ([]byte, bool, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, false, err
	}
	if !info.Mode().IsRegular() {
		return nil, false, fmt.Errorf("%w: %q is not a regular file", errRunMailboxEntryUnusable, name)
	}
	file, err := root.OpenFile(name, os.O_RDONLY|runMailboxNonBlockingOpen, 0)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return nil, false, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return nil, false, fmt.Errorf("%w: %q changed identity while opening", errRunMailboxEntryUnusable, name)
	}
	payload, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return nil, false, err
	}
	if len(payload) > limit {
		return payload[:limit], true, nil
	}
	return payload, false, nil
}

func writeRegularFile(root *os.Root, name string, payload []byte) error {
	if err := root.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		file.Close()
		return err
	}
	return file.Close()
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
	return validRunMailboxName(name, MaxRunMailboxEventNameBytes)
}

// validRunMailboxSegment bounds the run ID before it becomes a path component.
func validRunMailboxSegment(value string) bool {
	return validRunMailboxName(value, MaxRunMailboxEventNameBytes)
}

func validRunMailboxName(name string, limit int) bool {
	if name == "" || len(name) > limit || strings.HasPrefix(name, ".") {
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
