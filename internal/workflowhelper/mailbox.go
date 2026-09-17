// Package workflowhelper implements the authoring surface a workflow uses to
// report: the `wefty run` subcommands, which write run-mailbox event files,
// and the `wefty workflow init` scaffold, which writes a starter that uses
// them.
//
// Nothing here opens a Fabric connection, speaks HTTP or holds a credential.
// That is the point. A default job holds no run token (see
// docs/contracts/run-execution-context.md, "Run mailbox"): it reports by
// writing files into the job-owned mailbox, and the node agent — which already
// holds authority — publishes them to the ledger. A mailbox write is a claim
// about the writer's own run and nothing else.
//
// The file protocol is the contract; this package is only the recommended
// producer of it. The inline POSIX writer the scaffold emits for an image
// without the wefty binary produces byte-identical event files, and
// TestInlineBashWriterProducesByteIdenticalEvents holds that.
package workflowhelper

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"
)

// The protocol constants. They mirror docs/contracts/run-execution-context.md
// and the agent's parser; neither is imported, because a producer that could
// only agree with the parser by linking against it would not prove the file
// protocol is the contract.
const (
	protocolHeader  = "wefty-protocol"
	protocolVersion = "1"

	// KindEnvelope reports a step's outcome, KindStep brackets a step,
	// KindGate records a verdict, and KindResult carries the run's final
	// result document.
	KindEnvelope = "envelope"
	KindStep     = "step"
	KindGate     = "gate"
	KindResult   = "result"

	// PayloadText is raw text; PayloadJSON is a JSON document the agent nests
	// under its own extension namespace.
	PayloadText = "text"
	PayloadJSON = "json"

	// RunDirEnv names the run mailbox; HandoffDirEnv names the node-local
	// handoff directory the result convention writes into.
	RunDirEnv     = "WEFTY_RUN_DIR"
	HandoffDirEnv = "WEFTY_HANDOFF_DIR"

	eventsDirectory  = "events"
	stagingDirectory = "tmp"
	paramsFileName   = "params.json"

	// ResultFileName is the handoff-directory result convention: one JSON
	// document an operator can read off the node after the run.
	ResultFileName = "result.json"

	// maxEventNameBytes matches the protocol's own bound on an event file name.
	maxEventNameBytes = 128
	// maxSlugBytes bounds the readable part of a generated event file name.
	maxSlugBytes = 32
	// MaxEventBytes is the contract's bound on one event file. The agent
	// truncates a larger file, and a file truncated before its "--" separator
	// is one the parser must refuse — so this helper refuses to write it in
	// the first place, where the author can still read why.
	MaxEventBytes = 64 << 10
	// maxPayloadBytes keeps a helper-written event inside MaxEventBytes with
	// room for the whole bounded header block, so the agent never has to
	// truncate what this helper wrote.
	maxPayloadBytes = 56 << 10
	// maxIdentifierBytes is the v1 schema's bound on a step, name or key; the
	// agent truncates past it, and a silently truncated idempotency key is a
	// different document.
	maxIdentifierBytes = 255
	// maxSummaryBytes is what the agent's own summary bound allows.
	maxSummaryBytes = 2048
	// maxResultBytes bounds the result document copied into the handoff
	// directory. It is a refusal threshold rather than an expectation: the
	// handoff copy is written whole, because a truncated result.json is an
	// invalid document that still looks like one.
	maxResultBytes = 64 << 20
	// maxParamsBytes matches the agent's params bound.
	maxParamsBytes = 64 << 10

	payloadTruncationNotice = "\n[truncated by wefty run: the payload exceeded the run mailbox event bound]\n"
)

var (
	kinds          = []string{KindEnvelope, KindStep, KindGate, KindResult}
	envelopeStatus = []string{"succeeded", "failed", "partial"}
	stepStatus     = []string{"started", "ended"}
	gateOutcomes   = []string{"pass", "fail", "error", "skipped"}
	payloadFormats = []string{PayloadText, PayloadJSON}
)

// UsageError is a caller mistake rather than a failure of the run. The CLI
// turns it into its ordinary usage error, which exits 1: typed exit codes are
// a contract only on the Computer, access and Storage surfaces, and this
// surface does not claim one.
type UsageError string

func (e UsageError) Error() string { return string(e) }

// ErrNoRunDir is what every `wefty run` subcommand returns when the job has no
// mailbox. It names the reason rather than the variable alone, because the
// common case is a job kind that does not receive one yet.
var ErrNoRunDir = errors.New(RunDirEnv + " is not set: this job has no run mailbox to report through. " +
	"The mailbox is delivered to process one-shots that L3 dispatched; an OCI job does not receive one today. " +
	"See docs/contracts/run-execution-context.md, \"Run mailbox\"")

// Event is one run-mailbox event before it is encoded. Every field is the
// workload's claim about its own run; none of it carries authority.
type Event struct {
	Kind          string
	Name          string
	Step          string
	Status        string
	Outcome       string
	Summary       string
	Key           string
	PayloadFormat string
	Payload       []byte
	CreatedAt     time.Time
}

// sequence separates two events written by one process at the same clock
// reading. Ordering is carried by the file name because the agent publishes a
// sweep in lexical order, and each `wefty run` is its own process: a coarse
// timestamp would leave two events written back to back sorted alphabetically
// by kind rather than in the order the workflow reported them. The name
// therefore leads with a nanosecond stamp. Two processes that read the same
// nanosecond still fall back to the rest of the name, so ordering across
// processes is guaranteed only when their writes are distinguishable at
// nanosecond resolution; there is no cross-process allocator, because that
// would mean shared writable state in a directory the workload owns. Two
// processes that read the very same nanosecond fall back to kind, name and
// PID, which is arbitrary — a published best-effort limit, stated in the
// contract and in every scaffolded README, not an implementation detail.
var sequence atomic.Uint32

// Validate rejects what the agent's parser would refuse, at the point where the
// author can still read the message. The parser remains the authority; this is
// the same rule stated early, not a second one.
func (e *Event) Validate() error {
	if !slices.Contains(kinds, e.Kind) {
		return UsageError(fmt.Sprintf("kind %q is not one of %s", e.Kind, strings.Join(kinds, ", ")))
	}
	if e.PayloadFormat == "" {
		e.PayloadFormat = PayloadText
	}
	if !slices.Contains(payloadFormats, e.PayloadFormat) {
		return UsageError(fmt.Sprintf("payload format %q is not one of %s", e.PayloadFormat, strings.Join(payloadFormats, ", ")))
	}
	e.Name = headerValue(e.Name)
	e.Step = headerValue(e.Step)
	e.Summary = headerValue(e.Summary)
	e.Key = headerValue(e.Key)
	switch e.Kind {
	case KindGate:
		if e.Name == "" {
			return UsageError("a gate requires --name")
		}
		if !slices.Contains(gateOutcomes, e.Outcome) {
			return UsageError(fmt.Sprintf("gate outcome %q is not one of %s", e.Outcome, strings.Join(gateOutcomes, ", ")))
		}
	case KindStep:
		if e.Name == "" {
			return UsageError("a step requires --name")
		}
		if e.Status == "" {
			e.Status = "started"
		}
		if !slices.Contains(stepStatus, e.Status) {
			return UsageError(fmt.Sprintf("step status %q is not one of %s", e.Status, strings.Join(stepStatus, ", ")))
		}
	default:
		if e.Status == "" {
			e.Status = "succeeded"
		}
		if !slices.Contains(envelopeStatus, e.Status) {
			return UsageError(fmt.Sprintf("status %q is not one of %s", e.Status, strings.Join(envelopeStatus, ", ")))
		}
	}
	if e.Outcome != "" && e.Kind != KindGate {
		return UsageError(fmt.Sprintf("kind %q does not carry an outcome", e.Kind))
	}
	// A header value is refused rather than trimmed. A truncated payload still
	// carries the verdict, which is why the payload path truncates instead;
	// a truncated key is a different idempotency identity and a truncated step
	// is a different step, so quietly shortening either would change what the
	// ledger records.
	for _, bound := range []struct {
		flag  string
		value string
		limit int
	}{
		{"--step", e.Step, maxIdentifierBytes},
		{"--name", e.Name, maxIdentifierBytes},
		{"--key", e.Key, maxIdentifierBytes},
		{"--summary", e.Summary, maxSummaryBytes},
	} {
		if len(bound.value) > bound.limit {
			return UsageError(fmt.Sprintf("%s is %d bytes; the run mailbox bounds it at %d",
				bound.flag, len(bound.value), bound.limit))
		}
	}
	if len(e.Payload) > maxPayloadBytes {
		// Truncate here rather than let the agent do it, so the marker names
		// the producer that actually dropped the bytes.
		e.Payload = append(bytes.Clone(e.Payload[:maxPayloadBytes]), payloadTruncationNotice...)
		if e.PayloadFormat == PayloadJSON {
			// A truncated JSON document is no longer JSON. Keeping the format
			// header would make the agent reject the whole event, losing the
			// verdict to a large payload.
			e.PayloadFormat = PayloadText
		}
	}
	if e.PayloadFormat == PayloadJSON && !json.Valid(e.Payload) {
		// The agent decodes a json payload before nesting it, and a refusal
		// there costs the whole event. Caught here, it costs a message.
		return UsageError("the payload is marked json but is not a JSON document")
	}
	return nil
}

// Encode renders the event exactly as the protocol specifies: a line-oriented
// header block, a "--" separator line and a raw payload the producer never
// escapes. Header order is fixed so an independent producer — the inline POSIX
// writer the scaffold emits — can be byte-identical.
func (e Event) Encode() []byte {
	created := e.CreatedAt
	if created.IsZero() {
		created = time.Now()
	}
	var out bytes.Buffer
	fmt.Fprintf(&out, "%s: %s\n", protocolHeader, protocolVersion)
	fmt.Fprintf(&out, "kind: %s\n", e.Kind)
	for _, header := range []struct{ name, value string }{
		{"name", e.Name},
		{"step", e.Step},
		{"status", e.Status},
		{"outcome", e.Outcome},
		{"summary", e.Summary},
		{"key", e.Key},
	} {
		if header.value == "" {
			continue
		}
		fmt.Fprintf(&out, "%s: %s\n", header.name, header.value)
	}
	fmt.Fprintf(&out, "payload: %s\n", e.PayloadFormat)
	fmt.Fprintf(&out, "created-at: %s\n", created.UTC().Truncate(time.Second).Format(time.RFC3339))
	out.WriteString("--\n")
	out.Write(e.Payload)
	return out.Bytes()
}

// Write stages the event under tmp/ and renames it into events/. The rename is
// the only "done writing" signal the protocol has, so nothing incomplete is
// ever visible to the agent.
//
// Everything below the mailbox happens through one opened root. The mailbox is
// a directory the workload owns, so `tmp` or `events` can be a symlink by the
// time this runs; O_EXCL on the leaf would not notice, because it is the
// ancestor that was replaced. The root confines every lookup, and the explicit
// Lstat refuses a symlinked directory outright rather than following one that
// merely happens to stay inside.
func Write(runDir string, event Event) (string, error) {
	if strings.TrimSpace(runDir) == "" {
		return "", ErrNoRunDir
	}
	if err := event.Validate(); err != nil {
		return "", err
	}
	document := event.Encode()
	if len(document) > MaxEventBytes {
		return "", UsageError(fmt.Sprintf(
			"this event encodes to %d bytes; the run mailbox bounds one event at %d, and the agent would refuse a truncated file",
			len(document), MaxEventBytes))
	}
	// The agent creates the mailbox during preparation. Creating it anyway is
	// what lets a starter be exercised against a directory that is only a
	// mailbox by convention, which is how the scaffold's own test runs. Only
	// this one call is unrooted: it is the path the caller named.
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		return "", fmt.Errorf("create %s: %w", runDir, err)
	}
	root, err := os.OpenRoot(runDir)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", runDir, err)
	}
	defer root.Close()
	for _, directory := range []string{stagingDirectory, eventsDirectory} {
		if err := ensureDirectory(root, directory); err != nil {
			return "", err
		}
	}
	name := eventFileName(event)
	staged := stagingDirectory + "/" + name
	published := eventsDirectory + "/" + name
	if err := writeExclusive(root, staged, document); err != nil {
		return "", err
	}
	if err := root.Rename(staged, published); err != nil {
		_ = root.Remove(staged)
		return "", fmt.Errorf("publish %s in %s: %w", published, runDir, err)
	}
	syncDirectory(root, eventsDirectory)
	return filepath.Join(runDir, eventsDirectory, name), nil
}

// ensureDirectory creates one mailbox subdirectory through the root and proves
// what it created, or found, is a directory and not a link to one.
func ensureDirectory(root *os.Root, name string) error {
	if err := root.Mkdir(name, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("create %s in %s: %w", name, root.Name(), err)
	}
	info, err := root.Lstat(name)
	if err != nil {
		return fmt.Errorf("inspect %s in %s: %w", name, root.Name(), err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s in %s is not a directory (mode %s); the run mailbox layout is fixed",
			name, root.Name(), info.Mode())
	}
	return nil
}

// writeExclusive creates a new 0600 regular file through the root and refuses
// an existing one, including a symlink planted where the file was going to be:
// O_EXCL never follows. The content is flushed before the caller renames it
// into place, so a reader of the renamed name never sees a short file.
func writeExclusive(root *os.Root, name string, document []byte) error {
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("stage %s in %s: %w", name, root.Name(), err)
	}
	if _, err := file.Write(document); err != nil {
		file.Close()
		_ = root.Remove(name)
		return fmt.Errorf("stage %s in %s: %w", name, root.Name(), err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		_ = root.Remove(name)
		return fmt.Errorf("stage %s in %s: %w", name, root.Name(), err)
	}
	if err := file.Close(); err != nil {
		_ = root.Remove(name)
		return fmt.Errorf("stage %s in %s: %w", name, root.Name(), err)
	}
	return nil
}

// syncDirectory flushes the rename itself, so a crash cannot leave an event
// whose contents reached the disk but whose name did not. It is best effort:
// not every platform lets a directory be opened for sync, and a mailbox that
// is published but unsynced is still better than no event at all.
func syncDirectory(root *os.Root, name string) {
	directory, err := root.Open(name)
	if err != nil {
		return
	}
	_ = directory.Sync()
	_ = directory.Close()
}

// eventFileName sorts chronologically in the agent's lexical sweep, stays
// inside the protocol's name rules, and carries enough of the event to be read
// by a person looking at a retained handoff directory.
func eventFileName(event Event) string {
	slug := event.Name
	if slug == "" {
		slug = event.Step
	}
	if slug == "" {
		slug = event.Kind
	}
	name := fmt.Sprintf("%019d-%04d-%s-%s-%08x",
		time.Now().UTC().UnixNano(), sequence.Add(1)%10000, event.Kind, nameSlug(slug), os.Getpid())
	if len(name) > maxEventNameBytes {
		name = name[:maxEventNameBytes]
	}
	return name
}

// nameSlug keeps only what an event file name may contain.
func nameSlug(value string) string {
	slug := make([]byte, 0, len(value))
	for index := 0; index < len(value); index++ {
		character := value[index]
		switch {
		case character >= 'A' && character <= 'Z',
			character >= 'a' && character <= 'z',
			character >= '0' && character <= '9',
			character == '.' || character == '_' || character == '-':
			slug = append(slug, character)
		default:
			slug = append(slug, '-')
		}
	}
	if len(slug) > maxSlugBytes {
		slug = slug[:maxSlugBytes]
	}
	if len(slug) == 0 {
		return "event"
	}
	return string(slug)
}

// headerValue folds a value onto the single line a header can carry. It is
// byte-for-byte what the inline POSIX writer's wefty_header_value does:
// newlines and tabs become spaces, every other control byte is dropped, and
// the result is trimmed of spaces.
func headerValue(value string) string {
	folded := make([]byte, 0, len(value))
	for index := 0; index < len(value); index++ {
		character := value[index]
		switch {
		case character == '\n' || character == '\r' || character == '\t':
			folded = append(folded, ' ')
		case character < 0x20 || character == 0x7f:
		default:
			folded = append(folded, character)
		}
	}
	return strings.Trim(string(folded), " ")
}
