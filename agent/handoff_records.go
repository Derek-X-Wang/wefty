package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// A retained run's authority lives here, on the agent's side of the boundary,
// and never inside the directory the workload wrote.
//
// A process workload shares the agent's OS identity, so anything in the handoff
// directory is workload-writable. A record kept there would be a file the
// subject can edit to make another run expire, to prefer its own results when
// the node runs out of room, or to remove itself from accounting entirely --
// and a forged one in a foreign directory would be deletion authority over a
// directory the agent never owned. The ownership marker inside the handoff
// directory survives for what it was always for, proving to a cold rerun that
// the files it found are its own, and it is advisory. What decides retention
// and eviction is only ever a record under this root.
const (
	retentionRecordDirectoryName = "run-results"
	// uploadRecordDirectoryName holds one document per run saying what became
	// of its result upload.
	uploadRecordDirectoryName = "run-uploads"
	// maxRetentionRecordBytes bounds one record read. A record is a handful of
	// fields; anything larger is not one.
	maxRetentionRecordBytes = 8 << 10
	// retentionClockSkew is how far ahead of the agent's own clock a record's
	// timestamps may sit before they are treated as untrustworthy.
	retentionClockSkew = 5 * time.Minute
	// maxStructuralRefusals bounds how often the collector retries a record
	// whose run name is structurally not a directory it can sweep -- a symlink,
	// a FIFO, a regular file. That refusal is deterministic: it is identical on
	// every sweep, so repeating it hourly for as long as the node runs decides
	// nothing and fills the log. After this many the record is quarantined.
	//
	// It bounds *only* that class. A removal that failed because the
	// filesystem was busy, unreadable or unwritable is retried for as long as
	// it keeps failing, exactly as it was before quarantine existed, because
	// that fault can clear and the seven-day sweep has to still be there when
	// it does.
	maxStructuralRefusals = 4
	// maxQuarantineDetailBytes bounds the free-form cause kept beside the typed
	// quarantine reason, so one long OS error cannot push a record past
	// maxRetentionRecordBytes and make it unreadable.
	maxQuarantineDetailBytes = 256
	// maxRecordComponentBytes bounds the run-derived part of a record's file
	// name. A run ID may be 128 bytes and escaping can double that, so a name
	// past this carries a digest of the whole ID rather than a cut of it.
	maxRecordComponentBytes = 96
	// hashedRecordPrefix opens the namespace digest-named records live in, and
	// it is deliberately a byte a literal name escapes. That is the whole
	// separation: a literal name can never begin with it, so a digest name and
	// a literal name can never be the same name.
	hashedRecordPrefix = "~"
)

// handoffRecordAnomaly is a typed reason the agent could not do to a run's
// directory what its record asked: "this run's files are still on the node,
// and here is the one reason nothing is being done about them".
//
// It is typed rather than a free-form string so that a reader and a future
// consumer name the same condition. Today the only consumers are the agent log
// and the retention record's own JSON under the agent state root; nothing in
// `wefty inspect`, the node doctor or the agent status surfaces reads it yet.
// Reporting it is #494's later slices, and this type is what they will read.
type handoffRecordAnomaly string

const (
	// handoffBoundDirectoryReplaced: at finish the run's name no longer led to
	// the directory preparation pinned. Nothing was trimmed: not the pinned
	// directory, which the workload has unlinked or moved, and not whatever
	// stands at the name now, whose files the agent cannot prove are this
	// run's. What is there is unbounded and unaccounted, and deliberately
	// left untouched.
	handoffBoundDirectoryReplaced handoffRecordAnomaly = "bound_directory_replaced"
	// handoffBoundDirectoryUnverifiable: the agent could not establish either
	// way whether the name still led to the pinned directory. Nothing was
	// trimmed; the bound was skipped rather than applied to a directory whose
	// identity is unknown.
	handoffBoundDirectoryUnverifiable handoffRecordAnomaly = "bound_directory_unverifiable"
	// handoffExpiryNameNotADirectory: the run's name is structurally not
	// something the sweep can remove -- a symlink, a FIFO, a regular file -- and
	// has been for maxStructuralRefusals sweeps. The directory is left exactly
	// as it is: never followed, never deleted. The quarantine lifts by itself
	// the moment the name is a directory again.
	handoffExpiryNameNotADirectory handoffRecordAnomaly = "expiry_name_is_not_a_directory"
)

// retentionRecord is the agent's own answer to "what is this node holding,
// since when, until when, and what may be given up first".
//
// It has two states. A record with an admission and no window is a run this
// node is executing: its bytes count, and nothing expires it. A record with a
// window is a finished run's retained results. Preparation writes the first
// and finish updates it into the second, so the record is one run's whole
// life on this node rather than only its afterlife.
type retentionRecord struct {
	RunID     string `json:"run_id"`
	NodeID    string `json:"node_id"`
	Directory string `json:"directory"`
	// AdmittedAt is when preparation took responsibility for this directory,
	// and it is written before the workload starts rather than after it stops.
	//
	// A record that began at finish made an in-flight run invisible to
	// accounting and made a crash mid-run leave a directory no record named --
	// residue by construction, which nothing measures and nothing sweeps. A
	// record with an admission and no deadline is exactly that state, said out
	// loud: this node is holding these bytes and does not yet know for how
	// long. Startup resolves it (adoptResidue); the sweep never acts on it.
	AdmittedAt time.Time `json:"admitted_at,omitempty"`
	// RetainedAt and RetainUntil are the terminal window, written at finish.
	// Both are absent while a run is in flight, and a record carrying one
	// without the other is not trusted at all.
	RetainedAt  time.Time `json:"retained_at,omitempty"`
	RetainUntil time.Time `json:"retain_until,omitempty"`
	Published   bool      `json:"published,omitempty"`
	Succeeded   bool      `json:"succeeded,omitempty"`
	// Adopted marks a window the agent derived rather than one an attempt
	// wrote: either an admission that never finished, or a directory carrying
	// this node's ownership marker and no record at all. It changes nothing
	// about how the record is treated -- it is a note to whoever reads one and
	// wonders why a run has a deadline but no verdict.
	Adopted bool `json:"adopted,omitempty"`
	// BoundAnomaly names why the per-run bound did not reach the directory that
	// actually stood at this run's name when the attempt finished. It is
	// recorded rather than returned because a workload that destroys its own
	// handoff directory has not failed its run -- but a node whose bound
	// silently did nothing is exactly the state this field exists to stop being
	// invisible. It reaches a person through the agent log and this record's
	// own JSON; no status or doctor surface reads it yet.
	BoundAnomaly handoffRecordAnomaly `json:"bound_anomaly,omitempty"`
	// ExpiryFailures counts every consecutive sweep that could not remove this
	// run's directory, whatever the cause. It drives nothing; it exists so a
	// person reading the record can see that a run has been stuck and for how
	// long.
	ExpiryFailures int `json:"expiry_failures,omitempty"`
	// StructuralRefusals counts only the subset of those failures the sweep
	// will never resolve by waiting -- the run's name is not a directory. It is
	// what quarantine is bounded by, kept separate so a run that hit four
	// transient errors is not treated as a run whose name is a symlink.
	//
	// Both are persisted rather than held in memory so restarting the agent
	// does not reset a count that is never going to change.
	StructuralRefusals int `json:"structural_refusals,omitempty"`
	// Quarantine, once set, stops the sweep retrying this record while the
	// condition that set it still holds. Each sweep re-checks the run's name
	// cheaply and lifts it as soon as a directory is back, so this is a pause,
	// never a permanent retirement, and it needs no operator to clear.
	//
	// It means "unsafe to delete", and only that. It is deliberately not a
	// statement about accounting: these bytes are still on the node, and #494's
	// node budget must keep charging them, or quarantine becomes a way to hide
	// storage from the budget.
	Quarantine handoffRecordAnomaly `json:"quarantine,omitempty"`
	// QuarantineDetail is the last failure's own words, bounded. The typed
	// reason above is what anything branches on; this is what a person reads.
	QuarantineDetail string    `json:"quarantine_detail,omitempty"`
	QuarantinedAt    time.Time `json:"quarantined_at,omitempty"`
}

// uploadRecord is this node's own account of what happened to a run's result
// document. It is separate from the retention record on purpose, and written
// for every runtime rather than only the ones whose directory this agent owns.
//
// Separate, because retention is authority -- what may be deleted and when --
// and this is diagnosis. Folding a slower network outcome into the record that
// decides deletion would let a failed upload re-date a retention window.
//
// For every runtime, because the case this exists for is precisely the one the
// ledger cannot answer: an upload that never landed leaves no row there, so a
// reader sees an ordinary absence and the reason lives only here. An OCI run's
// handoff volume is the helper's and has no retention record at all, which is
// exactly why the upload outcome cannot live in one.
type uploadRecord struct {
	RunID     string                          `json:"run_id"`
	NodeID    string                          `json:"node_id"`
	AttemptID string                          `json:"attempt_id"`
	Uploaded  bool                            `json:"uploaded"`
	Reason    contract.ResultUploadSkipReason `json:"reason,omitempty"`
	At        time.Time                       `json:"at"`
}

func (m *handoffManager) uploadRecordRoot() string {
	return filepath.Join(m.stateRoot, uploadRecordDirectoryName)
}

// recordUpload writes what became of this attempt's result. It is best effort
// by design: it never fails an attempt, because a node that cannot write its
// own diagnosis has not changed what the run did.
func (m *handoffManager) recordUpload(runID, nodeID, attemptID string, result attemptResult) error {
	if m == nil || strings.TrimSpace(m.stateRoot) == "" || strings.TrimSpace(runID) == "" {
		return nil
	}
	record := uploadRecord{
		RunID: runID, NodeID: nodeID, AttemptID: attemptID,
		Uploaded: len(result.document) > 0 && result.skip == "",
		Reason:   result.skip, At: m.now().UTC(),
	}
	return writeStateDocument(m.uploadRecordRoot(), recordComponent(runID), record)
}

// readUploadRecord reads one run's upload outcome back. Nothing in the agent
// acts on it; it exists so an operator on the node, and the tests, can see the
// reason a result never reached the ledger.
func (m *handoffManager) readUploadRecord(runID string) (uploadRecord, bool, error) {
	if m == nil || strings.TrimSpace(m.stateRoot) == "" {
		return uploadRecord{}, false, nil
	}
	path := filepath.Join(m.uploadRecordRoot(), recordComponent(runID))
	payload, err := readStateDocument(path)
	if errors.Is(err, os.ErrNotExist) {
		// An older agent's name for the same run. The run ID inside is checked
		// below, because the old name was shared between run IDs.
		if legacy := legacyRecordComponent(runID); legacy != recordComponent(runID) {
			payload, err = readStateDocument(filepath.Join(m.uploadRecordRoot(), legacy))
		}
	}
	if errors.Is(err, os.ErrNotExist) {
		return uploadRecord{}, false, nil
	}
	if err != nil {
		return uploadRecord{}, false, err
	}
	var record uploadRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return uploadRecord{}, false, err
	}
	if record.RunID != runID {
		return uploadRecord{}, false, nil
	}
	return record, true, nil
}

func (m *handoffManager) recordRoot() string {
	return filepath.Join(m.stateRoot, retentionRecordDirectoryName)
}

// recordComponent maps a run ID to one safe file name, and maps two different
// run IDs to two different file names.
//
// The mapping it replaces did neither reliably. It rewrote every character
// outside [A-Za-z0-9_-] to "_" and cut the result at 96 bytes, so "run.live"
// and "run_live" shared a file, as did any two runs agreeing on their first 96
// characters -- and a run ID may be 128 bytes and may contain "." by the same
// rule the run mailbox applies (validRunMailboxSegment). A shared file is not a
// cosmetic collision: a record is deletion authority over a directory, so one
// run's record standing in for another's is one run holding another's expiry,
// and adoption writing at a colliding key would hand a workload a way to
// replace a record it does not own.
//
// So every byte outside [A-Za-z0-9_-] is escaped rather than folded, and a name
// too long for one component keeps a prefix and carries a digest of the whole
// run ID instead of being cut. Both directions are reversible enough to be
// injective, which is the only property that matters here.
//
// Names that need neither -- the ordinary case -- come out byte-for-byte as the
// old mapping produced them, so an upgraded node reads its own existing
// records. The rest are read through legacyRecordComponent as well.
//
// A digest name carries no readable prefix on purpose. Anything readable in it
// would be a string a literal run ID can also produce, and the two namespaces
// would meet again.
func recordComponent(runID string) string {
	escaped, complete := escapedRecordName(runID, maxRecordComponentBytes)
	if escaped == "" {
		// Not reachable for a validated run ID, which is never empty. A record
		// still needs a name rather than ".json".
		return "run.json"
	}
	if complete {
		return escaped + ".json"
	}
	// A hashed name shares no namespace with a literal one. Keeping a readable
	// prefix and appending a digest was not enough and the counterexample needs
	// no hash collision at all: 97 "a"s hash to some digest D, and the run ID
	// "79 a's - first 16 of D" is itself a valid run ID short enough to be
	// written literally -- producing byte for byte the name the first run's
	// digest produced. Preparation and the upload record write at these names
	// without going through adoption's guard, so that is one run overwriting
	// another's record with nothing forged.
	//
	// So a hashed name is the whole digest and nothing else, under a leading
	// "~". A literal name escapes every byte outside [A-Za-z0-9_-], so "~"
	// cannot appear in one at all, and the two forms can no longer meet.
	digest := sha256.Sum256([]byte(runID))
	return hashedRecordPrefix + hex.EncodeToString(digest[:]) + ".json"
}

// escapedRecordName escapes one run ID into at most limit bytes, and reports
// whether the whole ID fit. An escape is never split across the limit: half of
// "%2E" would still be a legal file name, but it would stop being the answer to
// "which run is this".
func escapedRecordName(runID string, limit int) (string, bool) {
	var builder strings.Builder
	for index := 0; index < len(runID); index++ {
		value := runID[index]
		piece := string(value)
		switch {
		case value >= 'a' && value <= 'z', value >= 'A' && value <= 'Z',
			value >= '0' && value <= '9', value == '-', value == '_':
		default:
			piece = fmt.Sprintf("%%%02X", value)
		}
		if builder.Len()+len(piece) > limit {
			return builder.String(), false
		}
		builder.WriteString(piece)
	}
	return builder.String(), true
}

// legacyRecordComponent is the mapping recordComponent replaced. It exists so a
// node that upgrades keeps reading the records it already wrote: those files
// are its only authority to expire directories, and a mapping change that made
// them unreadable would turn every retained run on the node into residue the
// sweep no longer owns.
//
// Nothing is ever written at this name. A record found under one is rewritten
// to the current name and the legacy file is removed, so no run ends up with
// two records.
func legacyRecordComponent(runID string) string {
	var builder strings.Builder
	for _, value := range runID {
		switch {
		case value >= 'a' && value <= 'z', value >= 'A' && value <= 'Z',
			value >= '0' && value <= '9', value == '-', value == '_':
			builder.WriteRune(value)
		default:
			builder.WriteByte('_')
		}
		if builder.Len() >= 96 {
			break
		}
	}
	if builder.Len() == 0 {
		return "run"
	}
	return builder.String() + ".json"
}

// recordPath is where this run's record is written. Writes only ever use this
// name.
func (m *handoffManager) recordPath(runID string) string {
	return filepath.Join(m.recordRoot(), recordComponent(runID))
}

// existingRecordPath is where this run's record is *read* from: the current
// name when a file is there, and otherwise the name an older agent would have
// written. It returns the current name when neither exists, so a caller that
// reports "not found" reports it against the name a write would create.
func (m *handoffManager) existingRecordPath(runID string) string {
	path := m.recordPath(runID)
	if info, err := os.Lstat(path); err == nil && info.Mode().IsRegular() {
		return path
	}
	if legacy, ok := m.legacyRecordPath(runID); ok {
		return legacy
	}
	return path
}

// legacyRecordPath reports the older name this run's record may still be under,
// and only when the file there really is this run's.
//
// The check is not a formality. The old mapping was not injective, so run
// "a.b"'s legacy name is run "a_b"'s legacy name, and following it blindly
// would hand one run the other's record -- as expiry authority over a directory
// it does not own, which is the whole failure the new mapping exists to remove.
func (m *handoffManager) legacyRecordPath(runID string) (string, bool) {
	legacy := filepath.Join(m.recordRoot(), legacyRecordComponent(runID))
	if legacy == m.recordPath(runID) {
		return "", false
	}
	record, err := m.readRecord(legacy)
	if err != nil || record.RunID != runID {
		return "", false
	}
	return legacy, true
}

// errRecordBelongsToAnotherRun is the one refusal every writer owes a record
// that is not its own.
var errRecordBelongsToAnotherRun = errors.New("a retention record at this name belongs to another run")

func (m *handoffManager) writeRecord(record retentionRecord) error {
	if strings.TrimSpace(m.stateRoot) == "" {
		return nil
	}
	// Every writer, not only adoption. The name is injective now, so reaching
	// this is either a file an older agent wrote under its shared mapping --
	// "a.b" and "a_b" were one name -- or a digest nobody should have been
	// able to produce. Either way the record standing there is deletion
	// authority over some other run's directory, and overwriting it would take
	// that run's expiry away silently.
	if standing, err := m.readRecord(m.recordPath(record.RunID)); err == nil &&
		standing.RunID != "" && standing.RunID != record.RunID {
		m.log("agent: refuse to write run %s's retention record: %q already belongs to run %q; that run keeps its own expiry and this one is not recorded",
			record.RunID, m.recordPath(record.RunID), standing.RunID)
		return fmt.Errorf("%w: %q belongs to run %q, not %q",
			errRecordBelongsToAnotherRun, m.recordPath(record.RunID), standing.RunID, record.RunID)
	}
	if err := writeStateDocument(m.recordRoot(), recordComponent(record.RunID), record); err != nil {
		return err
	}
	// A record that arrived under the old name is now at the new one. Leaving
	// the old file would give one run two records, and loadRecords would sweep
	// on both. Only this run's own old file is removed: the old name is shared
	// with other run IDs, and removing one of those would delete a record this
	// run has no claim on.
	legacy, ok := m.legacyRecordPath(record.RunID)
	if !ok {
		return nil
	}
	if err := os.Remove(legacy); err != nil && !errors.Is(err, os.ErrNotExist) {
		m.log("agent: run %s: remove the record's superseded file name %q: %v", record.RunID, legacy, err)
	}
	return nil
}

// writeStateDocument writes one small agent-owned JSON document by
// write-then-rename. The staging name is removed first rather than truncated,
// so a name that is anything but the file the agent expects is replaced rather
// than written through.
func writeStateDocument(root, name string, value any) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", root, err)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode agent state document: %w", err)
	}
	path := filepath.Join(root, name)
	staging := path + ".tmp"
	if err := os.Remove(staging); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("write agent state document: %w", err)
	}
	if _, err := file.Write(payload); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(staging, path)
}

// removeRecord drops both names a run's record can be under. Removing only the
// current one would leave an older agent's file behind as authority nothing
// updates.
func (m *handoffManager) removeRecord(runID string) error {
	if strings.TrimSpace(m.stateRoot) == "" {
		return nil
	}
	paths := []string{m.recordPath(runID)}
	// Again only this run's own older file: the old name is shared, so
	// removing it unconditionally would delete another run's record.
	if legacy, ok := m.legacyRecordPath(runID); ok {
		paths = append(paths, legacy)
	}
	for _, path := range paths {
		err := os.Remove(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// loadRecords reads every record the agent wrote. A record it cannot trust is
// skipped with a log line and never stops the sweep: one unreadable file must
// not be a way to keep a node from ever collecting again.
func (m *handoffManager) loadRecords() []retentionRecord {
	if strings.TrimSpace(m.stateRoot) == "" {
		return nil
	}
	root := m.recordRoot()
	entries, err := os.ReadDir(root)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			m.log("agent: read retention records: %v", err)
		}
		return nil
	}
	now := m.now().UTC()
	records := make([]retentionRecord, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		record, err := m.readRecord(filepath.Join(root, name))
		if err != nil {
			m.log("agent: skip unusable retention record %q: %v", name, err)
			continue
		}
		if err := validRetentionRecord(record, name, m.root, m.nodeID, m.retention, now); err != nil {
			m.log("agent: skip untrustworthy retention record %q: %v", name, err)
			continue
		}
		records = append(records, record)
	}
	return records
}

// readRecord is bounded, refuses anything that is not a regular file, and
// proves the object it opened is the object it checked.
func (m *handoffManager) readRecord(path string) (retentionRecord, error) {
	payload, err := readStateDocument(path)
	if err != nil {
		return retentionRecord{}, err
	}
	var record retentionRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return retentionRecord{}, err
	}
	return record, nil
}

// readStateDocument reads one small agent-owned JSON document. It refuses
// anything that is not a regular file, never follows a link, opens
// non-blocking, proves the opened object is the one it checked, and stops at
// the bound -- the same rules every other agent-side read in this package
// applies, because this directory shares the agent's own OS identity.
func readStateDocument(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("agent state document is not a regular file")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|noFollowOpenFlag|runMailboxNonBlockingOpen, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("agent state document changed identity while opening")
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxRetentionRecordBytes+1))
	if err != nil {
		return nil, err
	}
	if len(payload) > maxRetentionRecordBytes {
		return nil, fmt.Errorf("agent state document exceeds %d bytes", maxRetentionRecordBytes)
	}
	return payload, nil
}

// validRetentionRecord refuses a record whose identity does not match its own
// file name, its directory, or this node, and one whose timestamps do not make
// sense. A record is the authority for deleting a directory, so every field it
// carries is checked and a record missing any of them fails closed: an older
// agent's partial record is skipped, never guessed at.
func validRetentionRecord(record retentionRecord, fileName, root, nodeID string, retention time.Duration, now time.Time) error {
	if record.RunID == "" || record.NodeID == "" || record.Directory == "" {
		return errors.New("record is missing its identity")
	}
	// The run ID becomes a path component, so it must be exactly one -- the
	// same rule the run mailbox applies to every name it opens.
	if !validRunMailboxSegment(record.RunID) {
		return fmt.Errorf("record run %q is not one safe path component", record.RunID)
	}
	// Either name the record could legitimately be filed under: the one a
	// write produces now, or the one an older agent wrote before the mapping
	// became injective. Anything else is a record that does not belong to the
	// file it was found in, which is the check that stops one run's record
	// standing in as another's deletion authority.
	if recordComponent(record.RunID) != fileName && legacyRecordComponent(record.RunID) != fileName {
		return fmt.Errorf("record names run %q but is filed as %q", record.RunID, fileName)
	}
	if record.Directory != filepath.Join(root, record.RunID) {
		return fmt.Errorf("record directory %q is not this root's run %q", record.Directory, record.RunID)
	}
	if nodeID != "" && record.NodeID != nodeID {
		return fmt.Errorf("record belongs to node %q, not %q", record.NodeID, nodeID)
	}
	if record.AdmittedAt.After(now.Add(retentionClockSkew)) {
		return errors.New("record was admitted in the future")
	}
	switch {
	case record.RetainedAt.IsZero() && record.RetainUntil.IsZero():
		// A run admitted and not yet finished. It is a legitimate earlier
		// state of the same record, not a partial one: preparation writes it
		// so the node can account for a run while it is executing and so a
		// crash leaves a record instead of residue. It is never expiry
		// authority -- the sweep skips a record with no deadline -- so the
		// window checks below have nothing to check.
		if record.AdmittedAt.IsZero() {
			return errors.New("record carries neither an admission nor a retention window")
		}
	case record.RetainedAt.IsZero() || record.RetainUntil.IsZero():
		// Half a window is not one of the two states a record is allowed to be
		// in, so it fails closed exactly as a missing field always has.
		return errors.New("record carries half a retention window")
	default:
		if !record.RetainUntil.After(record.RetainedAt) {
			return errors.New("record expires before it was retained")
		}
		if record.RetainedAt.After(now.Add(retentionClockSkew)) {
			return errors.New("record was retained in the future")
		}
		if retention > 0 && record.RetainUntil.After(record.RetainedAt.Add(retention)) {
			return fmt.Errorf("record keeps run %q past the retention window", record.RunID)
		}
	}
	return nil
}
