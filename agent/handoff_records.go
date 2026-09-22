package agent

import (
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
	// maxExpiryAttempts bounds how often the collector retries one record whose
	// directory it cannot remove. A name a workload replaced with a symlink is
	// refused at every attempt, and an unbounded retry means the same refusal
	// is logged hourly for as long as the node runs, with nothing ever
	// deciding. After this many consecutive failures the record is quarantined
	// and the collector stops asking.
	maxExpiryAttempts = 4
	// maxQuarantineDetailBytes bounds the free-form cause kept beside the typed
	// quarantine reason, so one long OS error cannot push a record past
	// maxRetentionRecordBytes and make it unreadable.
	maxQuarantineDetailBytes = 256
)

// handoffRecordAnomaly is a typed reason the agent could not do to a run's
// directory what its record asked. It is typed rather than a free-form string
// because the node doctor and the log both have to name the same condition:
// "this run's files are still on the node, and here is the one reason nothing
// is being done about them".
type handoffRecordAnomaly string

const (
	// handoffBoundDirectoryUnreachable: at finish the run's name no longer led
	// to the directory preparation pinned, and the directory standing there now
	// could not be opened on the same no-follow, identity-checked path
	// preparation uses. The bound was enforced on the pinned handle only.
	handoffBoundDirectoryUnreachable handoffRecordAnomaly = "bound_directory_unreachable"
	// handoffBoundDirectoryForeign: the directory standing at the run's name
	// carries an ownership marker naming another run or another node. Trimming
	// it would bound one run's results by deleting another's, so nothing is
	// trimmed there.
	handoffBoundDirectoryForeign handoffRecordAnomaly = "bound_directory_not_this_runs"
	// handoffExpiryDirectoryUnremovable: expiry failed maxExpiryAttempts times
	// in a row. The directory is left exactly as it is -- never followed, never
	// deleted -- and the record stops driving retries.
	handoffExpiryDirectoryUnremovable handoffRecordAnomaly = "expiry_directory_unremovable"
)

// retentionRecord is the agent's own answer to "what is retained, since when,
// and what may be given up first".
type retentionRecord struct {
	RunID       string    `json:"run_id"`
	NodeID      string    `json:"node_id"`
	Directory   string    `json:"directory"`
	RetainedAt  time.Time `json:"retained_at"`
	RetainUntil time.Time `json:"retain_until"`
	Published   bool      `json:"published,omitempty"`
	Succeeded   bool      `json:"succeeded,omitempty"`
	// BoundAnomaly names why the per-run bound could not be enforced on the
	// directory that actually stood at this run's name when the attempt
	// finished. It is recorded rather than returned because a workload that
	// destroys its own handoff directory has not failed its run -- but a node
	// whose bound silently did nothing is exactly the state this field exists
	// to stop being invisible.
	BoundAnomaly handoffRecordAnomaly `json:"bound_anomaly,omitempty"`
	// ExpiryFailures counts consecutive collector attempts that could not
	// remove this run's directory. It is persisted rather than kept in memory
	// so an agent restart does not reset a retry that is never going to
	// succeed.
	ExpiryFailures int `json:"expiry_failures,omitempty"`
	// Quarantine, once set, stops the collector acting on this record at all.
	// The directory it names is left untouched: quarantine is the agent saying
	// it will not keep trying, not permission to try harder.
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
		return uploadRecord{}, false, nil
	}
	if err != nil {
		return uploadRecord{}, false, err
	}
	var record uploadRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return uploadRecord{}, false, err
	}
	return record, true, nil
}

func (m *handoffManager) recordRoot() string {
	return filepath.Join(m.stateRoot, retentionRecordDirectoryName)
}

// recordComponent maps a run ID to one safe file name. A run ID is L1's, not a
// workload's, but it still becomes a path component here.
func recordComponent(runID string) string {
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

func (m *handoffManager) writeRecord(record retentionRecord) error {
	if strings.TrimSpace(m.stateRoot) == "" {
		return nil
	}
	return writeStateDocument(m.recordRoot(), recordComponent(record.RunID), record)
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

func (m *handoffManager) removeRecord(runID string) error {
	if strings.TrimSpace(m.stateRoot) == "" {
		return nil
	}
	err := os.Remove(filepath.Join(m.recordRoot(), recordComponent(runID)))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
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
	if recordComponent(record.RunID) != fileName {
		return fmt.Errorf("record names run %q but is filed as %q", record.RunID, fileName)
	}
	if record.Directory != filepath.Join(root, record.RunID) {
		return fmt.Errorf("record directory %q is not this root's run %q", record.Directory, record.RunID)
	}
	if nodeID != "" && record.NodeID != nodeID {
		return fmt.Errorf("record belongs to node %q, not %q", record.NodeID, nodeID)
	}
	if record.RetainedAt.IsZero() || record.RetainUntil.IsZero() {
		return errors.New("record carries no retention window")
	}
	if !record.RetainUntil.After(record.RetainedAt) {
		return errors.New("record expires before it was retained")
	}
	if record.RetainedAt.After(now.Add(retentionClockSkew)) {
		return errors.New("record was retained in the future")
	}
	if retention > 0 && record.RetainUntil.After(record.RetainedAt.Add(retention)) {
		return fmt.Errorf("record keeps run %q past the retention window", record.RunID)
	}
	return nil
}
