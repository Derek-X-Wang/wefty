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
	// maxRetentionRecordBytes bounds one record read. A record is a handful of
	// fields; anything larger is not one.
	maxRetentionRecordBytes = 8 << 10
	// retentionClockSkew is how far ahead of the agent's own clock a record's
	// timestamps may sit before they are treated as untrustworthy.
	retentionClockSkew = 5 * time.Minute
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
	// Uploaded and UploadSkipReason are this node's own account of whether the
	// run's result reached the ledger. The ledger is authoritative for what it
	// holds; this is the node-side answer to "why is there nothing there", and
	// it is the only place a transport failure is recorded at all.
	Uploaded         bool                            `json:"uploaded,omitempty"`
	UploadSkipReason contract.ResultUploadSkipReason `json:"upload_skip_reason,omitempty"`
}

// noteResultUpload amends an already-written retention record with the outcome
// of the upload. It amends rather than writes: retention is decided at
// completion and must not be re-dated by a later, slower network call, and a
// record that is not this node's is not this node's to touch.
func (m *handoffManager) noteResultUpload(runID, nodeID string, result attemptResult) error {
	if m == nil || strings.TrimSpace(m.stateRoot) == "" || strings.TrimSpace(runID) == "" {
		return nil
	}
	path := filepath.Join(m.recordRoot(), recordComponent(runID))
	record, err := m.readRecord(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if record.RunID != runID || record.NodeID != nodeID {
		return nil
	}
	record.Uploaded = len(result.document) > 0 && result.skip == ""
	record.UploadSkipReason = result.skip
	return m.writeRecord(record)
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
	root := m.recordRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("create retention record directory: %w", err)
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode retention record: %w", err)
	}
	path := filepath.Join(root, recordComponent(record.RunID))
	staging := path + ".tmp"
	// Remove-then-create-exclusive, so a name that is anything but the file the
	// agent expects is replaced rather than written through.
	if err := os.Remove(staging); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("write retention record: %w", err)
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
	info, err := os.Lstat(path)
	if err != nil {
		return retentionRecord{}, err
	}
	if !info.Mode().IsRegular() {
		return retentionRecord{}, fmt.Errorf("retention record is not a regular file")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|noFollowOpenFlag|runMailboxNonBlockingOpen, 0)
	if err != nil {
		return retentionRecord{}, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return retentionRecord{}, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return retentionRecord{}, fmt.Errorf("retention record changed identity while opening")
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxRetentionRecordBytes+1))
	if err != nil {
		return retentionRecord{}, err
	}
	if len(payload) > maxRetentionRecordBytes {
		return retentionRecord{}, fmt.Errorf("retention record exceeds %d bytes", maxRetentionRecordBytes)
	}
	var record retentionRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return retentionRecord{}, err
	}
	return record, nil
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
