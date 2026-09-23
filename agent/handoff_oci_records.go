package agent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// ociHandoffRecordDirectoryName holds one document per OCI run whose handoff
// volume this node admitted.
//
// It is beside the process root's records rather than in them, and that is the
// whole reason it is a separate directory: everything that reads
// retentionRecordDirectoryName treats a record as authority over a directory
// under the agent's own handoff root -- the sweep opens it, the accounting pass
// measures it, adoption reconciles it against an ownership marker inside it.
// An OCI run has no such directory. Its volume is the helper's, on a filesystem
// the agent cannot stat, and folding the two kinds into one store would hand
// every one of those readers a record whose name resolves to nothing.
//
// What is shared is the substrate: the same atomic write, the same injective
// name mapping, the same bounded read.
const ociHandoffRecordDirectoryName = "run-oci-handoffs"

// ociHandoffRecord is this node's own account of one OCI run's handoff volume.
//
// It exists because the helper cannot hold either of the two facts the node
// budget decides on. The helper knows a volume is live only while an attempt
// it can see owns it; it never knows whether that run's evidence reached a
// ledger. Before this record there was nothing on the agent's side between
// "the helper says nobody is writing there" and "give those bytes up", and two
// defects lived in that gap: an eviction could arrive after a rerun had
// claimed the volume, and a rerun inherited the *previous* attempt's
// publication, so its own unpublished results were given up first.
//
// It is written before the runtime request, under the same path lease the
// budget's eviction takes, and completed at finish. An admission with no
// terminal time therefore means exactly what it does on the process root:
// this node is holding these bytes for a run that has not finished, and
// nothing may give them up.
type ociHandoffRecord struct {
	// OwnerKey is the identity the runtime derives the volume's name from --
	// `handoff_owner_run_id` when a rerun is pointed at a source run's
	// results, the run ID otherwise. It is the record's key, because it is the
	// volume's identity; two runs sharing results share this record, and the
	// later one replaces it.
	OwnerKey string `json:"owner_key"`
	NodeID   string `json:"node_id"`
	// RunID and AttemptID are which execution this record is about. AttemptID
	// is what binds publication: an upload outcome is joined to the attempt
	// that produced it, never to the owner key alone, because a rerun of a
	// published owner produces new contents that no ledger has seen.
	RunID     string `json:"run_id"`
	AttemptID string `json:"attempt_id"`
	// AdmittedAt is when preparation took responsibility, written before the
	// workload starts. RetainedAt is when that attempt finished. A record with
	// an admission and no RetainedAt is a run still writing.
	AdmittedAt time.Time `json:"admitted_at"`
	RetainedAt time.Time `json:"retained_at,omitempty"`
	// Published is the mailbox drain verdict and Uploaded is what became of
	// the result document. Either one is evidence that reached a ledger, so
	// eviction treats the volume as published when either is true. Both are
	// cleared by the next admission, which is the safe direction: a run that
	// looks unpublished is kept longer, never given up sooner.
	Published bool `json:"published,omitempty"`
	Succeeded bool `json:"succeeded,omitempty"`
	Uploaded  bool `json:"uploaded,omitempty"`
	// Adopted marks terminal facts this node derived at startup rather than
	// ones an attempt wrote, for an admission that never finished.
	Adopted bool `json:"adopted,omitempty"`
}

// live says an attempt is still writing into this volume as far as this node's
// own records go. It is the agent's half of the guard; the helper's half is
// the ownership it publishes under its own lock.
func (record ociHandoffRecord) live() bool { return record.RetainedAt.IsZero() }

// evidenceReachedLedger is the `published` fact the eviction order reads.
func (record ociHandoffRecord) evidenceReachedLedger() bool {
	return record.Published || record.Uploaded
}

// usesOCIHandoffLifecycle is the set of jobs whose handoff volume this agent
// admits: an OCI one-shot with a handoff owner. Services get service data, not
// a handoff volume (runtimeManagedVolumes), and a job naming no owner has no
// volume at all.
func usesOCIHandoffLifecycle(spec contract.JobSpec) bool {
	return spec.Kind == contract.JobKindOCI && spec.Class == contract.JobClassOneShot &&
		strings.TrimSpace(handoffOwnerRunID(spec)) != ""
}

// ociHandoffLeaseKey is the path-lock registry key for one handoff volume.
//
// The registry is keyed by absolute paths, so a key that is not one cannot
// collide with a process run's directory. The volume has no path on this node
// to use instead: it is the helper's, and on a Mac node it is inside a Lima VM.
func ociHandoffLeaseKey(ownerKey string) string {
	return "oci-handoff-volume:" + ownerKey
}

// lockOCIHandoff holds one handoff volume across preparation, execution and
// finish, exactly as the process root's lock does for a directory.
//
// It is the same registry and the same token, which is what makes the budget's
// eviction and an attempt's admission exclusive of one another: the eviction
// takes the lease with tryCollectLease, which refuses a key an attempt holds,
// and this call waits for one the collector holds.
func (m *handoffManager) lockOCIHandoff(ctx context.Context, ownerKey string) (*handoffLease, error) {
	if strings.TrimSpace(ownerKey) == "" {
		return nil, errors.New("an OCI handoff volume is admitted under its owner key")
	}
	return m.lockPath(ctx, ociHandoffLeaseKey(ownerKey))
}

func (m *handoffManager) ociRecordRoot() string {
	return filepath.Join(m.stateRoot, ociHandoffRecordDirectoryName)
}

func (m *handoffManager) ociRecordPath(ownerKey string) string {
	return filepath.Join(m.ociRecordRoot(), recordComponent(ownerKey))
}

// admitOCIHandoff records that this node is about to run an attempt into one
// handoff volume. It runs before the runtime request and under the volume's
// lease, so the budget pass cannot select a volume an attempt is claiming and
// cannot read a record an admission is half-way through writing.
//
// It replaces whatever stood at this owner key. That is the publication reset:
// a rerun's contents are its own, and inheriting the previous attempt's
// "published" would let the node give a run's only copy up first.
func (m *handoffManager) admitOCIHandoff(lease *handoffLease, spec contract.JobSpec, nodeID, attemptID string) error {
	if m == nil || strings.TrimSpace(m.stateRoot) == "" {
		return nil
	}
	ownerKey := handoffOwnerRunID(spec)
	if !m.holdsOCIHandoffLease(lease, ownerKey) {
		return errors.New("admitting an OCI handoff volume requires that volume's lease")
	}
	return writeStateDocument(m.ociRecordRoot(), recordComponent(ownerKey), ociHandoffRecord{
		OwnerKey: ownerKey, NodeID: strings.TrimSpace(nodeID),
		RunID: strings.TrimSpace(spec.Labels["run_id"]), AttemptID: strings.TrimSpace(attemptID),
		AdmittedAt: m.now().UTC(),
	})
}

// holdsOCIHandoffLease is the same receipt check preparation makes on the
// process root: a caller that does not hold the lease is not admitting under
// it, whatever it believes.
func (m *handoffManager) holdsOCIHandoffLease(lease *handoffLease, ownerKey string) bool {
	if lease == nil || lease.manager != m || lease.path != ociHandoffLeaseKey(ownerKey) {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return lease.pathLock.owner == lease
}

// finishOCIHandoff completes the admission with this attempt's terminal facts.
// The record keeps its admission rather than being replaced, so a reader can
// see one run's whole life on this node, and the attempt it names stays the
// attempt publication is joined to.
func (m *handoffManager) finishOCIHandoff(spec contract.JobSpec, nodeID, attemptID string, succeeded, published bool) error {
	if m == nil || strings.TrimSpace(m.stateRoot) == "" {
		return nil
	}
	ownerKey := handoffOwnerRunID(spec)
	record, found, err := m.readOCIRecord(ownerKey)
	if err != nil {
		return err
	}
	if !found {
		// No admission: an older agent ran this attempt, or the admission
		// write failed. Recording the terminal facts is still worth doing --
		// without them the volume is one the budget can never place -- and the
		// admission time is this moment, which is the conservative reading.
		record = ociHandoffRecord{OwnerKey: ownerKey, NodeID: strings.TrimSpace(nodeID),
			RunID: strings.TrimSpace(spec.Labels["run_id"]), AttemptID: strings.TrimSpace(attemptID), AdmittedAt: m.now().UTC()}
	}
	if record.AttemptID != strings.TrimSpace(attemptID) {
		// A later attempt has already admitted this volume. Its record is the
		// current one, and writing this attempt's verdict over it would give
		// the running attempt a terminal time it has not reached.
		m.log("agent: leave the OCI handoff record for run %s alone: attempt %s finished, and attempt %s has already admitted the volume",
			ownerKey, attemptID, record.AttemptID)
		return nil
	}
	record.RetainedAt = m.now().UTC()
	record.Succeeded, record.Published = succeeded, published
	return writeStateDocument(m.ociRecordRoot(), recordComponent(ownerKey), record)
}

// noteOCIHandoffUpload binds the result upload's outcome to the attempt that
// produced it. An upload recorded for an attempt the record no longer names is
// dropped rather than applied: that is precisely the stale authority a rerun
// used to inherit.
func (m *handoffManager) noteOCIHandoffUpload(ownerKey, attemptID string, uploaded bool) error {
	if m == nil || strings.TrimSpace(m.stateRoot) == "" || !uploaded {
		return nil
	}
	record, found, err := m.readOCIRecord(ownerKey)
	if err != nil || !found {
		return err
	}
	if record.AttemptID != strings.TrimSpace(attemptID) {
		return nil
	}
	record.Uploaded = true
	return writeStateDocument(m.ociRecordRoot(), recordComponent(ownerKey), record)
}

func (m *handoffManager) readOCIRecord(ownerKey string) (ociHandoffRecord, bool, error) {
	if m == nil || strings.TrimSpace(m.stateRoot) == "" || strings.TrimSpace(ownerKey) == "" {
		return ociHandoffRecord{}, false, nil
	}
	payload, err := readStateDocument(m.ociRecordPath(ownerKey))
	if errors.Is(err, os.ErrNotExist) {
		return ociHandoffRecord{}, false, nil
	}
	if err != nil {
		return ociHandoffRecord{}, false, err
	}
	record, ok := m.validOCIRecord(payload)
	return record, ok, nil
}

// validOCIRecord refuses a record this node cannot act on. The rules are the
// process root's, minus the ones about a directory it does not have: the
// document parses, names this node, carries an owner key, and does not claim
// to have been admitted or retained in this node's future.
func (m *handoffManager) validOCIRecord(payload []byte) (ociHandoffRecord, bool) {
	var record ociHandoffRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return ociHandoffRecord{}, false
	}
	if strings.TrimSpace(record.OwnerKey) == "" || record.NodeID != m.nodeID {
		return ociHandoffRecord{}, false
	}
	horizon := m.now().UTC().Add(retentionClockSkew)
	if record.AdmittedAt.IsZero() || record.AdmittedAt.After(horizon) || record.RetainedAt.After(horizon) {
		return ociHandoffRecord{}, false
	}
	return record, true
}

// loadOCIRecords reads every OCI handoff record this node holds, skipping the
// ones it cannot trust with a logged reason -- the same rule loadRecords
// applies, and for the same purpose: a record the agent cannot validate is not
// authority over anything.
func (m *handoffManager) loadOCIRecords() []ociHandoffRecord {
	if m == nil || strings.TrimSpace(m.stateRoot) == "" {
		return nil
	}
	entries, err := os.ReadDir(m.ociRecordRoot())
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			m.log("agent: read this node's OCI handoff records: %v", err)
		}
		return nil
	}
	records := make([]ociHandoffRecord, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		payload, err := readStateDocument(filepath.Join(m.ociRecordRoot(), entry.Name()))
		if err != nil {
			m.log("agent: skip the OCI handoff record %q: %v", entry.Name(), err)
			continue
		}
		record, ok := m.validOCIRecord(payload)
		if !ok {
			m.log("agent: skip the OCI handoff record %q: it is not one this node can act on", entry.Name())
			continue
		}
		if entry.Name() != recordComponent(record.OwnerKey) {
			// The file the record was found in is part of what validates it:
			// a document under another owner's name is not that owner's
			// authority, and it is not this one's either.
			m.log("agent: skip the OCI handoff record %q: it names owner %s, which is filed elsewhere",
				entry.Name(), record.OwnerKey)
			continue
		}
		if _, duplicate := seen[record.OwnerKey]; duplicate {
			continue
		}
		seen[record.OwnerKey] = struct{}{}
		records = append(records, record)
	}
	return records
}

func (m *handoffManager) removeOCIRecord(ownerKey string) error {
	if m == nil || strings.TrimSpace(m.stateRoot) == "" {
		return nil
	}
	if err := os.Remove(m.ociRecordPath(ownerKey)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// reconcileOCIAdmissions resolves what a crash left behind, and runs once, at
// startup, before anything is executing.
//
// An admission with no terminal time means "an attempt is writing here", and
// at startup that cannot be true of any attempt of this agent. Left alone it
// would be permanent: the budget would never consider the volume, the node
// would keep charging it, and nothing but the helper's own expiry would ever
// reclaim it. So the admission is given the terminal facts this node can still
// stand behind -- this moment, and whatever the run's upload record says --
// and marked as derived rather than as something an attempt wrote.
//
// It is the OCI arm of adoptResidue and runs from it, so the two roots are
// reconciled on the same pass, before the first sweep and the first budget.
func (m *handoffManager) reconcileOCIAdmissions() {
	if m == nil || strings.TrimSpace(m.stateRoot) == "" {
		return
	}
	now := m.now().UTC()
	for _, record := range m.loadOCIRecords() {
		if !record.live() {
			continue
		}
		record.RetainedAt = now
		record.Adopted = true
		// The upload record is the one place an interrupted attempt's
		// publication can still be read, and it is joined by attempt, not by
		// owner: an upload another attempt made says nothing about this one's
		// contents.
		if upload, found, err := m.readUploadRecord(record.OwnerKey); err == nil && found &&
			upload.AttemptID == record.AttemptID && upload.Uploaded {
			record.Uploaded = true
		}
		if err := writeStateDocument(m.ociRecordRoot(), recordComponent(record.OwnerKey), record); err != nil {
			m.log("agent: reconcile the OCI handoff admission for run %s: %v", record.OwnerKey, err)
			continue
		}
		m.log("agent: the OCI handoff volume for run %s was admitted by attempt %s and never finished; it is now retained from this startup%s",
			record.OwnerKey, record.AttemptID,
			map[bool]string{true: ", with its result upload recorded"}[record.Uploaded])
	}
}
