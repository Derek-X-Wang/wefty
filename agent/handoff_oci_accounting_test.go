//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// The node has two handoff roots and can measure only one of them: on a Mac
// node the OCI helper runs inside a Lima VM, so the second root's figures
// arrive over the runtime seam or not at all. This slice reads them and acts
// on none of them; the budget that will is a later one.

type stubHandoffInventory struct {
	report workloadrunner.RetainedHandoffReport
	err    error
	calls  int
}

func (stub *stubHandoffInventory) InventoryRetainedHandoffs(_ context.Context, after string) (workloadrunner.RetainedHandoffReport, error) {
	stub.calls++
	if after != "" {
		// One page and no more: this fixture's root fits in a single read, so
		// a second call would mean the pager asked for a tail that is not
		// there.
		return workloadrunner.RetainedHandoffReport{}, nil
	}
	return stub.report, stub.err
}

func (stub *stubHandoffInventory) RetainedHandoffVolumeName(ownerKey string) (string, error) {
	return ocihelper.DeterministicHandoffVolumeDirectory(ownerKey)
}

func TestTheAccountingPassReportsTheOCIHelpersHandoffRootToo(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.retain("run_known", true, true, map[string]int{"result.json": 16})
	known, err := ocihelper.DeterministicHandoffVolumeDirectory("run_known")
	if err != nil {
		t.Fatal(err)
	}
	stub := &stubHandoffInventory{report: workloadrunner.RetainedHandoffReport{
		Volumes: []workloadrunner.RetainedHandoffVolume{
			{Name: known, TerminalKnown: true, LogicalBytes: 4096, DedupedBytes: 4096, Entries: 2},
			{Name: "wefty-handoff-volume-deadbeefdeadbeefdeadbeefdeadbeef", LogicalBytes: 64, DedupedBytes: 64, Entries: 1,
				Anomalies: []string{string(ocihelper.HandoffAnomalyNoReceipt)}},
		},
	}}
	harness.manager.ociHandoffs = stub

	status := harness.account()
	if stub.calls != 1 {
		t.Fatalf("the accounting pass read the OCI handoff root %d times, want once", stub.calls)
	}
	if status.OCI == nil {
		t.Fatal("the accounting pass reported no OCI figures at all")
	}
	if status.OCI.Volumes != 2 || status.OCI.LogicalBytes != 4160 || status.OCI.DedupedBytes != 4160 || status.OCI.Entries != 3 {
		t.Fatalf("OCI figures = %+v", *status.OCI)
	}
	if status.OCI.TerminalUnknown != 1 || status.OCI.Anomalies != 1 || !status.OCI.Complete {
		t.Fatalf("OCI observations = %+v", *status.OCI)
	}
	// The run this node has a record for is placed; the one whose name no run
	// of this node derives is residue a crash left behind, and saying so is
	// the whole reason no owner key crosses the wire.
	if status.OCI.Unattributable != 1 {
		t.Fatalf("unattributable volumes = %d, want only the one this node cannot name", status.OCI.Unattributable)
	}
	if !harness.logged("retained results in this node's OCI handoff root") {
		t.Fatalf("the pass did not report the OCI root: %v", harness.logs)
	}
	if !harness.logged(string(ocihelper.HandoffAnomalyNoReceipt)) {
		t.Fatalf("a per-volume anomaly went unreported: %v", harness.logs)
	}

	// Nothing acts on any of it in this slice. The process root's own figures
	// are untouched by the second root being there.
	if status.Runs != 1 || status.LogicalBytes != 95 {
		t.Fatalf("the process root's figures moved: %+v", status)
	}
}

// An OCI run has no retention record at all -- its directory is the helper's --
// so the upload record, which every runtime writes, is the only place this
// agent names such a run.
func TestAnOCIRunIsPlacedFromItsUploadRecord(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	if err := harness.manager.recordUpload("run_oci", "node-1", "attempt-1", attemptResult{document: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	name, err := ocihelper.DeterministicHandoffVolumeDirectory("run_oci")
	if err != nil {
		t.Fatal(err)
	}
	harness.manager.ociHandoffs = &stubHandoffInventory{report: workloadrunner.RetainedHandoffReport{
		Volumes: []workloadrunner.RetainedHandoffVolume{{Name: name, TerminalKnown: true}},
	}}
	status := harness.account()
	if status.OCI == nil || status.OCI.Unattributable != 0 {
		t.Fatalf("an OCI run this node uploaded a result for read as crash residue: %+v", status.OCI)
	}
}

// A node whose accounting read failed is still a node that can place work.
// Reporting zeroes would read as an empty root, so the pass reports nothing
// and says why.
func TestAFailedOCIInventoryReadReportsNoFiguresRatherThanEmptyOnes(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.manager.ociHandoffs = &stubHandoffInventory{err: errors.New("helper session is not configured")}
	status := harness.account()
	if status.OCI != nil {
		t.Fatalf("a failed read produced figures: %+v", *status.OCI)
	}
	if !harness.logged("read the OCI helper's retained handoff volumes") {
		t.Fatalf("a failed read went unreported: %v", harness.logs)
	}
}

// A node with no OCI runtime has one handoff root, and its status says so
// rather than reporting an empty second one.
func TestANodeWithNoOCIRuntimeReportsNoOCIFigures(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	if status := harness.account(); status.OCI != nil {
		t.Fatalf("a node with no OCI runtime reported OCI figures: %+v", *status.OCI)
	}
}

// A rerun pointed at a source run's results keeps that run's handoff volume:
// the runtime names the volume from `handoff_owner_run_id`, not from the run
// that is executing. Attribution has to use the same identity, or the node
// reports a volume of its own as residue a crash left behind -- and a budget
// built on that would evict it as unowned.
func TestARerunsVolumeIsAttributedToTheRunItsResultsBelongTo(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	const owner, rerun = "run_source", "run_rerun"
	spec := handoffClaim(rerun, filepath.Join(harness.root, owner), nil).Job.Spec
	spec.Labels["handoff_owner_run_id"] = owner
	ownership := prepareHandoffForTest(t, harness.manager, spec)
	if err := harness.manager.finish(ownership, spec, "node-1", true, true); err != nil {
		t.Fatal(err)
	}
	ownership.lease.release()

	// What the runtime calls this run's volume, derived exactly as the helper
	// derives it.
	name, err := ocihelper.DeterministicHandoffVolumeDirectory(owner)
	if err != nil {
		t.Fatal(err)
	}
	if byRerun, err := ocihelper.DeterministicHandoffVolumeDirectory(rerun); err != nil || byRerun == name {
		t.Fatalf("the fixture does not distinguish the two identities: %q vs %q (%v)", byRerun, name, err)
	}

	harness.manager.ociHandoffs = &stubHandoffInventory{report: workloadrunner.RetainedHandoffReport{
		Volumes: []workloadrunner.RetainedHandoffVolume{{Name: name, TerminalKnown: true}},
	}}
	status := harness.account()
	if status.OCI == nil || status.OCI.Unattributable != 0 {
		t.Fatalf("a rerun's own handoff volume was reported as crash residue: %+v", status.OCI)
	}

	// And the record says which identity it is, rather than leaving a reader
	// to infer it from the run ID. Preparation happens to key the directory on
	// the owner key today, so the two agree here -- which is exactly why the
	// field is recorded: anything reading the run ID as the owner key is
	// relying on a coincidence nothing states.
	record := harness.record(owner)
	if record.handoffOwnerKey() != owner || record.RunID != owner {
		t.Fatalf("the retention record = run %q / owner %q, want both %q", record.RunID, record.handoffOwnerKey(), owner)
	}
}

// The coincidence broken: a record whose run ID and handoff owner key differ.
// Attribution has to follow the owner key, because that is the identity the
// runtime names the volume from.
func TestAttributionFollowsTheOwnerKeyWhenItDiffersFromTheRunID(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	const owner, rerun = "run_source", "run_rerun"
	if err := os.MkdirAll(filepath.Join(harness.root, rerun), 0o700); err != nil {
		t.Fatal(err)
	}
	now := harness.now
	if err := harness.manager.writeRecord(retentionRecord{
		RunID: rerun, NodeID: "node-1", Directory: filepath.Join(harness.root, rerun),
		HandoffOwnerKey: owner, AdmittedAt: now, RetainedAt: now, RetainUntil: now.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	name, err := ocihelper.DeterministicHandoffVolumeDirectory(owner)
	if err != nil {
		t.Fatal(err)
	}
	harness.manager.ociHandoffs = &stubHandoffInventory{report: workloadrunner.RetainedHandoffReport{
		Volumes: []workloadrunner.RetainedHandoffVolume{{Name: name, TerminalKnown: true}},
	}}
	status := harness.account()
	if status.OCI == nil || status.OCI.Unattributable != 0 {
		t.Fatalf("a volume named from the record's own owner key read as crash residue: %+v", status.OCI)
	}
}

// A record written before the owner key was persisted means its run ID, which
// is what preparation has always keyed the directory on.
func TestARecordWithNoOwnerKeyMeansItsRunID(t *testing.T) {
	if key := (retentionRecord{RunID: "run_legacy"}).handoffOwnerKey(); key != "run_legacy" {
		t.Fatalf("a record with no owner key resolved to %q", key)
	}
	if key := (retentionRecord{RunID: "run_legacy", HandoffOwnerKey: "run_source"}).handoffOwnerKey(); key != "run_source" {
		t.Fatalf("a record carrying an owner key resolved to %q", key)
	}
}
