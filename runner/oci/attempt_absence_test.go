package oci

import (
	"strings"
	"testing"
	"time"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// A helper that no longer holds an attempt as live refuses Delete with
// unauthorized_attempt, and no later Delete can make that refusal positive.
// Run 1 of #196 retried exactly that every fifteen seconds forever and pinned a
// node service slot until the node was rebuilt (#450). Absence has to come from
// the one proof a session holding no live attempt still offers.
func TestRemovalProvesAbsenceWhenDeleteRefusesANonLiveAttempt(t *testing.T) {
	engine := &adapterTestEngine{watch: ocihelper.WatchResponse{ExitCode: intPointer(0)}}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	if result, err := adapter.Run(t.Context(), request, nil); err != nil || result.Outcome.ExitCode == nil {
		t.Fatalf("run before the refusal = (%+v, %v)", result.Outcome, err)
	}
	first, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
	if err != nil || !first.RuntimeQuiesced || first.Evidence != workloadrunner.ReapEvidenceAttempt {
		t.Fatalf("ordinary attempt reap = (%+v, %v)", first, err)
	}
	// The helper has now ended this attempt on its own terms. The agent still
	// carries the run entry whenever it was the helper's guardian or deadman
	// rather than the agent that ended it, which is the run-1 shape.
	adapter.trackRun(request.Authority, runEntry{entered: true, volumes: helperManagedVolumes(request)})
	receipt, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
	if err != nil {
		t.Fatalf("removal wedged on a non-live attempt refusal: %v", err)
	}
	if !receipt.RuntimeQuiesced || receipt.Evidence != workloadrunner.ReapEvidenceAttempt || receipt.BootSessionID != request.Authority.BootSessionID {
		t.Fatalf("non-live attempt absence receipt = %+v", receipt)
	}
	adapter.mu.Lock()
	_, stillTracked := adapter.runEntered[request.Authority]
	adapter.mu.Unlock()
	if stillTracked {
		t.Fatal("a proven-absent attempt stayed tracked and would be reaped again")
	}
}

// The refusal alone is never absence. When the namespace still shows this
// attempt's runtime resources, the reap has to fail and carry both the helper
// refusal and what the projection actually saw.
func TestNonLiveAttemptRefusalIsNotAbsenceWhileRuntimeResidueRemains(t *testing.T) {
	residue := emptyAdapterInventory()
	authority := ocihelper.AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "job", AttemptID: "attempt",
		FencingToken: "fence", Class: "one-shot", RemovalGeneration: "attempt",
	}
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	residue.Containers = []string{identity.ContainerID}
	err = attemptAbsentFromNamespace(ocihelper.VerifyResponse{
		Absent: false, Inventory: emptyAdapterInventory(), RuntimeResidue: residue,
	}, authority, nil)
	if err == nil || !strings.Contains(err.Error(), identity.ContainerID) {
		t.Fatalf("residue projection = %v", err)
	}
}

// A log segment the helper is still sealing is the one thing allowed to
// survive, and only under a bounded retention that names this attempt. An
// unexplained survivor fails the proof.
func TestNonLiveAttemptAbsenceAcceptsOnlyBoundedHelperRetention(t *testing.T) {
	authority := ocihelper.AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "job", AttemptID: "attempt",
		FencingToken: "fence", Class: "one-shot", RemovalGeneration: "attempt",
	}
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	inventory := emptyAdapterInventory()
	inventory.LogSegments = []string{identity.LogSegmentDirectory}
	if err := attemptAbsentFromNamespace(ocihelper.VerifyResponse{
		Inventory: inventory, RuntimeResidue: emptyAdapterInventory(),
	}, authority, nil); err == nil {
		t.Fatal("an unexplained surviving log segment was accepted as absence")
	}
	recorded := time.Now().UTC()
	if err := attemptAbsentFromNamespace(ocihelper.VerifyResponse{
		Inventory: inventory, RuntimeResidue: emptyAdapterInventory(),
		DurableRetentions: []ocihelper.DurableRetention{{
			Class: ocihelper.RemovalResourceLogSegments, ID: identity.LogSegmentDirectory,
			Owner: ocihelper.DurableRetentionOwnerOCIHelper, Reason: ocihelper.DurableRetentionReasonLogSpoolSealing,
			AttemptID: authority.AttemptID, State: ocihelper.DurableRetentionStateUnsealed,
			Bound: time.Minute, RecordedAt: recorded, Deadline: recorded.Add(time.Minute),
		}},
	}, authority, nil); err != nil {
		t.Fatalf("a bounded sealing retention was refused: %v", err)
	}
}
