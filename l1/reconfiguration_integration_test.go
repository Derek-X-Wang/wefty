package l1

import (
	"context"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func reimageTarget(digit byte) contract.OCIImageSpec {
	digest := "sha256:" + string(make([]byte, 0))
	encoded := make([]byte, 64)
	for index := range encoded {
		encoded[index] = digit
	}
	digest = "sha256:" + string(encoded)
	return contract.OCIImageSpec{Reference: "example.test/computer:reimage", Digest: &digest}
}

func TestComputerReimagePreservesIdentityStorageAndIntent(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "reimage-stopped", Spec: computerCapabilityJobSpec("computer:reimage:v1"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.SetComputerDesiredState(t.Context(), computer.ComputerID,
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "stop"))
	if err != nil {
		t.Fatal(err)
	}
	original := computer
	request := ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
		Image: reimageTarget('d'), Chown: true, IdempotencyKey: "reimage-1"}
	reimaged, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, request)
	if err != nil {
		t.Fatal(err)
	}
	if reimaged.ComputerID != original.ComputerID || reimaged.Name != original.Name ||
		reimaged.PlacementNodeID != original.PlacementNodeID || reimaged.BoundNodeID != original.BoundNodeID ||
		reimaged.StorageID != original.StorageID || reimaged.StorageGeneration != original.StorageGeneration ||
		reimaged.DesiredDiskBytes != original.DesiredDiskBytes || reimaged.CurrentJobID == original.CurrentJobID ||
		reimaged.CurrentSpecRevision != original.CurrentSpecRevision+1 || reimaged.ReconfigurationPhase != ComputerReconfigurationStable ||
		reimaged.DesiredState != contract.ServiceDesiredStopped || reimaged.CurrentJob.Spec.Execution.OCI.Image.Digest == nil ||
		*reimaged.CurrentJob.Spec.Execution.OCI.Image.Digest != *request.Image.Digest {
		t.Fatalf("reimaged Computer = %#v", reimaged)
	}
	replayed, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, request)
	if err != nil || replayed.IntentRevision != reimaged.IntentRevision || replayed.CurrentJobID != reimaged.CurrentJobID {
		t.Fatalf("reimage replay = %#v err=%v", replayed, err)
	}
	history, err := h.store.ListComputerIntents(t.Context(), computer.ComputerID, "", MaxJobPageLimit)
	if err != nil || len(history.Intents) != 3 || history.Intents[2].Operation != ComputerIntentReimage ||
		history.Intents[2].DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("reimage intent history = %#v err=%v", history, err)
	}
	reimaged, err = h.store.SetComputerDesiredState(t.Context(), reimaged.ComputerID,
		computerDesiredRequest(reimaged, contract.ServiceDesiredRunning, "start"))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || !claim.ComputerStorage.Chown {
		t.Fatalf("reimage claim = %#v err=%v", claim, err)
	}
	observation := testImageObservation(claim.Lease.FencingToken)
	observation.SubmittedReference = request.Image.Reference
	observation.TopLevelDigest = *request.Image.Digest
	observation.PlatformManifestDigest = *request.Image.Digest
	if _, err := h.store.ObserveAttemptImage(t.Context(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, observation); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.StartAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	var chown bool
	if err := h.store.db.QueryRow(`SELECT chown FROM computer_job_projections WHERE job_id=?`, claim.Job.JobID).Scan(&chown); err != nil || chown {
		t.Fatalf("completed chown authorization = %t err=%v", chown, err)
	}
}

func TestRunningComputerReimageUsesInternalQuiescence(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "reimage-running", Spec: computerCapabilityJobSpec("computer:running-reimage:v1"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim = %#v err=%v", claim, err)
	}
	if _, err := h.store.ObserveAttemptImage(t.Context(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.StartAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.GetComputer(t.Context(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	request := ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
		Image: reimageTarget('e'), IdempotencyKey: "running-reimage"}
	if _, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, request); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("implicit session termination error = %v", err)
	}
	request.TerminateSessions = true
	reimaging, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, request)
	if err != nil {
		t.Fatal(err)
	}
	if reimaging.DesiredState != contract.ServiceDesiredRunning || reimaging.CurrentJob.State != contract.JobStopping ||
		reimaging.CurrentJob.DesiredState != contract.ServiceDesiredStopped || reimaging.ReconfigurationPhase != ComputerReconfigurationReimaging {
		t.Fatalf("reimaging Computer = %#v", reimaging)
	}
	if _, err := h.store.CompleteAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID,
		CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "reimage-quiesced",
			Result: ProcessResult{OutputError: "quiesced"}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}); err != nil {
		t.Fatal(err)
	}
	projected, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, request)
	if err != nil {
		t.Fatal(err)
	}
	if projected.DesiredState != contract.ServiceDesiredRunning || projected.CurrentJob.State != contract.JobQueued ||
		projected.ReconfigurationPhase != ComputerReconfigurationStable || projected.CurrentJobID == claim.Job.JobID {
		t.Fatalf("completed running reimage = %#v", projected)
	}
	replacement, err := h.store.ClaimJob(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || replacement == nil || replacement.Job.JobID != projected.CurrentJobID ||
		replacement.Lease.AttemptID == claim.Lease.AttemptID || replacement.ComputerStorage.StorageGeneration != computer.StorageGeneration {
		t.Fatalf("replacement reimage claim = %#v err=%v", replacement, err)
	}
}

func growReceiptFor(directive ComputerStorageGrowDirective) ComputerStorageGrowReceipt {
	return ComputerStorageGrowReceipt{Kind: computerStorageGrowAppliedKind, ReceiptID: "grow-receipt",
		ComputerID: directive.ComputerID, StorageID: directive.StorageID,
		StorageGeneration: directive.StorageGeneration, NodeID: directive.BoundNodeID,
		RootInstanceID: directive.RootInstanceID, JobID: directive.JobID,
		OperationRevision: directive.OperationRevision, OperationFence: directive.OperationFence,
		HelperGeneration: 7, OldDiskBytes: directive.OldDiskBytes, NewDiskBytes: directive.NewDiskBytes,
		Applied: true}
}

func TestComputerGrowPreservesJobAttemptAndStorageGeneration(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "grow-running", Spec: computerCapabilityJobSpec("computer:grow"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim = %#v err=%v", claim, err)
	}
	if _, err := h.store.ObserveAttemptImage(t.Context(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.StartAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.GetComputer(t.Context(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	oldBytes := computer.DesiredDiskBytes
	target := oldBytes + (1 << 30)
	request := ComputerGrowRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
		DiskBytes: target, IdempotencyKey: "grow-1"}
	growing, replayed, err := h.store.BeginComputerGrow(t.Context(), computer.ComputerID, request)
	if err != nil || replayed {
		t.Fatalf("begin grow = %#v replayed=%t err=%v", growing, replayed, err)
	}
	if growing.ReconfigurationPhase != ComputerReconfigurationGrowing || growing.DesiredDiskBytes != oldBytes ||
		growing.CurrentJobID != computer.CurrentJobID || growing.CurrentJob.CurrentAttemptID != claim.Lease.AttemptID ||
		growing.StorageGeneration != computer.StorageGeneration {
		t.Fatalf("growing Computer changed identity = %#v", growing)
	}
	directives, err := h.store.ListNodeComputerStorageGrowDirectives(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("grow directives = %#v err=%v", directives, err)
	}
	directive := directives[0]
	receipt := growReceiptFor(directive)
	ack := ComputerStorageGrowAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		IdempotencyKey: receipt.ReceiptID, Receipt: receipt}
	grown, err := h.store.AcknowledgeComputerStorageGrow(t.Context(), "fabric-computer-node", computer.ComputerID, ack)
	if err != nil {
		t.Fatal(err)
	}
	if grown.DesiredDiskBytes != target || grown.ReconfigurationPhase != ComputerReconfigurationStable ||
		grown.StorageGeneration != computer.StorageGeneration || grown.CurrentJobID != computer.CurrentJobID ||
		grown.CurrentJob.CurrentAttemptID != claim.Lease.AttemptID ||
		grown.CurrentJob.Spec.Execution.OCI.Computer.DiskBytes != oldBytes {
		t.Fatalf("grown Computer = %#v", grown)
	}
	staleRevision := grown.IntentRevision
	_, _, err = h.store.BeginComputerGrow(t.Context(), grown.ComputerID, ComputerGrowRequest{
		ComputerMutationPrecondition: computerPrecondition(grown, "operator"), DiskBytes: oldBytes,
		IdempotencyKey: "shrink"})
	if errorCode(err) != contract.ErrorConflict {
		t.Fatalf("shrink error = %v", err)
	}
	unchanged, _ := h.store.GetComputer(t.Context(), grown.ComputerID)
	if unchanged.IntentRevision != staleRevision || unchanged.DesiredDiskBytes != target {
		t.Fatalf("rejected shrink changed Computer = %#v", unchanged)
	}
}

func TestComputerGrowReceiptFailsEveryNegativeRow(t *testing.T) {
	row := computerStorageGrowRow{ComputerID: "computer", StorageID: "storage", StorageGeneration: 3,
		OldDiskBytes: 8 << 30, NewDiskBytes: 9 << 30, BoundNodeID: "node", RootInstanceID: "root",
		JobID: "job", OperationRevision: 7, OperationFence: "fence"}
	valid := ComputerStorageGrowReceipt{Kind: computerStorageGrowAppliedKind, ReceiptID: "receipt",
		ComputerID: row.ComputerID, StorageID: row.StorageID, StorageGeneration: row.StorageGeneration,
		NodeID: row.BoundNodeID, RootInstanceID: row.RootInstanceID, JobID: row.JobID,
		OperationRevision: row.OperationRevision, OperationFence: row.OperationFence, HelperGeneration: 9,
		OldDiskBytes: row.OldDiskBytes, NewDiskBytes: row.NewDiskBytes, Applied: true}
	mutations := []struct {
		name   string
		mutate func(*ComputerStorageGrowReceipt)
	}{
		{"kind", func(r *ComputerStorageGrowReceipt) { r.Kind = "other" }},
		{"receipt", func(r *ComputerStorageGrowReceipt) { r.ReceiptID = "" }},
		{"computer", func(r *ComputerStorageGrowReceipt) { r.ComputerID = "other" }},
		{"storage", func(r *ComputerStorageGrowReceipt) { r.StorageID = "other" }},
		{"generation", func(r *ComputerStorageGrowReceipt) { r.StorageGeneration++ }},
		{"node", func(r *ComputerStorageGrowReceipt) { r.NodeID = "other" }},
		{"root", func(r *ComputerStorageGrowReceipt) { r.RootInstanceID = "other" }},
		{"job", func(r *ComputerStorageGrowReceipt) { r.JobID = "other" }},
		{"revision", func(r *ComputerStorageGrowReceipt) { r.OperationRevision++ }},
		{"fence", func(r *ComputerStorageGrowReceipt) { r.OperationFence = "other" }},
		{"helper generation", func(r *ComputerStorageGrowReceipt) { r.HelperGeneration = 0 }},
		{"old bytes", func(r *ComputerStorageGrowReceipt) { r.OldDiskBytes++ }},
		{"new bytes", func(r *ComputerStorageGrowReceipt) { r.NewDiskBytes++ }},
		{"not applied", func(r *ComputerStorageGrowReceipt) { r.Applied = false }},
		{"success failure", func(r *ComputerStorageGrowReceipt) { r.FailureCode = "insufficient_disk" }},
		{"failed kind applied", func(r *ComputerStorageGrowReceipt) { r.Kind = computerStorageGrowFailedKind }},
		{"failed kind no failure", func(r *ComputerStorageGrowReceipt) { r.Kind = computerStorageGrowFailedKind; r.Applied = false }},
		{"unknown failure", func(r *ComputerStorageGrowReceipt) {
			r.Kind = computerStorageGrowFailedKind
			r.Applied = false
			r.FailureCode = "other"
		}},
		{"negative available", func(r *ComputerStorageGrowReceipt) {
			r.Kind = computerStorageGrowFailedKind
			r.Applied = false
			r.FailureCode = "insufficient_disk"
			r.ObservedAvailableBytes = -1
		}},
		{"blank failed receipt", func(r *ComputerStorageGrowReceipt) {
			r.Kind = computerStorageGrowFailedKind
			r.Applied = false
			r.FailureCode = "insufficient_disk"
			r.ReceiptID = ""
		}},
	}
	if len(mutations) != 20 {
		t.Fatalf("negative row count = %d", len(mutations))
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			receipt := valid
			test.mutate(&receipt)
			if err := validateComputerGrowReceipt(row, receipt); err == nil {
				t.Fatal("mutated receipt passed")
			}
		})
	}
}

func TestComputerGrowInsufficientDiskLatchesUntilExplicitRestart(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "grow-refused", Spec: computerCapabilityJobSpec("computer:grow-refused"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	oldBytes := computer.DesiredDiskBytes
	growing, _, err := h.store.BeginComputerGrow(t.Context(), computer.ComputerID, ComputerGrowRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
		DiskBytes:                    oldBytes + 1<<30, IdempotencyKey: "grow-refused"})
	if err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageGrowDirectives(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("directives = %#v err=%v", directives, err)
	}
	receipt := growReceiptFor(directives[0])
	receipt.Kind = computerStorageGrowFailedKind
	receipt.Applied = false
	receipt.FailureCode = "insufficient_disk"
	receipt.ObservedAvailableBytes = 512 << 20
	failed, err := h.store.AcknowledgeComputerStorageGrow(t.Context(), "fabric-computer-node", computer.ComputerID,
		ComputerStorageGrowAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
	if err != nil {
		t.Fatal(err)
	}
	if failed.ReconfigurationPhase != ComputerReconfigurationStable || failed.CurrentJob.State != contract.JobFailed ||
		failed.CurrentJob.DesiredState != contract.ServiceDesiredStopped || failed.DesiredState != contract.ServiceDesiredRunning ||
		failed.DesiredDiskBytes != oldBytes || len(failed.CurrentJob.LastFailure) == 0 {
		t.Fatalf("failed grow = %#v", failed)
	}
	restarted, replayed, err := h.store.RestartComputer(t.Context(), computer.ComputerID, ComputerRestartRequest{
		ComputerMutationPrecondition: computerPrecondition(failed, "operator"), IdempotencyKey: "recover-grow"})
	if err != nil || replayed || restarted.CurrentJob.State != contract.JobQueued || restarted.DesiredDiskBytes != oldBytes ||
		restarted.IntentRevision != growing.IntentRevision+1 {
		t.Fatalf("explicit recovery = %#v replayed=%t err=%v", restarted, replayed, err)
	}
}

func TestReconfigurationAbortRequiresDeadBoundNodeAndLeavesExplicitRestart(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "abort-reimage", Spec: computerCapabilityJobSpec("computer:abort"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim = %#v err=%v", claim, err)
	}
	if _, err := h.store.ObserveAttemptImage(t.Context(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.StartAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	computer, _ = h.store.GetComputer(t.Context(), computer.ComputerID)
	reimage := ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
		Image: reimageTarget('f'), TerminateSessions: true, IdempotencyKey: "abort-target"}
	reimaging, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, reimage)
	if err != nil || reimaging.ReconfigurationPhase != ComputerReconfigurationReimaging {
		t.Fatalf("reimage = %#v err=%v", reimaging, err)
	}
	abort := ComputerReconfigurationAbortRequest{
		ComputerMutationPrecondition: computerPrecondition(reimaging, "operator"), IdempotencyKey: "abort-1"}
	if _, _, err := h.store.AbortComputerReconfiguration(t.Context(), computer.ComputerID, abort); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("live-node abort error = %v", err)
	}
	if _, err := h.store.db.Exec(`UPDATE nodes SET state=? WHERE node_id=?`, contract.NodeDead, node.NodeID); err != nil {
		t.Fatal(err)
	}
	aborted, replayed, err := h.store.AbortComputerReconfiguration(t.Context(), computer.ComputerID, abort)
	if err != nil || replayed {
		t.Fatalf("abort = %#v replayed=%t err=%v", aborted, replayed, err)
	}
	if aborted.ReconfigurationPhase != ComputerReconfigurationStable || aborted.DesiredState != contract.ServiceDesiredRunning ||
		aborted.IntentRevision != reimaging.IntentRevision+1 || aborted.AppliedRevision != aborted.IntentRevision ||
		aborted.CurrentJobID != computer.CurrentJobID || aborted.CurrentJob.DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("aborted Computer = %#v", aborted)
	}
	var retired int64
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_job_projections
		WHERE computer_id=? AND current=0 AND retired_ns IS NOT NULL`, computer.ComputerID).Scan(&retired); err != nil || retired != 1 {
		t.Fatalf("retired staging projection = %d err=%v", retired, err)
	}
	replayedComputer, replayed, err := h.store.AbortComputerReconfiguration(t.Context(), computer.ComputerID, abort)
	if err != nil || !replayed || replayedComputer.IntentRevision != aborted.IntentRevision {
		t.Fatalf("abort replay = %#v replayed=%t err=%v", replayedComputer, replayed, err)
	}
}
