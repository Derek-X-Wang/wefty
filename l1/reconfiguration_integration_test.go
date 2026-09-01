package l1

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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

func acknowledgeReimagePreflight(t *testing.T, h *integrationHarness, node Node, computerID,
	attemptID, fencingToken string,
) {
	t.Helper()
	directive, request := reimagePreflightAcknowledgement(t, h, node, computerID, attemptID, fencingToken)
	policyChanged := h.store.computerPolicyChangeChannel()
	completed, err := h.store.AcknowledgeComputerReimagePreflight(t.Context(), "fabric-"+node.NodeID,
		computerID, request)
	if err != nil {
		t.Fatal(err)
	}
	if completed.ReconfigurationPhase != ComputerReconfigurationStable ||
		completed.ReconfigurationRevision != nil || completed.AppliedRevision != directive.OperationRevision ||
		completed.CurrentJobID != directive.StagingJobID {
		t.Fatalf("verified Computer reimage did not atomically activate its staged projection: %#v", completed)
	}
	select {
	case <-policyChanged:
	default:
		t.Fatal("verified Computer reimage preflight did not wake policy reconciliation")
	}
	replayed, err := h.store.AcknowledgeComputerReimagePreflight(t.Context(), "fabric-"+node.NodeID,
		computerID, request)
	if err != nil || replayed.ReconfigurationPhase != ComputerReconfigurationStable ||
		replayed.CurrentJobID != directive.StagingJobID {
		t.Fatalf("completed Computer reimage acknowledgement replay = %#v err=%v", replayed, err)
	}
}

func reimagePreflightAcknowledgement(t *testing.T, h *integrationHarness, node Node, computerID,
	attemptID, fencingToken string,
) (ComputerReimagePreflightDirective, ComputerReimagePreflightAcknowledgementRequest) {
	t.Helper()
	directives, err := h.store.ListNodeComputerReimagePreflightDirectives(t.Context(),
		"fabric-"+node.NodeID, node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("reimage preflight directives = %#v err=%v", directives, err)
	}
	if attemptID == "" {
		attemptID, fencingToken = "no-runtime", "no-runtime-fence"
	}
	directive := directives[0]
	receipt := ComputerReimagePreflightReceipt{Kind: computerReimagePreflightReceiptKind,
		ReceiptID: "preflight-" + directive.OperationFence, ComputerID: directive.ComputerID,
		StorageID: directive.StorageID, StorageGeneration: directive.StorageGeneration,
		OldJobID: directive.OldJobID, StagingJobID: directive.StagingJobID, NodeID: directive.BoundNodeID,
		RootInstanceID: directive.RootInstanceID, OperationRevision: directive.OperationRevision,
		OperationFence: directive.OperationFence, TargetDigest: *directive.TargetImage.Digest,
		PlatformOS: "linux", PlatformArchitecture: "amd64", ImageUID: 1000, ImageGID: 1000,
		DiskRootUID: 1000, DiskRootGID: 1000, DetachmentReceiptID: "detach-" + attemptID,
		StorageEvidenceKind: computerReimageDetachmentEvidenceKind, DetachmentAttemptID: attemptID,
		DetachmentFencingToken: fencingToken, HelperGeneration: 7}
	return directive, ComputerReimagePreflightAcknowledgementRequest{NodeID: node.NodeID,
		BootSessionID: node.BootSessionID, IdempotencyKey: "ack-" + directive.OperationFence,
		Receipt: receipt}
}

func TestComputerReimagePreflightReceiptFailsEveryNegativeRow(t *testing.T) {
	row := computerReimageOperation{ComputerID: "computer", OperationRevision: 7, OldJobID: "old",
		StagingJobID: "new", StorageID: "storage", StorageGeneration: 3, BoundNodeID: "node",
		RootInstanceID: "root", OperationFence: "fence", TargetDigest: "sha256:target", Chown: false}
	valid := ComputerReimagePreflightReceipt{Kind: computerReimagePreflightReceiptKind, ReceiptID: "receipt",
		ComputerID: row.ComputerID, StorageID: row.StorageID, StorageGeneration: row.StorageGeneration,
		OldJobID: row.OldJobID, StagingJobID: row.StagingJobID, NodeID: row.BoundNodeID,
		RootInstanceID: row.RootInstanceID, OperationRevision: row.OperationRevision,
		OperationFence: row.OperationFence, TargetDigest: row.TargetDigest, PlatformOS: "linux",
		PlatformArchitecture: "amd64", ImageUID: 1000, ImageGID: 1000, DiskRootUID: 1000,
		DiskRootGID: 1000, DetachmentReceiptID: "detach", DetachmentAttemptID: "attempt",
		StorageEvidenceKind:    computerReimageDetachmentEvidenceKind,
		DetachmentFencingToken: "attempt-fence", HelperGeneration: 9}
	mutations := []struct {
		name   string
		mutate func(*ComputerReimagePreflightReceipt)
	}{
		{"kind", func(r *ComputerReimagePreflightReceipt) { r.Kind = "other" }},
		{"receipt", func(r *ComputerReimagePreflightReceipt) { r.ReceiptID = "" }},
		{"helper", func(r *ComputerReimagePreflightReceipt) { r.HelperGeneration = 0 }},
		{"computer", func(r *ComputerReimagePreflightReceipt) { r.ComputerID = "other" }},
		{"storage", func(r *ComputerReimagePreflightReceipt) { r.StorageID = "other" }},
		{"generation", func(r *ComputerReimagePreflightReceipt) { r.StorageGeneration++ }},
		{"old job", func(r *ComputerReimagePreflightReceipt) { r.OldJobID = "other" }},
		{"staging job", func(r *ComputerReimagePreflightReceipt) { r.StagingJobID = "other" }},
		{"node", func(r *ComputerReimagePreflightReceipt) { r.NodeID = "other" }},
		{"root", func(r *ComputerReimagePreflightReceipt) { r.RootInstanceID = "other" }},
		{"revision", func(r *ComputerReimagePreflightReceipt) { r.OperationRevision++ }},
		{"fence", func(r *ComputerReimagePreflightReceipt) { r.OperationFence = "other" }},
		{"digest", func(r *ComputerReimagePreflightReceipt) { r.TargetDigest = "other" }},
		{"detachment receipt", func(r *ComputerReimagePreflightReceipt) { r.DetachmentReceiptID = "" }},
		{"detachment attempt", func(r *ComputerReimagePreflightReceipt) { r.DetachmentAttemptID = "" }},
		{"detachment fence", func(r *ComputerReimagePreflightReceipt) { r.DetachmentFencingToken = "" }},
		{"platform os", func(r *ComputerReimagePreflightReceipt) { r.PlatformOS = "other" }},
		{"platform architecture", func(r *ComputerReimagePreflightReceipt) { r.PlatformArchitecture = "other" }},
		{"uid", func(r *ComputerReimagePreflightReceipt) { r.ImageUID++ }},
		{"gid", func(r *ComputerReimagePreflightReceipt) { r.ImageGID++ }},
	}
	if len(mutations) != 20 {
		t.Fatalf("negative row count = %d", len(mutations))
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			receipt := valid
			test.mutate(&receipt)
			if err := validateComputerReimagePreflight(row, receipt, "linux", "amd64"); err == nil {
				t.Fatal("mutated reimage preflight receipt passed")
			}
		})
	}
	resetPreparation := valid
	resetPreparation.StorageEvidenceKind = computerReimageResetEvidenceKind
	resetPreparation.DetachmentReceiptID = ""
	resetPreparation.DetachmentAttemptID = ""
	resetPreparation.DetachmentFencingToken = ""
	resetPreparation.ResetPreparationReceiptID = "reset-preparation"
	if err := validateComputerReimagePreflight(row, resetPreparation, "linux", "amd64"); err != nil {
		t.Fatalf("explicit reset-preparation evidence was rejected: %v", err)
	}
	for _, test := range []struct {
		code   contract.SpawnFailureCode
		reason string
	}{
		{contract.SpawnFailureImageUnavailable, "image_unavailable"},
		{contract.SpawnFailureImagePlatformUnsupported, "image_platform_unsupported"},
	} {
		failed := valid
		failed.Kind = computerReimagePreflightFailedReceiptKind
		failed.FailureCode = string(test.code)
		failed.FailureStage = "image_identity"
		failed.FailureReason = test.reason
		failed.StorageEvidenceKind = ""
		failed.DetachmentReceiptID = ""
		failed.DetachmentAttemptID = ""
		failed.DetachmentFencingToken = ""
		if err := validateComputerReimagePreflight(row, failed, "linux", "amd64"); err == nil {
			t.Fatalf("%s failure without storage evidence passed", test.code)
		}
	}
}

func TestRestartedResourceLatchCanEnterReimageWithoutSecondRestart(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "restart-then-reimage", Spec: computerCapabilityJobSpec("computer:restart-then-reimage"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	failure, err := json.Marshal(contract.SpawnFailure{Code: contract.SpawnFailureInsufficientDisk,
		Message: "durable allocation is latched"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE jobs SET state='running' WHERE job_id=?`, computer.CurrentJobID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE service_jobs SET desired_state='running', last_failure=? WHERE job_id=?`,
		failure, computer.CurrentJobID); err != nil {
		t.Fatal(err)
	}
	latched, err := h.store.GetComputer(t.Context(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	restarted, _, err := h.store.RestartComputer(t.Context(), computer.ComputerID, ComputerRestartRequest{
		ComputerMutationPrecondition: computerPrecondition(latched, "operator"), IdempotencyKey: "restart-latch",
	})
	if err != nil || restarted.CurrentJob.State != contract.JobStopping ||
		restarted.CurrentJob.DesiredState != contract.ServiceDesiredRunning {
		t.Fatalf("active resource restart = %#v err=%v", restarted, err)
	}
	reimaged, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, ComputerReimageRequest{
		ComputerMutationPrecondition: computerPrecondition(restarted, "operator"), Image: reimageTarget('9'),
		TerminateSessions: true, IdempotencyKey: "reimage-after-restart",
	})
	if err != nil || reimaged.ReconfigurationPhase != ComputerReconfigurationReimaging ||
		reimaged.CurrentJob.State != contract.JobStopping || reimaged.CurrentJob.DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("restart-then-reimage = %#v err=%v", reimaged, err)
	}
}

func TestComputerReimageMissingGenerationBudgetFailsClosed(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "missing-reimage-budget", Spec: computerCapabilityJobSpec("computer:missing-reimage-budget"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.SetComputerDesiredState(t.Context(), computer.ComputerID,
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "stop"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE computers SET bound_node_id=? WHERE computer_id=?`, node.NodeID, computer.ComputerID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE service_jobs SET bound_node_id=? WHERE job_id=?`, node.NodeID, computer.CurrentJobID); err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.GetComputer(t.Context(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, ComputerReimageRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "operator"), Image: reimageTarget('6'),
		IdempotencyKey: "missing-generation-budget",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`DELETE FROM computer_storage_generations WHERE computer_id=? AND storage_id=? AND storage_generation=?`,
		computer.ComputerID, computer.StorageID, computer.StorageGeneration); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerReimagePreflightDirectives(t.Context(),
		"fabric-"+node.NodeID, node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0].DiskBytes != 0 {
		t.Fatalf("missing-budget reimage directives = %#v err=%v", directives, err)
	}
	directive := directives[0]
	receipt := ComputerReimagePreflightReceipt{Kind: computerReimagePreflightFailedReceiptKind,
		ReceiptID: "missing-budget-" + directive.OperationFence, ComputerID: directive.ComputerID,
		StorageID: directive.StorageID, StorageGeneration: directive.StorageGeneration,
		OldJobID: directive.OldJobID, StagingJobID: directive.StagingJobID, NodeID: directive.BoundNodeID,
		RootInstanceID: directive.RootInstanceID, OperationRevision: directive.OperationRevision,
		OperationFence: directive.OperationFence, TargetDigest: *directive.TargetImage.Digest,
		PlatformOS: "linux", PlatformArchitecture: "amd64", HelperGeneration: 7,
		FailureCode: string(contract.SpawnFailureReimagePreflight), FailureStage: "allocation_verify",
		FailureReason: "operation_failed"}
	failed, err := h.store.AcknowledgeComputerReimagePreflight(t.Context(), "fabric-"+node.NodeID,
		computer.ComputerID, ComputerReimagePreflightAcknowledgementRequest{NodeID: node.NodeID,
			BootSessionID: node.BootSessionID, IdempotencyKey: "missing-budget-refusal", Receipt: receipt})
	if err != nil || failed.ReconfigurationPhase != ComputerReconfigurationStable || failed.AppliedRevision != staged.IntentRevision {
		t.Fatalf("missing-budget typed refusal = %#v err=%v", failed, err)
	}
}

func TestComputerReimageAcknowledgementCapacityRefusalIsTypedOutcome(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "ack-capacity-refusal", Spec: computerCapabilityJobSpec("computer:ack-capacity-refusal"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.SetComputerDesiredState(t.Context(), computer.ComputerID,
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "stop"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE computers SET bound_node_id=? WHERE computer_id=?`, node.NodeID, computer.ComputerID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE service_jobs SET bound_node_id=? WHERE job_id=?`, node.NodeID, computer.CurrentJobID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE computers SET desired_state='running' WHERE computer_id=?`, computer.ComputerID); err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.GetComputer(t.Context(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, ComputerReimageRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "operator"), Image: reimageTarget('5'),
		IdempotencyKey: "ack-capacity-refusal",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, acknowledgement := reimagePreflightAcknowledgement(t, h, node, computer.ComputerID, "attempt", "attempt-fence")
	if _, err := h.store.db.Exec(`UPDATE nodes SET max_service_slots=0 WHERE node_id=?`, node.NodeID); err != nil {
		t.Fatal(err)
	}
	failed, err := h.store.AcknowledgeComputerReimagePreflight(t.Context(), "fabric-"+node.NodeID,
		computer.ComputerID, acknowledgement)
	var failure contract.SpawnFailure
	if err != nil || failed.ReconfigurationPhase != ComputerReconfigurationStable ||
		failed.AppliedRevision != staged.IntentRevision || failed.CurrentJobID != computer.CurrentJobID ||
		json.Unmarshal(failed.CurrentJob.LastFailure, &failure) != nil ||
		failure.Code != contract.SpawnFailureReimagePreflight || !strings.Contains(failure.Message, string(contract.ErrorCapacityExhausted)) {
		t.Fatalf("ack-time capacity refusal = %#v failure=%#v err=%v", failed, failure, err)
	}
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
	if reimaged.ReconfigurationPhase != ComputerReconfigurationReimaging || reimaged.CurrentJobID != original.CurrentJobID {
		t.Fatalf("staged reimage = %#v", reimaged)
	}
	acknowledgeReimagePreflight(t, h, node, computer.ComputerID, "", "")
	reimaged, err = h.store.ReimageComputer(t.Context(), computer.ComputerID, request)
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

func TestComputerReimageFailedDigestKeepsPriorProjectionOperable(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "reimage-failed-digest", Spec: computerCapabilityJobSpec("computer:reimage-failed:v1"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.SetComputerDesiredState(t.Context(), computer.ComputerID,
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "stop"))
	if err != nil {
		t.Fatal(err)
	}
	oldJobID, oldSpecRevision := computer.CurrentJobID, computer.CurrentSpecRevision
	before, err := h.store.IssueComputerPolicySnapshot(t.Context(), "fabric-"+node.NodeID, "fabric-test",
		node.NodeID, node.BootSessionID, time.Minute)
	if err != nil || before == nil || len(before.Computers) != 1 || before.Computers[0].ComputerID != computer.ComputerID {
		t.Fatalf("stable Computer policy = %#v err=%v", before, err)
	}
	staged, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, ComputerReimageRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "operator"), Image: reimageTarget('7'),
		IdempotencyKey: "missing-digest",
	})
	if err != nil {
		t.Fatal(err)
	}
	during, err := h.store.IssueComputerPolicySnapshot(t.Context(), "fabric-"+node.NodeID, "fabric-test",
		node.NodeID, node.BootSessionID, time.Minute)
	if err != nil || during == nil || len(during.Computers) != 0 {
		t.Fatalf("reimaging Computer remained in take-over policy = %#v err=%v", during, err)
	}
	directives, err := h.store.ListNodeComputerReimagePreflightDirectives(t.Context(),
		"fabric-"+node.NodeID, node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("reimage directives = %#v err=%v", directives, err)
	}
	directive := directives[0]
	if directive.DiskBytes != computer.DesiredDiskBytes {
		t.Fatalf("reimage directive disk_bytes = %d, want durable generation budget %d", directive.DiskBytes, computer.DesiredDiskBytes)
	}
	receipt := ComputerReimagePreflightReceipt{Kind: computerReimagePreflightFailedReceiptKind,
		ReceiptID: "failed-" + directive.OperationFence, ComputerID: directive.ComputerID,
		StorageID: directive.StorageID, StorageGeneration: directive.StorageGeneration,
		OldJobID: directive.OldJobID, StagingJobID: directive.StagingJobID, NodeID: directive.BoundNodeID,
		RootInstanceID: directive.RootInstanceID, OperationRevision: directive.OperationRevision,
		OperationFence: directive.OperationFence, TargetDigest: *directive.TargetImage.Digest,
		PlatformOS: "linux", PlatformArchitecture: "amd64", StorageEvidenceKind: computerReimageDetachmentEvidenceKind,
		DetachmentReceiptID: "detach", DetachmentAttemptID: "attempt", DetachmentFencingToken: "attempt-fence",
		HelperGeneration: 7, FailureCode: string(contract.SpawnFailureImageUnavailable),
		FailureStage: "image_identity", FailureReason: "image_unavailable"}
	failed, err := h.store.AcknowledgeComputerReimagePreflight(t.Context(), "fabric-"+node.NodeID,
		computer.ComputerID, ComputerReimagePreflightAcknowledgementRequest{NodeID: node.NodeID,
			BootSessionID: node.BootSessionID, IdempotencyKey: "ack-failed-digest", Receipt: receipt})
	if err != nil {
		t.Fatal(err)
	}
	var failure contract.SpawnFailure
	if failed.ReconfigurationPhase != ComputerReconfigurationStable || failed.CurrentJobID != oldJobID ||
		failed.CurrentJob.State != contract.JobStopped || failed.CurrentSpecRevision != oldSpecRevision ||
		failed.IntentRevision != staged.IntentRevision || failed.AppliedRevision != staged.IntentRevision ||
		json.Unmarshal(failed.CurrentJob.LastFailure, &failure) != nil || failure.Code != contract.SpawnFailureImageUnavailable {
		t.Fatalf("failed reimage changed prior authority = %#v failure=%#v", failed, failure)
	}
	var status string
	var retired bool
	if err := h.store.db.QueryRow(`SELECT status FROM computer_reimage_operations
		WHERE computer_id=? AND operation_revision=?`, computer.ComputerID, staged.IntentRevision).Scan(&status); err != nil || status != "failed" {
		t.Fatalf("reimage failure status = %q err=%v", status, err)
	}
	if err := h.store.db.QueryRow(`SELECT retired_ns IS NOT NULL FROM computer_job_projections WHERE job_id=?`,
		directive.StagingJobID).Scan(&retired); err != nil || !retired {
		t.Fatalf("failed staging projection retired = %t err=%v", retired, err)
	}
	retried, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, ComputerReimageRequest{
		ComputerMutationPrecondition: computerPrecondition(failed, "operator"), Image: reimageTarget('8'),
		IdempotencyKey: "retry-after-missing-digest",
	})
	if err != nil || retried.ReconfigurationPhase != ComputerReconfigurationReimaging ||
		retried.CurrentSpecRevision != oldSpecRevision || retried.IntentRevision != failed.IntentRevision+1 {
		t.Fatalf("retry reimage = %#v err=%v", retried, err)
	}
	var retrySpecRevision int64
	if err := h.store.db.QueryRow(`SELECT spec_revision FROM computer_job_projections
		WHERE computer_id=? AND job_id<>? AND retired_ns IS NULL`, computer.ComputerID, oldJobID).Scan(&retrySpecRevision); err != nil ||
		retrySpecRevision != oldSpecRevision+2 {
		t.Fatalf("retry spec revision = %d err=%v", retrySpecRevision, err)
	}
	directives, err = h.store.ListNodeComputerReimagePreflightDirectives(t.Context(),
		"fabric-"+node.NodeID, node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("retry reimage directives = %#v err=%v", directives, err)
	}
	directive = directives[0]
	receipt = ComputerReimagePreflightReceipt{Kind: computerReimagePreflightFailedReceiptKind,
		ReceiptID: "failed-allocation-" + directive.OperationFence, ComputerID: directive.ComputerID,
		StorageID: directive.StorageID, StorageGeneration: directive.StorageGeneration,
		OldJobID: directive.OldJobID, StagingJobID: directive.StagingJobID, NodeID: directive.BoundNodeID,
		RootInstanceID: directive.RootInstanceID, OperationRevision: directive.OperationRevision,
		OperationFence: directive.OperationFence, TargetDigest: *directive.TargetImage.Digest,
		PlatformOS: "linux", PlatformArchitecture: "amd64", HelperGeneration: 8,
		FailureCode: string(contract.SpawnFailureReimagePreflight), FailureStage: "allocation_verify",
		FailureReason: "operation_failed"}
	failed, err = h.store.AcknowledgeComputerReimagePreflight(t.Context(), "fabric-"+node.NodeID,
		computer.ComputerID, ComputerReimagePreflightAcknowledgementRequest{NodeID: node.NodeID,
			BootSessionID: node.BootSessionID, IdempotencyKey: "ack-failed-allocation", Receipt: receipt})
	if err != nil || json.Unmarshal(failed.CurrentJob.LastFailure, &failure) != nil ||
		failure.Code != contract.SpawnFailureReimagePreflight ||
		!strings.Contains(failure.Message, "allocation_verify: operation_failed") ||
		failed.ReconfigurationPhase != ComputerReconfigurationStable {
		t.Fatalf("typed mechanics failure = %#v failure=%#v err=%v", failed, failure, err)
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
	renewed, err := h.store.RenewLease(t.Context(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, claim.Lease.FencingToken)
	if err != nil || renewed.Directive != AttemptDirectiveStop {
		t.Fatalf("running Computer reimage renewal = %#v err=%v, want stop directive", renewed, err)
	}
	if _, err := h.store.CompleteAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID,
		CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "reimage-quiesced",
			Result: ProcessResult{OutputError: "quiesced"}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}); err != nil {
		t.Fatal(err)
	}
	acknowledgeReimagePreflight(t, h, node, computer.ComputerID, claim.Lease.AttemptID, claim.Lease.FencingToken)
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
		grown.CurrentJob.Spec.Execution.OCI.Computer.DiskBytes != target {
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
	if failed.ReconfigurationPhase != ComputerReconfigurationStable || failed.CurrentJob.State != contract.JobRunning ||
		failed.CurrentJob.DesiredState != contract.ServiceDesiredRunning || failed.DesiredState != contract.ServiceDesiredRunning ||
		failed.CurrentJob.CurrentAttemptID != claim.Lease.AttemptID || failed.DesiredDiskBytes != oldBytes ||
		len(failed.CurrentJob.LastFailure) == 0 {
		t.Fatalf("failed grow = %#v", failed)
	}
	restarted, replayed, err := h.store.RestartComputer(t.Context(), computer.ComputerID, ComputerRestartRequest{
		ComputerMutationPrecondition: computerPrecondition(failed, "operator"), IdempotencyKey: "recover-grow"})
	if err != nil || replayed || restarted.CurrentJob.State != contract.JobStopping || restarted.DesiredDiskBytes != oldBytes ||
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
		aborted.CurrentJobID != computer.CurrentJobID || aborted.CurrentJob.State != contract.JobStopped ||
		aborted.CurrentJob.DesiredState != contract.ServiceDesiredStopped || len(aborted.CurrentJob.LastFailure) == 0 {
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
	next := ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(aborted, "operator"),
		Image: reimageTarget('b'), TerminateSessions: true, IdempotencyKey: "reimage-after-abort"}
	stagedAgain, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, next)
	if err != nil || stagedAgain.ReconfigurationPhase != ComputerReconfigurationReimaging ||
		stagedAgain.CurrentSpecRevision != aborted.CurrentSpecRevision {
		t.Fatalf("reimage after abort = %#v err=%v", stagedAgain, err)
	}
	var stagedRevision int64
	if err := h.store.db.QueryRow(`SELECT MAX(spec_revision) FROM computer_job_projections WHERE computer_id=?`,
		computer.ComputerID).Scan(&stagedRevision); err != nil || stagedRevision != aborted.CurrentSpecRevision+2 {
		t.Fatalf("post-abort staging spec revision = %d err=%v", stagedRevision, err)
	}
}

func TestRemovalAcceptsAbortedComputerWithStoppedProjection(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "remove-aborted", Spec: computerCapabilityJobSpec("computer:remove-aborted"), Actor: "operator"})
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
	reimaging, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, ComputerReimageRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "operator"), Image: reimageTarget('f'),
		TerminateSessions: true, IdempotencyKey: "abort-before-remove"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE nodes SET state=? WHERE node_id=?`, contract.NodeDead, node.NodeID); err != nil {
		t.Fatal(err)
	}
	aborted, _, err := h.store.AbortComputerReconfiguration(t.Context(), computer.ComputerID,
		ComputerReconfigurationAbortRequest{ComputerMutationPrecondition: computerPrecondition(reimaging, "operator"), IdempotencyKey: "abort-before-remove"})
	if err != nil || aborted.CurrentJob.State != contract.JobStopped || aborted.CurrentJob.DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("aborted Computer = %#v err=%v", aborted, err)
	}
	removed, err := h.store.RemoveComputer(t.Context(), computer.ComputerID,
		ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(aborted, "operator")})
	if err != nil || removed.DesiredState != contract.ServiceDesiredRemoved ||
		removed.ReconfigurationPhase != ComputerReconfigurationRemoving || removed.CurrentJob.State != contract.JobRemovalPending {
		t.Fatalf("removed aborted Computer = %#v err=%v", removed, err)
	}
	var projectionDesiredState contract.ServiceDesiredState
	if err := h.store.db.QueryRow(`SELECT desired_state FROM service_jobs WHERE job_id=?`, removed.CurrentJobID).Scan(&projectionDesiredState); err != nil ||
		projectionDesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("removed aborted projection desired state = %q err=%v", projectionDesiredState, err)
	}
}

func TestResetAndGrowAbortRemainRetryableAndFenceLateAcknowledgement(t *testing.T) {
	for _, operation := range []string{"reset", "grow"} {
		t.Run(operation, func(t *testing.T) {
			h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
				"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
			})
			node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
				"kind:oci": true, "cgroup_v2": true, "computer": true,
			}, []string{contract.StableNodeTagPrefix + "computer-node"})
			computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
				Name: "abort-" + operation, Spec: computerCapabilityJobSpec("computer:abort:" + operation), Actor: "operator"})
			if err != nil {
				t.Fatal(err)
			}
			if operation == "reset" {
				computer, err = h.store.SetComputerDesiredState(t.Context(), computer.ComputerID,
					computerDesiredRequest(computer, contract.ServiceDesiredStopped, "stop"))
				if err != nil {
					t.Fatal(err)
				}
				computer, _, err = h.store.BeginComputerStorageReset(t.Context(), computer.ComputerID,
					ComputerStorageResetRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
						IdempotencyKey: "reset-before-abort"})
			} else {
				computer, _, err = h.store.BeginComputerGrow(t.Context(), computer.ComputerID,
					ComputerGrowRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
						DiskBytes: computer.DesiredDiskBytes + 1<<20, IdempotencyKey: "grow-before-abort"})
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.db.Exec(`UPDATE nodes SET state=? WHERE node_id=?`, contract.NodeDead, node.NodeID); err != nil {
				t.Fatal(err)
			}
			aborted, _, err := h.store.AbortComputerReconfiguration(t.Context(), computer.ComputerID,
				ComputerReconfigurationAbortRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
					IdempotencyKey: "abort-" + operation})
			if err != nil || aborted.ReconfigurationPhase != ComputerReconfigurationStable ||
				aborted.CurrentJob.State != contract.JobStopped {
				t.Fatalf("%s abort = %#v err=%v", operation, aborted, err)
			}
			if operation == "reset" {
				retrying, _, err := h.store.BeginComputerStorageReset(t.Context(), aborted.ComputerID,
					ComputerStorageResetRequest{ComputerMutationPrecondition: computerPrecondition(aborted, "operator"),
						IdempotencyKey: "reset-after-abort"})
				if err != nil || retrying.ReconfigurationPhase != ComputerReconfigurationResetting {
					t.Fatalf("reset retry = %#v err=%v", retrying, err)
				}
				var maximum int64
				if err := h.store.db.QueryRow(`SELECT MAX(storage_generation) FROM computer_storage_generations WHERE computer_id=?`,
					aborted.ComputerID).Scan(&maximum); err != nil || maximum != aborted.StorageGeneration+2 {
					t.Fatalf("reset retry generation = %d err=%v", maximum, err)
				}
			} else {
				row, err := readComputerGrowByRevision(t.Context(), h.store.db, aborted.ComputerID, computer.IntentRevision)
				if err != nil {
					t.Fatal(err)
				}
				receipt := ComputerStorageGrowReceipt{Kind: computerStorageGrowAppliedKind, ReceiptID: "late",
					ComputerID: row.ComputerID, StorageID: row.StorageID, StorageGeneration: row.StorageGeneration,
					NodeID: row.BoundNodeID, RootInstanceID: row.RootInstanceID, JobID: row.JobID,
					OperationRevision: row.OperationRevision, OperationFence: row.OperationFence,
					HelperGeneration: 7, OldDiskBytes: row.OldDiskBytes, NewDiskBytes: row.NewDiskBytes, Applied: true}
				_, err = h.store.AcknowledgeComputerStorageGrow(t.Context(), "fabric-computer-node", aborted.ComputerID,
					ComputerStorageGrowAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
						IdempotencyKey: "late-ack", Receipt: receipt})
				if errorCode(err) != contract.ErrorIdempotencyConflict {
					t.Fatalf("late grow acknowledgement error = %v", err)
				}
				retrying, _, err := h.store.BeginComputerGrow(t.Context(), aborted.ComputerID,
					ComputerGrowRequest{ComputerMutationPrecondition: computerPrecondition(aborted, "operator"),
						DiskBytes: aborted.DesiredDiskBytes + 2<<20, IdempotencyKey: "grow-after-abort"})
				if err != nil || retrying.ReconfigurationPhase != ComputerReconfigurationGrowing {
					t.Fatalf("grow retry = %#v err=%v", retrying, err)
				}
			}
		})
	}
}

func TestAbortCrashBoundariesRollbackAtomically(t *testing.T) {
	for _, checkpoint := range []string{"abort_artifacts_superseded", "abort_projection_stopped", "abort_recorded"} {
		t.Run(checkpoint, func(t *testing.T) {
			injected := errors.New("injected abort crash")
			active := checkpoint
			h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second,
				ReconfigurationHook: func(observed string) error {
					if observed == active {
						return injected
					}
					return nil
				}}, map[string]NodePolicy{"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 1}})
			node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
				"kind:oci": true, "cgroup_v2": true, "computer": true,
			}, []string{contract.StableNodeTagPrefix + "computer-node"})
			computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
				Name: "abort-crash-" + checkpoint, Spec: computerCapabilityJobSpec("computer:" + checkpoint), Actor: "operator"})
			if err != nil {
				t.Fatal(err)
			}
			growing, _, err := h.store.BeginComputerGrow(t.Context(), computer.ComputerID,
				ComputerGrowRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
					DiskBytes: computer.DesiredDiskBytes + 1<<20, IdempotencyKey: "grow"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.db.Exec(`UPDATE nodes SET state=? WHERE node_id=?`, contract.NodeDead, node.NodeID); err != nil {
				t.Fatal(err)
			}
			request := ComputerReconfigurationAbortRequest{ComputerMutationPrecondition: computerPrecondition(growing, "operator"),
				IdempotencyKey: "abort"}
			if _, _, err := h.store.AbortComputerReconfiguration(t.Context(), computer.ComputerID, request); !errors.Is(err, injected) {
				t.Fatalf("injected error = %v", err)
			}
			unchanged, _ := h.store.GetComputer(t.Context(), computer.ComputerID)
			if unchanged.ReconfigurationPhase != ComputerReconfigurationGrowing || unchanged.CurrentJob.State != contract.JobQueued {
				t.Fatalf("partially committed abort = %#v", unchanged)
			}
			active = ""
			if aborted, _, err := h.store.AbortComputerReconfiguration(t.Context(), computer.ComputerID, request); err != nil ||
				aborted.ReconfigurationPhase != ComputerReconfigurationStable {
				t.Fatalf("resumed abort = %#v err=%v", aborted, err)
			}
		})
	}
}

func TestReimageCrashBoundariesResumeWithoutPartialProjection(t *testing.T) {
	for _, checkpoint := range []string{"projection_staged", "projection_quiesced"} {
		t.Run(checkpoint, func(t *testing.T) {
			injected := errors.New("injected projection crash")
			active := checkpoint
			h := newIntegrationHarnessWithOptions(t, StoreOptions{ReconfigurationHook: func(observed string) error {
				if observed == active {
					return injected
				}
				return nil
			}}, map[string]NodePolicy{"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 1}})
			registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
				"kind:oci": true, "cgroup_v2": true, "computer": true,
			}, []string{contract.StableNodeTagPrefix + "computer-node"})
			computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
				Name: "projection-crash-" + checkpoint, Spec: computerCapabilityJobSpec("computer:" + checkpoint), Actor: "operator"})
			if err != nil {
				t.Fatal(err)
			}
			computer, err = h.store.SetComputerDesiredState(t.Context(), computer.ComputerID,
				computerDesiredRequest(computer, contract.ServiceDesiredStopped, "stop"))
			if err != nil {
				t.Fatal(err)
			}
			request := ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
				Image: reimageTarget('c'), Chown: true, IdempotencyKey: "reimage"}
			if _, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, request); !errors.Is(err, injected) {
				t.Fatalf("injected error = %v", err)
			}
			unchanged, _ := h.store.GetComputer(t.Context(), computer.ComputerID)
			var staged int
			if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_job_projections WHERE computer_id=? AND current=0`,
				computer.ComputerID).Scan(&staged); err != nil || unchanged.ReconfigurationPhase != ComputerReconfigurationStable || staged != 0 {
				t.Fatalf("partial projection = %#v staged=%d err=%v", unchanged, staged, err)
			}
			active = ""
			if resumed, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, request); err != nil ||
				resumed.ReconfigurationPhase != ComputerReconfigurationReimaging {
				t.Fatalf("resumed reimage = %#v err=%v", resumed, err)
			}
		})
	}

	t.Run("projection_finalized", func(t *testing.T) {
		active := ""
		injected := errors.New("injected finalize crash")
		h := newIntegrationHarnessWithOptions(t, StoreOptions{ReconfigurationHook: func(observed string) error {
			if observed == active {
				return injected
			}
			return nil
		}}, map[string]NodePolicy{"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 1}})
		node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
			"kind:oci": true, "cgroup_v2": true, "computer": true,
		}, []string{contract.StableNodeTagPrefix + "computer-node"})
		computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
			Name: "projection-finalize-crash", Spec: computerCapabilityJobSpec("computer:finalize-crash"), Actor: "operator"})
		if err != nil {
			t.Fatal(err)
		}
		computer, err = h.store.SetComputerDesiredState(t.Context(), computer.ComputerID,
			computerDesiredRequest(computer, contract.ServiceDesiredStopped, "stop"))
		if err != nil {
			t.Fatal(err)
		}
		request := ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
			Image: reimageTarget('d'), Chown: true, IdempotencyKey: "reimage"}
		staged, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, request)
		if err != nil {
			t.Fatal(err)
		}
		active = "projection_finalized"
		_, acknowledgement := reimagePreflightAcknowledgement(t, h, node, computer.ComputerID, "", "")
		if _, err := h.store.AcknowledgeComputerReimagePreflight(t.Context(), "fabric-"+node.NodeID,
			computer.ComputerID, acknowledgement); !errors.Is(err, injected) {
			t.Fatalf("injected finalize error = %v", err)
		}
		unchanged, _ := h.store.GetComputer(t.Context(), computer.ComputerID)
		if unchanged.ReconfigurationPhase != ComputerReconfigurationReimaging || unchanged.CurrentJobID != staged.CurrentJobID {
			t.Fatalf("partial finalize = %#v", unchanged)
		}
		active = ""
		if completed, err := h.store.AcknowledgeComputerReimagePreflight(t.Context(), "fabric-"+node.NodeID,
			computer.ComputerID, acknowledgement); err != nil ||
			completed.ReconfigurationPhase != ComputerReconfigurationStable || completed.CurrentJobID == staged.CurrentJobID {
			t.Fatalf("resumed finalize = %#v err=%v", completed, err)
		}
	})
}
