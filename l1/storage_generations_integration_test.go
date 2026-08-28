package l1

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func resetReceipt(directive ComputerStorageResetDirective) ComputerStorageResetReceipt {
	return ComputerStorageResetReceipt{Kind: computerStorageResetReceiptKind, ReceiptID: "helper-receipt-" + directive.CleanupFence,
		ComputerID: directive.ComputerID, StorageID: directive.StorageID, OldGeneration: directive.OldGeneration,
		NewGeneration: directive.NewGeneration, NodeID: directive.BoundNodeID, JobID: directive.JobID,
		IntentRevision: directive.IntentRevision, CleanupFence: directive.CleanupFence, HelperGeneration: 1}
}

func TestStorageResetAcknowledgementHashExcludesCurrentBootSession(t *testing.T) {
	receipt := ComputerStorageResetReceipt{Kind: computerStorageResetReceiptKind, ReceiptID: "receipt",
		ComputerID: "computer", StorageID: "storage", OldGeneration: 1, NewGeneration: 2,
		NodeID: "node", JobID: "job", IntentRevision: 2, CleanupFence: "fence", HelperGeneration: 7}
	first, _, err := storageResetAcknowledgementHash(ComputerStorageResetAcknowledgementRequest{
		NodeID: "node", BootSessionID: "boot-a", IdempotencyKey: "reset", Receipt: receipt})
	if err != nil {
		t.Fatal(err)
	}
	resumed, _, err := storageResetAcknowledgementHash(ComputerStorageResetAcknowledgementRequest{
		NodeID: "node", BootSessionID: "boot-b", IdempotencyKey: "reset", Receipt: receipt})
	if err != nil {
		t.Fatal(err)
	}
	if first != resumed {
		t.Fatalf("acknowledgement hash changed across boot resume: %q != %q", first, resumed)
	}
}

func TestComputerStorageResetReceiptFailsClosedAcrossAuthorityFields(t *testing.T) {
	directive := computerStorageResetRow{ComputerID: "computer", StorageID: "storage", OldGeneration: 3, NewGeneration: 4,
		BoundNodeID: "node", JobID: "job", IntentRevision: 7, CleanupFence: "fence"}
	valid := ComputerStorageResetReceipt{Kind: computerStorageResetReceiptKind, ReceiptID: "receipt",
		ComputerID: directive.ComputerID, StorageID: directive.StorageID, OldGeneration: directive.OldGeneration,
		NewGeneration: directive.NewGeneration, NodeID: directive.BoundNodeID, JobID: directive.JobID,
		IntentRevision: directive.IntentRevision, CleanupFence: directive.CleanupFence, HelperGeneration: 9}
	mutations := map[string]func(*ComputerStorageResetReceipt){
		"kind":              func(receipt *ComputerStorageResetReceipt) { receipt.Kind = "other" },
		"receipt":           func(receipt *ComputerStorageResetReceipt) { receipt.ReceiptID = "" },
		"helper generation": func(receipt *ComputerStorageResetReceipt) { receipt.HelperGeneration = 0 },
		"computer":          func(receipt *ComputerStorageResetReceipt) { receipt.ComputerID = "other" },
		"storage":           func(receipt *ComputerStorageResetReceipt) { receipt.StorageID = "other" },
		"old generation":    func(receipt *ComputerStorageResetReceipt) { receipt.OldGeneration++ },
		"new generation":    func(receipt *ComputerStorageResetReceipt) { receipt.NewGeneration++ },
		"node":              func(receipt *ComputerStorageResetReceipt) { receipt.NodeID = "other" },
		"job":               func(receipt *ComputerStorageResetReceipt) { receipt.JobID = "other" },
		"intent revision":   func(receipt *ComputerStorageResetReceipt) { receipt.IntentRevision++ },
		"cleanup fence":     func(receipt *ComputerStorageResetReceipt) { receipt.CleanupFence = "other" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			receipt := valid
			mutate(&receipt)
			if err := validateStorageResetReceipt(directive, receipt); errorCode(err) != contract.ErrorConflict {
				t.Fatalf("mutated receipt error = %v, want %q", err, contract.ErrorConflict)
			}
		})
	}
}

func assertComputerStorageResetLifecycle(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "resettable", Spec: computerCapabilityJobSpec("computer:storage-reset"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	grantsJSON, err := json.Marshal([]ComputerGrant{{UserID: "operator-user", Permission: ComputerGrantControl}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.ExecContext(context.Background(), `UPDATE computers SET grants_json=? WHERE computer_id=?`, grantsJSON, computer.ComputerID); err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	original := computer
	request := ComputerStorageResetRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "reset-1"}
	reserved, replayed, err := h.store.BeginComputerStorageReset(context.Background(), computer.ComputerID, request)
	if err != nil || replayed {
		t.Fatalf("reserve Storage reset = %#v replayed=%t err=%v", reserved, replayed, err)
	}
	if reserved.StorageGeneration != 1 || reserved.IntentRevision != 2 || reserved.AppliedRevision != 1 ||
		reserved.ReconfigurationPhase != ComputerReconfigurationResetting || reserved.CurrentJob.State != contract.JobStopped ||
		reserved.Name != original.Name || reserved.StorageID != original.StorageID || reserved.PlacementNodeID != original.PlacementNodeID ||
		reserved.DesiredDiskBytes != original.DesiredDiskBytes || len(reserved.Grants) != 1 || reserved.Grants[0] != original.Grants[0] {
		t.Fatalf("reserved Computer Storage reset = %#v", reserved)
	}
	var databaseSequence int
	var databaseName, databasePath string
	if err := h.store.db.QueryRowContext(context.Background(), "PRAGMA database_list").Scan(&databaseSequence, &databaseName, &databasePath); err != nil {
		t.Fatal(err)
	}
	resumedStore, err := OpenStore(databasePath, StoreOptions{Clock: h.clock, LeaseDuration: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer resumedStore.Close()
	if replay, wasReplay, err := resumedStore.BeginComputerStorageReset(context.Background(), computer.ComputerID, request); err != nil ||
		!wasReplay || replay.IntentRevision != reserved.IntentRevision {
		t.Fatalf("reservation replay = %#v replayed=%t err=%v", replay, wasReplay, err)
	}
	generations, err := resumedStore.ListComputerStorageGenerations(context.Background(), computer.ComputerID)
	if err != nil || len(generations.Generations) != 2 ||
		generations.Generations[0].Phase != ComputerStorageGenerationCurrent ||
		generations.Generations[1].Phase != ComputerStorageGenerationStaging ||
		generations.Generations[1].StorageGeneration != 2 ||
		generations.Generations[1].DiskBytes != original.DesiredDiskBytes {
		t.Fatalf("reserved Storage generations = %#v err=%v", generations, err)
	}
	if claim, err := resumedStore.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService); err != nil || claim != nil {
		t.Fatalf("resetting Computer became claimable: %#v err=%v", claim, err)
	}
	directives, err := resumedStore.ListNodeComputerStorageResetDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0].OldGeneration != 1 || directives[0].NewGeneration != 2 ||
		directives[0].IntentRevision != reserved.IntentRevision || directives[0].CleanupFence == "" {
		t.Fatalf("Storage reset directives = %#v err=%v", directives, err)
	}
	directive := directives[0]
	receipt := resetReceipt(directive)
	stale := receipt
	stale.IntentRevision++
	if err := resumedStore.recordComputerStorageResetVerification(context.Background(), "fabric-computer-node", computer.ComputerID,
		ComputerStorageResetAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: "stale", Receipt: stale}); errorCode(err) != contract.ErrorStaleIntentRevision {
		t.Fatalf("stale helper receipt error = %v, want %q", err, contract.ErrorStaleIntentRevision)
	}
	acknowledgement := ComputerStorageResetAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		IdempotencyKey: receipt.ReceiptID, Receipt: receipt}
	if err := resumedStore.recordComputerStorageResetVerification(context.Background(), "fabric-computer-node", computer.ComputerID, acknowledgement); err != nil {
		t.Fatal(err)
	}
	verifiedOnly, err := resumedStore.GetComputer(context.Background(), computer.ComputerID)
	if err != nil || verifiedOnly.StorageGeneration != 1 || verifiedOnly.ReconfigurationPhase != ComputerReconfigurationResetting {
		t.Fatalf("verification prematurely published Storage: %#v err=%v", verifiedOnly, err)
	}
	publicationResume, err := OpenStore(databasePath, StoreOptions{Clock: h.clock, LeaseDuration: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer publicationResume.Close()
	published, err := publicationResume.publishVerifiedComputerStorageReset(context.Background(), computer.ComputerID, reserved.IntentRevision)
	if err != nil {
		t.Fatal(err)
	}
	if published.StorageGeneration != 2 || published.IntentRevision != 2 || published.AppliedRevision != 2 ||
		published.ReconfigurationPhase != ComputerReconfigurationStable || published.CurrentJob.State != contract.JobQueued ||
		published.ComputerID != original.ComputerID || published.StorageID != original.StorageID || published.Name != original.Name ||
		published.PlacementNodeID != original.PlacementNodeID || published.DesiredDiskBytes != original.DesiredDiskBytes ||
		len(published.Grants) != 1 || published.Grants[0] != original.Grants[0] {
		t.Fatalf("published Computer Storage reset = %#v", published)
	}
	replayedPublication, err := publicationResume.publishVerifiedComputerStorageReset(context.Background(), computer.ComputerID, reserved.IntentRevision)
	if err != nil || replayedPublication.StorageGeneration != published.StorageGeneration ||
		replayedPublication.AppliedRevision != published.AppliedRevision {
		t.Fatalf("publication replay = %#v err=%v", replayedPublication, err)
	}
	generations, err = publicationResume.ListComputerStorageGenerations(context.Background(), computer.ComputerID)
	if err != nil || len(generations.Generations) != 2 || generations.Generations[0].Phase != ComputerStorageGenerationRetired ||
		generations.Generations[0].RetiredAt == nil || generations.Generations[1].Phase != ComputerStorageGenerationCurrent {
		t.Fatalf("published Storage generations = %#v err=%v", generations, err)
	}
	claim, err := publicationResume.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.ComputerStorage == nil || claim.ComputerStorage.StorageGeneration != 2 ||
		claim.ComputerStorage.IntentRevision != published.IntentRevision {
		t.Fatalf("post-reset claim = %#v err=%v", claim, err)
	}
}

func TestComputerStorageResetLifecycle(t *testing.T) {
	assertComputerStorageResetLifecycle(t)
}

func TestComputerStorageResetOperatorAndAgentRoutes(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	client := h.client(fabric.Identity{NodeID: "reset-operator", Tags: []string{DefaultClientPrincipalTag}})
	status, _, body := h.do(client, "POST", "/v1/computers", CreateComputerRequest{
		Name: "route-reset", Spec: computerCapabilityJobSpec("computer:route-reset"),
	})
	if status != 201 {
		t.Fatalf("create Computer status=%d body=%s", status, body)
	}
	var computer Computer
	if err := json.Unmarshal(body, &computer); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(client, "POST", "/v1/computers/"+computer.ComputerID+"/storage-reset",
		ComputerStorageResetRequest{ComputerMutationPrecondition: computerPrecondition(computer, "forged"), IdempotencyKey: "route-reset"})
	if status != 202 {
		t.Fatalf("reserve reset status=%d body=%s", status, body)
	}
	status, _, body = h.do(client, "GET", "/v1/computers/"+computer.ComputerID+"/storage-generations", nil)
	if status != 200 || !json.Valid(body) {
		t.Fatalf("list Storage generations status=%d body=%s", status, body)
	}
	agentClient := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	status, _, body = h.do(agentClient, "POST", "/v1/agent/nodes/"+node.NodeID+"/heartbeat", heartbeatRequestForNode(node))
	if status != 200 {
		t.Fatalf("reset heartbeat status=%d body=%s", status, body)
	}
	var heartbeat HeartbeatResponse
	if err := json.Unmarshal(body, &heartbeat); err != nil || len(heartbeat.StorageResetDirectives) != 1 {
		t.Fatalf("reset heartbeat = %#v err=%v", heartbeat, err)
	}
	directive := heartbeat.StorageResetDirectives[0]
	receipt := resetReceipt(directive)
	status, _, body = h.do(agentClient, "POST", "/v1/agent/computers/"+computer.ComputerID+"/storage-reset-acknowledgement",
		ComputerStorageResetAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
	if status != 200 {
		t.Fatalf("reset acknowledgement status=%d body=%s", status, body)
	}
	var published Computer
	if err := json.Unmarshal(body, &published); err != nil || published.StorageGeneration != 2 ||
		published.ReconfigurationPhase != ComputerReconfigurationStable {
		t.Fatalf("published reset route = %#v err=%v", published, err)
	}
}
