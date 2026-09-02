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
		RootInstanceID: directive.RootInstanceID,
		IntentRevision: directive.IntentRevision, CleanupFence: directive.CleanupFence, HelperGeneration: 1}
}

func TestComputerStorageCleanupQuarantineEvidenceIsAuthorityBound(t *testing.T) {
	receipt := ComputerStorageCleanupQuarantine{Kind: "managed_volume_cleanup_quarantined", ReceiptID: "receipt",
		Operation: ComputerStorageCleanupReset, VolumeKind: "computer_disk", ComputerID: "computer", StorageID: "storage", StorageGeneration: 1,
		NodeID: "node", BootSessionID: "boot", JobID: "job", RemovalGeneration: 2, CleanupFence: "fence",
		FailureReason: "operation_failed", Attempts: 3}
	if err := validateComputerStorageCleanupQuarantine(receipt, ComputerStorageCleanupReset, "computer", "storage", 1, "node", "boot", "job", 2, "fence"); err != nil {
		t.Fatal(err)
	}
	receipt.JobID = "other"
	if err := validateComputerStorageCleanupQuarantine(receipt, ComputerStorageCleanupReset, "computer", "storage", 1, "node", "boot", "job", 2, "fence"); errorCode(err) != contract.ErrorInvalidRequest {
		t.Fatalf("unbound cleanup quarantine error = %v", err)
	}
}

func TestStorageResetAcknowledgementHashExcludesCurrentBootSession(t *testing.T) {
	receipt := ComputerStorageResetReceipt{Kind: computerStorageResetReceiptKind, ReceiptID: "receipt",
		ComputerID: "computer", StorageID: "storage", OldGeneration: 1, NewGeneration: 2,
		NodeID: "node", RootInstanceID: "root", JobID: "job", IntentRevision: 2, CleanupFence: "fence", HelperGeneration: 7}
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
		BoundNodeID: "node", RootInstanceID: "root", JobID: "job", IntentRevision: 7, CleanupFence: "fence"}
	valid := ComputerStorageResetReceipt{Kind: computerStorageResetReceiptKind, ReceiptID: "receipt",
		ComputerID: directive.ComputerID, StorageID: directive.StorageID, OldGeneration: directive.OldGeneration,
		NewGeneration: directive.NewGeneration, NodeID: directive.BoundNodeID, JobID: directive.JobID,
		RootInstanceID: directive.RootInstanceID,
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
		"managed root":      func(receipt *ComputerStorageResetReceipt) { receipt.RootInstanceID = "other" },
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
	computer, err = h.store.SetComputerDesiredState(context.Background(), computer.ComputerID,
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "stop-before-reset"))
	if err != nil || computer.DesiredState != contract.ServiceDesiredStopped || computer.CurrentJob.State != contract.JobStopped {
		t.Fatalf("stop before reset = %#v err=%v", computer, err)
	}
	original := computer
	request := ComputerStorageResetRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "reset-1"}
	reserved, replayed, err := h.store.BeginComputerStorageReset(context.Background(), computer.ComputerID, request)
	if err != nil || replayed {
		t.Fatalf("reserve Storage reset = %#v replayed=%t err=%v", reserved, replayed, err)
	}
	if reserved.StorageGeneration != 1 || reserved.IntentRevision != original.IntentRevision+1 || reserved.AppliedRevision != original.AppliedRevision ||
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
	otherRegistration := node.NodeRegistration
	otherRegistration.NodeID = "other-reset-node"
	otherRegistration.BootSessionID = "other-reset-boot"
	otherRegistration.RootInstanceID = "other-managed-root"
	otherNode, err := h.store.RegisterNode(context.Background(), fabric.Identity{NodeID: "fabric-computer-node"},
		otherRegistration, NodePolicy{MaxOneshotSlots: 1, MaxServiceSlots: 1}, false)
	if err != nil {
		t.Fatal(err)
	}
	forgedNode := receipt
	forgedNode.NodeID = otherNode.NodeID
	forgedNode.RootInstanceID = otherNode.RootInstanceID
	if err := resumedStore.recordComputerStorageResetVerification(context.Background(), "fabric-computer-node", computer.ComputerID,
		ComputerStorageResetAcknowledgementRequest{NodeID: otherNode.NodeID, BootSessionID: otherNode.BootSessionID,
			IdempotencyKey: "forged-node", Receipt: forgedNode}); errorCode(err) != contract.ErrorAttemptNotOwned {
		t.Fatalf("directive-node substitution error = %v, want %q", err, contract.ErrorAttemptNotOwned)
	}
	if _, err := resumedStore.db.Exec(`UPDATE nodes SET root_instance_id='recreated-root' WHERE node_id=?`, node.NodeID); err != nil {
		t.Fatal(err)
	}
	if err := resumedStore.recordComputerStorageResetVerification(context.Background(), "fabric-computer-node", computer.ComputerID,
		ComputerStorageResetAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: "stale-root", Receipt: receipt}); errorCode(err) != contract.ErrorStaleFence {
		t.Fatalf("recreated-root receipt error = %v, want %q", err, contract.ErrorStaleFence)
	}
	if _, err := resumedStore.db.Exec(`UPDATE nodes SET root_instance_id=? WHERE node_id=?`, directive.RootInstanceID, node.NodeID); err != nil {
		t.Fatal(err)
	}
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
	if published.StorageGeneration != 2 || published.IntentRevision != reserved.IntentRevision || published.AppliedRevision != original.AppliedRevision ||
		published.ReconfigurationPhase != ComputerReconfigurationResetting || published.CurrentJob.State != contract.JobStopped ||
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
	postPublicationDirectives, err := publicationResume.ListNodeComputerStorageResetDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(postPublicationDirectives) != 1 || postPublicationDirectives[0].Phase != "published" {
		t.Fatalf("post-publication retirement directive = %#v err=%v", postPublicationDirectives, err)
	}
	completed, err := publicationResume.AcknowledgeComputerStorageRetirement(context.Background(), "fabric-computer-node", computer.ComputerID,
		RemovalAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			RemovalGeneration: uint64(reserved.IntentRevision), CleanupFence: directive.CleanupFence,
			RootInstanceID: directive.RootInstanceID, IdempotencyKey: "retired"})
	if err != nil || completed.ReconfigurationPhase != ComputerReconfigurationStable ||
		completed.AppliedRevision != completed.IntentRevision || completed.DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("completed reset = %#v err=%v", completed, err)
	}
	claim, err := publicationResume.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim != nil {
		t.Fatalf("post-reset stopped Computer became claimable = %#v err=%v", claim, err)
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
	status, _, body = h.do(client, "PUT", "/v1/computers/"+computer.ComputerID+"/desired-state",
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "stop-before-reset"))
	if status != 202 || json.Unmarshal(body, &computer) != nil {
		t.Fatalf("stop before route reset status=%d body=%s", status, body)
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
		published.ReconfigurationPhase != ComputerReconfigurationResetting || published.DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("published reset route = %#v err=%v", published, err)
	}
	status, _, body = h.do(agentClient, "POST", "/v1/agent/computers/"+computer.ComputerID+"/storage-retirement-acknowledgement",
		RemovalAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			RemovalGeneration: uint64(directive.IntentRevision), CleanupFence: directive.CleanupFence,
			RootInstanceID: directive.RootInstanceID, IdempotencyKey: "route-retired"})
	if status != 200 || json.Unmarshal(body, &published) != nil || published.ReconfigurationPhase != ComputerReconfigurationStable {
		t.Fatalf("retirement acknowledgement status=%d body=%s", status, body)
	}
}

func TestComputerStorageResetRefusalMatrixAndGenerationUniqueness(t *testing.T) {
	newStopped := func(t *testing.T, name string) (*integrationHarness, Computer) {
		t.Helper()
		h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
			"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 2},
		})
		registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
			"kind:oci": true, "cgroup_v2": true, "computer": true,
		}, []string{contract.StableNodeTagPrefix + "computer-node"})
		computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
			Name: name, Spec: computerCapabilityJobSpec("computer:" + name), Actor: "operator"})
		if err != nil {
			t.Fatal(err)
		}
		computer, err = h.store.SetComputerDesiredState(t.Context(), computer.ComputerID,
			computerDesiredRequest(computer, contract.ServiceDesiredStopped, "stop"))
		if err != nil {
			t.Fatal(err)
		}
		return h, computer
	}

	t.Run("desired-running but detached", func(t *testing.T) {
		h, computer := newStopped(t, "running-refusal")
		if _, err := h.store.db.Exec(`UPDATE computers SET desired_state='running' WHERE computer_id=?`, computer.ComputerID); err != nil {
			t.Fatal(err)
		}
		computer.DesiredState = contract.ServiceDesiredRunning
		resetting, _, err := h.store.BeginComputerStorageReset(t.Context(), computer.ComputerID,
			ComputerStorageResetRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "running"})
		if err != nil || resetting.DesiredState != contract.ServiceDesiredRunning ||
			resetting.ReconfigurationPhase != ComputerReconfigurationResetting {
			t.Fatalf("detached desired-running reset = %#v err=%v", resetting, err)
		}
	})

	for _, phase := range []ComputerReconfigurationPhase{ComputerReconfigurationProjecting, ComputerReconfigurationRemoving} {
		t.Run(string(phase), func(t *testing.T) {
			h, computer := newStopped(t, "phase-"+string(phase))
			if _, err := h.store.db.Exec(`UPDATE computers SET reconfiguration_phase=?, reconfiguration_revision=intent_revision WHERE computer_id=?`, phase, computer.ComputerID); err != nil {
				t.Fatal(err)
			}
			computer.ReconfigurationPhase = phase
			revision := computer.IntentRevision
			computer.ReconfigurationRevision = &revision
			_, _, err := h.store.BeginComputerStorageReset(t.Context(), computer.ComputerID,
				ComputerStorageResetRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "phase"})
			if errorCode(err) != contract.ErrorConflict {
				t.Fatalf("phase %q reset error = %v", phase, err)
			}
		})
	}

	t.Run("resetting removal supersedes", func(t *testing.T) {
		h, computer := newStopped(t, "resetting-removal")
		request := ComputerStorageResetRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "reset"}
		resetting, _, err := h.store.BeginComputerStorageReset(t.Context(), computer.ComputerID, request)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = h.store.BeginComputerStorageReset(t.Context(), computer.ComputerID,
			ComputerStorageResetRequest{ComputerMutationPrecondition: computerPrecondition(resetting, "operator"), IdempotencyKey: "second"})
		if errorCode(err) != contract.ErrorConflict {
			t.Fatalf("resetting reset error = %v", err)
		}
		changedAuthority := request
		changedAuthority.Actor = "different-actor"
		if _, _, err := h.store.BeginComputerStorageReset(t.Context(), computer.ComputerID, changedAuthority); errorCode(err) != contract.ErrorConflict {
			t.Fatalf("idempotency authority reuse error = %v", err)
		}
		removed, err := h.store.RemoveComputer(t.Context(), computer.ComputerID,
			ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(resetting, "operator")})
		if err != nil || removed.DesiredState != contract.ServiceDesiredRemoved {
			t.Fatalf("removal did not supersede reset: %#v err=%v", removed, err)
		}
		var status string
		if err := h.store.db.QueryRow(`SELECT status FROM computer_storage_resets WHERE computer_id=?`, computer.ComputerID).Scan(&status); err != nil || status != "superseded" {
			t.Fatalf("superseded reset status = %q err=%v", status, err)
		}
		_, _, err = h.store.BeginComputerStorageReset(t.Context(), computer.ComputerID,
			ComputerStorageResetRequest{ComputerMutationPrecondition: computerPrecondition(removed, "operator"), IdempotencyKey: "removed"})
		if errorCode(err) != contract.ErrorConflict {
			t.Fatalf("removed reset error = %v", err)
		}
	})

	t.Run("one current generation", func(t *testing.T) {
		h, computer := newStopped(t, "unique-current")
		_, err := h.store.db.Exec(`INSERT INTO computer_storage_generations(
			computer_id, storage_id, storage_generation, disk_bytes, phase, created_ns
		) VALUES(?, ?, 99, ?, 'current', ?)`, computer.ComputerID, computer.StorageID,
			computer.DesiredDiskBytes, h.clock.Now().UnixNano())
		if err == nil {
			t.Fatalf("partial unique index accepted a second current generation: %v", err)
		}
	})
}
