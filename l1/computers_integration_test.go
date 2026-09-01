package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestComputerOperatorRoutesPreserveCASAndRedaction(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{})
	client := h.client(fabric.Identity{NodeID: "computer-client", Tags: []string{DefaultClientPrincipalTag}})
	spec := computerCapabilityJobSpec("computer:http")
	spec.Execution.SensitiveEnv = map[string]string{"TOKEN": "never-publish"}
	status, _, body := h.do(client, http.MethodPost, "/v1/computers", map[string]any{
		"name": "alice", "spec": spec, "actor": "forged-actor",
	})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	status, headers, body := h.do(client, http.MethodPost, "/v1/computers", CreateComputerRequest{
		Name: "alice", Spec: spec, Actor: "api-test",
	})
	if status != http.StatusCreated {
		t.Fatalf("create Computer status = %d body=%s", status, body)
	}
	if headers.Get("Idempotent-Replay") != "" || string(body) == "" {
		t.Fatalf("create Computer headers/body = %v / %s", headers, body)
	}
	if containsJSONSecret(body, "never-publish") {
		t.Fatalf("Computer response leaked SensitiveEnv: %s", body)
	}
	if !bytes.Contains(body, []byte(`"display_endpoint":null`)) {
		t.Fatalf("inactive Computer response did not publish an explicit null display endpoint: %s", body)
	}
	var computer Computer
	if err := json.Unmarshal(body, &computer); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(client, http.MethodGet, "/v1/computers/"+computer.ComputerID+"/intents?limit=1", nil)
	if status != http.StatusOK {
		t.Fatalf("list Computer intents status = %d body=%s", status, body)
	}
	var intents ComputerIntentList
	if err := json.Unmarshal(body, &intents); err != nil {
		t.Fatal(err)
	}
	if len(intents.Intents) != 1 || intents.Intents[0].Actor != "computer-client" {
		t.Fatalf("server-derived Computer actor = %#v", intents)
	}
	status, _, body = h.do(client, http.MethodGet, "/v1/computers/"+computer.ComputerID, nil)
	if status != http.StatusOK || containsJSONSecret(body, "never-publish") {
		t.Fatalf("get Computer status/body = %d / %s", status, body)
	}
	status, _, body = h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/desired-state",
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "api-stop"))
	if status != http.StatusAccepted {
		t.Fatalf("stop Computer status = %d body=%s", status, body)
	}
	var stopped Computer
	if err := json.Unmarshal(body, &stopped); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/desired-state",
		computerDesiredRequest(computer, contract.ServiceDesiredRunning, "stale-api-start"))
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorStaleIntentRevision)
	status, _, body = h.do(client, http.MethodPut,
		"/v1/jobs/"+computer.CurrentJobID+"/desired-state?class=service",
		ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredRunning})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorComputerResourceRequired)
	status, _, body = h.do(client, http.MethodPost, "/v1/computers/"+computer.ComputerID+"/remove",
		ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(stopped, "api-remove")})
	if status != http.StatusAccepted {
		t.Fatalf("remove Computer status = %d body=%s", status, body)
	}
	var neverBoundRemoved Computer
	if err := json.Unmarshal(body, &neverBoundRemoved); err != nil {
		t.Fatal(err)
	}
	if neverBoundRemoved.RemovalOutcome != "removed_verified" || neverBoundRemoved.CurrentJob.State != contract.JobRemovedVerified {
		t.Fatalf("never-bound Computer removal stayed non-terminal: %#v", neverBoundRemoved)
	}
	provenance, err := h.store.ListComputerStorageProvenance(context.Background(), neverBoundRemoved.ComputerID)
	if err != nil || provenance.CustodyTainted || len(provenance.CustodyForks) != 1 ||
		provenance.CustodyForks[0].RemovalOutcome != "removed_verified" {
		t.Fatalf("verified removal provenance projection = %#v err=%v", provenance, err)
	}
	var storedSpec []byte
	if err := h.store.db.QueryRow(`SELECT spec_json FROM jobs WHERE job_id=?`, computer.CurrentJobID).Scan(&storedSpec); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(storedSpec, []byte("never-publish")) {
		t.Fatalf("Computer removal retained SensitiveEnv in controller state: %s", storedSpec)
	}
}

func containsJSONSecret(body []byte, secret string) bool {
	return len(secret) > 0 && bytes.Contains(body, []byte(secret))
}

func TestComputerRestartProjectionAndIntentRoutes(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{})
	client := h.client(fabric.Identity{NodeID: "route-operator", Tags: []string{DefaultClientPrincipalTag}})
	status, _, body := h.do(client, http.MethodPost, "/v1/computers", CreateComputerRequest{
		Name: "route-computer", Spec: computerCapabilityJobSpec("computer:routes:v1"),
	})
	if status != http.StatusCreated {
		t.Fatalf("create Computer status = %d body=%s", status, body)
	}
	var computer Computer
	if err := json.Unmarshal(body, &computer); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(client, http.MethodPut, "/v1/computers/"+computer.ComputerID+"/desired-state",
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "forged-helper-actor"))
	if status != http.StatusAccepted {
		t.Fatalf("stop Computer status = %d body=%s", status, body)
	}
	var stopped Computer
	if err := json.Unmarshal(body, &stopped); err != nil {
		t.Fatal(err)
	}
	nextSpec := computerCapabilityJobSpec("computer:routes:v2")
	digest := "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	nextSpec.Execution.OCI.Image.Digest = &digest
	status, _, body = h.do(client, http.MethodPost, "/v1/computers/"+computer.ComputerID+"/projections",
		ComputerProjectionRequest{ComputerMutationPrecondition: computerPrecondition(stopped, "forged-helper-actor"), Spec: nextSpec})
	if status != http.StatusAccepted {
		t.Fatalf("install Computer projection status = %d body=%s", status, body)
	}
	var projected Computer
	if err := json.Unmarshal(body, &projected); err != nil {
		t.Fatal(err)
	}
	if projected.CurrentJobID == stopped.CurrentJobID || projected.CurrentSpecRevision != 2 {
		t.Fatalf("route-installed Computer projection = %#v", projected)
	}
	restartRequest := ComputerRestartRequest{
		ComputerMutationPrecondition: computerPrecondition(projected, "forged-helper-actor"),
		IdempotencyKey:               "route-restart",
	}
	status, headers, body := h.do(client, http.MethodPost, "/v1/computers/"+computer.ComputerID+"/restart", restartRequest)
	if status != http.StatusAccepted || headers.Get("Idempotent-Replay") != "" {
		t.Fatalf("restart Computer status/header = %d/%q body=%s", status, headers.Get("Idempotent-Replay"), body)
	}
	status, headers, body = h.do(client, http.MethodPost, "/v1/computers/"+computer.ComputerID+"/restart", restartRequest)
	if status != http.StatusOK || headers.Get("Idempotent-Replay") != "true" {
		t.Fatalf("restart Computer replay = %d/%q body=%s", status, headers.Get("Idempotent-Replay"), body)
	}
	status, _, body = h.do(client, http.MethodGet, "/v1/computers/"+computer.ComputerID+"/intents?limit=2", nil)
	if status != http.StatusOK {
		t.Fatalf("first intent page status = %d body=%s", status, body)
	}
	var firstPage ComputerIntentList
	if err := json.Unmarshal(body, &firstPage); err != nil {
		t.Fatal(err)
	}
	if len(firstPage.Intents) != 2 || firstPage.NextCursor == "" {
		t.Fatalf("first intent page = %#v", firstPage)
	}
	for _, intent := range firstPage.Intents {
		if intent.Actor != "route-operator" {
			t.Fatalf("client-forged actor reached history: %#v", intent)
		}
	}
	status, _, body = h.do(client, http.MethodGet, "/v1/computers/"+computer.ComputerID+
		"/intents?limit=2&cursor="+firstPage.NextCursor, nil)
	if status != http.StatusOK {
		t.Fatalf("second intent page status = %d body=%s", status, body)
	}
	var secondPage ComputerIntentList
	if err := json.Unmarshal(body, &secondPage); err != nil {
		t.Fatal(err)
	}
	if len(secondPage.Intents) != 2 || secondPage.NextCursor != "" ||
		secondPage.Intents[0].Operation != ComputerIntentProject ||
		secondPage.Intents[1].Operation != ComputerIntentRestart {
		t.Fatalf("second intent page = %#v", secondPage)
	}
}

func TestComputerIntentOwnsLifecycleAndSlot(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {
			Tags:            []string{contract.StableNodeTagPrefix + "computer-node"},
			MaxOneshotSlots: 1, MaxServiceSlots: 1,
		},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, replayed, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "alice", Spec: computerCapabilityJobSpec("computer:alice:v1"), Actor: "operator-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed {
		t.Fatal("first Computer creation was reported as a replay")
	}
	if computer.ComputerID == computer.CurrentJobID || computer.StorageID == computer.ComputerID ||
		computer.PlacementNodeID != node.NodeID || computer.IntentRevision != 1 ||
		computer.AppliedRevision != 1 || computer.CurrentSpecRevision != 1 ||
		computer.ReconfigurationPhase != ComputerReconfigurationStable {
		t.Fatalf("initial Computer = %#v", computer)
	}
	history, err := h.store.ListComputerIntents(context.Background(), computer.ComputerID, "", 1)
	if err != nil || len(history.Intents) != 1 || history.Intents[0].Actor != "operator-a" || history.NextCursor != "" {
		t.Fatalf("initial Computer intents = %#v err=%v", history, err)
	}
	replayedComputer, replayed, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "alice", Spec: computerCapabilityJobSpec("computer:alice:v1"), Actor: "operator-a",
	})
	if err != nil || !replayed || replayedComputer.ComputerID != computer.ComputerID {
		t.Fatalf("Computer creation replay = %#v replayed=%t err=%v", replayedComputer, replayed, err)
	}
	if _, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "alice", Spec: computerCapabilityJobSpec("computer:alice:v1"), Actor: "different-operator",
	}); errorCode(err) != contract.ErrorDispatchKeyConflict {
		t.Fatalf("different-actor creation replay error = %v, want %q", err, contract.ErrorDispatchKeyConflict)
	}

	claim, err := h.store.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil || claim.Job.JobID != computer.CurrentJobID {
		t.Fatalf("Computer claim = %#v, want Job %q", claim, computer.CurrentJobID)
	}
	if claim.ComputerStorage == nil || claim.ComputerStorage.ComputerID != computer.ComputerID ||
		claim.ComputerStorage.StorageID != computer.StorageID || claim.ComputerStorage.StorageGeneration != computer.StorageGeneration {
		t.Fatalf("Computer claim Storage identity = %#v, want %s/%s@%d", claim.ComputerStorage,
			computer.ComputerID, computer.StorageID, computer.StorageGeneration)
	}
	if _, err := h.store.ObserveAttemptImage(context.Background(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.StartAttempt(context.Background(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	if computer.BoundNodeID != node.NodeID || !computer.CurrentJob.HoldsSlot(computer.CurrentJob.State) {
		t.Fatalf("claimed Computer binding/slot = %#v", computer)
	}
	retainedStorage := computer.StorageID

	stopping, err := h.store.SetComputerDesiredState(context.Background(), computer.ComputerID,
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator-stop"))
	if err != nil {
		t.Fatal(err)
	}
	if stopping.DesiredState != contract.ServiceDesiredStopped || stopping.CurrentJob.State != contract.JobStopping ||
		stopping.IntentRevision != 2 {
		t.Fatalf("stopping Computer = %#v", stopping)
	}
	if _, err := h.store.CompleteAttempt(context.Background(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, CompletionRequest{
			FencingToken: claim.Lease.FencingToken, IdempotencyKey: "computer-stop",
			Result:                    ProcessResult{OutputError: "logs finalized after positive reap"},
			RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt,
		}); err != nil {
		t.Fatal(err)
	}
	stopped, err := h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.CurrentJob.State != contract.JobStopped || stopped.CurrentJob.HoldsSlot(stopped.CurrentJob.State) ||
		stopped.BoundNodeID != node.NodeID || stopped.StorageID != retainedStorage || len(stopped.Grants) != 0 {
		t.Fatalf("stopped Computer did not release only its Slot: %#v", stopped)
	}

	wrongStorage := computerDesiredRequest(stopped, contract.ServiceDesiredRunning, "wrong-storage")
	wrongStorage.StorageGeneration++
	if _, err := h.store.SetComputerDesiredState(context.Background(), stopped.ComputerID, wrongStorage); errorCode(err) != contract.ErrorStorageReferenceConflict {
		t.Fatalf("wrong storage error = %v, want %q", err, contract.ErrorStorageReferenceConflict)
	}
	unchanged, err := h.store.GetComputer(context.Background(), stopped.ComputerID)
	if err != nil || unchanged.IntentRevision != stopped.IntentRevision || unchanged.CurrentJob.State != contract.JobStopped {
		t.Fatalf("storage conflict mutated Computer: %#v err=%v", unchanged, err)
	}

	type mutationResult struct {
		computer Computer
		err      error
	}
	start := make(chan struct{})
	results := make(chan mutationResult, 2)
	var wait sync.WaitGroup
	for index := 0; index < 2; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			started, err := h.store.SetComputerDesiredState(context.Background(), stopped.ComputerID,
				computerDesiredRequest(stopped, contract.ServiceDesiredRunning, "competing-start"))
			results <- mutationResult{computer: started, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	wins, stale := 0, 0
	for result := range results {
		if result.err == nil {
			wins++
			if result.computer.IntentRevision != stopped.IntentRevision+1 ||
				result.computer.CurrentJob.State != contract.JobQueued {
				t.Fatalf("winning start = %#v", result.computer)
			}
		} else if errorCode(result.err) == contract.ErrorStaleIntentRevision {
			stale++
		} else {
			t.Fatalf("competing start error = %v", result.err)
		}
	}
	if wins != 1 || stale != 1 {
		t.Fatalf("competing starts = %d wins / %d stale, want 1/1", wins, stale)
	}
	started, err := h.store.GetComputer(context.Background(), stopped.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	if !started.CurrentJob.HoldsSlot(started.CurrentJob.State) || started.BoundNodeID != node.NodeID {
		t.Fatalf("started Computer did not reacquire exactly one Slot: %#v", started)
	}
}

func TestComputerProjectionRetirementAndRemovalForecloseClaims(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {
			Tags:            []string{contract.StableNodeTagPrefix + "computer-node"},
			MaxOneshotSlots: 1, MaxServiceSlots: 1,
		},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "alice", Spec: computerCapabilityJobSpec("computer:projection:v1"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim = %#v err=%v", claim, err)
	}
	if _, err := h.store.ObserveAttemptImage(context.Background(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.StartAttempt(context.Background(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.SetComputerDesiredState(context.Background(), computer.ComputerID,
		computerDesiredRequest(computer, contract.ServiceDesiredStopped, "projection-stop")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.CompleteAttempt(context.Background(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, CompletionRequest{
			FencingToken: claim.Lease.FencingToken, IdempotencyKey: "projection-stop",
			Result:                    ProcessResult{OutputError: "logs finalized after positive reap"},
			RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt,
		}); err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	if computer.DesiredState != contract.ServiceDesiredStopped || computer.CurrentJob.State != contract.JobStopped {
		t.Fatalf("quiescent Computer = %#v", computer)
	}
	oldJobID := computer.CurrentJobID
	nextSpec := computerCapabilityJobSpec("computer:projection:v2")
	newDigest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	nextSpec.Execution.OCI.Image.Digest = &newDigest
	projected, err := h.store.InstallComputerProjection(context.Background(), computer.ComputerID, ComputerProjectionRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "reconfigure"),
		Spec:                         nextSpec,
	})
	if err != nil {
		t.Fatal(err)
	}
	if projected.CurrentJobID == oldJobID || projected.CurrentSpecRevision != 2 ||
		projected.IntentRevision != computer.IntentRevision+1 || projected.CurrentJob.State != contract.JobStopped ||
		projected.AppliedRevision != projected.IntentRevision || projected.BoundNodeID != node.NodeID ||
		projected.CurrentJob.BoundNodeID != node.NodeID {
		t.Fatalf("projected Computer = %#v", projected)
	}
	listed, err := h.store.ListServiceJobs(context.Background(), "", MaxJobPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	for _, listedJob := range listed.Jobs {
		if listedJob.JobID == oldJobID {
			t.Fatalf("retired Computer projection appeared in service list: %#v", listedJob)
		}
	}
	var oldCurrent int
	if err := h.store.db.QueryRow(`SELECT current FROM computer_job_projections WHERE job_id=?`, oldJobID).Scan(&oldCurrent); err != nil {
		t.Fatal(err)
	}
	if oldCurrent != 0 {
		t.Fatalf("old Computer Job current = %d, want 0", oldCurrent)
	}
	if _, err := h.store.SetServiceDesiredState(context.Background(), oldJobID, contract.ServiceDesiredRunning); errorCode(err) != contract.ErrorComputerResourceRequired {
		t.Fatalf("old Job direct start error = %v, want %q", err, contract.ErrorComputerResourceRequired)
	}
	if _, err := h.store.db.Exec(`UPDATE jobs SET state=? WHERE job_id=?`, contract.JobQueued, oldJobID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE service_jobs SET desired_state=?, bound_node_id=? WHERE job_id=?`,
		contract.ServiceDesiredRunning, node.NodeID, oldJobID); err != nil {
		t.Fatal(err)
	}
	claim, err = h.store.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil {
		t.Fatal(err)
	}
	if claim != nil {
		t.Fatalf("retired Computer Job became claimable: %#v", claim)
	}

	removed, err := h.store.RemoveComputer(context.Background(), projected.ComputerID, ComputerRemoveRequest{
		ComputerMutationPrecondition: computerPrecondition(projected, "remove"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if removed.DesiredState != contract.ServiceDesiredRemoved || removed.ReconfigurationPhase != ComputerReconfigurationRemoving ||
		removed.CurrentJob.State != contract.JobRemovalPending {
		t.Fatalf("removed Computer = %#v", removed)
	}
	if _, err := h.store.db.Exec(`UPDATE jobs SET state=? WHERE job_id=?`, contract.JobQueued, removed.CurrentJobID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE service_jobs SET desired_state=? WHERE job_id=?`, contract.ServiceDesiredRunning, removed.CurrentJobID); err != nil {
		t.Fatal(err)
	}
	claim, err = h.store.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil {
		t.Fatal(err)
	}
	if claim != nil {
		t.Fatalf("removed Computer minted a new attempt: %#v", claim)
	}
	nextSpec.DispatchKey = "computer:projection:v3"
	if _, err := h.store.InstallComputerProjection(context.Background(), removed.ComputerID, ComputerProjectionRequest{
		ComputerMutationPrecondition: computerPrecondition(removed, "late-project"), Spec: nextSpec,
	}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("removed Computer projection error = %v, want conflict", err)
	}
}

func TestComputerProjectionQuiescesRunningObservationWithInternalServiceStop(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {
			Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1,
		},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "alice-running-project", Spec: computerCapabilityJobSpec("computer:running-project:v1"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim = %#v err=%v", claim, err)
	}
	if _, err := h.store.ObserveAttemptImage(context.Background(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.StartAttempt(context.Background(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	oldJobID := computer.CurrentJobID
	nextSpec := computerCapabilityJobSpec("computer:running-project:v2")
	digest := "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	nextSpec.Execution.OCI.Image.Digest = &digest
	projecting, err := h.store.InstallComputerProjection(context.Background(), computer.ComputerID, ComputerProjectionRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "operator"), Spec: nextSpec,
	})
	if err != nil {
		t.Fatal(err)
	}
	if projecting.DesiredState != contract.ServiceDesiredRunning ||
		projecting.ReconfigurationPhase != ComputerReconfigurationProjecting ||
		projecting.CurrentJobID != oldJobID || projecting.CurrentJob.State != contract.JobStopping ||
		projecting.CurrentJob.DesiredState != contract.ServiceDesiredStopped ||
		projecting.IntentRevision != computer.IntentRevision+1 || projecting.AppliedRevision != computer.AppliedRevision {
		t.Fatalf("projecting Computer = %#v", projecting)
	}
	history, err := h.store.ListComputerIntents(context.Background(), computer.ComputerID, "", MaxJobPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(history.Intents) != 2 || history.Intents[1].Operation != ComputerIntentProject ||
		history.Intents[1].DesiredState != contract.ServiceDesiredRunning {
		t.Fatalf("projection intent history fabricated a stop: %#v", history.Intents)
	}
	if _, err := h.store.CompleteAttempt(context.Background(), "fabric-computer-node", oldJobID,
		claim.Lease.AttemptID, CompletionRequest{
			FencingToken: claim.Lease.FencingToken, IdempotencyKey: "project-quiesced",
			Result:                    ProcessResult{OutputError: "logs finalized after positive reap"},
			RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt,
		}); err != nil {
		t.Fatal(err)
	}
	projected, err := h.store.InstallComputerProjection(context.Background(), computer.ComputerID, ComputerProjectionRequest{
		ComputerMutationPrecondition: computerPrecondition(projecting, "operator"), Spec: nextSpec,
	})
	if err != nil {
		t.Fatal(err)
	}
	if projected.ReconfigurationPhase != ComputerReconfigurationStable || projected.CurrentJobID == oldJobID ||
		projected.CurrentJob.State != contract.JobQueued || projected.CurrentJob.DesiredState != contract.ServiceDesiredRunning ||
		projected.AppliedRevision != projected.IntentRevision {
		t.Fatalf("completed projection = %#v", projected)
	}
}

func TestComputerRestartRecoversFailedAndStoppedObservations(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {
			Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1,
		},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	spec := computerCapabilityJobSpec("computer:restart")
	memoryBytes := int64(1 << 30)
	spec.Execution.OCI.Limits.MemoryBytes = &memoryBytes
	computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "restartable", Spec: spec, Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	firstClaim, err := h.store.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || firstClaim == nil {
		t.Fatalf("first claim = %#v err=%v", firstClaim, err)
	}
	if _, err := h.store.ObserveAttemptImage(context.Background(), "fabric-computer-node", computer.CurrentJobID,
		firstClaim.Lease.AttemptID, testImageObservation(firstClaim.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.StartAttempt(context.Background(), "fabric-computer-node", computer.CurrentJobID,
		firstClaim.Lease.AttemptID, StartedRequest{FencingToken: firstClaim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	exitCode := 137
	if _, err := h.store.CompleteAttempt(context.Background(), "fabric-computer-node", computer.CurrentJobID,
		firstClaim.Lease.AttemptID, CompletionRequest{
			FencingToken: firstClaim.Lease.FencingToken, IdempotencyKey: "runtime-oom",
			Result: ProcessResult{ExitCode: &exitCode, OOM: true},
		}); err != nil {
		t.Fatal(err)
	}
	failed, err := h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil || failed.CurrentJob.State != contract.JobFailed || failed.CurrentJob.NextRestartAt != nil ||
		failed.CurrentJob.RestartStreak != 0 || failed.CurrentJob.LifetimeRestartCount != 0 || failed.CurrentJob.HoldsSlot(failed.CurrentJob.State) ||
		failed.BoundNodeID != node.NodeID || failed.StorageID != computer.StorageID || failed.CurrentJob.Spec.Execution.OCI.Image.Digest == nil {
		t.Fatalf("failed Computer = %#v err=%v", failed, err)
	}
	var resourceFailure contract.SpawnFailure
	if err := json.Unmarshal(failed.CurrentJob.LastFailure, &resourceFailure); err != nil ||
		resourceFailure.Code != contract.SpawnFailureInsufficientMemory || resourceFailure.NodeID != node.NodeID ||
		resourceFailure.RequestedBytes != 1<<30 || resourceFailure.ObservedAvailableBytes != 0 {
		t.Fatalf("failed Computer resource evidence = %+v err=%v", resourceFailure, err)
	}
	if _, err := h.store.SetComputerDesiredState(context.Background(), failed.ComputerID,
		computerDesiredRequest(failed, contract.ServiceDesiredRunning, "operator")); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("latched start error = %v, want conflict", err)
	}
	restartRequest := ComputerRestartRequest{
		ComputerMutationPrecondition: computerPrecondition(failed, "operator"), IdempotencyKey: "failed-restart",
	}
	restarted, replayed, err := h.store.RestartComputer(context.Background(), failed.ComputerID, restartRequest)
	if err != nil || replayed {
		t.Fatalf("restart = %#v replayed=%t err=%v", restarted, replayed, err)
	}
	if restarted.CurrentJob.State != contract.JobQueued || restarted.IntentRevision != failed.IntentRevision+1 ||
		restarted.AppliedRevision != restarted.IntentRevision || restarted.BoundNodeID != node.NodeID ||
		restarted.StorageID != failed.StorageID {
		t.Fatalf("restarted Computer = %#v", restarted)
	}
	replayedComputer, replayed, err := h.store.RestartComputer(context.Background(), failed.ComputerID, restartRequest)
	if err != nil || !replayed || replayedComputer.IntentRevision != restarted.IntentRevision {
		t.Fatalf("restart replay = %#v replayed=%t err=%v", replayedComputer, replayed, err)
	}
	secondClaim, err := h.store.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || secondClaim == nil || secondClaim.Lease.AttemptID == firstClaim.Lease.AttemptID {
		t.Fatalf("fresh attempt after restart = %#v err=%v", secondClaim, err)
	}
	stoppedCandidate, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "restart-from-stopped", Spec: computerCapabilityJobSpec("computer:restart-stopped"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := h.store.SetComputerDesiredState(context.Background(), stoppedCandidate.ComputerID,
		computerDesiredRequest(stoppedCandidate, contract.ServiceDesiredStopped, "operator"))
	if err != nil || stopped.CurrentJob.State != contract.JobStopped {
		t.Fatalf("stop before restart = %#v err=%v", stopped, err)
	}
	fromStopped, _, err := h.store.RestartComputer(context.Background(), stopped.ComputerID, ComputerRestartRequest{
		ComputerMutationPrecondition: computerPrecondition(stopped, "operator"), IdempotencyKey: "stopped-restart",
	})
	if err != nil || fromStopped.CurrentJob.State != contract.JobQueued {
		t.Fatalf("restart from stopped = %#v err=%v", fromStopped, err)
	}
}

func TestComputerPinnedPlacementRejectsWrongTaggedNode(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{})
	placementTag := contract.StableNodeTagPrefix + "computer-node"
	wrong := registerCapabilityNodeWithTags(t, h, "wrong-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{placementTag})
	computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "pinned", Spec: computerCapabilityJobSpec("computer:pinned"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(context.Background(), "fabric-wrong-node", wrong.NodeID, wrong.BootSessionID, contract.JobClassService)
	if err != nil {
		t.Fatal(err)
	}
	if claim != nil {
		t.Fatalf("wrong node with matching tags claimed pinned Computer: %#v", claim)
	}
	correct := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{placementTag})
	if _, err := h.store.db.Exec(`UPDATE computers SET bound_node_id=? WHERE computer_id=?`, wrong.NodeID, computer.ComputerID); err != nil {
		t.Fatal(err)
	}
	claim, err = h.store.ClaimJob(context.Background(), "fabric-computer-node", correct.NodeID, correct.BootSessionID, contract.JobClassService)
	if errorCode(err) != contract.ErrorInternal || claim != nil {
		t.Fatalf("divergent Computer binding claim = %#v err=%v, want loud internal failure", claim, err)
	}
	if _, err := h.store.db.Exec(`UPDATE computers SET bound_node_id=NULL WHERE computer_id=?`, computer.ComputerID); err != nil {
		t.Fatal(err)
	}
	claim, err = h.store.ClaimJob(context.Background(), "fabric-computer-node", correct.NodeID, correct.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != computer.CurrentJobID {
		t.Fatalf("pinned node claim = %#v err=%v", claim, err)
	}
}

func TestComputerRemovalDirectiveCompletionReleasesSlot(t *testing.T) {
	assertComputerRemovalDirectiveCompletionReleasesSlot(t)
}

func assertComputerRemovalDirectiveCompletionReleasesSlot(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {
			Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1,
		},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "removable", Spec: computerCapabilityJobSpec("computer:remove-slot"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim = %#v err=%v", claim, err)
	}
	computer, err = h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := h.store.RemoveComputer(context.Background(), computer.ComputerID, ComputerRemoveRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
	})
	if err != nil {
		t.Fatal(err)
	}
	var removalBinding string
	if err := h.store.db.QueryRow(`SELECT bound_node_id FROM service_jobs WHERE job_id=?`, computer.CurrentJobID).Scan(&removalBinding); err != nil {
		t.Fatal(err)
	}
	if removed.CurrentJob.State != contract.JobRemovalPending ||
		!(ServiceJob{BoundNodeID: removalBinding}).HoldsSlot(removed.CurrentJob.State) {
		t.Fatalf("pending Computer removal must retain Slot until cleanup: %#v binding=%q", removed.CurrentJob, removalBinding)
	}
	directives, err := h.store.ListNodeRemovalDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0].JobID != computer.CurrentJobID || directives[0].ComputerStorage == nil ||
		directives[0].ComputerStorage.ComputerID != computer.ComputerID || directives[0].ComputerStorage.StorageID != computer.StorageID ||
		directives[0].ComputerStorage.StorageGeneration != computer.StorageGeneration {
		t.Fatalf("Computer removal directives = %#v err=%v", directives, err)
	}
	directive := directives[0]
	acknowledgement := RemovalAcknowledgementRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		RemovalGeneration: directive.RemovalGeneration, CleanupFence: directive.CleanupFence,
		RootInstanceID: directive.RootInstanceID, IdempotencyKey: "computer-cleaned",
	}
	if _, err := h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-computer-node",
		computer.CurrentJobID, acknowledgement); err != nil {
		t.Fatal(err)
	}
	finalized, changed, err := h.store.FinalizeServiceRemoval(context.Background(), computer.CurrentJobID)
	if err != nil || !changed || finalized.State != contract.JobRemovedVerified ||
		(ServiceJob{BoundNodeID: removalBinding}).HoldsSlot(finalized.State) {
		t.Fatalf("finalized Computer removal = %#v changed=%t err=%v", finalized, changed, err)
	}
	if replayed, err := h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-computer-node",
		computer.CurrentJobID, acknowledgement); err != nil || replayed.State != contract.JobRemovedVerified {
		t.Fatalf("finalized Computer acknowledgement replay = %#v err=%v", replayed, err)
	}
	currentNode, err := getNode(context.Background(), h.store.db, node.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if currentNode.ServiceOccupancy != 0 {
		t.Fatalf("Computer removal left node occupancy %d/1", currentNode.ServiceOccupancy)
	}
	replacementSpec := capabilityJobSpec("replacement-service", contract.JobKindOCI, contract.JobClassService, "", nil)
	replacementSpec.RoutingTags = []string{contract.StableNodeTagPrefix + "computer-node"}
	replacement, _, err := h.store.CreateJob(context.Background(), replacementSpec)
	if err != nil {
		t.Fatal(err)
	}
	replacementClaim, err := h.store.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || replacementClaim == nil || replacementClaim.Job.JobID != replacement.JobID {
		t.Fatalf("replacement claim after Computer cleanup = %#v err=%v", replacementClaim, err)
	}
}

func TestPlainServiceLifecycleRemainsIndependentOfComputerIntent(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{})
	plain, _, err := h.store.CreateJob(context.Background(), capabilityJobSpec(
		"plain-service", contract.JobKindProcess, contract.JobClassService, "", nil,
	))
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := h.store.SetServiceDesiredState(context.Background(), plain.JobID, contract.ServiceDesiredStopped)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.State != contract.JobStopped || stopped.DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("plain service stop = %#v", stopped)
	}
	if _, _, err := h.store.CreateJob(context.Background(), computerCapabilityJobSpec("bare-computer")); errorCode(err) != contract.ErrorComputerResourceRequired {
		t.Fatalf("bare Computer Job error = %v, want %q", err, contract.ErrorComputerResourceRequired)
	}
}

func computerPrecondition(computer Computer, actor string) ComputerMutationPrecondition {
	return ComputerMutationPrecondition{
		IntentRevision: computer.IntentRevision, StorageID: computer.StorageID,
		StorageGeneration: computer.StorageGeneration, Actor: actor,
	}
}

func computerDesiredRequest(computer Computer, desired contract.ServiceDesiredState, actor string) ComputerDesiredStateRequest {
	return ComputerDesiredStateRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, actor),
		DesiredState:                 desired,
	}
}
