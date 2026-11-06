package l1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func computerKeyedReplayHarness(t *testing.T) (*integrationHarness, Node, Computer) {
	t.Helper()
	h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	}, true, time.Hour)
	node := registerCapabilityNodeWithTags(t, h, "computer-node", computerNodeCapabilities(),
		[]string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "keyed-replay", Spec: computerCapabilityJobSpec("computer:keyed-replay"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	return h, node, computer
}

func TestComputerReimageKeyedReplayAfterPreflightFailure(t *testing.T) {
	h, node, computer := computerKeyedReplayHarness(t)
	claim := startComputerAttempt(t, h, node, nil)
	computer = mustGetComputer(t, h, computer.ComputerID)
	request := ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
		Image: reimageTarget('7'), TerminateSessions: true, IdempotencyKey: "failed-reimage"}
	accepted, replayed, err := h.store.ReimageComputerWithReplay(t.Context(), computer.ComputerID, request)
	if err != nil || replayed || accepted.CurrentJob.State != contract.JobStopping || accepted.owedRevocationID == 0 {
		t.Fatalf("accept reimage: replayed=%t err=%v computer=%#v", replayed, err, accepted)
	}
	quiescing, replayed, err := h.store.ReimageComputerWithReplay(t.Context(), computer.ComputerID, request)
	if err != nil || !replayed || quiescing.owedRevocationID != 0 || quiescing.IntentRevision != accepted.IntentRevision ||
		quiescing.CurrentJobID != computer.CurrentJobID || quiescing.CurrentJob.State != contract.JobStopping ||
		quiescing.ReconfigurationPhase != ComputerReconfigurationReimaging {
		t.Fatalf("quiescing retry: replayed=%t err=%v computer=%#v", replayed, err, quiescing)
	}
	if _, err := h.store.CompleteAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID,
		CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "quiesced",
			Result: ProcessResult{OutputError: "quiesced"}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}); err != nil {
		t.Fatal(err)
	}
	_, acknowledgement := reimagePreflightAcknowledgement(t, h, node, computer.ComputerID, claim.Lease.AttemptID, claim.Lease.FencingToken)
	acknowledgement.Receipt.Kind = computerReimagePreflightFailedReceiptKind
	acknowledgement.Receipt.FailureCode = string(contract.SpawnFailureImageUnavailable)
	acknowledgement.Receipt.FailureStage = "image_identity"
	acknowledgement.Receipt.FailureReason = "image_unavailable"
	failed, err := h.store.AcknowledgeComputerReimagePreflight(t.Context(), "fabric-computer-node", computer.ComputerID, acknowledgement)
	if err != nil || failed.ReconfigurationPhase != ComputerReconfigurationStable ||
		failed.IntentRevision != accepted.IntentRevision || failed.CurrentJobID != computer.CurrentJobID ||
		*failed.CurrentJob.Spec.Execution.OCI.Image.Digest != *computer.CurrentJob.Spec.Execution.OCI.Image.Digest {
		t.Fatalf("failed preflight did not retain prior image: err=%v computer=%#v", err, failed)
	}
	conflicting := request
	conflicting.IdempotencyKey = "different-key-stale-revision"
	if _, _, err := h.store.ReimageComputerWithReplay(t.Context(), computer.ComputerID, conflicting); errorCode(err) != contract.ErrorStaleIntentRevision {
		t.Fatalf("different key with stale revision: %v", err)
	}
	result, replayed, err := h.store.ReimageComputerWithReplay(t.Context(), computer.ComputerID, request)
	if err != nil || !replayed || result.IntentRevision != accepted.IntentRevision || result.CurrentJobID != failed.CurrentJobID {
		t.Fatalf("accepted reimage replay refused after preflight failure: replayed=%t revision=%d want=%d err=%v",
			replayed, result.IntentRevision, accepted.IntentRevision, err)
	}
}

func TestComputerGrowKeyedReplayProjectsSelectedOutcome(t *testing.T) {
	h, node, computer := computerKeyedReplayHarness(t)
	startComputerAttempt(t, h, node, nil)
	computer = mustGetComputer(t, h, computer.ComputerID)
	first := ComputerGrowRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
		DiskBytes: computer.DesiredDiskBytes + (1 << 30), IdempotencyKey: "grow-a"}
	var failed Computer
	for index := range 2 {
		request := first
		if index == 1 {
			request.ComputerMutationPrecondition = computerPrecondition(computer, "operator")
			request.IdempotencyKey = "grow-b"
			request.DiskBytes += 1 << 30
		}
		if _, replayed, err := h.store.BeginComputerGrow(t.Context(), computer.ComputerID, request); err != nil || replayed {
			t.Fatalf("grow %d: replayed=%t err=%v", index, replayed, err)
		}
		directives, err := h.store.ListNodeComputerStorageGrowDirectives(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID)
		if err != nil || len(directives) != 1 {
			t.Fatalf("grow directives: %#v err=%v", directives, err)
		}
		receipt := growReceiptFor(directives[0])
		if index == 0 {
			receipt.Kind, receipt.Applied = computerStorageGrowFailedKind, false
			receipt.FailureCode = "insufficient_disk"
			receipt.ObservedAvailableBytes = 123
		}
		computer, err = h.store.AcknowledgeComputerStorageGrow(t.Context(), "fabric-computer-node", computer.ComputerID,
			ComputerStorageGrowAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
				IdempotencyKey: request.IdempotencyKey + "-ack", Receipt: receipt})
		if err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			failed = computer
		}
	}
	if computer.LastGrowOperation.Status != "applied" || computer.LastGrowOperation.OperationRevision == failed.LastGrowOperation.OperationRevision {
		t.Fatalf("second grow did not supersede failed grow: %#v", computer.LastGrowOperation)
	}
	client := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	status, headers, body := h.do(client, http.MethodPost, "/v1/computers/"+computer.ComputerID+"/grow", first)
	var result Computer
	if status != http.StatusOK || headers.Get("Idempotent-Replay") != "true" || json.Unmarshal(body, &result) != nil {
		t.Fatalf("grow replay: status=%d headers=%v body=%s", status, headers, body)
	}
	if !reflect.DeepEqual(result.LastGrowOperation, failed.LastGrowOperation) {
		t.Fatalf("grow A replay projected another outcome: got=%#v want=%#v", result.LastGrowOperation, failed.LastGrowOperation)
	}
}

func TestComputerReimageSameDigestDoesNotRevoke(t *testing.T) {
	for _, reference := range []string{"current-reference", "new-tag"} {
		for _, revision := range []string{"current-revision", "stale-revision"} {
			t.Run(reference+"/"+revision, func(t *testing.T) {
				h, node, computer := computerKeyedReplayHarness(t)
				startComputerAttempt(t, h, node, nil)
				computer = mustGetComputer(t, h, computer.ComputerID)
				var revocations []ComputerTokenRevocation
				h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(_ context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
					revocations = append(revocations, request)
					return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID,
						ComputerAttemptID: request.ComputerAttemptID, CommittedAt: h.clock.Now()}, nil
				}}
				request := ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
					Image: computer.CurrentJob.Spec.Execution.OCI.Image, IdempotencyKey: "noop-reimage"}
				if reference == "new-tag" {
					request.Image.Reference = "example.invalid/computer:new-tag"
				}
				if revision == "stale-revision" {
					request.IntentRevision--
				}
				before := owedRevocationAuditRows(t, h, computer.ComputerID)
				client := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
				status, headers, body := h.do(client, http.MethodPost, "/v1/computers/"+computer.ComputerID+"/reimage", request)
				var result Computer
				if status != http.StatusAccepted || json.Unmarshal(body, &result) != nil {
					t.Fatalf("same-digest reimage: status=%d body=%s", status, body)
				}
				if headers.Get("Mutation-Applied") != "false" || headers.Get("Idempotent-Replay") != "" {
					t.Errorf("same-digest no-op receipt: %v", headers)
				}
				if len(revocations) != 0 {
					t.Errorf("same-digest no-op requested revocation: %#v", revocations)
				}
				if after := owedRevocationAuditRows(t, h, computer.ComputerID); !reflect.DeepEqual(after, before) {
					t.Errorf("same-digest no-op changed revocation records: before=%#v after=%#v", before, after)
				}
				if result.IntentRevision != computer.IntentRevision || result.CurrentJobID != computer.CurrentJobID ||
					result.CurrentJob.Spec.Execution.OCI.Image.Reference != computer.CurrentJob.Spec.Execution.OCI.Image.Reference {
					t.Errorf("same-digest no-op changed Computer: %#v", result)
				}
			})
		}
	}
}

func TestComputerReimageKeyedReplayDoesNotRevokeAgain(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		name := "original-settled"
		if unavailable {
			name = "original-owed"
		}
		t.Run(name, func(t *testing.T) {
			h, node, computer := computerKeyedReplayHarness(t)
			claim := startComputerAttempt(t, h, node, nil)
			computer = mustGetComputer(t, h, computer.ComputerID)
			var revocations []ComputerTokenRevocation
			h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(_ context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
				revocations = append(revocations, request)
				if unavailable {
					return contract.ComputerTokenRevocationReceipt{}, errors.New("test ledger unavailable")
				}
				return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID,
					ComputerAttemptID: request.ComputerAttemptID, CommittedAt: h.clock.Now()}, nil
			}}
			client := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
			request := ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
				Image: reimageTarget('8'), TerminateSessions: true, IdempotencyKey: "reimage-once"}
			path := "/v1/computers/" + computer.ComputerID + "/reimage"
			status, headers, body := h.do(client, http.MethodPost, path, request)
			if unavailable {
				assertRunLedgerUnavailable(t, status, body, false, "the Computer mutation applied", "is owed")
			} else if status != http.StatusAccepted || headers.Get("Mutation-Applied") != "true" {
				t.Fatalf("original reimage: status=%d headers=%v body=%s", status, headers, body)
			}
			if len(revocations) != 1 || !revocations[0].RevokeAll || revocations[0].Reason != "computer_reimaged" {
				t.Fatalf("original reimage did not revoke: %#v", revocations)
			}
			if _, err := h.store.CompleteAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID,
				CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "quiesced",
					Result: ProcessResult{OutputError: "quiesced"}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}); err != nil {
				t.Fatal(err)
			}
			acknowledgeReimagePreflight(t, h, node, computer.ComputerID, claim.Lease.AttemptID, claim.Lease.FencingToken)
			before := owedRevocationAuditRows(t, h, computer.ComputerID)
			calls := len(revocations)
			status, headers, body = h.do(client, http.MethodPost, path, request)
			if status != http.StatusOK || headers.Get("Idempotent-Replay") != "true" || headers.Get("Mutation-Applied") != "false" {
				t.Fatalf("completed reimage replay: status=%d headers=%v body=%s", status, headers, body)
			}
			if len(revocations) != calls {
				t.Fatalf("completed replay requested a new revocation: before=%d after=%d", calls, len(revocations))
			}
			if after := owedRevocationAuditRows(t, h, computer.ComputerID); !reflect.DeepEqual(after, before) {
				t.Fatalf("completed replay changed revocation records: before=%#v after=%#v", before, after)
			}
			if unavailable {
				unavailable = false
				heartbeatComputerNode(t, h, node)
				for _, row := range owedRevocationAuditRows(t, h, computer.ComputerID) {
					if row.verb == ComputerRevocationVerbReimage && (row.record == nil || row.settlement == "") {
						t.Fatalf("original reimage revocation was not settled by heartbeat: %#v", row)
					}
				}
				if len(revocations) <= calls {
					t.Fatal("heartbeat did not honor the original owed revocation")
				}
			}
		})
	}
}
