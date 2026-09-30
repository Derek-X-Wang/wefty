package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// TestRestoreReceiptRacingARemovalNeverFailsTheHeartbeat is wefty #600's
// first race. The heartbeat revokes a restoring Computer's grants at the run
// ledger, and the operator removes that Computer before the receipt is
// written. The receipt's CAS then fails stale. That is one Computer's write,
// not the node's: the heartbeat still answers 200, withholds only that
// restore, and the next heartbeat carries the removal. Before the fix the
// whole heartbeat answered 409 stale_intent_revision, and the agent backed
// off and withdrew kind:oci over one Computer (the #548 invariant).
func TestRestoreReceiptRacingARemovalNeverFailsTheHeartbeat(t *testing.T) {
	h, node, computer, source, _ := publishedBackupForStorageCopy(t, 2)
	if _, _, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID,
		ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
			BackupID: source.BackupID, IdempotencyKey: "race"}); err != nil {
		t.Fatal(err)
	}
	logs := &recordedLog{}
	h.server.logf = logs.record
	var mu sync.Mutex
	restoreRevocations := 0
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(_ context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
		if request.RestoreOperationRevision != 0 {
			mu.Lock()
			restoreRevocations++
			first := restoreRevocations == 1
			mu.Unlock()
			if first {
				current, err := h.store.GetComputer(context.Background(), computer.ComputerID)
				if err != nil {
					t.Error(err)
				} else if _, err := h.store.RemoveComputer(context.Background(), computer.ComputerID,
					ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(current, "operator")}); err != nil {
					t.Errorf("remove the restoring Computer while its revocation is at the run ledger: %v", err)
				}
			}
		}
		return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID, ComputerAttemptID: request.ComputerAttemptID,
			RestoreOperationRevision: request.RestoreOperationRevision, SubmitIntentRevision: request.NewSubmitIntentRevision,
			CommittedAt: h.clock.Now()}, nil
	}}
	agentClient := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})

	status, _, body := h.do(agentClient, http.MethodPost, "/v1/agent/nodes/"+node.NodeID+"/heartbeat", heartbeatRequestForNode(node))
	var racing HeartbeatResponse
	if status != http.StatusOK || json.Unmarshal(body, &racing) != nil {
		t.Fatalf("heartbeat racing a removal status=%d body=%s", status, body)
	}
	if racing.NodeID != node.NodeID {
		t.Fatalf("heartbeat racing a removal answered for node %q", racing.NodeID)
	}
	if len(racing.StorageCopyDirectives) != 0 {
		t.Fatalf("heartbeat handed out a restore whose receipt was never written: %#v", racing.StorageCopyDirectives)
	}
	if !strings.Contains(logs.text(), "event=l1_restore_revocation_receipt_deferred computer_id="+computer.ComputerID) {
		t.Fatalf("the deferred restore receipt was not named in the log: %s", logs.text())
	}

	status, _, body = h.do(agentClient, http.MethodPost, "/v1/agent/nodes/"+node.NodeID+"/heartbeat", heartbeatRequestForNode(node))
	var next HeartbeatResponse
	if status != http.StatusOK || json.Unmarshal(body, &next) != nil {
		t.Fatalf("heartbeat after the removal status=%d body=%s", status, body)
	}
	delivered := false
	for _, directive := range next.RemovalDirectives {
		delivered = delivered || directive.JobID == computer.CurrentJobID
	}
	if !delivered {
		t.Fatalf("the removed Computer's removal directive was not delivered: %#v", next.RemovalDirectives)
	}
	if len(next.StorageCopyDirectives) != 0 {
		t.Fatalf("a removed Computer's restore reached the node: %#v", next.StorageCopyDirectives)
	}
	mu.Lock()
	defer mu.Unlock()
	// A superseded restore is owed nothing: the next heartbeat does not ask
	// the run ledger again.
	if restoreRevocations != 1 {
		t.Fatalf("pre-restore revocations sent = %d, want 1", restoreRevocations)
	}
	removed, err := h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil || removed.LastRestoreRevocation != nil {
		t.Fatalf("a removed restore recorded a receipt = %#v err=%v", removed.LastRestoreRevocation, err)
	}
}
