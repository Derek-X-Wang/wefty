package l1

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestComputerTakeoverAuditIsFencedIdempotentAndPrivacyBounded(t *testing.T) {
	assertComputerTakeoverAuditContract(t)
}

func assertComputerTakeoverAuditContract(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 2 * time.Second}, map[string]NodePolicy{
		"computer-node": DefaultNodePolicy(contract.StableNodeTagPrefix + "computer-node"),
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "audit-computer", Spec: computerCapabilityJobSpec("computer:audit"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim Computer = %#v err=%v", claim, err)
	}
	occurred := h.clock.Now().Add(time.Millisecond)
	event := ComputerTakeoverAuditEvent{
		EventID: "session-1:open", Kind: ComputerTakeoverSessionOpen,
		ComputerID: computer.ComputerID, JobID: claim.Job.JobID, AttemptID: claim.Lease.AttemptID,
		SessionID: "session-1", FabricID: "fabric-test", UserID: "person-a", DeviceID: "device-a",
		AuthorizedRole: ComputerGrantControl, AdmittedMode: "view", PolicyRevision: 7, OccurredAt: occurred,
	}
	request := ComputerTakeoverAuditRequest{FencingToken: claim.Lease.FencingToken, Event: event}
	receipt, err := h.store.AppendComputerTakeoverAudit(context.Background(), "fabric-computer-node",
		computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, request)
	if err != nil || receipt.Replayed || receipt.Event.AuthorityGeneration != node.AuthorityGeneration {
		t.Fatalf("first audit receipt = %#v err=%v", receipt, err)
	}
	replay, err := h.store.AppendComputerTakeoverAudit(context.Background(), "fabric-computer-node",
		computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, request)
	if err != nil || !replay.Replayed || replay.Event != receipt.Event {
		t.Fatalf("audit replay = %#v err=%v", replay, err)
	}
	conflict := request
	conflict.Event.Reason = ComputerTakeoverClientClosed
	if _, err := h.store.AppendComputerTakeoverAudit(context.Background(), "fabric-computer-node",
		computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, conflict); errorCode(err) != contract.ErrorIdempotencyConflict {
		t.Fatalf("conflicting audit replay error = %v", err)
	}
	stale := request
	stale.Event.EventID = "session-2:open"
	stale.Event.SessionID = "session-2"
	stale.FencingToken += "-stale"
	if _, err := h.store.AppendComputerTakeoverAudit(context.Background(), "fabric-computer-node",
		computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, stale); errorCode(err) != contract.ErrorStaleFence {
		t.Fatalf("stale audit fence error = %v", err)
	}

	var storedColumns string
	rows, err := h.store.db.Query(`PRAGMA table_info(computer_takeover_audit)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var ordinal, notNull, primaryKey int
		var name, dataType string
		var defaultValue any
		if err := rows.Scan(&ordinal, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		storedColumns += " " + name
	}
	for _, forbidden := range []string{"fencing_token", "framebuffer", "display", "click", "key", "pointer"} {
		if strings.Contains(storedColumns, forbidden) {
			t.Fatalf("take-over audit schema contains forbidden %q: %s", forbidden, storedColumns)
		}
	}
	var count int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_takeover_audit WHERE attempt_id=?`, claim.Lease.AttemptID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("durable audit row count = %d err=%v", count, err)
	}

	closeEvent := event
	closeEvent.EventID = "session-1:close"
	closeEvent.Kind = ComputerTakeoverSessionClose
	closeEvent.Reason = "client_closed"
	agentClient := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	path := "/v1/agent/computers/" + computer.ComputerID + "/jobs/" + claim.Job.JobID +
		"/attempts/" + claim.Lease.AttemptID + "/takeover-audit"
	status, _, body := h.do(agentClient, http.MethodPost, path,
		ComputerTakeoverAuditRequest{FencingToken: claim.Lease.FencingToken, Event: closeEvent})
	if status != http.StatusOK || strings.Contains(string(body), `"fencing_token"`) || !strings.Contains(string(body), `"kind":"session_close"`) {
		t.Fatalf("take-over audit route status=%d body=%s", status, body)
	}
	h.clock.Advance(3 * time.Second)
	if _, err := h.store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_takeover_audit WHERE attempt_id=?`, claim.Lease.AttemptID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("attempt expiry removed immutable audit: count=%d err=%v", count, err)
	}
	h.clock.Advance(DefaultComputerTakeoverAuditRetentionAge + time.Second)
	result, err := h.store.Reconcile(context.Background())
	if err != nil || result.PrunedComputerTakeoverAuditEvents != 2 {
		t.Fatalf("independent audit retention result=%#v err=%v", result, err)
	}
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_takeover_audit`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("expired audit rows=%d err=%v", count, err)
	}
}
