package l1

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func registerSeatLogBoot(t *testing.T, f *seatLifecycleFixture, boot string) error {
	t.Helper()
	_, err := f.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-computer-node"}, contract.NodeRegistration{NodeID: f.node.NodeID, BootSessionID: boot, RootInstanceID: "root", OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{CapabilitySeatRequests: true, "kind:oci": true, "computer": true, "cgroup_v2": true}, CapabilityRevision: 2, CapabilityObservedAt: f.clock.Now(), MissingCapabilities: []string{}}, NodePolicy{Tags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, MaxServiceSlots: 4}, true)
	return err
}

func TestSeatLogMissingHistoryCannotBlockNodeRegistration(t *testing.T) {
	for _, withCall := range []bool{false, true} {
		t.Run(map[bool]string{false: "approval-only", true: "caller-known"}[withCall], func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			id := "seat_00000000000000000000000000000001"
			if withCall {
				if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
					t.Fatal(err)
				}
			}
			f.request(t, 1, true)
			if _, err := f.store.db.ExecContext(t.Context(), `DELETE FROM seat_logs WHERE seat_id=?`, id); err != nil {
				t.Fatal(err)
			}
			if err := registerSeatLogBoot(t, f, "next-boot"); err != nil {
				t.Fatalf("missing history blocked node registration: %v", err)
			}
			result, err := f.store.GetSeatRequest(t.Context(), id)
			if err != nil || result.State != SeatRefused {
				t.Fatalf("lost approval not refused: %+v %v", result, err)
			}
			if withCall {
				if page, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100); err != nil || len(page.Events) == 0 {
					t.Fatalf("recovered log lost caller: %+v %v", page, err)
				}
			}
		})
	}
}

func TestSeatLogActiveNeverLiveSurvivesRetentionSweep(t *testing.T) {
	for _, action := range []string{"wait", "start", "new-boot"} {
		t.Run(action, func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			id := "seat_00000000000000000000000000000001"
			if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
				t.Fatal(err)
			}
			if action != "wait" {
				// Keep the approved, never-live Seat active across the retention
				// sweep; this fixture tests log retention, not the start timeout.
				reopenSeatLife(t, f, StoreOptions{SeatStartWait: 92 * 24 * time.Hour})
				f.request(t, 1, true)
				f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
			}
			f.clock.Advance(91 * 24 * time.Hour)
			if action == "start" {
				if err := registerSeatLogBoot(t, f, f.node.BootSessionID); err != nil {
					t.Fatal(err)
				}
			}
			swept, err := f.store.Reconcile(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if swept.PrunedSeatLogs != 0 {
				t.Errorf("active request log pruned: %+v", swept)
			}
			if _, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100); err != nil {
				t.Errorf("active history unavailable: %v", err)
			}
			switch action {
			case "wait":
				result, err := f.store.GetSeatCall(t.Context(), "cli", id)
				if err != nil || result.State != SeatRefused {
					t.Fatalf("old pending wait cannot refuse: %+v %v", result, err)
				}
			case "start":
				result, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute})
				if err != nil || result.State != SeatStarting {
					t.Fatalf("old approved Seat cannot start: %+v %v", result, err)
				}
			case "new-boot":
				if err := registerSeatLogBoot(t, f, "next-boot"); err != nil {
					t.Fatalf("old approval blocked node: %v", err)
				}
			}
		})
	}
}

func TestSeatLogMissingApprovalHistoryCanStartOrAnswer(t *testing.T) {
	for _, action := range []string{"start", "answer"} {
		t.Run(action, func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			id := "seat_00000000000000000000000000000001"
			if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
				t.Fatal(err)
			}
			f.request(t, 1, action == "start")
			if action == "start" {
				f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
			}
			if _, err := f.store.db.ExecContext(t.Context(), `DELETE FROM seat_logs WHERE seat_id=?`, id); err != nil {
				t.Fatal(err)
			}
			if action == "start" {
				if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
					t.Fatalf("missing history blocked start: %v", err)
				}
			} else {
				if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100); err != nil {
				t.Fatalf("recovered log lost original caller: %v", err)
			}
		})
	}
}

func TestSeatLogMigrationIncludesApprovalAndIdentityHistory(t *testing.T) {
	for _, source := range []string{"approval", "identity"} {
		t.Run(source, func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			id := f.request(t, 1, true)
			if source == "identity" {
				if _, err := f.store.db.ExecContext(t.Context(), `INSERT INTO computer_seat_identities(seat_id,computer_id,place,introduced_revision,ever_live) VALUES(?,?,1,1,1)`, id, f.computer.ComputerID); err != nil {
					t.Fatal(err)
				}
				if _, err := f.store.db.ExecContext(t.Context(), `DELETE FROM seat_approvals WHERE seat_id=?`, id); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.store.db.ExecContext(t.Context(), `DELETE FROM seat_logs WHERE seat_id=?`, id); err != nil {
				t.Fatal(err)
			}
			// Keep the old seed markers, but model upgrading from #1145 before the
			// authority-source repair migration existed.
			if _, err := f.store.db.ExecContext(t.Context(), `DELETE FROM l1_data_migrations WHERE name='seat_log_authority_seed'`); err != nil {
				t.Fatal(err)
			}
			f.reopen(t)
			if err := f.store.AppendSeatEvent(t.Context(), SeatLogEvent{SeatID: id, EventID: "control-after-upgrade", Kind: SeatEventControlTaken}); err != nil {
				t.Fatalf("upgrade omitted %s history: %v", source, err)
			}
			if source == "identity" {
				f.clock.Advance(91 * 24 * time.Hour)
				result, err := f.store.Reconcile(t.Context())
				if err != nil || result.PrunedSeatLogs != 0 {
					t.Fatalf("once-live identity lost history: %+v %v", result, err)
				}
			}
		})
	}
}

func TestSeatLogOutcomeCannotConsumeEarlierOwnerNote(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := "seat_00000000000000000000000000000001"
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	f.request(t, 1, true)
	if err := f.store.AppendSeatEvent(t.Context(), SeatLogEvent{SeatID: id, EventID: "owner-note", Kind: SeatEventControlTaken, Note: "Please inspect the current page"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Refused: true, Reason: "denied by the owner"}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result, err := f.store.GetSeatCall(t.Context(), "cli", id)
		if err != nil || result.UnreadEvents != 2 {
			t.Fatalf("outcome consumed earlier owner note: %+v %v", result, err)
		}
	}
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.MarkSeatLogRead(t.Context(), "cli", id, "", page.NextCursor); err != nil {
		t.Fatal(err)
	}
	result, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || result.UnreadEvents != 0 {
		t.Fatalf("explicit read did not consume note: %+v %v", result, err)
	}
}
