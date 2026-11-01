package l1

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
)

func TestSeatRequestUnpolledAlwaysEnds(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(fmt.Sprint(configured), func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			caps := map[string]bool{"kind:oci": true, "computer": true, "cgroup_v2": true, "computer:seat-requests": configured}
			_, err := f.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-computer-node"}, contract.NodeRegistration{NodeID: f.node.NodeID, BootSessionID: "boot", RootInstanceID: "root", OS: "darwin", Architecture: "arm64", AgentVersion: "test", Capabilities: caps, CapabilityRevision: 2, CapabilityObservedAt: f.clock.Now(), MissingCapabilities: []string{}}, NodePolicy{Tags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, MaxServiceSlots: 4}, true)
			if err != nil {
				t.Fatal(err)
			}
			input := SeatCallInput{SeatID: fmt.Sprintf("seat_%032x", 901), NodeID: f.node.NodeID, Folder: "/workspace", Form: "any", Network: "none"}
			value, err := f.store.CreateSeatCall(t.Context(), "cli", input)
			if err != nil {
				t.Fatal(err)
			}
			if !configured {
				if value.State != SeatRefused || value.Reason != "Computer room is not configured on this node" {
					t.Fatalf("no room must refuse immediately: %+v", value)
				}
			} else {
				if value.State != SeatRequested {
					t.Fatalf("initial: %+v", value)
				}
				f.clock.Advance(MaxSeatApprovalWait)
				value, err = f.store.GetSeatCall(t.Context(), "cli", input.SeatID)
				if err != nil || value.State != SeatRefused || value.Reason != "the node did not pick up the request" {
					t.Fatalf("unpolled request: %+v %v", value, err)
				}
			}
		})
	}
}

type seatCallInterleavingClock struct {
	base  *fakeClock
	calls atomic.Int32
	at    int
	hook  func()
}

func (c *seatCallInterleavingClock) Now() time.Time {
	if c.calls.Add(1) == int32(c.at) {
		c.hook()
	}
	return c.base.Now()
}

func TestSeatRequestPollCannotRefuseRoomWinner(t *testing.T) {
	for _, approved := range []bool{true, false} {
		t.Run(fmt.Sprint(approved), func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			input := SeatCallInput{SeatID: fmt.Sprintf("seat_%032x", 902), NodeID: f.node.NodeID, Folder: "/workspace", Form: "any", Network: "none"}
			poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: "boot"}, Room: DefaultComputerRoomConfig(1 << 30), HostEnabled: true, AnswerWait: time.Minute}
			if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
				t.Fatal(err)
			}
			if approved {
				if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", input.SeatID, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: "boot", Approved: true}); err != nil {
					t.Fatal(err)
				}
			}
			// The read snapshot pins Approved before sampling the injected clock. Advance
			// commits its reservation while that old snapshot is still being projected.
			hook := &seatCallInterleavingClock{base: f.clock, at: 2}
			hook.hook = func() {
				if !approved {
					if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", input.SeatID, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: "boot", Approved: true}); err != nil {
						t.Fatal(err)
					}
				}
				v, err := f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", input.SeatID, poll)
				if err != nil || v.State != SeatStarting {
					t.Fatalf("interleaved start: %+v %v", v, err)
				}
			}
			f.store.clock = hook
			if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
				t.Fatal(err)
			}
			if hook.calls.Load() < 2 {
				t.Fatal("interleaving did not run")
			}
			value, err := f.store.GetSeatCall(t.Context(), "cli", input.SeatID)
			if err != nil || value.State != SeatStarting {
				t.Fatalf("room winner was refused: %+v %v", value, err)
			}
		})
	}
}

func TestSeatRequestWindowRefusesBeforeDialog(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	input := SeatCallInput{SeatID: fmt.Sprintf("seat_%032x", 903), NodeID: f.node.NodeID, Folder: "/workspace", Form: "any", Network: "none", WindowHours: 8}
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: "boot"}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
	// JSON is the real node protocol; old L1 ignores the proposed setting here.
	if err := json.Unmarshal([]byte(`{"window_hours":[1]}`), &poll); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
		t.Fatal(err)
	}
	list, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll)
	if err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatCall(t.Context(), "cli", input.SeatID)
	if err != nil || value.State != SeatRefused || len(list.Requests) != 0 || value.Reason != "Seat approval window 8h is not enabled; allowed windows: 1h" {
		t.Fatalf("unsupported window: %+v list=%+v %v", value, list, err)
	}
}

func TestSeatRequestBadCleanupDoesNotBlockOtherCalls(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	f.store.computerDefaultImage = "ghcr.io/example/desktop@sha256:" + strings.Repeat("a", 64)
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: "boot"}, Room: DefaultComputerRoomConfig(6 << 30), HostEnabled: true, AnswerWait: time.Minute}
	bad := SeatCallInput{SeatID: fmt.Sprintf("seat_%032x", 904), NodeID: f.node.NodeID, Folder: "/workspace", Form: "dedicated", Network: "none"}
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", bad); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", bad.SeatID, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: "boot", Approved: true}); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", bad.SeatID, poll)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", bad.SeatID, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: "boot", Reason: "start failed", Refused: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.db.Exec(`CREATE TRIGGER cleanup_failure BEFORE UPDATE ON computers WHEN OLD.computer_id='` + value.ComputerID + `' BEGIN SELECT RAISE(ABORT,'injected cleanup failure'); END`); err != nil {
		t.Fatal(err)
	}
	good := bad
	good.SeatID = fmt.Sprintf("seat_%032x", 905)
	good.Form = "any"
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", good); err != nil {
		t.Fatal(err)
	}
	var logs string
	f.store.logf = func(format string, args ...any) { logs += fmt.Sprintf(format, args...) }
	list, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll)
	if err != nil || len(list.Requests) != 1 || list.Requests[0].SeatID != good.SeatID {
		t.Fatalf("bad candidate blocked healthy request: %+v %v", list, err)
	}
	if !strings.Contains(logs, bad.SeatID) || !strings.Contains(logs, "store Computer removal intent") {
		t.Fatalf("candidate error not logged: %s", logs)
	}
}

func TestSeatRequestSlowFirstImagePullKeepsOriginalDeadline(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	// A ready Mac already has a running host VM; image delivery is a warm start.
	if _, err := f.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-computer-node"}, contract.NodeRegistration{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, RootInstanceID: "root", OS: "darwin", Architecture: "arm64", AgentVersion: "test", Capabilities: f.node.Capabilities, CapabilityRevision: 2, CapabilityObservedAt: f.clock.Now(), MissingCapabilities: []string{}}, NodePolicy{Tags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, MaxServiceSlots: 4}, true); err != nil {
		t.Fatal(err)
	}

	f.store.computerDefaultImage = "ghcr.io/example/desktop@sha256:" + strings.Repeat("a", 64)
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: "boot"}, Room: DefaultComputerRoomConfig(6 << 30), HostEnabled: true, AnswerWait: time.Minute}
	input := SeatCallInput{SeatID: fmt.Sprintf("seat_%032x", 906), NodeID: f.node.NodeID, Folder: "/workspace", Form: "dedicated", Network: "none"}
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", input.SeatID, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: "boot", Approved: true}); err != nil {
		t.Fatal(err)
	}
	first, err := f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", input.SeatID, poll)
	if err != nil {
		t.Fatal(err)
	}
	// EnsureImage pulls the pinned public image before the task and the helper's
	// independent 60-second Seat timer can start. A running host VM keeps
	// the prestart infrastructure budget plus one minute from start.
	if !first.Deadline.Equal(f.clock.Now().Add(11 * time.Minute)) {
		t.Fatalf("warm start deadline=%s, want eleven minutes from start", first.Deadline)
	}
	f.reopen(t)
	f.clock.Advance(3*time.Minute + time.Second)
	if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatCall(t.Context(), "cli", input.SeatID)
	if err != nil || value.State != SeatStarting {
		t.Fatalf("slow first pull prematurely refused: %+v %v", value, err)
	}
	replay, err := f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", input.SeatID, poll)
	if err != nil || !replay.Deadline.Equal(first.Deadline) {
		t.Fatalf("replay moved deadline: %+v %v", replay, err)
	}
	f.clock.Advance(10 * time.Minute)
	if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
		t.Fatal(err)
	}
	value, err = f.store.GetSeatCall(t.Context(), "cli", input.SeatID)
	if err != nil || value.State != SeatRefused {
		t.Fatalf("request never timed out: %+v %v", value, err)
	}
}

func TestSeatCallFinalCredentialErasedAndRetentionFollowsSeat(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	input := SeatCallInput{SeatID: fmt.Sprintf("seat_%032x", 907), NodeID: f.node.NodeID, Folder: "/workspace", Form: "any", Network: "none"}
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: "boot"}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", input.SeatID, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: "boot", Reason: "denied by the owner"}); err != nil {
		t.Fatal(err)
	}
	var secret string
	if err := f.store.db.QueryRow(`SELECT credential FROM seat_request_calls WHERE seat_id=?`, input.SeatID).Scan(&secret); err != nil {
		t.Fatal(err)
	}
	if secret != "" {
		t.Fatal("final refusal retained a credential in L1")
	}
	if _, err := f.store.db.Exec(`DELETE FROM seat_approvals WHERE seat_id=?`, input.SeatID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.GetSeatCall(t.Context(), "cli", input.SeatID); errorCode(err) != contract.ErrorNotFound {
		t.Fatalf("call outlived pruned final Seat: %v", err)
	}
}

func TestSeatCallNodeForeignKeyMigrates(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	// Recreate the prior committed table, then upgrade it with a pending receipt.
	if _, err := f.store.db.Exec(`DROP TABLE seat_request_calls;` + strings.ReplaceAll(seatCallSchema, " REFERENCES nodes(node_id) ON DELETE CASCADE", " REFERENCES nodes(node_id)")); err != nil {
		t.Fatal(err)
	}
	input := SeatCallInput{SeatID: fmt.Sprintf("seat_%032x", 908), NodeID: f.node.NodeID, Folder: "/workspace", Form: "any", Network: "none"}
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	rows, err := f.store.db.Query(`PRAGMA foreign_key_list(seat_request_calls)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var id, seq int
		var table, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &table, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			t.Fatal(err)
		}
		if table == "nodes" {
			found = true
			if onDelete != "CASCADE" {
				t.Fatalf("node foreign key delete rule=%s", onDelete)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("node foreign key missing")
	}
	value, err := f.store.GetSeatCall(t.Context(), "cli", input.SeatID)
	if err != nil || value.State != SeatRequested {
		t.Fatalf("migration lost pending call: %+v %v", value, err)
	}
}

func TestSeatCallSelectionDoesNotBlockAnotherNode(t *testing.T) {
	blocked := newSeatLifecycleFixture(t)
	_, err := blocked.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-other-node"}, contract.NodeRegistration{NodeID: "other-node", BootSessionID: "boot", RootInstanceID: "root", OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{}, MissingCapabilities: []string{}}, DefaultNodePolicy(), true)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	hook := &seatCallInterleavingClock{base: blocked.clock, at: 1, hook: func() { close(entered); <-release }}
	blocked.store.clock = hook
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: blocked.node.NodeID, BootSessionID: "boot"}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
	done := make(chan struct{})
	go func() {
		defer close(done)
		blocked.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", fmt.Sprintf("seat_%032x", 909), poll)
	}()
	<-entered
	defer func() { close(release); <-done }()
	progressed := make(chan error, 1)
	go func() {
		otherPoll := poll
		otherPoll.NodeID = "other-node"
		_, err := blocked.store.AdvanceSeatCall(t.Context(), "fabric-other-node", fmt.Sprintf("seat_%032x", 910), otherPoll)
		progressed <- err
	}()
	select {
	case err := <-progressed:
		if errorCode(err) != contract.ErrorNotFound {
			t.Fatalf("other node: %v", err)
		}
	case <-time.After(testwait.Bound):
		t.Fatal("one node selection blocked an independent node")
	}
}

func TestSeatCallCannotReuseRetainedSeatID(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := fmt.Sprintf("seat_%032x", 911)
	if _, err := f.store.SetComputerWantedSeats(t.Context(), f.computer.ComputerID, 0, []WantedComputerSeat{{SeatID: id, Place: 1}}); err != nil {
		t.Fatal(err)
	}
	input := SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Folder: "/workspace", Form: "any", Network: "none"}
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); errorCode(err) != contract.ErrorDispatchKeyConflict {
		t.Fatalf("reused a permanent Seat identity: %v", err)
	}
}
