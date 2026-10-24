package l1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// SL-1: the public request is durable before a node agent opens a dialog.
func TestSeatRequestCallReplayAndCallerFence(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 4}})
	registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{"kind:oci": true, "computer": true, "cgroup_v2": true}, []string{contract.StableNodeTagPrefix + "computer-node"})
	caller := h.client(fabric.Identity{NodeID: "cli", Tags: []string{DefaultClientPrincipalTag}})
	other := h.client(fabric.Identity{NodeID: "other-cli", Tags: []string{DefaultClientPrincipalTag}})
	id := fmt.Sprintf("seat_%032x", 1)
	input := map[string]any{"seat_id": id, "node_id": "computer-node", "harness": "test", "folder": "/workspace", "form": "any", "network": "none", "window_hours": 0}
	for range 2 {
		status, _, body := h.do(caller, http.MethodPost, "/v1/seat-requests", input)
		if status != 200 {
			t.Fatalf("request: %d %s", status, body)
		}
		var result map[string]any
		if err := json.Unmarshal(body, &result); err != nil || result["seat_id"] != id || result["state"] != "requested" || result["credential"] != nil {
			t.Fatalf("pending result: %s %v", body, err)
		}
	}
	status, _, body := h.do(other, http.MethodGet, "/v1/seat-requests/"+id, nil)
	if status != 403 {
		t.Fatalf("foreign wait: %d %s", status, body)
	}
	input["network"] = "internet"
	status, _, body = h.do(caller, http.MethodPost, "/v1/seat-requests", input)
	if status != 409 {
		t.Fatalf("changed replay: %d %s", status, body)
	}
	var computers int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computers`).Scan(&computers); err != nil || computers != 0 {
		t.Fatalf("unapproved Computers=%d %v", computers, err)
	}
}

func TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	// Startup configuration supplies the pinned default, as in the installed L1.
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.store, err = OpenStore(f.path, StoreOptions{Clock: f.clock, LeaseDuration: time.Hour, ComputerDefaultImage: "ghcr.io/example/desktop@sha256:" + strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
	claims := map[string]*Claim{}
	generations := map[string]uint64{}
	sequences := map[string]int64{}
	request := func(n int, form string) SeatRequest {
		t.Helper()
		id := fmt.Sprintf("seat_%032x", n)
		input := SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: form, Network: "none"}
		if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
			t.Fatal(err)
		}
		pending, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll)
		if err != nil {
			t.Fatal(err)
		}
		value, err := f.store.GetSeatRequest(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		if value.State == SeatRefused {
			for _, candidate := range pending.Requests {
				if candidate.SeatID == id {
					t.Fatal("refused request was offered for an owner dialog")
				}
			}
			return value
		}
		if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
			t.Fatal(err)
		}
		value, err = f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", id, poll)
		if err != nil {
			t.Fatal(err)
		}
		replay, err := f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", id, poll)
		if err != nil || replay != value {
			t.Fatalf("advance replay=%+v original=%+v %v", replay, value, err)
		}
		claim := claims[value.ComputerID]
		if claim == nil {
			claim, err = f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
			if err != nil || claim == nil || claim.ComputerStorage == nil || claim.ComputerStorage.ComputerID != value.ComputerID {
				t.Fatalf("claim did not bind requested Computer: %v", err)
			}
			claims[value.ComputerID] = claim
		}
		auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
		list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", value.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
		if err != nil {
			t.Fatal(err)
		}
		seats := make([]ComputerSeatObservation, 0, len(list.Seats))
		for _, binding := range list.Seats {
			seats = append(seats, ComputerSeatObservation{SeatID: binding.SeatID, Place: binding.Place, State: ComputerSeatReady})
		}
		generations[value.ComputerID]++
		sequences[value.ComputerID]++
		if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", value.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: generations[value.ComputerID], Sequence: sequences[value.ComputerID], Seats: seats}); err != nil {
			t.Fatal(err)
		}
		value, err = f.store.GetSeatRequest(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	one, two := request(1, "any"), request(2, "any")
	if one.State != SeatLive || two.State != SeatLive || one.ComputerID != two.ComputerID || one.Place == two.Place {
		t.Fatalf("shared placement: %+v %+v", one, two)
	}
	three := request(3, "any")
	if three.State != SeatLive || three.ComputerID == one.ComputerID {
		t.Fatalf("RM-2 second shared Computer: %+v", three)
	}
	computer, err := f.store.GetComputer(t.Context(), three.ComputerID)
	if err != nil || computer.DesiredDiskBytes != 8<<30 || computer.Network != ComputerNetworkNone || computer.SeatLimit != 2 {
		t.Fatalf("new Computer defaults: %+v %v", computer, err)
	}
	fourth := request(4, "any")
	if fourth.State != SeatRefused || fourth.Reason != "out of room, try again later" {
		t.Fatalf("RM-1 fourth request: %+v", fourth)
	}
	for _, id := range []string{one.SeatID, two.SeatID, three.SeatID} {
		value, err := f.store.GetSeatRequest(t.Context(), id)
		if err != nil || value.State != SeatLive {
			t.Fatalf("RM-1 disturbed existing Seat: %+v %v", value, err)
		}
	}
	// Separate node-room settings allow a dedicated request, then refuse the
	// next before any owner dialog or Computer creation.
	poll.Room = DefaultComputerRoomConfig(6 << 30)
	dedicated := request(5, "dedicated")
	if dedicated.State != SeatLive {
		t.Fatalf("dedicated start: %+v", dedicated)
	}
	capped := request(6, "dedicated")
	if capped.State != SeatRefused || !strings.HasPrefix(capped.Reason, "dedicated Computer cap reached") {
		t.Fatalf("RM-3 cap: %+v", capped)
	}
}

func TestSeatRequestLosingRoomCreatesNoComputer(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.store, err = OpenStore(f.path, StoreOptions{Clock: f.clock, LeaseDuration: time.Hour, ComputerDefaultImage: "ghcr.io/example/desktop@sha256:" + strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID}, Room: DefaultComputerRoomConfig(1 << 30), HostEnabled: true, AnswerWait: time.Minute}
	for n := 1; n <= 2; n++ {
		id := fmt.Sprintf("seat_%032x", n)
		if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "dedicated", Network: "none"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
		t.Fatal(err)
	}
	for n := 1; n <= 2; n++ {
		id := fmt.Sprintf("seat_%032x", n)
		if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
			t.Fatal(err)
		}
	}
	for n := 1; n <= 2; n++ {
		value, err := f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", fmt.Sprintf("seat_%032x", n), poll)
		if err != nil {
			t.Fatal(err)
		}
		if n == 1 && value.State != SeatStarting || n == 2 && value.State != SeatRefused {
			t.Fatalf("last-room winner %d: %+v", n, value)
		}
	}
	list, err := f.store.ListComputers(t.Context(), "", 100)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture's unused shared Computer plus the winning dedicated one.
	if len(list.Computers) != 2 {
		t.Fatalf("losing approval created a Computer: %+v", list.Computers)
	}
}

func TestSeatRequestDeadlineIncludesDispatchDelay(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := fmt.Sprintf("seat_%032x", 40)
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	created := f.clock.Now()
	f.clock.now = f.clock.now.Add(40 * time.Second)
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
	if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
		t.Fatal(err)
	}
	approval, err := readSeatApproval(t.Context(), f.store.db, id)
	if err != nil || !approval.Deadline.Equal(created.Add(time.Minute)) {
		t.Fatalf("dispatch delayed original answer deadline: %v want %v err=%v", approval.Deadline, created.Add(time.Minute), err)
	}
}

func TestSeatRequestReadyCredentialReplayAndRefusedCleanup(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := fmt.Sprintf("seat_%032x", 50)
	input := SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", id, poll); err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim: %v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatReady}}}); err != nil {
		t.Fatal(err)
	}
	ready, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || ready.State != SeatLive || len(ready.Credential) != 64 {
		t.Fatalf("ready credential missing state=%s length=%d err=%v", ready.State, len(ready.Credential), err)
	}
	f.reopen(t)
	replay, err := f.store.CreateSeatCall(t.Context(), "cli", input)
	if err != nil || replay.Credential != ready.Credential || replay.ComputerID != ready.ComputerID {
		t.Fatalf("credential replay changed: %v", err)
	}
	if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	ended, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || ended.Credential != "" {
		t.Fatalf("ended Seat returned credential: %v", err)
	}
	// Denial consumes no room and leaves no new Computer or wanted membership.
	id = fmt.Sprintf("seat_%032x", 51)
	input.SeatID = id
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: false}); err != nil {
		t.Fatal(err)
	}
	denied, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || denied.State != SeatRefused || denied.ComputerID != "" || denied.Credential != "" {
		t.Fatalf("denied request: state=%s Computer=%s err=%v", denied.State, denied.ComputerID, err)
	}
	computers, err := f.store.ListComputers(t.Context(), "", 100)
	if err != nil || len(computers.Computers) != 1 {
		t.Fatalf("SL-2 denied created Computer count=%d err=%v", len(computers.Computers), err)
	}
}

func TestSeatRequestFailedNewComputerUsesVerifiedRemoval(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.store, err = OpenStore(f.path, StoreOptions{Clock: f.clock, LeaseDuration: time.Hour, ComputerDefaultImage: "ghcr.io/example/desktop@sha256:" + strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("seat_%032x", 60)
	input := SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "dedicated", Network: "internet"}
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", id, poll)
	if err != nil {
		t.Fatal(err)
	}
	computer, err := f.store.GetComputer(t.Context(), value.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	if computer.Network != ComputerNetworkInternet || computer.SeatLimit != 1 {
		t.Fatalf("dedicated settings: %+v", computer.ComputerSettings)
	}
	// Injected time expires a not-yet-ready new Computer. No Lima/runtime runs.
	f.clock.now = f.clock.now.Add(time.Minute)
	if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
		t.Fatal(err)
	}
	result, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || result.State != SeatRefused || result.Credential != "" {
		t.Fatalf("failed request state=%s err=%v", result.State, err)
	}
	computer, err = f.store.GetComputer(t.Context(), value.ComputerID)
	if err != nil || computer.DesiredState != contract.ServiceDesiredRemoved || computer.RemovalOutcome != "removed_verified" || result.CleanupPending {
		t.Fatalf("new Computer did not settle removal: desired=%s outcome=%s pending=%v err=%v", computer.DesiredState, computer.RemovalOutcome, result.CleanupPending, err)
	}
	room, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, poll.Room)
	if err != nil || room.AvailableSeats != 3 || room.RunningDedicated != 0 {
		t.Fatalf("failed start retained room/cap: %+v %v", room, err)
	}
	// The retained Storage-generation row is historical authority, not a
	// physical allocation. This queued Computer never had an Attempt; verified
	// removal proves no Storage cleanup or admission latch remains owed.
	if computer.CurrentJob.State != contract.JobRemovedVerified {
		t.Fatalf("failed start still has runtime authority: %s", computer.CurrentJob.State)
	}
	directives, err := f.store.ListNodeRemovalDirectives(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID)
	if err != nil || len(directives) != 0 {
		t.Fatalf("failed start still owes cleanup: %d %v", len(directives), err)
	}
}

func TestSeatRequestRuntimeFailureNewRemovalAndExistingLatch(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		t.Run(fmt.Sprintf("new=%v", fresh), func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			if fresh {
				if _, err := f.store.RemoveComputer(t.Context(), f.computer.ComputerID, ComputerRemoveRequest{ComputerMutationPrecondition: ComputerMutationPrecondition{IntentRevision: f.computer.IntentRevision, StorageID: f.computer.StorageID, StorageGeneration: f.computer.StorageGeneration, Actor: "test"}}); err != nil {
					t.Fatal(err)
				}
				if err := f.store.Close(); err != nil {
					t.Fatal(err)
				}
				var err error
				f.store, err = OpenStore(f.path, StoreOptions{Clock: f.clock, LeaseDuration: time.Hour, ComputerDefaultImage: "ghcr.io/example/desktop@sha256:" + strings.Repeat("a", 64)})
				if err != nil {
					t.Fatal(err)
				}
			}
			id := fmt.Sprintf("seat_%032x", 70)
			form := "any"
			if fresh {
				form = "dedicated"
			}
			poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
			if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: form, Network: "none"}); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
				t.Fatal(err)
			}
			value, err := f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", id, poll)
			if err != nil {
				t.Fatal(err)
			}
			claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
			if err != nil || claim == nil {
				t.Fatalf("claim: %v %v", claim, err)
			}
			if _, err := f.store.CompleteAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "start-failed", RuntimeQuiescenceEvidence: RuntimeQuiescenceComputerAttemptAbsent, Result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureInsufficientMemory, Message: "runtime admission failed", RequestedBytes: 1 << 30}}}); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
				t.Fatal(err)
			}
			result, err := f.store.GetSeatCall(t.Context(), "cli", id)
			if err != nil || result.State != SeatRefused || result.Credential != "" {
				t.Fatalf("failure not refused: %s %v", result.State, err)
			}
			computer, err := f.store.GetComputer(t.Context(), value.ComputerID)
			if err != nil {
				t.Fatal(err)
			}
			if !fresh {
				if computer.CurrentJob.State != contract.JobFailed || computer.DesiredState != contract.ServiceDesiredRunning || len(computer.CurrentJob.LastFailure) == 0 || computer.StorageID != f.computer.StorageID {
					t.Fatalf("existing latch or Storage changed: state=%s desired=%s", computer.CurrentJob.State, computer.DesiredState)
				}
				return
			}
			if !result.CleanupPending || computer.DesiredState != contract.ServiceDesiredRemoved {
				t.Fatalf("new failure returned before cleanup: pending=%v desired=%s", result.CleanupPending, computer.DesiredState)
			}
			directives, err := f.store.ListNodeRemovalDirectives(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID)
			if err != nil || len(directives) != 1 || directives[0].ComputerStorage == nil || directives[0].ComputerStorage.StorageID != computer.StorageID {
				t.Fatalf("failed new Computer omitted Storage cleanup: directives=%d err=%v", len(directives), err)
			}
			d := directives[0]
			if _, err := f.store.AcknowledgeServiceRemoval(t.Context(), "fabric-computer-node", computer.CurrentJobID, RemovalAcknowledgementRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, RemovalGeneration: d.RemovalGeneration, CleanupFence: d.CleanupFence, RootInstanceID: d.RootInstanceID, IdempotencyKey: "failed-new-cleaned"}); err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.store.FinalizeServiceRemoval(t.Context(), computer.CurrentJobID); err != nil {
				t.Fatal(err)
			}
			result, err = f.store.GetSeatCall(t.Context(), "cli", id)
			if err != nil || result.CleanupPending {
				t.Fatalf("verified cleanup did not settle refusal: pending=%v err=%v", result.CleanupPending, err)
			}
			snapshot, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, poll.Room)
			if err != nil || snapshot.AvailableSeats != 3 || snapshot.RunningDedicated != 0 {
				t.Fatalf("verified cleanup left room or cap charged: %+v %v", snapshot, err)
			}
		})
	}
}

func approveSeatCallForObservationTest(t *testing.T, f *seatLifecycleFixture, id string, poll SeatCallPoll) SeatRequest {
	t.Helper()
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", id, poll)
	if err != nil || value.State != SeatStarting {
		t.Fatalf("start state=%s err=%v", value.State, err)
	}
	return value
}

func TestSeatRequestRefusalWaitsForExistingSeatCleanup(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := fmt.Sprintf("seat_%032x", 80)
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
	value := approveSeatCallForObservationTest(t, f, id, poll)
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim: %v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", value.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	report := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: value.Place, State: ComputerSeatFailed, Reason: "desktop failed"}}}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", value.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, report); err != nil {
		t.Fatal(err)
	}
	result, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || result.State != SeatRefused || !result.CleanupPending {
		t.Fatalf("refusal completed with Seat cleanup still owed: state=%s cleanup_pending=%v err=%v", result.State, result.CleanupPending, err)
	}
	list, err = f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", value.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	report.ListRevision, report.Generation, report.Sequence = list.Revision, 2, 2
	report.Seats[0].State, report.Seats[0].CleanupComplete = ComputerSeatCleaned, true
	report.Seats[0].Reason = ""
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", value.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, report); err != nil {
		t.Fatal(err)
	}
	result, err = f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || result.CleanupPending {
		t.Fatalf("cleaned existing Seat still pending: %v %v", result.CleanupPending, err)
	}
}

func TestSeatRequestOnceLiveComputerSurvivesLaterSeatFailure(t *testing.T) {
	for _, firstReady := range []bool{true, false} {
		t.Run(fmt.Sprintf("first_ready=%v", firstReady), func(t *testing.T) {

			f := newSeatLifecycleFixture(t)
			if _, err := f.store.RemoveComputer(t.Context(), f.computer.ComputerID, ComputerRemoveRequest{ComputerMutationPrecondition: ComputerMutationPrecondition{IntentRevision: f.computer.IntentRevision, StorageID: f.computer.StorageID, StorageGeneration: f.computer.StorageGeneration, Actor: "test"}}); err != nil {
				t.Fatal(err)
			}
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			f.store, err = OpenStore(f.path, StoreOptions{Clock: f.clock, LeaseDuration: time.Hour, ComputerDefaultImage: "ghcr.io/example/desktop@sha256:" + strings.Repeat("a", 64)})
			if err != nil {
				t.Fatal(err)
			}
			id := fmt.Sprintf("seat_%032x", 81)
			poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
			first := approveSeatCallForObservationTest(t, f, id, poll)
			claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
			if err != nil || claim == nil {
				t.Fatalf("claim: %v %v", claim, err)
			}
			auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
			observe := func(sequence int64, generation uint64, seats []ComputerSeatObservation) {
				t.Helper()
				list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", first.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", first.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: generation, Sequence: sequence, Seats: seats}); err != nil {
					t.Fatal(err)
				}
			}
			if firstReady {
				observe(1, 1, []ComputerSeatObservation{{SeatID: id, Place: first.Place, State: ComputerSeatReady}})
			}
			secondID := fmt.Sprintf("seat_%032x", 82)
			second := approveSeatCallForObservationTest(t, f, secondID, poll)
			if second.ComputerID != first.ComputerID {
				t.Fatal("second Seat did not reuse shared Computer")
			}
			firstState := ComputerSeatStarting
			if firstReady {
				firstState = ComputerSeatReady
			}
			observe(2, 2, []ComputerSeatObservation{{SeatID: id, Place: first.Place, State: firstState}, {SeatID: secondID, Place: second.Place, State: ComputerSeatReady}})
			observe(3, 2, []ComputerSeatObservation{{SeatID: id, Place: first.Place, State: ComputerSeatFailed, Reason: "desktop failed later"}, {SeatID: secondID, Place: second.Place, State: ComputerSeatReady}})
			// A cleanup queued from an older read must recheck the positive start
			// receipt in the removal write, even though the poll now excludes it.
			if err := f.store.cleanupSeatCallComputer(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
				t.Fatal(err)
			}
			computer, err := f.store.GetComputer(t.Context(), first.ComputerID)
			if err != nil || computer.DesiredState != contract.ServiceDesiredRunning {
				t.Fatalf("once-live Computer treated as failed new start: desired=%s err=%v", computer.DesiredState, err)
			}
			other, err := f.store.GetSeatCall(t.Context(), "cli", secondID)
			if err != nil || other.State != SeatLive {
				t.Fatalf("later Seat failure disturbed neighbour: %s %v", other.State, err)
			}
			observe(4, 3, []ComputerSeatObservation{{SeatID: id, Place: first.Place, State: ComputerSeatCleaned, CleanupComplete: true}, {SeatID: secondID, Place: second.Place, State: ComputerSeatReady}})
			result, err := f.store.GetSeatCall(t.Context(), "cli", id)
			if err != nil || result.CleanupPending {
				t.Fatalf("cleaned refusal waits for a live Computer's removal: pending=%v err=%v", result.CleanupPending, err)
			}

		})
	}
}

func TestSeatRequestStaleStartRefusalCannotEndLiveSeat(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := fmt.Sprintf("seat_%032x", 90)
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
	value := approveSeatCallForObservationTest(t, f, id, poll)
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim: %v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", value.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", value.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: value.Place, State: ComputerSeatReady}}}); err != nil {
		t.Fatal(err)
	}
	// Simulate an expired-start decision queued before the ready receipt won.
	result, err := f.store.refuseSeatCall(t.Context(), "fabric-computer-node", id, poll, "Seat readiness deadline exceeded")
	if err != nil || result.State != SeatLive {
		t.Fatalf("stale start decision ended a live Seat: state=%s err=%v", result.State, err)
	}
}
