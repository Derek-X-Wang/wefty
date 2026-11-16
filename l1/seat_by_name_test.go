package l1

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type byNameFixture struct {
	*seatLifecycleFixture
	poll  SeatCallPoll
	seats int
}

func newByNameFixture(t *testing.T) *byNameFixture {
	t.Helper()
	f := newSeatLifecycleFixture(t)
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	// The installed default pins the test image's digest: the fallback case.
	f.store, err = openTestStore(t, f.path, StoreOptions{Clock: f.clock, LeaseDuration: time.Hour, ComputerDefaultImage: "ghcr.io/example/tool@" + testTopDigest})
	if err != nil {
		t.Fatal(err)
	}
	// Only the by-name targets may be claimed in these tests.
	if _, err := f.store.SetComputerDesiredState(t.Context(), f.computer.ComputerID, computerDesiredRequest(f.computer, contract.ServiceDesiredStopped, "test")); err != nil {
		t.Fatal(err)
	}
	return &byNameFixture{seatLifecycleFixture: f, poll: SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID},
		Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}}
}

// owner creates an owner-made Computer. labelled images use another digest
// than the installed default, so only their recorded places count.
func (f *byNameFixture) owner(t *testing.T, name string, limit, places int, labelled bool) Computer {
	t.Helper()
	spec := computerCapabilityJobSpec(name)
	if labelled {
		digest := "sha256:" + strings.Repeat("e", 64)
		spec.Execution.OCI.Image.Digest = &digest
	}
	c, _, err := f.store.CreateComputer(t.Context(), CreateComputerRequest{Name: name, Spec: spec, Actor: "owner",
		SeatLimit: intPointer(limit), IdleAction: ComputerIdleStop, ImageSeatPlaces: places})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// ask files one by-name request and runs the node's preflight. It returns
// the Seat ID, its state, and whether the node would show a dialog for it.
func (f *byNameFixture) ask(t *testing.T, name, network string) (string, SeatRequest, *SeatCallInput) {
	t.Helper()
	f.seats++
	id := fmt.Sprintf("seat_%032x", 0xb000+f.seats)
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: SeatFormByName, Network: network, Computer: name}); err != nil {
		t.Fatal(err)
	}
	list, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", f.poll)
	if err != nil {
		t.Fatal(err)
	}
	var offered *SeatCallInput
	for i := range list.Requests {
		if list.Requests[i].SeatID == id {
			offered = &list.Requests[i]
		}
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return id, value, offered
}

func (f *byNameFixture) approve(t *testing.T, id string) SeatRequest {
	t.Helper()
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", id, f.poll)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func (f *byNameFixture) room(t *testing.T) ComputerRoomSnapshot {
	t.Helper()
	snapshot, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, f.poll.Room)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// TestSeatByNameRefusalsComeBeforeTheDialog covers BN-4 and CT-6 (spec P12):
// each by-name refusal is recorded at once with its reason and never reaches
// the owner's dialog.
func TestSeatByNameRefusalsComeBeforeTheDialog(t *testing.T) {
	f := newByNameFixture(t)
	var baseline int
	if err := f.store.db.QueryRow(`SELECT COUNT(*) FROM computers`).Scan(&baseline); err != nil {
		t.Fatal(err)
	}
	f.owner(t, "desk", 2, 4, true)
	f.owner(t, "plain", 2, 0, true)
	f.owner(t, "no-seats", 0, 4, true)
	failed := f.owner(t, "broken", 2, 4, true)
	if _, err := f.store.db.Exec(`UPDATE jobs SET state='failed' WHERE job_id=?`, failed.CurrentJobID); err != nil {
		t.Fatal(err)
	}
	removed := f.owner(t, "gone", 2, 4, true)
	if _, err := f.store.db.Exec(`UPDATE computers SET desired_state='removed' WHERE computer_id=?`, removed.ComputerID); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, network, prefix, text string
	}{
		{"desk", "none", SeatByNameNetworkPrefix, `"desk" has network internet`},
		{"plain", "internet", SeatByNameUnsupportedPrefix, "--seat-support` for a new image digest, or create the Computer with --seat-limit"},
		{"no-seats", "internet", SeatByNameUnsupportedPrefix, "Seat limit 0"},
		{"broken", "internet", SeatByNameUnavailablePrefix, "failed; the owner decides"},
		{"gone", "internet", SeatByNameUnavailablePrefix, "was removed"},
		{"missing", "internet", SeatByNameUnavailablePrefix, `no Computer named "missing"`},
	} {
		id, value, offered := f.ask(t, test.name, test.network)
		if value.State != SeatRefused || !strings.HasPrefix(value.Reason, test.prefix) || !strings.Contains(value.Reason, test.text) || offered != nil {
			t.Fatalf("%s: %s = %+v offered=%v", test.name, id, value, offered != nil)
		}
		approval, err := readByNameApprovalRequest(t, f.store, id)
		if err != nil || approval.Form != SeatFormByName || approval.Computer != test.name {
			t.Fatalf("%s refusal record = %+v err=%v", test.name, approval, err)
		}
	}
	var computers int
	if err := f.store.db.QueryRow(`SELECT COUNT(*) FROM computers`).Scan(&computers); err != nil || computers != baseline+5 {
		t.Fatalf("a refused by-name request made a Computer: %d %v", computers, err)
	}
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: fmt.Sprintf("seat_%032x", 0xbfff), NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: SeatFormByName, Network: "internet", Computer: "desk", StartComputer: true}); errorCode(err) != contract.ErrorInvalidRequest {
		t.Fatalf("caller-supplied start_computer err=%v", err)
	}
}

// TestSeatByNameOwnerComputerSeatsShareItsMemory covers BN-1: two Seats on an
// owner-made Computer with Seat limit 2 live within its own memory limit, and
// the Mac's room does not change when they join or leave.
func TestSeatByNameOwnerComputerSeatsShareItsMemory(t *testing.T) {
	f := newByNameFixture(t)
	// Places 0 with the installed default image: the fallback supports Seats.
	desk := f.owner(t, "default-desk", 2, 0, false)
	before := f.room(t)
	var seats []SeatRequest
	for range 2 {
		id, value, offered := f.ask(t, "default-desk", "internet")
		if value.State == SeatRefused || offered == nil || offered.StartComputer {
			t.Fatalf("by-name request %s = %+v offered=%+v", id, value, offered)
		}
		value = f.approve(t, id)
		if value.ComputerID != desk.ComputerID || value.State != SeatStarting {
			t.Fatalf("by-name Seat %s = %+v", id, value)
		}
		seats = append(seats, value)
	}
	if seats[0].Place == seats[1].Place {
		t.Fatalf("both Seats took place %d", seats[0].Place)
	}
	if after := f.room(t); after != before {
		t.Fatalf("owner-made Seats changed the Mac's room: %+v -> %+v", before, after)
	}
	var limits int
	if err := f.store.db.QueryRow(`SELECT COUNT(*) FROM computer_room_limits WHERE computer_id=?`, desk.ComputerID).Scan(&limits); err != nil || limits != 0 {
		t.Fatalf("owner-made Computer got a growable memory record: %d %v", limits, err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.ComputerStorage == nil || claim.ComputerStorage.ComputerID != desk.ComputerID ||
		claim.ComputerStorage.SeatRequestMade || claim.ComputerStorage.SeatLimit != 2 || claim.ComputerStorage.ImageSeatPlaces != MaxComputerSeatPlaces {
		t.Fatalf("owner-made claim cannot run Seats: %#v err=%v", claim, err)
	}
	if *claim.Job.Spec.Execution.OCI.Limits.MemoryBytes != *desk.CurrentJob.Spec.Execution.OCI.Limits.MemoryBytes {
		t.Fatal("Seats changed the owner's memory limit")
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", desk.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil || len(list.Seats) != 2 || list.Recovery != nil && list.Recovery.Room != nil {
		t.Fatalf("owner-made Seat list = %+v err=%v", list, err)
	}
	ready := []ComputerSeatObservation{}
	for _, binding := range list.Seats {
		ready = append(ready, ComputerSeatObservation{SeatID: binding.SeatID, Place: binding.Place, State: ComputerSeatReady})
	}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", desk.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: ready}); err != nil {
		t.Fatal(err)
	}
	for _, seat := range seats {
		if live, err := f.store.GetSeatRequest(t.Context(), seat.SeatID); err != nil || live.State != SeatLive || live.ComputerID != desk.ComputerID {
			t.Fatalf("Seat %s = %+v err=%v", seat.SeatID, live, err)
		}
	}
	if after := f.room(t); after != before {
		t.Fatalf("live owner-made Seats changed the Mac's room: %+v -> %+v", before, after)
	}

	// A third request is refused at once: the Computer holds its 2 Seats.
	_, third, offered := f.ask(t, "default-desk", "internet")
	if third.State != SeatRefused || !strings.HasPrefix(third.Reason, SeatByNameFullPrefix) || offered != nil {
		t.Fatalf("third Seat = %+v", third)
	}
	if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", seats[0].SeatID, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	if after := f.room(t); after != before {
		t.Fatalf("a Seat leaving changed the Mac's room: %+v -> %+v", before, after)
	}
}

// TestSeatByNameStartsAStoppedComputerOnlyWithApproval covers BN-2: the
// dialog says "and start it", and that approval starts the Computer. An
// approval that did not say so never starts one.
func TestSeatByNameStartsAStoppedComputerOnlyWithApproval(t *testing.T) {
	f := newByNameFixture(t)
	stop := func(c Computer) Computer {
		t.Helper()
		c, err := f.store.GetComputer(t.Context(), c.ComputerID)
		if err != nil {
			t.Fatal(err)
		}
		c, err = f.store.SetComputerDesiredState(t.Context(), c.ComputerID, computerDesiredRequest(c, contract.ServiceDesiredStopped, "owner"))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	desk := stop(f.owner(t, "desk", 2, 4, true))
	id, value, offered := f.ask(t, "desk", "internet")
	if value.State == SeatRefused || offered == nil || !offered.StartComputer {
		t.Fatalf("stopped by-name request %+v offered=%+v", value, offered)
	}
	approval, err := readByNameApprovalRequest(t, f.store, id)
	if err != nil || !approval.StartComputer || approval.Computer != "desk" {
		t.Fatalf("approval request = %+v err=%v", approval, err)
	}
	value = f.approve(t, id)
	started, err := f.store.GetComputer(t.Context(), desk.ComputerID)
	if err != nil || started.DesiredState != contract.ServiceDesiredRunning || value.ComputerID != desk.ComputerID || value.State != SeatStarting {
		t.Fatalf("approval did not start the Computer: %+v seat=%+v err=%v", started.DesiredState, value, err)
	}
	history, err := testReads(f.store).ListComputerIntents(t.Context(), desk.ComputerID, "", MaxJobPageLimit)
	if last := history.Intents[len(history.Intents)-1]; err != nil || last.Actor != "seat-request" || last.DesiredState != contract.ServiceDesiredRunning {
		t.Fatalf("start intent = %+v err=%v", last, err)
	}

	// Running at the dialog, stopped by the owner before the approval lands.
	other := f.owner(t, "other", 2, 4, true)
	id, _, offered = f.ask(t, "other", "internet")
	if offered == nil || offered.StartComputer {
		t.Fatalf("running Computer offered=%+v", offered)
	}
	stop(other)
	value = f.approve(t, id)
	still, err := f.store.GetComputer(t.Context(), other.ComputerID)
	if err != nil || value.State != SeatRefused || !strings.HasPrefix(value.Reason, SeatByNameUnavailablePrefix) || still.DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("an approval without \"and start it\" started a Computer: seat=%+v computer=%s err=%v", value, still.DesiredState, err)
	}
}

// TestSeatByNameStartCommitsOnlyWithItsReservation proves the start and the
// room reservation commit together: a reservation refused for room leaves the
// stopped Computer stopped (review, #1155).
func TestSeatByNameStartCommitsOnlyWithItsReservation(t *testing.T) {
	f := newByNameFixture(t)
	// The fixture's Seat-request-made Computer is stopped and charges room
	// per Seat, so its reservation can be refused.
	id, value, offered := f.ask(t, f.computer.Name, "none")
	if value.State == SeatRefused || offered == nil || !offered.StartComputer {
		t.Fatalf("stopped Seat-request-made target: %+v offered=%+v", value, offered)
	}
	f.poll.Room = DefaultComputerRoomConfig(0) // the Mac filled up during the dialog
	value = f.approve(t, id)
	c, err := f.store.GetComputer(t.Context(), f.computer.ComputerID)
	if err != nil || value.State != SeatRefused || value.Reason != ErrComputerRoomFull.Error() || c.DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("refused reservation kept its start: seat=%+v computer=%s err=%v", value, c.DesiredState, err)
	}
}

// TestSeatByNameReservationTakesOnlyTheNamedComputer proves the node's
// reservation route cannot move a by-name approval to another Computer.
func TestSeatByNameReservationTakesOnlyTheNamedComputer(t *testing.T) {
	f := newByNameFixture(t)
	f.owner(t, "desk", 2, 4, true)
	other := f.owner(t, "other", 2, 4, true)
	id, _, offered := f.ask(t, "desk", "internet")
	if offered == nil {
		t.Fatal("by-name request was not offered")
	}
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
		t.Fatal(err)
	}
	_, err := f.store.ReserveSeatRoom(t.Context(), "fabric-computer-node", id, SeatRoomReservationRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ComputerID: other.ComputerID, Place: 1, Room: f.poll.Room})
	if errorCode(err) != contract.ErrorConflict {
		t.Fatalf("reservation on another Computer err=%v", err)
	}
}

// TestStageAOwnerComputerKeepsItsOrdinaryPath proves an owner-made Computer
// stored before stage B (Seat limit 0) keeps the ordinary Computer path even
// on the installed default image: its claim carries Seat limit 0, so the
// node agent never runs the Seat controller for it (agent test
// TestComputerTakesSeatsOnlyWhenItsImageSupportsThem, "Seat limit 0"), and a
// by-name request is refused before any dialog.
func TestStageAOwnerComputerKeepsItsOrdinaryPath(t *testing.T) {
	f := newByNameFixture(t)
	desk, _, err := f.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "stage-a-desk", Spec: computerCapabilityJobSpec("stage-a-desk"), Actor: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	// Recreate the stored stage A row: the pre-Seat migration default.
	if _, err := f.store.db.Exec(`UPDATE computers SET seat_limit=0 WHERE computer_id=?`, desk.ComputerID); err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.ComputerStorage == nil || claim.ComputerStorage.ComputerID != desk.ComputerID ||
		claim.ComputerStorage.SeatRequestMade || claim.ComputerStorage.SeatLimit != 0 {
		t.Fatalf("stage A claim = %#v err=%v", claim, err)
	}
	_, value, offered := f.ask(t, "stage-a-desk", "internet")
	if value.State != SeatRefused || !strings.Contains(value.Reason, "Seat limit 0") || offered != nil {
		t.Fatalf("by name reached a stage A Computer: %+v", value)
	}
}

// readByNameApprovalRequest returns the stored, immutable dialog input.
func readByNameApprovalRequest(t *testing.T, store *Store, id string) (SeatApprovalRequest, error) {
	t.Helper()
	value, err := readSeatApproval(t.Context(), store.db, id)
	return value.Request, err
}
