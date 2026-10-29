package scripts

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Exercise the observer on a database made by L1, including its migrations and
// actual lifecycle event JSON. No host boot query, runtime or Mac UI is used.
func TestStageAObserverRealStore(t *testing.T) {
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "l1.sqlite")
	store, err := l1.OpenStore(path, l1.StoreOptions{Clock: l1.ClockFunc(func() time.Time { return now }), LeaseDuration: time.Hour, ComputerDefaultImage: "ghcr.io/example/desktop@sha256:" + strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	node, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: "proof-node"}, contract.NodeRegistration{NodeID: "proof-node", BootSessionID: "proof-boot", RootInstanceID: "proof-root", OS: "linux", Architecture: "arm64", AgentVersion: "test", Capabilities: map[string]bool{"computer:seat-requests": true, "kind:oci": true, "computer": true, "cgroup_v2": true}, CapabilityRevision: 1, CapabilityObservedAt: now}, l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + "proof-node"}, MaxServiceSlots: 4}, true)
	if err != nil {
		t.Fatal(err)
	}
	claims := map[string]*l1.Claim{}
	revisions := map[string]int64{}
	poll := l1.SeatCallPoll{SeatNodeRequest: l1.SeatNodeRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID}, Room: l1.DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
	for n := 1; n <= 4; n++ {
		id := fmt.Sprintf("seat_%032x", n)
		_, err = store.CreateSeatCall(t.Context(), "proof-cli", l1.SeatCallInput{SeatID: id, NodeID: node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.PollSeatCalls(t.Context(), "proof-node", poll); err != nil {
			t.Fatal(err)
		}
		if n == 4 {
			break // L1's actual room admission refuses before the dialog.
		}
		now = now.Add(time.Second)
		_, err = store.AnswerSeatApproval(t.Context(), "proof-node", id, l1.SeatApprovalAnswer{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Approved: true})
		if err != nil {
			t.Fatal(err)
		}
		value, err := store.AdvanceSeatCall(t.Context(), "proof-node", id, poll)
		if err != nil {
			t.Fatal(err)
		}
		claim := claims[value.ComputerID]
		if claim == nil {
			claim, err = store.ClaimJob(t.Context(), "proof-node", node.NodeID, node.BootSessionID, contract.JobClassService)
			if err != nil || claim == nil {
				t.Fatalf("claim: %v", err)
			}
			claims[value.ComputerID] = claim
		}
		auth := l1.ComputerSeatAttemptRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, FencingToken: claim.Lease.FencingToken}
		list, err := store.GetComputerSeatListForAttempt(t.Context(), "proof-node", value.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
		if err != nil {
			t.Fatal(err)
		}
		var observations []l1.ComputerSeatObservation
		for _, seat := range list.Seats {
			observations = append(observations, l1.ComputerSeatObservation{SeatID: seat.SeatID, Place: seat.Place, State: l1.ComputerSeatReady})
		}
		revisions[value.ComputerID]++
		now = now.Add(time.Second)
		_, err = store.ReportComputerSeatObservations(t.Context(), "proof-node", value.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, l1.ComputerSeatObservationReport{NodeID: node.NodeID, BootSessionID: node.BootSessionID, FencingToken: claim.Lease.FencingToken, ListRevision: list.Revision, Generation: uint64(revisions[value.ComputerID]), Sequence: revisions[value.ComputerID], Seats: observations})
		if err != nil {
			t.Fatal(err)
		}
	}
	// Preserve an actual coherent live database, not a hand-written projection.
	livePath := filepath.Join(t.TempDir(), "live.sqlite")
	backup := exec.Command("python3", "-c", "import sqlite3,sys; src=sqlite3.connect(sys.argv[1]); dst=sqlite3.connect(sys.argv[2]); src.backup(dst); dst.execute('PRAGMA journal_mode=DELETE'); dst.close(); src.close()", path, livePath)
	if output, err := backup.CombinedOutput(); err != nil {
		t.Fatalf("snapshot backup: %v %s", err, output)
	}
	// Exercise real expiry, including warning-before-end, on the injected clock.
	now = now.Add(31 * time.Minute)
	if err := store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	for id := range claims {
		c, err := store.GetComputer(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.RemoveComputer(t.Context(), id, l1.ComputerRemoveRequest{ComputerMutationPrecondition: l1.ComputerMutationPrecondition{IntentRevision: c.IntentRevision, StorageID: c.StorageID, StorageGeneration: c.StorageGeneration, Actor: "test"}}); err != nil {
			t.Fatal(err)
		}
	}
	directives, err := store.ListNodeRemovalDirectives(t.Context(), "proof-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 2 {
		t.Fatalf("removal directives=%d: %v", len(directives), err)
	}
	for _, d := range directives {
		if _, err := store.AcknowledgeServiceRemoval(t.Context(), "proof-node", d.JobID, l1.RemovalAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, RemovalGeneration: d.RemovalGeneration, CleanupFence: d.CleanupFence, RootInstanceID: d.RootInstanceID, IdempotencyKey: "proof-removed"}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.FinalizeServiceRemoval(t.Context(), d.JobID); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("python3", "-m", "unittest", "discover", "-s", ".", "-p", "test_stage_a.py", "-v")
	command.Env = append(os.Environ(), "STAGE_A_TEST_DB="+livePath, "STAGE_A_ENDED_DB="+path)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Stage A observer: %v\n%s", err, output)
	}
}
