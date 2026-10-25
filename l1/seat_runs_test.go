package l1

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestSeatRunBindingRefusesEndedSeatAndAttemptReuse(t *testing.T) {
	f, id, secret, auth := liveSeatForCopy(t)
	binding, err := f.store.ResolveSeatRun(t.Context(), id, secret)
	if err != nil || binding.SeatID != id || binding.ComputerAttemptID != auth.AttemptID {
		t.Fatalf("binding=%+v %v", binding, err)
	}
	p := contract.SeatRunParams{Version: 1, SeatRun: contract.SeatRunInput{SeatID: id, Argv: []string{"env"}}, Binding: binding}
	raw, _ := json.Marshal(p)
	spec := computerCapabilityJobSpec("seat-run")
	spec.Kind = contract.JobKindProcess
	spec.Class = contract.JobClassOneShot
	spec.Execution = contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/false"}, Argv: []string{"false"}, WorkingDirectory: "/tmp"}
	spec.RoutingTags = []string{contract.StableNodeTagPrefix + binding.NodeID}
	spec.Labels = map[string]string{"run_id": "run_00000000000000000000000000000001", contract.LabelRunParams: string(raw)}
	if _, _, err = f.store.CreateJobAs(t.Context(), spec, JobOrigin{OriginatingSubmitter: "cli", SubmittedByRunLedger: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.ResolveSeatRun(t.Context(), id, secret); errorCode(err) != contract.ErrorCode("seat_ended") {
		t.Fatalf("ended binding=%v", err)
	}
	spec.DispatchKey = "later-seat-run"
	if _, _, err = f.store.CreateJobAs(t.Context(), spec, JobOrigin{OriginatingSubmitter: "cli", SubmittedByRunLedger: true}); err == nil {
		t.Fatal("old Seat binding admitted after release")
	}
}

func startSeatRunFixture(t *testing.T) (*seatLifecycleFixture, string, string, Claim, SeatRunControlRequest) {
	t.Helper()
	f, id, secret, _ := liveSeatForRun(t)
	binding, err := f.store.ResolveSeatRun(t.Context(), id, secret)
	if err != nil {
		t.Fatal(err)
	}
	p := contract.SeatRunParams{Version: 1, SeatRun: contract.SeatRunInput{SeatID: id, Argv: []string{"env"}, MaxRuntimeSeconds: 3600}, Binding: binding}
	raw, _ := json.Marshal(p)
	spec := contract.JobSpec{SchemaVersion: 1, DispatchKey: "seat-run-command", Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, RoutingTags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/false"}, Argv: []string{"false"}, WorkingDirectory: "/tmp"}, Labels: map[string]string{"run_id": "run_00000000000000000000000000000001", contract.LabelRunParams: string(raw)}}
	job, _, err := f.store.CreateJobAs(t.Context(), spec, JobOrigin{OriginatingSubmitter: "cli", SubmittedByRunLedger: true})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassOneShot)
	if err != nil || claim == nil {
		t.Fatalf("claim=%+v %v job=%s", claim, err, job.JobID)
	}
	input := SeatRunControlRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken, Phase: "start"}
	if _, err = f.store.ControlSeatRun(t.Context(), "fabric-computer-node", job.JobID, claim.Lease.AttemptID, input); err != nil {
		t.Fatal(err)
	}
	return f, id, secret, *claim, input
}
func TestSeatRunFinishRejectsExitCodeAtDeadline(t *testing.T) {
	f, id, secret, claim, input := startSeatRunFixture(t)
	zero := 0
	input.Phase = "finish"
	input.Result = &ProcessResult{ExitCode: &zero, LimitReached: contract.LimitReachedMaxRuntime}
	if _, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, input); err == nil {
		t.Fatal("deadline accepted a process exit code")
	}
	input.Result = &ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent, LimitReached: contract.LimitReachedMaxRuntime}
	for range 2 {
		if _, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, input); err != nil {
			t.Fatal(err)
		}
	}
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	starts, ends := 0, 0
	for _, event := range page.Events {
		if event.RunID != claim.Job.Spec.Labels["run_id"] {
			continue
		}
		if event.Kind == SeatEventCommandStarted {
			starts++
		}
		if event.Kind == SeatEventCommandFinished {
			ends++
			if event.ExitCode != nil || event.Reason != "max_runtime" || !strings.Contains(event.Note, "longer --max-runtime") {
				t.Fatalf("deadline event=%+v", event)
			}
		}
	}
	if starts != 1 || ends != 1 {
		t.Fatalf("start=%d end=%d", starts, ends)
	}
	f.clock.Advance(29 * time.Minute)
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatLive {
		t.Fatalf("Seat after deadline=%+v %v", value, err)
	}
	f.clock.Advance(time.Minute)
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err = f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatEnding {
		t.Fatalf("idle Seat=%+v %v", value, err)
	}
}

func liveSeatForRun(t *testing.T) (*seatLifecycleFixture, string, string, SeatCopyAuthorityInput) {
	t.Helper()
	f := newSeatLifecycleFixture(t)
	var registrationErr error
	f.node, registrationErr = f.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-computer-node"}, contract.NodeRegistration{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, RootInstanceID: "root", OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true, "kind:oci": true, "cgroup_v2": true, "computer": true, "computer:seat-requests": true}, CapabilityRevision: 2, CapabilityObservedAt: f.clock.Now(), MissingCapabilities: []string{}}, NodePolicy{Tags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, MaxServiceSlots: 4, MaxOneshotSlots: 2}, true)
	if registrationErr != nil {
		t.Fatal(registrationErr)
	}

	id := "seat_00000000000000000000000000000001"
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	f.request(t, 1, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim=%v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatReady}}}); err != nil {
		t.Fatal(err)
	}
	result, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || result.Credential == "" {
		t.Fatalf("credential: %+v %v", result, err)
	}
	return f, id, result.Credential, SeatCopyAuthorityInput{ComputerSeatAttemptRequest: auth, ComputerID: f.computer.ComputerID, JobID: claim.Job.JobID, AttemptID: claim.Lease.AttemptID, Credential: result.Credential}
}
func TestSeatRunLateCancelAfterPlaceReuseLeavesLaterRunLive(t *testing.T) {
	f, old, secret, first, input := startSeatRunFixture(t)
	binding, err := f.store.ResolveSeatRun(t.Context(), old, secret)
	if err != nil {
		t.Fatal(err)
	}
	pollInput := input
	pollInput.Phase = "poll"
	currentCommand, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", first.Job.JobID, first.Lease.AttemptID, pollInput)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.EndSeatRequest(t.Context(), "fabric-computer-node", old, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	// Confirm cleanup before reusing the same place, exactly as the image does.
	computerAuthority := func(generation, sequence uint64, seats []ComputerSeatObservation) {
		t.Helper()
		// The start reply is node-only and contains the Computer's exact fence.
		fence := currentCommand.FencingToken
		auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: fence}
		list, e := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", binding.ComputerID, binding.ComputerJobID, binding.ComputerAttemptID, auth)
		if e != nil {
			t.Fatal(e)
		}
		_, e = f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", binding.ComputerID, binding.ComputerJobID, binding.ComputerAttemptID, ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: generation, Sequence: int64(sequence), Seats: seats})
		if e != nil {
			t.Fatal(e)
		}
	}
	computerAuthority(2, 2, []ComputerSeatObservation{{SeatID: old, Place: 1, State: ComputerSeatCleaned, CleanupComplete: true}})
	nextID := "seat_00000000000000000000000000000002"
	if _, err = f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: nextID, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	next := f.request(t, 2, true)
	f.reserve(t, next, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	if _, err = f.store.StartSeatRequest(t.Context(), "fabric-computer-node", next, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	computerAuthority(3, 3, []ComputerSeatObservation{{SeatID: next, Place: 1, State: ComputerSeatReady}})
	result, err := f.store.GetSeatCall(t.Context(), "cli", next)
	if err != nil {
		t.Fatal(err)
	}
	laterBinding, err := f.store.ResolveSeatRun(t.Context(), next, result.Credential)
	if err != nil {
		t.Fatal(err)
	}
	p := contract.SeatRunParams{Version: 1, SeatRun: contract.SeatRunInput{SeatID: next, Argv: []string{"sleep", "1000"}}, Binding: laterBinding}
	raw, _ := json.Marshal(p)
	spec := first.Job.Spec
	spec.DispatchKey = "later-command"
	spec.Labels = map[string]string{"run_id": "run_00000000000000000000000000000002", contract.LabelRunParams: string(raw)}
	job, _, err := f.store.CreateJobAs(t.Context(), spec, JobOrigin{OriginatingSubmitter: "cli", SubmittedByRunLedger: true})
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassOneShot)
	if err != nil || second == nil {
		t.Fatalf("second claim=%+v %v", second, err)
	}
	later := input
	later.FencingToken = second.Lease.FencingToken
	if _, err = f.store.ControlSeatRun(t.Context(), "fabric-computer-node", job.JobID, second.Lease.AttemptID, later); err != nil {
		t.Fatal(err)
	}
	input.Phase = "poll"
	oldPoll, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", first.Job.JobID, first.Lease.AttemptID, input)
	if err != nil || !oldPoll.Cancel {
		t.Fatalf("old poll=%+v %v", oldPoll, err)
	}
	if _, err = f.store.ControlSeatRun(t.Context(), "fabric-computer-node", job.JobID, first.Lease.AttemptID, input); err == nil {
		t.Fatal("old Attempt controlled later Run")
	}
	input.Phase = "finish"
	input.Result = &ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}
	if _, err = f.store.ControlSeatRun(t.Context(), "fabric-computer-node", first.Job.JobID, first.Lease.AttemptID, input); err != nil {
		t.Fatal(err)
	}
	later.Phase = "poll"
	newPoll, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", job.JobID, second.Lease.AttemptID, later)
	if err != nil || newPoll.Cancel {
		t.Fatalf("new poll=%+v %v", newPoll, err)
	}
	f.clock.Advance(31 * time.Minute)
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), next)
	if err != nil || value.State != SeatLive {
		t.Fatalf("later Seat=%+v %v", value, err)
	}
}

func TestSeatRunHoldRecoversAfterLostFinishReceipt(t *testing.T) {
	f, id, _, claim, _ := startSeatRunFixture(t)
	code := 0
	if _, err := f.store.CompleteAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "ended-without-seat-receipt", Result: ProcessResult{ExitCode: &code}}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(30 * time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatEnding {
		t.Fatalf("completed Run kept its Seat alive: %+v %v", value, err)
	}
}

func TestSeatRunResolveCountsAsOneTimeSeatCall(t *testing.T) {
	f, id, secret, _ := liveSeatForRun(t)
	f.clock.Advance(29 * time.Minute)
	if _, err := f.store.ResolveSeatRun(t.Context(), id, secret); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatLive {
		t.Fatalf("command submission did not renew Seat: %+v %v", value, err)
	}
}

func TestSeatRunClusterDeadlineSettingReachesResolution(t *testing.T) {
	f, id, secret, _ := liveSeatForRun(t)
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.store, err = OpenStore(f.path, StoreOptions{Clock: f.clock, LeaseDuration: time.Hour, SeatCommandMaxRuntime: 2 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := f.store.ResolveSeatRun(t.Context(), id, secret)
	if err != nil || binding.DefaultMaxRuntimeSeconds != 7200 {
		t.Fatalf("configured deadline=%+v %v", binding, err)
	}
}
