package l1

import (
	"encoding/json"
	"github.com/Derek-X-Wang/wefty/contract"
	"testing"
	"time"
)

func TestSeatRunWorkingDirectoryStartAndIdleAfterCommand(t *testing.T) {
	f, id, secret, _ := liveSeatForRun(t)
	binding, err := f.store.ResolveSeatRun(t.Context(), id, secret)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"seat_id":"` + id + `","argv":["sh","-c","sleep 600 &"],"working_directory":"work/pkg"}`)
	var source contract.SeatRunInput
	if err = json.Unmarshal(raw, &source); err != nil {
		t.Fatal(err)
	}
	params, _ := json.Marshal(contract.SeatRunParams{Version: 2, SeatRun: source, Binding: binding})
	spec := contract.JobSpec{SchemaVersion: 1, DispatchKey: "directory-command", Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, RoutingTags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/false"}, Argv: []string{"false"}, WorkingDirectory: "/tmp"}, Labels: map[string]string{"run_id": "run_00000000000000000000000000000002", contract.LabelRunParams: string(params)}}
	original := spec.Labels[contract.LabelRunParams]
	for _, directory := range []string{"/outside", "work/../outside", "work//pkg"} {
		var value map[string]any
		if err := json.Unmarshal([]byte(original), &value); err != nil {
			t.Fatal(err)
		}
		value["seat_run"].(map[string]any)["working_directory"] = directory
		invalid, _ := json.Marshal(value)
		spec.Labels[contract.LabelRunParams] = string(invalid)
		if _, _, err := f.store.CreateJobAs(t.Context(), spec, JobOrigin{OriginatingSubmitter: "cli", SubmittedByRunLedger: true}); errorCode(err) != contract.ErrorInvalidRequest {
			t.Fatalf("directory %q: %v", directory, err)
		}
	}
	spec.Labels[contract.LabelRunParams] = original
	job, _, err := f.store.CreateJobAs(t.Context(), spec, JobOrigin{OriginatingSubmitter: "cli", SubmittedByRunLedger: true})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassOneShot)
	if err != nil || claim == nil {
		t.Fatalf("claim=%+v %v", claim, err)
	}
	input := SeatRunControlRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken, Phase: "start"}
	command, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", job.JobID, claim.Lease.AttemptID, input)
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := json.Marshal(command)
	var fields map[string]any
	_ = json.Unmarshal(wire, &fields)
	if fields["working_directory"] != "work/pkg" {
		t.Fatalf("start lost folder: %s", wire)
	}
	// L1 only holds the command, not its processes. Once its leader finishes,
	// advancing the injected idle clock ends the Seat even with background argv.
	zero := 0
	input.Phase = "finish"
	input.Result = &ProcessResult{ExitCode: &zero}
	if _, err = f.store.ControlSeatRun(t.Context(), "fabric-computer-node", job.JobID, claim.Lease.AttemptID, input); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(30 * time.Minute)
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	seat, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || seat.State != SeatEnding || seat.Reason != "expired" {
		t.Fatalf("command retained idle hold: %+v %v", seat, err)
	}
}
