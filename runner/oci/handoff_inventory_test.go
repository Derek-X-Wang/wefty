package oci

import (
	"context"
	"testing"
	"time"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// The retained-handoff inventory is a two-sided contract with no shared
// identity on the wire: the helper reports directory names, and the agent
// places them by deriving the name each of its own runs would have. These
// tests drive the real server and the real client, because what is being
// proved is that the two derivations agree and that the report survives the
// crossing intact -- not that one package can call itself.

type handoffInventoryEngine struct {
	adapterTestEngine
	response ocihelper.InventoryHandoffVolumesResponse
}

func (engine *handoffInventoryEngine) InventoryHandoffVolumes(context.Context, ocihelper.InventoryHandoffVolumesRequest) (ocihelper.InventoryHandoffVolumesResponse, error) {
	return engine.response, nil
}

func TestRetainedHandoffInventoryCrossesTheRuntimeSeamIntact(t *testing.T) {
	terminal := time.Date(2026, 9, 22, 10, 30, 0, 0, time.UTC)
	name, err := ocihelper.DeterministicHandoffVolumeDirectory("run-alpha")
	if err != nil {
		t.Fatal(err)
	}
	engine := &handoffInventoryEngine{response: ocihelper.InventoryHandoffVolumesResponse{
		Volumes: []ocihelper.RetainedHandoffVolume{
			{Name: name, TerminalAt: terminal, TerminalKnown: true, LogicalBytes: 4096, DedupedBytes: 4096, Entries: 3},
			{Name: "wefty-handoff-volume-deadbeefdeadbeefdeadbeefdeadbeef", Live: true, Anomaly: "no helper-owned retention receipt"},
		},
		Exhausted: true,
	}}
	adapter, stop := startAdapterTestServer(t, engine)
	defer stop()

	report, err := adapter.InventoryRetainedHandoffs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Exhausted || len(report.Volumes) != 2 {
		t.Fatalf("retained handoff report = %+v", report)
	}
	first := report.Volumes[0]
	if first.Name != name || !first.TerminalKnown || !first.TerminalAt.Equal(terminal) ||
		first.LogicalBytes != 4096 || first.DedupedBytes != 4096 || first.Entries != 3 || first.Live || first.Anomaly != "" {
		t.Fatalf("the retained volume lost facts crossing the seam: %+v", first)
	}
	second := report.Volumes[1]
	if !second.Live || second.TerminalKnown || second.Anomaly == "" {
		t.Fatalf("the live, receiptless volume lost facts crossing the seam: %+v", second)
	}
}

// No owner key crosses this boundary in either direction. The agent places a
// reported name by deriving what its own run would be called, so the two sides
// have to spell the same run the same way -- and a name it cannot derive is
// precisely the crash-residue signal the design relies on.
func TestTheAgentDerivesTheSameHandoffVolumeNameTheHelperReports(t *testing.T) {
	var inventory workloadrunner.RetainedHandoffInventory = &Adapter{}
	for _, ownerKey := range []string{"run-alpha", "run-beta", "a run with spaces", "run.with.dots"} {
		derived, err := inventory.RetainedHandoffVolumeName(ownerKey)
		if err != nil {
			t.Fatalf("derive the handoff volume name for %q: %v", ownerKey, err)
		}
		helper, err := ocihelper.DeterministicHandoffVolumeDirectory(ownerKey)
		if err != nil {
			t.Fatalf("the helper cannot name %q: %v", ownerKey, err)
		}
		if derived != helper {
			t.Fatalf("the agent would look for %q where the helper reports %q", derived, helper)
		}
	}
	if _, err := inventory.RetainedHandoffVolumeName(""); err == nil {
		t.Fatal("an empty owner key produced a handoff volume name")
	}
}
