package upgradecompat

import (
	"reflect"
	"slices"
	"testing"
)

func TestDecide(t *testing.T) {
	primary := Generation{ID: "current", Role: Primary, Protocols: []uint32{1, 2}}
	older := Generation{ID: "older", Role: Retiring, Protocols: []uint32{1}}
	tests := []struct {
		name   string
		change func(*Input)
		want   Decision
	}{
		{"supported overlap", nil, Decision{Rolling, Compatible, 1}},
		{"ordinary process one-shot", func(in *Input) { in.Work = []WorkKind{ProcessOneshot} }, Decision{Rolling, Compatible, 1}},
		{"restartable process one-shot", func(in *Input) { in.Work = []WorkKind{RestartableProcessOneshot} }, Decision{Rolling, Compatible, 1}},
		{"services recover selectively", func(in *Input) { in.Work = []WorkKind{ProcessService} }, Decision{Rolling, Compatible, 1}},
		{"verified idle", func(in *Input) { in.Work = nil }, Decision{Rolling, Compatible, 1}},
		{"empty verified work set", func(in *Input) { in.Work = []WorkKind{} }, Decision{Rolling, Compatible, 1}},
		{"incompatible candidate", func(in *Input) { in.CandidateProtocols = []uint32{3} }, Decision{Refuse, IncompatibleProtocols, 0}},
		{"compatible primary incompatible older retiree", func(in *Input) { in.CandidateProtocols = []uint32{2} }, Decision{Refuse, IncompatibleProtocols, 0}},
		{"candidate without support", func(in *Input) { in.CandidateProtocols = nil }, Decision{Refuse, CandidateUnsupported, 0}},
		{"legacy primary bootstrap", func(in *Input) { in.Live = []Generation{{ID: "legacy", Role: Primary}} }, Decision{Drain, LegacyPrimary, 0}},
		{"legacy candidate and primary", func(in *Input) { in.CandidateProtocols = nil; in.Live = []Generation{{ID: "legacy", Role: Primary}} }, Decision{Drain, LegacyPrimary, 0}},
		{"OCI work", func(in *Input) { in.Live = []Generation{primary}; in.Work = []WorkKind{ProcessOneshot, OCIWork} }, Decision{Drain, UnsupportedWork, 0}},
		{"Mac fleet", func(in *Input) { in.Live = []Generation{primary}; in.Installation = FleetMac }, Decision{Drain, UnsupportedInstallation, 0}},
		{"Linux", func(in *Input) { in.Live = []Generation{primary}; in.Installation = Linux }, Decision{Drain, UnsupportedInstallation, 0}},
		{"OCI during overlap", func(in *Input) { in.Work = []WorkKind{OCIWork} }, Decision{Refuse, UnsafeDrainDuringOverlap, 0}},
		{"fleet during overlap", func(in *Input) { in.Installation = FleetMac }, Decision{Refuse, UnsafeDrainDuringOverlap, 0}},
		{"Linux during overlap", func(in *Input) { in.Installation = Linux }, Decision{Refuse, UnsafeDrainDuringOverlap, 0}},
		{"legacy primary during overlap", func(in *Input) { in.Live[0].Protocols = nil }, Decision{Refuse, UnsafeDrainDuringOverlap, 0}},
		{"legacy retiree", func(in *Input) { in.Live[1].Protocols = nil }, Decision{Refuse, IncompatibleProtocols, 0}},
		{"highest common version", func(in *Input) { in.CandidateProtocols = []uint32{1, 3, 2}; in.Live[1].Protocols = []uint32{2, 1} }, Decision{Rolling, Compatible, 2}},
		{"pairwise overlap is insufficient", func(in *Input) { in.Live[0].Protocols = []uint32{2}; in.CandidateProtocols = []uint32{1, 2} }, Decision{Refuse, IncompatibleProtocols, 0}},
		{"duplicate versions are set members", func(in *Input) { in.CandidateProtocols = []uint32{1, 1} }, Decision{Rolling, Compatible, 1}},
		{"primary need not be first", func(in *Input) { in.Live[0], in.Live[1] = in.Live[1], in.Live[0] }, Decision{Rolling, Compatible, 1}},
		{"many retiring generations", func(in *Input) {
			in.Live = append(in.Live, Generation{ID: "oldest", Role: Retiring, Protocols: []uint32{1}}, Generation{ID: "middle", Role: Retiring, Protocols: []uint32{1, 2}})
		}, Decision{Rolling, Compatible, 1}},
		{"no fixed generation limit", func(in *Input) {
			for id := 'a'; id <= 'z'; id++ {
				in.Live = append(in.Live, Generation{ID: string(id), Role: Retiring, Protocols: []uint32{1}})
			}
		}, Decision{Rolling, Compatible, 1}},
		{"incompatible last retiree", func(in *Input) {
			in.Live = append(in.Live, Generation{ID: "oldest", Role: Retiring, Protocols: []uint32{3}})
		}, Decision{Refuse, IncompatibleProtocols, 0}},
		{"no live generations", func(in *Input) { in.Live = nil }, Decision{Refuse, InvalidInventory, 0}},
		{"no primary", func(in *Input) { in.Live = []Generation{older} }, Decision{Refuse, InvalidInventory, 0}},
		{"two primaries", func(in *Input) { in.Live[1].Role = Primary }, Decision{Refuse, InvalidInventory, 0}},
		{"unknown role", func(in *Input) { in.Live[1].Role = "staged" }, Decision{Refuse, InvalidInventory, 0}},
		{"empty identity", func(in *Input) { in.Live[0].ID = "" }, Decision{Refuse, InvalidInventory, 0}},
		{"duplicate identity", func(in *Input) { in.Live[1].ID = primary.ID }, Decision{Refuse, InvalidInventory, 0}},
		{"unknown installation", func(in *Input) { in.Installation = "" }, Decision{Refuse, InvalidInventory, 0}},
		{"unknown work", func(in *Input) { in.Work = []WorkKind{"future_kind"} }, Decision{Refuse, InvalidInventory, 0}},
		{"zero candidate protocol", func(in *Input) { in.CandidateProtocols = []uint32{0, 1} }, Decision{Refuse, InvalidInventory, 0}},
		{"zero live protocol", func(in *Input) { in.Live[1].Protocols = []uint32{0, 1} }, Decision{Refuse, InvalidInventory, 0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Fresh slices prevent one matrix row from changing another's inputs.
			in := Input{
				Installation: PlainMac, CandidateProtocols: []uint32{1},
				Live: []Generation{primary, older},
				Work: []WorkKind{ProcessOneshot, RestartableProcessOneshot, ProcessService},
			}
			if test.change != nil {
				test.change(&in)
			}
			before := cloneInput(in)
			if got := Decide(in); got != test.want {
				t.Fatalf("Decide() = %+v, want %+v", got, test.want)
			}
			if !reflect.DeepEqual(in, before) {
				t.Fatal("Decide changed its input")
			}
		})
	}
}

func cloneInput(in Input) Input {
	copy := in
	copy.CandidateProtocols = slices.Clone(in.CandidateProtocols)
	copy.Work = slices.Clone(in.Work)
	copy.Live = slices.Clone(in.Live)
	for i := range copy.Live {
		copy.Live[i].Protocols = slices.Clone(in.Live[i].Protocols)
	}
	return copy
}
