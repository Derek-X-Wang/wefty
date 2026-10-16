package main

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func TestStatusThisMachineLoad(t *testing.T) {
	for _, tc := range []struct {
		name        string
		occupied    int
		load, limit float64
		err         error
		holding     bool
	}{
		{name: "holding", occupied: 1, load: 18, limit: 18, holding: true},
		{name: "below", occupied: 1, load: 17, limit: 18},
		{name: "first", load: 99, limit: 18},
		{name: "disabled", occupied: 1, load: 99, limit: 0},
		{name: "unreadable", occupied: 1, limit: 18, err: errors.New("unreadable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newStatusHarness(t)
			node := readyNode("local")
			node.AuthoritativeTags = []string{"wefty:node:local"}
			node.OneshotOccupancy = tc.occupied
			h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
			clients := h.clients()
			clients.nodeConfigPath = filepath.Join(t.TempDir(), "node.json")
			if err := ocicontrol.WriteInstalledConfig(clients.nodeConfigPath, ocicontrol.InstalledConfig{Version: 1, NodeID: "local"}); err != nil {
				t.Fatal(err)
			}
			clients.oneshotLoadLimit = &tc.limit
			clients.readLoad = func() (float64, error) { return tc.load, tc.err }
			status := collectStatus(t.Context(), clients)
			if status.LocalLoad == nil || status.LocalLoad.Limit != tc.limit || status.LocalLoad.Holding != tc.holding {
				t.Fatalf("local load=%+v", status.LocalLoad)
			}
			if tc.err == nil && (status.LocalLoad.Average == nil || *status.LocalLoad.Average != tc.load) {
				t.Fatalf("average=%v", status.LocalLoad.Average)
			}
			var out bytes.Buffer
			if err := writeStatus(&out, status); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "this machine: local") || !strings.Contains(out.String(), "; load ") || !strings.Contains(out.String(), "; limit ") || strings.Contains(out.String(), "holding new work: machine busy") != tc.holding {
				t.Fatal(out.String())
			}
			if tc.err != nil && !strings.Contains(out.String(), "load unavailable") {
				t.Fatal(out.String())
			}
		})
	}
}

func TestBusyCountUsesExistingCompletePageOrOmitsCount(t *testing.T) {
	for _, cursor := range []string{"", "another-page"} {
		t.Run(cursor, func(t *testing.T) {
			h := newStatusHarness(t)
			node := readyNode("busy")
			node.OneshotOccupancy = node.MaxOneshotSlots
			h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
			h.queued = l3.RunListPage{Runs: []l3.RunSummary{{RunID: "waiting", Status: contract.RunQueued}}, NextCursor: cursor}
			status := collectStatus(t.Context(), h.clients())
			if !status.Ready || !strings.HasPrefix(status.Busy, "busy:") {
				t.Fatalf("status=%+v", status)
			}
			if cursor == "" {
				if status.WaitingRuns == nil || *status.WaitingRuns != 1 || status.Busy != "busy: all one-shot slots in use; 1 runs waiting; a new run waits in line" {
					t.Fatalf("count=%v busy=%s", status.WaitingRuns, status.Busy)
				}
			} else if status.WaitingRuns != nil || status.Busy != "busy: all one-shot slots in use; a new run waits in line" {
				t.Fatalf("count=%v busy=%s", status.WaitingRuns, status.Busy)
			}
		})
	}
}
