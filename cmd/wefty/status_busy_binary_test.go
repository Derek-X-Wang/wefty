package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestStatusBusyAndDeadFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, scenario := range []string{"busy", "dead", "no configured slots"} {
		t.Run(scenario, func(t *testing.T) {
			node := readyNode("busy-node")
			node.State = contract.NodeAlive
			if scenario == "dead" {
				node.State = contract.NodeDead
			}
			if scenario == "no configured slots" {
				node.MaxOneshotSlots = 0
			}
			node.OneshotOccupancy = node.MaxOneshotSlots
			address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/nodes":
					_ = json.NewEncoder(w).Encode(l1.NodeList{Nodes: []l1.Node{node}})
				default:
					_ = json.NewEncoder(w).Encode(map[string]any{"runs": []any{}})
				}
			})
			profilePath := filepath.Join(t.TempDir(), "profile.json")
			if err := os.WriteFile(profilePath, []byte(`{"version":1,"fabric":"plain"}`), 0600); err != nil {
				t.Fatal(err)
			}
			code, output := runWefty(t, binary, 30*time.Second, "--profile="+profilePath, "--node-config="+filepath.Join(t.TempDir(), "absent.json"), "--l1", address, "--l3", address, "status")
			if scenario == "busy" {
				if code != 0 || !strings.HasPrefix(output, "ready\n") || !strings.Contains(output, "busy: all one-shot slots in use; 0 runs waiting; a new run waits in line") {
					t.Fatalf("busy exit=%d output=%s", code, output)
				}
			} else {
				want := "no node is alive"
				if scenario == "no configured slots" {
					want = "no alive, claiming node has one-shot slots configured"
				}
				if code != 12 || !strings.Contains(output, want) {
					t.Fatalf("%s exit=%d output=%s", scenario, code, output)
				}
			}
		})
	}
}
