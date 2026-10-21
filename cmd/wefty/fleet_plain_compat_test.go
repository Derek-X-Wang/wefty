package main

import (
	"bytes"
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

// These bytes were captured on slice 6a's unmodified base, 33e89d33.
func TestFleetPlainHumanCompatibility(t *testing.T) {
	t.Run("status", func(t *testing.T) {
		status := clusterStatus{Ready: true, Verdict: "ready", Identity: fabricStatus{Kind: identityMachine, Detail: "machine principal"}, Services: []serviceStatus{{Name: "L1", Endpoint: "control", Reachable: true}, {Name: "L3", Endpoint: "ledger", Reachable: true}}, Nodes: []nodeStatus{{NodeID: "plain-node", State: contract.NodeAlive, ClaimsEnabled: true, Kinds: []string{"process"}, FreeOneshot: 4, TotalOneshot: 4, FreeService: 2, TotalService: 2}}, Capacity: map[string]int{"process": 4, "oci": 0}}
		var out bytes.Buffer
		if err := writeStatus(&out, status); err != nil {
			t.Fatal(err)
		}
		assertPlainBaseBytes(t, "plain-status-base.txt", out.String())
	})
	t.Run("doctor", func(t *testing.T) {
		f := newPlainBinaryFixture(t, buildPlainInstallCLI(t))
		f.setup.setup(t, setupRequest{})
		mustWrite(t, filepath.Join(f.setup.home, setupFolders.config, "profile.json"), "{")
		code, out, _ := f.run(t, "node", "doctor")
		if code != 12 {
			t.Fatalf("exit=%d output=%s", code, out)
		}
		out = strings.ReplaceAll(out, f.setup.home, "<HOME>")
		assertPlainBaseBytes(t, "plain-doctor-base.txt", out)
	})
}

func assertPlainBaseBytes(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("plain output changed from base capture\nwant:\n%s\ngot:\n%s", want, got)
	}
}

func TestFleetPlainStatusBinaryCompatibility(t *testing.T) {
	binary := buildWefty(t)
	node := readyNode("plain-node")
	node.AgentVersion = "base-version"
	node.ConnectHost = "plain-node.example.test"
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/nodes" {
			_ = json.NewEncoder(w).Encode(l1.NodeList{Nodes: []l1.Node{node}})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{"runs": []any{}})
		}
	})
	home := t.TempDir()
	profile := filepath.Join(home, "profile.json")
	mustWrite(t, profile, `{"version":1,"fabric":"plain"}`)
	code, out := runWeftyWithEnv(t, binary, 10*time.Second, []string{"HOME=" + home}, "--profile="+profile, "--node-config="+filepath.Join(home, "absent.json"), "--l1="+address, "--l3="+address, "status")
	if code != 0 {
		t.Fatalf("plain status exit=%d output=%s", code, out)
	}
	out = strings.ReplaceAll(out, address, "<ENDPOINT>")
	out = strings.ReplaceAll(out, home, "<HOME>")
	assertPlainBaseBytes(t, "plain-status-binary-base.txt", out)
}

func TestFleetPlainMultiNodeStatusBinaryCompatibility(t *testing.T) {
	binary := buildWefty(t)
	a, b := readyNode("plain-a"), readyNode("plain-b")
	a.AgentVersion, b.AgentVersion = "base-version", "base-version"
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/nodes" {
			_ = json.NewEncoder(w).Encode(l1.NodeList{Nodes: []l1.Node{a, b}})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{"runs": []any{}})
		}
	})
	home := t.TempDir()
	profile := filepath.Join(home, "profile.json")
	mustWrite(t, profile, `{"version":1,"fabric":"plain"}`)
	code, out := runWeftyWithEnv(t, binary, 10*time.Second, []string{"HOME=" + home}, "--profile="+profile, "--node-config="+filepath.Join(home, "absent.json"), "--l1="+address, "--l3="+address, "status")
	if code != 0 || strings.Contains(out, "NETWORK NAME") {
		t.Fatalf("legacy multi-node text changed: exit=%d output=%s", code, out)
	}
	out = strings.ReplaceAll(out, address, "<ENDPOINT>")
	out = strings.ReplaceAll(out, home, "<HOME>")
	assertPlainBaseBytes(t, "plain-multi-node-status-binary-base.txt", out)
}
