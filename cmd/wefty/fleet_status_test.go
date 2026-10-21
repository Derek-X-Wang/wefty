package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestStatusFleetPlacementAcrossPages(t *testing.T) {
	nodeA, nodeB, nodeC := readyNode("node-a"), readyNode("node-b"), readyNode("node-c")
	nodeA.ConnectHost = "wefty-node-a.example.test"
	nodeA.AutomaticallyAdmitted = true
	nodeA.AgentVersion = "version-a"
	nodeA.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "job-a", Class: contract.JobClassOneShot}, {JobID: "job-b", Class: contract.JobClassService}}
	nodeB.AgentVersion = "version-b"
	nodeB.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "job-c", Class: contract.JobClassOneShot}, {JobID: "job-d", Class: contract.JobClassOneShot}}
	nodeC.State = contract.NodeDead
	nodeC.ClaimsEnabled = false
	requests := 0
	rt := fleetRoundTripper(func(r *http.Request) (*http.Response, error) {
		var payload any
		switch r.URL.Path {
		case "/v1/nodes":
			requests++
			switch r.URL.Query().Get("cursor") {
			case "":
				payload = l1.NodeList{Nodes: []l1.Node{nodeC, nodeA}, NextCursor: "page-2"}
			case "page-2":
				payload = l1.NodeList{Nodes: []l1.Node{nodeB}}
			default:
				t.Fatal("unexpected page")
			}
		case "/v1/whoami":
			payload = map[string]any{}
		default:
			payload = map[string]any{"runs": []any{}}
		}
		b, _ := json.Marshal(payload)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(b))}, nil
	})
	clients := &apiClients{l1: &apiClient{name: "L1", address: "control", client: &http.Client{Transport: rt}}, l3: &apiClient{name: "L3", address: "ledger", client: &http.Client{Transport: rt}}, clientIdentityID: "saved-client"}
	var out bytes.Buffer
	if err := executeStatus(t.Context(), clients, true, nil, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	var report map[string]any
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	nodes := report["nodes"].([]any)
	if len(nodes) != 3 || requests != 2 {
		t.Fatalf("fleet pages nodes=%v reads=%d", nodes, requests)
	}
	first := nodes[0].(map[string]any)
	counts, ok := first["active_attempts_by_class"].(map[string]any)
	if first["connect_host"] != "wefty-node-a.example.test" || first["automatically_admitted"] != true || !ok || counts["one-shot"] != float64(1) || counts["service"] != float64(1) {
		t.Fatalf("placement JSON=%s", &out)
	}
	for _, key := range []string{"node_id", "state", "claims_enabled", "kinds", "free_oneshot_slots", "total_oneshot_slots", "free_service_slots", "total_service_slots", "accepts_oneshot", "agent_version"} {
		if _, exists := first[key]; !exists {
			t.Errorf("existing field removed: %s", key)
		}
	}
	out.Reset()
	requests = 0
	if err := executeStatus(t.Context(), clients, false, nil, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"NETWORK NAME", "AUTO-ADMITTED", "ACTIVE ONE-SHOT", "ACTIVE SERVICE", "wefty-node-a.example.test", "node-c", "version-a"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %s: %s", want, &out)
		}
	}
}

func TestStatusFleetPlacementRealBinary(t *testing.T) {
	binary := buildWefty(t)
	a, b := readyNode("node-a"), readyNode("node-b")
	a.ConnectHost = "wefty-node-a.example.test"
	a.AutomaticallyAdmitted = true
	a.AgentVersion = "build-a"
	a.ActiveAttempts = []l1.NodeActiveAttempt{{Class: contract.JobClassOneShot}, {Class: contract.JobClassService}}
	b.State = contract.NodeDead
	b.ClaimsEnabled = false
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/nodes" {
			_ = json.NewEncoder(w).Encode(l1.NodeList{Nodes: []l1.Node{a, b}})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{"runs": []any{}})
		}
	})
	home := newFleetTestHome(t)
	t.Setenv("HOME", home)
	profile := filepath.Join(home, "profile.json")
	p := connprofile.Profile{Version: 1, Fabric: "plain", L1Address: address, L3Address: address, Participants: map[string]connprofile.Participant{
		"client": {Name: "wefty://client/test", NodeID: "machine-client", StateDirectory: savedChoiceState(t, "client")},
	}}
	if err := connprofile.Write(profile, p); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(home, "client", "s")
	t.Setenv(local.SocketEnvironment, socket)
	startClientServiceForTest(t, binary, profile, socket)
	for _, jsonMode := range []bool{false, true} {
		args := []string{"--profile=" + profile, "--node-config=" + filepath.Join(home, "absent.json"), "--l1=" + address, "--l3=" + address, "status"}
		if jsonMode {
			args = append([]string{"--json"}, args...)
		}
		code, out := runWeftyWithEnv(t, binary, 10*time.Second, []string{"HOME=" + home}, args...)
		if code != 0 {
			t.Fatalf("fleet status exit=%d output=%s", code, out)
		}
		if jsonMode {
			var report struct {
				Nodes []struct {
					NodeID                string         `json:"node_id"`
					ConnectHost           string         `json:"connect_host"`
					AutomaticallyAdmitted bool           `json:"automatically_admitted"`
					Counts                map[string]int `json:"active_attempts_by_class"`
				} `json:"nodes"`
			}
			if err := json.Unmarshal([]byte(out), &report); err != nil || len(report.Nodes) != 2 || report.Nodes[0].ConnectHost != "wefty-node-a.example.test" || !report.Nodes[0].AutomaticallyAdmitted || report.Nodes[0].Counts["one-shot"] != 1 || report.Nodes[0].Counts["service"] != 1 {
				t.Fatalf("fleet JSON=%s err=%v", out, err)
			}
		} else {
			for _, want := range []string{"NETWORK NAME", "AUTO-ADMITTED", "ACTIVE ONE-SHOT", "wefty-node-a.example.test", "node-b", "build-a"} {
				if !strings.Contains(out, want) {
					t.Errorf("fleet text missing %s: %s", want, out)
				}
			}
		}
	}
}
