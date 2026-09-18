package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// statusHarness is a control plane that answers however a test needs it to,
// including by never answering at all.
type statusHarness struct {
	t      *testing.T
	stall  chan struct{}
	nodes  l1.NodeList
	person l1.AuthenticatedPerson
	l1Down bool
	l3Down bool
}

func newStatusHarness(t *testing.T) *statusHarness {
	t.Helper()
	return &statusHarness{t: t, person: l1.AuthenticatedPerson{
		FabricID: "fabric-1", UserID: "alice", DeviceID: "laptop", SeenAt: time.Now().UTC()}}
}

func (h *statusHarness) clients() *apiClients {
	h.t.Helper()
	handler := func(down bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if h.stall != nil {
				select {
				case <-h.stall:
				case <-r.Context().Done():
				}
				return
			}
			if down {
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
					Code: contract.ErrorInternal, Message: "not today", Retryable: true}})
				return
			}
			w.Header().Set("Content-Type", "application/json")
			switch {
			case strings.HasPrefix(r.URL.Path, "/v1/nodes"):
				_ = json.NewEncoder(w).Encode(h.nodes)
			case strings.HasPrefix(r.URL.Path, "/v1/whoami"):
				_ = json.NewEncoder(w).Encode(h.person)
			default:
				_ = json.NewEncoder(w).Encode(map[string]any{"runs": []any{}})
			}
		}
	}
	l1Server := httptest.NewServer(handler(h.l1Down))
	l3Server := httptest.NewServer(handler(h.l3Down))
	h.t.Cleanup(l1Server.Close)
	h.t.Cleanup(l3Server.Close)
	return &apiClients{
		l1: statusClient(h.t, "L1", "wefty://control-plane", l1Server),
		l3: statusClient(h.t, "L3", "wefty://run-ledger", l3Server),
	}
}

func statusClient(t *testing.T, name, address string, server *httptest.Server) *apiClient {
	t.Helper()
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return &apiClient{name: name, flag: strings.ToLower(name), address: address, client: &http.Client{
		Transport: &redirectingTransport{target: target, inner: server.Client().Transport},
	}}
}

func readyNode(nodeID string) l1.Node {
	node := l1.Node{
		State: contract.NodeAlive, ClaimsEnabled: true,
		MaxOneshotSlots: 4, OneshotOccupancy: 1,
		MaxServiceSlots: 2, ServiceOccupancy: 2,
	}
	node.NodeID = nodeID
	node.Capabilities = map[string]bool{"kind:process": true, "kind:oci": true, "cgroup_v2": true}
	return node
}

// TestStatusReportsReadyWithCapabilitiesAndSlots is the ordinary answer, and
// the one an agent reads before submitting anything.
func TestStatusReportsReadyWithCapabilitiesAndSlots(t *testing.T) {
	t.Parallel()

	h := newStatusHarness(t)
	h.nodes = l1.NodeList{Nodes: []l1.Node{readyNode("node-b"), readyNode("node-a")}}

	var out, errOut bytes.Buffer
	if err := executeStatus(t.Context(), h.clients(), true, nil, &out, &errOut); err != nil {
		t.Fatalf("a ready cluster reported: %v", err)
	}
	var status clusterStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil {
		t.Fatalf("status --json is not JSON: %v\n%s", err, out.String())
	}
	if !status.Ready || status.Verdict != "ready" {
		t.Fatalf("verdict = %q ready=%t", status.Verdict, status.Ready)
	}
	if status.Identity.UserID != "alice" || status.Identity.DeviceID != "laptop" {
		t.Fatalf("identity = %#v", status.Identity)
	}
	// The endpoints it resolved, not the ones it was handed after rewriting.
	if len(status.Services) != 2 || status.Services[0].Endpoint != "wefty://control-plane" ||
		status.Services[1].Endpoint != "wefty://run-ledger" {
		t.Fatalf("services = %#v", status.Services)
	}
	for _, service := range status.Services {
		if !service.Reachable || service.Detail != "" {
			t.Fatalf("service %s = %#v", service.Name, service)
		}
	}
	if len(status.Nodes) != 2 || status.Nodes[0].NodeID != "node-a" {
		t.Fatalf("nodes are not listed in a stable order: %#v", status.Nodes)
	}
	node := status.Nodes[0]
	if strings.Join(node.Kinds, ",") != "oci,process" {
		t.Fatalf("kinds = %v; the capability vocabulary is not being translated", node.Kinds)
	}
	// Free is policy minus occupancy, and a full class is zero free rather than
	// absent: "2/2 busy" and "no service slots configured" are different facts.
	if node.FreeOneshot != 3 || node.TotalOneshot != 4 || node.FreeService != 0 || node.TotalService != 2 {
		t.Fatalf("slots = %#v", node)
	}
	if !node.AcceptsOneshot {
		t.Fatal("a live node with free slots does not accept work")
	}

	var human, humanErr bytes.Buffer
	if err := executeStatus(t.Context(), h.clients(), false, nil, &human, &humanErr); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"ready", "alice on laptop", "SERVICE", "NODE", "oci,process", "3/4 free"} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("human status is missing %q:\n%s", want, human.String())
		}
	}
}

// TestStatusSaysWhatIsMissing walks the reasons a cluster cannot take work.
// Each one is a different thing to go and fix, so each gets its own sentence.
func TestStatusSaysWhatIsMissing(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		configure func(*statusHarness)
		want      string
	}{
		"no node is registered": {
			configure: func(h *statusHarness) { h.nodes = l1.NodeList{} },
			want:      "no node is registered",
		},
		"no node is alive": {
			configure: func(h *statusHarness) {
				node := readyNode("node-a")
				node.State = contract.NodeDead
				h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
			},
			want: "no node is alive",
		},
		"claims are disabled everywhere": {
			configure: func(h *statusHarness) {
				node := readyNode("node-a")
				node.ClaimsEnabled = false
				h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
			},
			want: "every node has claims disabled",
		},
		"every slot is taken": {
			configure: func(h *statusHarness) {
				node := readyNode("node-a")
				node.OneshotOccupancy = node.MaxOneshotSlots
				h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
			},
			want: "no node has a free one-shot slot",
		},
		"L1 is down": {
			configure: func(h *statusHarness) { h.l1Down = true; h.nodes = l1.NodeList{Nodes: []l1.Node{readyNode("node-a")}} },
			want:      "L1 at wefty://control-plane is not reachable",
		},
		"L3 is down": {
			configure: func(h *statusHarness) { h.l3Down = true; h.nodes = l1.NodeList{Nodes: []l1.Node{readyNode("node-a")}} },
			want:      "L3 at wefty://run-ledger is not reachable",
		},
	}
	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newStatusHarness(t)
			test.configure(h)
			var out, errOut bytes.Buffer
			err := executeStatus(t.Context(), h.clients(), true, nil, &out, &errOut)
			if err == nil {
				t.Fatalf("a cluster that cannot take work reported ready:\n%s", out.String())
			}
			if code := commandExitCode(err); code != exitNotReady {
				t.Fatalf("not ready exited %d, want %d", code, exitNotReady)
			}
			var status clusterStatus
			if err := json.Unmarshal(out.Bytes(), &status); err != nil {
				t.Fatalf("status --json is not JSON: %v\n%s", err, out.String())
			}
			if status.Ready {
				t.Fatalf("status = %#v", status)
			}
			if !strings.HasPrefix(status.Verdict, "not ready: ") || !strings.Contains(status.Verdict, test.want) {
				t.Fatalf("verdict = %q, want it to name %q", status.Verdict, test.want)
			}
		})
	}
}

// TestStatusAnswersWithinItsBudgetWhenNothingAnswers is the rule that makes this
// command worth running at all: a diagnostic that hangs is the problem it exists
// to diagnose.
func TestStatusAnswersWithinItsBudgetWhenNothingAnswers(t *testing.T) {
	t.Parallel()

	h := newStatusHarness(t)
	h.stall = make(chan struct{})
	t.Cleanup(func() { close(h.stall) })

	var out, errOut bytes.Buffer
	started := time.Now()
	err := executeStatus(t.Context(), h.clients(), true, []string{"--timeout", "600ms"}, &out, &errOut)
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("a cluster that never answered reported ready")
	}
	if code := commandExitCode(err); code != exitNotReady {
		t.Fatalf("a stalled cluster exited %d, want %d", code, exitNotReady)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("status took %s against a 600ms budget", elapsed)
	}
	var status clusterStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil {
		t.Fatalf("status --json is not JSON: %v\n%s", err, out.String())
	}
	// Both services are reported, not just the first one to fail: one stalled
	// service must not hide the state of the others.
	if len(status.Services) != 2 {
		t.Fatalf("services = %#v", status.Services)
	}
	for _, service := range status.Services {
		if service.Reachable || service.Detail == "" {
			t.Fatalf("service %s did not report why it is unreachable: %#v", service.Name, service)
		}
	}
	if !strings.Contains(status.Verdict, "L1") || !strings.Contains(status.Verdict, "L3") {
		t.Fatalf("verdict = %q, want both services named", status.Verdict)
	}
}

// TestStatusIsReadableWithoutAPersonIdentity keeps the command usable by the
// principal most likely to run it in anger: a machine, mid-script.
func TestStatusIsReadableWithoutAPersonIdentity(t *testing.T) {
	t.Parallel()

	h := newStatusHarness(t)
	h.nodes = l1.NodeList{Nodes: []l1.Node{readyNode("node-a")}}
	h.person = l1.AuthenticatedPerson{}

	var out, errOut bytes.Buffer
	if err := executeStatus(t.Context(), h.clients(), true, nil, &out, &errOut); err != nil {
		t.Fatalf("status refused a caller with no person identity: %v", err)
	}
	var status clusterStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if !status.Ready {
		t.Fatalf("status = %#v", status)
	}
}

// TestStatusRefusesArgumentsItCannotHonour keeps usage mistakes exit 2, apart
// from the cluster's own answer.
func TestStatusRefusesArgumentsItCannotHonour(t *testing.T) {
	t.Parallel()

	h := newStatusHarness(t)
	for _, args := range [][]string{{"extra"}, {"--timeout", "0"}, {"--timeout", "-1s"}} {
		var out, errOut bytes.Buffer
		err := executeStatus(context.Background(), h.clients(), false, args, &out, &errOut)
		if code := commandExitCode(err); code != exitUsage {
			t.Fatalf("status %v exited %d (%v), want %d", args, code, err, exitUsage)
		}
	}
}
