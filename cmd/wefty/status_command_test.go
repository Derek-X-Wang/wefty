package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
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
	// personStatus and personCode refuse the person protocol the way a real L1
	// refuses a machine principal.
	personStatus int
	personCode   contract.ErrorCode
	l1Down       bool
	l3Down       bool
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
				if h.personStatus != 0 {
					w.WriteHeader(h.personStatus)
					_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
						Code: h.personCode, Message: "machine principals cannot use person protocols"}})
					return
				}
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
	if status.Identity.Kind != identityPerson || status.Identity.UserID != "alice" ||
		status.Identity.DeviceID != "laptop" {
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
		code      string
	}{
		"no node is registered": {
			configure: func(h *statusHarness) { h.nodes = l1.NodeList{} },
			want:      "no node is registered",
			code:      "no_node_registered",
		},
		"no node is alive": {
			configure: func(h *statusHarness) {
				node := readyNode("node-a")
				node.State = contract.NodeDead
				h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
			},
			want: "no node is alive",
			code: "no_alive_node",
		},
		"claims are disabled everywhere": {
			configure: func(h *statusHarness) {
				node := readyNode("node-a")
				node.ClaimsEnabled = false
				h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
			},
			want: "every alive node has claims disabled",
			code: "claims_disabled",
		},
		"every slot is taken": {
			configure: func(h *statusHarness) {
				node := readyNode("node-a")
				node.OneshotOccupancy = node.MaxOneshotSlots
				h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
			},
			want: "no node has a free one-shot slot",
			code: "no_free_slot",
		},
		"L1 is down": {
			configure: func(h *statusHarness) { h.l1Down = true; h.nodes = l1.NodeList{Nodes: []l1.Node{readyNode("node-a")}} },
			want:      "L1 at wefty://control-plane is not reachable",
			code:      "l1_unreachable",
		},
		"L3 is down": {
			configure: func(h *statusHarness) { h.l3Down = true; h.nodes = l1.NodeList{Nodes: []l1.Node{readyNode("node-a")}} },
			want:      "L3 at wefty://run-ledger is not reachable",
			code:      "l3_unreachable",
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
			// The sentence a person reads and the code a script branches on
			// are built from the same reason, so they cannot disagree.
			var codes []string
			for _, reason := range status.Reasons {
				codes = append(codes, reason.Code)
			}
			if !slices.Contains(codes, test.code) {
				t.Fatalf("reason codes = %v, want %q", codes, test.code)
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
	// The budget that was asked for, not merely some budget: a command that
	// honoured only its default would have passed the old assertion.
	if elapsed > 3*time.Second {
		t.Fatalf("status took %s against a requested 600ms budget", elapsed)
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

// TestAMachinePrincipalIsReportedAsOneRatherThanAsAnError is what CI found. A
// machine principal is the ordinary caller for this command, and the person
// protocols are closed to it by design; surfacing the refusal's protocol error
// made a working cluster read as though something were wrong.
func TestAMachinePrincipalIsReportedAsOneRatherThanAsAnError(t *testing.T) {
	t.Parallel()

	h := newStatusHarness(t)
	h.nodes = l1.NodeList{Nodes: []l1.Node{readyNode("node-a")}}
	h.personStatus = http.StatusForbidden
	h.personCode = contract.ErrorPrincipalForbidden

	var out, errOut bytes.Buffer
	if err := executeStatus(t.Context(), h.clients(), true, nil, &out, &errOut); err != nil {
		t.Fatalf("a machine principal could not read status: %v\n%s", err, out.String())
	}
	var status clusterStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if !status.Ready {
		t.Fatalf("a machine principal saw a not-ready cluster: %#v", status)
	}
	if status.Identity.Kind != identityMachine {
		t.Fatalf("identity kind = %q, want %q", status.Identity.Kind, identityMachine)
	}
	// The protocol error must not be the thing a reader sees.
	if strings.Contains(status.Identity.Detail, "principal_forbidden") ||
		strings.Contains(status.Identity.Detail, "cannot use person protocols") {
		t.Fatalf("the identity leaks a protocol error: %q", status.Identity.Detail)
	}
	if !strings.Contains(status.Identity.Detail, "machine principal") {
		t.Fatalf("the identity does not say what the caller is: %q", status.Identity.Detail)
	}

	var human, humanErr bytes.Buffer
	if err := executeStatus(t.Context(), h.clients(), false, nil, &human, &humanErr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(human.String(), "identity: this caller is a machine principal") {
		t.Fatalf("the human line does not read as a fact:\n%s", human.String())
	}
}

// TestAnUnreachableControlPlaneLeavesTheIdentityUnknown keeps "we could not
// ask" apart from "there is no person here".
func TestAnUnreachableControlPlaneLeavesTheIdentityUnknown(t *testing.T) {
	t.Parallel()

	h := newStatusHarness(t)
	h.l1Down = true
	var out, errOut bytes.Buffer
	_ = executeStatus(t.Context(), h.clients(), true, nil, &out, &errOut)
	var status clusterStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Identity.Kind != identityUnknown {
		t.Fatalf("identity kind = %q, want %q", status.Identity.Kind, identityUnknown)
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

// TestANodeThatCanRunNothingIsNotCapacity is the false-ready this command must
// never produce. L1's claim query requires kind:<kind>, so a node advertising
// no executable kind can never be claimed however many slots it has free.
func TestANodeThatCanRunNothingIsNotCapacity(t *testing.T) {
	t.Parallel()

	h := newStatusHarness(t)
	idle := readyNode("node-idle")
	idle.Capabilities = map[string]bool{"cgroup_v2": true}
	h.nodes = l1.NodeList{Nodes: []l1.Node{idle}}

	var out, errOut bytes.Buffer
	err := executeStatus(t.Context(), h.clients(), true, nil, &out, &errOut)
	if err == nil {
		t.Fatalf("a node that can run nothing was reported as ready:\n%s", out.String())
	}
	var status clusterStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Nodes[0].AcceptsOneshot {
		t.Fatalf("a node with no executable kind accepts work: %#v", status.Nodes[0])
	}
	if !strings.Contains(status.Verdict, "executable kind") {
		t.Fatalf("verdict = %q", status.Verdict)
	}
	if status.Capacity["process"] != 0 || status.Capacity["oci"] != 0 {
		t.Fatalf("capacity = %v", status.Capacity)
	}
}

// TestAnUnknownCapabilityIsNeverAFalsePositive keeps this command conservative
// about vocabulary it does not know: an unrecognised capability adds no
// capacity, and it does not take away the capacity a known one provides.
func TestAnUnknownCapabilityIsNeverAFalsePositive(t *testing.T) {
	t.Parallel()

	h := newStatusHarness(t)
	exotic := readyNode("node-exotic")
	exotic.Capabilities = map[string]bool{"kind:teleport": true, "some_future_thing": true}
	mixed := readyNode("node-mixed")
	mixed.Capabilities = map[string]bool{"kind:process": true, "kind:teleport": true}
	h.nodes = l1.NodeList{Nodes: []l1.Node{exotic, mixed}}

	var out, errOut bytes.Buffer
	if err := executeStatus(t.Context(), h.clients(), true, nil, &out, &errOut); err != nil {
		t.Fatalf("a node with a known kind did not make the cluster ready: %v\n%s", err, out.String())
	}
	var status clusterStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	byNode := map[string]nodeStatus{}
	for _, node := range status.Nodes {
		byNode[node.NodeID] = node
	}
	if byNode["node-exotic"].AcceptsOneshot {
		t.Fatal("a node advertising only an unknown kind was counted as capacity")
	}
	if !byNode["node-mixed"].AcceptsOneshot {
		t.Fatal("an unknown capability took away a known one")
	}
	// Only the node that can actually run a process contributes to it.
	if status.Capacity["process"] != byNode["node-mixed"].FreeOneshot {
		t.Fatalf("capacity = %v", status.Capacity)
	}
	if status.Capacity["oci"] != 0 {
		t.Fatalf("oci capacity was invented: %v", status.Capacity)
	}
}

// TestCapacityIsReportedPerKind keeps "ready" from meaning the same thing for
// two different kinds of job. A process-only fleet is not ready for OCI work.
func TestCapacityIsReportedPerKind(t *testing.T) {
	t.Parallel()

	h := newStatusHarness(t)
	processOnly := readyNode("node-process")
	processOnly.Capabilities = map[string]bool{"kind:process": true}
	h.nodes = l1.NodeList{Nodes: []l1.Node{processOnly}}

	// A process-only fleet is ready: it can take work. The documented
	// single-machine setup is exactly this, and telling an agent to stop there
	// would be wrong. What it cannot do is a limitation, reported and not
	// blocking.
	var out, errOut bytes.Buffer
	if err := executeStatus(t.Context(), h.clients(), true, nil, &out, &errOut); err != nil {
		t.Fatalf("a process-only fleet was reported as not ready: %v\n%s", err, out.String())
	}
	var status clusterStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Capacity["process"] != 3 || status.Capacity["oci"] != 0 {
		t.Fatalf("capacity = %v", status.Capacity)
	}
	if len(status.Reasons) != 0 {
		t.Fatalf("a ready cluster reported blocking reasons: %#v", status.Reasons)
	}
	var codes []string
	for _, limitation := range status.Limitations {
		codes = append(codes, limitation.Code)
	}
	if !slices.Contains(codes, "no_free_slot:oci") {
		t.Fatalf("limitations = %v, want the kind that cannot run named", codes)
	}
	if slices.Contains(codes, "no_free_slot:process") {
		t.Fatalf("a kind with free slots was reported short: %v", codes)
	}
	// And a person reading the table is told, without being told to stop.
	var human, humanErr bytes.Buffer
	if err := executeStatus(t.Context(), h.clients(), false, nil, &human, &humanErr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(human.String(), "note: no node with a free one-shot slot can run kind=oci") {
		t.Fatalf("the limitation is not shown:\n%s", human.String())
	}
}

// TestEveryReasonIsReportedAtOnce keeps a person from fixing one thing, running
// the command again, and finding another.
func TestEveryReasonIsReportedAtOnce(t *testing.T) {
	t.Parallel()

	h := newStatusHarness(t)
	h.l3Down = true
	h.nodes = l1.NodeList{}

	var out, errOut bytes.Buffer
	if err := executeStatus(t.Context(), h.clients(), true, nil, &out, &errOut); err == nil {
		t.Fatal("a cluster with two problems reported ready")
	}
	var status clusterStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	var codes []string
	for _, reason := range status.Reasons {
		codes = append(codes, reason.Code)
	}
	if !slices.Contains(codes, "l3_unreachable") || !slices.Contains(codes, "no_node_registered") {
		t.Fatalf("reasons = %v, want both problems", codes)
	}
	// And the sentence says both, built from the same reasons.
	for _, reason := range status.Reasons {
		if !strings.Contains(status.Verdict, reason.Detail) {
			t.Fatalf("verdict %q omits reason %q", status.Verdict, reason.Detail)
		}
	}
}

// TestNodeReasonsAreComputedOverEligibleCandidates keeps one node's problem from
// being reported as another's: a dead node is not evidence that claims are off.
func TestNodeReasonsAreComputedOverEligibleCandidates(t *testing.T) {
	t.Parallel()

	h := newStatusHarness(t)
	dead := readyNode("node-dead")
	dead.State = contract.NodeDead
	dead.ClaimsEnabled = false
	live := readyNode("node-live")
	live.OneshotOccupancy = live.MaxOneshotSlots
	h.nodes = l1.NodeList{Nodes: []l1.Node{dead, live}}

	var out, errOut bytes.Buffer
	if err := executeStatus(t.Context(), h.clients(), true, nil, &out, &errOut); err == nil {
		t.Fatal("a full cluster reported ready")
	}
	var status clusterStatus
	if err := json.Unmarshal(out.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	var codes []string
	for _, reason := range status.Reasons {
		codes = append(codes, reason.Code)
	}
	// The live node has claims on, so "claims disabled" would be about the
	// dead one -- a node that was never a candidate.
	if slices.Contains(codes, "claims_disabled") || slices.Contains(codes, "no_alive_node") {
		t.Fatalf("reasons = %v, computed over nodes that were never candidates", codes)
	}
	if !slices.Contains(codes, "no_free_slot") {
		t.Fatalf("reasons = %v, want the real reason", codes)
	}
}
