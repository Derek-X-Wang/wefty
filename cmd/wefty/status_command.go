package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// `wefty status` answers one question: can I submit work here right now.
//
// It exists because every other failure mode in this system looks the same from
// the outside -- a submit that hangs, a run that never leaves queued, a logs
// command that says nothing -- and the cause is almost always one of three
// things that a person or an agent could have been told up front: the control
// plane is not running, the ledger is not running, or no node can take the work.
//
// Three rules shape it. It answers within a bounded time even when nothing is
// listening, because a diagnostic that hangs is the problem it is meant to
// diagnose. It re-probes nothing: the agent already observed its own
// capabilities and reported them, and L1 already knows the slot policy and what
// is occupying it, so this reads those facts rather than forming a second
// opinion. And it asks for no authority beyond the person's own Fabric
// identity -- the thing you would have anyway if you were about to submit.

const (
	// statusBudget bounds the whole command. Five seconds is long enough for a
	// loopback or a LAN Fabric to answer and short enough that a person waiting
	// on an unreachable control plane learns so immediately.
	statusBudget = 5 * time.Second
	// statusProbeBudget bounds one probe, so a single stalled service cannot
	// consume the whole budget and leave the others unreported.
	statusProbeBudget = 3 * time.Second
)

// serviceStatus is one control-plane service as this command found it.
type serviceStatus struct {
	Name string `json:"name"`
	// Endpoint is the Fabric address this command resolved and used. Half of
	// "it is not reachable" is where you looked.
	Endpoint  string `json:"endpoint"`
	Reachable bool   `json:"reachable"`
	// Detail is why it is not reachable, or empty when it is.
	Detail string `json:"detail,omitempty"`
}

// nodeStatus is a node's ability to take work, not its whole projection.
type nodeStatus struct {
	NodeID string             `json:"node_id"`
	State  contract.NodeState `json:"state"`
	// ClaimsEnabled is durable operator intent. A node can be perfectly healthy
	// and still be closed for work, and that is a different sentence.
	ClaimsEnabled bool `json:"claims_enabled"`
	// Kinds is what this node can execute: process, oci, computer. The names
	// are the capability vocabulary's, shortened for reading.
	Kinds          []string `json:"kinds"`
	FreeOneshot    int      `json:"free_oneshot_slots"`
	TotalOneshot   int      `json:"total_oneshot_slots"`
	FreeService    int      `json:"free_service_slots"`
	TotalService   int      `json:"total_service_slots"`
	AcceptsOneshot bool     `json:"accepts_oneshot"`
}

// fabricStatus is who this command was, as the control plane sees it.
type fabricStatus struct {
	FabricID string `json:"fabric_id,omitempty"`
	UserID   string `json:"user_id,omitempty"`
	DeviceID string `json:"device_id,omitempty"`
	// Detail explains an identity the control plane would not name -- a machine
	// principal, for instance. It is not a failure: status is readable without
	// a person identity, and says so rather than refusing.
	Detail string `json:"detail,omitempty"`
}

// clusterStatus is the whole answer, and the shape `--json` publishes.
type clusterStatus struct {
	Ready bool `json:"ready"`
	// Verdict is the one line a person reads: "ready", or "not ready: ..."
	// naming what is missing rather than what was checked.
	Verdict   string          `json:"verdict"`
	Identity  fabricStatus    `json:"identity"`
	Services  []serviceStatus `json:"services"`
	Nodes     []nodeStatus    `json:"nodes"`
	CheckedAt time.Time       `json:"checked_at"`
}

// notReadyError carries the verdict out to an exit code. It is not a command
// failure: the command worked, and the answer is no.
type notReadyError struct {
	verdict string
}

func (e *notReadyError) Error() string { return e.verdict }

func executeStatus(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var budget time.Duration
	flags.DurationVar(&budget, "timeout", statusBudget, "give up probing after this long")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError("usage: wefty status [--timeout D]")
	}
	if budget <= 0 {
		return usageError("--timeout must be positive")
	}

	bounded, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	status := collectStatus(bounded, clients)

	if jsonOutput {
		if err := writeJSON(stdout, status); err != nil {
			return err
		}
	} else if err := writeStatus(stdout, status); err != nil {
		return err
	}
	if !status.Ready {
		return &notReadyError{verdict: status.Verdict}
	}
	return nil
}

// collectStatus probes everything at once. Concurrency is not for speed here:
// it is so one unreachable service cannot hide the state of the others, which
// is exactly the case a person runs this command in.
func collectStatus(ctx context.Context, clients *apiClients) clusterStatus {
	status := clusterStatus{CheckedAt: time.Now().UTC()}
	l1Status := serviceStatus{Name: "L1", Endpoint: clients.l1.address}
	l3Status := serviceStatus{Name: "L3", Endpoint: clients.l3.address}

	var nodes l1.NodeList
	var group sync.WaitGroup
	group.Add(3)
	go func() {
		defer group.Done()
		probeCtx, done := context.WithTimeout(ctx, statusProbeBudget)
		defer done()
		listed, err := clients.listNodes(probeCtx)
		if err != nil {
			l1Status.Detail = probeDetail(err)
			return
		}
		l1Status.Reachable = true
		nodes = listed
	}()
	go func() {
		defer group.Done()
		probeCtx, done := context.WithTimeout(ctx, statusProbeBudget)
		defer done()
		if clients.l3.client == nil {
			l3Status.Detail = "no L3 endpoint is configured; this is an L1-only installation"
			return
		}
		if _, err := clients.listRuns(probeCtx, "", 1); err != nil {
			l3Status.Detail = probeDetail(err)
			return
		}
		l3Status.Reachable = true
	}()
	go func() {
		defer group.Done()
		probeCtx, done := context.WithTimeout(ctx, statusProbeBudget)
		defer done()
		person, err := clients.whoAmI(probeCtx)
		if err != nil {
			// A machine principal, or an L1 that cannot be reached. Neither
			// stops this command: the identity line reports what it knows and
			// reachability is the services' business, not the identity's.
			status.Identity.Detail = probeDetail(err)
			return
		}
		status.Identity = fabricStatus{
			FabricID: person.FabricID, UserID: person.UserID, DeviceID: person.DeviceID,
		}
	}()
	group.Wait()

	status.Services = []serviceStatus{l1Status, l3Status}
	status.Nodes = nodeStatuses(nodes)
	status.Ready, status.Verdict = verdictFor(l1Status, l3Status, status.Nodes)
	return status
}

// probeDetail turns a failure into the sentence a reader acts on. A protocol
// error already says what it is; anything else is the transport, and the useful
// shape of that is short.
func probeDetail(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "did not answer within the probe budget"
	}
	var responseErr *apiResponseError
	if errors.As(err, &responseErr) {
		return string(responseErr.APIError.Code) + ": " + responseErr.APIError.Message
	}
	return "not reachable: " + err.Error()
}

// nodeStatuses reduces the operator projection to the question at hand. Free
// slots are the policy minus the occupancy, never negative: an overcommitted
// node has none free, not a negative number of them.
func nodeStatuses(nodes l1.NodeList) []nodeStatus {
	statuses := make([]nodeStatus, 0, len(nodes.Nodes))
	for _, node := range nodes.Nodes {
		status := nodeStatus{
			NodeID: node.NodeID, State: node.State, ClaimsEnabled: node.ClaimsEnabled,
			Kinds:        executionKinds(node.Capabilities),
			TotalOneshot: node.MaxOneshotSlots, TotalService: node.MaxServiceSlots,
			FreeOneshot: freeSlots(node.MaxOneshotSlots, node.OneshotOccupancy),
			FreeService: freeSlots(node.MaxServiceSlots, node.ServiceOccupancy),
		}
		status.AcceptsOneshot = node.State == contract.NodeAlive && node.ClaimsEnabled && status.FreeOneshot > 0
		statuses = append(statuses, status)
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].NodeID < statuses[j].NodeID })
	return statuses
}

func freeSlots(total, occupied int) int {
	if free := total - occupied; free > 0 {
		return free
	}
	return 0
}

// executionKinds names what a node can run, in the vocabulary a person uses
// when submitting rather than the capability strings the agent reports.
func executionKinds(capabilities map[string]bool) []string {
	kinds := make([]string, 0, 3)
	for capability, short := range map[string]string{
		"kind:process": "process",
		"kind:oci":     "oci",
		"computer":     "computer",
	} {
		if capabilities[capability] {
			kinds = append(kinds, short)
		}
	}
	sort.Strings(kinds)
	return kinds
}

// verdictFor is the whole judgement, and it names what is missing rather than
// what was checked. The order is the order a person would fix things in.
func verdictFor(l1Status, l3Status serviceStatus, nodes []nodeStatus) (bool, string) {
	var missing []string
	if !l1Status.Reachable {
		missing = append(missing, "L1 at "+endpointOrNone(l1Status.Endpoint)+" is not reachable")
	}
	if !l3Status.Reachable {
		missing = append(missing, "L3 at "+endpointOrNone(l3Status.Endpoint)+" is not reachable")
	}
	if l1Status.Reachable {
		switch {
		case len(nodes) == 0:
			missing = append(missing, "no node is registered")
		case !slicesAny(nodes, func(node nodeStatus) bool { return node.State == contract.NodeAlive }):
			missing = append(missing, "no node is alive")
		case !slicesAny(nodes, func(node nodeStatus) bool { return node.ClaimsEnabled }):
			missing = append(missing, "every node has claims disabled")
		case !slicesAny(nodes, func(node nodeStatus) bool { return node.AcceptsOneshot }):
			missing = append(missing, "no node has a free one-shot slot")
		}
	}
	if len(missing) == 0 {
		return true, "ready"
	}
	return false, "not ready: " + strings.Join(missing, "; ")
}

func endpointOrNone(endpoint string) string {
	if strings.TrimSpace(endpoint) == "" {
		return "no configured endpoint"
	}
	return endpoint
}

func slicesAny(nodes []nodeStatus, match func(nodeStatus) bool) bool {
	for _, node := range nodes {
		if match(node) {
			return true
		}
	}
	return false
}

// writeStatus prints the verdict first. Everything below it is the evidence for
// that one line, and a reader who trusts the line should not have to read on.
func writeStatus(writer io.Writer, status clusterStatus) error {
	if _, err := fmt.Fprintf(writer, "%s\n\n", status.Verdict); err != nil {
		return err
	}
	identity := "no person identity"
	if status.Identity.UserID != "" {
		identity = status.Identity.UserID
		if status.Identity.DeviceID != "" {
			identity += " on " + status.Identity.DeviceID
		}
	} else if status.Identity.Detail != "" {
		identity = "no person identity (" + status.Identity.Detail + ")"
	}
	if _, err := fmt.Fprintf(writer, "identity: %s\n\n", identity); err != nil {
		return err
	}
	services := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(services, "SERVICE\tENDPOINT\tREACHABLE\tDETAIL"); err != nil {
		return err
	}
	for _, service := range status.Services {
		if _, err := fmt.Fprintf(services, "%s\t%s\t%t\t%s\n",
			service.Name, endpointOrNone(service.Endpoint), service.Reachable, valueOrNA(service.Detail)); err != nil {
			return err
		}
	}
	if err := services.Flush(); err != nil {
		return err
	}
	if len(status.Nodes) == 0 {
		_, err := fmt.Fprintln(writer, "\nno nodes")
		return err
	}
	nodes := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(nodes, "\nNODE\tSTATE\tCLAIMS\tKINDS\tONE-SHOT\tSERVICE"); err != nil {
		return err
	}
	for _, node := range status.Nodes {
		claims := "enabled"
		if !node.ClaimsEnabled {
			claims = "disabled"
		}
		kinds := "none"
		if len(node.Kinds) > 0 {
			kinds = strings.Join(node.Kinds, ",")
		}
		if _, err := fmt.Fprintf(nodes, "%s\t%s\t%s\t%s\t%d/%d free\t%d/%d free\n",
			node.NodeID, node.State, claims, kinds,
			node.FreeOneshot, node.TotalOneshot, node.FreeService, node.TotalService); err != nil {
			return err
		}
	}
	return nodes.Flush()
}
