package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
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
	//
	// What the budget covers is the verdict: it is printed within this window,
	// dials included. It is not a promise about when the process exits -- on a
	// tsnet Fabric the process may take longer to leave, flushing state that
	// belongs to the Fabric rather than to this command.
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
	Kinds        []string `json:"kinds"`
	FreeOneshot  int      `json:"free_oneshot_slots"`
	TotalOneshot int      `json:"total_oneshot_slots"`
	FreeService  int      `json:"free_service_slots"`
	TotalService int      `json:"total_service_slots"`
	// AcceptsOneshot is the whole eligibility question for a one-shot job:
	// alive, open to claims, with a slot free, and able to execute something.
	// A node advertising no executable kind can never be claimed -- L1's claim
	// query requires kind:<kind> -- so counting it as capacity would be a
	// confident "ready" for a cluster that will never run anything.
	AcceptsOneshot bool `json:"accepts_oneshot"`
}

// executableKinds are the kinds a one-shot job can actually name. A node that
// advertises none of them is not capacity, whatever else it advertises; a node
// that advertises something this list does not know is simply not counted for
// that kind, which is a missing positive rather than a false one.
var executableKinds = []string{contract.JobKindProcess, contract.JobKindOCI}

// statusReason is one named thing standing between this cluster and taking
// work. The code is stable so a script can branch on it; the detail is the
// sentence a person reads.
type statusReason struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

// fabricStatus is who this command was, as the control plane sees it.
type fabricStatus struct {
	// Kind says which sort of caller this is, so a reader never has to infer
	// it from whether the other fields are empty: "person" when the control
	// plane named one, "machine" when the caller is a machine principal, and
	// "unknown" when the control plane could not be asked at all.
	Kind     string `json:"kind"`
	FabricID string `json:"fabric_id,omitempty"`
	UserID   string `json:"user_id,omitempty"`
	DeviceID string `json:"device_id,omitempty"`
	// Detail is one plain sentence about the identity. A machine principal is
	// a normal, expected caller -- most scripts are one -- so it reads as a
	// fact rather than as the protocol error the refusal arrived as.
	Detail string `json:"detail,omitempty"`
}

const (
	identityPerson  = "person"
	identityMachine = "machine"
	identityUnknown = "unknown"
)

// clusterStatus is the whole answer, and the shape `--json` publishes.
type clusterStatus struct {
	Ready bool `json:"ready"`
	// Verdict is the one line a person reads: "ready", or "not ready: ..."
	// naming what is missing rather than what was checked.
	Verdict string `json:"verdict"`
	// Reasons is every reason at once, not the first one found. A cluster with
	// no ledger and no node has two problems, and telling a person about one
	// of them earns a second run of this command.
	Reasons []statusReason `json:"reasons,omitempty"`
	// Limitations are true but not blocking: this cluster can take work, and
	// there is something it cannot take. A fleet of process-only nodes is
	// ready -- the single-machine setup in the docs is exactly that -- and it
	// still cannot run an OCI job, which a reader had better be told without
	// being told to stop.
	Limitations []statusReason  `json:"limitations,omitempty"`
	Identity    fabricStatus    `json:"identity"`
	Services    []serviceStatus `json:"services"`
	Nodes       []nodeStatus    `json:"nodes"`
	// Capacity is free one-shot slots per executable kind, counted over the
	// nodes that could actually take a claim.
	Capacity  map[string]int `json:"capacity_by_kind"`
	CheckedAt time.Time      `json:"checked_at"`
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
		// This command's exit codes are part of what it promises, so a bad
		// flag has to be a usage error rather than the generic failure a raw
		// flag error becomes.
		return usageError(err.Error())
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
			status.Identity = identityFromRefusal(err)
			return
		}
		status.Identity = fabricStatus{
			Kind: identityPerson, FabricID: person.FabricID, UserID: person.UserID, DeviceID: person.DeviceID,
		}
	}()
	group.Wait()

	status.Services = []serviceStatus{l1Status, l3Status}
	status.Nodes = nodeStatuses(nodes)
	status.Capacity = capacityByKind(status.Nodes)
	status.Reasons, status.Limitations = reasonsFor(l1Status, l3Status, status.Nodes, status.Capacity)
	status.Ready = len(status.Reasons) == 0
	status.Verdict = verdictOf(status.Reasons)
	return status
}

// identityFromRefusal reads a refusal as the fact it is.
//
// A machine principal is the ordinary caller here -- most scripts are one, and
// the person protocols are closed to them by design -- so it is reported as a
// kind of caller rather than as the protocol error string the refusal arrived
// as. Leaking "principal_forbidden: machine principals cannot use person
// protocols" into a status report reads as something being wrong, when nothing
// is.
func identityFromRefusal(err error) fabricStatus {
	var responseErr *apiResponseError
	if errors.As(err, &responseErr) {
		switch responseErr.APIError.Code {
		case contract.ErrorPrincipalForbidden, contract.ErrorPersonIdentityRequired:
			return fabricStatus{Kind: identityMachine,
				Detail: "this caller is a machine principal, which holds no person identity"}
		}
	}
	return fabricStatus{Kind: identityUnknown, Detail: "the control plane could not name this caller: " + probeDetail(err)}
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
		status.AcceptsOneshot = node.State == contract.NodeAlive && node.ClaimsEnabled &&
			status.FreeOneshot > 0 && len(nodeExecutableKinds(status.Kinds)) > 0
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

// nodeExecutableKinds narrows a node's advertised kinds to the ones a one-shot
// job can name. "computer" is a capability a Computer job needs, not a kind a
// one-shot names, so it is not capacity for this question.
func nodeExecutableKinds(kinds []string) []string {
	executable := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		if slices.Contains(executableKinds, kind) {
			executable = append(executable, kind)
		}
	}
	return executable
}

// capacityByKind counts free one-shot slots per kind over the nodes that could
// actually take a claim. A node advertising two kinds contributes its free
// slots to both, because either could use them -- this is capacity, not a
// reservation.
func capacityByKind(nodes []nodeStatus) map[string]int {
	capacity := map[string]int{}
	for _, kind := range executableKinds {
		capacity[kind] = 0
	}
	for _, node := range nodes {
		if !node.AcceptsOneshot {
			continue
		}
		for _, kind := range nodeExecutableKinds(node.Kinds) {
			capacity[kind] += node.FreeOneshot
		}
	}
	return capacity
}

// reasonsFor names everything standing in the way, computed over the same
// candidate set at every step so the answer narrows honestly: a dead node
// cannot also be the reason claims are disabled, and a node with claims off is
// not evidence that the fleet is out of slots.
func reasonsFor(l1Status, l3Status serviceStatus, nodes []nodeStatus,
	capacity map[string]int) (reasons, limitations []statusReason) {
	if !l1Status.Reachable {
		reasons = append(reasons, statusReason{Code: "l1_unreachable",
			Detail: "L1 at " + endpointOrNone(l1Status.Endpoint) + " is not reachable"})
	}
	if !l3Status.Reachable {
		reasons = append(reasons, statusReason{Code: "l3_unreachable",
			Detail: "L3 at " + endpointOrNone(l3Status.Endpoint) + " is not reachable"})
	}
	if !l1Status.Reachable {
		// Without L1 there is no node projection at all, so anything said
		// about nodes here would be said about an empty list.
		return reasons, nil
	}
	if len(nodes) == 0 {
		return append(reasons, statusReason{Code: "no_node_registered", Detail: "no node is registered"}), nil
	}
	alive := filterNodes(nodes, func(node nodeStatus) bool { return node.State == contract.NodeAlive })
	if len(alive) == 0 {
		return append(reasons, statusReason{Code: "no_alive_node", Detail: "no node is alive"}), nil
	}
	claiming := filterNodes(alive, func(node nodeStatus) bool { return node.ClaimsEnabled })
	if len(claiming) == 0 {
		return append(reasons, statusReason{Code: "claims_disabled",
			Detail: "every alive node has claims disabled"}), nil
	}
	executing := filterNodes(claiming, func(node nodeStatus) bool {
		return len(nodeExecutableKinds(node.Kinds)) > 0
	})
	if len(executing) == 0 {
		return append(reasons, statusReason{Code: "no_executable_kind",
			Detail: "no alive, claiming node advertises an executable kind (" +
				strings.Join(executableKinds, " or ") + ")"}), nil
	}
	// Per kind, because "ready for a process job" and "ready for an OCI job"
	// are different answers. A kind with no capacity is a limitation; only a
	// cluster with no capacity for any kind is not ready, because a
	// process-only fleet -- which is what the documented single-machine setup
	// is -- can take work and must not be told to stop.
	var short []statusReason
	for _, kind := range executableKinds {
		if capacity[kind] == 0 {
			short = append(short, statusReason{Code: "no_free_slot:" + kind,
				Detail: "no node with a free one-shot slot can run kind=" + kind})
		}
	}
	if len(short) == len(executableKinds) {
		return append(reasons, statusReason{Code: "no_free_slot",
			Detail: "no node has a free one-shot slot"}), nil
	}
	return reasons, short
}

// verdictOf is the one line, built from the reasons rather than alongside them
// so the sentence and the codes can never disagree.
func verdictOf(reasons []statusReason) string {
	if len(reasons) == 0 {
		return "ready"
	}
	details := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		details = append(details, reason.Detail)
	}
	return "not ready: " + strings.Join(details, "; ")
}

func filterNodes(nodes []nodeStatus, match func(nodeStatus) bool) []nodeStatus {
	kept := make([]nodeStatus, 0, len(nodes))
	for _, node := range nodes {
		if match(node) {
			kept = append(kept, node)
		}
	}
	return kept
}

func endpointOrNone(endpoint string) string {
	if strings.TrimSpace(endpoint) == "" {
		return "no configured endpoint"
	}
	return endpoint
}

// writeStatus prints the verdict first. Everything below it is the evidence for
// that one line, and a reader who trusts the line should not have to read on.
func writeStatus(writer io.Writer, status clusterStatus) error {
	if _, err := fmt.Fprintf(writer, "%s\n", status.Verdict); err != nil {
		return err
	}
	for _, limitation := range status.Limitations {
		if _, err := fmt.Fprintf(writer, "note: %s\n", limitation.Detail); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(writer); err != nil {
		return err
	}
	identity := "no person identity"
	switch {
	case status.Identity.UserID != "":
		identity = status.Identity.UserID
		if status.Identity.DeviceID != "" {
			identity += " on " + status.Identity.DeviceID
		}
	case status.Identity.Detail != "":
		identity = status.Identity.Detail
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
	kinds := make([]string, 0, len(status.Capacity))
	for _, kind := range executableKinds {
		kinds = append(kinds, fmt.Sprintf("%s=%d", kind, status.Capacity[kind]))
	}
	if _, err := fmt.Fprintf(writer, "\nfree one-shot slots by kind: %s\n", strings.Join(kinds, " ")); err != nil {
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
