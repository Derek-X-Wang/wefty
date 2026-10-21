package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/nodeload"
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
// diagnose. It does not re-probe capabilities: the agent already observed its own
// capabilities and reported them. Only this machine's load is read locally.
// L1 already knows the slot policy and what
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
	// Keep the typed cause for readiness advice without changing the wire shape.
	sandbox *networkSandboxError
}

// nodeStatus is a node's ability to take work, not its whole projection.
type nodeStatus struct {
	AgentVersion string             `json:"agent_version,omitempty"`
	NodeID       string             `json:"node_id"`
	ThisMachine  bool               `json:"this_machine,omitempty"`
	State        contract.NodeState `json:"state"`
	// ClaimsEnabled is durable operator intent. A node can be perfectly healthy
	// and still be closed for work, and that is a different sentence.
	ClaimsEnabled      bool `json:"claims_enabled"`
	Upgrading          bool `json:"upgrading,omitempty"`
	UpgradeInterrupted bool `json:"upgrade_interrupted,omitempty"`
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
	// These are local configuration facts, not a person identity probe.
	NodeID       string `json:"node_id,omitempty"`
	PersonSignIn string `json:"person_sign_in,omitempty"`
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

// localLoadStatus describes only this machine's directly read load.
type localLoadStatus struct {
	Average *float64 `json:"average,omitempty"`
	Limit   float64  `json:"limit"`
	Holding bool     `json:"holding"`
}

// clusterStatus is the whole answer, and the shape `--json` publishes.
type clusterStatus struct {
	VersionWarnings  []statusReason   `json:"version_warnings,omitempty"`
	UpgradeRecovery  string           `json:"upgrade_recovery,omitempty"`
	Busy             string           `json:"busy,omitempty"`
	WaitingRuns      *int             `json:"waiting_runs,omitempty"`
	LocalLoad        *localLoadStatus `json:"local_load,omitempty"`
	InstalledVersion string           `json:"installed_version,omitempty"`
	Ready            bool             `json:"ready"`
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
	// LocalNode is null unless both the installation and L1 identify this
	// machine's node. A local installation problem does not block the cluster.
	LocalNode      *localNodeIdentity `json:"local_node"`
	LocalNodeError *statusReason      `json:"local_node_error,omitempty"`
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
	budget, err := parseStatusBudget(args, jsonOutput, stderr)
	if err != nil {
		return err
	}

	bounded, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	status := collectStatus(bounded, clients)
	status.InstalledVersion = installedStackVersion(clients.nodeConfigPath)

	return writeStatusResult(stdout, status, jsonOutput)
}

func parseStatusBudget(args []string, jsonOutput bool, stderr io.Writer) (time.Duration, error) {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(flagErrorOutput(stderr, jsonOutput))
	var budget time.Duration
	flags.DurationVar(&budget, "timeout", statusBudget, "give up probing after this long")
	if err := parseCommandFlags(flags, args, false); err != nil {
		// This command's exit codes are part of what it promises, so a bad
		// flag has to be a usage error rather than the generic failure a raw
		// flag error becomes.
		return 0, flagUsageError(err)
	}
	if flags.NArg() != 0 {
		return 0, usageError("usage: wefty status [--timeout D]")
	}
	if budget <= 0 {
		return 0, usageError("--timeout must be positive")
	}

	return budget, nil
}

func writeStatusResult(stdout io.Writer, status clusterStatus, jsonOutput bool) error {
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

// reportParticipantCheckFailure reports what is known without probing through
// an unverified participant or making claims about the remote services.
func reportParticipantCheckFailure(stdout io.Writer, options globalOptions, config fabricconfig.Config, cause error) error {
	detail := cause.Error()
	status := clusterStatus{
		CheckedAt: time.Now().UTC(), InstalledVersion: installedStackVersion(options.nodeConfigPath),
		Identity: fabricStatus{Kind: identityUnknown, NodeID: config.Identity.NodeID, PersonSignIn: localPersonSignIn(options, time.Now()), Detail: detail},
		Reasons:  []statusReason{{Code: "participant_check_failed", Detail: detail}},
		Services: []serviceStatus{{Name: "L1", Endpoint: options.l1Address, Detail: "not probed: participant check failed"}, {Name: "L3", Endpoint: options.l3Address, Detail: "not probed: participant check failed"}},
		Nodes:    []nodeStatus{}, Capacity: map[string]int{},
	}
	status.Verdict = verdictOf(status.Reasons)
	return writeStatusResult(stdout, status, options.jsonOutput)
}

// collectStatus probes everything at once. Concurrency is not for speed here:
// it is so one unreachable service cannot hide the state of the others, which
// is exactly the case a person runs this command in.
func collectStatus(ctx context.Context, clients *apiClients) clusterStatus {
	status := clusterStatus{CheckedAt: time.Now().UTC()}
	l1Status := serviceStatus{Name: "L1", Endpoint: clients.l1.address}
	l3Status := serviceStatus{Name: "L3", Endpoint: clients.l3.address}

	var nodes l1.NodeList
	var stuck []queuedRunNote
	var waitingRuns *int
	var group sync.WaitGroup
	group.Add(4)
	go func() {
		defer group.Done()
		probeCtx, done := context.WithTimeout(ctx, statusProbeBudget)
		defer done()
		local, err := resolveLocalNode(probeCtx, clients)
		if err != nil {
			var localErr *localNodeError
			if errors.As(err, &localErr) {
				status.LocalNodeError = &statusReason{Code: string(localErr.Code), Detail: localErr.Detail}
			}
			return
		}
		status.LocalNode = &local
	}()
	go func() {
		defer group.Done()
		probeCtx, done := context.WithTimeout(ctx, statusProbeBudget)
		defer done()
		listed, err := clients.listNodes(probeCtx)
		if err != nil {
			l1Status.Detail = probeDetail(err)
			errors.As(err, &l1Status.sandbox)
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
		// The queued listing is the reachability probe too, so the one
		// thing a ready cluster can still hide -- a run nothing can take
		// (#604) -- costs no extra round trip unless there is a queued run.
		queued, err := clients.listRuns(probeCtx, string(contract.RunQueued), maxAnnotatedQueuedRuns)
		if err != nil {
			l3Status.Detail = probeDetail(err)
			errors.As(err, &l3Status.sandbox)
			return
		}
		l3Status.Reachable = true
		if queued.NextCursor == "" {
			n := len(queued.Runs)
			waitingRuns = &n
		}
		runIDs := make([]string, 0, len(queued.Runs))
		for _, run := range queued.Runs {
			runIDs = append(runIDs, run.RunID)
		}
		reasons := queuedRunReasons(probeCtx, clients, runIDs)
		for _, runID := range runIDs {
			if reason := reasons[runID]; reason != "" {
				stuck = append(stuck, queuedRunNote{runID: runID, reason: reason})
			}
		}
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
	status.Identity.NodeID = clients.clientIdentityID
	status.Identity.PersonSignIn = clients.localPersonSignIn

	status.Services = []serviceStatus{l1Status, l3Status}
	status.Nodes = nodeStatuses(nodes)
	status.VersionWarnings = nodeVersionWarnings(nodes)
	receipt, upgradeActive := localUpgradeStatus(clients.nodeConfigPath)
	if receipt != nil && !upgradeActive {
		status.UpgradeRecovery = "interrupted upgrade receipt; run wefty upgrade to finish recovery"
		for _, node := range nodes.Nodes {
			if receipt.ownsClaims(node) {
				status.UpgradeRecovery = "claims disabled by an interrupted upgrade; run wefty upgrade to restore admission"
				// nodeStatuses sorts its result, so match by identity rather than index.
				for j := range status.Nodes {
					if status.Nodes[j].NodeID == node.NodeID {
						status.Nodes[j].Upgrading = false
						status.Nodes[j].UpgradeInterrupted = true
					}
				}
			}
		}
	}
	upgradeDraining := false
	if receipt != nil && upgradeActive {
		for i := range status.Nodes {
			if status.Nodes[i].NodeID == receipt.NodeID {
				for _, node := range nodes.Nodes {
					if receipt.ownsClaims(node) {
						status.Nodes[i].Upgrading = true
						status.Nodes[i].UpgradeInterrupted = false
					}
				}
				upgradeDraining = status.Nodes[i].State == contract.NodeDraining
			}
		}
	}
	if status.LocalNode != nil {
		for i := range status.Nodes {
			status.Nodes[i].ThisMachine = status.Nodes[i].NodeID == status.LocalNode.NodeID
		}
	}
	if status.LocalNode != nil {
		var local l1.Node
		for _, node := range nodes.Nodes {
			if node.NodeID == status.LocalNode.NodeID {
				local = node
				break
			}
		}
		load := readLocalNodeLoad(clients, local)
		status.LocalLoad = &load
	}
	status.WaitingRuns = waitingRuns
	status.Capacity = capacityByKind(status.Nodes)
	status.Reasons, status.Limitations = reasonsFor(l1Status, l3Status, status.Nodes, status.Capacity)
	// The durable local receipt and live installation lock explain these brief
	// restart outages. Keep reachability facts visible, but don't send agents to
	// repair an installation that an upgrade is currently replacing.
	if upgradeActive && (!l1Status.Reachable || !l3Status.Reachable) {
		status.Reasons = nil
		status.Busy = "busy: upgrading; retry in a few seconds"
	} else if upgradeActive && upgradeDraining {
		status.Reasons = nil
		status.Busy = upgradeBusyMessage
	}
	if note, ok := unschedulableRunsNote(stuck); ok {
		status.Limitations = append(status.Limitations, note)
	}
	status.Ready = len(status.Reasons) == 0
	status.Verdict = verdictOf(status.Reasons)
	if status.Busy != "" {
		// A restart-window answer takes precedence over ordinary slot occupancy.
	} else if status.Ready && upgradeQueueOnly(status.Nodes) {
		status.Busy = upgradeBusyMessage
	} else if status.Ready && allOneshotSlotsBusy(status.Nodes) {
		status.Busy = "busy: all one-shot slots in use; "
		if waitingRuns != nil {
			status.Busy += fmt.Sprintf("%d runs waiting; ", *waitingRuns)
		}
		status.Busy += "a new run waits in line"
	}
	return status
}

// queuedRunNote is one queued run L1 says no tag-eligible node can take.
type queuedRunNote struct {
	runID, reason string
}

// unschedulableRunsNote is a limitation, not a reason: the cluster can still
// take other work, and the run may yet be placed when a matching node joins.
func unschedulableRunsNote(stuck []queuedRunNote) (statusReason, bool) {
	if len(stuck) == 0 {
		return statusReason{}, false
	}
	detail := fmt.Sprintf("queued run %s has no eligible node: %s", stuck[0].runID, stuck[0].reason)
	if len(stuck) > 1 {
		detail = fmt.Sprintf("%d queued runs have no eligible node, e.g. run %s: %s", len(stuck), stuck[0].runID, stuck[0].reason)
	}
	return statusReason{Code: "queued_run_unschedulable", Detail: detail}, true
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
		upgradeReason := !node.ClaimsEnabled && strings.HasPrefix(node.IntentReason, upgradeIntentPrefix)
		upgradeRecent := upgradeReasonActive(node, time.Now())
		status := nodeStatus{
			NodeID: node.NodeID, AgentVersion: node.AgentVersion, State: node.State, ClaimsEnabled: node.ClaimsEnabled,
			Upgrading:          upgradeReason && upgradeRecent,
			UpgradeInterrupted: upgradeReason && !upgradeRecent,
			Kinds:              executionKinds(node.Capabilities),
			TotalOneshot:       node.MaxOneshotSlots, TotalService: node.MaxServiceSlots,
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
		detail := "L1 at " + endpointOrNone(l1Status.Endpoint) + " is not reachable (" + addressHint("l1") + ")"
		if l1Status.sandbox != nil {
			detail = l1Status.sandbox.Error()
		}
		reasons = append(reasons, statusReason{Code: "l1_unreachable",
			Detail: detail})
	}
	if !l3Status.Reachable {
		detail := "L3 at " + endpointOrNone(l3Status.Endpoint) + " is not reachable (" + addressHint("l3") + ")"
		if l3Status.sandbox != nil {
			detail = l3Status.sandbox.Error()
		}
		reasons = append(reasons, statusReason{Code: "l3_unreachable",
			Detail: detail})
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
		// A draining node is not dead: it stopped taking work and is
		// finishing what it holds, and a replacement cannot register until it
		// is done. Saying "no node is alive" sent people to start a second
		// agent, which fails (#604).
		draining := filterNodes(nodes, func(node nodeStatus) bool { return node.State == contract.NodeDraining })
		if len(draining) > 0 {
			ids := make([]string, 0, len(draining))
			for _, node := range draining {
				ids = append(ids, node.NodeID)
			}
			return append(reasons, statusReason{Code: "nodes_draining",
				Detail: "no node is alive; draining (finishing resident attempts before it stops): " + strings.Join(ids, ", ")}), nil
		}
		return append(reasons, statusReason{Code: "no_alive_node", Detail: "no node is alive"}), nil
	}
	claiming := filterNodes(alive, func(node nodeStatus) bool { return node.ClaimsEnabled || node.Upgrading })
	if len(claiming) == 0 {
		for _, node := range alive {
			if node.UpgradeInterrupted {
				return append(reasons, statusReason{Code: "claims_disabled", Detail: "claims disabled by an interrupted upgrade; run wefty upgrade to restore admission"}), nil
			}
		}
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
	// A node configured with no one-shot slots cannot move the line at all.
	// Keep that configuration problem distinct from occupied capacity.
	if len(filterNodes(executing, func(node nodeStatus) bool { return node.TotalOneshot > 0 })) == 0 {
		return append(reasons, statusReason{Code: "no_free_slot",
			Detail: "no alive, claiming node has one-shot slots configured"}), nil
	}
	// Missing immediate capacity is a limitation. An alive, claiming node
	// with an executable kind will take queued work once capacity returns.
	var short []statusReason
	for _, kind := range executableKinds {
		if capacity[kind] == 0 {
			short = append(short, statusReason{Code: "no_free_slot:" + kind,
				Detail: "no node with a free one-shot slot can run kind=" + kind})
		}
	}
	return reasons, short
}

// Upgrade admission is temporary. Keep actual eligibility and capacity at
// zero, but let agents submit into the queue while the upgrade owns the gate.
func upgradeQueueOnly(nodes []nodeStatus) bool {
	upgrading := false
	for _, node := range nodes {
		if node.State != contract.NodeAlive || node.TotalOneshot <= 0 || len(nodeExecutableKinds(node.Kinds)) == 0 {
			continue
		}
		if node.AcceptsOneshot {
			return false
		}
		upgrading = upgrading || node.Upgrading
	}
	return upgrading
}

func allOneshotSlotsBusy(nodes []nodeStatus) bool {
	eligible := filterNodes(nodes, func(node nodeStatus) bool {
		return node.State == contract.NodeAlive && node.ClaimsEnabled && len(nodeExecutableKinds(node.Kinds)) > 0
	})
	if len(eligible) == 0 {
		return false
	}
	for _, node := range eligible {
		if node.FreeOneshot > 0 {
			return false
		}
	}
	return true
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
	if status.Busy != "" {
		if _, err := fmt.Fprintln(writer, status.Busy); err != nil {
			return err
		}
	}
	if status.UpgradeRecovery != "" {
		if _, err := fmt.Fprintln(writer, status.UpgradeRecovery); err != nil {
			return err
		}
	}
	if status.InstalledVersion != "" {
		if _, err := fmt.Fprintln(writer, "installed version:", status.InstalledVersion); err != nil {
			return err
		}
	}
	for _, warning := range status.VersionWarnings {
		if _, err := fmt.Fprintln(writer, "warning:", warning.Detail); err != nil {
			return err
		}
	}
	for _, node := range status.Nodes {
		if node.AgentVersion != "" {
			if _, err := fmt.Fprintf(writer, "node version: %s %s\n", node.NodeID, node.AgentVersion); err != nil {
				return err
			}
		}
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
	if status.Identity.NodeID != "" {
		identity = "client " + status.Identity.NodeID
		if status.Identity.Detail != "" {
			identity += "; " + status.Identity.Detail
		}
	}
	if status.Identity.PersonSignIn != "" {
		identity += "; local person sign-in: " + status.Identity.PersonSignIn
	}
	if _, err := fmt.Fprintf(writer, "identity: %s\n\n", identity); err != nil {
		return err
	}
	localDetail := "no installed node"
	if status.LocalNode != nil {
		localDetail = fmt.Sprintf("%s (Pinned tag: %s)", status.LocalNode.NodeID, status.LocalNode.Tag)
	} else if status.LocalNodeError != nil {
		localDetail = status.LocalNodeError.Detail
	}
	if status.LocalLoad != nil {
		load := "unavailable"
		if status.LocalLoad.Average != nil {
			load = fmt.Sprintf("%.2f", *status.LocalLoad.Average)
		}
		localDetail += fmt.Sprintf("; load %s; limit %.2f", load, status.LocalLoad.Limit)
		if status.LocalLoad.Holding {
			localDetail += "; holding new work: machine busy"
		}
	}
	if _, err := fmt.Fprintf(writer, "this machine: %s\n\n", localDetail); err != nil {
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
		nodeID := node.NodeID
		if node.ThisMachine {
			nodeID += " (this machine)"
		}
		claims := "enabled"
		if !node.ClaimsEnabled {
			claims = "disabled"
		}
		kinds := "none"
		if len(node.Kinds) > 0 {
			kinds = strings.Join(node.Kinds, ",")
		}
		if _, err := fmt.Fprintf(nodes, "%s\t%s\t%s\t%s\t%d/%d free\t%d/%d free\n",
			nodeID, node.State, claims, kinds,
			node.FreeOneshot, node.TotalOneshot, node.FreeService, node.TotalService); err != nil {
			return err
		}
	}
	return nodes.Flush()
}

// Status and exec must report the same local admission facts and settings.
func readLocalNodeLoad(clients *apiClients, node l1.Node) localLoadStatus {
	reader := clients.readLoad
	if reader == nil {
		reader = nodeload.Read
	}
	limit := nodeload.DefaultLimit()
	if clients.oneshotLoadLimit != nil {
		limit = *clients.oneshotLoadLimit
	}
	status := localLoadStatus{Limit: limit}
	if load, err := reader(); err == nil {
		status.Average = &load
		status.Holding = node.State == contract.NodeAlive && node.ClaimsEnabled &&
			len(nodeExecutableKinds(executionKinds(node.Capabilities))) > 0 && nodeload.Holding(node.OneshotOccupancy, load, limit)
	}
	return status
}

// Receipt + released installation lock identifies a local interrupted upgrade.
// A live upgrader holds that directory lock through restoration and deletion.
func localUpgradeStatus(nodeConfig string) (*upgradeDrainReceipt, bool) {
	if nodeConfig == "" {
		return nil, false
	}
	config, err := filepath.EvalSymlinks(filepath.Dir(nodeConfig))
	if err != nil {
		return nil, false
	}
	receipt, err := readUpgradeDrain(config)
	if err != nil {
		return nil, false
	}
	paths, err := resolveSetupPaths(receipt.Home)
	if err != nil || paths.nodeConfig != filepath.Join(config, filepath.Base(nodeConfig)) {
		return nil, false
	}
	return receipt, setupLockHeld(paths)
}
