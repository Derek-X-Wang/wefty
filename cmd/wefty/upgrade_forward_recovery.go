package main

import (
	"context"
	"encoding/json"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/l1"
	"io"
	"reflect"
	"slices"
)

// A refused rollback grants no authority. A fresh response from the recorded
// primary's installed adapter is required before preparing another activation.
// The previous rolling receipt survives build/preflight failure; committed L1
// and activation receipts remain the operation history after forward replacement.
func prepareFurtherRolling(ctx context.Context, host setupHost, op upgradeHost, paths setupPaths, prior *upgradeRollingReceipt, out io.Writer) (*upgradeDrainReceipt, error) {
	profile, err := connprofile.Read(paths.profile)
	if err != nil {
		return nil, err
	}
	boot, err := op.bootIdentity()
	if err != nil {
		return nil, err
	}
	store := upgradeGenerationStore(host, paths, op)
	c, err := readUpgradeCatalog(store)
	if err != nil {
		return nil, generationControlTransportError(err)
	}
	g, ok := c.Generations[c.Primary]
	if prior.RecoveryState != "rollback_refused" || prior.NodeID != profile.Identities.Node || c.NodeID != prior.NodeID || !hostboot.SameBoot(boot, prior.SystemBootID) || !ok || c.Primary != prior.Candidate.ID || c.PendingReceipt != "" || g.Process == nil {
		return nil, generationControlConflict("forward repair lacks the recorded promoted primary")
	}
	activation, err := store.ReadActivationReceipt(prior.ActivationReceiptID)
	if err != nil {
		return nil, generationControlTransportError(err)
	}
	if !activation.Committed || activation.L1OperationID != prior.OperationID || activation.Candidate != g.ID {
		return nil, generationControlConflict("forward repair lacks committed activation evidence")
	}
	payload, err := op.generationControl(ctx, store, agentinstall.ControlRequest{ID: prior.OperationID, NodeID: prior.NodeID, GenerationID: g.ID, BootSessionID: g.Process.BootSessionID, Action: "generation_inventory"})
	if err != nil {
		return nil, err
	}
	var in l1.NodeGenerationInventory
	if err = json.Unmarshal(payload, &in); err != nil {
		return nil, err
	}
	primary := false
	for _, remote := range in.Generations {
		if remote.AgentGenerationID == g.ID {
			primary = remote.Role == l1.NodeGenerationPrimary && remote.BootSessionID == g.Process.BootSessionID && remote.PromotionOperationID == prior.OperationID
		}
	}
	if in.NodeID != prior.NodeID || !primary {
		return nil, generationControlConflict("answering control adapter is not the recorded L1 primary")
	}
	r := rollingUpgrade{op: op, paths: paths, profile: profile, store: store, receipt: *prior}
	drain, err := r.recoveryDrain()
	if err != nil {
		return nil, err
	}
	if drain == nil {
		return nil, generationControlConflict("forward repair lacks original admission evidence")
	}
	node, err := op.readNode(ctx, profile)
	if err != nil {
		return nil, err
	}
	if node.NodeID != prior.NodeID || node.AgentGenerationID != g.ID || node.BootSessionID != g.Process.BootSessionID {
		return nil, generationControlConflict("forward repair primary observation changed")
	}
	// Reconcile the existing service queue while admission stays closed. No
	// ordinary Attempt is restarted. Lost replies retain the same restart keys.
	if err = resumeUpgradeServices(ctx, op, paths, profile, drain, node, false, out); err != nil {
		return nil, err
	}
	r.receipt.RecoveryDrain = drain
	if err = r.persist(r.receipt.Phase); err != nil {
		return nil, err
	}
	if !drain.ownsClaims(node) {
		return nil, nil
	} // changed operator intent is never restored
	inherited := *drain
	inherited.Services = nil
	// Keep the predecessor's exact settled proof until the successor drain is
	// durable. Recovery can encounter either side of this handoff.
	return &inherited, nil
}
func (r *rollingUpgrade) inheritForwardAdmission(drain *upgradeDrainReceipt) {
	if prior := r.receipt.ForwardAdmission; prior != nil && drain != nil && drain.NodeID == prior.NodeID && drain.Revision == prior.Revision && drain.Reason == prior.Reason {
		drain.RestoreClaims = prior.RestoreClaims
		drain.PreviousReason = prior.PreviousReason
	}
}

// A successor may die before replacing the predecessor's drain file. Its
// prepared receipt retains the predecessor operation and exact settled proof.
// Consume only that handoff; never reinterpret a foreign proof as a new service
// quiescence or repeat the predecessor's service restart queue.
func (r *rollingUpgrade) reconcileForwardDrain(drain *upgradeDrainReceipt) (*upgradeDrainReceipt, error) {
	if drain == nil || drain.ServiceQuiescence == nil || drain.ServiceQuiescence.OperationID == r.receipt.OperationID {
		return drain, nil
	}
	prior := r.receipt.ForwardAdmission
	if prior == nil || r.receipt.ForwardFromOperation == "" || r.receipt.ForwardFromOperation == r.receipt.OperationID || prior.ServiceQuiescence == nil || prior.ServiceQuiescence.OperationID != r.receipt.ForwardFromOperation || !slices.Contains([]string{"prepared", "admission_closing", "rolling_back"}, r.receipt.Phase) || len(prior.Services) != 0 || len(drain.Services) != 0 || prior.Home != drain.Home || prior.NodeID != drain.NodeID || prior.Revision != drain.Revision || prior.Reason != drain.Reason || prior.PreviousReason != drain.PreviousReason || prior.RestoreClaims != drain.RestoreClaims || prior.OldBoot != drain.OldBoot || !reflect.DeepEqual(prior.ServiceQuiescence, drain.ServiceQuiescence) {
		return nil, generationControlConflict("service proof differs from recorded predecessor drain handoff")
	}
	if err := validateUpgradeServiceQuiescence(prior); err != nil {
		return nil, err
	}
	copy := *drain
	copy.ServiceQuiescence = nil
	copy.OldBoot = ""
	return &copy, nil
}
