package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/upgradecompat"
	"github.com/Derek-X-Wang/wefty/l1"
)

const upgradeRollingFile = "upgrade-rolling.json"

// The existing conflict exit (5) is used; no new process exit code is added.
type upgradeRollingRefusedError struct {
	apiResponseError
	Reason upgradecompat.Reason
}

func rollingRefused(reason upgradecompat.Reason, detail string) error {
	return &upgradeRollingRefusedError{apiResponseError: apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: fmt.Sprintf("rolling upgrade refused: %s: %s", reason, detail)}}, Reason: reason}
}
func (e *upgradeRollingRefusedError) Unwrap() error { return &e.apiResponseError }

// Rolling receipts deliberately survive failures. #1180 reconciles external
// effects by these exact requests/operation IDs before another activation or
// legacy drain recovery can change ownership.
type upgradeRollingReceipt struct {
	ForwardFromOperation string                           `json:"forward_from_operation,omitempty"`
	ForwardAdmission     *upgradeDrainReceipt             `json:"forward_admission,omitempty"`
	LegacyPlistDigest    string                           `json:"legacy_plist_sha256,omitempty"`
	LegacyBase           *launchagent.Agent               `json:"legacy_generation_agent,omitempty"`
	StagedAbort          *agentinstall.StagedAbortControl `json:"staged_abort,omitempty"`
	StageRegistration    *contract.NodeRegistration       `json:"stage_registration,omitempty"`
	CompletionFiles      []rollingCompletionFile          `json:"completion_files,omitempty"`
	RebootAttempts       map[string]l1.Attempt            `json:"reboot_attempts,omitempty"`
	DrainIntentSequenced bool                             `json:"drain_intent_sequenced,omitempty"`
	RecoveryDrain        *upgradeDrainReceipt             `json:"recovery_drain,omitempty"`
	RecoveryState        string                           `json:"recovery_state,omitempty"`
	Protocol             uint32                           `json:"retirement_protocol"`
	LocalCatalogRevision uint64                           `json:"local_catalog_revision"`
	TrackedResidents     map[string]l1.ResidentAttempt    `json:"tracked_residents,omitempty"`
	ControlRenewalProof  map[string]l1.Attempt            `json:"control_plane_renewal_proof,omitempty"`
	ControlsRestartedAt  time.Time                        `json:"controls_restarted_at,omitempty"`
	BinaryDigests        map[string]string                `json:"binary_sha256,omitempty"`

	Version             int                              `json:"version"`
	OperationID         string                           `json:"operation_id"`
	NodeID              string                           `json:"node_id"`
	Phase               string                           `json:"phase"`
	SystemBootID        string                           `json:"system_boot_id"`
	Candidate           agentinstall.Generation          `json:"candidate"`
	Inventory           l1.NodeGenerationInventory       `json:"inventory"`
	Preserved           []l1.NodeActiveAttempt           `json:"preserved_attempts"`
	Promotion           *l1.PromoteNodeGenerationRequest `json:"promotion_request,omitempty"`
	PromotionResult     *l1.NodeGenerationInventory      `json:"promotion_result,omitempty"`
	ActivationReceiptID string                           `json:"activation_receipt_id,omitempty"`
	CutoverAt           time.Time                        `json:"cutover_at,omitempty"`
	RenewalProof        map[string]l1.Attempt            `json:"renewal_proof,omitempty"`
	RetirementAcks      map[string]l1.NodeGeneration     `json:"retirement_acknowledgments,omitempty"`
}

func writeUpgradeRolling(paths setupPaths, r *upgradeRollingReceipt, phase string) error {
	r.Phase = phase
	root, err := os.OpenRoot(paths.config)
	if err != nil {
		return err
	}
	defer root.Close()
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return durable.WriteFile(root, upgradeRollingFile, append(payload, '\n'), 0600)
}
func readUpgradeRolling(paths setupPaths) (*upgradeRollingReceipt, error) {
	root, err := os.OpenRoot(paths.config)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	payload, err := agentinstall.ReadPrivateFile(root, upgradeRollingFile)
	if err != nil {
		return nil, generationControlTransportError(err)
	}
	var r upgradeRollingReceipt
	if err = json.Unmarshal(payload, &r); err != nil {
		return nil, err
	}
	if r.Version != 1 || r.OperationID == "" || r.NodeID == "" || r.Phase == "" {
		return nil, generationControlConflict("invalid rolling upgrade receipt")
	}
	return &r, nil
}
func refuseInterruptedRolling(paths setupPaths) error {
	r, err := readUpgradeRolling(paths)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if r.Phase != "complete" && r.Phase != "rolled_back" && r.Phase != "reboot_reconciled" {
		return rollingRefused(upgradecompat.InvalidInventory, "interrupted activation "+r.OperationID+" at "+r.Phase+" requires generation-aware recovery; receipts retained")
	}
	return nil
}

type rollingUpgrade struct {
	verifiedPrimaryBoot string
	op                  upgradeHost
	paths               setupPaths
	profile             connprofile.Profile
	store               agentinstall.Store
	receipt             upgradeRollingReceipt
	protocols           []uint32
	primary             l1.NodeGeneration
}

// Tests can exercise stat failures without changing any installed directories.
func (op upgradeHost) statGenerationRoot(path string) (os.FileInfo, error) {
	if op.generationRootStat != nil {
		return op.generationRootStat(path)
	}
	return os.Lstat(path)
}

func upgradeGenerationStore(host setupHost, paths setupPaths, op upgradeHost) agentinstall.Store {
	if op.generationStore != nil {
		return op.generationStore(paths)
	}
	return agentinstall.Store{Data: paths.data, LaunchAgents: paths.launchAgents, Manager: launchagent.Manager{Run: host.launchctl, UID: host.uid}}
}
func checkRollingProfiles(ctx context.Context, op upgradeHost, paths setupPaths, destination, loginPATH string) error {
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for _, name := range setupBinaries {
		output, err := op.command(bounded, destination, upgradeEnvironment(loginPATH), filepath.Join(destination, name), "--profile-preflight", "--plain", "--profile="+paths.profile)
		if err != nil {
			return err
		}
		if strings.TrimSpace(string(output)) != "profile-readable plain" {
			return fmt.Errorf("candidate %s returned a malformed plain profile preflight reply: %q", name, strings.TrimSpace(string(output)))
		}
	}
	return nil
}
func inspectUpgradeProtocols(ctx context.Context, op upgradeHost, path, loginPATH string) ([]uint32, error) {
	before, err := executableDigest(path)
	if err != nil {
		return nil, err
	}
	payload, err := op.command(ctx, filepath.Dir(path), upgradeEnvironment(loginPATH), path, "--retirement-protocols")
	if err != nil {
		return nil, err
	}
	var observation struct {
		Version  int      `json:"version"`
		Versions []uint32 `json:"retirement_protocol_versions"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&observation); err != nil {
		return nil, fmt.Errorf("invalid candidate protocol observation: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return nil, fmt.Errorf("invalid candidate protocol observation: %w", err)
		}
		return nil, errors.New("invalid candidate protocol observation: trailing value")
	}
	if observation.Version != 1 {
		return nil, errors.New("invalid candidate protocol observation version")
	}
	after, err := executableDigest(path)
	if err != nil {
		return nil, err
	}
	if before != after {
		return nil, rollingRefused(upgradecompat.InvalidInventory, "candidate changed during protocol inspection")
	}
	return observation.Versions, nil
}

func prepareRollingUpgrade(ctx context.Context, host setupHost, op upgradeHost, paths setupPaths, profile connprofile.Profile, node l1.Node, destination, commit, loginPATH string) (*rollingUpgrade, bool, error) {
	if err := checkRollingProfiles(ctx, op, paths, destination, loginPATH); err != nil {
		return nil, false, err
	}
	protocols, err := inspectUpgradeProtocols(ctx, op, filepath.Join(destination, "wefty-agent"), loginPATH)
	if err != nil {
		return nil, false, err
	}
	inventory, err := op.generations(ctx, profile)
	if err != nil {
		return nil, false, generationControlTransportError(err)
	}
	input := upgradecompat.Input{Installation: upgradecompat.PlainMac, CandidateProtocols: protocols}
	for _, g := range inventory.Generations {
		if g.Role == l1.NodeGenerationClosed {
			continue
		}
		if g.Role != l1.NodeGenerationPrimary && g.Role != l1.NodeGenerationRetiring {
			return nil, false, rollingRefused(upgradecompat.InvalidInventory, "staged generation requires reconciliation")
		}
		input.Live = append(input.Live, upgradecompat.Generation{ID: g.AgentGenerationID, Role: upgradecompat.Role(g.Role), Protocols: g.RetirementProtocolVersions})
	}
	if len(input.Live) == 0 {
		// A successful empty L1 observation plus no catalog positively identifies a
		// legacy install. Never invent protocol support for its resident process.
		if _, err := op.statGenerationRoot(filepath.Join(paths.data, agentinstall.Directory)); err == nil {
			return nil, false, rollingRefused(upgradecompat.InvalidInventory, "legacy observation conflicts with local generation catalog")
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, false, err
		}
		input.Live = []upgradecompat.Generation{{ID: node.BootSessionID, Role: upgradecompat.Primary}}
	}
	for _, a := range node.ActiveAttempts {
		kind, err := rollingWorkKind(a)
		if err != nil {
			return nil, false, err
		}
		input.Work = append(input.Work, kind)
	}
	// Every retiree's frozen membership must be read too, even when its Attempt
	// has left the Node active projection. Missing/expired work cannot disappear.
	for _, g := range inventory.Generations {
		if g.Role == l1.NodeGenerationRetiring {
			for _, r := range g.ResidentAttempts {
				job, err := op.readJob(ctx, profile, r.JobID)
				if err != nil {
					return nil, false, err
				}
				kind, err := rollingWorkKind(l1.NodeActiveAttempt{Kind: job.Spec.Kind, Class: job.Spec.Class, Restartable: job.Spec.Restartable})
				if err != nil {
					return nil, false, err
				}
				input.Work = append(input.Work, kind)
			}
		}
	}
	decision := upgradecompat.Decide(input)
	if decision.Mode == upgradecompat.Refuse {
		return nil, false, rollingRefused(decision.Reason, "the complete live cohort does not permit activation")
	}
	if decision.Mode == upgradecompat.Drain {
		// Only legacy enrollment is in scope here. OCI/fleet retain their existing
		// installation refusal; they must never enter the overlapping path.
		if decision.Reason == upgradecompat.LegacyPrimary && len(inventory.Generations) == 0 && slices.Contains(protocols, uint32(1)) {
			return nil, true, nil
		}
		return nil, false, rollingRefused(decision.Reason, "this installation needs the default drain upgrade")
	}
	if decision.Protocol != 1 {
		return nil, false, rollingRefused(upgradecompat.IncompatibleProtocols, "the installed adapter supports only protocol 1; cohort transition acknowledgment is unavailable")
	}
	boot, err := op.bootIdentity()
	if err != nil {
		return nil, false, err
	}
	r := &rollingUpgrade{op: op, paths: paths, profile: profile, store: upgradeGenerationStore(host, paths, op), protocols: protocols, receipt: upgradeRollingReceipt{DrainIntentSequenced: true, Version: 1, Protocol: decision.Protocol, OperationID: rand.Text(), NodeID: node.NodeID, SystemBootID: boot, Inventory: inventory, Preserved: slices.Clone(node.ActiveAttempts)}}
	r.receipt.BinaryDigests = map[string]string{}
	for _, name := range setupBinaries {
		digest, err := executableDigest(filepath.Join(destination, name))
		if err != nil {
			return nil, false, err
		}
		r.receipt.BinaryDigests[name] = digest
	}
	r.receipt.Candidate.Version = commit
	r.receipt.Candidate.Digest = strings.TrimPrefix(r.receipt.BinaryDigests["wefty-agent"], "sha256:")
	if err = r.captureInventory(ctx, inventory); err != nil {
		return nil, false, err
	}
	if err = r.validateInventory(ctx, inventory, node); err != nil {
		return nil, false, err
	}
	for _, g := range inventory.Generations {
		if g.Role == l1.NodeGenerationPrimary {
			r.primary = g
		}
	}
	return r, false, nil
}
func rollingWorkKind(a l1.NodeActiveAttempt) (upgradecompat.WorkKind, error) {
	if a.Kind == contract.JobKindOCI {
		return upgradecompat.OCIWork, nil
	}
	if a.Kind != contract.JobKindProcess {
		return "", rollingRefused(upgradecompat.InvalidInventory, "unknown resident work kind")
	}
	if a.Class == contract.JobClassService {
		return upgradecompat.ProcessService, nil
	}
	if a.Class != contract.JobClassOneShot {
		return "", rollingRefused(upgradecompat.InvalidInventory, "unknown resident work class")
	}
	if a.Restartable {
		return upgradecompat.RestartableProcessOneshot, nil
	}
	return upgradecompat.ProcessOneshot, nil
}

func (r *rollingUpgrade) validateInventory(ctx context.Context, in l1.NodeGenerationInventory, node l1.Node) error {
	c, err := readUpgradeCatalog(r.store)
	err = generationControlTransportError(err)
	if err != nil {
		return err
	}
	r.receipt.LocalCatalogRevision = c.Revision
	if in.NodeID != r.profile.Identities.Node || c.NodeID != in.NodeID {
		return rollingRefused(upgradecompat.InvalidInventory, "generation catalog identity or pending promotion differs")
	}
	if c.PendingReceipt != "" {
		pending, err := r.store.ReadActivationReceipt(c.PendingReceipt)
		if err != nil {
			return generationControlTransportError(err)
		}
		if pending.L1OperationID != r.receipt.OperationID || pending.Candidate != r.receipt.Candidate.ID {
			return generationControlConflict("inventory pending activation differs")
		}
	}
	seen := map[string]bool{}
	for _, g := range in.Generations {
		if g.Role == l1.NodeGenerationClosed {
			// Closure ends L1 authority, not local file ownership. Keep these
			// files for #1180 / RemoveFinished without requiring a live process.
			if local, ok := c.Generations[g.AgentGenerationID]; ok {
				if seen[g.AgentGenerationID] || local.Role != "retiring" || local.Process == nil || local.Process.BootSessionID != g.BootSessionID {
					return rollingRefused(upgradecompat.InvalidInventory, "closed history differs from local ownership")
				}
				seen[g.AgentGenerationID] = true
			}
			continue
		}
		if g.Role == l1.NodeGenerationStaged && g.AgentGenerationID == r.receipt.Candidate.ID {
			continue
		}
		local, ok := c.Generations[g.AgentGenerationID]
		if !ok || seen[g.AgentGenerationID] || local.Role != string(g.Role) || local.Version != g.Registration.AgentVersion || local.Process == nil || local.Process.BootSessionID != g.BootSessionID || !hostboot.SameBoot(local.Process.SystemBootID, r.receipt.SystemBootID) || g.AuthorityGeneration <= 0 || len(g.RetirementProtocolVersions) == 0 || !slices.Contains(g.RetirementProtocolVersions, uint32(1)) {
			return rollingRefused(upgradecompat.InvalidInventory, "local and L1 live cohort differ")
		}
		if g.Role == l1.NodeGenerationPrimary && (c.Primary != g.AgentGenerationID || node.BootSessionID != g.BootSessionID || node.AuthorityGeneration != g.AuthorityGeneration) {
			return rollingRefused(upgradecompat.InvalidInventory, "primary authority changed")
		}
		if g.Role == l1.NodeGenerationRetiring && g.SelectedProtocol != 1 {
			return rollingRefused(upgradecompat.IncompatibleProtocols, "cohort transition acknowledgment is unavailable")
		}
		state, err := r.store.Manager.State(ctx, local.Label)
		if err != nil {
			return err
		}
		if !state.Running || state.PID != local.Process.PID {
			return rollingRefused(upgradecompat.InvalidInventory, "surviving generation process unavailable")
		}
		if err := r.store.VerifyRunning(ctx, g.AgentGenerationID, r.receipt.SystemBootID); err != nil {
			return generationControlTransportError(err)
		}
		seen[g.AgentGenerationID] = true
	}
	for id := range c.Generations {
		if id != r.receipt.Candidate.ID && !seen[id] {
			return rollingRefused(upgradecompat.InvalidInventory, "local generation omitted from L1 inventory")
		}
	}
	return nil
}
func (r *rollingUpgrade) persist(phase string) error {
	if err := writeUpgradeRolling(r.paths, &r.receipt, phase); err != nil {
		return err
	}
	if r.op.rollingStep != nil {
		return r.op.rollingStep(phase)
	}
	return nil
}

// Preserve the shared drain guard's default reports. Only rolling callers
// add their local gate reason to its explicit intent conflict.
func checkRollingUpgradeReplacement(node l1.Node, receipt *upgradeDrainReceipt) error {
	err := checkUpgradeReplacement(node, receipt, true)
	var refusal *apiResponseError
	if errors.As(err, &refusal) && refusal.APIError.Code == contract.ErrorConflict {
		return generationControlConflict(refusal.APIError.Message)
	}
	return err
}

func (r *rollingUpgrade) closeAdmission(ctx context.Context, node l1.Node, bound time.Duration, drain **upgradeDrainReceipt, work *[]l1.NodeActiveAttempt) error {
	if err := r.persist("prepared"); err != nil {
		return err
	}
	var err error
	*drain, err = beginUpgradeDrain(ctx, r.drainSnapshotHost(), r.paths, r.profile, node, bound)
	if err != nil {
		return err
	}
	r.inheritForwardAdmission(*drain)
	if err = r.persist("admission_closed"); err != nil {
		return err
	}
	current, err := r.op.observe(ctx, r.profile)
	if err != nil {
		return err
	}
	if err = checkRollingUpgradeReplacement(current, *drain); err != nil {
		return err
	}
	in, err := r.op.generations(ctx, r.profile)
	if err != nil {
		return err
	}
	if !sameRollingCohort(r.receipt.Inventory, in) {
		return rollingRefused(upgradecompat.InvalidInventory, "generation cohort changed after closing admission")
	}
	if err = r.validateInventory(ctx, in, current); err != nil {
		return err
	}
	r.receipt.Inventory = in
	if err = r.captureInventory(ctx, in); err != nil {
		return err
	}
	for _, a := range current.ActiveAttempts {
		kind, err := rollingWorkKind(a)
		if err != nil {
			return err
		}
		if kind == upgradecompat.OCIWork {
			return rollingRefused(upgradecompat.UnsupportedWork, "late OCI claim cannot survive rolling activation")
		}
		recordUpgradeWork(work, a)
		recordUpgradeWork(&r.receipt.Preserved, a)
		if a.Class == contract.JobClassService {
			(*drain).Services = append((*drain).Services, a)
		}
	}
	(*drain).OldBoot = r.primary.BootSessionID
	if err = writeUpgradeDrain(r.paths.config, *drain); err != nil {
		return err
	}
	if err = r.persist("services_quiescing"); err != nil {
		return err
	}
	proof := agent.UpgradeServiceQuiescence{IntentRevision: (*drain).Revision, IntentReason: (*drain).Reason, OperationID: r.receipt.OperationID, NodeID: node.NodeID, AgentGenerationID: r.primary.AgentGenerationID, BootSessionID: r.primary.BootSessionID, Services: slices.Clone((*drain).Services)}
	payload, _ := json.Marshal(proof)
	result, err := r.op.generationControl(ctx, r.store, agentinstall.ControlRequest{ID: r.receipt.OperationID, Action: "quiesce_services", NodeID: node.NodeID, GenerationID: r.primary.AgentGenerationID, BootSessionID: r.primary.BootSessionID, Payload: payload})
	if err != nil {
		return err
	}
	var accepted agent.UpgradeServiceQuiescence
	if err = json.Unmarshal(result, &accepted); err != nil {
		return err
	}
	if accepted.NodeID == "" || accepted.OperationID == "" || accepted.AgentGenerationID == "" || accepted.BootSessionID == "" {
		return errors.New("malformed service quiescence reply")
	}
	if !reflect.DeepEqual(proof, accepted) {
		return generationControlConflict("service quiescence returned different identities")
	}
	(*drain).ServiceQuiescence = &accepted
	if err = writeUpgradeDrain(r.paths.config, *drain); err != nil {
		return err
	}
	return r.persist("services_quiesced")
}
func sameRollingCohort(a, b l1.NodeGenerationInventory) bool {
	if a.NodeID != b.NodeID || a.CatalogRevision != b.CatalogRevision {
		return false
	}
	old := map[string]l1.NodeGeneration{}
	for _, g := range a.Generations {
		if g.Role != l1.NodeGenerationClosed {
			old[g.AgentGenerationID] = g
		}
	}
	for _, g := range b.Generations {
		h, known := old[g.AgentGenerationID]
		if g.Role == l1.NodeGenerationClosed {
			if known && h.Role != l1.NodeGenerationRetiring {
				return false
			}
			delete(old, g.AgentGenerationID)
			continue
		}
		if !known || g.BootSessionID != h.BootSessionID || g.AuthorityGeneration != h.AuthorityGeneration || g.Role != h.Role || !slices.Equal(g.RetirementProtocolVersions, h.RetirementProtocolVersions) {
			return false
		}
		delete(old, g.AgentGenerationID)
	}
	return len(old) == 0
}

func (r *rollingUpgrade) activate(ctx context.Context, base launchagent.Agent, source, version string, drain *upgradeDrainReceipt) (launchagent.Action, error) {
	if err := r.persist("candidate_preparing"); err != nil {
		return "", err
	}
	protocols := make([]int, len(r.protocols))
	for i, v := range r.protocols {
		protocols[i] = int(v)
	}
	candidate, err := r.store.PrepareCandidate(source, version, r.receipt.SystemBootID, protocols, r.receipt.OperationID)
	err = generationControlTransportError(err)
	if err != nil {
		return "", err
	}
	r.receipt.Candidate = candidate
	if candidate.Digest != strings.TrimPrefix(r.receipt.BinaryDigests["wefty-agent"], "sha256:") {
		return "", rollingRefused(upgradecompat.InvalidInventory, "candidate changed after protocol inspection")
	}
	if err = r.persist("candidate_loading"); err != nil {
		return "", err
	}
	started := r.op.now()
	action, err := r.store.LoadCandidate(ctx, candidate.ID, base)
	err = generationControlTransportError(err)
	if err != nil {
		return action, err
	}
	if err = r.waitReady(ctx, candidate, started); err != nil {
		return action, err
	}
	boundCatalog, err := readUpgradeCatalog(r.store)
	err = generationControlTransportError(err)
	if err != nil {
		return action, err
	}
	r.receipt.Candidate = boundCatalog.Generations[candidate.ID]
	if err = r.persist("candidate_ready"); err != nil {
		return action, err
	}
	return r.promoteReady(ctx, drain, action)
}

func (r *rollingUpgrade) promoteReady(ctx context.Context, drain *upgradeDrainReceipt, action launchagent.Action) (launchagent.Action, error) {
	candidate := r.receipt.Candidate
	version := candidate.Version
	node, err := r.op.observe(ctx, r.profile)
	if err != nil {
		return action, err
	}
	if err = checkRollingUpgradeReplacement(node, drain); err != nil {
		return action, err
	}
	in, err := r.op.generations(ctx, r.profile)
	if err != nil {
		return action, err
	}
	var staged *l1.NodeGeneration
	cohort := in
	cohort.Generations = slices.Clone(in.Generations)
	cohort.Generations = slices.DeleteFunc(cohort.Generations, func(g l1.NodeGeneration) bool {
		if g.AgentGenerationID == candidate.ID && g.Role == l1.NodeGenerationStaged {
			copy := g
			staged = &copy
			return true
		}
		return false
	})
	if staged != nil {
		if staged.BootSessionID != candidate.CandidateBootSessionID || staged.Registration.AgentVersion != candidate.Version || staged.Registration.NodeID != r.receipt.NodeID || staged.AuthorityGeneration != 0 || len(staged.ResidentAttempts) != 0 {
			return action, generationControlConflict("staged recovery candidate identity differs")
		}
		if r.receipt.StageRegistration != nil {
			expected := *r.receipt.StageRegistration
			if expected.SystemBootID != "" && hostboot.SameBoot(expected.SystemBootID, staged.Registration.SystemBootID) {
				expected.SystemBootID = staged.Registration.SystemBootID
			}
			expected.MissingCapabilities = append([]string{}, expected.MissingCapabilities...)
			actual := staged.Registration
			actual.MissingCapabilities = append([]string{}, actual.MissingCapabilities...)
			if !reflect.DeepEqual(expected, actual) {
				return action, generationControlConflict("staged recovery payload differs from the original request")
			}
		}
	}
	if err = r.store.VerifyRunning(ctx, candidate.ID, r.receipt.SystemBootID); err != nil {
		return action, generationControlTransportError(err)
	}
	if !sameRollingCohort(r.receipt.Inventory, cohort) {
		return action, rollingRefused(upgradecompat.InvalidInventory, "cohort changed before staging")
	}
	if err = r.validateInventory(ctx, in, node); err != nil {
		return action, err
	}
	for _, a := range node.ActiveAttempts {
		recordUpgradeWork(&r.receipt.Preserved, a)
	}
	// Stage only a ready candidate. Use current normalized Node facts; candidate
	// startup publishes its own observations after local activation is committed.
	registration := node.NodeRegistration
	registration.SystemBootID = r.receipt.SystemBootID
	registration.AgentGenerationID = candidate.ID
	registration.BootSessionID = candidate.CandidateBootSessionID
	registration.AgentVersion = version
	registration.CapabilityRevision++
	registration.CapabilityObservedAt = r.op.now()
	if staged != nil {
		registration = staged.Registration
		r.receipt.StageRegistration = &registration
	} else if r.receipt.StageRegistration != nil {
		registration = *r.receipt.StageRegistration
	} else {
		r.receipt.StageRegistration = &registration
	}
	if err = r.persist("candidate_staging"); err != nil {
		return action, err
	}
	if staged == nil {
		in, err = r.op.stageGeneration(ctx, r.profile, registration)
		if err != nil {
			return action, err
		}
	}
	if err = r.captureInventory(ctx, in); err != nil {
		return action, err
	}
	evidence := agentinstall.PromotionEvidence{ExpectedAuthorityGeneration: r.primary.AuthorityGeneration, IntentReason: drain.Reason, IntentRevision: drain.Revision, L1CatalogRevision: in.CatalogRevision, Residents: map[string][]agentinstall.ResidentAttempt{candidate.ID: {}}, ClosedGenerations: map[string]agentinstall.Binding{}, ServiceRecoveryKeys: map[string]string{}}
	c, err := readUpgradeCatalog(r.store)
	err = generationControlTransportError(err)
	if err != nil {
		return action, err
	}
	retirees := []string{}
	var residents []l1.ResidentAttempt
	for _, g := range in.Generations {
		if g.Role == l1.NodeGenerationClosed {
			if local, ok := c.Generations[g.AgentGenerationID]; ok {
				if local.Role != "retiring" || local.Process == nil || local.Process.BootSessionID != g.BootSessionID {
					return action, rollingRefused(upgradecompat.InvalidInventory, "closed history differs from local ownership")
				}
				evidence.ClosedGenerations[g.AgentGenerationID] = *local.Process
			}
			continue
		}
		if g.Role != l1.NodeGenerationPrimary && g.Role != l1.NodeGenerationRetiring {
			continue
		}
		evidence.Residents[g.AgentGenerationID] = []agentinstall.ResidentAttempt{}
		for _, a := range g.ResidentAttempts {
			evidence.Residents[g.AgentGenerationID] = append(evidence.Residents[g.AgentGenerationID], agentinstall.ResidentAttempt(a))
		}
		if g.Role == l1.NodeGenerationPrimary {
			residents = append([]l1.ResidentAttempt{}, g.ResidentAttempts...)
		} else {
			retirees = append(retirees, g.AgentGenerationID)
		}
	}
	for _, a := range drain.Services {
		evidence.ServiceRecoveryKeys[a.AttemptID] = upgradeServiceRecoveryKey(drain, a)
	}
	var receipt agentinstall.ActivationReceipt
	if c.PendingReceipt != "" {
		receipt, err = r.store.ReadActivationReceipt(c.PendingReceipt)
		if err == nil && (receipt.L1OperationID != r.receipt.OperationID || receipt.Candidate != candidate.ID || receipt.IntentRevision != drain.Revision) {
			err = generationControlConflict("pending activation belongs to another operation")
		}
	} else {
		receipt, err = r.store.BeginPromotion(candidate.ID, r.receipt.SystemBootID, r.receipt.OperationID, c.Revision, 1, evidence)
	}
	err = generationControlTransportError(err)
	if err != nil {
		return action, err
	}
	r.receipt.ActivationReceiptID = receipt.ID
	slices.Sort(retirees)
	request := l1.PromoteNodeGenerationRequest{OperationID: r.receipt.OperationID, ExpectedPrimaryGenerationID: r.primary.AgentGenerationID, ExpectedPrimaryBootSessionID: r.primary.BootSessionID, ExpectedAuthorityGeneration: r.primary.AuthorityGeneration, CandidateGenerationID: candidate.ID, CandidateBootSessionID: candidate.CandidateBootSessionID, ExpectedCatalogRevision: in.CatalogRevision, CatalogRevision: in.CatalogRevision + 1, IntentRevision: drain.Revision, SelectedProtocol: 1, RetiringGenerationIDs: retirees, ResidentAttempts: residents}
	if err = r.store.BindPromotionRequest(receipt.ID, request); err != nil {
		return action, generationControlTransportError(err)
	}
	r.receipt.Promotion = &request
	if err = r.persist("promotion_requested"); err != nil {
		return action, err
	}
	return r.finishPromotion(ctx, action)
}

func (r *rollingUpgrade) finishPromotion(ctx context.Context, action launchagent.Action) (launchagent.Action, error) {
	originalPhase := r.receipt.Phase
	acknowledged := slices.Contains([]string{"retirement_acknowledged", "survivor_renewal_proved", "installation_completing", "installation_complete", "admission_settling"}, originalPhase)
	proved := slices.Contains([]string{"survivor_renewal_proved", "installation_completing", "installation_complete", "admission_settling"}, originalPhase)
	request := *r.receipt.Promotion
	var promoted l1.NodeGenerationInventory
	var err error
	if r.receipt.PromotionResult != nil {
		promoted = *r.receipt.PromotionResult
	} else {
		// Conservative observation boundary: even a slow RPC must be followed by a
		// renewal accepted after its reply. Ambiguity leaves the exact request intact.
		promoted, err = r.op.promoteGeneration(ctx, r.profile, request)
		if err != nil {
			return action, err
		}
		if err = validateRollingPromotion(promoted, request, r.receipt.NodeID); err != nil {
			return action, err
		}
		r.receipt.PromotionResult = &promoted
		r.receipt.CutoverAt = r.op.now()
		if err = r.persist("promotion_committed"); err != nil {
			return action, err
		}
	}
	if err = validateRollingPromotion(promoted, request, r.receipt.NodeID); err != nil {
		return action, err
	}
	if err = r.store.CommitPromotion(ctx, r.receipt.ActivationReceiptID, request.OperationID, r.receipt.SystemBootID, promoted.CatalogRevision); err != nil {
		return action, generationControlTransportError(err)
	}
	if r.receipt.RecoveryState != "" {
		if err = r.waitRecoveryPrimary(ctx); err != nil {
			return action, err
		}
	}
	if !acknowledged {
		if err = r.persist("catalog_reconciled"); err != nil {
			return action, err
		}
		if err = r.waitRetirement(ctx, promoted); err != nil {
			return action, err
		}
		if err = r.persist("retirement_acknowledged"); err != nil {
			return action, err
		}
	}
	if !proved || r.receipt.RecoveryState != "" {
		if err = r.waitSurvivors(ctx, promoted, r.receipt.CutoverAt); err != nil {
			return action, err
		}
	}
	return action, r.persist("survivor_renewal_proved")
}
func validateRollingPromotion(in l1.NodeGenerationInventory, request l1.PromoteNodeGenerationRequest, expectedNode ...string) error {
	if in.NodeID == "" || in.CatalogRevision <= 0 || len(in.Generations) == 0 {
		return errors.New("malformed promotion result")
	}
	if len(expectedNode) > 0 && in.NodeID != expectedNode[0] {
		return generationControlConflict("promotion result node identity differs")
	}
	if in.CatalogRevision != request.CatalogRevision {
		return generationControlConflict("promotion result revision differs")
	}
	primary, old := false, false
	for _, g := range in.Generations {
		if g.AgentGenerationID == request.CandidateGenerationID {
			primary = g.Role == l1.NodeGenerationPrimary && g.BootSessionID == request.CandidateBootSessionID && g.PromotionOperationID == request.OperationID
		}
		if g.AgentGenerationID == request.ExpectedPrimaryGenerationID {
			old = g.Role == l1.NodeGenerationRetiring && g.PromotionOperationID == request.OperationID && slices.Equal(g.ResidentAttempts, request.ResidentAttempts)
		}
	}
	if !primary || !old {
		return generationControlConflict("promotion result identities differ")
	}
	return nil
}
func (r *rollingUpgrade) waitReady(ctx context.Context, g agentinstall.Generation, started time.Time) error {
	bounded, cancel := context.WithTimeout(ctx, upgradeReplacementBudget)
	defer cancel()
	for r.op.now().Sub(started) < upgradeReplacementBudget && bounded.Err() == nil {
		state, err := r.store.Manager.State(bounded, g.Label)
		if err != nil {
			return err
		}
		if state.Running {
			_, err = r.store.BindReady(bounded, g.ID, agentinstall.Binding{PID: state.PID, BootSessionID: g.CandidateBootSessionID, SystemBootID: r.receipt.SystemBootID}, started)
			if err == nil {
				return nil
			}
			var pending *agentinstall.RecordPendingError
			if !errors.As(err, &pending) {
				return generationControlTransportError(err)
			}
		}
		if err = r.op.wait(bounded, 100*time.Millisecond); err != nil {
			return err
		}
	}
	return errors.Join(errors.New("replacement generation readiness unverified"), bounded.Err())
}
func (r *rollingUpgrade) waitRetirement(ctx context.Context, in l1.NodeGenerationInventory) error {
	started := r.op.now()
	r.receipt.RetirementAcks = map[string]l1.NodeGeneration{}
	for r.op.now().Sub(started) < upgradeReplacementBudget && ctx.Err() == nil {
		complete := true
		for _, g := range in.Generations {
			if g.Role != l1.NodeGenerationRetiring {
				continue
			}
			ack, err := r.store.ReadRetirement(g.AgentGenerationID)
			var pending *agentinstall.RecordPendingError
			if errors.As(err, &pending) {
				complete = false
				continue
			}
			if err != nil {
				return generationControlTransportError(err)
			}
			if ack.AgentGenerationID == "" || ack.BootSessionID == "" || ack.PromotionOperationID == "" || ack.Role == "" {
				return errors.New("malformed generation retirement acknowledgment")
			}
			if ack.AgentGenerationID != g.AgentGenerationID || ack.BootSessionID != g.BootSessionID || ack.PromotionOperationID != g.PromotionOperationID || ack.Role != l1.NodeGenerationRetiring || ack.AuthorityGeneration != g.AuthorityGeneration || ack.SelectedProtocol != g.SelectedProtocol || !slices.Equal(ack.ResidentAttempts, g.ResidentAttempts) {
				return generationControlConflict("generation retirement acknowledgment identities differ")
			}
			r.receipt.RetirementAcks[g.AgentGenerationID] = ack
		}
		if complete {
			return nil
		}
		if err := r.op.wait(ctx, 100*time.Millisecond); err != nil {
			return err
		}
	}
	return errors.Join(errors.New("dedicated generation retirement acknowledgment unverified"), ctx.Err())
}

// Every frozen retiring member is checked by exact Job/Attempt, including
// those absent from active_attempts. An expired Lease or queued successor never
// passes. UpdatedAt is L1's accepted renewal time; a heartbeat cannot move it.
func (r *rollingUpgrade) waitSurvivors(ctx context.Context, in l1.NodeGenerationInventory, after time.Time) error {
	start := r.op.now()
	r.receipt.RenewalProof = map[string]l1.Attempt{}
	for r.op.now().Sub(start) < upgradeReplacementBudget && ctx.Err() == nil {
		complete := true
		for _, old := range r.receipt.Preserved {
			if old.Class == contract.JobClassService {
				continue
			}
			job, err := r.op.readJob(ctx, r.profile, old.JobID)
			if err != nil {
				return fmt.Errorf("observe surviving job %s attempt %s: %w", old.JobID, old.AttemptID, err)
			}
			if r.receipt.RecoveryState != "" {
				if job.JobID == "" {
					return errors.New("malformed surviving Job observation")
				}
				if job.JobID != old.JobID {
					return generationControlConflict("surviving Job identity differs")
				}
			}
			found := false
			for _, a := range job.Attempts {
				if a.AttemptID != old.AttemptID {
					continue
				}
				found = true
				terminal := (a.State == contract.AttemptSucceeded || a.State == contract.AttemptFailed) && a.Result != nil
				renewed := (a.State == contract.AttemptClaimed || a.State == contract.AttemptRunning || a.State == contract.AttemptAwaitingInput) && a.UpdatedAt.After(after) && a.LeaseExpiresAt.Add(-l1.DefaultLeaseDuration).After(after) && a.LeaseExpiresAt.After(r.op.now())
				if terminal || renewed {
					if renewed && r.receipt.RecoveryState != "" {
						if err := r.verifyRetiringProcess(ctx, old); err != nil {
							return err
						}
					}
					r.receipt.RenewalProof[a.AttemptID] = a
				} else {
					complete = false
				}
			}
			if !found {
				complete = false
			}
		}
		node, err := r.op.observe(ctx, r.profile)
		if err != nil {
			return err
		}
		expectedBoot := r.receipt.Candidate.CandidateBootSessionID
		if r.verifiedPrimaryBoot != "" {
			expectedBoot = r.verifiedPrimaryBoot
		}
		primaryHealthy := node.BootSessionID == expectedBoot && node.AgentVersion == r.receipt.Candidate.Version && node.State == contract.NodeAlive && node.LastHeartbeatAt.After(after)
		if complete && primaryHealthy {
			return nil
		}
		if err = r.op.wait(ctx, 100*time.Millisecond); err != nil {
			return err
		}
	}
	return errors.Join(errors.New("post-cutover Lease renewal of every retiring Attempt and new primary health unverified"), ctx.Err())
}

func requestGenerationControl(ctx context.Context, store agentinstall.Store, request agentinstall.ControlRequest) (json.RawMessage, error) {
	if request.RequestID == "" {
		request.RequestID = rand.Text()
	}
	if err := store.RequestControl(request); err != nil {
		return nil, generationControlTransportError(err)
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Minute+time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := store.ReadControlReply(request)
		if err == nil && (reflect.DeepEqual(result.Request, request) || (result.SuccessfulPromotion() && agentinstall.SamePromotionControl(result.Request, request))) {
			if result.Error != "" {
				if result.APIError != nil {
					apiError := *result.APIError
					if result.ServiceFailure != nil {
						// Retain the barrier context in both text and JSON reports.
						apiError.Message = result.Error
						apiError.Details = maps.Clone(apiError.Details)
						if apiError.Details == nil {
							apiError.Details = map[string]any{}
						}
						apiError.Details["service_failure"] = *result.ServiceFailure
					}
					return nil, &apiResponseError{Service: "L1", StatusCode: result.StatusCode, APIError: apiError}
				}
				return nil, &generationOperationFault{Message: result.Error}
			}
			return result.Payload, nil
		}
		var pending *agentinstall.RecordPendingError
		if err != nil && !errors.As(err, &pending) {
			return nil, generationControlTransportError(err)
		}
		select {
		case <-bounded.Done():
			return nil, bounded.Err()
		case <-ticker.C:
		}
	}
}

// Envelope validation is a local fence refusal; transport/storage failures
// keep their original errors and cannot be mistaken for an L1 gate decision.
func generationControlTransportError(err error) error {
	var catalogFault *upgradeCatalogFault
	if errors.As(err, &catalogFault) {
		return err
	}
	var refusal *agentinstall.ControlRequestRefusal
	var verification *agentinstall.VerificationRefusal
	if explicitGenerationControlRefusal(err) && (errors.As(err, &refusal) || errors.As(err, &verification)) {
		return generationControlConflict(err.Error())
	}
	return err
}

func explicitGenerationControlRefusal(err error) bool {
	switch e := err.(type) {
	case *agentinstall.VerificationRefusal, *agentinstall.ControlRequestRefusal:
		return true
	case interface{ Unwrap() []error }:
		causes := e.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !explicitGenerationControlRefusal(cause) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return explicitGenerationControlRefusal(e.Unwrap())
	default:
		return false
	}
}

func generationControlConflict(message string) error {
	return &apiResponseError{Service: "agent", StatusCode: http.StatusConflict, APIError: contract.APIError{Code: contract.ErrorConflict, Message: message, Details: map[string]any{"reason": string(upgradecompat.InvalidInventory)}}}
}

// The upgrade CLI never opens a node participant. Installed generation authority stays
// in the primary agent, reached through its closed, owner-private protocol.
func generationUpgradeControl(ctx context.Context, host setupHost, op upgradeHost, p connprofile.Profile, action string, payload any) (in l1.NodeGenerationInventory, err error) {
	paths, issues := resolveFleetMaintenancePaths(host.home, host)
	if len(issues) > 0 {
		return in, issues[0].err
	}
	store := upgradeGenerationStore(host, paths, op)
	catalog, err := readUpgradeCatalog(store)
	err = generationControlTransportError(err)
	if errors.Is(err, os.ErrNotExist) && action == "generation_inventory" {
		if _, statErr := op.statGenerationRoot(store.Root()); statErr == nil {
			return in, err // Existing storage is never legacy; retain the read cause.
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return in, statErr
		}
		// Positively identify legacy through the ordinary client projection. No
		// local catalog plus a missing observation is never an empty cohort.
		node, readErr := op.readNode(ctx, p)
		if readErr != nil {
			return in, readErr
		}
		if node.NodeID != p.Identities.Node || node.AgentGenerationID != "" {
			return in, generationControlConflict("legacy generation observation conflicts with node authority")
		}
		return l1.NodeGenerationInventory{NodeID: node.NodeID, Generations: []l1.NodeGeneration{}}, nil
	}
	if err != nil {
		return in, err
	}
	primary := catalog.Generations[catalog.Primary]
	if catalog.NodeID != p.Identities.Node || primary.Process == nil {
		return in, generationControlConflict("generation control lacks a bound local primary")
	}
	operation := rand.Text()
	if action != "generation_inventory" {
		receipt, readErr := readUpgradeRolling(paths)
		if readErr != nil {
			return in, readErr
		}
		if receipt.NodeID != catalog.NodeID || receipt.OperationID == "" {
			return in, generationControlConflict("generation control lacks an upgrade operation")
		}
		operation = receipt.OperationID
	}
	if action == "promote_generation" {
		request, ok := payload.(l1.PromoteNodeGenerationRequest)
		if !ok {
			return in, generationControlConflict("promotion control requires the typed request")
		}
		previous, readErr := store.ReadPromotionResult(request.ExpectedPrimaryGenerationID)
		var pending *agentinstall.RecordPendingError
		if readErr == nil && previous.SuccessfulPromotion() {
			body, encodeErr := json.Marshal(payload)
			if encodeErr != nil {
				return in, encodeErr
			}
			expected := agentinstall.ControlRequest{ID: operation, NodeID: catalog.NodeID, GenerationID: request.ExpectedPrimaryGenerationID, BootSessionID: request.ExpectedPrimaryBootSessionID, Action: action, Payload: body}
			if !agentinstall.SamePromotionControl(previous.Request, expected) {
				return in, generationControlConflict("promotion replay payload differs")
			}
			err = json.Unmarshal(previous.Payload, &in)
			return in, err
		}
		if readErr != nil && !errors.As(readErr, &pending) {
			return in, readErr
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return in, err
	}
	bounded, cancel := context.WithTimeout(ctx, upgradeMutationBudget)
	defer cancel()
	result, err := op.generationControl(bounded, store, agentinstall.ControlRequest{ID: operation, NodeID: catalog.NodeID, GenerationID: primary.ID, BootSessionID: primary.Process.BootSessionID, Action: action, Payload: body})
	if err != nil {
		return in, err
	}
	err = json.Unmarshal(result, &in)
	if err == nil && in.NodeID == "" {
		err = errors.New("generation control returned a malformed inventory without node identity")
	}
	if err == nil && in.NodeID != catalog.NodeID {
		err = generationControlConflict("generation control returned another node")
	}
	return in, err
}

func (r *rollingUpgrade) waitControlRenewal(ctx context.Context, baseline l1.Node, start, after time.Time) (time.Duration, error) {
	var diagnostic string
	var lastErr error
	r.receipt.ControlsRestartedAt = after
	r.receipt.ControlRenewalProof = map[string]l1.Attempt{}
	for r.op.now().Sub(start) < upgradeRenewalWindow && ctx.Err() == nil {
		node, err := r.op.observe(ctx, r.profile)
		if err == nil {
			diagnostic = fmt.Sprintf("observe=%v boot=%s/%s authority=%d/%d heartbeat=%s after=%s", err, node.BootSessionID, baseline.BootSessionID, node.AuthorityGeneration, baseline.AuthorityGeneration, node.LastHeartbeatAt, after)
			complete := node.BootSessionID == baseline.BootSessionID && node.AuthorityGeneration == baseline.AuthorityGeneration && node.LastHeartbeatAt.After(after)
			// Baseline is captured after service quiescence. Include every retiree's
			// frozen members as well, including Attempts absent from Node's active list.
			attempts := []l1.NodeActiveAttempt{}
			for _, old := range r.receipt.Preserved {
				if old.Class != contract.JobClassService {
					recordUpgradeWork(&attempts, old)
				}
			}
			for _, old := range baseline.ActiveAttempts {
				recordUpgradeWork(&attempts, old)
			}
			for _, g := range r.receipt.Inventory.Generations {
				if g.Role == l1.NodeGenerationRetiring {
					for _, a := range g.ResidentAttempts {
						recordUpgradeWork(&attempts, l1.NodeActiveAttempt{JobID: a.JobID, AttemptID: a.AttemptID})
					}
				}
			}
			for _, old := range attempts {
				var job l1.Job
				job, err = r.op.readJob(ctx, r.profile, old.JobID)
				if err != nil {
					err = fmt.Errorf("observe surviving job %s attempt %s: %w", old.JobID, old.AttemptID, err)
					break
				}
				found := false
				for _, a := range job.Attempts {
					if a.AttemptID != old.AttemptID {
						continue
					}
					found = true
					terminal := (a.State == contract.AttemptSucceeded || a.State == contract.AttemptFailed) && a.Result != nil
					renewed := a.UpdatedAt.After(after) && a.LeaseExpiresAt.After(old.LeaseExpiresAt) && a.LeaseExpiresAt.Add(-l1.DefaultLeaseDuration).After(after)
					if !terminal && !renewed && job.Outcome != contract.JobOutcomeCanceled {
						diagnostic += fmt.Sprintf(" attempt=%s state=%s updated=%s expires=%s old=%s", a.AttemptID, a.State, a.UpdatedAt, a.LeaseExpiresAt, old.LeaseExpiresAt)
						complete = false
					} else {
						r.receipt.ControlRenewalProof[a.AttemptID] = a
					}
				}
				if !found {
					complete = false
				}
			}
			if complete && err == nil && r.op.now().Sub(start) < upgradeRenewalWindow && ctx.Err() == nil {
				return r.op.now().Sub(start), nil
			}
		}
		if err != nil {
			var transient *transientExchangeError
			var refusal *apiResponseError
			// An explicit response wins even when another failed exchange is
			// joined into its cause chain. Only transport uncertainty retries.
			if errors.As(err, &refusal) || !errors.As(err, &transient) {
				return r.op.now().Sub(start), err
			}
			lastErr = err
		}
		if r.op.now().Sub(start) >= upgradeRenewalWindow || ctx.Err() != nil {
			break
		}
		if err = r.op.wait(ctx, 100*time.Millisecond); err != nil {
			lastErr = errors.Join(lastErr, err)
			break
		}
	}
	return r.op.now().Sub(start), errors.Join(fmt.Errorf("control-plane renewal of every surviving generation Attempt unverified (elapsed %s, %s)", r.op.now().Sub(start), diagnostic), lastErr, ctx.Err())
}

// Only an explicitly requested legacy drain enrolls the first immutable
// generation. The default upgrade continues through the original setup path.
func enrollUpgradeGeneration(ctx context.Context, host setupHost, op upgradeHost, paths setupPaths, p connprofile.Profile, base launchagent.Agent, source, version string, drain *upgradeDrainReceipt) (launchagent.Action, error) {
	store := upgradeGenerationStore(host, paths, op)
	boot, err := op.bootIdentity()
	if err != nil {
		return "", err
	}
	// Persist before stopping the legacy agent. A failed enrollment is never
	// sent through generic install rollback or assumed safe to repeat.
	r, err := readUpgradeRolling(paths)
	if err != nil {
		return "", err
	}
	if r.NodeID != p.Identities.Node || !hostboot.SameBoot(r.SystemBootID, boot) || r.Phase != "legacy_admission_closing" {
		return "", generationControlConflict("legacy enrollment intent changed")
	}
	r.LegacyBase = &base
	r.RecoveryDrain = drain
	if err = writeLegacyRolling(paths, op, r, "legacy_draining"); err != nil {
		return "", err
	}
	legacyPlist := filepath.Join(paths.launchAgentsRel, setupNodeAgentLabel+".plist")
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return "", err
	}
	defer root.Close()
	configRoot, err := root.OpenRoot(paths.configRel)
	if err != nil {
		return "", err
	}
	manifestPayload, err := agentinstall.ReadPrivateFile(configRoot, setupManifestName)
	configRoot.Close()
	if err != nil {
		return "", generationControlTransportError(err)
	}
	var manifest setupManifest
	if err = json.Unmarshal(manifestPayload, &manifest); err != nil {
		return "", err
	}
	plistRoot, err := root.OpenRoot(paths.launchAgentsRel)
	if err != nil {
		return "", err
	}
	plistPayload, err := agentinstall.ReadOwnedRegularFile(plistRoot, setupNodeAgentLabel+".plist", 0644)
	plistRoot.Close()
	if err != nil {
		return "", generationControlTransportError(err)
	}
	if manifest.Version != 1 || manifest.Files[legacyPlist] != payloadDigest(plistPayload) {
		return "", generationControlConflict("legacy generation enrollment lacks exact plist ownership")
	}
	oldNode, err := op.observe(ctx, p)
	if err != nil {
		return "", err
	}
	oldAttempts := slices.Clone(oldNode.ActiveAttempts)
	for _, service := range drain.Services {
		recordUpgradeWork(&oldAttempts, service)
	}
	r.Preserved = oldAttempts
	r.LegacyPlistDigest = payloadDigest(plistPayload)
	if err = writeLegacyRolling(paths, op, r, "legacy_stopping"); err != nil {
		return "", err
	}
	if err = store.Manager.Unload(ctx, setupNodeAgentLabel); err != nil {
		return "", err
	}
	if op.rollingStep != nil {
		if err = op.rollingStep("legacy_unloaded_before_plist_removal"); err != nil {
			return "", err
		}
	}
	started := op.now()
	for {
		settled := true
		for _, a := range oldAttempts {
			job, err := op.readJob(ctx, p, a.JobID)
			if err != nil {
				return "", err
			}
			if !upgradeAttemptSettled(job, a.AttemptID) {
				settled = false
			}
		}
		if settled {
			break
		}
		if op.now().Sub(started) >= upgradeReplacementBudget {
			return "", errors.New("legacy services have not settled before generation enrollment")
		}
		if err = op.wait(ctx, 100*time.Millisecond); err != nil {
			return "", err
		}
	}
	cleanup := rollingUpgrade{paths: paths, store: store, receipt: *r}
	if err = cleanup.removeLegacyAutostart(ctx); err != nil {
		return "", err
	}
	g, err := store.Initialize(p.Identities.Node, source, version, boot, []int{1}, r.OperationID)
	err = generationControlTransportError(err)
	if err != nil {
		return "", err
	}
	r.Candidate = g
	if g.Digest != strings.TrimPrefix(r.BinaryDigests["wefty-agent"], "sha256:") {
		return "", rollingRefused(upgradecompat.InvalidInventory, "candidate changed after protocol inspection")
	}
	if err = writeLegacyRolling(paths, op, r, "legacy_generation_loading"); err != nil {
		return "", err
	}
	action, err := store.LoadCandidate(ctx, g.ID, base)
	err = generationControlTransportError(err)
	if err != nil {
		return action, err
	}
	// Bind the real registered boot rather than the unused candidate boot ID.
	started = op.now()
	for op.now().Sub(started) < upgradeReplacementBudget && ctx.Err() == nil {
		node, observeErr := op.observe(ctx, p)
		state, stateErr := store.Manager.State(ctx, g.Label)
		if observeErr != nil {
			return action, observeErr
		}
		if stateErr != nil {
			return action, stateErr
		}
		if node.AgentGenerationID == g.ID && node.AgentVersion == version && node.State == contract.NodeAlive && node.LastHeartbeatAt.After(started) {
			ready, bindErr := store.BindReady(ctx, g.ID, agentinstall.Binding{PID: state.PID, BootSessionID: node.BootSessionID, SystemBootID: boot}, started)
			err = bindErr
			if err == nil {
				r.Candidate.Process = &ready.Binding
				return action, writeLegacyRolling(paths, op, r, "legacy_generation_ready")
			}
			var pending *agentinstall.RecordPendingError
			if !errors.As(err, &pending) {
				return action, generationControlTransportError(err)
			}
		}
		if err = op.wait(ctx, 100*time.Millisecond); err != nil {
			return action, err
		}
	}
	return action, errors.Join(errors.New("first installed generation readiness unverified"), ctx.Err())
}

func (r *rollingUpgrade) captureInventory(ctx context.Context, in l1.NodeGenerationInventory) error {
	if r.receipt.TrackedResidents == nil {
		r.receipt.TrackedResidents = map[string]l1.ResidentAttempt{}
	}
	for _, g := range in.Generations {
		if g.Role != l1.NodeGenerationPrimary && g.Role != l1.NodeGenerationRetiring {
			continue
		}
		for _, resident := range g.ResidentAttempts {
			if old, ok := r.receipt.TrackedResidents[resident.AttemptID]; ok && old != resident {
				return rollingRefused(upgradecompat.InvalidInventory, "captured resident identity changed")
			}
			job, err := r.op.readJob(ctx, r.profile, resident.JobID)
			if err != nil {
				return err
			}
			found := false
			for _, a := range job.Attempts {
				if a.AttemptID == resident.AttemptID {
					found = true
					recordUpgradeWork(&r.receipt.Preserved, l1.NodeActiveAttempt{JobID: resident.JobID, AttemptID: resident.AttemptID, BootSessionID: resident.BootSessionID, Kind: job.Spec.Kind, Class: job.Spec.Class, Restartable: job.Spec.Restartable, State: a.State, LeaseExpiresAt: a.LeaseExpiresAt})
				}
			}
			if !found {
				return rollingRefused(upgradecompat.InvalidInventory, "captured resident Attempt observation missing")
			}
			r.receipt.TrackedResidents[resident.AttemptID] = resident
		}
	}
	return nil
}

// Scope refusal still uses the shared gate, including its overlap precedence.
// With no local generation ownership this is a legacy installation: the scope
// decision needs no candidate execution or network enrollment. Existing local
// ownership requires authoritative L1 inventory, so closed history is excluded
// and a surviving retiree selects unsafe_drain_during_overlap.
func refuseUnsupportedRolling(ctx context.Context, host setupHost, paths setupPaths, op upgradeHost, profile connprofile.Profile, installation upgradecompat.Installation) error {
	input := upgradecompat.Input{Installation: installation}
	if _, err := op.statGenerationRoot(filepath.Join(paths.data, agentinstall.Directory)); errors.Is(err, os.ErrNotExist) {
		input.Live = []upgradecompat.Generation{{ID: profile.Identities.Node, Role: upgradecompat.Primary}}
	} else {
		if err != nil {
			return generationControlTransportError(err)
		}
		if _, err := readUpgradeCatalog(upgradeGenerationStore(host, paths, op)); err != nil {
			return generationControlTransportError(err)
		}
		bounded, cancel := context.WithTimeout(ctx, upgradeMutationBudget)
		defer cancel()
		inventory, err := op.generations(bounded, profile)
		if err != nil {
			return generationControlTransportError(err)
		}
		if inventory.NodeID != profile.Identities.Node {
			return rollingRefused(upgradecompat.InvalidInventory, "generation inventory node differs")
		}
		for _, g := range inventory.Generations {
			if g.Role == l1.NodeGenerationClosed {
				continue
			}
			input.Live = append(input.Live, upgradecompat.Generation{ID: g.AgentGenerationID, Role: upgradecompat.Role(g.Role), Protocols: g.RetirementProtocolVersions})
		}
	}
	decision := upgradecompat.Decide(input)
	return rollingRefused(decision.Reason, "this installation cannot use rolling activation; the default upgrade path is unchanged")
}

func writeLegacyRolling(paths setupPaths, op upgradeHost, r *upgradeRollingReceipt, phase string) error {
	if err := writeUpgradeRolling(paths, r, phase); err != nil {
		return err
	}
	if op.rollingStep != nil {
		return op.rollingStep(phase)
	}
	return nil
}

// A correlated reply proves the adapter finished this request. Transport
// timeout remains distinct because its L1 outcome may still be ambiguous.
type generationOperationFault struct{ Message string }

func (e *generationOperationFault) Error() string { return e.Message }
