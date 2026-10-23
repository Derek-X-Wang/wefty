package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/l1"
)

const (
	upgradeDrainTimeout        = 5 * time.Minute
	upgradeDrainPoll           = 5 * time.Second
	upgradeDrainRecoveryBudget = 10 * time.Second
	upgradeDrainJobReadBudget  = 2 * time.Second
	upgradeMutationBudget      = 5 * time.Second
	upgradeDrainMinimum        = 10 * time.Second
	upgradeIntentMargin        = 30 * time.Minute
	upgradeReasonMaxAge        = upgradeDrainTimeout + upgradeIntentMargin
	upgradeDeadlineField       = " drain_deadline="
	upgradeDrainFile           = "upgrade-drain.json"
	upgradeIntentPrefix        = "wefty upgrade "
	upgradeBusyMessage         = "busy, queued: upgrading; new runs wait and start when it ends"
)

type upgradeDrainReceipt struct {
	Home           string                 `json:"home"`
	Version        int                    `json:"version"`
	NodeID         string                 `json:"node_id"`
	Revision       int64                  `json:"intent_revision"`
	Reason         string                 `json:"intent_reason"`
	PreviousReason string                 `json:"previous_reason"`
	RestoreClaims  bool                   `json:"restore_claims"`
	OldBoot        string                 `json:"old_boot,omitempty"`
	Services       []l1.NodeActiveAttempt `json:"services,omitempty"`
	restarted      []string
	claimsDisabled bool
	counts         upgradeDrainCounts
	deadline       time.Time
	disableNotSent bool
}

func (receipt *upgradeDrainReceipt) ownsClaims(node l1.Node) bool {
	return receipt.RestoreClaims && node.NodeID == receipt.NodeID && node.IntentRevision == receipt.Revision && node.IntentReason == receipt.Reason && !node.ClaimsEnabled
}

func writeUpgradeDrain(config string, receipt *upgradeDrainReceipt) error {
	root, err := os.OpenRoot(config)
	if err != nil {
		return err
	}
	defer root.Close()
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	return durable.WriteFile(root, upgradeDrainFile, append(payload, '\n'), 0600)
}

func readUpgradeDrain(config string) (*upgradeDrainReceipt, error) {
	root, err := os.OpenRoot(config)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	info, err := root.Lstat(upgradeDrainFile)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("upgrade drain receipt must be a regular file")
	}
	payload, err := root.ReadFile(upgradeDrainFile)
	if err != nil {
		return nil, err
	}
	var receipt upgradeDrainReceipt
	if err := json.Unmarshal(payload, &receipt); err != nil {
		return nil, err
	}
	if receipt.Version != 1 || !filepath.IsAbs(receipt.Home) || receipt.NodeID == "" || receipt.Revision < 0 || (receipt.RestoreClaims && !strings.HasPrefix(receipt.Reason, upgradeIntentPrefix)) {
		return nil, errors.New("invalid upgrade drain receipt")
	}
	return &receipt, nil
}

func removeUpgradeDrain(config string) error {
	root, err := os.OpenRoot(config)
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.Remove(upgradeDrainFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return durable.SyncDir(root)
}

// An absolute deadline from the gap phase includes receipt persistence and the
// disable RPC. Duration-only callers retain the single-phase entry point.
func beginUpgradeDrain(ctx context.Context, op upgradeHost, paths setupPaths, profile connprofile.Profile, node l1.Node, timeout time.Duration, absoluteDeadline ...time.Time) (*upgradeDrainReceipt, error) {
	var deadline time.Time
	if len(absoluteDeadline) > 0 {
		deadline = absoluteDeadline[0]
	} else {
		deadline = op.now().Add(timeout)
	}
	left := deadline.Sub(op.now())
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if left <= 0 {
		return nil, upgradeDrainTimedOut(node.ClaimsEnabled, node.ActiveAttempts)
	}
	bounded, cancel := context.WithTimeout(ctx, left)
	defer cancel()
	receipt := &upgradeDrainReceipt{
		Home: paths.home, Version: 1, NodeID: node.NodeID, Revision: node.IntentRevision,
		Reason: node.IntentReason, PreviousReason: node.IntentReason, RestoreClaims: node.ClaimsEnabled,
		claimsDisabled: !node.ClaimsEnabled, deadline: deadline, disableNotSent: true,
	}
	budgetError := func() error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !op.now().Before(deadline) || errors.Is(bounded.Err(), context.DeadlineExceeded) {
			return upgradeDrainTimedOut(node.ClaimsEnabled && receipt.disableNotSent, node.ActiveAttempts)
		}
		return bounded.Err()
	}
	if node.ClaimsEnabled {
		receipt.Revision++
		receipt.Reason = upgradeIntentPrefix + rand.Text() + upgradeDeadlineField + deadline.UTC().Format(time.RFC3339Nano)
	}
	if err := budgetError(); err != nil {
		return nil, err
	}
	// Persist expected post-CAS ownership before sending the disable. A lost
	// response or process death still uses the existing receipt recovery.
	persist := op.writeDrain
	if persist == nil {
		persist = func(ctx context.Context, config string, r *upgradeDrainReceipt) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return writeUpgradeDrain(config, r)
		}
	}
	if err := persist(bounded, paths.config, receipt); err != nil {
		if budgetErr := budgetError(); budgetErr != nil {
			return receipt, budgetErr
		}
		return receipt, err
	}
	// A slow fsync must consume the budget, never create a fresh disable window.
	if err := budgetError(); err != nil {
		return receipt, err
	}
	if !receipt.RestoreClaims {
		receipt.disableNotSent = false
		return receipt, nil
	}
	mutationBudget := deadline.Sub(op.now())
	if mutationBudget <= 0 {
		return receipt, upgradeDrainTimedOut(node.ClaimsEnabled, node.ActiveAttempts)
	}
	mutation, cancelMutation := context.WithTimeout(bounded, mutationBudget)
	defer cancelMutation()
	if err := budgetError(); err != nil {
		return receipt, err
	}
	receipt.disableNotSent = false
	_, err := op.setClaims(mutation, profile, l1.NodeIntentRequest{ClaimsEnabled: false, IntentRevision: node.IntentRevision, Reason: receipt.Reason})
	if budgetErr := budgetError(); budgetErr != nil {
		return receipt, budgetErr
	}
	return receipt, err
}

// New reasons carry the configured deadline so remote readers need no receipt.
// A malformed deadline is not a legacy reason and must not extend busy status.
func upgradeReasonDeadline(reason string) (time.Time, error) {
	_, value, found := strings.Cut(reason, upgradeDeadlineField)
	if !found {
		return time.Time{}, nil
	}
	if len(reason) > 128 {
		return time.Time{}, errors.New("upgrade intent reason exceeds 128 bytes")
	}
	deadline, err := time.Parse(time.RFC3339Nano, value)
	if err == nil && deadline.IsZero() {
		err = errors.New("upgrade drain deadline must not be zero")
	}
	return deadline, err
}

func upgradeReasonActive(node l1.Node, now time.Time) bool {
	deadline, err := upgradeReasonDeadline(node.IntentReason)
	if err != nil {
		return false
	}
	if !deadline.IsZero() {
		return !now.After(deadline.Add(upgradeIntentMargin))
	}
	return node.IntentUpdatedAt != nil && now.Sub(*node.IntentUpdatedAt) <= upgradeReasonMaxAge
}

func waitUpgradeDrain(ctx context.Context, op upgradeHost, profile connprofile.Profile, receipt *upgradeDrainReceipt, yes bool, timeout time.Duration, out io.Writer, work *[]l1.NodeActiveAttempt) (l1.Node, error) {
	deadline := receipt.deadline
	if deadline.IsZero() {
		deadline = op.now().Add(timeout)
	}
	if receipt.RestoreClaims {
		if recorded, err := upgradeReasonDeadline(receipt.Reason); err == nil && !recorded.IsZero() {
			deadline = recorded
		}
	}
	bounded, cancel := context.WithTimeout(ctx, deadline.Sub(op.now()))
	defer cancel()
	first := true
	var active []l1.NodeActiveAttempt
	interruptions := map[string]*upgradeInterruption{}
	track := func(attempt l1.NodeActiveAttempt) {
		if !yes && attempt.Restartable && attempt.Class != contract.JobClassService && interruptions[attempt.AttemptID] == nil {
			interruptions[attempt.AttemptID] = &upgradeInterruption{
				attempt:  attempt,
				deadline: minTime(deadline, op.now().Add(agent.DefaultRenewalInterval+agent.ForcedShutdownBound(agent.ShutdownTimeouts{}))),
				key:      "upgrade:" + rand.Text(),
			}
		}
	}
	// Admission can close after an observed Attempt expires. Retain its exact
	// identity even when the first disabled snapshot no longer lists it.
	for _, attempt := range *work {
		track(attempt)
	}
	for {
		if !first && !yes && (!op.now().Before(deadline) || errors.Is(bounded.Err(), context.DeadlineExceeded)) && ctx.Err() == nil {
			return l1.Node{}, upgradeDrainTimedOut(false, active)
		}
		// Read after disabling, including claimed attempts committed just before CAS.
		// The initial read has the observer's own budget, even on an idle node.
		probe := bounded
		if first {
			probe = ctx
		}
		first = false
		node, err := op.observe(probe, profile)
		if err != nil {
			if ctx.Err() == nil && errors.Is(bounded.Err(), context.DeadlineExceeded) {
				return node, upgradeDrainTimedOut(false, active)
			}
			return node, err
		}
		if node.IntentRevision != receipt.Revision || node.ClaimsEnabled || node.IntentReason != receipt.Reason {
			return node, &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: "node intent changed during upgrade drain; installed stack unchanged"}}
		}
		active = node.ActiveAttempts
		receipt.counts = upgradeDrainCounts{}
		for _, stop := range interruptions {
			stop.active = false
		}
		for _, attempt := range node.ActiveAttempts {
			recordUpgradeWork(work, attempt)
			if attempt.Class == contract.JobClassService {
				continue
			}
			if !attempt.Restartable {
				receipt.counts.WaitingNonrestartable++
			} else if yes {
				receipt.counts.StoppingRestartable++
			} else {
				track(attempt)
				interruptions[attempt.AttemptID].active = true
			}
		}
		if yes {
			return node, ctx.Err()
		}
		// Initiate every stop before waiting for any of them. Retries always name
		// the same Attempt, boot, intent and key, including after a lost reply.
		for _, stop := range interruptions {
			if !stop.active || stop.acknowledged || stop.settled {
				continue
			}
			probe, cancel := context.WithTimeout(bounded, min(upgradeMutationBudget, stop.deadline.Sub(op.now())))
			_, err := op.interruptAttempt(probe, profile, stop.attempt.AttemptID, l1.InterruptRestartableAttemptRequest{
				JobID: stop.attempt.JobID, BootSessionID: stop.attempt.BootSessionID,
				IntentRevision: receipt.Revision, IdempotencyKey: stop.key, Reason: l1.InterruptionUpgrade,
			})
			cancel()
			if ctx.Err() != nil {
				return node, ctx.Err()
			}
			stop.acknowledged = err == nil
			// A natural exit or cancellation may race the request. Resolve even a
			// refused request by reading its exact Attempt below.
		}
		for _, stop := range interruptions {
			if stop.settled {
				if stop.pending {
					receipt.counts.RestartPending++
				}
				continue
			}
			probe, cancel := context.WithTimeout(bounded, min(upgradeMutationBudget, stop.deadline.Sub(op.now())))
			job, err := op.readJob(probe, profile, stop.attempt.JobID)
			cancel()
			if err == nil {
				stop.settled = upgradeAttemptSettled(job, stop.attempt.AttemptID)
				if stop.settled && job.State == contract.JobQueued {
					receipt.counts.RestartPending++
					stop.pending = true
				}
			}
			if !stop.settled {
				receipt.counts.StoppingRestartable++
				if !op.now().Before(stop.deadline) {
					return node, upgradeDrainTimedOut(false, active)
				}
			}
		}
		if _, err := fmt.Fprintf(out, "Draining: %d one-shot attempts remain; waiting_nonrestartable=%d stopping_restartable=%d restart_pending=%d; deadline %s.\n",
			receipt.counts.WaitingNonrestartable+receipt.counts.StoppingRestartable, receipt.counts.WaitingNonrestartable,
			receipt.counts.StoppingRestartable, receipt.counts.RestartPending, deadline.Format(time.RFC3339)); err != nil {
			return node, err
		}
		if receipt.counts.WaitingNonrestartable == 0 && receipt.counts.StoppingRestartable == 0 {
			return node, ctx.Err()
		}
		left := deadline.Sub(op.now())
		if left <= 0 || errors.Is(bounded.Err(), context.DeadlineExceeded) {
			return node, upgradeDrainTimedOut(false, active)
		}
		for _, stop := range interruptions {
			if !stop.settled {
				left = min(left, stop.deadline.Sub(op.now()))
			}
		}
		if err := op.wait(bounded, min(upgradeDrainPoll, left)); err != nil {
			if ctx.Err() != nil {
				return node, ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return node, upgradeDrainTimedOut(false, active)
			}
			return node, err
		}
	}
}

type upgradeInterruption struct {
	active       bool
	attempt      l1.NodeActiveAttempt
	deadline     time.Time
	key          string
	acknowledged bool
	settled      bool
	pending      bool
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func recordUpgradeWork(work *[]l1.NodeActiveAttempt, attempt l1.NodeActiveAttempt) {
	for _, old := range *work {
		if old.AttemptID == attempt.AttemptID {
			return
		}
	}
	*work = append(*work, attempt)
}

// Absence from active_attempts is insufficient: Lease expiry can hide a live
// process. Require positive settlement of the exact old Attempt. Natural exit
// and cancellation retain their normal terminal results.
func upgradeAttemptSettled(job l1.Job, id string) bool {
	for _, attempt := range job.Attempts {
		if attempt.AttemptID == id {
			return attempt.State == contract.AttemptInterrupted || attempt.State == contract.AttemptSucceeded || attempt.State == contract.AttemptFailed
		}
	}
	return false
}

// Revalidate after control-plane renewal, before publishing or stopping the node.
// An operator may reopen admission while we wait, and a late claim may appear.
func checkUpgradeReplacement(node l1.Node, receipt *upgradeDrainReceipt, yes bool) error {
	if node.NodeID != receipt.NodeID || node.ClaimsEnabled || node.IntentRevision != receipt.Revision || node.IntentReason != receipt.Reason {
		return &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: "node intent changed before node replacement; installed stack unchanged"}}
	}
	if !yes {
		for _, attempt := range node.ActiveAttempts {
			if attempt.Class != contract.JobClassService {
				return &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: "one-shot became active before node replacement; installed stack unchanged"}}
			}
		}
	}
	return nil
}

type upgradeDrainTimeoutError struct {
	apiResponseError
	blockingJobs []string
	hint         string
}

func (e *upgradeDrainTimeoutError) Unwrap() error { return &e.apiResponseError }

func (e *upgradeDrainTimeoutError) setRetryable(claimsOpen bool) {
	e.APIError.Retryable = claimsOpen
	e.APIError.Message = "drain timed out; installed stack unchanged"
	if claimsOpen {
		e.APIError.Message += e.hint
	}
}

func (e *upgradeDrainTimeoutError) setHint(blockers []string) {
	e.hint = "; try again when the running one-shots finish"
	if len(blockers) > 0 {
		e.hint += " (blocked by: " + strings.Join(blockers, ", ") + ")"
	}
}

func upgradeDrainTimedOut(claimsOpen bool, attempts []l1.NodeActiveAttempt) *upgradeDrainTimeoutError {
	var jobs []string
	for _, attempt := range attempts {
		if attempt.Class == contract.JobClassService || attempt.Restartable || attempt.JobID == "" {
			continue
		}
		if !slices.Contains(jobs, attempt.JobID) {
			jobs = append(jobs, attempt.JobID)
		}
		if len(jobs) == 3 {
			break
		}
	}
	err := &upgradeDrainTimeoutError{
		apiResponseError: apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict}},
		blockingJobs:     jobs,
	}
	err.setHint(jobs)
	err.setRetryable(claimsOpen)
	return err
}

// Resolve only the final retryable error, after intent restoration. The drain
// deadline has passed, so each best-effort read gets an independent budget.
func upgradeDrainRunHint(ctx context.Context, op upgradeHost, profile connprofile.Profile, result error) error {
	var timeout *upgradeDrainTimeoutError
	if !errors.As(result, &timeout) || !timeout.APIError.Retryable {
		return result
	}
	blockers := slices.Clone(timeout.blockingJobs)
	for i, jobID := range timeout.blockingJobs {
		probe, cancel := context.WithTimeout(context.WithoutCancel(ctx), upgradeDrainJobReadBudget)
		job, err := op.readJob(probe, profile, jobID)
		cancel()
		if err == nil {
			if runID := strings.TrimSpace(job.Spec.Labels[contract.LabelRunID]); runID != "" {
				blockers[i] = runID + " (" + jobID + ")"
			}
		}
	}
	timeout.setHint(blockers)
	timeout.setRetryable(true)
	return result
}

// Post-disable timeouts become retryable only after cleanup confirms open claims.
// A failed restore or a newer operator disable must retain the recovery outcome.
func upgradeDrainResult(primary, restore error, claimsOpen bool) error {
	var timeout *upgradeDrainTimeoutError
	if errors.As(primary, &timeout) {
		timeout.setRetryable(restore == nil && claimsOpen)
	}
	if restore != nil {
		return &upgradeCleanupError{primary: primary, restore: restore}
	}
	return primary
}

func recoverUpgradeDrain(ctx context.Context, op upgradeHost, paths setupPaths, profile connprofile.Profile, out io.Writer) error {
	receipt, err := readUpgradeDrain(paths.config)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read interrupted upgrade receipt: %w", err)
	}
	if receipt.NodeID != profile.Identities.Node {
		return errors.New("upgrade drain receipt names a different node")
	}
	if err := finishUpgradeDrain(ctx, op, paths, profile, receipt, true, out); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "Restored intent from interrupted upgrade.")
	return err
}

func finishUpgradeDrain(ctx context.Context, op upgradeHost, paths setupPaths, profile connprofile.Profile, receipt *upgradeDrainReceipt, recovering bool, out io.Writer) (resultErr error) {
	// This invocation knows no CAS was sent. Remove the receipt without an
	// admission fence; a receipt read after a crash keeps conservative recovery.
	if receipt.disableNotSent {
		return removeUpgradeDrain(paths.config)
	}
	defer func() {
		for _, jobID := range receipt.restarted {
			if _, err := fmt.Fprintln(out, "Restarted service", jobID); err != nil {
				resultErr = errors.Join(resultErr, err)
				break
			}
		}
	}()
	// Ctrl-C and rollback must not cancel cleanup. Bound it independently.
	recovery, cancel := context.WithTimeout(context.WithoutCancel(ctx), upgradeDrainRecoveryBudget)
	defer cancel()
	deadline := op.now().Add(upgradeDrainRecoveryBudget)
	var lastErr error
	for recovery.Err() == nil && op.now().Before(deadline) {
		node, err := op.readNode(recovery, profile)
		if err == nil {
			pendingDisable := receipt.RestoreClaims && node.ClaimsEnabled && node.IntentRevision == receipt.Revision-1 && node.IntentReason == receipt.PreviousReason
			// A new upgrade must not be held hostage by service-only failures.
			// Once our intent is settled, retry each remaining service once and
			// leave manual restart advice for any failure instead of blocking it.
			finalServiceFailures := recovering && !receipt.ownsClaims(node) && !pendingDisable
			serviceErr := resumeUpgradeServices(recovery, op, paths, profile, receipt, node, finalServiceFailures, out)
			reason := receipt.PreviousReason
			if strings.TrimSpace(reason) == "" {
				reason = "wefty upgrade completed; claims restored"
			}
			if receipt.ownsClaims(node) {
				_, err = op.setClaims(recovery, profile, l1.NodeIntentRequest{ClaimsEnabled: true, IntentRevision: receipt.Revision, Reason: reason})
			} else if pendingDisable {
				// The disable response may have been lost before its CAS committed.
				// Fence that pending CAS with an enabled intent at the same old
				// revision. Whichever CAS wins, a re-read resolves it before deletion.
				_, err = op.setClaims(recovery, profile, l1.NodeIntentRequest{ClaimsEnabled: true, IntentRevision: node.IntentRevision, Reason: reason})
			} else if err == nil && serviceErr == nil {
				receipt.claimsDisabled = !node.ClaimsEnabled
				return removeUpgradeDrain(paths.config)
			}
			if err == nil && serviceErr == nil {
				continue // Confirm the restoration/fence revision before deleting evidence.
			}
			err = errors.Join(err, serviceErr)
			// Resolve ambiguous replies by re-reading. A newer intent is never
			// overwritten, even if an operator races the restoration CAS.
		}
		lastErr = err
		if err := op.wait(recovery, min(time.Second, deadline.Sub(op.now()))); err != nil {
			lastErr = err
			break
		}
	}
	return fmt.Errorf("restore upgrade intent failed; receipt kept at %s; run wefty upgrade to recover: %w", filepath.Join(paths.config, upgradeDrainFile), lastErr)
}

func withUpgradeClients(ctx context.Context, profile connprofile.Profile, action func(context.Context, *apiClients) error) error {
	participant, closeFabric, err := fabricconfig.Open(ctx, fabricconfig.Config{Mode: "plain", PlainFabricID: profile.PlainFabricID, Identity: fabric.Identity{NodeID: profile.Identities.CLI, Tags: []string{l1.DefaultClientPrincipalTag}}})
	if closeFabric != nil {
		defer closeFabric()
	}
	if err != nil {
		return err
	}
	clients, err := newAPIClients(participant, profile.L1Address, profile.L3Address)
	if err != nil {
		return err
	}
	defer clients.close()
	probe, cancel := context.WithTimeout(ctx, upgradeMutationBudget)
	defer cancel()
	return action(probe, clients)
}

func setUpgradeClaims(ctx context.Context, profile connprofile.Profile, request l1.NodeIntentRequest) (node l1.Node, err error) {
	err = withUpgradeClients(ctx, profile, func(ctx context.Context, clients *apiClients) error {
		var err error
		node, err = clients.setNodeClaims(ctx, profile.Identities.Node, request)
		return err
	})
	return
}

func interruptUpgradeAttempt(ctx context.Context, profile connprofile.Profile, attempt string, request l1.InterruptRestartableAttemptRequest) (job l1.Job, err error) {
	err = withUpgradeClients(ctx, profile, func(ctx context.Context, clients *apiClients) error {
		var err error
		job, err = clients.interruptRestartableAttempt(ctx, profile.Identities.Node, attempt, request)
		return err
	})
	return
}

func restartUpgradeService(ctx context.Context, profile connprofile.Profile, jobID, key string) (job l1.Job, err error) {
	err = withUpgradeClients(ctx, profile, func(ctx context.Context, clients *apiClients) error {
		var err error
		job, err = clients.restartService(ctx, jobID, key)
		return err
	})
	return
}

// A node replacement is our interruption, including for restart=never. Use the
// existing explicit-restart API, whose durable key also covers lost replies.
// Keep the binding, data directory, spec and published-port reservation intact.
func resumeUpgradeServices(ctx context.Context, op upgradeHost, paths setupPaths, profile connprofile.Profile, receipt *upgradeDrainReceipt, node l1.Node, finalFailures bool, out io.Writer) error {
	if receipt.OldBoot == "" || (node.BootSessionID == receipt.OldBoot && node.State == contract.NodeAlive) {
		return nil
	}
	for len(receipt.Services) > 0 {
		old := receipt.Services[0]
		job, err := op.readJob(ctx, profile, old.JobID)
		if err == nil && job.ServiceJob != nil && job.ComputerID == "" && job.BoundNodeID == receipt.NodeID && job.DesiredState == contract.ServiceDesiredRunning && job.Removal == nil && job.PolicyStop == nil && (job.CurrentAttemptID == "" || job.CurrentAttemptID == old.AttemptID) {
			_, err = op.restartService(ctx, profile, old.JobID, receipt.Reason+":"+old.AttemptID)
			if err == nil {
				receipt.restarted = append(receipt.restarted, old.JobID)
			}
		} else if err == nil && job.ServiceJob != nil && job.DesiredState == contract.ServiceDesiredRunning && (job.State == contract.JobClaimed || job.State == contract.JobRunning) && job.CurrentAttemptID != "" && job.CurrentAttemptID != old.AttemptID {
			receipt.restarted = append(receipt.restarted, old.JobID)
		}
		if err != nil {
			if !finalFailures && !permanentUpgradeRefusal(err) {
				return err
			}
			if _, err := fmt.Fprintf(out, "Warning: service %s restart refused: %v; restart manually: wefty services restart %s --idempotency-key upgrade-recovery-%s\n", old.JobID, err, old.JobID, old.AttemptID); err != nil {
				return err
			}
		}
		receipt.Services = receipt.Services[1:]
		if err := writeUpgradeDrain(paths.config, receipt); err != nil {
			// Keep the in-memory queue consistent with the durable retry evidence.
			receipt.Services = append([]l1.NodeActiveAttempt{old}, receipt.Services...)
			return err
		}
	}
	return nil
}

func permanentUpgradeRefusal(err error) bool {
	var response *apiResponseError
	return errors.As(err, &response) && (response.APIError.Code == contract.ErrorCapacityExhausted ||
		response.StatusCode >= 400 && response.StatusCode < 500 && response.StatusCode != 408 && response.StatusCode != 429 && !response.APIError.Retryable)
}

type upgradeCleanupError struct{ primary, restore error }

func (e *upgradeCleanupError) Error() string { return errors.Join(e.primary, e.restore).Error() }

// Cleanup must remain visible without replacing the original command's exit code.
func (e *upgradeCleanupError) Unwrap() error {
	if e.primary != nil {
		return e.primary
	}
	return e.restore
}

func readUpgradeIntent(ctx context.Context, profile connprofile.Profile) (node l1.Node, err error) {
	err = withUpgradeClients(ctx, profile, func(ctx context.Context, clients *apiClients) error {
		var err error
		node, err = clients.getNode(ctx, profile.Identities.Node)
		return err
	})
	return
}
