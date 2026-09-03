package lima

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const (
	defaultSupervisorPollInterval = 5 * time.Second
	defaultLimaCommandTimeout     = 30 * time.Second
	defaultLimaRecoveryTimeout    = 5 * time.Minute
	defaultLimaRepairBackoff      = time.Second
	maximumLimaRepairBackoff      = 30 * time.Second
)

// InstanceState is the closed sanitized Lima lifecycle state.
type InstanceState string

const (
	InstanceUnknown InstanceState = "unknown"
	InstanceRunning InstanceState = "running"
	InstanceStopped InstanceState = "stopped"
	InstanceBroken  InstanceState = "broken"
)

func (state InstanceState) Valid() bool {
	switch state {
	case InstanceUnknown, InstanceRunning, InstanceStopped, InstanceBroken:
		return true
	default:
		return false
	}
}

// SupervisorFacts is the bounded supervisor observation exported to #128.
type SupervisorFacts struct {
	Instance    string                        `json:"instance"`
	State       InstanceState                 `json:"state"`
	Enabled     bool                          `json:"enabled"`
	Recovering  bool                          `json:"recovering"`
	ReasonCode  contract.CapabilityReasonCode `json:"reason_code,omitempty"`
	ObservedAt  time.Time                     `json:"observed_at"`
	RepairCount uint64                        `json:"repair_count"`
}

type timeoutContext func(context.Context, time.Duration) (context.Context, context.CancelFunc)

// SupervisorConfig supplies lifecycle commands, read-only intent, and clocks.
type SupervisorConfig struct {
	Instance        string
	Limactl         string
	Intent          IntentSource
	PollInterval    time.Duration
	CommandTimeout  time.Duration
	RecoveryTimeout time.Duration
	InitialBackoff  time.Duration
	run             commandRunner
	now             func() time.Time
	wait            func(context.Context, time.Duration) error
	withTimeout     timeoutContext
	Logf            func(string, ...any)
}

// Supervisor is the sole Lima lifecycle mutator in the macOS agent process.
type Supervisor struct {
	config SupervisorConfig

	ensureMu sync.Mutex
	mu       sync.RWMutex
	facts    SupervisorFacts
}

func NewSupervisor(config SupervisorConfig) (*Supervisor, error) {
	if !instanceNamePattern.MatchString(config.Instance) {
		return nil, errors.New("Lima supervisor requires a valid instance name")
	}
	if config.Limactl == "" {
		config.Limactl = "limactl"
	}
	if config.Intent == nil {
		return nil, errors.New("Lima supervisor requires a read-only OCI intent source")
	}
	if config.PollInterval <= 0 {
		config.PollInterval = defaultSupervisorPollInterval
	}
	if config.CommandTimeout <= 0 {
		config.CommandTimeout = defaultLimaCommandTimeout
	}
	if config.RecoveryTimeout <= 0 {
		config.RecoveryTimeout = defaultLimaRecoveryTimeout
	}
	if config.InitialBackoff <= 0 {
		config.InitialBackoff = defaultLimaRepairBackoff
	}
	if config.run == nil {
		config.run = runCommand
	}
	if config.now == nil {
		config.now = time.Now
	}
	if config.wait == nil {
		config.wait = waitForSupervisor
	}
	if config.withTimeout == nil {
		config.withTimeout = context.WithTimeout
	}
	intent, _ := config.Intent.ReadIntent(context.Background())
	facts := SupervisorFacts{
		Instance: config.Instance, State: InstanceUnknown, Enabled: intent.Enabled,
		ObservedAt: config.now().UTC().Round(0),
	}
	if !intent.Enabled {
		facts.ReasonCode = contract.CapabilityReasonOCIIntentDisabled
	}
	return &Supervisor{config: config, facts: facts}, nil
}

func (supervisor *Supervisor) Facts() SupervisorFacts {
	if supervisor == nil {
		return SupervisorFacts{State: InstanceUnknown, ReasonCode: contract.CapabilityReasonProbeFailed}
	}
	supervisor.mu.RLock()
	defer supervisor.mu.RUnlock()
	return supervisor.facts
}

func (supervisor *Supervisor) CapabilityReasonCode() contract.CapabilityReasonCode {
	reason := supervisor.Facts().ReasonCode
	if reason.Valid() {
		return reason
	}
	return contract.CapabilityReasonBootSweepFailed
}

func (supervisor *Supervisor) Ensure(ctx context.Context) error {
	if supervisor == nil {
		return errors.New("Lima supervisor is unavailable")
	}
	recoveryContext, cancel := supervisor.config.withTimeout(ctx, supervisor.config.RecoveryTimeout)
	defer cancel()
	supervisor.ensureMu.Lock()
	defer supervisor.ensureMu.Unlock()
	return supervisor.ensureWithin(recoveryContext)
}

// Stop enforces an already-persisted disabled intent. It is intentionally not
// an intent writer; the node-local controller must durably disable first.
func (supervisor *Supervisor) Stop(ctx context.Context) error {
	if supervisor == nil {
		return errors.New("Lima supervisor is unavailable")
	}
	recoveryContext, cancel := supervisor.config.withTimeout(ctx, supervisor.config.RecoveryTimeout)
	defer cancel()
	supervisor.ensureMu.Lock()
	defer supervisor.ensureMu.Unlock()
	intent, err := supervisor.readIntent(recoveryContext)
	if err != nil {
		return err
	}
	if intent.Enabled {
		return errors.New("Lima stop requires disabled OCI intent")
	}
	state, inspectErr := supervisor.inspect(recoveryContext)
	if inspectErr == nil && state != InstanceStopped {
		if err := supervisor.forceStop(recoveryContext); err != nil {
			supervisor.record(state, false, false, contract.CapabilityReasonOCIIntentDisabled, false)
			return err
		}
		state = InstanceStopped
	}
	supervisor.record(state, false, false, contract.CapabilityReasonOCIIntentDisabled, false)
	return inspectErr
}

func (supervisor *Supervisor) ensureWithin(ctx context.Context) error {
	intent, err := supervisor.readIntent(ctx)
	if err != nil || !intent.Enabled {
		return supervisor.enforceDisabled(ctx, err)
	}
	state, err := supervisor.inspect(ctx)
	if err != nil {
		if intentErr := supervisor.recheckEnabled(ctx, intent); intentErr != nil {
			return supervisor.cancelToStopped(ctx, InstanceUnknown, errors.Join(err, intentErr))
		}
		supervisor.record(InstanceUnknown, true, false, contract.CapabilityReasonProbeFailed, false)
		return err
	}
	if err := supervisor.recheckEnabled(ctx, intent); err != nil {
		return supervisor.cancelToStopped(ctx, state, err)
	}
	switch state {
	case InstanceRunning:
		supervisor.record(state, true, false, "", false)
		return nil
	case InstanceStopped:
		supervisor.record(state, true, true, contract.CapabilityReasonLimaStopped, false)
		return supervisor.startAndVerify(ctx, intent, false)
	case InstanceBroken:
		supervisor.record(state, true, true, contract.CapabilityReasonLimaBroken, false)
		return supervisor.repair(ctx, intent)
	default:
		supervisor.record(InstanceUnknown, true, false, contract.CapabilityReasonProbeFailed, false)
		return fmt.Errorf("Lima instance %q has unsupported state", supervisor.config.Instance)
	}
}

func (supervisor *Supervisor) recoveryNeeded(ctx context.Context, helperReady bool) bool {
	if supervisor == nil {
		return false
	}
	supervisor.ensureMu.Lock()
	defer supervisor.ensureMu.Unlock()
	intent, intentErr := supervisor.readIntent(ctx)
	state, inspectErr := supervisor.inspect(ctx)
	if intentErr != nil || !intent.Enabled {
		supervisor.record(state, false, false, contract.CapabilityReasonOCIIntentDisabled, false)
		return state == InstanceRunning || state == InstanceBroken
	}
	if inspectErr != nil {
		supervisor.record(InstanceUnknown, true, false, contract.CapabilityReasonProbeFailed, false)
		return true
	}
	reason := reasonForInstanceState(state)
	recovering := state != InstanceRunning || !helperReady
	if state == InstanceRunning && !helperReady {
		reason = contract.CapabilityReasonHelperUnreachable
	}
	supervisor.record(state, true, recovering, reason, false)
	return recovering
}

func (supervisor *Supervisor) enforceDisabled(ctx context.Context, intentErr error) error {
	state, inspectErr := supervisor.inspect(ctx)
	if inspectErr != nil || state == InstanceRunning || state == InstanceBroken {
		if stopErr := supervisor.forceStop(ctx); stopErr != nil {
			supervisor.record(state, false, false, contract.CapabilityReasonOCIIntentDisabled, false)
			return errors.Join(errors.New("OCI intent is disabled"), intentErr, stopErr)
		}
		state = InstanceStopped
	}
	supervisor.record(state, false, false, contract.CapabilityReasonOCIIntentDisabled, false)
	return errors.Join(errors.New("OCI intent is disabled"), intentErr, inspectErr)
}

func (supervisor *Supervisor) startAndVerify(ctx context.Context, expected OCIIntent, repaired bool) error {
	if err := supervisor.recheckEnabled(ctx, expected); err != nil {
		return supervisor.cancelToStopped(ctx, InstanceStopped, err)
	}
	_, startErr := supervisor.runBounded(ctx, "start", supervisor.config.Instance)
	if intentErr := supervisor.recheckEnabled(ctx, expected); intentErr != nil {
		return supervisor.cancelToStopped(ctx, InstanceRunning, errors.Join(startErr, intentErr))
	}
	if startErr != nil {
		reason := contract.CapabilityReasonLimaStopped
		if errors.Is(startErr, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			reason = contract.CapabilityReasonLimaStartTimeout
		}
		supervisor.record(InstanceStopped, true, false, reason, repaired)
		return fmt.Errorf("start Lima instance: %w", startErr)
	}
	state, err := supervisor.inspect(ctx)
	if err != nil {
		supervisor.record(InstanceUnknown, true, false, contract.CapabilityReasonLimaStartTimeout, repaired)
		return fmt.Errorf("verify started Lima instance: %w", err)
	}
	if err := supervisor.recheckEnabled(ctx, expected); err != nil {
		return supervisor.cancelToStopped(ctx, state, err)
	}
	if state != InstanceRunning {
		supervisor.record(state, true, false, contract.CapabilityReasonLimaStartTimeout, repaired)
		return errors.New("verify started Lima instance: instance is not running")
	}
	supervisor.record(InstanceRunning, true, false, "", repaired)
	return nil
}

func (supervisor *Supervisor) repair(ctx context.Context, expected OCIIntent) error {
	if err := supervisor.recheckEnabled(ctx, expected); err != nil {
		return supervisor.cancelToStopped(ctx, InstanceBroken, err)
	}
	if err := supervisor.forceStop(ctx); err != nil {
		if intentErr := supervisor.recheckEnabled(ctx, expected); intentErr != nil {
			supervisor.record(InstanceBroken, false, false, contract.CapabilityReasonOCIIntentDisabled, true)
			return errors.Join(err, intentErr)
		}
		supervisor.record(InstanceBroken, true, false, contract.CapabilityReasonLimaBroken, true)
		return fmt.Errorf("force-stop Broken Lima instance: %w", err)
	}
	if err := supervisor.recheckEnabled(ctx, expected); err != nil {
		supervisor.record(InstanceStopped, false, false, contract.CapabilityReasonOCIIntentDisabled, true)
		return err
	}
	backoff := supervisor.config.InitialBackoff
	for {
		if err := supervisor.startAndVerify(ctx, expected, false); err == nil {
			supervisor.record(InstanceRunning, true, false, "", true)
			return nil
		} else if disabledIntent(err) {
			return err
		}
		if err := supervisor.config.wait(ctx, backoff); err != nil {
			supervisor.record(InstanceStopped, true, false, contract.CapabilityReasonLimaStartTimeout, true)
			return fmt.Errorf("repair Lima instance: %w", err)
		}
		if err := supervisor.recheckEnabled(ctx, expected); err != nil {
			return supervisor.cancelToStopped(ctx, InstanceStopped, err)
		}
		backoff *= 2
		if backoff > maximumLimaRepairBackoff {
			backoff = maximumLimaRepairBackoff
		}
	}
}

func (supervisor *Supervisor) cancelToStopped(ctx context.Context, state InstanceState, cause error) error {
	if state != InstanceStopped {
		if err := supervisor.forceStop(ctx); err != nil {
			supervisor.record(state, false, false, contract.CapabilityReasonOCIIntentDisabled, false)
			return errors.Join(cause, fmt.Errorf("stop Lima after OCI disable: %w", err))
		}
	}
	supervisor.record(InstanceStopped, false, false, contract.CapabilityReasonOCIIntentDisabled, false)
	return errors.Join(errors.New("OCI intent was disabled during Lima recovery"), cause)
}

func (supervisor *Supervisor) readIntent(ctx context.Context) (OCIIntent, error) {
	intent, err := supervisor.config.Intent.ReadIntent(ctx)
	if err != nil || !intent.valid() {
		return OCIIntent{}, err
	}
	return intent, nil
}

func (supervisor *Supervisor) recheckEnabled(ctx context.Context, expected OCIIntent) error {
	intent, err := supervisor.readIntent(ctx)
	if err != nil {
		return err
	}
	if !intent.Enabled {
		return errors.New("OCI intent is disabled")
	}
	if intent.Revision != expected.Revision {
		return errors.New("OCI intent revision changed during recovery")
	}
	return nil
}

func disabledIntent(err error) bool {
	return err != nil && strings.Contains(err.Error(), "intent")
}

func (supervisor *Supervisor) inspect(ctx context.Context) (InstanceState, error) {
	output, err := supervisor.runBounded(ctx, "list", "--json", supervisor.config.Instance)
	if err != nil {
		return InstanceUnknown, fmt.Errorf("inspect Lima instance: %w", err)
	}
	return decodeInstanceState(output, supervisor.config.Instance)
}

func (supervisor *Supervisor) forceStop(ctx context.Context) error {
	_, err := supervisor.runBounded(ctx, "stop", "--force", supervisor.config.Instance)
	return err
}

func (supervisor *Supervisor) runBounded(ctx context.Context, arguments ...string) ([]byte, error) {
	commandContext, cancel := supervisor.config.withTimeout(ctx, supervisor.config.CommandTimeout)
	defer cancel()
	return supervisor.config.run(commandContext, supervisor.config.Limactl, arguments...)
}

func (supervisor *Supervisor) record(state InstanceState, enabled, recovering bool, reason contract.CapabilityReasonCode, repaired bool) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	changed := supervisor.facts.State != state || supervisor.facts.Enabled != enabled ||
		supervisor.facts.Recovering != recovering || supervisor.facts.ReasonCode != reason
	supervisor.facts.State = state
	supervisor.facts.Enabled = enabled
	supervisor.facts.Recovering = recovering
	supervisor.facts.ReasonCode = reason
	if changed || supervisor.facts.ObservedAt.IsZero() {
		supervisor.facts.ObservedAt = supervisor.config.now().UTC().Round(0)
	}
	if repaired {
		supervisor.facts.RepairCount++
	}
}

func reasonForInstanceState(state InstanceState) contract.CapabilityReasonCode {
	switch state {
	case InstanceStopped:
		return contract.CapabilityReasonLimaStopped
	case InstanceBroken:
		return contract.CapabilityReasonLimaBroken
	case InstanceRunning:
		return ""
	default:
		return contract.CapabilityReasonProbeFailed
	}
}

func decodeInstanceState(payload []byte, instance string) (InstanceState, error) {
	type limaInstance struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	for {
		var entry limaInstance
		if err := decoder.Decode(&entry); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return InstanceUnknown, errors.New("inspect Lima instance: invalid JSON")
		}
		if entry.Name != instance {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(entry.Status)) {
		case "running":
			return InstanceRunning, nil
		case "stopped":
			return InstanceStopped, nil
		case "broken":
			return InstanceBroken, nil
		default:
			return InstanceUnknown, nil
		}
	}
	return InstanceUnknown, fmt.Errorf("inspect Lima instance: instance %q is absent", instance)
}

func waitForSupervisor(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// SupervisedBootBarrier serializes Lima lifecycle and helper boot-sweep work.
type SupervisedBootBarrier struct {
	Supervisor *Supervisor
	Barrier    *ocihelper.BootBarrier

	cycleMu sync.Mutex
	mu      sync.RWMutex
	reason  contract.CapabilityReasonCode
}

// Stop quiesces before taking the shared Lima/helper cycle lock, then performs
// the VM transition under that lock. Holding cycleMu across quiescence would
// invert with an attempt's helper-loss recovery path, which also calls Ensure.
func (barrier *SupervisedBootBarrier) Stop(ctx context.Context, quiesce func(context.Context) error) error {
	if barrier == nil || barrier.Supervisor == nil || barrier.Barrier == nil || quiesce == nil {
		return errors.New("supervised OCI stop cycle is unavailable")
	}
	if err := quiesce(ctx); err != nil {
		return err
	}
	if err := lockSupervisorCycle(ctx, &barrier.cycleMu); err != nil {
		return err
	}
	defer barrier.cycleMu.Unlock()
	barrier.Barrier.Invalidate()
	return barrier.Supervisor.Stop(ctx)
}

func (barrier *SupervisedBootBarrier) Restart(ctx context.Context, quiesce, recover func(context.Context) error) error {
	if barrier == nil || barrier.Supervisor == nil || barrier.Barrier == nil || quiesce == nil || recover == nil {
		return errors.New("supervised OCI restart cycle is unavailable")
	}
	if err := quiesce(ctx); err != nil {
		return err
	}
	if err := lockSupervisorCycle(ctx, &barrier.cycleMu); err != nil {
		return err
	}
	barrier.Supervisor.ensureMu.Lock()
	intent, intentErr := barrier.Supervisor.readIntent(ctx)
	if intentErr == nil && intent.Enabled {
		intentErr = barrier.Supervisor.forceStop(ctx)
	}
	barrier.Barrier.Invalidate()
	barrier.Supervisor.ensureMu.Unlock()
	barrier.cycleMu.Unlock()
	if intentErr != nil {
		return intentErr
	}
	if !intent.Enabled {
		return errors.New("OCI intent is disabled")
	}
	return recover(ctx)
}

// Recreate replaces the Lima instance from an explicit setup template, then
// runs the ordinary helper/sweep/probe recovery before reporting success.
func (barrier *SupervisedBootBarrier) Recreate(ctx context.Context, quiesce, recover func(context.Context) error, template TemplateConfig) error {
	if barrier == nil || barrier.Supervisor == nil || barrier.Barrier == nil || quiesce == nil || recover == nil {
		return errors.New("supervised OCI recreate cycle is unavailable")
	}
	payload, err := RenderTemplate(template)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp("", ".wefty-lima-recreate-*.yaml")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := quiesce(ctx); err != nil {
		return err
	}
	if err := lockSupervisorCycle(ctx, &barrier.cycleMu); err != nil {
		return err
	}
	barrier.Supervisor.ensureMu.Lock()
	intent, intentErr := barrier.Supervisor.readIntent(ctx)
	if intentErr == nil && intent.Enabled {
		barrier.Barrier.Invalidate()
		if _, intentErr = barrier.Supervisor.runBounded(ctx, "delete", "--force", barrier.Supervisor.config.Instance); intentErr == nil {
			_, intentErr = barrier.Supervisor.runBounded(ctx, "start", "--name="+barrier.Supervisor.config.Instance, temporaryPath)
		}
	}
	barrier.Supervisor.ensureMu.Unlock()
	barrier.cycleMu.Unlock()
	if intentErr != nil {
		return intentErr
	}
	if !intent.Enabled {
		return errors.New("OCI intent is disabled")
	}
	return recover(ctx)
}

func (barrier *SupervisedBootBarrier) Ready() bool {
	return barrier != nil && barrier.Barrier != nil && barrier.Barrier.Ready()
}

func (barrier *SupervisedBootBarrier) Ensure(ctx context.Context) error {
	if barrier == nil || barrier.Supervisor == nil || barrier.Barrier == nil {
		return errors.New("supervised OCI boot barrier is unavailable")
	}
	if err := lockSupervisorCycle(ctx, &barrier.cycleMu); err != nil {
		return err
	}
	defer barrier.cycleMu.Unlock()
	recoveryContext, cancel := barrier.Supervisor.config.withTimeout(ctx, barrier.Supervisor.config.RecoveryTimeout)
	defer cancel()
	barrier.Supervisor.ensureMu.Lock()
	defer barrier.Supervisor.ensureMu.Unlock()
	if err := barrier.Supervisor.ensureWithin(recoveryContext); err != nil {
		return err
	}
	intent, err := barrier.Supervisor.readIntent(recoveryContext)
	if err != nil || !intent.Enabled {
		return barrier.Supervisor.cancelToStopped(recoveryContext, InstanceRunning, err)
	}
	err = barrier.ensureHelperReady(recoveryContext, intent)
	barrier.setReason(classifyHelperBarrierError(err))
	return err
}

func lockSupervisorCycle(ctx context.Context, mutex *sync.Mutex) error {
	if mutex.TryLock() {
		return nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if mutex.TryLock() {
				return nil
			}
		}
	}
}

func (barrier *SupervisedBootBarrier) ensureHelperReady(ctx context.Context, expected OCIIntent) error {
	backoff := barrier.Supervisor.config.InitialBackoff
	for {
		err := barrier.Barrier.Ensure(ctx)
		if err == nil {
			if intentErr := barrier.Supervisor.recheckEnabled(ctx, expected); intentErr != nil {
				barrier.Barrier.Invalidate()
				return barrier.Supervisor.cancelToStopped(ctx, InstanceRunning, intentErr)
			}
			return nil
		}
		reason := classifyHelperBarrierError(err)
		if reason == contract.CapabilityReasonHelperVersionMismatch || reason == contract.CapabilityReasonLocalPermissionDenied ||
			reason == contract.CapabilityReasonBootSweepFailed {
			return err
		}
		intentErr := barrier.Supervisor.recheckEnabled(ctx, expected)
		if intentErr != nil {
			return barrier.Supervisor.cancelToStopped(ctx, InstanceRunning, errors.Join(err, intentErr))
		}
		if waitErr := barrier.Supervisor.config.wait(ctx, backoff); waitErr != nil {
			break
		}
		backoff *= 2
		if backoff > maximumLimaRepairBackoff {
			backoff = maximumLimaRepairBackoff
		}
	}
	barrier.Barrier.Invalidate()
	stopContext, cancel := barrier.Supervisor.config.withTimeout(context.WithoutCancel(ctx), barrier.Supervisor.config.CommandTimeout)
	defer cancel()
	stopErr := barrier.Supervisor.forceStop(stopContext)
	barrier.Supervisor.record(InstanceStopped, true, false, contract.CapabilityReasonLimaStartTimeout, true)
	return errors.Join(errors.New("Lima helper readiness exceeded recovery deadline"), ctx.Err(), stopErr)
}

func (barrier *SupervisedBootBarrier) Invalidate() {
	if barrier == nil || barrier.Barrier == nil {
		return
	}
	barrier.cycleMu.Lock()
	barrier.Barrier.Invalidate()
	barrier.cycleMu.Unlock()
}

func (barrier *SupervisedBootBarrier) Run(ctx context.Context, recover func(context.Context) error) {
	if barrier == nil || barrier.Supervisor == nil || recover == nil {
		return
	}
	for {
		if err := barrier.Supervisor.config.wait(ctx, barrier.Supervisor.config.PollInterval); err != nil {
			return
		}
		barrier.cycleMu.Lock()
		needed := barrier.Supervisor.recoveryNeeded(ctx, barrier.Barrier.Ready())
		barrier.cycleMu.Unlock()
		if needed {
			if err := recover(ctx); err != nil {
				barrier.setReason(classifyHelperBarrierError(err))
				if barrier.Supervisor.config.Logf != nil {
					barrier.Supervisor.config.Logf("Lima background OCI convergence: %v", err)
				}
			}
		}
	}
}

func (barrier *SupervisedBootBarrier) Generation() (ocihelper.HelperSession, bool) {
	if barrier == nil || barrier.Barrier == nil {
		return ocihelper.HelperSession{}, false
	}
	return barrier.Barrier.Generation()
}

func (barrier *SupervisedBootBarrier) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	if barrier == nil || barrier.Barrier == nil {
		return ocihelper.VerifiedSweepReceipt{}, false
	}
	return barrier.Barrier.SweepReceipt()
}

func (barrier *SupervisedBootBarrier) Session() (*ocihelper.Session, error) {
	if barrier == nil || barrier.Barrier == nil {
		return nil, errors.New("supervised OCI boot barrier is unavailable")
	}
	return barrier.Barrier.Session()
}

func (barrier *SupervisedBootBarrier) SetLossHandler(handler func(ocihelper.HelperSession, error)) {
	if barrier != nil && barrier.Barrier != nil {
		barrier.Barrier.SetLossHandler(func(generation ocihelper.HelperSession, err error) {
			barrier.setReason(contract.CapabilityReasonHelperUnreachable)
			if handler != nil {
				handler(generation, err)
			}
		})
	}
}

func (barrier *SupervisedBootBarrier) Close() error {
	if barrier == nil || barrier.Barrier == nil {
		return nil
	}
	barrier.cycleMu.Lock()
	defer barrier.cycleMu.Unlock()
	return barrier.Barrier.Close()
}

func (barrier *SupervisedBootBarrier) CapabilityReasonCode() contract.CapabilityReasonCode {
	if barrier == nil || barrier.Supervisor == nil {
		return contract.CapabilityReasonBootSweepFailed
	}
	if reason := barrier.Supervisor.Facts().ReasonCode; reason.Valid() {
		return reason
	}
	barrier.mu.RLock()
	reason := barrier.reason
	barrier.mu.RUnlock()
	if reason.Valid() {
		return reason
	}
	return contract.CapabilityReasonBootSweepFailed
}

func (barrier *SupervisedBootBarrier) setReason(reason contract.CapabilityReasonCode) {
	if barrier == nil {
		return
	}
	barrier.mu.Lock()
	barrier.reason = reason
	barrier.mu.Unlock()
}

func classifyHelperBarrierError(err error) contract.CapabilityReasonCode {
	if err == nil {
		return ""
	}
	var unavailable *ocihelper.HelperUnitUnavailableError
	if errors.As(err, &unavailable) {
		return contract.CapabilityReasonHelperUnreachable
	}
	var rpcErr *ocihelper.RPCError
	if errors.As(err, &rpcErr) {
		switch rpcErr.Code {
		case ocihelper.CodeChecksumMismatch, ocihelper.CodeVersionMismatch:
			return contract.CapabilityReasonHelperVersionMismatch
		case ocihelper.CodePeerUnauthenticated:
			return contract.CapabilityReasonLocalPermissionDenied
		}
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "dial oci helper") {
		return contract.CapabilityReasonHelperUnreachable
	}
	if strings.Contains(message, "handshake") {
		return contract.CapabilityReasonHelperHandshakeFailed
	}
	return contract.CapabilityReasonBootSweepFailed
}
