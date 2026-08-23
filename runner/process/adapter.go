package process

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// Executor is the native process mechanism behind Adapter. Keeping it narrow
// preserves deterministic process-runner fakes without making them pretend to
// support other workload kinds.
type Executor interface {
	Run(context.Context, Request, OutputSink) (contract.ProcessResult, error)
}

// Adapter implements the runtime-neutral WorkloadRuntime seam for
// kind=process. Guardian, logging, readiness, and idle behavior remain owned by
// the existing Executor.
type Adapter struct {
	executor      Executor
	bootSessionID string
	mu            sync.Mutex
	reaped        map[workloadrunner.AttemptAuthority]error
}

func NewAdapter(executor Executor) *Adapter {
	return &Adapter{executor: executor, reaped: make(map[workloadrunner.AttemptAuthority]error)}
}

// NewAdapterForBoot binds Guardian's agent-disconnect guarantee to the
// current process adapter boot. It is used by the production agent; tests and
// embedded callers that do not own a boot barrier use NewAdapter.
func NewAdapterForBoot(executor Executor, bootSessionID string) *Adapter {
	return &Adapter{
		executor: executor, bootSessionID: bootSessionID,
		reaped: make(map[workloadrunner.AttemptAuthority]error),
	}
}

func (adapter *Adapter) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	admission := workloadrunner.Admission{Request: request, Release: func() {}}
	if err := validateAuthority(request.Authority); err != nil {
		return admission, workloadrunner.Result{Outcome: spawnFailure(contract.SpawnFailureProcessRequest, err)}, err
	}
	if adapter == nil {
		err := errors.New("process runtime adapter is not configured")
		return admission, workloadrunner.Result{Outcome: spawnFailure(contract.SpawnFailureRuntimeUnavailable, err)}, err
	}
	adapter.recordReap(request.Authority, nil)
	if adapter.executor == nil {
		err := errors.New("process runtime executor is not configured")
		return admission, workloadrunner.Result{Outcome: spawnFailure(contract.SpawnFailureRuntimeUnavailable, err)}, err
	}
	if request.RuntimeHandler != "" {
		err := fmt.Errorf("runtime handler %q is not supported for process jobs", request.RuntimeHandler)
		return admission, workloadrunner.Result{Outcome: spawnFailure(contract.SpawnFailureUnsupportedRuntimeHandler, err)}, err
	}
	execution, cleanup, err := materializeExecutable(request.Execution, request.Authority.AttemptID)
	if err != nil {
		return admission, workloadrunner.Result{Outcome: spawnFailure(contract.SpawnFailureExecutableMaterialization, err)}, err
	}
	request.Execution = execution
	admission.Request = request
	admission.Release = cleanup
	return admission, workloadrunner.Result{}, nil
}

func (adapter *Adapter) Run(ctx context.Context, request workloadrunner.Request, sink workloadrunner.OutputSink) (workloadrunner.Result, error) {
	result, err := adapter.executor.Run(ctx, Request{
		AttemptID:        request.Authority.AttemptID,
		Execution:        request.Execution,
		Limits:           request.Limits,
		IdlePolicy:       IdlePolicy(request.IdlePolicy),
		CompletionSignal: request.CompletionSignal,
		Started:          request.Started,
		ServiceAddress:   request.ServiceAddress,
		ReadinessChanged: request.ReadinessChanged,
		Guarded:          request.LifetimeBoundary == workloadrunner.AgentBootLifetime,
	}, OutputSink(sink))
	adapter.recordReap(request.Authority, err)
	return workloadrunner.Result{Outcome: result}, err
}

// ReapAndVerify is a positive receipt over the process executor's ordinary
// return contract: Wait reaped the payload (and, for a guarded service, the
// Guardian reported its terminal result). A bounded process-reap timeout is
// retained as negative evidence instead of being promoted to a clean receipt.
func (adapter *Adapter) ReapAndVerify(_ context.Context, request workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	if err := validateAuthority(request.Authority); err != nil {
		return workloadrunner.ReapReceipt{}, err
	}
	if adapter == nil {
		return workloadrunner.ReapReceipt{}, errors.New("process reap verification has no adapter")
	}
	adapter.mu.Lock()
	reapErr, recorded := adapter.reaped[request.Authority]
	if recorded {
		delete(adapter.reaped, request.Authority)
	}
	adapter.mu.Unlock()
	if !recorded {
		return workloadrunner.ReapReceipt{}, fmt.Errorf("process reap verification has no evidence for attempt %q", request.Authority.AttemptID)
	}
	if errors.Is(reapErr, ErrProcessReapTimeout) {
		return workloadrunner.ReapReceipt{}, ErrProcessReapTimeout
	}
	return workloadrunner.ReapReceipt{
		RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt,
		BootSessionID: request.Authority.BootSessionID,
	}, nil
}

// ReapPriorBoot returns boot-scoped evidence only when both the managed-root
// record and this adapter agree that the payload belonged to an earlier boot.
// Guardian reaps every guarded process when its agent endpoint disconnects.
func (adapter *Adapter) ReapPriorBoot(_ context.Context, request workloadrunner.PriorBootReapRequest) (workloadrunner.ReapReceipt, error) {
	if adapter == nil || adapter.bootSessionID == "" {
		return workloadrunner.ReapReceipt{}, errors.New("process runtime has no boot-scoped Guardian authority")
	}
	if request.NodeID == "" || request.JobID == "" || request.PriorBootSessionID == "" || request.CurrentBootSessionID == "" {
		return workloadrunner.ReapReceipt{}, errors.New("prior-boot process reap requires node, job, prior boot, and current boot")
	}
	if request.CurrentBootSessionID != adapter.bootSessionID {
		return workloadrunner.ReapReceipt{}, errors.New("prior-boot process reap current boot does not match adapter boot")
	}
	if request.PriorBootSessionID == request.CurrentBootSessionID {
		return workloadrunner.ReapReceipt{}, errors.New("prior-boot process reap cannot verify a same-boot payload")
	}
	return workloadrunner.ReapReceipt{
		RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidencePriorBootGuardian,
		BootSessionID: request.PriorBootSessionID,
	}, nil
}

func (adapter *Adapter) recordReap(authority workloadrunner.AttemptAuthority, runErr error) {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.reaped == nil {
		adapter.reaped = make(map[workloadrunner.AttemptAuthority]error)
	}
	if errors.Is(runErr, ErrProcessReapTimeout) {
		adapter.reaped[authority] = ErrProcessReapTimeout
		return
	}
	adapter.reaped[authority] = nil
}

func validateAuthority(authority workloadrunner.AttemptAuthority) error {
	if authority.AttemptID == "" {
		return errors.New("process runtime authority requires an attempt ID")
	}
	return nil
}

var _ workloadrunner.WorkloadRuntime = (*Adapter)(nil)
var _ workloadrunner.PriorBootReaper = (*Adapter)(nil)
