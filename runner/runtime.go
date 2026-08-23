// Package runner defines the agent-owned seam between workload lifecycle
// policy and kind-specific execution mechanics.
package runner

import (
	"context"

	"github.com/Derek-X-Wang/wefty/contract"
)

// IdlePolicy controls whether a workload is supervised for output inactivity.
// Its zero value preserves the one-shot hang guard.
type IdlePolicy uint8

const (
	MonitorIdle IdlePolicy = iota
	IgnoreIdle
)

// LifetimeBoundary describes the runtime ownership required by the agent. It
// is deliberately mechanical: workload class remains agent lifecycle policy
// and never selects or enters a runtime adapter.
type LifetimeBoundary uint8

const (
	CallerLifetime LifetimeBoundary = iota
	AgentBootLifetime
)

// AttemptAuthority is the runtime-neutral identity and fence for one attempt.
type AttemptAuthority struct {
	NodeID        string
	BootSessionID string
	JobID         string
	AttemptID     string
	FencingToken  string
}

// ManagedResources is an opaque agent-owned handle. Adapters must not infer
// workload class or depend on host paths hidden behind the handle.
type ManagedResources any

// Request contains the mechanics needed by any workload runtime. Kind and
// class are intentionally absent: kind selected the adapter already, while
// class-specific lifecycle policy was compiled into these fields by the agent.
type Request struct {
	Authority        AttemptAuthority
	RuntimeHandler   string
	Execution        contract.ExecutionSpec
	Limits           *contract.JobLimits
	ManagedResources ManagedResources
	LifetimeBoundary LifetimeBoundary
	IdlePolicy       IdlePolicy
	CompletionSignal <-chan struct{}
	Started          func()
	// TODO(#147): replace this process TCP backend with an adapter-supplied,
	// opaque dial endpoint.
	ServiceAddress   string
	ReadinessChanged func(startupSatisfied, ready bool)
}

// Result is the runtime-neutral structured outcome of one workload.
type Result struct {
	Outcome contract.ProcessResult
}

// Admission is the adapter-prepared request and its owned cleanup. Preflight
// runs before the agent acquires managed resources, ports, bridges, or sinks.
type Admission struct {
	Request Request
	Release func()
}

// ReapRequest identifies the runtime resources that must be absent before an
// attempt may finish or a removal may be acknowledged.
type ReapRequest struct {
	Authority        AttemptAuthority
	ManagedResources ManagedResources
}

// ReapEvidence names the authority behind a positive quiescence receipt.
type ReapEvidence string

const (
	ReapEvidenceAttempt           ReapEvidence = "attempt"
	ReapEvidencePriorBootGuardian ReapEvidence = "prior_boot_guardian"
)

// ReapReceipt is positive evidence that runtime-owned workload state is gone.
type ReapReceipt struct {
	RuntimeQuiesced bool
	Evidence        ReapEvidence
	BootSessionID   string
}

// PriorBootReapRequest asks a runtime to verify the boot boundary for a
// service whose attempt was owned by an earlier agent boot.
type PriorBootReapRequest struct {
	NodeID               string
	JobID                string
	PriorBootSessionID   string
	CurrentBootSessionID string
}

// PriorBootReaper is optional because not every runtime has boot-barrier
// evidence. Process derives it from Guardian disconnect; OCI will derive it
// from the helper boot sweep.
type PriorBootReaper interface {
	ReapPriorBoot(context.Context, PriorBootReapRequest) (ReapReceipt, error)
}

// OutputSink receives raw output events. Calls may be concurrent across
// streams, so implementations must provide any required synchronization.
type OutputSink interface {
	WriteOutput(context.Context, contract.LogEvent) error
}

// OutputSinkFunc adapts a function into an OutputSink.
type OutputSinkFunc func(context.Context, contract.LogEvent) error

func (function OutputSinkFunc) WriteOutput(ctx context.Context, event contract.LogEvent) error {
	return function(ctx, event)
}

// WorkloadRuntime executes one open workload kind and positively verifies
// that its runtime-owned resources have been reaped.
type WorkloadRuntime interface {
	Preflight(context.Context, Request) (Admission, Result, error)
	Run(context.Context, Request, OutputSink) (Result, error)
	ReapAndVerify(context.Context, ReapRequest) (ReapReceipt, error)
}
