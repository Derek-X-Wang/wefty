package agent

import (
	"fmt"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// WorkloadRuntime is the agent-owned, kind-selected execution seam.
type WorkloadRuntime = workloadrunner.WorkloadRuntime

type workloadRuntimeSet map[string]WorkloadRuntime

func newWorkloadRuntimeSet(configured map[string]WorkloadRuntime) (workloadRuntimeSet, error) {
	runtimes := make(workloadRuntimeSet, len(configured)+1)
	for kind, adapter := range configured {
		if kind == "" {
			return nil, fmt.Errorf("agent: workload runtime kind is required")
		}
		if adapter == nil {
			return nil, fmt.Errorf("agent: workload runtime %q is nil", kind)
		}
		if _, duplicate := runtimes[kind]; duplicate {
			return nil, fmt.Errorf("agent: workload runtime kind %q is duplicated", kind)
		}
		runtimes[kind] = adapter
	}
	return runtimes, nil
}

func (runtimes workloadRuntimeSet) selectKind(kind string) (WorkloadRuntime, bool) {
	adapter, found := runtimes[kind]
	return adapter, found && adapter != nil
}
