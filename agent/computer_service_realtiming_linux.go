//go:build service_acceptance_realtiming && linux

package agent

import (
	"context"
	"net"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// RunComputerServiceRealtiming drives the production Computer publication
// controller from the native helper lane without exporting helper endpoints.
// It exists only in the explicitly attended realtiming build.
func RunComputerServiceRealtiming(
	ctx context.Context,
	runtimeAdapter WorkloadRuntime,
	request workloadrunner.Request,
	privateFabric fabric.Fabric,
	dial func(context.Context, string) (net.Conn, error),
	publish func(context.Context, bool, string) error,
) (contract.ProcessResult, error) {
	cache := NewComputerPolicyCache(systemClock{}, request.Authority.NodeID, request.Authority.BootSessionID)
	defer cache.Close()
	storageID, storageGeneration := "reference-storage", int64(1)
	for _, volume := range request.ManagedVolumes {
		if volume.ComputerStorage != nil {
			storageID, storageGeneration = volume.ComputerStorage.StorageID, volume.ComputerStorage.StorageGeneration
			break
		}
	}
	return runComputerService(ctx, runtimeAdapter, request, nil, computerServiceConfig{
		clock: systemClock{}, fabric: privateFabric, authorizer: cache, auditor: realtimingComputerAuditor{},
		computerID: "reference-computer", jobID: request.Authority.JobID, attemptID: request.Authority.AttemptID,
		storageID: storageID, storageGeneration: storageGeneration,
		fencingToken: request.Authority.FencingToken, dial: dial, publish: publish,
	})
}

type realtimingComputerAuditor struct{}

func (realtimingComputerAuditor) AppendComputerTakeoverAudit(
	context.Context, string, string, string, l1.ComputerTakeoverAuditRequest,
) (l1.ComputerTakeoverAuditReceipt, error) {
	return l1.ComputerTakeoverAuditReceipt{}, nil
}
