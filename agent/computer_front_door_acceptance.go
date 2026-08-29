//go:build service_acceptance

package agent

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

// ComputerFrontDoorAcceptanceConfig exposes only the seams needed for the
// tagged CLI acceptance test. Production construction remains private.
type ComputerFrontDoorAcceptanceConfig struct {
	AuthorityContext context.Context
	Fabric           fabric.Fabric
	Snapshot         l1.ComputerPolicySnapshot
	ComputerID       string
	JobID            string
	AttemptID        string
	FencingToken     string
	Dial             func(context.Context, string) (net.Conn, error)
	SetControlState  func(context.Context, bool) error
	Record           func(context.Context, l1.ComputerTakeoverAuditEvent) (l1.ComputerTakeoverAuditReceipt, error)
}

type acceptanceComputerAuditor struct {
	record func(context.Context, l1.ComputerTakeoverAuditEvent) (l1.ComputerTakeoverAuditReceipt, error)
}

func (auditor acceptanceComputerAuditor) AppendComputerTakeoverAudit(
	ctx context.Context, _, _, _ string, request l1.ComputerTakeoverAuditRequest,
) (l1.ComputerTakeoverAuditReceipt, error) {
	return auditor.record(ctx, request.Event)
}

// NewComputerFrontDoorAcceptanceHandler builds the real policy cache, tenure,
// relay, sideband, and audit path for tagged command acceptance.
func NewComputerFrontDoorAcceptanceHandler(config ComputerFrontDoorAcceptanceConfig) (http.Handler, error) {
	cache := NewComputerPolicyCache(systemClock{}, config.Snapshot.NodeID, config.Snapshot.BootSessionID)
	if _, err := cache.Install(config.Snapshot); err != nil {
		return nil, err
	}
	authority := config.AuthorityContext
	if authority == nil {
		authority = context.Background()
	}
	record := config.Record
	if record == nil {
		record = func(_ context.Context, event l1.ComputerTakeoverAuditEvent) (l1.ComputerTakeoverAuditReceipt, error) {
			event.AuthorityGeneration = 1
			return l1.ComputerTakeoverAuditReceipt{Event: event}, nil
		}
	}
	tenure, err := newControllerTenure(controllerTenureConfig{
		authorityContext: authority,
		dial:             config.Dial,
		setControlState:  config.SetControlState,
		record:           record,
	})
	if err != nil {
		return nil, err
	}
	frontDoor, err := newComputerFrontDoor(computerFrontDoorConfig{
		authorityContext: authority,
		fabric:           config.Fabric,
		authorizer:       cache,
		auditor:          acceptanceComputerAuditor{record: record},
		computerID:       config.ComputerID,
		jobID:            config.JobID,
		attemptID:        config.AttemptID,
		fencingToken:     config.FencingToken,
		dial:             config.Dial,
		controlTenure:    tenure,
		sessionCap:       time.Hour,
	})
	if err != nil {
		return nil, err
	}
	tenure.config.report = frontDoor.report
	frontDoor.SetReady(true)
	return frontDoor, nil
}
