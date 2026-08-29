package agent

import (
	"context"
	"fmt"
	"sync"

	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type custodyController struct {
	client         *Client
	exporter       workloadrunner.ComputerCustodyExporter
	nodeID         string
	bootSessionID  string
	rootInstanceID string
	logf           func(string, ...any)
	mu             sync.Mutex
	inflight       map[string]struct{}
	wg             sync.WaitGroup
}

func newCustodyController(client *Client, exporter workloadrunner.ComputerCustodyExporter,
	nodeID, bootSessionID, rootInstanceID string, logf func(string, ...any)) *custodyController {
	if exporter == nil {
		return nil
	}
	return &custodyController{client: client, exporter: exporter, nodeID: nodeID,
		bootSessionID: bootSessionID, rootInstanceID: rootInstanceID, logf: logf, inflight: make(map[string]struct{})}
}

func (controller *custodyController) process(ctx context.Context, directive l1.ComputerCustodyExportDirective) error {
	if directive.BoundNodeID != controller.nodeID {
		return fmt.Errorf("Custody export belongs to node %q, not %q", directive.BoundNodeID, controller.nodeID)
	}
	if directive.RootInstanceID != controller.rootInstanceID {
		return fmt.Errorf("Custody export belongs to managed-root instance %q, not %q", directive.RootInstanceID, controller.rootInstanceID)
	}
	receipt, err := controller.exporter.ExportComputerCustody(ctx, workloadrunner.ComputerCustodyExportRequest{
		ExportID: directive.ExportID, BackupID: directive.BackupID, CopyID: directive.CopyID,
		Storage: workloadrunner.ComputerStorage{ComputerID: directive.ComputerID, StorageID: directive.StorageID,
			StorageGeneration: directive.StorageGeneration, IntentRevision: directive.OperationRevision,
			DiskBytes: directive.AllocatedSize},
		SourceSize: directive.AllocatedSize, SourceDigest: directive.ContentDigest,
		ExternalPath: directive.ExternalPath, NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
		RootInstanceID: directive.RootInstanceID, OperationRevision: directive.OperationRevision,
		CustodyFence: directive.CustodyFence,
	})
	if err != nil {
		return err
	}
	_, err = controller.client.AcknowledgeComputerCustodyExport(ctx, directive.ComputerID,
		l1.ComputerCustodyExportAcknowledgementRequest{NodeID: controller.nodeID,
			BootSessionID: controller.bootSessionID, IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
	return err
}

func (controller *custodyController) enqueue(ctx context.Context, directive l1.ComputerCustodyExportDirective, failures chan<- destinationError) {
	if controller == nil {
		return
	}
	controller.mu.Lock()
	if _, exists := controller.inflight[directive.ExportID]; exists {
		controller.mu.Unlock()
		return
	}
	controller.inflight[directive.ExportID] = struct{}{}
	controller.wg.Add(1)
	controller.mu.Unlock()
	go func() {
		defer controller.wg.Done()
		defer func() {
			controller.mu.Lock()
			delete(controller.inflight, directive.ExportID)
			controller.mu.Unlock()
		}()
		if err := controller.process(ctx, directive); err != nil && ctx.Err() == nil {
			classification := classifyAgentProtocolError(err)
			if classification.destination == errorDestinationNodeSession && failures != nil {
				select {
				case failures <- destinationError{destination: classification.destination, err: err}:
				default:
				}
				return
			}
			if controller.logf != nil {
				controller.logf("agent: reconcile Custody export %q: %v", directive.ExportID, err)
			}
		}
	}()
}

func (controller *custodyController) wait() {
	if controller != nil {
		controller.wg.Wait()
	}
}
