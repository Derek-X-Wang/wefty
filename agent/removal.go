package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

var errServiceRemovalRequested = errors.New("service removal requested")

// removalController executes the node-scoped removal directive. Filesystem
// deletion remains entirely inside managedResourceManager; this type owns only
// the ordering between durable local intent, process reaping, spool cleanup,
// the guardrail call, and L1 attestation.
type removalController struct {
	client         *Client
	outbox         *evidenceOutbox
	managed        managedResourceManager
	session        *agentSession
	nodeID         string
	bootSessionID  string
	logf           func(string, ...any)
	beginRemoval   func(context.Context, localRemoval) error
	reapService    func(context.Context, string) (workloadrunner.ReapReceipt, error)
	clearReap      func(string)
	purgeJob       func(context.Context, string) error
	removeResource func(context.Context, localRemoval) error
	ackRemoval     func(context.Context, localRemoval) error
	finishRemoval  func(context.Context, localRemoval) error

	mu       sync.Mutex
	inflight map[string]struct{}
	wg       sync.WaitGroup
}

func newRemovalController(
	client *Client,
	outbox *evidenceOutbox,
	managed managedResourceManager,
	session *agentSession,
	nodeID, bootSessionID string,
	logf func(string, ...any),
) *removalController {
	controller := &removalController{
		client: client, outbox: outbox, managed: managed, session: session,
		nodeID: nodeID, bootSessionID: bootSessionID, logf: logf,
		inflight: make(map[string]struct{}),
	}
	if outbox != nil {
		controller.beginRemoval = outbox.beginRemoval
		controller.purgeJob = outbox.purgeJob
		controller.finishRemoval = outbox.completeRemoval
	}
	if session != nil {
		controller.reapService = session.reapServiceForRemoval
		controller.clearReap = session.clearRuntimeReap
	}
	if managed != nil {
		controller.removeResource = managed.remove
	}
	controller.ackRemoval = controller.acknowledge
	return controller
}

func (controller *removalController) enqueue(
	ctx context.Context,
	directive l1.RemovalDirective,
	failures chan<- destinationError,
) {
	if controller == nil || controller.managed == nil || controller.outbox == nil {
		return
	}
	key := removalKey(directive.JobID, directive.RemovalGeneration)
	controller.mu.Lock()
	if _, exists := controller.inflight[key]; exists {
		controller.mu.Unlock()
		return
	}
	controller.inflight[key] = struct{}{}
	controller.wg.Add(1)
	controller.mu.Unlock()

	go func() {
		defer controller.wg.Done()
		defer func() {
			controller.mu.Lock()
			delete(controller.inflight, key)
			controller.mu.Unlock()
		}()
		if err := controller.process(ctx, directive); err != nil && ctx.Err() == nil {
			classification := classifyAgentProtocolError(err)
			if classification.destination == errorDestinationNodeSession {
				select {
				case failures <- destinationError{destination: classification.destination, err: fmt.Errorf("agent: remove service %q: %w", directive.JobID, err)}:
				default:
				}
				return
			}
			controller.log("agent: remove service %q: %v", directive.JobID, err)
		}
	}()
}

func (controller *removalController) process(ctx context.Context, directive l1.RemovalDirective) error {
	if directive.BoundNodeID != controller.nodeID {
		return fmt.Errorf("removal directive belongs to node %q, not %q", directive.BoundNodeID, controller.nodeID)
	}
	removal := localRemoval{
		jobID: directive.JobID, generation: directive.RemovalGeneration,
		rootInstanceID: directive.RootInstanceID, cleanupFence: directive.CleanupFence,
	}
	// This FULL-synchronous SQLite write must precede any signal sent to the
	// guardian. A crash after it leaves an unambiguous local removing record.
	if err := controller.beginRemoval(ctx, removal); err != nil {
		return err
	}
	receipt, err := controller.reapService(ctx, directive.JobID)
	if err != nil {
		return err
	}
	if !receipt.RuntimeQuiesced || receipt.Evidence == "" {
		return fmt.Errorf("service %q removal has no positive runtime reap receipt", directive.JobID)
	}
	// TODO(#150): persist the runtime receipt in the removal record. Until then,
	// the legacy boolean remains the crash-resume compatibility marker only.
	removal.processTreeReaped = true
	if err := controller.purgeJob(ctx, directive.JobID); err != nil {
		return err
	}
	if err := controller.removeResource(ctx, removal); err != nil {
		return fmt.Errorf("delete managed service resource: %w", err)
	}
	if err := controller.ackRemoval(ctx, removal); err != nil {
		return err
	}
	if err := controller.finishRemoval(ctx, removal); err != nil {
		return err
	}
	if controller.clearReap != nil {
		controller.clearReap(directive.JobID)
	}
	return nil
}

// prepareAuthorityLoss closes the renewal-vs-heartbeat race for a running
// service. If L1 fenced the attempt because removal was requested, the
// renewal path fetches and persists that standing directive before it tells
// the lifecycle to signal the guardian. Other authority losses have no
// removal directive and retain their immediate reap behavior.
func (controller *removalController) prepareAuthorityLoss(ctx context.Context, jobID string) error {
	if controller == nil || controller.outbox == nil {
		return nil
	}
	response, err := controller.session.heartbeat(ctx)
	if err != nil {
		return err
	}
	for _, directive := range response.RemovalDirectives {
		if directive.JobID != jobID {
			continue
		}
		if directive.BoundNodeID != controller.nodeID {
			return fmt.Errorf("removal directive belongs to node %q, not %q", directive.BoundNodeID, controller.nodeID)
		}
		return controller.outbox.beginRemoval(ctx, localRemoval{
			jobID: directive.JobID, generation: directive.RemovalGeneration,
			rootInstanceID: directive.RootInstanceID, cleanupFence: directive.CleanupFence,
		})
	}
	return nil
}

func (controller *removalController) resume(ctx context.Context) error {
	if controller == nil || controller.managed == nil {
		return nil
	}
	completed, err := controller.managed.resumeRemovals(ctx)
	if err != nil {
		return fmt.Errorf("resume managed service removals: %w", err)
	}
	for _, removal := range completed {
		if err := controller.purgeJob(ctx, removal.jobID); err != nil {
			return err
		}
		if err := controller.ackRemoval(ctx, removal); err != nil {
			return err
		}
		if err := controller.finishRemoval(ctx, removal); err != nil {
			return err
		}
	}
	return nil
}

func (controller *removalController) acknowledge(ctx context.Context, removal localRemoval) error {
	request := l1.RemovalAcknowledgementRequest{
		NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence,
		RootInstanceID: removal.rootInstanceID,
		IdempotencyKey: removalAcknowledgementKey(removal, controller.bootSessionID),
	}
	if _, err := controller.client.AcknowledgeRemoval(ctx, removal.jobID, request); err != nil {
		return fmt.Errorf("acknowledge completed service removal: %w", err)
	}
	return nil
}

func (controller *removalController) wait() { controller.wg.Wait() }

func (controller *removalController) log(format string, args ...any) {
	if controller.logf != nil {
		controller.logf(format, args...)
	}
}

func removalKey(jobID string, generation uint64) string {
	return fmt.Sprintf("%s:%d", jobID, generation)
}

func removalAcknowledgementKey(removal localRemoval, bootSessionID string) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"%s\x00%d\x00%s\x00%s\x00%s",
		removal.jobID, removal.generation, removal.cleanupFence, removal.rootInstanceID, bootSessionID,
	)))
	return "removal:" + hex.EncodeToString(digest[:])
}
