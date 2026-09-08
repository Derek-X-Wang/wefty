package agent

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestComputerPublicationOperationPreservesCallerAuthority(t *testing.T) {
	for _, mode := range []string{"earlier_execution_deadline", "expired_execution_deadline", "canceled_without_deadline"} {
		t.Run(mode, func(t *testing.T) {
			type valueKey struct{}
			execution := context.WithValue(t.Context(), valueKey{}, "retained")
			// Remove the test runner's deadline only for the explicit no-deadline case.
			if mode == "canceled_without_deadline" {
				execution = context.WithoutCancel(execution)
			}
			var deadline time.Time
			var cancelExecution context.CancelFunc
			switch mode {
			case "earlier_execution_deadline":
				deadline = time.Now().Add(3 * DefaultPublicationRetryInterval)
			case "expired_execution_deadline":
				deadline = time.Now().Add(-time.Second)
			}
			if !deadline.IsZero() {
				execution, cancelExecution = context.WithDeadline(execution, deadline)
			} else {
				execution, cancelExecution = context.WithCancel(execution)
			}
			defer cancelExecution()
			cancelExecution()
			client := &Client{operationTimeout: DefaultOperationTimeout}
			var factoryParent, factoryResult context.Context
			calls := 0
			operation, cleanup := newComputerPublicationOperation(execution, func(parent context.Context) (context.Context, context.CancelFunc) {
				calls++
				factoryParent = parent
				var cancel context.CancelFunc
				factoryResult, cancel = client.boundedContext(parent)
				return factoryResult, cancel
			})
			defer cleanup()
			if calls != 1 || operation != factoryResult {
				t.Fatalf("factory calls=%d returned identity changed=%t", calls, operation != factoryResult)
			}
			if factoryParent.Value(valueKey{}) != "retained" || operation.Value(valueKey{}) != "retained" {
				t.Fatal("execution value lost")
			}
			if errors.Is(context.Cause(factoryParent), context.Canceled) || errors.Is(context.Cause(operation), context.Canceled) {
				t.Fatal("execution cancellation reached publication operation")
			}
			parentDeadline, parentHasDeadline := factoryParent.Deadline()
			operationDeadline, operationHasDeadline := operation.Deadline()
			if !deadline.IsZero() {
				if !parentHasDeadline || !operationHasDeadline || !parentDeadline.Equal(deadline) || !operationDeadline.Equal(deadline) {
					t.Errorf("exact execution deadline lost: parent=%s operation=%s want=%s", parentDeadline, operationDeadline, deadline)
				}
				if mode == "expired_execution_deadline" && (!errors.Is(context.Cause(factoryParent), context.DeadlineExceeded) || !errors.Is(context.Cause(operation), context.DeadlineExceeded)) {
					t.Error("expired execution deadline granted fresh operation authority")
				}
			} else {
				if parentHasDeadline || !operationHasDeadline {
					t.Error("execution detachment or default operation bound lost")
				}
			}
			cleanup()
			if operation.Err() == nil {
				t.Error("cleanup did not cancel operation")
			}
			if parentHasDeadline && factoryParent.Err() == nil {
				t.Error("cleanup did not cancel preserved-deadline parent")
			}
		})
	}
}
