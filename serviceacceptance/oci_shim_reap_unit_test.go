package serviceacceptance

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func acceptanceShimResidue() *ocihelper.NamespaceResidueError {
	return &ocihelper.NamespaceResidueError{
		Operation:      "verify OCI runtime namespace",
		Observed:       ocihelper.ResourceInventory{Shims: []string{"wefty-container-previous"}},
		RuntimeResidue: ocihelper.ResourceInventory{Shims: []string{"wefty-container-previous"}},
	}
}

func TestOnlyExitingAcceptanceShims(t *testing.T) {
	residue := acceptanceShimResidue()
	residue.DurableRetained.ComputerDiskImages = []string{"retained-disk"}
	if !onlyExitingAcceptanceShims(fmt.Errorf("provision: %w", residue)) {
		t.Fatal("shim-only post-delete residue must allow a bounded wait, including alongside retained durable data")
	}
	if onlyExitingAcceptanceShims(nil) || onlyExitingAcceptanceShims(errors.New("transport failed")) ||
		onlyExitingAcceptanceShims(&ocihelper.NamespaceResidueError{}) {
		t.Fatal("only a typed, nonempty shim residue may be retried")
	}
	for _, id := range []string{"foreign-shim", "wefty-container-"} {
		foreign := acceptanceShimResidue()
		foreign.RuntimeResidue.Shims = append(foreign.RuntimeResidue.Shims, id)
		if onlyExitingAcceptanceShims(foreign) {
			t.Fatalf("unrecognized shim %q must fail closed", id)
		}
	}
	// Cover every inventory class, including future additions, so a mixed
	// inventory can never accidentally become a retryable shim-only refusal.
	typeOfInventory := reflect.TypeFor[ocihelper.ResourceInventory]()
	for i := 0; i < typeOfInventory.NumField(); i++ {
		field := typeOfInventory.Field(i)
		if field.Name == "Shims" {
			continue
		}
		t.Run(field.Name, func(t *testing.T) {
			mixed := acceptanceShimResidue()
			value := reflect.ValueOf(&mixed.RuntimeResidue).Elem().Field(i)
			value.Set(reflect.MakeSlice(value.Type(), 1, 1))
			if onlyExitingAcceptanceShims(mixed) {
				t.Fatalf("shim plus %s residue must fail closed", field.Name)
			}
		})
	}
}

func TestAcceptanceNamespaceShimReap(t *testing.T) {
	t.Run("requires fresh positive verification", func(t *testing.T) {
		calls, waits := 0, 0
		residue := acceptanceShimResidue()
		err := ensureAcceptanceNamespace(context.Background(), func(ctx context.Context) error {
			calls++
			if calls > 1 {
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > acceptanceShimReapWindow {
					t.Fatal("retry verification must share the short shim deadline")
				}
			}
			if calls < 3 {
				return residue
			}
			return nil
		}, func(ctx context.Context, interval time.Duration) error {
			waits++
			if interval != acceptanceShimReapPoll {
				t.Fatalf("poll = %s", interval)
			}
			return nil
		})
		if err != nil || calls != 3 || waits != 2 {
			t.Fatalf("exiting shim: calls=%d waits=%d err=%v, want positive verification on call 3", calls, waits, err)
		}
	})
	t.Run("never exiting shim fails at the bound", func(t *testing.T) {
		calls, waits := 0, 0
		residue := acceptanceShimResidue()
		var retryContext context.Context
		var retryDeadline time.Time
		err := ensureAcceptanceNamespace(context.Background(), func(ctx context.Context) error {
			calls++
			return residue
		}, func(ctx context.Context, interval time.Duration) error {
			retryContext = ctx
			waits++
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > acceptanceShimReapWindow {
				t.Fatal("persistent shim wait must have a short absolute deadline")
			}
			if retryDeadline.IsZero() {
				retryDeadline = deadline
			} else if !deadline.Equal(retryDeadline) {
				t.Fatal("another inventory must not extend the shim reaping deadline")
			}
			// Advance the fake wait to its deadline without wall-clock sleeps.
			if waits == 3 {
				return context.DeadlineExceeded
			}
			return nil
		})
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, residue) || calls != 3 || waits != 3 {
			t.Fatalf("persistent shim: calls=%d waits=%d err=%v, want bounded failure preserving inventory", calls, waits, err)
		}
		if !errors.Is(retryContext.Err(), context.Canceled) {
			t.Fatal("retry deadline must be released when the harness fails")
		}
	})
	t.Run("other failures are immediate", func(t *testing.T) {
		mixed := acceptanceShimResidue()
		mixed.RuntimeResidue.Tasks = []string{"wefty-container-live"}
		for _, failure := range []error{mixed, errors.New("transport failed"), &ocihelper.ResidueClassificationError{}} {
			calls := 0
			err := ensureAcceptanceNamespace(context.Background(), func(context.Context) error {
				calls++
				return failure
			}, func(context.Context, time.Duration) error {
				t.Fatal("non-shim failure must not wait")
				return nil
			})
			if err != failure || calls != 1 {
				t.Fatalf("calls=%d err=%v, want original immediate refusal %v", calls, err, failure)
			}
		}
	})
	t.Run("new residue during reaping fails immediately", func(t *testing.T) {
		calls := 0
		mixed := acceptanceShimResidue()
		mixed.RuntimeResidue.Containers = []string{"wefty-container-live"}
		err := ensureAcceptanceNamespace(context.Background(), func(context.Context) error {
			calls++
			if calls == 1 {
				return acceptanceShimResidue()
			}
			return mixed
		}, func(context.Context, time.Duration) error { return nil })
		if err != mixed || calls != 2 {
			t.Fatalf("calls=%d err=%v, want immediate mixed-residue refusal", calls, err)
		}
	})
	t.Run("parent cancellation prevents another verification", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		residue := acceptanceShimResidue()
		err := ensureAcceptanceNamespace(parent, func(context.Context) error {
			calls++
			return residue
		}, func(context.Context, time.Duration) error {
			cancel()
			return nil
		})
		if !errors.Is(err, context.Canceled) || !errors.Is(err, residue) || calls != 1 {
			t.Fatalf("calls=%d err=%v, want cancellation without another verification", calls, err)
		}
	})
	t.Run("late success cannot bypass the deadline", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		residue := acceptanceShimResidue()
		err := ensureAcceptanceNamespace(parent, func(context.Context) error {
			calls++
			if calls == 1 {
				return residue
			}
			cancel()
			return nil
		}, func(context.Context, time.Duration) error { return nil })
		if !errors.Is(err, context.Canceled) || !errors.Is(err, residue) || calls != 2 {
			t.Fatalf("calls=%d err=%v, want cancellation and last inventory even after late success", calls, err)
		}
	})
}
