package l1

import "context"

// readObservationKey is a package-private diagnostic seam for deterministic
// concurrency tests. Production requests never install an observer. Observers
// run after a statement has released its rows, while its owning snapshot (if
// any) remains open. They neither acquire nor expose database handles.
type readObservationKey struct{}

func observeRead(ctx context.Context, point string) {
	if observe, ok := ctx.Value(readObservationKey{}).(func(string)); ok {
		observe(point)
	}
}
