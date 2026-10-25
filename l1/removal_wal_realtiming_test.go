//go:build service_acceptance_realtiming

package l1

import "testing"

// Removals under one held reader coalesce: a removal does not queue for the
// checkpoint handle behind another removal, and does not retry a checkpoint
// that has just been deferred, so none of them takes more than about one
// checkpoint wait. The bound is wall-clock, so it runs only in the realtiming
// lane (one package at a time); the default suite keeps a load-tolerant bound
// (#1262).
func TestConcurrentServiceRemovalsUnderAHeldReaderEachReturnWithinOneWait(t *testing.T) {
	store, path := openRemovalWALStore(t)
	removals := createRemovalServices(t, store, concurrentRemovals)
	assertRemovalsDeferWALTruncation(t, store, path, removals, removalWALPromptBound)
}
