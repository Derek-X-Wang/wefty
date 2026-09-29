package agent

import "testing"

func TestRetainedResultsStatusNodeChargedBytes(t *testing.T) {
	process := RetainedResultsStatus{ChargedBytes: 10}
	if got, known := process.NodeChargedBytes(); !known || got != 10 {
		t.Fatalf("process-only = %d, %t", got, known)
	}
	both := RetainedResultsStatus{ChargedBytes: 10, OCI: &RetainedOCIResultsStatus{ChargedBytes: 5}}
	if got, known := both.NodeChargedBytes(); !known || got != 15 {
		t.Fatalf("both roots = %d, %t", got, known)
	}
	unread := RetainedResultsStatus{ChargedBytes: 10, OCIInventoryFailed: true}
	if _, known := unread.NodeChargedBytes(); known {
		t.Fatal("an unread OCI root produced a node-wide figure")
	}
}
