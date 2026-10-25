package l1

import (
	"encoding/json"
	"testing"
)

func TestSeatSyncRetainedLogCounts(t *testing.T) {
	f, id, secret, auth := liveSeatForCopy(t)
	receipt := SeatCopyReceipt{SeatCopyAuthorityInput: auth, CopyID: "sync-copy", Direction: "sync", Path: "work", Outcome: "sending"}
	if err := f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, receipt); err != nil {
		t.Fatal(err)
	}
	receipt.Outcome = "completed"
	receipt.SizeBytes = 7
	raw, _ := json.Marshal(receipt)
	var fields map[string]any
	_ = json.Unmarshal(raw, &fields)
	fields["sync"] = map[string]any{"files_written": 3, "files_removed": 2}
	raw, _ = json.Marshal(fields)
	_ = json.Unmarshal(raw, &receipt)
	for range 2 {
		if err := f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, receipt); err != nil {
			t.Fatal(err)
		}
	}
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, event := range page.Events {
		if event.CopyID != "sync-copy" {
			continue
		}
		found++
		raw, _ = json.Marshal(event)
		var got map[string]any
		_ = json.Unmarshal(raw, &got)
		counts, _ := got["sync"].(map[string]any)
		if got["direction"] != "sync" || counts["files_written"] != float64(3) || counts["files_removed"] != float64(2) || got["size_bytes"] != float64(7) {
			t.Fatalf("counts lost: %s", raw)
		}
	}
	if found != 1 {
		t.Fatalf("sync events=%d", found)
	}
	fields["sync"] = map[string]any{"files_written": 4, "files_removed": 2}
	raw, _ = json.Marshal(fields)
	_ = json.Unmarshal(raw, &receipt)
	if err := f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, receipt); err == nil {
		t.Fatal("changed counts replayed")
	}
}
