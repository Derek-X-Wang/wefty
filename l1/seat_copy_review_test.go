package l1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func copyOutcomeRequest(t *testing.T, server *Server, id, secret string, value SeatCopyOutcomeReceipt) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/seats/"+id+"/copy-outcome", bytes.NewReader(body))
	req.Header.Set(SeatCredentialHeader, secret)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, req)
	return response
}

func TestSeatCopyPreclaimedIDCannotHideNodeEvidence(t *testing.T) {
	for _, direction := range []string{"in", "out"} {
		t.Run(direction, func(t *testing.T) {
			f, id, secret, auth := liveSeatForCopy(t)
			server, err := NewServer(copyOutcomeFabric{}, f.store, ServerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			claim := SeatCopyOutcomeReceipt{CopyID: "preclaimed", Direction: "out", Path: "decoy", Outcome: "failed", Reason: "destination_failed"}
			response := copyOutcomeRequest(t, server, id, secret, claim)
			var failure contract.ErrorResponse
			_ = json.Unmarshal(response.Body.Bytes(), &failure)
			if response.Code != http.StatusNotFound || failure.Error.Code != contract.ErrorNotFound {
				t.Errorf("unused ID must be typed 404: %d %s", response.Code, response.Body.String())
			}
			outcome := "sent"
			if direction == "in" {
				outcome = "completed"
			}
			node := SeatCopyReceipt{SeatCopyAuthorityInput: auth, CopyID: claim.CopyID, Direction: direction, Path: "secrets.tar", SizeBytes: 999, Outcome: outcome}
			if err = f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, node); err != nil {
				t.Errorf("caller hid node evidence: %v", err)
			}
			page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
			if err != nil {
				t.Fatal(err)
			}
			var copies []SeatLogEvent
			for _, event := range page.Events {
				if event.CopyID == claim.CopyID {
					copies = append(copies, event)
				}
			}
			if len(copies) != 1 || copies[0].Path != node.Path || copies[0].NodeOutcome != outcome || copies[0].SizeBytes == nil || *copies[0].SizeBytes != 999 {
				t.Fatalf("node copy relabeled or missing: %+v", copies)
			}
			if direction == "out" {
				valid := SeatCopyOutcomeReceipt{CopyID: node.CopyID, Direction: "out", Path: node.Path, SizeBytes: node.SizeBytes, Outcome: "failed", Reason: "destination_failed"}
				for _, changed := range []SeatCopyOutcomeReceipt{
					{CopyID: valid.CopyID, Direction: "out", Path: "decoy", SizeBytes: 999, Outcome: "failed", Reason: "destination_failed"},
					{CopyID: valid.CopyID, Direction: "out", Path: valid.Path, SizeBytes: 0, Outcome: "failed", Reason: "destination_failed"},
					{CopyID: valid.CopyID, Direction: "in", Path: valid.Path, SizeBytes: 999, Outcome: "failed", Reason: "destination_failed"},
				} {
					if err := f.store.ReportSeatCopyOutcome(t.Context(), id, secret, changed); err == nil {
						t.Fatalf("caller rewrote node metadata: %+v", changed)
					}
				}
				if err := f.store.ReportSeatCopyOutcome(t.Context(), id, secret, valid); err != nil {
					t.Fatal(err)
				}
				// A node retry remains valid after the local publication failed.
				if err := f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, node); err != nil {
					t.Fatalf("caller result blocked node retry: %v", err)
				}
			}
		})
	}
}

func TestSeatCopyFreshOutcomeIDsNeverGrowLog(t *testing.T) {
	for _, ended := range []bool{false, true} {
		t.Run(fmt.Sprint(ended), func(t *testing.T) {
			f, id, secret, _ := liveSeatForCopy(t)
			if ended {
				if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released"}); err != nil {
					t.Fatal(err)
				}
			}
			server, err := NewServer(copyOutcomeFabric{}, f.store, ServerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			before, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
			if err != nil {
				t.Fatal(err)
			}
			for i := range 50 {
				r := SeatCopyOutcomeReceipt{CopyID: fmt.Sprintf("fresh-%d", i), Direction: "out", Path: "decoy", Outcome: "failed", Reason: "destination_failed"}
				response := copyOutcomeRequest(t, server, id, secret, r)
				if response.Code == http.StatusOK {
					t.Errorf("fresh ID %d created caller-only evidence", i)
				}
			}
			after, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
			if err != nil {
				t.Fatal(err)
			}
			if len(after.Events) != len(before.Events) || after.NextCursor != before.NextCursor {
				t.Fatalf("50 calls grew log: before=%d after=%d", len(before.Events), len(after.Events))
			}
		})
	}
}
