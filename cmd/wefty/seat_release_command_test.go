package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestSeatReleaseRealBinary(t *testing.T) {
	binary := buildWefty(t)
	id := "seat_" + strings.Repeat("b", 32)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("TMPDIR", t.TempDir())
	if _, err := storeSeatCredential(id, "seat-release-secret"); err != nil {
		t.Fatal(err)
	}
	for _, ended := range []bool{false, true} {
		t.Run(map[bool]string{false: "released", true: "expired"}[ended], func(t *testing.T) {
			address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.URL.Path != "/v1/seats/"+id+"/release" || r.Header.Get(l1.SeatCredentialHeader) != "seat-release-secret" {
					t.Errorf("unexpected release %s %s", r.Method, r.URL.Path)
				}
				if ended {
					w.WriteHeader(http.StatusConflict)
					json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: "seat_ended", Message: "Seat ended: expired"}})
					return
				}
				json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: id, State: l1.SeatEnding, Reason: "released"}, CleanupPending: true})
			})
			code, out := runWefty(t, binary, 30*time.Second, "--l1", address, "--json", "seat", "release", id)
			want := 0
			if ended {
				want = 5
			}
			if code != want || !strings.Contains(out, id) && !ended || strings.Contains(out, "seat-release-secret") {
				t.Fatalf("release exit=%d body=%s", code, out)
			}
			if ended && !strings.Contains(out, "seat_ended") {
				t.Fatalf("missing typed end: %s", out)
			}
		})
	}
}
