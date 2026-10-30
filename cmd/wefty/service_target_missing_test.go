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

// TestServiceTargetTotalMissNamesServiceAndComputer: a target that is neither
// a service, a Computer ID nor a Computer name is reported as such, not as a
// missing Computer, with not_found and exit 4 (#1172).
func TestServiceTargetTotalMissNamesServiceAndComputer(t *testing.T) {
	binary := buildWefty(t)
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/computers":
			_ = json.NewEncoder(w).Encode(l1.ComputerList{})
		case strings.HasPrefix(r.URL.Path, "/v1/jobs/"):
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorNotFound, Message: "job was not found"}})
		case strings.HasPrefix(r.URL.Path, "/v1/computers/"):
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorNotFound, Message: "Computer was not found"}})
		default:
			t.Errorf("unexpected L1 call %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusTeapot)
		}
	})
	for _, target := range []string{"job_" + strings.Repeat("0", 32), "no-such-computer"} {
		for _, command := range [][]string{{"services", "status", target}, {"open", target, "--no-browser"}} {
			code, out := runWefty(t, binary, 30*time.Second, append([]string{"--l1", address, "--json"}, command...)...)
			var document contract.ErrorResponse
			want := `no service or Computer "` + target + `" was found`
			if code != 4 || json.Unmarshal([]byte(out), &document) != nil || document.Error.Code != contract.ErrorNotFound || document.Error.Message != want {
				t.Fatalf("%v: exit=%d output=%s, want exit 4 not_found %q", command, code, out, want)
			}
		}
	}
}
