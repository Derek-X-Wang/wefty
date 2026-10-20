package main

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

func TestCancelFromAnotherMachineHintsWithoutPersonRetry(t *testing.T) {
	var requests atomic.Int32
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorNotFound, Message: "job was not found"}})
	})

	for _, participantProfile := range []bool{false, true} {
		requests.Store(0)
		profile := connprofile.Profile{Version: 1, Fabric: "plain", Identities: connprofile.Identities{CLI: "legacy-client", PersonUserID: "owner", PersonDeviceID: "owner-device"}}
		if participantProfile {
			profile.Participants = map[string]connprofile.Participant{"client": {Name: "wefty://client/machine-1", StateDirectory: savedChoiceState(t, "client"), NodeID: "machine-client"}}
		}
		path := filepath.Join(t.TempDir(), "profile.json")
		if err := connprofile.Write(path, profile); err != nil {
			t.Fatal(err)
		}
		code, output := runWefty(t, buildWefty(t), 30*time.Second, "--profile="+path, "--l1="+address, "cancel", "job-other-machine")
		hasHint := strings.Contains(output, "submitted from another machine")
		if code != 4 || hasHint != participantProfile || requests.Load() != 1 {
			t.Fatalf("participant profile=%v cancel: exit=%d requests=%d output=%s", participantProfile, code, requests.Load(), output)
		}
	}
}
