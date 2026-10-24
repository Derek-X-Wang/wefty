package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func TestSeatRequestHelpAndMissingNetworkRealBinary(t *testing.T) {
	binary := buildWefty(t)
	code, out := runWefty(t, binary, 30*time.Second, "seat", "--help")
	if code != 0 || !strings.Contains(out, "seat request --network internet|none") || !strings.Contains(out, "seat wait SEAT_ID") {
		t.Fatalf("help exit=%d output=%s", code, out)
	}
	code, out = runWefty(t, binary, 30*time.Second, "seat", "request")
	if code != 2 || !strings.Contains(out, "--network internet|none") {
		t.Fatalf("missing network exit=%d output=%s", code, out)
	}
}

func TestSeatRequestRefusalsAndCredentialFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	id := "seat_" + strings.Repeat("a", 32)
	cases := []struct{ reason, code string }{
		{"out of room, try again later", "seat_room_full"},
		{"no answer from the owner", "seat_no_answer"},
		{"denied by the owner", "seat_denied"},
		{"dedicated Computer cap reached", "seat_dedicated_cap"},
		{"the host VM is off", "seat_host_off"},
		{"Computer failed to start", "seat_start_failed"},
	}
	for _, c := range cases {
		t.Run(c.code, func(t *testing.T) {
			address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/v1/seat-requests/"+id {
					t.Errorf("unexpected call %s %s", r.Method, r.URL.Path)
				}
				json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: id, State: l1.SeatRefused, Reason: c.reason}})
			})
			code, out := runWefty(t, binary, 30*time.Second, "--l1", address, "seat", "wait", id, "--json")
			var envelope contract.ErrorResponse
			if err := json.Unmarshal([]byte(out), &envelope); err != nil || code != 5 || string(envelope.Error.Code) != c.code || envelope.Error.Details["seat_id"] != id {
				t.Fatalf("refusal exit=%d body=%s err=%v", code, out, err)
			}
		})
	}
	t.Run("private credential", func(t *testing.T) {
		directory := t.TempDir()
		t.Setenv("TMPDIR", directory)
		address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: id, State: l1.SeatLive, ComputerID: "computer-example"}, Credential: "private-seat-credential"})
		})
		code, out := runWefty(t, binary, 30*time.Second, "--l1", address, "seat", "wait", id, "--json")
		if code != 0 || strings.Contains(out, "private-seat-credential") {
			t.Fatalf("credential output exit=%d body=%s", code, out)
		}
		var result struct {
			CredentialFile string `json:"credential_file"`
		}
		if err := json.Unmarshal([]byte(out), &result); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(result.CredentialFile)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("private file mode: %v %v", info, err)
		}
		data, err := os.ReadFile(result.CredentialFile)
		if err != nil || !strings.Contains(string(data), "private-seat-credential") {
			t.Fatalf("credential file missing: %v", err)
		}
	})
}

func TestSeatRequestLostPostReplyReplaysPrivateReceipt(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	node := readyNode("mac")
	node.AuthoritativeTags = []string{contract.StableNodeTagPrefix + "mac"}
	var first l1.SeatCallInput
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/nodes/mac":
			json.NewEncoder(w).Encode(node)
		case "/v1/seat-requests":
			posts++
			var input l1.SeatCallInput
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			if posts == 1 {
				first = input
				w.WriteHeader(http.StatusServiceUnavailable)
				json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorUnavailable, Message: "lost submission observation", Retryable: true}})
			} else {
				if input != first {
					t.Errorf("replay changed input")
				}
				json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: input.SeatID, State: l1.SeatLive, ComputerID: "computer-example"}, Credential: strings.Repeat("a", 64)})
			}
		default:
			if r.Method != "GET" || r.URL.Path != "/v1/seat-requests/"+first.SeatID {
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			}
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorNotFound, Message: "request not committed"}})
		}
	}))
	defer server.Close()
	clients := &apiClients{l1: statusClient(t, "L1", "test", server), nodeConfigPath: filepath.Join(t.TempDir(), "node.json")}
	if err := ocicontrol.WriteInstalledConfig(clients.nodeConfigPath, ocicontrol.InstalledConfig{Version: 1, NodeID: "mac"}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := executeSeatRequest(t.Context(), clients, true, []string{"request", "--network", "none"}, &output); err == nil {
		t.Fatal("lost observation was hidden")
	}
	if !seatCLIIdentifier.MatchString(first.SeatID) || !strings.Contains(output.String(), first.SeatID) {
		t.Fatalf("ID was not printed before lost reply: %s", output.String())
	}
	output.Reset()
	if err := executeSeatRequest(t.Context(), clients, true, []string{"wait", first.SeatID}, &output); err != nil {
		t.Fatal(err)
	}
	if posts != 2 || !strings.Contains(output.String(), `"state": "live"`) || strings.Contains(output.String(), strings.Repeat("a", 64)) {
		t.Fatalf("replay posts=%d output=%s", posts, output.String())
	}
}

func TestSeatRequestAdditionalRefusalsRealBinary(t *testing.T) {
	binary := buildWefty(t)
	id := "seat_00000000000000000000000000000903"
	for _, c := range []struct {
		reason, code string
		state        l1.SeatRequestState
		retry        bool
	}{
		{"Computer room is not configured on this node", "seat_room_unconfigured", l1.SeatRefused, false},
		{"the node did not pick up the request", "seat_node_unavailable", l1.SeatRefused, true},
		{"Seat approval window 8h is not enabled; allowed windows: 1h", "seat_window_not_allowed", l1.SeatRefused, false},
		{"released", "seat_ended", l1.SeatEnded, false},
	} {
		t.Run(c.code, func(t *testing.T) {
			address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: id, State: c.state, Reason: c.reason}})
			})
			code, out := runWefty(t, binary, 30*time.Second, "--l1", address, "seat", "wait", id, "--json")
			var response contract.ErrorResponse
			if err := json.Unmarshal([]byte(out), &response); err != nil || code != 5 || string(response.Error.Code) != c.code || response.Error.Retryable != c.retry {
				t.Fatalf("exit=%d response=%s err=%v", code, out, err)
			}
		})
	}
}
