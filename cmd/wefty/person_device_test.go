package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestAdminAddDeviceFlagFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	requests := make(chan string, 4)
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests <- r.Method + " " + r.URL.Path + " " + string(body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"revision":2}`)
	})
	for _, command := range [][]string{{"admin", "policy", "add"}, {"admins", "add"}} {
		args := append([]string{"--json", "--plain-user-id", "owner", "--plain-device-id", "enrolled-device", "--l1", address}, command...)
		args = append(args, "second", "--policy-revision", "1", "--device-id", "--json=nope")
		code, out := runWefty(t, binary, 30*time.Second, args...)
		if code != 0 {
			t.Fatalf("flag exit=%d output=%s", code, out)
		}
		select {
		case request := <-requests:
			if !strings.Contains(request, `"device_id":"--json=nope"`) {
				t.Fatalf("device value lost: %s", request)
			}
		default:
			t.Fatal("add sent no request")
		}
	}
}

func TestPersonDeviceMismatchExitFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(403)
		json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: "person_device_mismatch", Message: "use the enrolled person device, or rebind it locally on the control plane"}})
	})
	for _, command := range [][]string{{"admin", "policy", "get"}, {"whoami"}} {
		args := append([]string{"--json", "--plain-user-id", "owner", "--plain-device-id", "enrolled-device", "--l1", address}, command...)
		code, out := runWefty(t, binary, 30*time.Second, args...)
		var response contract.ErrorResponse
		if err := json.Unmarshal([]byte(out), &response); err != nil || code != 3 || response.Error.Code != "person_device_mismatch" {
			t.Fatalf("exit=%d output=%s decode=%v", code, out, err)
		}
	}
}

func TestAdminDeviceUsageAndSupportedSpellings(t *testing.T) {
	binary := buildWefty(t)
	for _, args := range [][]string{{"admin", "add", "second", "--policy-revision", "1"}, {"admin"}, {"admin", "policy", "add"}} {
		command := append([]string{"--json", "--plain-user-id", "owner", "--plain-device-id", "device"}, args...)
		code, out := runWefty(t, binary, 30*time.Second, command...)
		var response contract.ErrorResponse
		if err := json.Unmarshal([]byte(out), &response); err != nil || code != 2 || response.Error.Code != "invalid_request" {
			t.Fatalf("unsupported spelling or usage %v: exit=%d output=%s", args, code, out)
		}
		if !strings.Contains(response.Error.Message, "--device-id") {
			t.Fatalf("usage omitted --device-id: %s", out)
		}
	}
	for _, line := range strings.Split(rootUsage, "\n") {
		if strings.HasPrefix(line, "  admin policy add|remove") && !strings.Contains(line, "    ") {
			t.Fatalf("misaligned admin help: %s", line)
		}
		if strings.HasPrefix(line, "  admin add ") {
			t.Fatalf("unsupported spelling remains in help: %s", line)
		}
	}
}
