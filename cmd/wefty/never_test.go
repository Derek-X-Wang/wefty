package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestServiceNeverCreateFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	script := filepath.Join(t.TempDir(), "service.sh")
	if err := os.WriteFile(script, []byte("exit 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	server := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		var spec contract.JobSpec
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			t.Error(err)
		}
		if spec.Restart != "never" {
			t.Errorf("restart = %q", spec.Restart)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(l1.Job{JobID: "never-service", Spec: spec})
	})
	code, output := runWefty(t, binary, 30*time.Second, "--json", "--l1="+server, "services", "create", "--script", script, "--restart=never")
	if code != 0 || !strings.Contains(output, "never-service") {
		t.Fatalf("create = %d %s", code, output)
	}
	code, output = runWefty(t, binary, 30*time.Second, "--l1="+server, "services", "create", "--computer", "--script", script, "--restart=never")
	if code != exitUsage || !strings.Contains(output, "Computers require") {
		t.Fatalf("Computer create = %d %s", code, output)
	}
}
