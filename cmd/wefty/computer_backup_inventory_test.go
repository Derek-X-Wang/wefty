package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
)

func reviewStorageClients(t *testing.T, refuse bool) (*apiClients, *int, *int) {
	t.Helper()
	backupPages, provenancePages := 0, 0
	response := func(status int, body string) *http.Response {
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
	}
	client := &apiClients{l1: &apiClient{name: "L1", client: &http.Client{Transport: storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/storage-provenance"):
			provenancePages++
			if refuse {
				return response(409, `{"error":{"code":"conflict","message":"custody family exceeds bound","retryable":false,"details":{"reason":"storage_custody_limit"}}}`), nil
			}
			expected := ""
			next := "p1"
			if provenancePages == 2 {
				expected = "p1"
				next = "p2"
			}
			if provenancePages == 3 {
				expected = "p2"
				next = ""
			}
			if provenancePages > 3 || r.URL.Query().Get("cursor") != expected {
				t.Fatalf("provenance cursor=%s page=%d", r.URL.RawQuery, provenancePages)
			}
			return response(200, fmt.Sprintf(`{"computer_id":"computer-review","storage_id":"storage-review","storage_generation":1,"custody_tainted":true,"custody_forks":[{"computer_id":"computer-review"}],"custody_exports":[],"storage_provenance":[{"provenance_id":"p%d"}],"next_cursor":%q}`, provenancePages, next)), nil
		case strings.HasSuffix(r.URL.Path, "/backups"):
			backupPages++
			expected := ""
			next := "b1"
			if backupPages == 2 {
				expected = "b1"
				next = "b2"
			}
			if backupPages == 3 {
				expected = "b2"
				next = ""
			}
			if backupPages > 3 || r.URL.Query().Get("cursor") != expected {
				t.Fatalf("Backup cursor=%s page=%d", r.URL.RawQuery, backupPages)
			}
			return response(200, fmt.Sprintf(`{"backups":[{"backup_id":"b%d"}],"next_cursor":%q}`, backupPages, next)), nil
		default:
			return response(200, `{"computer_id":"computer-review"}`), nil
		}
	})}}}
	return client, &backupPages, &provenancePages
}

func TestComputerCLIBackupInventoryWalksSeveralPages(t *testing.T) {
	clients, backups, provenance := reviewStorageClients(t, false)
	var out, stderr bytes.Buffer
	if err := executeComputerBackups(t.Context(), clients, true, []string{"list", "computer-review"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	var inventory computerBackupInventory
	if err := json.Unmarshal(out.Bytes(), &inventory); err != nil || *backups != 3 || *provenance != 3 || len(inventory.Backups) != 3 || len(inventory.Provenance) != 3 || len(inventory.CustodyForks) != 1 {
		t.Fatalf("inventory=%s pages=%d/%d err=%v", out.String(), *backups, *provenance, err)
	}
}

func TestComputerCLIBackupInventorySurvivesProvenanceRefusal(t *testing.T) {
	for _, jsonOutput := range []bool{true, false} {
		t.Run(fmt.Sprint(jsonOutput), func(t *testing.T) {
			clients, backups, _ := reviewStorageClients(t, true)
			var out, stderr bytes.Buffer
			err := executeComputerBackups(t.Context(), clients, jsonOutput, []string{"list", "computer-review"}, &out, &stderr)
			if jsonOutput && strings.Contains(out.String(), `"custody_tainted"`) {
				t.Fatalf("refused provenance must not imply untainted custody: %s", out.String())
			}
			if err != nil || *backups != 3 || !strings.Contains(out.String(), "b3") || !strings.Contains(out.String(), "provenance unavailable") {
				t.Fatalf("Backup result lost=%s err=%v", out.String(), err)
			}
		})
	}
}

func TestComputerCLIMutationKeepsResultOnProvenanceRefusal(t *testing.T) {
	clients, _, _ := reviewStorageClients(t, true)
	prior := &custodyImportOutcomeError{importID: "import-review", outcome: custodyImportOutcome("failed")}
	for _, operationErr := range []error{nil, prior} {
		output := storageMutationOutput{MutationApplied: true, Computer: &l1.Computer{ComputerID: "computer-review"}, Observation: &storageWaitObservation{Status: "observed"}}
		err := attachStorageProvenance(t.Context(), clients, "computer-review", &output, operationErr)
		var out bytes.Buffer
		if writeErr := writeStorageMutation(&out, output, true); writeErr != nil {
			t.Fatal(writeErr)
		}
		if err != operationErr || output.Observation.Status != "observed" || !strings.Contains(out.String(), "provenance unavailable") || !strings.Contains(out.String(), `"mutation_applied": true`) {
			t.Fatalf("operation result lost=%s err=%v", out.String(), err)
		}
		out.Reset()
		if err := writeStorageMutation(&out, output, false); err != nil || !strings.Contains(out.String(), "provenance unavailable") {
			t.Fatalf("human notice=%s %v", out.String(), err)
		}
	}
}
