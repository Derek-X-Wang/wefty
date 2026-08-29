package serviceacceptance

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

const linuxComputerMatrixVersion = 1

var linuxComputerMatrixRows = []struct {
	ID, Proof string
}{
	{"linux.create_boot", "Create and boot"},
	{"linux.remote_takeover", "Remote take-over"},
	{"linux.restart_survival", "Restart survival"},
	{"linux.reconfiguration", "Reconfiguration"},
	{"linux.storage_provenance", "Storage provenance"},
	{"linux.guest_authority", "Guest authority"},
	{"linux.removal", "Removal"},
}

type linuxComputerMatrixReceipt struct {
	Version      int                               `json:"version"`
	Status       string                            `json:"status"`
	CandidateSHA string                            `json:"candidate_sha"`
	Platform     string                            `json:"platform"`
	Image        linuxComputerImageEvidence        `json:"image"`
	ComputerIDs  []string                          `json:"computer_ids"`
	JobIDs       []string                          `json:"job_ids"`
	AttemptIDs   []string                          `json:"attempt_ids"`
	StorageIDs   []string                          `json:"storage_ids"`
	Rows         map[string]linuxComputerMatrixRow `json:"rows"`
	NotRunIssue  int                               `json:"not_run_issue,omitempty"`
	NotRunReason string                            `json:"not_run_reason,omitempty"`
	StartedAt    time.Time                         `json:"started_at"`
	CompletedAt  time.Time                         `json:"completed_at"`
}

type linuxComputerImageEvidence struct {
	Reference      string `json:"reference"`
	IndexDigest    string `json:"index_digest"`
	PlatformDigest string `json:"platform_digest"`
	Archive        string `json:"archive_basename"`
}

type linuxComputerMatrixRow struct {
	ID           string            `json:"id"`
	Proof        string            `json:"proof"`
	Status       string            `json:"status"`
	Assertions   map[string]bool   `json:"assertions"`
	Evidence     map[string]string `json:"evidence,omitempty"`
	NotRunIssue  int               `json:"not_run_issue,omitempty"`
	NotRunReason string            `json:"not_run_reason,omitempty"`
	StartedAt    *time.Time        `json:"started_at,omitempty"`
	CompletedAt  *time.Time        `json:"completed_at,omitempty"`
}

func newLinuxComputerMatrixReceipt() *linuxComputerMatrixReceipt {
	rows := make(map[string]linuxComputerMatrixRow, len(linuxComputerMatrixRows))
	for _, required := range linuxComputerMatrixRows {
		rows[required.ID] = linuxComputerMatrixRow{ID: required.ID, Proof: required.Proof, Status: "NOT-RUN",
			Assertions: map[string]bool{}, NotRunIssue: 157,
			NotRunReason: "the M3 complete OCI acceptance matrix is not yet published; this row has not executed"}
	}
	return &linuxComputerMatrixReceipt{Version: linuxComputerMatrixVersion, Status: "NOT-RUN",
		Platform: runtime.GOOS + "/" + runtime.GOARCH, Rows: rows, StartedAt: time.Now().UTC()}
}

func (receipt *linuxComputerMatrixReceipt) begin(id string) {
	row := receipt.Rows[id]
	now := time.Now().UTC()
	row.Status, row.StartedAt, row.CompletedAt = "FAIL", &now, nil
	row.NotRunIssue, row.NotRunReason = 0, ""
	receipt.Rows[id] = row
}

func (receipt *linuxComputerMatrixReceipt) pass(id string, assertions map[string]bool, evidence map[string]string) {
	for name, passed := range assertions {
		if !passed {
			panic(fmt.Sprintf("Linux Computer matrix row %s assertion %s was false", id, name))
		}
	}
	row := receipt.Rows[id]
	now := time.Now().UTC()
	row.Status, row.Assertions, row.Evidence, row.CompletedAt = "PASS", assertions, evidence, &now
	receipt.Rows[id] = row
}

func (receipt *linuxComputerMatrixReceipt) notRun(id string, issue int, reason string, assertions map[string]bool, evidence map[string]string) {
	for name, passed := range assertions {
		if !passed {
			panic(fmt.Sprintf("Linux Computer matrix row %s partial assertion %s was false", id, name))
		}
	}
	row := receipt.Rows[id]
	now := time.Now().UTC()
	row.Status, row.Assertions, row.Evidence, row.CompletedAt = "NOT-RUN", assertions, evidence, &now
	row.NotRunIssue, row.NotRunReason = issue, reason
	receipt.Rows[id] = row
}

func (receipt *linuxComputerMatrixReceipt) finish() {
	receipt.CompletedAt = time.Now().UTC()
	receipt.Status = "PASS"
	for _, row := range receipt.Rows {
		if row.Status == "PASS" && (row.StartedAt == nil || row.CompletedAt == nil || len(row.Assertions) == 0) {
			receipt.Status = "FAIL"
			return
		}
		if row.Status == "FAIL" {
			receipt.Status = "FAIL"
			return
		}
		if row.Status == "NOT-RUN" {
			receipt.Status = "NOT-RUN"
			receipt.NotRunIssue = row.NotRunIssue
			receipt.NotRunReason = "one or more required Linux rows did not execute"
		}
	}
}

func TestLinuxComputerMatrixReceiptRejectsUnearnedPass(t *testing.T) {
	receipt := newLinuxComputerMatrixReceipt()
	for id, row := range receipt.Rows {
		row.Status = "PASS"
		receipt.Rows[id] = row
	}
	receipt.finish()
	if receipt.Status != "FAIL" {
		t.Fatalf("unearned matrix PASS finished as %s", receipt.Status)
	}
}

func TestLinuxComputerMatrixRowsAreStableAndComplete(t *testing.T) {
	receipt := newLinuxComputerMatrixReceipt()
	if len(receipt.Rows) != 7 {
		t.Fatalf("Linux Computer matrix rows = %d, want 7", len(receipt.Rows))
	}
	for _, required := range linuxComputerMatrixRows {
		row, ok := receipt.Rows[required.ID]
		if !ok || row.ID != required.ID || row.Proof != required.Proof || row.Status != "NOT-RUN" || row.NotRunIssue != 157 {
			t.Fatalf("Linux Computer matrix row %s = %#v, present=%t", required.ID, row, ok)
		}
	}
}

func TestLinuxComputerMatrixMutationRowsFailSevenOfSeven(t *testing.T) {
	for _, required := range linuxComputerMatrixRows {
		t.Run(required.ID, func(t *testing.T) {
			receipt := newLinuxComputerMatrixReceipt()
			receipt.begin(required.ID)
			receipt.finish()
			if receipt.Status != "FAIL" || receipt.Rows[required.ID].Status != "FAIL" {
				t.Fatalf("mutated row %s produced aggregate=%s row=%s", required.ID, receipt.Status, receipt.Rows[required.ID].Status)
			}
		})
	}
}
