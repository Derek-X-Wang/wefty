package serviceacceptance

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const linuxComputerMatrixVersion = 2

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
	Version              int                                    `json:"version"`
	Status               string                                 `json:"status"`
	CandidateSHA         string                                 `json:"candidate_sha"`
	Platform             string                                 `json:"platform"`
	Image                linuxComputerImageEvidence             `json:"image"`
	FabricIdentities     []linuxComputerFabricIdentity          `json:"fabric_identities"`
	AuthorityGenerations []int64                                `json:"authority_generations"`
	ResourceCaps         linuxComputerResourceCaps              `json:"resource_caps"`
	ComputerIDs          []string                               `json:"computer_ids"`
	JobIDs               []string                               `json:"job_ids"`
	AttemptIDs           []string                               `json:"attempt_ids"`
	StorageIDs           []string                               `json:"storage_ids"`
	ResidueInventories   map[string]ocihelper.ResourceInventory `json:"residue_inventories"`
	Timings              map[string]string                      `json:"timings"`
	Deviations           []linuxComputerDeviation               `json:"deviations"`
	Rows                 map[string]linuxComputerMatrixRow      `json:"rows"`
	NotRunIssue          int                                    `json:"not_run_issue,omitempty"`
	NotRunReason         string                                 `json:"not_run_reason,omitempty"`
	StartedAt            time.Time                              `json:"started_at"`
	CompletedAt          time.Time                              `json:"completed_at"`
}

type linuxComputerImageEvidence struct {
	Variant        string `json:"variant"`
	Reference      string `json:"reference"`
	IndexDigest    string `json:"index_digest"`
	PlatformDigest string `json:"platform_digest"`
	Archive        string `json:"archive_basename"`
}

type linuxComputerFabricIdentity struct {
	Role     string `json:"role"`
	FabricID string `json:"fabric_id"`
	UserID   string `json:"user_id"`
	DeviceID string `json:"device_id"`
}

type linuxComputerResourceCaps struct {
	MemoryBytes       int64 `json:"memory_bytes"`
	DiskBytes         int64 `json:"disk_bytes"`
	BackupCap         int64 `json:"backup_cap"`
	SubmitMaxInflight int   `json:"submit_max_inflight"`
}

type linuxComputerDeviation struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Reason string `json:"reason"`
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
		rows[required.ID] = linuxComputerMatrixRow{ID: required.ID, Proof: required.Proof, Status: "MISSING", Assertions: map[string]bool{}}
	}
	return &linuxComputerMatrixReceipt{
		Version: linuxComputerMatrixVersion, Status: "MISSING", Platform: runtime.GOOS + "/" + runtime.GOARCH,
		Rows: rows, ResidueInventories: map[string]ocihelper.ResourceInventory{}, Timings: map[string]string{}, StartedAt: time.Now().UTC(),
	}
}

func (receipt *linuxComputerMatrixReceipt) begin(id string) {
	row := receipt.Rows[id]
	now := time.Now().UTC()
	row.Status, row.StartedAt, row.CompletedAt = "FAIL", &now, nil
	row.Assertions, row.Evidence = map[string]bool{}, nil
	row.NotRunIssue, row.NotRunReason = 0, ""
	receipt.Rows[id] = row
}

func (receipt *linuxComputerMatrixReceipt) pass(id string, assertions map[string]bool, evidence map[string]string) error {
	row := receipt.Rows[id]
	row.Assertions, row.Evidence = assertions, evidence
	for name, passed := range assertions {
		if !passed {
			receipt.Rows[id] = row
			return fmt.Errorf("Linux Computer matrix row %s assertion %s was false", id, name)
		}
	}
	now := time.Now().UTC()
	row.Status, row.CompletedAt = "PASS", &now
	receipt.Rows[id] = row
	return nil
}

func (receipt *linuxComputerMatrixReceipt) notRun(id string, issue int, reason string, assertions map[string]bool, evidence map[string]string) error {
	if issue <= 0 || reason == "" {
		return fmt.Errorf("Linux Computer matrix row %s NOT-RUN requires an owning ticket and reason", id)
	}
	for name, passed := range assertions {
		if !passed {
			return fmt.Errorf("Linux Computer matrix row %s partial assertion %s was false", id, name)
		}
	}
	row := receipt.Rows[id]
	now := time.Now().UTC()
	row.Status, row.Assertions, row.Evidence, row.CompletedAt = "NOT-RUN", assertions, evidence, &now
	row.NotRunIssue, row.NotRunReason = issue, reason
	receipt.Rows[id] = row
	return nil
}

func (receipt *linuxComputerMatrixReceipt) finish() {
	receipt.CompletedAt = time.Now().UTC()
	receipt.Status, receipt.NotRunIssue, receipt.NotRunReason = "PASS", 0, ""
	for _, required := range linuxComputerMatrixRows {
		row := receipt.Rows[required.ID]
		if row.Status == "MISSING" || row.Status == "FAIL" || row.StartedAt == nil || row.CompletedAt == nil || len(row.Assertions) == 0 {
			receipt.Status = "FAIL"
			return
		}
		if row.Status == "NOT-RUN" {
			receipt.Status = "NOT-RUN"
			if receipt.NotRunIssue == 0 {
				receipt.NotRunIssue = row.NotRunIssue
				receipt.NotRunReason = row.NotRunReason
			}
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
		if !ok || row.ID != required.ID || row.Proof != required.Proof || row.Status != "MISSING" || row.NotRunIssue != 0 {
			t.Fatalf("Linux Computer matrix row %s = %#v, present=%t", required.ID, row, ok)
		}
	}
}

func TestLinuxComputerMatrixMutationToggleFailsExactlyOwningRow(t *testing.T) {
	for _, mutated := range linuxComputerMatrixRows {
		t.Run(mutated.ID, func(t *testing.T) {
			receipt := newLinuxComputerMatrixReceipt()
			for _, required := range linuxComputerMatrixRows {
				receipt.begin(required.ID)
				assertions := map[string]bool{"live_product_path": required.ID != mutated.ID}
				if required.ID == mutated.ID {
					if err := receipt.pass(required.ID, assertions, nil); err == nil {
						t.Fatalf("mutated live row %s passed", required.ID)
					}
					continue
				}
				if err := receipt.pass(required.ID, assertions, nil); err != nil {
					t.Fatal(err)
				}
			}
			receipt.finish()
			failures := 0
			for _, row := range receipt.Rows {
				if row.Status == "FAIL" {
					failures++
				}
			}
			if receipt.Status != "FAIL" || failures != 1 || receipt.Rows[mutated.ID].Status != "FAIL" {
				t.Fatalf("mutation %s aggregate=%s failures=%d row=%s", mutated.ID, receipt.Status, failures, receipt.Rows[mutated.ID].Status)
			}
		})
	}
}
