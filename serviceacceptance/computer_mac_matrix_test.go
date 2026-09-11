package serviceacceptance

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// --- The attended Mac half of the agent-computer acceptance matrix (#195) ----
//
// The Linux half is linuxComputerMatrixReceipt, written by the realtiming lane.
// This is its owner-hardware mirror: the same nine stable rows under a mac.
// prefix, plus the spec section 11 item 4 narrowness row. The owner writes this
// receipt by hand from docs/acceptance/m3.5-mac-computer.md; it is also the
// fragment that scripts/assemble-computer-acceptance-matrix.sh merges, so there
// is no second mapping step to keep honest.
//
// Unlike the Linux receipt this type is never produced by a test run. The tests
// below are its consumer: they prove the shape cannot launder an unearned PASS
// and that the destination sentence cannot be asserted without the rows.

const macComputerMatrixVersion = 1

const (
	// No attended owner-hardware session ran. Hosted macOS cannot boot nested
	// Lima vz, so this owns every row of an absent Mac half.
	macComputerAbsentIssue = 128
	// The Lima vz bridge-bind defect: no Computer payload starts on a Mac until
	// it is fixed. Named as the blocking cause on the rows it blocks.
	macComputerGatewayIssue = 394
)

var macComputerMatrixRows = []struct {
	ID, Proof string
	// BlockedBy is the product defect that stops this row from being proven on
	// owner hardware today, or zero when only the setup is missing.
	BlockedBy int
}{
	{"mac.create_boot", "Create and boot", 0},
	{"mac.network_egress", "Private network outbound", macComputerGatewayIssue},
	{"mac.screen_crossover_refused", "Screen crossover refused", macComputerGatewayIssue},
	{"mac.remote_takeover", "Remote take-over", macComputerGatewayIssue},
	{"mac.restart_survival", "Restart survival", macComputerGatewayIssue},
	{"mac.reconfiguration", "Reconfiguration", macComputerGatewayIssue},
	{"mac.storage_provenance", "Storage provenance", macComputerGatewayIssue},
	{"mac.guest_authority", "Guest authority", macComputerGatewayIssue},
	{"mac.removal", "Removal", macComputerGatewayIssue},
	{"mac.reference_image_narrowness", "Reimage and reference-image narrowness", 0},
}

// macComputerContributingRows are the rows whose PASS the destination sentence
// depends on. A clause may be attested by hand; the row underneath it may not.
var macComputerDestinationClauses = []struct {
	Clause string
	Rows   []string
}{
	{"computer_on_mac_node", []string{"mac.create_boot"}},
	{"watched_and_controlled_from_second_device", []string{"mac.remote_takeover"}},
	{"survives_runtime_and_agent_restart_state_intact", []string{"mac.restart_survival"}},
	{"screen_withdrawn_on_authority_loss", []string{"mac.remote_takeover", "mac.restart_survival"}},
	{"unauthorized_peer_denied", []string{"mac.remote_takeover"}},
	{"removal_proves_managed_residue_absent", []string{"mac.removal"}},
}

// macComputerRedactionDenylist are substrings that must never reach the
// attended artifact. Spec section 10.1 permits Fabric user/device IDs and
// resource facts; it permits no credential, capability, or display content.
var macComputerRedactionDenylist = []string{
	"WEFTY_COMPUTER_TOKEN", "WEFTY_RUN_TOKEN", "session_token_value",
	"tskey-", "Bearer ", "authkey", "keystroke", "framebuffer_bytes",
	"BEGIN PRIVATE KEY", "BEGIN OPENSSH PRIVATE KEY",
}

type macComputerMatrixReceipt struct {
	Version             int                             `json:"version"`
	Status              string                          `json:"status"`
	SessionID           string                          `json:"session_id"`
	Commit              string                          `json:"commit"`
	ArtifactSHA256      string                          `json:"artifact_sha256"`
	Platform            string                          `json:"platform"`
	Host                macComputerHostEvidence         `json:"host"`
	Image               linuxComputerImageEvidence      `json:"image"`
	FabricIdentities    []linuxComputerFabricIdentity   `json:"fabric_identities"`
	AuthorityGeneration []int64                         `json:"authority_generations"`
	ResourceCaps        linuxComputerResourceCaps       `json:"resource_caps"`
	ComputerIDs         []string                        `json:"computer_ids"`
	JobIDs              []string                        `json:"job_ids"`
	AttemptIDs          []string                        `json:"attempt_ids"`
	StorageIDs          []string                        `json:"storage_ids"`
	Rendering           macComputerRenderingEvidence    `json:"rendering"`
	Timings             map[string]string               `json:"timings"`
	Deviations          []linuxComputerDeviation        `json:"deviations"`
	ResidueInventories  map[string]json.RawMessage      `json:"residue_inventories"`
	ResidueAssertions   map[string]bool                 `json:"residue_assertions"`
	Destination         macComputerDestination          `json:"destination"`
	AttendedRowCounts   map[string]int                  `json:"attended_row_counts"`
	Rows                map[string]macComputerMatrixRow `json:"rows"`
	NotRunIssue         int                             `json:"not_run_issue,omitempty"`
	NotRunReason        string                          `json:"not_run_reason,omitempty"`
	StartedAt           time.Time                       `json:"started_at"`
	CompletedAt         time.Time                       `json:"completed_at"`
}

type macComputerHostEvidence struct {
	MacOS           string `json:"macos"`
	Limactl         string `json:"limactl"`
	Containerd      string `json:"containerd"`
	Runc            string `json:"runc"`
	HostMemoryBytes int64  `json:"host_memory_bytes"`
	VMMemoryBytes   int64  `json:"vm_memory_bytes"`
	VMCPUs          int    `json:"vm_cpus"`
	VMDiskBytes     int64  `json:"vm_disk_bytes"`
	LimaInstances   int    `json:"lima_instances"`
}

// macComputerRenderingEvidence records the CPU-rendered/no-GPU facts spec
// section 10 asks for. They are facts, never a ratified threshold.
type macComputerRenderingEvidence struct {
	GPU        bool   `json:"gpu"`
	Renderer   string `json:"renderer"`
	DRIPresent bool   `json:"dri_present"`
}

type macComputerDestination struct {
	Asserted   bool            `json:"asserted"`
	Assertions map[string]bool `json:"assertions"`
	// Attestation is "human" when the owner watched and drove the Computer from
	// the second device by hand, or "scripted" when a pointer-history
	// comparison produced the evidence. v1 accepts either.
	Attestation string `json:"attestation"`
}

type macComputerMatrixRow struct {
	ID          string            `json:"id"`
	Proof       string            `json:"proof"`
	Status      string            `json:"status"`
	Assertions  map[string]bool   `json:"assertions"`
	Attested    map[string]string `json:"attested,omitempty"`
	Evidence    map[string]string `json:"evidence,omitempty"`
	Gaps        map[string]string `json:"gaps,omitempty"`
	NotRunIssue int               `json:"not_run_issue,omitempty"`
	Reason      string            `json:"reason,omitempty"`
	StartedAt   *time.Time        `json:"started_at,omitempty"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
}

func newMacComputerMatrixReceipt() *macComputerMatrixReceipt {
	rows := make(map[string]macComputerMatrixRow, len(macComputerMatrixRows))
	for _, required := range macComputerMatrixRows {
		rows[required.ID] = macComputerMatrixRow{ID: required.ID, Proof: required.Proof, Status: "MISSING", Assertions: map[string]bool{}}
	}
	return &macComputerMatrixReceipt{
		Version: macComputerMatrixVersion, Status: "MISSING", Platform: "darwin/arm64",
		Rows: rows, Timings: map[string]string{}, ResidueInventories: map[string]json.RawMessage{},
		ResidueAssertions: map[string]bool{}, AttendedRowCounts: map[string]int{},
		Destination: macComputerDestination{Assertions: map[string]bool{}},
		StartedAt:   time.Now().UTC(),
	}
}

func (receipt *macComputerMatrixReceipt) begin(id string) {
	row := receipt.Rows[id]
	now := time.Now().UTC()
	row.Status, row.StartedAt, row.CompletedAt = "FAIL", &now, nil
	row.Assertions, row.Evidence, row.Gaps = map[string]bool{}, nil, nil
	row.NotRunIssue, row.Reason = 0, ""
	receipt.Rows[id] = row
}

func (receipt *macComputerMatrixReceipt) pass(id string, assertions map[string]bool, evidence map[string]string) error {
	row := receipt.Rows[id]
	row.Assertions, row.Evidence = assertions, evidence
	if len(assertions)+len(row.Attested) == 0 {
		receipt.Rows[id] = row
		return fmt.Errorf("Mac Computer matrix row %s PASS carries no assertion", id)
	}
	for name, passed := range assertions {
		if !passed {
			receipt.Rows[id] = row
			return fmt.Errorf("Mac Computer matrix row %s assertion %s was false", id, name)
		}
	}
	if len(row.Gaps) != 0 {
		receipt.Rows[id] = row
		return fmt.Errorf("Mac Computer matrix row %s claims PASS while declaring a gap", id)
	}
	now := time.Now().UTC()
	row.Status, row.CompletedAt = "PASS", &now
	receipt.Rows[id] = row
	return nil
}

func (receipt *macComputerMatrixReceipt) notRun(id string, issue int, reason string, assertions map[string]bool, gaps map[string]string) error {
	if issue <= 0 || reason == "" {
		return fmt.Errorf("Mac Computer matrix row %s NOT-RUN requires an owning ticket and reason", id)
	}
	for name, passed := range assertions {
		if !passed {
			return fmt.Errorf("Mac Computer matrix row %s partial assertion %s was false", id, name)
		}
	}
	row := receipt.Rows[id]
	now := time.Now().UTC()
	row.Status, row.Assertions, row.Gaps, row.CompletedAt = "NOT-RUN", assertions, gaps, &now
	row.NotRunIssue, row.Reason = issue, reason
	receipt.Rows[id] = row
	return nil
}

// evaluateDestination recomputes the destination sentence from the rows. A hand
// written clause map is an input; the verdict is not.
func (receipt *macComputerMatrixReceipt) evaluateDestination() bool {
	for _, deviation := range receipt.Deviations {
		if deviation.ID == "dev.plain_fabric_identity" {
			return false
		}
	}
	for _, clause := range macComputerDestinationClauses {
		if !receipt.Destination.Assertions[clause.Clause] {
			return false
		}
		for _, id := range clause.Rows {
			if receipt.Rows[id].Status != "PASS" {
				return false
			}
		}
	}
	return true
}

func (receipt *macComputerMatrixReceipt) finish() {
	receipt.CompletedAt = time.Now().UTC()
	receipt.Status, receipt.NotRunIssue, receipt.NotRunReason = "PASS", 0, ""
	counts := map[string]int{"pass": 0, "fail": 0, "not_run": 0}
	for _, required := range macComputerMatrixRows {
		row := receipt.Rows[required.ID]
		switch row.Status {
		case "PASS":
			counts["pass"]++
		case "FAIL":
			counts["fail"]++
		case "NOT-RUN":
			counts["not_run"]++
		}
		if row.Status == "MISSING" || row.Status == "FAIL" || row.StartedAt == nil || row.CompletedAt == nil {
			receipt.Status = "FAIL"
			continue
		}
		if row.Status == "NOT-RUN" && receipt.Status != "FAIL" {
			receipt.Status = "NOT-RUN"
			if receipt.NotRunIssue == 0 {
				receipt.NotRunIssue, receipt.NotRunReason = row.NotRunIssue, row.Reason
			}
		}
	}
	for _, passed := range receipt.ResidueAssertions {
		if !passed {
			receipt.Status = "FAIL"
		}
	}
	receipt.AttendedRowCounts = counts
	receipt.Destination.Asserted = receipt.evaluateDestination()
}

// validateAttendedMacComputerReceipt is the gate every attended artifact passes
// before it becomes a matrix fragment. Like the OCI fragment producer it does
// not require PASS: a red attended session must still yield an honest, typed
// fragment. It requires the artifact to be well formed, bound, and redacted.
func validateAttendedMacComputerReceipt(payload []byte, candidate string) (*macComputerMatrixReceipt, error) {
	for _, forbidden := range macComputerRedactionDenylist {
		if bytes.Contains(bytes.ToLower(payload), bytes.ToLower([]byte(forbidden))) {
			return nil, fmt.Errorf("attended artifact carries redacted material %q", forbidden)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var receipt macComputerMatrixReceipt
	if err := decoder.Decode(&receipt); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("attended artifact contains trailing data")
	}
	if receipt.Version != macComputerMatrixVersion {
		return nil, fmt.Errorf("attended artifact version %d, want %d", receipt.Version, macComputerMatrixVersion)
	}
	if receipt.Commit != candidate {
		return nil, fmt.Errorf("attended artifact commit %q does not match candidate %q", receipt.Commit, candidate)
	}
	if receipt.SessionID == "" {
		return nil, errors.New("attended artifact carries no session id")
	}
	if receipt.Host.Limactl == "" || receipt.Host.Containerd == "" || receipt.Host.Runc == "" {
		return nil, errors.New("attended artifact omitted host tool versions")
	}
	if receipt.Host.LimaInstances != 1 {
		return nil, fmt.Errorf("attended artifact records %d Lima instances; one shared VM is the contract", receipt.Host.LimaInstances)
	}
	if err := validateMacComputerSetupEvidence(&receipt); err != nil {
		return nil, err
	}
	if len(receipt.Rows) != len(macComputerMatrixRows) {
		return nil, fmt.Errorf("attended artifact carries %d rows, want %d", len(receipt.Rows), len(macComputerMatrixRows))
	}
	for _, required := range macComputerMatrixRows {
		row, ok := receipt.Rows[required.ID]
		if !ok {
			return nil, fmt.Errorf("attended artifact omitted row %s", required.ID)
		}
		if row.ID != required.ID || row.Proof != required.Proof {
			return nil, fmt.Errorf("row %s does not carry its own stable id and proof: %+v", required.ID, row)
		}
		switch row.Status {
		case "PASS":
			if len(row.Assertions)+len(row.Attested) == 0 {
				return nil, fmt.Errorf("row %s claims PASS without earning it", required.ID)
			}
			for name, passed := range row.Assertions {
				if !passed {
					return nil, fmt.Errorf("row %s claims PASS with a false assertion %s", required.ID, name)
				}
			}
			if len(row.Gaps) != 0 {
				return nil, fmt.Errorf("row %s claims PASS while declaring a gap", required.ID)
			}
		case "NOT-RUN":
			if row.NotRunIssue <= 0 || strings.TrimSpace(row.Reason) == "" {
				return nil, fmt.Errorf("row %s is an untyped skip", required.ID)
			}
			for name, passed := range row.Assertions {
				if !passed {
					return nil, fmt.Errorf("row %s records a false partial assertion %s", required.ID, name)
				}
			}
		case "FAIL":
			if strings.TrimSpace(row.Reason) == "" {
				return nil, fmt.Errorf("row %s fails without a reason", required.ID)
			}
		default:
			return nil, fmt.Errorf("row %s carries status %q", required.ID, row.Status)
		}
	}
	if err := validateMacComputerLiveEvidence(&receipt); err != nil {
		return nil, err
	}
	if receipt.Destination.Asserted != receipt.evaluateDestination() {
		return nil, fmt.Errorf("destination.asserted=%t does not follow from the rows", receipt.Destination.Asserted)
	}
	if receipt.Destination.Asserted && receipt.Destination.Attestation != "human" && receipt.Destination.Attestation != "scripted" {
		return nil, fmt.Errorf("destination attestation %q is neither human nor scripted", receipt.Destination.Attestation)
	}
	return &receipt, nil
}

// macComputerRequiredRoles are the four Fabric identities the runbook makes the
// owner provision. They are setup facts, knowable before any Computer boots, so
// every attended artifact carries them however blocked the session was.
var macComputerRequiredRoles = []string{"administrator", "viewer", "second_device", "unauthorized"}

func isSHA256Digest(value string) bool {
	const prefix = "sha256:"
	if !strings.HasPrefix(value, prefix) || len(value) != len(prefix)+64 {
		return false
	}
	for _, character := range value[len(prefix):] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

// validateMacComputerSetupEvidence checks the facts that exist whether or not a
// single Computer ever booted: who was on the tailnet, which images were pinned,
// and the caps the contract fixes. A session blocked by #394 still has all of
// them, so they are unconditional.
func validateMacComputerSetupEvidence(receipt *macComputerMatrixReceipt) error {
	if len(receipt.FabricIdentities) < len(macComputerRequiredRoles) {
		return fmt.Errorf("attended artifact carries %d Fabric identities, want at least %d",
			len(receipt.FabricIdentities), len(macComputerRequiredRoles))
	}
	devices := make(map[string]string, len(receipt.FabricIdentities))
	roles := make(map[string]struct{}, len(receipt.FabricIdentities))
	for _, identity := range receipt.FabricIdentities {
		if identity.FabricID == "" || identity.UserID == "" || identity.DeviceID == "" {
			return fmt.Errorf("Fabric identity %+v omits an id", identity)
		}
		if owner, taken := devices[identity.DeviceID]; taken {
			return fmt.Errorf("roles %s and %s share device %s; the denial proofs need distinct devices",
				owner, identity.Role, identity.DeviceID)
		}
		devices[identity.DeviceID] = identity.Role
		roles[identity.Role] = struct{}{}
	}
	for _, role := range macComputerRequiredRoles {
		if _, ok := roles[role]; !ok {
			return fmt.Errorf("attended artifact omits the %s Fabric identity", role)
		}
	}
	if !isSHA256Digest(receipt.Image.IndexDigest) || !isSHA256Digest(receipt.Image.PlatformDigest) {
		return fmt.Errorf("attended artifact image digests are not pinned: %+v", receipt.Image)
	}
	caps := receipt.ResourceCaps
	if caps.MemoryBytes <= 0 || caps.DiskBytes <= 0 {
		return fmt.Errorf("attended artifact resource caps are unset: %+v", caps)
	}
	// The backup cap and the inflight boundary are contract constants (spec
	// sections 3.2 and 8), not host facts, so they are exact here as on Linux.
	if caps.BackupCap != 4 || caps.SubmitMaxInflight != 20 {
		return fmt.Errorf("attended artifact resource caps = %+v, want backup_cap 4 and submit_max_inflight 20", caps)
	}
	return nil
}

// validateMacComputerLiveEvidence checks the facts only a booted Computer can
// produce. A fully blocked session has none of them and must still be a valid
// fragment, so these bind to the first PASS row rather than to the artifact.
func validateMacComputerLiveEvidence(receipt *macComputerMatrixReceipt) error {
	passed := false
	for _, row := range receipt.Rows {
		if row.Status == "PASS" {
			passed = true
			break
		}
	}
	if !passed {
		return nil
	}
	if len(receipt.AuthorityGeneration) == 0 {
		return errors.New("attended artifact passes a row without recording an authority generation")
	}
	for _, generation := range receipt.AuthorityGeneration {
		if generation <= 0 {
			return fmt.Errorf("attended artifact records authority generation %d", generation)
		}
	}
	for name, identifiers := range map[string][]string{
		"computer_ids": receipt.ComputerIDs, "job_ids": receipt.JobIDs,
		"attempt_ids": receipt.AttemptIDs, "storage_ids": receipt.StorageIDs,
	} {
		if len(identifiers) == 0 {
			return fmt.Errorf("attended artifact passes a row with no %s", name)
		}
		for _, identifier := range identifiers {
			if strings.TrimSpace(identifier) == "" {
				return fmt.Errorf("attended artifact records an empty entry in %s", name)
			}
		}
	}
	return nil
}

func TestMacComputerMatrixRowsAreStableAndComplete(t *testing.T) {
	receipt := newMacComputerMatrixReceipt()
	if len(receipt.Rows) != 10 {
		t.Fatalf("Mac Computer matrix rows = %d, want 10", len(receipt.Rows))
	}
	for _, required := range macComputerMatrixRows {
		row, ok := receipt.Rows[required.ID]
		if !ok || row.ID != required.ID || row.Proof != required.Proof || row.Status != "MISSING" || row.NotRunIssue != 0 {
			t.Fatalf("Mac Computer matrix row %s = %#v, present=%t", required.ID, row, ok)
		}
		mirrored := strings.Replace(required.ID, "mac.", "linux.", 1)
		if required.ID == "mac.reference_image_narrowness" {
			continue
		}
		if !slices.ContainsFunc(linuxComputerMatrixRows, func(linux struct{ ID, Proof string }) bool {
			return linux.ID == mirrored && linux.Proof == required.Proof
		}) {
			t.Fatalf("row %s does not mirror a Linux row id and proof", required.ID)
		}
	}
}

func TestMacComputerMatrixReceiptRejectsUnearnedPass(t *testing.T) {
	receipt := newMacComputerMatrixReceipt()
	for id, row := range receipt.Rows {
		row.Status = "PASS"
		receipt.Rows[id] = row
	}
	receipt.finish()
	if receipt.Status != "FAIL" {
		t.Fatalf("unearned matrix PASS finished as %s", receipt.Status)
	}
	if receipt.Destination.Asserted {
		t.Fatal("the destination sentence was asserted over rows that never ran")
	}
}

func TestMacComputerMatrixMutationToggleFailsExactlyOwningRow(t *testing.T) {
	for _, mutated := range macComputerMatrixRows {
		t.Run(mutated.ID, func(t *testing.T) {
			receipt := newMacComputerMatrixReceipt()
			for _, required := range macComputerMatrixRows {
				receipt.begin(required.ID)
				assertions := map[string]bool{"attended_product_path": required.ID != mutated.ID}
				if required.ID == mutated.ID {
					if err := receipt.pass(required.ID, assertions, nil); err == nil {
						t.Fatalf("mutated row %s passed", required.ID)
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

func TestMacComputerDestinationRequiresEveryContributingRow(t *testing.T) {
	green := func(t *testing.T) *macComputerMatrixReceipt {
		t.Helper()
		receipt := newMacComputerMatrixReceipt()
		for _, required := range macComputerMatrixRows {
			receipt.begin(required.ID)
			if err := receipt.pass(required.ID, map[string]bool{"attended_product_path": true}, nil); err != nil {
				t.Fatal(err)
			}
		}
		for _, clause := range macComputerDestinationClauses {
			receipt.Destination.Assertions[clause.Clause] = true
		}
		receipt.Destination.Attestation = "human"
		return receipt
	}

	t.Run("every clause and row true", func(t *testing.T) {
		receipt := green(t)
		receipt.finish()
		if receipt.Status != "PASS" || !receipt.Destination.Asserted {
			t.Fatalf("status=%s asserted=%t; a complete green session asserts the destination", receipt.Status, receipt.Destination.Asserted)
		}
	})

	for _, clause := range macComputerDestinationClauses {
		t.Run("clause not attested/"+clause.Clause, func(t *testing.T) {
			receipt := green(t)
			delete(receipt.Destination.Assertions, clause.Clause)
			receipt.finish()
			if receipt.Destination.Asserted {
				t.Fatalf("the destination sentence survived a missing %s clause", clause.Clause)
			}
		})
	}

	for _, id := range []string{"mac.create_boot", "mac.remote_takeover", "mac.restart_survival", "mac.removal"} {
		t.Run("contributing row not PASS/"+id, func(t *testing.T) {
			receipt := green(t)
			if err := receipt.notRun(id, macComputerGatewayIssue, "blocked by #394", nil, map[string]string{"bridge": "bind refused"}); err != nil {
				t.Fatal(err)
			}
			receipt.finish()
			if receipt.Destination.Asserted {
				t.Fatalf("the destination sentence survived %s being NOT-RUN", id)
			}
		})
	}

	t.Run("plain-Fabric deviation forbids the destination", func(t *testing.T) {
		receipt := green(t)
		receipt.Deviations = append(receipt.Deviations, linuxComputerDeviation{
			ID: "dev.plain_fabric_identity", Status: "DEVIATION",
			Reason: "the attended session ran without the owner tailnet",
		})
		receipt.finish()
		if receipt.Status != "PASS" {
			t.Fatalf("status=%s; a deviation does not fail the rows", receipt.Status)
		}
		if receipt.Destination.Asserted {
			t.Fatal("the destination sentence says another tailnet device; plain identity does not satisfy it")
		}
	})
}

func TestAttendedMacComputerReceiptValidation(t *testing.T) {
	candidate := strings.Repeat("a", 40)
	conformant := func(t *testing.T) *macComputerMatrixReceipt {
		t.Helper()
		receipt := newMacComputerMatrixReceipt()
		receipt.SessionID, receipt.Commit = "attended-computer-2026-09-11T090000Z", candidate
		receipt.Host = macComputerHostEvidence{
			MacOS: "25.5.0", Limactl: "2.2.0", Containerd: "2.3.3", Runc: "1.5.1",
			HostMemoryBytes: 17179869184, VMMemoryBytes: 4294967296, VMCPUs: 4,
			VMDiskBytes: 34359738368, LimaInstances: 1,
		}
		receipt.Rendering = macComputerRenderingEvidence{GPU: false, Renderer: "cpu-xvfb", DRIPresent: false}
		receipt.Image = linuxComputerImageEvidence{
			Variant: "xfce", Reference: "ghcr.io/derek-x-wang/wefty-computer-reference",
			IndexDigest: "sha256:" + strings.Repeat("a", 64), PlatformDigest: "sha256:" + strings.Repeat("b", 64),
			Archive: "wefty-computer-reference.oci.tar",
		}
		receipt.FabricIdentities = []linuxComputerFabricIdentity{
			{Role: "administrator", FabricID: "fabric-admin", UserID: "owner", DeviceID: "mac-node"},
			{Role: "viewer", FabricID: "fabric-viewer", UserID: "owner", DeviceID: "second-laptop"},
			{Role: "second_device", FabricID: "fabric-second", UserID: "owner", DeviceID: "owner-phone"},
			{Role: "unauthorized", FabricID: "fabric-stranger", UserID: "stranger", DeviceID: "stranger-laptop"},
		}
		receipt.ResourceCaps = linuxComputerResourceCaps{
			MemoryBytes: 1 << 30, DiskBytes: 128 << 20, BackupCap: 4, SubmitMaxInflight: 20,
		}
		for _, required := range macComputerMatrixRows {
			receipt.begin(required.ID)
			issue := macComputerAbsentIssue
			reason := "the owner-hardware setup in docs/acceptance/m3.5-mac-computer.md has not been performed"
			if required.BlockedBy != 0 {
				issue, reason = required.BlockedBy, "blocked by #394: no Computer payload starts on a Mac"
			}
			if err := receipt.notRun(required.ID, issue, reason, nil, map[string]string{"blocked": reason}); err != nil {
				t.Fatal(err)
			}
		}
		receipt.finish()
		return receipt
	}
	marshal := func(t *testing.T, receipt *macComputerMatrixReceipt) []byte {
		t.Helper()
		payload, err := json.Marshal(receipt)
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}

	t.Run("a fully blocked session is still a valid fragment", func(t *testing.T) {
		receipt := conformant(t)
		if receipt.Status != "NOT-RUN" || receipt.AttendedRowCounts["not_run"] != 10 {
			t.Fatalf("status=%s counts=%v", receipt.Status, receipt.AttendedRowCounts)
		}
		if _, err := validateAttendedMacComputerReceipt(marshal(t, receipt), candidate); err != nil {
			t.Fatalf("honest blocked fragment rejected: %v", err)
		}
	})

	for name, mutate := range map[string]func(*testing.T, *macComputerMatrixReceipt){
		"missing row": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			delete(receipt.Rows, "mac.removal")
		},
		"row does not carry its own id": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			row := receipt.Rows["mac.removal"]
			row.ID = "mac.other"
			receipt.Rows["mac.removal"] = row
		},
		"untyped skip": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			row := receipt.Rows["mac.removal"]
			row.NotRunIssue = 0
			receipt.Rows["mac.removal"] = row
		},
		"skip without a reason": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			row := receipt.Rows["mac.removal"]
			row.Reason = ""
			receipt.Rows["mac.removal"] = row
		},
		"forged PASS with no assertion": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			row := receipt.Rows["mac.removal"]
			row.Status, row.Assertions, row.Gaps, row.NotRunIssue, row.Reason = "PASS", map[string]bool{}, nil, 0, ""
			receipt.Rows["mac.removal"] = row
		},
		"forged PASS with a false assertion": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			row := receipt.Rows["mac.removal"]
			row.Status, row.Assertions, row.Gaps, row.NotRunIssue, row.Reason = "PASS", map[string]bool{"residue_absent": false}, nil, 0, ""
			receipt.Rows["mac.removal"] = row
		},
		"PASS while declaring a gap": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			row := receipt.Rows["mac.removal"]
			row.Status, row.Assertions, row.NotRunIssue, row.Reason = "PASS", map[string]bool{"residue_absent": true}, 0, ""
			receipt.Rows["mac.removal"] = row
		},
		"FAIL without a reason": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			row := receipt.Rows["mac.removal"]
			row.Status, row.Reason, row.Gaps, row.NotRunIssue = "FAIL", "", nil, 0
			receipt.Rows["mac.removal"] = row
		},
		"unknown status": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			row := receipt.Rows["mac.removal"]
			row.Status = "SKIPPED"
			receipt.Rows["mac.removal"] = row
		},
		"forged destination": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.Destination.Asserted = true
			receipt.Destination.Attestation = "human"
			for _, clause := range macComputerDestinationClauses {
				receipt.Destination.Assertions[clause.Clause] = true
			}
		},
		"no session id": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.SessionID = ""
		},
		"wrong version": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.Version = 2
		},
		"no host versions": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.Host.Limactl = ""
		},
		"more than one Lima instance": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.Host.LimaInstances = 2
		},
		"fewer than four Fabric identities": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.FabricIdentities = receipt.FabricIdentities[:3]
		},
		"no second-device identity": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.FabricIdentities[2].Role = "viewer"
		},
		"no unauthorized identity": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.FabricIdentities[3].Role = "viewer"
		},
		"two roles share one device": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.FabricIdentities[2].DeviceID = receipt.FabricIdentities[0].DeviceID
		},
		"Fabric identity without a device id": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.FabricIdentities[1].DeviceID = ""
		},
		"Fabric identity without a user id": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.FabricIdentities[1].UserID = ""
		},
		"unpinned image index digest": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.Image.IndexDigest = ""
		},
		"malformed image platform digest": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.Image.PlatformDigest = "sha256:not-a-digest"
		},
		"unset memory cap": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.ResourceCaps.MemoryBytes = 0
		},
		"unset disk cap": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.ResourceCaps.DiskBytes = 0
		},
		"backup cap off contract": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.ResourceCaps.BackupCap = 3
		},
		"inflight boundary off contract": func(_ *testing.T, receipt *macComputerMatrixReceipt) {
			receipt.ResourceCaps.SubmitMaxInflight = 21
		},
	} {
		t.Run("reject/"+name, func(t *testing.T) {
			receipt := conformant(t)
			mutate(t, receipt)
			if _, err := validateAttendedMacComputerReceipt(marshal(t, receipt), candidate); err == nil {
				t.Fatal("the attended fragment gate accepted unearned evidence")
			}
		})
	}

	// The live-evidence checks bind to a PASS row, not to the artifact: a fully
	// blocked session has no Computer, no attempt and no authority generation,
	// and must still be a valid fragment.
	withOnePassingRow := func(t *testing.T) *macComputerMatrixReceipt {
		t.Helper()
		receipt := conformant(t)
		receipt.begin("mac.create_boot")
		if err := receipt.pass("mac.create_boot", map[string]bool{"trait_only_refusal_observed": true}, nil); err != nil {
			t.Fatal(err)
		}
		receipt.AuthorityGeneration = []int64{1}
		receipt.ComputerIDs, receipt.JobIDs = []string{"computer-1"}, []string{"job-1"}
		receipt.AttemptIDs, receipt.StorageIDs = []string{"attempt-1"}, []string{"storage-1"}
		receipt.finish()
		return receipt
	}

	t.Run("a session that passed a row carries its live evidence", func(t *testing.T) {
		if _, err := validateAttendedMacComputerReceipt(marshal(t, withOnePassingRow(t)), candidate); err != nil {
			t.Fatalf("a green row with full live evidence was rejected: %v", err)
		}
	})

	for name, mutate := range map[string]func(*macComputerMatrixReceipt){
		"no authority generation": func(receipt *macComputerMatrixReceipt) {
			receipt.AuthorityGeneration = nil
		},
		"non-positive authority generation": func(receipt *macComputerMatrixReceipt) {
			receipt.AuthorityGeneration = []int64{0}
		},
		"no computer ids": func(receipt *macComputerMatrixReceipt) { receipt.ComputerIDs = nil },
		"no job ids":      func(receipt *macComputerMatrixReceipt) { receipt.JobIDs = nil },
		"no attempt ids":  func(receipt *macComputerMatrixReceipt) { receipt.AttemptIDs = nil },
		"no storage ids":  func(receipt *macComputerMatrixReceipt) { receipt.StorageIDs = nil },
		"blank attempt id": func(receipt *macComputerMatrixReceipt) {
			receipt.AttemptIDs = []string{" "}
		},
	} {
		t.Run("reject/passing row without live evidence/"+name, func(t *testing.T) {
			receipt := withOnePassingRow(t)
			mutate(receipt)
			if _, err := validateAttendedMacComputerReceipt(marshal(t, receipt), candidate); err == nil {
				t.Fatal("a PASS row was accepted without the evidence only a booted Computer produces")
			}
		})
	}

	t.Run("a fully blocked session needs no live evidence", func(t *testing.T) {
		receipt := conformant(t)
		if len(receipt.ComputerIDs) != 0 || len(receipt.AuthorityGeneration) != 0 {
			t.Fatal("the blocked fixture invented live evidence")
		}
		if _, err := validateAttendedMacComputerReceipt(marshal(t, receipt), candidate); err != nil {
			t.Fatalf("the shipping case was rejected: %v", err)
		}
	})

	t.Run("reject/commit mismatch", func(t *testing.T) {
		receipt := conformant(t)
		if _, err := validateAttendedMacComputerReceipt(marshal(t, receipt), strings.Repeat("b", 40)); err == nil {
			t.Fatal("a fragment bound to another candidate was accepted")
		}
	})

	t.Run("reject/unknown field", func(t *testing.T) {
		payload := marshal(t, conformant(t))
		injected := append([]byte(`{"invented_field":true,`), payload[1:]...)
		if _, err := validateAttendedMacComputerReceipt(injected, candidate); err == nil {
			t.Fatal("an artifact with an unknown field was accepted")
		}
	})

	t.Run("reject/trailing data", func(t *testing.T) {
		payload := append(marshal(t, conformant(t)), []byte("\n{}")...)
		if _, err := validateAttendedMacComputerReceipt(payload, candidate); err == nil {
			t.Fatal("an artifact with trailing data was accepted")
		}
	})

	for _, forbidden := range macComputerRedactionDenylist {
		t.Run("reject/redaction/"+forbidden, func(t *testing.T) {
			receipt := conformant(t)
			receipt.Timings["leak"] = "value " + forbidden + " leaked"
			if _, err := validateAttendedMacComputerReceipt(marshal(t, receipt), candidate); err == nil {
				t.Fatalf("an artifact carrying %q was accepted", forbidden)
			}
		})
	}
}

// TestServiceAcceptanceAttendedMacComputerFragment folds the attended artifact
// from docs/acceptance/m3.5-mac-computer.md into the Mac half of the #196
// matrix. Like the OCI fragment producer it does not require destination
// success: a red or blocked attended session must still produce an honest,
// typed fragment.
func TestServiceAcceptanceAttendedMacComputerFragment(t *testing.T) {
	path := os.Getenv("WEFTY_MAC_COMPUTER_ACCEPTANCE_ARTIFACT")
	if path == "" {
		receipt, _ := json.Marshal(map[string]any{
			"status": "NOT-RUN", "not_run_issue": macComputerAbsentIssue,
			"reason": "set WEFTY_MAC_COMPUTER_ACCEPTANCE_ARTIFACT to the redacted owner-hardware receipt from docs/acceptance/m3.5-mac-computer.md",
		})
		t.Skip(string(receipt))
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("git", "rev-parse", "HEAD")
	command.Dir = ".."
	head, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	candidate := string(bytes.TrimSpace(head))
	receipt, err := validateAttendedMacComputerReceipt(payload, candidate)
	if err != nil {
		t.Fatal(err)
	}
	receipt.ArtifactSHA256 = fmt.Sprintf("%x", sha256.Sum256(payload))
	t.Logf("attended Mac Computer rows: %d PASS / %d FAIL / %d NOT-RUN; destination asserted=%t",
		receipt.AttendedRowCounts["pass"], receipt.AttendedRowCounts["fail"],
		receipt.AttendedRowCounts["not_run"], receipt.Destination.Asserted)
	out := os.Getenv("WEFTY_MAC_COMPUTER_MATRIX_OUT")
	if out == "" {
		return
	}
	fragment, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, append(fragment, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote the Mac Computer matrix fragment to %s", filepath.Clean(out))
}
