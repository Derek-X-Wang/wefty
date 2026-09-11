package scripts_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The frozen row inventory of the agent-computer acceptance matrix
// (agent-computer spec section 10): the nine Linux Computer rows the realtiming
// receipt carries, and their ten attended Mac mirrors.
var computerMatrixRows = []string{
	"linux.create_boot", "linux.network_egress", "linux.screen_crossover_refused",
	"linux.remote_takeover", "linux.restart_survival", "linux.reconfiguration",
	"linux.storage_provenance", "linux.guest_authority", "linux.removal",
	"mac.create_boot", "mac.network_egress", "mac.screen_crossover_refused",
	"mac.remote_takeover", "mac.restart_survival", "mac.reconfiguration",
	"mac.storage_provenance", "mac.guest_authority", "mac.removal",
	"mac.reference_image_narrowness",
}

const computerMatrixCandidate = "0123456789abcdef0123456789abcdef01234567"

const (
	// No attended owner-hardware session ran; hosted macOS cannot boot nested vz.
	computerMatrixAbsentIssue = 128
	// The Lima vz bridge-bind defect that stops any Computer payload on a Mac.
	computerMatrixGatewayIssue = 394
	// The Linux lane's own standing skip: the complete M3 OCI matrix root result.
	computerMatrixLinuxSkipIssue = 157
)

// computerMatrixGatewayBlockedRows need a booted Computer, so #394 is named as
// their blocking cause even when the reason they were not run is the absent
// attended session. The other two rows can record partial evidence today.
var computerMatrixGatewayBlockedRows = []string{
	"mac.network_egress", "mac.screen_crossover_refused", "mac.remote_takeover",
	"mac.restart_survival", "mac.reconfiguration", "mac.storage_provenance",
	"mac.guest_authority", "mac.removal",
}

func TestComputerAcceptanceMatrixCoversEverySpecCell(t *testing.T) {
	for _, shell := range []string{"/bin/sh", "/bin/bash"} {
		t.Run(filepath.Base(shell), func(t *testing.T) {
			matrix := assembleComputerMatrix(t, shell, conformantLinuxComputerEvidence(t),
				conformantMacComputerFragment(t, true), "published-artifact", "owner-hardware")
			rows := matrix["rows"].(map[string]any)
			if got := len(rows); got != len(computerMatrixRows) {
				t.Fatalf("matrix rows = %d, want %d", got, len(computerMatrixRows))
			}
			for _, id := range computerMatrixRows {
				row, ok := rows[id].(map[string]any)
				if !ok {
					t.Fatalf("matrix omitted spec cell %s", id)
				}
				if row["id"] != id || row["proof"] == "" || row["spec_refs"] == "" {
					t.Fatalf("row %s = %#v; every cell names its proof and its spec reference", id, row)
				}
			}
			if got := rows["mac.reference_image_narrowness"].(map[string]any)["spec_refs"]; got != "11 item 4" {
				t.Fatalf("the narrowness row cites %v, want the section 11 item 4 amendment", got)
			}
			if got := int(matrix["linux_evidence"].(map[string]any)["receipt_version"].(float64)); got != 5 {
				t.Fatalf("linux receipt_version = %d, want the unbumped 5", got)
			}
		})
	}
}

func TestComputerAcceptanceMatrixGate(t *testing.T) {
	for _, shell := range []string{"/bin/sh", "/bin/bash"} {
		t.Run(filepath.Base(shell), func(t *testing.T) {
			linux := conformantLinuxComputerEvidence(t)

			t.Run("Mac half absent is honest, not green", func(t *testing.T) {
				matrix := assembleComputerMatrix(t, shell, linux, "none", "published-artifact", "github-hosted")
				if matrix["complete"] != false || matrix["mac_source"] != "absent" {
					t.Fatalf("complete=%v mac_source=%v", matrix["complete"], matrix["mac_source"])
				}
				rows := matrix["rows"].(map[string]any)
				for _, id := range computerMatrixRows {
					if !strings.HasPrefix(id, "mac.") {
						continue
					}
					row := rows[id].(map[string]any)
					if row["status"] != "NOT-RUN" || int(row["not_run_issue"].(float64)) != computerMatrixAbsentIssue {
						t.Fatalf("row %s = %v/#%v, want a NOT-RUN owned by #%d", id, row["status"], row["not_run_issue"], computerMatrixAbsentIssue)
					}
					gaps := row["gaps"].(map[string]any)
					if _, ok := gaps["attended_session"]; !ok {
						t.Fatalf("row %s does not name the absent attended session", id)
					}
					defect, blocked := gaps["product_defect"]
					wantBlocked := containsComputerRow(computerMatrixGatewayBlockedRows, id)
					if blocked != wantBlocked {
						t.Fatalf("row %s product_defect present=%t, want %t", id, blocked, wantBlocked)
					}
					if wantBlocked && !strings.Contains(defect.(string), "#394") {
						t.Fatalf("row %s names %q, want the #394 blocker", id, defect)
					}
					if !wantBlocked {
						if _, ok := gaps["owner_hardware_setup"]; !ok {
							t.Fatalf("row %s is not blocked by #394 and does not name the missing setup", id)
						}
					}
				}
				output, err := runComputerMatrixGate(t, shell, matrix, computerMatrixCandidate, "published-artifact", "github-hosted", "")
				if err != nil {
					t.Fatalf("absent Mac half rejected: %v", err)
				}
				if !strings.Contains(output, "matrix incomplete: 10 Mac rows not run") {
					t.Fatalf("summary line = %q; a green CI must never read as a finished matrix", output)
				}
			})

			t.Run("the Linux standing skip stays owned by #157", func(t *testing.T) {
				matrix := assembleComputerMatrix(t, shell, linux, "none", "published-artifact", "github-hosted")
				row := matrix["rows"].(map[string]any)["linux.guest_authority"].(map[string]any)
				if row["status"] != "NOT-RUN" || int(row["not_run_issue"].(float64)) != computerMatrixLinuxSkipIssue {
					t.Fatalf("row = %#v, want the Linux receipt's own #157 skip carried through", row)
				}
				if strings.TrimSpace(row["reason"].(string)) == "" {
					t.Fatal("the Linux skip lost its reason on the way into the matrix")
				}
			})

			t.Run("complete attended matrix", func(t *testing.T) {
				matrix := assembleComputerMatrix(t, shell, conformantLinuxComputerEvidenceAllPass(t),
					conformantMacComputerFragment(t, true), "published-artifact", "owner-hardware")
				if matrix["complete"] != true || matrix["status"] != "PASS" {
					t.Fatalf("complete=%v status=%v", matrix["complete"], matrix["status"])
				}
				output, err := runComputerMatrixGate(t, shell, matrix, computerMatrixCandidate, "published-artifact", "owner-hardware", "")
				if err != nil {
					t.Fatalf("conformant matrix rejected: %v", err)
				}
				if !strings.Contains(output, "matrix complete:") {
					t.Fatalf("summary line = %q", output)
				}
			})

			// The shipping case today: the owner ran the lane, #394 blocked every
			// row, and the fragment says so honestly. It must pass the gate and it
			// must not read as a finished matrix.
			t.Run("an honest blocked attended session passes without completing", func(t *testing.T) {
				matrix := assembleComputerMatrix(t, shell, linux,
					conformantMacComputerFragment(t, false), "published-artifact", "owner-hardware")
				if matrix["complete"] != false || matrix["mac_source"] != "attended-owner-hardware" {
					t.Fatalf("complete=%v mac_source=%v", matrix["complete"], matrix["mac_source"])
				}
				rows := matrix["rows"].(map[string]any)
				for _, id := range computerMatrixRows {
					if !strings.HasPrefix(id, "mac.") {
						continue
					}
					row := rows[id].(map[string]any)
					if row["status"] != "NOT-RUN" || int(row["not_run_issue"].(float64)) != computerMatrixGatewayIssue {
						t.Fatalf("row %s = %v/#%v, want the attended lane's own #394 skip", id, row["status"], row["not_run_issue"])
					}
					if row["source"] != "attended-owner-hardware" {
						t.Fatalf("row %s is sourced from %v, not the attended artifact", id, row["source"])
					}
				}
				output, err := runComputerMatrixGate(t, shell, matrix, computerMatrixCandidate, "published-artifact", "owner-hardware", "")
				if err != nil {
					t.Fatalf("an honest blocked attended session was rejected: %v", err)
				}
				if !strings.Contains(output, "matrix incomplete: 10 Mac rows not run") {
					t.Fatalf("summary line = %q; a blocked session must not read as a finished matrix", output)
				}
			})

			t.Run("mutation dispatch requires exactly its own row", func(t *testing.T) {
				mutated := mutatedLinuxComputerEvidence(t, "linux.reconfiguration")
				matrix := assembleComputerMatrix(t, shell, mutated, "none", "published-artifact", "github-hosted")
				if matrix["status"] != "FAIL" {
					t.Fatalf("status=%v, want FAIL while a Linux row is mutated", matrix["status"])
				}
				if _, err := runComputerMatrixGate(t, shell, matrix, computerMatrixCandidate, "published-artifact", "github-hosted", ""); err == nil {
					t.Fatal("an unannounced failing row passed the gate")
				}
				output, err := runComputerMatrixGate(t, shell, matrix, computerMatrixCandidate, "published-artifact", "github-hosted", "linux.reconfiguration")
				if err != nil {
					t.Fatalf("the announced mutation was rejected: %v", err)
				}
				if !strings.Contains(output, "matrix mutation observed: linux.reconfiguration") {
					t.Fatalf("summary line = %q", output)
				}
				if _, err := runComputerMatrixGate(t, shell, matrix, computerMatrixCandidate, "published-artifact", "github-hosted", "linux.removal"); err == nil {
					t.Fatal("a mutation dispatch naming the wrong row passed the gate")
				}
			})

			t.Run("a GitHub-hosted runner can never claim the attended Mac half", func(t *testing.T) {
				matrix := assembleComputerMatrix(t, shell, conformantLinuxComputerEvidenceAllPass(t),
					conformantMacComputerFragment(t, true), "published-artifact", "github-hosted")
				output, err := runComputerMatrixGate(t, shell, matrix, computerMatrixCandidate, "published-artifact", "github-hosted", "")
				if err == nil {
					t.Fatal("a hosted runner claimed attended owner-hardware evidence")
				}
				if !strings.Contains(output, "a GitHub-hosted runner claimed attended Mac evidence") {
					t.Fatalf("the gate rejected for the wrong reason: %q", output)
				}
			})

			conformant := assembleComputerMatrix(t, shell, conformantLinuxComputerEvidenceAllPass(t),
				conformantMacComputerFragment(t, true), "published-artifact", "owner-hardware")
			for name, mutate := range map[string]func(map[string]any){
				"missing required row": func(matrix map[string]any) {
					delete(matrix["rows"].(map[string]any), "mac.removal")
				},
				"unknown extra row": func(matrix map[string]any) {
					matrix["rows"].(map[string]any)["mac.invented"] = map[string]any{
						"id": "mac.invented", "status": "PASS", "assertions": map[string]any{"invented": true},
						"attested": map[string]any{}, "gaps": map[string]any{}, "not_run_issue": 0, "reason": "",
					}
				},
				"row does not carry its own id": func(matrix map[string]any) {
					matrix["rows"].(map[string]any)["mac.removal"].(map[string]any)["id"] = "mac.other"
				},
				"failing row": func(matrix map[string]any) {
					row := matrix["rows"].(map[string]any)["mac.removal"].(map[string]any)
					row["status"], row["reason"] = "FAIL", "the guest retained a loop device"
					matrix["status"], matrix["complete"] = "FAIL", false
				},
				"failing row without a reason": func(matrix map[string]any) {
					row := matrix["rows"].(map[string]any)["mac.removal"].(map[string]any)
					row["status"], row["reason"] = "FAIL", ""
					matrix["status"], matrix["complete"] = "FAIL", false
				},
				"MISSING row": func(matrix map[string]any) {
					matrix["rows"].(map[string]any)["mac.service_missing"] = nil
					delete(matrix["rows"].(map[string]any), "mac.service_missing")
					matrix["rows"].(map[string]any)["mac.storage_provenance"].(map[string]any)["status"] = "MISSING"
				},
				"Linux evidence bound to another commit": func(matrix map[string]any) {
					matrix["linux_evidence"].(map[string]any)["commit"] = strings.Repeat("d", 40)
				},
				"Linux evidence without a receipt version": func(matrix map[string]any) {
					matrix["linux_evidence"].(map[string]any)["receipt_version"] = 0
				},
				"forged Mac PASS without the attended artifact": func(matrix map[string]any) {
					matrix["mac_source"] = "absent"
					matrix["mac_evidence"].(map[string]any)["source"] = "absent"
				},
				"forged attended evidence on a GitHub-hosted runner": func(matrix map[string]any) {
					matrix["runner_environment"] = "github-hosted"
				},
				"attended artifact bound to another commit": func(matrix map[string]any) {
					matrix["mac_evidence"].(map[string]any)["commit"] = strings.Repeat("b", 40)
				},
				"attended artifact without a session": func(matrix map[string]any) {
					matrix["mac_evidence"].(map[string]any)["session_id"] = ""
				},
				"Mac row not sourced from the attended artifact": func(matrix map[string]any) {
					matrix["rows"].(map[string]any)["mac.create_boot"].(map[string]any)["source"] = "realtiming-linux"
				},
				"untyped skip": func(matrix map[string]any) {
					row := matrix["rows"].(map[string]any)["mac.removal"].(map[string]any)
					row["status"], row["not_run_issue"], row["reason"] = "NOT-RUN", 0, "blocked"
					matrix["status"], matrix["complete"] = "NOT-RUN", false
				},
				"skip without a reason": func(matrix map[string]any) {
					row := matrix["rows"].(map[string]any)["mac.removal"].(map[string]any)
					row["status"], row["not_run_issue"], row["reason"] = "NOT-RUN", 394, ""
					matrix["status"], matrix["complete"] = "NOT-RUN", false
				},
				"false assertion on a passing row": func(matrix map[string]any) {
					matrix["rows"].(map[string]any)["mac.create_boot"].(map[string]any)["assertions"] = map[string]any{"attended": false}
				},
				"passing row with neither an assertion nor an attestation": func(matrix map[string]any) {
					row := matrix["rows"].(map[string]any)["mac.create_boot"].(map[string]any)
					row["assertions"], row["attested"] = map[string]any{}, map[string]any{}
				},
				"passing row that still declares a gap": func(matrix map[string]any) {
					matrix["rows"].(map[string]any)["mac.create_boot"].(map[string]any)["gaps"] = map[string]any{"unproven": "not actually run"}
				},
				"complete Mac half without the destination sentence": func(matrix map[string]any) {
					matrix["mac_evidence"].(map[string]any)["destination_asserted"] = false
				},
				"destination asserted over a plain-Fabric deviation": func(matrix map[string]any) {
					matrix["mac_evidence"].(map[string]any)["plain_fabric_deviation"] = true
				},
				"destination asserted with a Mac row still open": func(matrix map[string]any) {
					row := matrix["rows"].(map[string]any)["mac.removal"].(map[string]any)
					row["status"], row["not_run_issue"] = "NOT-RUN", computerMatrixGatewayIssue
					row["reason"], row["assertions"] = "blocked by #394", map[string]any{}
					matrix["status"], matrix["complete"] = "NOT-RUN", false
				},
				"aggregate status laundered to PASS": func(matrix map[string]any) {
					matrix["rows"].(map[string]any)["mac.removal"].(map[string]any)["status"] = "NOT-RUN"
					matrix["rows"].(map[string]any)["mac.removal"].(map[string]any)["not_run_issue"] = 394
					matrix["rows"].(map[string]any)["mac.removal"].(map[string]any)["reason"] = "blocked by #394"
				},
				"completeness laundered": func(matrix map[string]any) {
					matrix["rows"].(map[string]any)["mac.removal"].(map[string]any)["status"] = "NOT-RUN"
					matrix["rows"].(map[string]any)["mac.removal"].(map[string]any)["not_run_issue"] = 394
					matrix["rows"].(map[string]any)["mac.removal"].(map[string]any)["reason"] = "blocked by #394"
					matrix["status"] = "NOT-RUN"
				},
				"mac_source disagrees with the evidence": func(matrix map[string]any) {
					matrix["mac_source"] = "absent"
				},
				"unsupported version": func(matrix map[string]any) {
					matrix["version"] = 2
				},
				"unknown row status": func(matrix map[string]any) {
					matrix["rows"].(map[string]any)["mac.removal"].(map[string]any)["status"] = "SKIPPED"
				},
			} {
				t.Run("reject/"+name, func(t *testing.T) {
					matrix := copyComputerMatrix(t, conformant)
					mutate(matrix)
					if _, err := runComputerMatrixGate(t, shell, matrix, computerMatrixCandidate, "published-artifact", "owner-hardware", ""); err == nil {
						t.Fatal("the matrix gate accepted unearned evidence")
					}
				})
			}
		})
	}
}

// TestComputerAcceptanceMatrixIsWiredIntoBothRealtimingLanes keeps the two
// evidence workflows from drifting: a matrix gated on one lane and not the other
// is a matrix that can be dodged.
func TestComputerAcceptanceMatrixIsWiredIntoBothRealtimingLanes(t *testing.T) {
	assemble := extractComputerMatrixInvocation(t, "assemble-computer-acceptance-matrix.sh")
	check := extractComputerMatrixInvocation(t, "check-computer-acceptance-matrix.sh")
	for _, lines := range []map[string][]string{assemble, check} {
		var reference []string
		for workflow, extracted := range lines {
			if reference == nil {
				reference = extracted
				continue
			}
			if strings.Join(reference, "\n") != strings.Join(extracted, "\n") {
				t.Fatalf("%s invokes the matrix differently:\n%s\nvs\n%s",
					workflow, strings.Join(reference, "\n"), strings.Join(extracted, "\n"))
			}
		}
	}
	for _, workflow := range computerMatrixWorkflows() {
		payload := readComputerMatrixWorkflow(t, workflow)
		for _, required := range []string{
			"COMPUTER_MATRIX_CANDIDATE_SHA:",
			"COMPUTER_MATRIX_EVIDENCE_SOURCE:",
			"COMPUTER_MATRIX_MUTATED_ROW:",
			"name: computer-acceptance-matrix-${{ needs.resolve-published-artifact.outputs.candidate-sha }}",
			"path: ${{ runner.temp }}/computer-acceptance-matrix.json",
		} {
			if !strings.Contains(payload, required) {
				t.Fatalf("%s does not carry %q", workflow, required)
			}
		}
	}
}

func computerMatrixWorkflows() []string {
	return []string{
		"../.github/workflows/service-acceptance-realtiming.yml",
		"../.github/workflows/service-acceptance-realtiming-scheduled.yml",
	}
}

func readComputerMatrixWorkflow(t *testing.T, path string) string {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}

// extractComputerMatrixInvocation returns the continued shell invocation of the
// named script from every realtiming workflow, trimmed of indentation.
func extractComputerMatrixInvocation(t *testing.T, script string) map[string][]string {
	t.Helper()
	found := make(map[string][]string, len(computerMatrixWorkflows()))
	for _, workflow := range computerMatrixWorkflows() {
		lines := strings.Split(readComputerMatrixWorkflow(t, workflow), "\n")
		for index, line := range lines {
			if !strings.HasPrefix(strings.TrimSpace(line), "scripts/"+script) {
				continue
			}
			invocation := []string{strings.TrimSpace(line)}
			for next := index + 1; next < len(lines) && strings.HasSuffix(invocation[len(invocation)-1], "\\"); next++ {
				invocation = append(invocation, strings.TrimSpace(lines[next]))
			}
			found[workflow] = invocation
			break
		}
		if _, ok := found[workflow]; !ok {
			t.Fatalf("%s never invokes scripts/%s", workflow, script)
		}
	}
	return found
}

func containsComputerRow(rows []string, id string) bool {
	for _, row := range rows {
		if row == id {
			return true
		}
	}
	return false
}

func assembleComputerMatrix(t *testing.T, shell, linuxDirectory, macFragment, source, environment string) map[string]any {
	t.Helper()
	output := filepath.Join(t.TempDir(), "computer-acceptance-matrix.json")
	command := exec.Command(shell, "../scripts/assemble-computer-acceptance-matrix.sh",
		output, computerMatrixCandidate, source, linuxDirectory, macFragment, environment)
	if combined, err := command.CombinedOutput(); err != nil {
		t.Fatalf("assemble matrix: %v: %s", err, combined)
	}
	payload, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var matrix map[string]any
	if err := json.Unmarshal(payload, &matrix); err != nil {
		t.Fatal(err)
	}
	return matrix
}

func runComputerMatrixGate(t *testing.T, shell string, matrix map[string]any, candidate, source, environment, mutated string) (string, error) {
	t.Helper()
	payload, err := json.Marshal(matrix)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "computer-acceptance-matrix.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	arguments := []string{"../scripts/check-computer-acceptance-matrix.sh", path, candidate, source, environment}
	if mutated != "" {
		arguments = append(arguments, mutated)
	}
	combined, err := exec.Command(shell, arguments...).CombinedOutput()
	t.Logf("computer matrix gate: %s", combined)
	if err != nil {
		return string(combined), &computerMatrixGateError{err: err, output: string(combined)}
	}
	return string(combined), nil
}

type computerMatrixGateError struct {
	err    error
	output string
}

func (err *computerMatrixGateError) Error() string {
	return err.err.Error() + ": " + err.output
}

func copyComputerMatrix(t *testing.T, matrix map[string]any) map[string]any {
	t.Helper()
	payload, err := json.Marshal(matrix)
	if err != nil {
		t.Fatal(err)
	}
	var copied map[string]any
	if err := json.Unmarshal(payload, &copied); err != nil {
		t.Fatal(err)
	}
	return copied
}

// conformantLinuxComputerEvidence mirrors the realtiming evidence directory the
// Linux Computer lane uploads: the version-5 typed receipt plus its provenance.
func conformantLinuxComputerEvidence(t *testing.T) string {
	t.Helper()
	return writeLinuxComputerEvidence(t, func(rows map[string]any) {
		guest := rows["linux.guest_authority"].(map[string]any)
		guest["status"] = "NOT-RUN"
		guest["not_run_issue"] = computerMatrixLinuxSkipIssue
		guest["not_run_reason"] = "the complete M3 OCI matrix does not publish the candidate-bound root Run result"
	})
}

func conformantLinuxComputerEvidenceAllPass(t *testing.T) string {
	t.Helper()
	return writeLinuxComputerEvidence(t, func(map[string]any) {})
}

func mutatedLinuxComputerEvidence(t *testing.T, mutated string) string {
	t.Helper()
	return writeLinuxComputerEvidence(t, func(rows map[string]any) {
		row := rows[mutated].(map[string]any)
		row["status"] = "FAIL"
		row["assertions"] = map[string]any{"live_product_path": false}
		row["not_run_reason"] = "the mutation dispatch removed one owning product-path assertion"
	})
}

func writeLinuxComputerEvidence(t *testing.T, mutate func(map[string]any)) string {
	t.Helper()
	directory := t.TempDir()
	rows := map[string]any{}
	for _, id := range computerMatrixRows {
		if !strings.HasPrefix(id, "linux.") {
			continue
		}
		rows[id] = map[string]any{
			"id": id, "proof": "live product path", "status": "PASS",
			"assertions":   map[string]any{"live_product_path": true},
			"evidence":     map[string]any{"computer_id": "computer-1"},
			"started_at":   "2026-08-29T00:00:00Z",
			"completed_at": "2026-08-29T00:01:00Z",
		}
	}
	mutate(rows)
	receipt := map[string]any{
		"version": 5, "status": "NOT-RUN", "not_run_issue": computerMatrixLinuxSkipIssue,
		"candidate_sha": computerMatrixCandidate, "platform": "linux/amd64",
		"image": map[string]any{"variant": "xfce"}, "rows": rows,
	}
	writeComputerJSON(t, filepath.Join(directory, "linux-computer-matrix.json"), receipt)
	writeComputerJSON(t, filepath.Join(directory, "provenance-receipt.json"), map[string]any{
		"version": 1, "commit": computerMatrixCandidate,
		"source": "published-artifact", "artifact_run_id": "4242",
	})
	return directory
}

// conformantMacComputerFragment mirrors the attended artifact that
// TestServiceAcceptanceAttendedMacComputerFragment validates and copies out.
func conformantMacComputerFragment(t *testing.T, green bool) string {
	t.Helper()
	rows := map[string]any{}
	for _, id := range computerMatrixRows {
		if !strings.HasPrefix(id, "mac.") {
			continue
		}
		row := map[string]any{
			"id": id, "status": "PASS", "assertions": map[string]any{"attended_product_path": true},
			"attested": map[string]any{}, "evidence": map[string]any{"attestation": "human"},
			"gaps": map[string]any{}, "not_run_issue": 0, "reason": "",
		}
		if !green {
			row["status"] = "NOT-RUN"
			row["assertions"] = map[string]any{}
			row["not_run_issue"] = computerMatrixGatewayIssue
			row["reason"] = "blocked by #394"
			row["gaps"] = map[string]any{"product_defect": "blocked by #394"}
		}
		rows[id] = row
	}
	path := filepath.Join(t.TempDir(), "mac-computer-matrix.json")
	writeComputerJSON(t, path, map[string]any{
		"version": 1, "status": "PASS", "session_id": "attended-computer-2026-09-11T090000Z",
		"commit": computerMatrixCandidate, "artifact_sha256": strings.Repeat("c", 64),
		"attended_row_counts": map[string]int{"pass": 10, "fail": 0, "not_run": 0},
		"destination":         map[string]any{"asserted": green, "attestation": "human"},
		"deviations":          []any{},
		"rows":                rows,
	})
	return path
}

func writeComputerJSON(t *testing.T, path string, value any) {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
}
