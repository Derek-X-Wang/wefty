package scripts_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The frozen row inventory of the M3 OCI acceptance matrix (spec section 9).
// Nine class rows under both platform prefixes, one capability row each, the
// Linux-only list and the Mac-only list.
var ociMatrixRows = []string{
	"linux.oneshot.image_identity", "linux.oneshot.delivery", "linux.oneshot.engine_loss",
	"linux.service.publication", "linux.service.restart", "linux.service.stop_start",
	"linux.service.data", "linux.service.crash_recovery", "linux.service.removal",
	"linux.node.capability_claims",
	"linux.only.unprivileged_agent", "linux.only.socket_activated_helper",
	"linux.only.cgroup_v2_limits", "linux.only.no_raw_containerd",
	"mac.oneshot.image_identity", "mac.oneshot.delivery", "mac.oneshot.engine_loss",
	"mac.service.publication", "mac.service.restart", "mac.service.stop_start",
	"mac.service.data", "mac.service.crash_recovery", "mac.service.removal",
	"mac.node.capability_claims",
	"mac.only.stopped_vm_autostart", "mac.only.helper_socket_authorization",
	"mac.only.raw_containerd_denied", "mac.only.dynamic_forwarding_disabled",
	"mac.only.dial_attempt_port", "mac.only.template_convergence",
	"mac.only.launch_topology", "mac.only.headless_reboot",
}

const ociMatrixCandidate = "0123456789abcdef0123456789abcdef01234567"

func TestOCIAcceptanceMatrixCoversEverySpecCell(t *testing.T) {
	for _, shell := range []string{"/bin/sh", "/bin/bash"} {
		t.Run(filepath.Base(shell), func(t *testing.T) {
			matrix := assembleOCIMatrix(t, shell, conformantLinuxOCIEvidence(t), conformantMacMatrixFragment(t), "published-artifact", "owner-hardware")
			if got := len(matrix["rows"].(map[string]any)); got != len(ociMatrixRows) {
				t.Fatalf("matrix rows = %d, want %d", got, len(ociMatrixRows))
			}
			for _, id := range ociMatrixRows {
				row, ok := matrix["rows"].(map[string]any)[id].(map[string]any)
				if !ok {
					t.Fatalf("matrix omitted spec cell %s", id)
				}
				if row["id"] != id || row["proof"] == "" || row["spec_refs"] == "" {
					t.Fatalf("row %s = %#v; every cell names its proof and its spec reference", id, row)
				}
				switch row["status"] {
				case "PASS":
				case "NOT-RUN":
					if row["not_run_issue"].(float64) <= 0 || strings.TrimSpace(row["not_run_reason"].(string)) == "" {
						t.Fatalf("row %s is an untyped skip: %#v", id, row)
					}
				default:
					t.Fatalf("row %s = %v; a conformant matrix has no other status", id, row["status"])
				}
			}
		})
	}
}

func TestOCIAcceptanceMatrixGate(t *testing.T) {
	for _, shell := range []string{"/bin/sh", "/bin/bash"} {
		t.Run(filepath.Base(shell), func(t *testing.T) {
			linux := conformantLinuxOCIEvidence(t)

			t.Run("complete attended matrix", func(t *testing.T) {
				fragment := conformantMacMatrixFragment(t)
				setMacFragmentRow(t, fragment, "mac.only.headless_reboot", map[string]any{
					"status": "PASS", "assertions": map[string]bool{"headless_reboot": true},
					"evidence": map[string]string{}, "gaps": map[string]string{},
					"not_run_issue": 0, "not_run_reason": "",
				})
				matrix := assembleOCIMatrix(t, shell, linux, fragment, "published-artifact", "owner-hardware")
				if matrix["complete"] != true || matrix["mac_open_rows"].(float64) != 0 {
					t.Fatalf("complete=%v mac_open_rows=%v", matrix["complete"], matrix["mac_open_rows"])
				}
				output, err := runOCIMatrixGate(t, shell, matrix, ociMatrixCandidate, "published-artifact", "owner-hardware")
				if err != nil {
					t.Fatalf("conformant matrix rejected: %v", err)
				}
				if !strings.Contains(output, "matrix complete:") {
					t.Fatalf("summary line = %q", output)
				}
			})

			t.Run("Mac half absent is honest, not green", func(t *testing.T) {
				matrix := assembleOCIMatrix(t, shell, linux, "none", "published-artifact", "github-hosted")
				if matrix["complete"] != false || matrix["mac_source"] != "absent" {
					t.Fatalf("complete=%v mac_source=%v", matrix["complete"], matrix["mac_source"])
				}
				output, err := runOCIMatrixGate(t, shell, matrix, ociMatrixCandidate, "published-artifact", "github-hosted")
				if err != nil {
					t.Fatalf("absent Mac half rejected: %v", err)
				}
				if !strings.Contains(output, "matrix incomplete: 18 Mac rows not run") {
					t.Fatalf("summary line = %q; a green CI must never read as a finished matrix", output)
				}
			})

			t.Run("pull-request lane typed skip", func(t *testing.T) {
				pr := t.TempDir()
				copyOCIEvidence(t, linux, pr)
				replaceOCIFact(t, filepath.Join(pr, "native-linux-oci.txt"), "pull_from_empty",
					"pull_from_empty=NOT-RUN\npull_from_empty_reason=pr-build: image not published")
				matrix := assembleOCIMatrix(t, shell, pr, "none", "pr-build", "github-hosted")
				row := matrix["rows"].(map[string]any)["linux.oneshot.image_identity"].(map[string]any)
				if row["status"] != "NOT-RUN" || !strings.Contains(row["not_run_reason"].(string), "pr-build: image not published") {
					t.Fatalf("pull-request row = %#v, want a typed skip naming the lane", row)
				}
				if _, err := runOCIMatrixGate(t, shell, matrix, ociMatrixCandidate, "pr-build", "github-hosted"); err != nil {
					t.Fatalf("typed pull-request skip rejected: %v", err)
				}
			})

			conformant := assembleOCIMatrix(t, shell, linux, conformantMacMatrixFragment(t), "published-artifact", "owner-hardware")
			for name, mutate := range map[string]func(*testing.T, map[string]any){
				"missing required row": func(t *testing.T, matrix map[string]any) {
					delete(matrix["rows"].(map[string]any), "linux.service.removal")
				},
				"unknown extra row": func(t *testing.T, matrix map[string]any) {
					matrix["rows"].(map[string]any)["linux.service.invented"] = map[string]any{
						"id": "linux.service.invented", "status": "PASS",
						"assertions": map[string]any{"invented": true}, "gaps": map[string]any{},
						"not_run_issue": 0, "not_run_reason": "",
					}
				},
				"row does not carry its own id": func(t *testing.T, matrix map[string]any) {
					matrix["rows"].(map[string]any)["linux.oneshot.delivery"].(map[string]any)["id"] = "linux.oneshot.other"
				},
				"MISSING row": func(t *testing.T, matrix map[string]any) {
					matrix["rows"].(map[string]any)["mac.service.data"].(map[string]any)["status"] = "MISSING"
				},
				"forged Mac PASS without the attended artifact": func(t *testing.T, matrix map[string]any) {
					matrix["mac_source"] = "absent"
					matrix["mac_evidence"].(map[string]any)["source"] = "absent"
					matrix["rows"].(map[string]any)["mac.service.removal"].(map[string]any)["status"] = "PASS"
				},
				"forged attended evidence on a GitHub-hosted runner": func(t *testing.T, matrix map[string]any) {
					matrix["runner_environment"] = "github-hosted"
				},
				"attended artifact bound to another commit": func(t *testing.T, matrix map[string]any) {
					matrix["mac_evidence"].(map[string]any)["commit"] = strings.Repeat("b", 40)
				},
				"attended artifact without a session": func(t *testing.T, matrix map[string]any) {
					matrix["mac_evidence"].(map[string]any)["session_id"] = ""
				},
				"Mac row not sourced from the attended artifact": func(t *testing.T, matrix map[string]any) {
					matrix["rows"].(map[string]any)["mac.only.launch_topology"].(map[string]any)["source"] = "realtiming-linux"
				},
				"untyped skip": func(t *testing.T, matrix map[string]any) {
					matrix["rows"].(map[string]any)["linux.node.capability_claims"].(map[string]any)["not_run_issue"] = 0
				},
				"skip without a reason": func(t *testing.T, matrix map[string]any) {
					matrix["rows"].(map[string]any)["linux.node.capability_claims"].(map[string]any)["not_run_reason"] = ""
				},
				"false assertion on a passing row": func(t *testing.T, matrix map[string]any) {
					matrix["rows"].(map[string]any)["linux.oneshot.delivery"].(map[string]any)["assertions"] = map[string]any{"oneshot_bridge_once": false}
				},
				"passing row with no assertion at all": func(t *testing.T, matrix map[string]any) {
					matrix["rows"].(map[string]any)["linux.oneshot.delivery"].(map[string]any)["assertions"] = map[string]any{}
				},
				"passing row that still declares a gap": func(t *testing.T, matrix map[string]any) {
					matrix["rows"].(map[string]any)["linux.oneshot.delivery"].(map[string]any)["gaps"] = map[string]any{"unproven": "not actually run"}
				},
				"aggregate status laundered to PASS": func(t *testing.T, matrix map[string]any) {
					matrix["status"] = "PASS"
				},
				"completeness laundered": func(t *testing.T, matrix map[string]any) {
					matrix["complete"] = true
				},
				"open Mac row count laundered": func(t *testing.T, matrix map[string]any) {
					matrix["mac_open_rows"] = 0
				},
				"mac_source disagrees with the evidence": func(t *testing.T, matrix map[string]any) {
					matrix["mac_source"] = "absent"
				},
				"unsupported version": func(t *testing.T, matrix map[string]any) {
					matrix["version"] = 2
				},
			} {
				t.Run("reject/"+name, func(t *testing.T) {
					matrix := copyOCIMatrix(t, conformant)
					mutate(t, matrix)
					if _, err := runOCIMatrixGate(t, shell, matrix, ociMatrixCandidate, "published-artifact", "owner-hardware"); err == nil {
						t.Fatal("the matrix gate accepted unearned evidence")
					}
				})
			}
		})
	}
}

func assembleOCIMatrix(t *testing.T, shell, linuxDirectory, macFragment, source, environment string) map[string]any {
	t.Helper()
	output := filepath.Join(t.TempDir(), "oci-acceptance-matrix.json")
	command := exec.Command(shell, "../scripts/assemble-oci-acceptance-matrix.sh",
		output, ociMatrixCandidate, source, linuxDirectory, macFragment, environment)
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

func runOCIMatrixGate(t *testing.T, shell string, matrix map[string]any, candidate, source, environment string) (string, error) {
	t.Helper()
	payload, err := json.Marshal(matrix)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "oci-acceptance-matrix.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(shell, "../scripts/check-oci-acceptance-matrix.sh", path, candidate, source, environment)
	combined, err := command.CombinedOutput()
	t.Logf("matrix gate: %s", combined)
	if err != nil {
		return string(combined), &ociMatrixGateError{err: err, output: string(combined)}
	}
	return string(combined), nil
}

type ociMatrixGateError struct {
	err    error
	output string
}

func (err *ociMatrixGateError) Error() string {
	return err.err.Error() + ": " + err.output
}

func setMacFragmentRow(t *testing.T, fragmentPath, id string, row map[string]any) {
	t.Helper()
	payload, err := os.ReadFile(fragmentPath)
	if err != nil {
		t.Fatal(err)
	}
	var fragment map[string]any
	if err := json.Unmarshal(payload, &fragment); err != nil {
		t.Fatal(err)
	}
	fragment["rows"].(map[string]any)[id] = row
	updated, err := json.Marshal(fragment)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fragmentPath, updated, 0o600); err != nil {
		t.Fatal(err)
	}
}

func copyOCIEvidence(t *testing.T, from, to string) {
	t.Helper()
	entries, err := os.ReadDir(from)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		payload, err := os.ReadFile(filepath.Join(from, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(to, entry.Name()), payload, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// replaceOCIFact rewrites one fact line in place, the way a lane that records a
// typed skip instead of a proof would have written it.
func replaceOCIFact(t *testing.T, path, key, replacement string) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(payload), "\n")
	replaced := false
	for index, line := range lines {
		if strings.HasPrefix(line, key+"=") {
			lines[index], replaced = replacement, true
			break
		}
	}
	if !replaced {
		t.Fatalf("evidence has no %s fact to replace", key)
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
}

func copyOCIMatrix(t *testing.T, matrix map[string]any) map[string]any {
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

// conformantLinuxOCIEvidence mirrors the realtiming evidence directory that
// TestNativeLinuxOCIAdapterLifecycle and the OCI service publication lane write.
func conformantLinuxOCIEvidence(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	write := func(name string, facts ...string) {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(strings.Join(facts, "\n")+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("native-linux-oci.txt",
		"agent_uid=1001", "helper_uid=0", "helper_socket_root_owned=true", "raw_socket_denied=true",
		"public_acceptance_image=true", "node_load_image=true", "archive_platform_filtered=true",
		"pull_from_empty=true", "pull_import_digest_equal=true", "binding_repull_reconciliation=true",
		"registry_disabled_pull_rejected=true", "registry_disabled_import=true", "import_run=true",
		"prestart_requeue_pinned=true", "tag_refloat_resolved_once=true",
		"service_echo_health=true", "service_echo_body=true",
		"service_data_root_user=true", "service_data_numeric_user=true", "service_data_named_user=true",
		"service_data_restart_persistent=true", "service_data_stop_start_persistent=true",
		"service_rootfs_discarded=true", "service_data_same_digest_replacement_fresh=true",
		"oneshot_handoff_marker_bytes=true", "oneshot_bridge_once=true", "oneshot_split_streams=true",
		"oneshot_digest_evidence=true", "ordinary_l3_oci_submission=true", "ordinary_l3_frozen_rerun=true",
		"wait_before_start=true", "live_log_delivery=true", "exit_code=7", "plain_137_exit=true",
		"oom_kill=true", "shim_loss=runtime_failure", "containerd_stop=runtime_failure",
		"control_loss_reaped=true", "stdout_log=true", "stderr_log=true", "namespace_absent=true")
	write("oci-service-publication-linux.txt",
		"health=true", "echo=true", "startup_timeout=true", "withdrawal=true", "republication=true",
		"port_collision_avoided=true", "portless_started=true", "helper_tunnel=true",
		"term_cooperative_stop=true", "term_kill_escalation=true", "term_kill_log_seal_pairing=true",
		"term_kill_stdout_log=true", "term_kill_stderr_log=true", "term_grace_stop=true",
		"fresh_restart_authority=true")
	write("oci-service-l1-agent-linux.txt",
		"fresh_restart=true", "stop_start=true", "slot_saturation=true", "retained_binding_digest=true",
		"service_helper_loss_injected=true", "service_helper_loss_observed=true",
		"service_fresh_attempt_readmission=true", "service_barrier_prefaced_during_startup=true",
		"service_lost_log_typed=true", "service_residue_verified_absent=true",
		"service_retained_binding_verified=true", "removal_manifest_complete=true", "removal_pending=true",
		"removal_every_attempt=true", "removal_service_data_volume=true",
		"removal_service_data_owner_record=true", "removal_post_delete_attestation=true",
		"removal_delete_attest_crash_injected=true", "removal_completed=true",
		"removal_prior_boot_oci_sweep=true")
	write("helper-restart-timeline.txt", "socket_and_service_active_after_recovery=true")
	return directory
}

// conformantMacMatrixFragment mirrors the fragment that writeMatrixFragment
// emits from the attended owner-hardware session.
func conformantMacMatrixFragment(t *testing.T) string {
	t.Helper()
	rows := map[string]any{}
	for _, id := range ociMatrixRows {
		if !strings.HasPrefix(id, "mac.") {
			continue
		}
		rows[id] = map[string]any{
			"status": "PASS", "assertions": map[string]bool{"attended": true},
			"evidence": map[string]string{"attended_rows": "probe"}, "gaps": map[string]string{},
			"not_run_issue": 0, "not_run_reason": "",
		}
	}
	rows["mac.only.headless_reboot"] = map[string]any{
		"status": "NOT-RUN", "assertions": map[string]bool{},
		"evidence": map[string]string{}, "gaps": map[string]string{"headless_reboot_evidence": "owned by #128"},
		"not_run_issue": 128, "not_run_reason": "headless cold-reboot evidence is owned by the #128 prototype",
	}
	payload, err := json.Marshal(map[string]any{
		"version": 1, "session_id": "attended-2026-09-11T075000Z", "commit": ociMatrixCandidate,
		"artifact_sha256":     strings.Repeat("c", 64),
		"attended_row_counts": map[string]int{"pass": 34, "fail": 0, "not_run": 0},
		"rows":                rows,
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "mac-oci-matrix-rows.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
