package scripts

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var linuxComputerReceiptRows = []string{
	"linux.create_boot",
	"linux.network_egress",
	"linux.screen_crossover_refused",
	"linux.remote_takeover",
	"linux.restart_survival",
	"linux.reconfiguration",
	"linux.storage_provenance",
	"linux.guest_authority",
	"linux.removal",
}

func TestLinuxComputerReceiptGate(t *testing.T) {
	const candidate = "0123456789abcdef0123456789abcdef01234567"
	writeReceipt := func(t *testing.T, receipt map[string]any) string {
		t.Helper()
		payload, err := json.Marshal(receipt)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "linux-computer-matrix.json")
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	runGate := func(t *testing.T, receipt map[string]any, image, mutated string) error {
		t.Helper()
		arguments := []string{"../scripts/check-linux-computer-receipt.sh", writeReceipt(t, receipt), candidate, image}
		if mutated != "" {
			arguments = append(arguments, mutated)
		}
		command := exec.Command("sh", arguments...)
		if output, err := command.CombinedOutput(); err != nil {
			return &receiptGateError{err: err, output: string(output)}
		}
		return nil
	}

	for _, image := range []string{"xfce", "wayland"} {
		if err := runGate(t, conformantLinuxComputerReceipt(candidate, image), image, ""); err != nil {
			t.Fatalf("conformant %s receipt failed: %v", image, err)
		}
		t.Logf("green %s receipt gate: PASS", image)
	}
	for _, mutated := range linuxComputerReceiptRows {
		t.Run("mutation/"+mutated, func(t *testing.T) {
			receipt := conformantLinuxComputerReceipt(candidate, "xfce")
			receipt["status"] = "FAIL"
			row := receipt["rows"].(map[string]any)[mutated].(map[string]any)
			row["status"] = "FAIL"
			row["assertions"] = map[string]bool{"live_product_path": false}
			if err := runGate(t, receipt, "xfce", mutated); err != nil {
				t.Fatalf("owning-row mutation receipt failed: %v", err)
			}
			t.Logf("mutated %s receipt gate: PASS", mutated)
		})
	}
	for name, mutate := range map[string]func(map[string]any){
		"missing row": func(receipt map[string]any) {
			delete(receipt["rows"].(map[string]any), "linux.removal")
		},
		"unexpected not-run": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.create_boot"].(map[string]any)["status"] = "NOT-RUN"
		},
		"wrong aggregate not-run issue": func(receipt map[string]any) {
			receipt["not_run_issue"] = 286
		},
		"missing restore token revocation evidence": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.storage_provenance"].(map[string]any)["evidence"] = map[string]string{}
		},
		"false assertion": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.create_boot"].(map[string]any)["assertions"] = map[string]bool{"live_product_path": false}
		},
		"wrong image variant": func(receipt map[string]any) {
			receipt["image"].(map[string]any)["variant"] = "wayland"
		},
		"missing crossover target liveness": func(receipt map[string]any) {
			delete(receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string), "target_liveness_view")
		},
		"wrong crossover view errno": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["view_read_errno"] = "EACCES"
		},
		"wrong crossover view outcome": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["view_read_outcome"] = "protocol_error"
		},
		"wrong crossover control outcome": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["control_inject_outcome"] = "protocol_error"
		},
		"wrong crossover control errno": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["control_inject_errno"] = "EACCES"
		},
		"wrong crossover abstract visibility": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["abstract_socket_visible"] = "true"
		},
		"wrong crossover abstract outcome": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["abstract_socket_outcome"] = "connected"
		},
		"wrong crossover abstract errno": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["abstract_socket_errno"] = "ECONNREFUSED"
		},
		"wrong crossover display class": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["derived_display_class"] = "x_auth"
		},
		"wrong crossover display outcome": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["derived_display_outcome"] = "auth_refused"
		},
		"wrong crossover egress outcome": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["egress_address_outcome"] = "connected"
		},
		"wrong crossover egress errno": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["egress_address_errno"] = "EHOSTUNREACH"
		},
		"wrong crossover target control liveness": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["target_liveness_control"] = "refused"
		},
		"wrong crossover target X liveness": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["target_liveness_x"] = "refused"
		},
		"wrong crossover target egress liveness": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["target_liveness_egress"] = "refused"
		},
		"wrong network helper body": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.network_egress"].(map[string]any)["evidence"].(map[string]string)["helper_http_body"] = "wrong"
		},
		"missing network veth address": func(receipt map[string]any) {
			delete(receipt["rows"].(map[string]any)["linux.network_egress"].(map[string]any)["evidence"].(map[string]string), "veth_address")
		},
		"missing network veth gateway": func(receipt map[string]any) {
			delete(receipt["rows"].(map[string]any)["linux.network_egress"].(map[string]any)["evidence"].(map[string]string), "veth_gateway")
		},
		"missing crossover source computer": func(receipt map[string]any) {
			delete(receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string), "source_computer_id")
		},
		"missing crossover source attempt": func(receipt map[string]any) {
			delete(receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string), "source_attempt_id")
		},
		"missing crossover target computer": func(receipt map[string]any) {
			delete(receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string), "target_computer_id")
		},
		"missing crossover target attempt": func(receipt map[string]any) {
			delete(receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string), "target_attempt_id")
		},
		"crossover targets source computer": func(receipt map[string]any) {
			evidence := receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)
			evidence["target_computer_id"] = evidence["source_computer_id"]
		},
		"crossover targets source attempt": func(receipt map[string]any) {
			evidence := receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)
			evidence["target_attempt_id"] = evidence["source_attempt_id"]
		},
		"missing crossover target address": func(receipt map[string]any) {
			delete(receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string), "target_egress_address")
		},
		"missing crossover target view port": func(receipt map[string]any) {
			delete(receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string), "target_view_port")
		},
		"missing crossover target control port": func(receipt map[string]any) {
			delete(receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string), "target_control_port")
		},
		"missing crossover target egress port": func(receipt map[string]any) {
			delete(receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string), "target_egress_port")
		},
		"crossover view does not target recorded computer": func(receipt map[string]any) {
			receipt["rows"].(map[string]any)["linux.screen_crossover_refused"].(map[string]any)["evidence"].(map[string]string)["view_read_address"] = "198.18.0.10:42002"
		},
	} {
		t.Run("reject/"+name, func(t *testing.T) {
			receipt := conformantLinuxComputerReceipt(candidate, "xfce")
			mutate(receipt)
			if err := runGate(t, receipt, "xfce", ""); err == nil {
				t.Fatal("receipt gate accepted invalid evidence")
			}
		})
	}
}

type receiptGateError struct {
	err    error
	output string
}

func (err *receiptGateError) Error() string {
	return err.err.Error() + ": " + err.output
}

func conformantLinuxComputerReceipt(candidate, variant string) map[string]any {
	rows := make(map[string]any, len(linuxComputerReceiptRows))
	for _, id := range linuxComputerReceiptRows {
		rows[id] = map[string]any{
			"status":       "PASS",
			"started_at":   "2026-08-29T00:00:00Z",
			"completed_at": "2026-08-29T00:01:00Z",
			"assertions":   map[string]bool{"live_product_path": true},
			"evidence":     map[string]string{},
		}
	}
	guest := rows["linux.guest_authority"].(map[string]any)
	guest["status"] = "NOT-RUN"
	guest["not_run_issue"] = 157
	guest["not_run_reason"] = "complete M3 OCI matrix root result is not published"
	guest["evidence"] = map[string]string{"blocked_assertion": "candidate-bound root Run route"}
	storage := rows["linux.storage_provenance"].(map[string]any)
	storage["evidence"] = map[string]string{"restore_token_revocation_receipt": "operation_revision=9 revoke_all=true computer_id=computer-1"}
	crossover := rows["linux.screen_crossover_refused"].(map[string]any)
	crossover["assertions"] = map[string]bool{"crossover_refused": true}
	crossoverEvidence := map[string]string{
		"source_computer_id": "computer-1", "source_attempt_id": "attempt-1",
		"target_computer_id": "computer-2", "target_attempt_id": "attempt-2",
		"target_view_port": "42002", "target_control_port": "42003", "target_egress_address": "198.18.0.6", "target_veth_gateway": "198.18.0.5", "target_egress_port": "43999",
		"view_read_outcome": "refused", "view_read_errno": "ECONNREFUSED",
		"view_read_address":      "198.18.0.6:42002",
		"control_inject_outcome": "refused", "control_inject_errno": "ECONNREFUSED",
		"control_inject_address":  "198.18.0.6:42003",
		"abstract_socket_outcome": "refused", "abstract_socket_errno": "ENOENT",
		"abstract_socket_visible": "false", "derived_display_outcome": "transport_refused", "derived_display_class": "x_transport",
		"target_liveness_view": "read_succeeded", "target_liveness_control": "inject_succeeded", "target_liveness_x": "read_succeeded", "target_liveness_egress": "connected",
		"egress_address_outcome": "refused", "egress_address_errno": "ECONNREFUSED",
		"egress_address_target":      "198.18.0.6:43999",
		"node_listener_ipv6_address": "[fe80::1%eth0]:45000", "node_listener_ipv6_outcome": "refused", "node_listener_ipv6_errno": "ENETUNREACH",
	}
	crossover["assertions"] = map[string]bool{"crossover_refused": true, "target_alive_at_refusal_edge": true}
	if variant == "wayland" {
		crossoverEvidence["abstract_socket_outcome"] = "not_applicable"
		crossoverEvidence["derived_display_outcome"] = "not_applicable"
	}
	crossover["evidence"] = crossoverEvidence
	network := rows["linux.network_egress"].(map[string]any)
	network["assertions"] = map[string]bool{"private_veth_address_present": true, "resolver_reachable": true, "helper_http_through_veth": true, "node_listener_ipv4_refused": true, "node_listener_ipv6_refused": true}
	network["evidence"] = map[string]string{
		"computer_id": "computer-1", "attempt_id": "attempt-1", "veth_address": "198.18.0.2", "veth_gateway": "198.18.0.1",
		"resolved_name": "example.com", "resolved_address": "192.0.2.1", "helper_http_status": "200",
		"helper_http_body": "wefty-computer-egress-v1", "node_listener_ipv4_address": "198.18.0.1:45000", "node_listener_ipv4_outcome": "refused", "node_listener_ipv4_errno": "ECONNREFUSED",
		"node_listener_ipv6_address": "[fe80::1%eth0]:45000", "node_listener_ipv6_outcome": "refused", "node_listener_ipv6_errno": "ENETUNREACH",
	}
	rows["linux.removal"].(map[string]any)["evidence"] = map[string]string{"inventory_source": "helper VerifyNamespaceReadOnly route"}
	digest := "sha256:" + strings.Repeat("a", 64)
	return map[string]any{
		"version":       5,
		"status":        "NOT-RUN",
		"not_run_issue": 157,
		"candidate_sha": candidate,
		"platform":      "linux/amd64",
		"image": map[string]any{
			"variant":         variant,
			"index_digest":    digest,
			"platform_digest": digest,
		},
		"fabric_identities": []map[string]string{
			{"role": "administrator", "fabric_id": "fabric-admin", "user_id": "admin", "device_id": "admin-device"},
			{"role": "viewer", "fabric_id": "fabric-viewer", "user_id": "viewer", "device_id": "viewer-device"},
		},
		"authority_generations": []int64{1},
		"computer_ids":          []string{"computer-1", "computer-2", "computer-3", "computer-4"},
		"job_ids":               []string{"job-1", "job-2", "job-3", "job-4"},
		"attempt_ids":           []string{"attempt-1", "attempt-2", "attempt-3", "attempt-4"},
		"storage_ids":           []string{"storage-1", "storage-2", "storage-3", "storage-4"},
		"resource_caps": map[string]any{
			"memory_bytes": 1073741824, "disk_bytes": 134217728, "backup_cap": 4, "submit_max_inflight": 20,
		},
		"timings":    map[string]string{"l1_lease": "30s", "l1_node_dead": "2m", "l3_reconcile": "production-default"},
		"deviations": []map[string]string{{"id": "dev.plain_fabric_identity", "status": "DEVIATION"}},
		"rows":       rows,
		"residue_inventories": map[string]any{
			"post_removal_helper_namespace": map[string]any{},
		},
	}
}
