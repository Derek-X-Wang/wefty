package scripts

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestFabricIdentityWorkflowContract(t *testing.T) {
	realtiming, realtimingBytes := readWorkflow(t, "../.github/workflows/service-acceptance-realtiming.yml")
	scheduled, scheduledBytes := readWorkflow(t, "../.github/workflows/service-acceptance-realtiming-scheduled.yml")
	if _, ok := realtiming.On["pull_request_target"]; ok {
		t.Fatal("Fabric identity workflows must never use pull_request_target")
	}
	for name, fixture := range map[string]struct {
		workflow workflowContract
		text     string
	}{
		"main":      {workflow: realtiming, text: string(realtimingBytes)},
		"scheduled": {workflow: scheduled, text: string(scheduledBytes)},
	} {
		machine, machineOK := fixture.workflow.Jobs["fabric-machine-identity"]
		person, personOK := fixture.workflow.Jobs["fabric-person-identity"]
		if !machineOK || !personOK {
			t.Fatalf("%s workflow is missing Fabric identity jobs", name)
		}
		trustGuard := "github.event_name == 'workflow_run' && github.event.workflow_run.event == 'push' && github.event.workflow_run.head_branch == 'main'"
		if name == "scheduled" {
			trustGuard = "github.event_name == 'schedule' && github.ref == 'refs/heads/main'"
		}
		machineGuard := "${{ !cancelled() && " + trustGuard + " && needs.resolve-published-artifact.outputs.available == 'true' && vars.TSNET_SMOKE_REQUIRED == 'true' }}"
		personGuard := strings.TrimSuffix(machineGuard, " }}") + " && vars.TSNET_CI_TESTER_REQUIRED == 'true' }}"
		if machine.If != machineGuard || person.If != personGuard {
			t.Fatalf("%s Fabric credential guards changed: machine=%q, want %q; person=%q, want %q", name, machine.If, machineGuard, person.If, personGuard)
		}
		for jobName, job := range map[string]workflowJob{"machine": machine, "person": person} {
			goTimeout := workflowGoTestTimeoutMinutes(t, name+" "+jobName, marshalJob(t, job), "tsnet_smoke")
			if job.TimeoutMinutes != 10 || goTimeout != 5 || job.TimeoutMinutes < goTimeout+5 {
				t.Fatalf("%s %s Fabric timeouts: job=%dm go=%dm, want job=10m and go=5m", name, jobName, job.TimeoutMinutes, goTimeout)
			}
		}
		result := fixture.workflow.Jobs["realtiming-result"]
		needs := workflowNeeds(t, result.Needs)
		for _, required := range []string{"fabric-machine-identity", "fabric-person-identity"} {
			if !slices.Contains(needs, required) {
				t.Fatalf("%s result gate does not need %s: %#v", name, required, needs)
			}
		}
		resultText := marshalJob(t, result)
		assembleGuarded := false
		for _, step := range result.Steps {
			if strings.Contains(step.Run, "assemble-fabric-identity-receipt.sh") {
				assembleGuarded = step.If == "needs.resolve-published-artifact.outputs.available == 'true'"
			}
		}
		if !assembleGuarded {
			t.Fatalf("%s receipt assembler must be guarded by artifact availability", name)
		}
		for _, required := range []string{"assemble-fabric-identity-receipt.sh", "check-fabric-identity-receipt.sh", "MACHINE_RESULT", "PERSON_RESULT", "MACHINE_ARMED", "PERSON_ARMED", "fabric-identity-receipt.json"} {
			if !strings.Contains(resultText, required) {
				t.Fatalf("%s result gate is missing %q", name, required)
			}
		}
		for _, required := range []string{"secrets.TS_AUTHKEY", "secrets.TS_AUTHKEY_CI_TESTER", "go test -count=1 -timeout=5m -v -tags=tsnet_smoke ./fabric/tsnet"} {
			if !strings.Contains(fixture.text, required) {
				t.Fatalf("%s workflow is missing %q", name, required)
			}
		}
	}
	for _, forbidden := range []string{"secrets.TS_AUTHKEY", "secrets.TS_AUTHKEY_CI_TESTER"} {
		if strings.Contains(marshalJob(t, realtiming.Jobs["build-pr-artifacts"]), forbidden) ||
			strings.Contains(marshalJob(t, realtiming.Jobs["service-acceptance-realtiming"]), forbidden) {
			t.Fatalf("PR-capable jobs unexpectedly consume %s", forbidden)
		}
	}
	assertFileContains(t, "check-fabric-boundary.sh", "MagicDNS", `\.ts\.net`, "svc:", ":(exclude)scripts/fabric_identity_receipt_contract_test.go")
	boundary, err := os.ReadFile("check-fabric-boundary.sh")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(boundary), ":(exclude)scripts/*_test.go") {
		t.Fatal("Fabric boundary guard excludes every scripts test instead of its one provider-shape fixture")
	}
}

func TestFabricIdentityReceiptSkipIsNeverSuccess(t *testing.T) {
	candidate := strings.Repeat("a", 40)
	directory := t.TempDir()
	emptyReceipt := filepath.Join(directory, "absent.json")
	machineReceipt := machineFabricReceipt(t, directory, candidate)
	for _, test := range []struct {
		name                        string
		machineResult, personResult string
		machineArmed, personArmed   string
		wantSuccess                 bool
	}{
		{name: "reality both credentials unarmed", machineResult: "skipped", personResult: "skipped", machineArmed: "false", personArmed: "false", wantSuccess: true},
		{name: "armed machine skipped", machineResult: "skipped", personResult: "skipped", machineArmed: "true", personArmed: "false", wantSuccess: false},
		{name: "machine ran person unarmed", machineResult: "success", personResult: "skipped", machineArmed: "true", personArmed: "false", wantSuccess: true},
		{name: "machine failed", machineResult: "failure", personResult: "skipped", machineArmed: "true", personArmed: "false", wantSuccess: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			assembled := filepath.Join(directory, strings.ReplaceAll(test.name, " ", "-")+".json")
			assembleFabricIdentityReceipt(t, assembled, candidate, "trusted", test.machineResult, test.personResult, test.machineArmed, test.personArmed, machineReceipt, emptyReceipt)
			if test.machineResult == "failure" || (test.machineResult == "skipped" && test.machineArmed == "true") {
				assertFabricDeviationRetained(t, assembled, "fabric.machine_dns_acl", "fabric.machine_second_peer_reachability")
			}
			checkFabricIdentityReceipt(t, assembled, candidate, "trusted", test.machineResult, test.personResult, test.machineArmed, test.personArmed, test.wantSuccess)
		})
	}

	valid := filepath.Join(directory, "valid-unarmed-person.json")
	assembleFabricIdentityReceipt(t, valid, candidate, "trusted", "success", "skipped", "true", "false", machineReceipt, emptyReceipt)
	mutated := filepath.Join(directory, "mutated.json")
	mutateFabricReceipt(t, valid, mutated, func(root map[string]any) {
		rows := root["rows"].(map[string]any)
		rows["fabric.person_whoami"] = fabricPassFixture(
			map[string]string{"listener_connect_host": "node-1111111111111111", "peer_connect_host": "node-3333333333333333"},
			map[string]bool{"person_identity_complete": true, "whoami_authenticated": true},
		)
	})
	checkFabricIdentityReceipt(t, mutated, candidate, "trusted", "success", "skipped", "true", "false", false)
}

func TestFabricIdentityReceiptFullyArmedAndPullRequest(t *testing.T) {
	candidate := strings.Repeat("b", 40)
	directory := t.TempDir()
	machine := machineFabricReceipt(t, directory, candidate)
	person := personFabricReceipt(t, directory, candidate)

	fullyArmed := filepath.Join(directory, "fully-armed.json")
	assembleFabricIdentityReceipt(t, fullyArmed, candidate, "trusted", "success", "success", "true", "true", machine, person)
	checkFabricIdentityReceipt(t, fullyArmed, candidate, "trusted", "success", "success", "true", "true", true)

	pullRequest := filepath.Join(directory, "pull-request.json")
	assembleFabricIdentityReceipt(t, pullRequest, candidate, "pull_request", "skipped", "skipped", "true", "true", filepath.Join(directory, "missing-machine"), filepath.Join(directory, "missing-person"))
	checkFabricIdentityReceipt(t, pullRequest, candidate, "pull_request", "skipped", "skipped", "true", "true", true)

	manualDispatch := filepath.Join(directory, "manual-dispatch.json")
	assembleFabricIdentityReceipt(t, manualDispatch, candidate, "workflow_dispatch", "skipped", "skipped", "true", "true", filepath.Join(directory, "missing-machine"), filepath.Join(directory, "missing-person"))
	checkFabricIdentityReceipt(t, manualDispatch, candidate, "workflow_dispatch", "skipped", "skipped", "true", "true", true)
}

func TestFabricIdentityReceiptRejectsUnboundPassRows(t *testing.T) {
	candidate := strings.Repeat("c", 40)
	directory := t.TempDir()
	valid := filepath.Join(directory, "valid.json")
	assembleFabricIdentityReceipt(t, valid, candidate, "trusted", "success", "success", "true", "true", machineFabricReceipt(t, directory, candidate), personFabricReceipt(t, directory, candidate))

	for _, rowID := range []string{"fabric.machine_dns_acl", "fabric.machine_second_peer_reachability", "fabric.person_whoami"} {
		t.Run(rowID, func(t *testing.T) {
			mutated := filepath.Join(directory, strings.ReplaceAll(rowID, ".", "-")+".json")
			mutateFabricReceipt(t, valid, mutated, func(root map[string]any) {
				row := fabricReceiptRow(t, root, rowID)
				row["assertions"] = map[string]any{"anything": true}
			})
			checkFabricIdentityReceipt(t, mutated, candidate, "trusted", "success", "success", "true", "true", false)
		})
	}

	t.Run("missing ConnectHost evidence", func(t *testing.T) {
		mutated := filepath.Join(directory, "missing-connect-host.json")
		mutateFabricReceipt(t, valid, mutated, func(root map[string]any) {
			fabricReceiptRow(t, root, "fabric.machine_second_peer_reachability")["evidence"] = map[string]any{}
		})
		checkFabricIdentityReceipt(t, mutated, candidate, "trusted", "success", "success", "true", "true", false)
	})

	t.Run("person identifiers are not public evidence", func(t *testing.T) {
		mutated := filepath.Join(directory, "person-user-id.json")
		mutateFabricReceipt(t, valid, mutated, func(root map[string]any) {
			evidence := fabricReceiptRow(t, root, "fabric.person_whoami")["evidence"].(map[string]any)
			evidence["user_id"] = "private-tailnet-user"
		})
		checkFabricIdentityReceipt(t, mutated, candidate, "trusted", "success", "success", "true", "true", false)
	})
}

func TestFabricIdentityReceiptRejectsProviderSpecificOrInvalidConnectHosts(t *testing.T) {
	candidate := strings.Repeat("d", 40)
	directory := t.TempDir()
	valid := filepath.Join(directory, "valid.json")
	assembleFabricIdentityReceipt(t, valid, candidate, "trusted", "success", "success", "true", "true", machineFabricReceipt(t, directory, candidate), personFabricReceipt(t, directory, candidate))

	for _, value := range []string{"node.example.ts.net", "svc:node", "MagicDNS", "Tailscale", "not a hostname"} {
		t.Run(value, func(t *testing.T) {
			mutated := filepath.Join(directory, strings.NewReplacer(".", "-", ":", "-", " ", "-").Replace(value)+".json")
			mutateFabricReceipt(t, valid, mutated, func(root map[string]any) {
				evidence := fabricReceiptRow(t, root, "fabric.machine_dns_acl")["evidence"].(map[string]any)
				evidence["listener_connect_host"] = value
			})
			checkFabricIdentityReceipt(t, mutated, candidate, "trusted", "success", "success", "true", "true", false)
		})
	}

	t.Run("non-string ConnectHost", func(t *testing.T) {
		mutated := filepath.Join(directory, "non-string-connect-host.json")
		mutateFabricReceipt(t, valid, mutated, func(root map[string]any) {
			evidence := fabricReceiptRow(t, root, "fabric.machine_dns_acl")["evidence"].(map[string]any)
			evidence["listener_connect_host"] = 42
		})
		checkFabricIdentityReceipt(t, mutated, candidate, "trusted", "success", "success", "true", "true", false)
	})
}

func machineFabricReceipt(t *testing.T, directory, candidate string) string {
	t.Helper()
	return writeFabricFixture(t, directory, "machine.json", map[string]any{
		"version": 1, "candidate_sha": candidate,
		"rows": map[string]any{
			"fabric.machine_dns_acl":                  fabricPassFixture(map[string]string{"listener_connect_host": "node-1111111111111111", "peer_connect_host": "node-2222222222222222"}, map[string]bool{"dns_resolved": true, "shared_tagged_key_dial_succeeded": true, "machine_identity_authenticated": true}),
			"fabric.machine_second_peer_reachability": fabricPassFixture(map[string]string{"listener_connect_host": "node-1111111111111111", "peer_connect_host": "node-2222222222222222"}, map[string]bool{"peer_address_distinct": true, "echo_round_trip": true}),
		},
	})
}

func personFabricReceipt(t *testing.T, directory, candidate string) string {
	t.Helper()
	return writeFabricFixture(t, directory, "person.json", map[string]any{
		"version": 1, "candidate_sha": candidate,
		"rows": map[string]any{
			"fabric.person_whoami": fabricPassFixture(map[string]string{"listener_connect_host": "node-1111111111111111", "peer_connect_host": "node-3333333333333333"}, map[string]bool{"whoami_authenticated": true, "person_identity_complete": true}),
		},
	})
}

func fabricPassFixture(evidence map[string]string, assertions map[string]bool) map[string]any {
	return map[string]any{"status": "PASS", "assertions": assertions, "evidence": evidence, "deviations": []any{}}
}

func writeFabricFixture(t *testing.T, directory, name string, value any) string {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func mutateFabricReceipt(t *testing.T, source, output string, mutate func(map[string]any)) {
	t.Helper()
	payload, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := json.Unmarshal(payload, &receipt); err != nil {
		t.Fatal(err)
	}
	mutate(receipt)
	writeFabricFixture(t, filepath.Dir(output), filepath.Base(output), receipt)
}

func fabricReceiptRow(t *testing.T, root map[string]any, rowID string) map[string]any {
	t.Helper()
	rows, ok := root["rows"].(map[string]any)
	if !ok {
		t.Fatalf("receipt rows have type %T", root["rows"])
	}
	row, ok := rows[rowID].(map[string]any)
	if !ok {
		t.Fatalf("receipt row %s has type %T", rowID, rows[rowID])
	}
	return row
}

func assertFabricDeviationRetained(t *testing.T, receiptPath string, rowIDs ...string) {
	t.Helper()
	payload, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := json.Unmarshal(payload, &receipt); err != nil {
		t.Fatal(err)
	}
	for _, rowID := range rowIDs {
		row := fabricReceiptRow(t, receipt, rowID)
		deviations, ok := row["deviations"].([]any)
		if !ok || len(deviations) != 1 {
			t.Fatalf("%s deviations = %#v, want retained dev.plain_fabric_identity", rowID, row["deviations"])
		}
		deviation, ok := deviations[0].(map[string]any)
		if !ok || deviation["id"] != "dev.plain_fabric_identity" || deviation["status"] != "DEVIATION" {
			t.Fatalf("%s deviation = %#v, want dev.plain_fabric_identity", rowID, deviations[0])
		}
	}
}

func assembleFabricIdentityReceipt(t *testing.T, output, candidate, domain, machineResult, personResult, machineArmed, personArmed, machineReceipt, personReceipt string) {
	t.Helper()
	command := exec.Command("sh", "assemble-fabric-identity-receipt.sh", output, candidate, domain, machineResult, personResult, machineArmed, personArmed, machineReceipt, personReceipt)
	if payload, err := command.CombinedOutput(); err != nil {
		t.Fatalf("assemble Fabric identity receipt: %v\n%s", err, payload)
	}
}

func checkFabricIdentityReceipt(t *testing.T, receipt, candidate, domain, machineResult, personResult, machineArmed, personArmed string, wantSuccess bool) {
	t.Helper()
	command := exec.Command("sh", "check-fabric-identity-receipt.sh", receipt, candidate, domain, machineResult, personResult, machineArmed, personArmed)
	payload, err := command.CombinedOutput()
	if (err == nil) != wantSuccess {
		t.Fatalf("check Fabric identity receipt success=%t, want %t: %v\n%s", err == nil, wantSuccess, err, payload)
	}
}
