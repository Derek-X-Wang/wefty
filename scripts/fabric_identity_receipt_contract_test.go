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
		for _, required := range []string{"TSNET_SMOKE_REQUIRED", "needs.resolve-published-artifact.outputs.available == 'true'"} {
			if !strings.Contains(machine.If, required) || !strings.Contains(person.If, required) {
				t.Fatalf("%s Fabric job guards do not contain %q: machine=%q person=%q", name, required, machine.If, person.If)
			}
		}
		if !strings.Contains(person.If, "TSNET_CI_TESTER_REQUIRED") {
			t.Fatalf("%s person job is not guarded by TSNET_CI_TESTER_REQUIRED: %q", name, person.If)
		}
		result := fixture.workflow.Jobs["realtiming-result"]
		needs := workflowNeeds(t, result.Needs)
		for _, required := range []string{"fabric-machine-identity", "fabric-person-identity"} {
			if !slices.Contains(needs, required) {
				t.Fatalf("%s result gate does not need %s: %#v", name, required, needs)
			}
		}
		resultText := marshalJob(t, result)
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
	if !strings.Contains(realtiming.Jobs["fabric-machine-identity"].If, "github.event_name == 'workflow_run'") ||
		!strings.Contains(realtiming.Jobs["fabric-person-identity"].If, "github.event_name == 'workflow_run'") {
		t.Fatal("pull_request jobs must be structurally unable to evaluate repository secrets")
	}
	for _, forbidden := range []string{"secrets.TS_AUTHKEY", "secrets.TS_AUTHKEY_CI_TESTER"} {
		if strings.Contains(marshalJob(t, realtiming.Jobs["build-pr-artifacts"]), forbidden) ||
			strings.Contains(marshalJob(t, realtiming.Jobs["service-acceptance-realtiming"]), forbidden) {
			t.Fatalf("PR-capable jobs unexpectedly consume %s", forbidden)
		}
	}
	assertFileContains(t, "check-fabric-boundary.sh", "MagicDNS", `\.ts\.net`, "svc:")
}

func TestFabricIdentityReceiptSkipIsNeverSuccess(t *testing.T) {
	candidate := strings.Repeat("a", 40)
	directory := t.TempDir()
	emptyReceipt := filepath.Join(directory, "absent.json")
	valid := filepath.Join(directory, "valid.json")
	assembleFabricIdentityReceipt(t, valid, candidate, "trusted", "success", "skipped", "true", "false", machineFabricReceipt(t, directory, candidate), emptyReceipt, "")
	checkFabricIdentityReceipt(t, valid, candidate, "trusted", "success", "skipped", "true", "false", true)

	mutated := filepath.Join(directory, "mutated.json")
	assembleFabricIdentityReceipt(t, mutated, candidate, "trusted", "success", "skipped", "true", "false", machineFabricReceipt(t, directory, candidate), emptyReceipt, "skipped_person_as_success")
	checkFabricIdentityReceipt(t, mutated, candidate, "trusted", "success", "skipped", "true", "false", false)
}

func TestFabricIdentityReceiptFullyArmedAndPullRequest(t *testing.T) {
	candidate := strings.Repeat("b", 40)
	directory := t.TempDir()
	machine := machineFabricReceipt(t, directory, candidate)
	person := personFabricReceipt(t, directory, candidate)

	fullyArmed := filepath.Join(directory, "fully-armed.json")
	assembleFabricIdentityReceipt(t, fullyArmed, candidate, "trusted", "success", "success", "true", "true", machine, person, "")
	checkFabricIdentityReceipt(t, fullyArmed, candidate, "trusted", "success", "success", "true", "true", true)

	pullRequest := filepath.Join(directory, "pull-request.json")
	assembleFabricIdentityReceipt(t, pullRequest, candidate, "pull_request", "skipped", "skipped", "true", "true", filepath.Join(directory, "missing-machine"), filepath.Join(directory, "missing-person"), "")
	checkFabricIdentityReceipt(t, pullRequest, candidate, "pull_request", "skipped", "skipped", "true", "true", true)
}

func machineFabricReceipt(t *testing.T, directory, candidate string) string {
	t.Helper()
	return writeFabricFixture(t, directory, "machine.json", map[string]any{
		"version": 1, "candidate_sha": candidate,
		"rows": map[string]any{
			"fabric.machine_dns_acl":                  fabricPassFixture(map[string]string{"listener_connect_host": "server.fabric.invalid", "peer_connect_host": "peer.fabric.invalid"}, map[string]bool{"dns_resolved": true, "acl_dial_succeeded": true, "machine_identity_authenticated": true}),
			"fabric.machine_second_peer_reachability": fabricPassFixture(map[string]string{"listener_connect_host": "server.fabric.invalid", "peer_connect_host": "peer.fabric.invalid"}, map[string]bool{"distinct_peer": true, "echo_round_trip": true}),
		},
	})
}

func personFabricReceipt(t *testing.T, directory, candidate string) string {
	t.Helper()
	return writeFabricFixture(t, directory, "person.json", map[string]any{
		"version": 1, "candidate_sha": candidate,
		"rows": map[string]any{
			"fabric.person_whoami": fabricPassFixture(map[string]string{"listener_connect_host": "server.fabric.invalid", "peer_connect_host": "person.fabric.invalid", "fabric_id": "fabric-1", "user_id": "user-1", "device_id": "device-1"}, map[string]bool{"whoami_authenticated": true, "person_identity_complete": true}),
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

func assembleFabricIdentityReceipt(t *testing.T, output, candidate, domain, machineResult, personResult, machineArmed, personArmed, machineReceipt, personReceipt, mutation string) {
	t.Helper()
	command := exec.Command("sh", "assemble-fabric-identity-receipt.sh", output, candidate, domain, machineResult, personResult, machineArmed, personArmed, machineReceipt, personReceipt, mutation)
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
