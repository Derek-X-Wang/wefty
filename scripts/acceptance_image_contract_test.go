package scripts

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type workflowContract struct {
	On          map[string]any         `yaml:"on"`
	Permissions map[string]string      `yaml:"permissions"`
	Jobs        map[string]workflowJob `yaml:"jobs"`
}

type workflowJob struct {
	If          string            `yaml:"if"`
	Needs       any               `yaml:"needs"`
	Permissions map[string]string `yaml:"permissions"`
	Uses        string            `yaml:"uses"`
	Steps       []workflowStep    `yaml:"steps"`
}

type workflowStep struct {
	Uses string         `yaml:"uses"`
	Run  string         `yaml:"run"`
	With map[string]any `yaml:"with"`
}

func TestAcceptanceImageWorkflowContract(t *testing.T) {
	image, imageBytes := readWorkflow(t, "../.github/workflows/acceptance-image.yml")
	build, buildBytes := readWorkflow(t, "../.github/workflows/acceptance-image-build.yml")
	gate, gateBytes := readWorkflow(t, "../.github/workflows/contract-gate.yml")
	realtiming, realtimingBytes := readWorkflow(t, "../.github/workflows/service-acceptance-realtiming.yml")
	scheduled, scheduledBytes := readWorkflow(t, "../.github/workflows/service-acceptance-realtiming-scheduled.yml")

	if _, ok := build.On["workflow_call"]; !ok {
		t.Fatal("acceptance-image must expose the secretless required workflow_call lane")
	}
	if _, ok := image.On["push"]; !ok {
		t.Fatal("acceptance-image must publish from push to main")
	}
	for _, workflow := range []workflowContract{image, build, gate, realtiming, scheduled} {
		if _, ok := workflow.On["pull_request_target"]; ok {
			t.Fatal("acceptance workflows must never use pull_request_target")
		}
	}
	if image.Permissions["packages"] != "" {
		t.Fatal("acceptance-image top-level permissions must not grant packages write")
	}
	publish := image.Jobs["publish"]
	if publish.Permissions["packages"] != "write" || !strings.Contains(publish.If, "github.event_name == 'push'") || !strings.Contains(publish.If, "refs/heads/main") {
		t.Fatalf("publisher permissions/guard = %+v if=%q", publish.Permissions, publish.If)
	}
	computerPublish := image.Jobs["publish-reference-computer"]
	if computerPublish.Permissions["packages"] != "write" || !strings.Contains(computerPublish.If, "github.event_name == 'push'") || !strings.Contains(computerPublish.If, "refs/heads/main") {
		t.Fatalf("Computer publisher permissions/guard = %+v if=%q", computerPublish.Permissions, computerPublish.If)
	}
	for name, job := range map[string]workflowJob{"publish": publish, "publish-reference-computer": computerPublish} {
		text := marshalJob(t, job)
		for _, required := range []string{"crane\" push", "crane\" index append", ".oci.tar"} {
			if !strings.Contains(text, required) {
				t.Fatalf("%s does not promote executed OCI archives with %q", name, required)
			}
		}
		if strings.Contains(text, "docker buildx") || strings.Contains(text, "docker/build-push-action") {
			t.Fatalf("%s re-solves image bytes instead of promoting the executed archive", name)
		}
	}
	for name, job := range image.Jobs {
		if name != "publish" && name != "publish-reference-computer" && job.Permissions["packages"] != "" {
			t.Fatalf("PR-callable job %s grants package permissions", name)
		}
	}
	for name, job := range build.Jobs {
		if job.Permissions["packages"] != "" {
			t.Fatalf("PR-callable job %s grants package permissions", name)
		}
	}
	pinnedAction := regexp.MustCompile(`^[^./][^@]*@[0-9a-f]{40}$`)
	for name, job := range image.Jobs {
		for _, step := range job.Steps {
			if step.Uses != "" && !pinnedAction.MatchString(step.Uses) {
				t.Fatalf("packages-write workflow job %s has mutable action ref %q", name, step.Uses)
			}
		}
	}
	for name, job := range build.Jobs {
		for _, step := range job.Steps {
			if step.Uses != "" && !pinnedAction.MatchString(step.Uses) {
				t.Fatalf("PR image workflow job %s has mutable action ref %q", name, step.Uses)
			}
		}
	}
	if strings.Contains(string(imageBytes), "secrets.") {
		t.Fatal("acceptance-image must not use repository secrets")
	}
	if strings.Contains(string(buildBytes), "secrets.") || strings.Contains(string(buildBytes), "github.token") {
		t.Fatal("PR-callable image build must not consume github.token")
	}
	for _, required := range []string{"Dry-run digest-only release manifest assembly", "scripts/build-oci-install-manifest.sh", "--probe-reference \"$IMAGE_NAME\"", "synthetic_digest=", ".probe_reference == $reference", ".probe_digest == $digest"} {
		if !strings.Contains(string(buildBytes), required) {
			t.Fatalf("PR-callable image build does not exercise release manifest assembly %q", required)
		}
	}
	for _, required := range []string{"reference-computer-platform-build", "examples/computer/Dockerfile", "scripts/test-computer-image-runtime.sh", "wefty-computer-reference-platform-", "usr/lib/chromium/chromium"} {
		if !strings.Contains(string(buildBytes), required) {
			t.Fatalf("PR-callable reference Computer build is missing %q", required)
		}
	}

	called := gate.Jobs["acceptance-image"]
	if called.Uses != "./.github/workflows/acceptance-image-build.yml" {
		t.Fatalf("contract-gate image lane uses %q", called.Uses)
	}
	needs := stringSlice(t, gate.Jobs["all-tests-pass"].Needs)
	if !slices.Contains(needs, "acceptance-image") || !strings.Contains(string(gateBytes), "ACCEPTANCE_IMAGE_RESULT") {
		t.Fatal("all-tests-pass does not fail closed on acceptance-image")
	}

	if _, ok := realtiming.On["workflow_run"]; !ok {
		t.Fatal("realtiming must be causally downstream of acceptance-image")
	}
	if _, ok := scheduled.On["schedule"]; !ok {
		t.Fatal("scheduled realtiming must retain its evidence cadence")
	}
	if _, ok := scheduled.On["workflow_dispatch"]; !ok {
		t.Fatal("scheduled realtiming must remain manually dispatchable")
	}
	realtimeText := string(realtimingBytes)
	for _, required := range []string{"workflow_run.head_sha", "workflow_run.id", "actions/download-artifact@", "run-id:", "acceptance-image-index-digest.txt", "$ECHO_REFERENCE@$ECHO_DIGEST", "wefty-computer-reference-", "WEFTY_OCI_COMPUTER_ARCHIVE"} {
		if !strings.Contains(realtimeText, required) {
			t.Fatalf("realtiming is missing immutable artifact contract %q", required)
		}
	}
	for _, forbidden := range []string{"$IMAGE_NAME:$CANDIDATE_SHA", "crane pull"} {
		if strings.Contains(realtimeText, forbidden) {
			t.Fatalf("realtiming reintroduced mutable consumption %q", forbidden)
		}
	}
	for _, required := range []string{"if: github.event_name == 'workflow_run'", "ref: ${{ github.event.workflow_run.head_sha }}", "PUBLISHED_SHA"} {
		if !strings.Contains(realtimeText, required) {
			t.Fatalf("workflow-run realtiming checkout is missing %q", required)
		}
	}
	if strings.Contains(realtimeText, "ref: main") {
		t.Fatal("workflow-run realtiming must check out the triggering main SHA directly")
	}
	scheduledText := string(scheduledBytes)
	for _, required := range []string{"ref: main", "typed-skip: no successful acceptance-image publication exists", "acceptance-image-index-digest.txt", "$ECHO_REFERENCE@$ECHO_DIGEST", "wefty-computer-reference-", "WEFTY_OCI_COMPUTER_ARCHIVE"} {
		if !strings.Contains(scheduledText, required) {
			t.Fatalf("scheduled realtiming is missing %q", required)
		}
	}
	if strings.Contains(scheduledText, "ref: ${{ github.event.workflow_run.head_sha }}") {
		t.Fatal("scheduled realtiming must not dynamically check out publication bytes")
	}
	for _, required := range []string{"$IMAGE_NAME@$index_digest", "--probe-reference \"$IMAGE_NAME\"", "bin/wefty", "wefty-acceptance-image-release.tar", "Public GHCR verification failed"} {
		if !strings.Contains(string(imageBytes), required) {
			t.Fatalf("publication contract is missing %q", required)
		}
	}
	for _, required := range []string{"publish-reference-computer", "$COMPUTER_IMAGE_NAME@$index_digest", "wefty-computer-reference.oci.tar", "wefty-computer-reference-release.tar", "computer-image-receipt.json", "reference-computer-platform-build"} {
		if !strings.Contains(string(imageBytes), required) {
			t.Fatalf("reference Computer publication contract is missing %q", required)
		}
	}
	assertFileContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", "exec /usr/local/bin/wefty-echo-service", "published-echo-service:", "clean-cache wefty node load-image")
	assertFileContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", "WEFTY_OCI_COMPUTER_REFERENCE", "exerciseNativeLinuxReferenceComputer", "ComputerStartupReadinessTimeout", "assertReferenceComputerWireNegatives")
	assertFileMatches(t, "../examples/oci-echo-service/Dockerfile", `(?m)^# syntax=.*@sha256:[0-9a-f]{64}$`, `(?m)^ARG GO_IMAGE=.*@sha256:[0-9a-f]{64}$`, `(?m)^ARG BUSYBOX_IMAGE=.*@sha256:[0-9a-f]{64}$`)
	assertFileMatches(t, "../examples/computer/Dockerfile", `(?m)^# syntax=.*@sha256:[0-9a-f]{64}$`, `(?m)^ARG DEBIAN_IMAGE=.*@sha256:[0-9a-f]{64}$`)
	assertFileContains(t, "../examples/computer/Dockerfile", "snapshot.debian.org/archive/debian/20260827T000000Z", "ARG SOURCE_DATE_EPOCH=0", "chromium=", "/var/log/dpkg.log", "/var/log/alternatives.log", "/var/log/apt/*")
	assertFileContains(t, "../examples/computer/entrypoint.sh", "WEFTY_COMPUTER_VIEW_PORT", "WEFTY_COMPUTER_CONTROL_PORT", "/wefty/service")
	assertFileContains(t, "../examples/computer/rfb-websocket.py", `target.path != "/websockify"`, `"binary" not in offered`, "BinaryOnlyWebSocket")
	assertFileContains(t, "../examples/computer/rfb-backend.py", `command.append("-viewonly")`, "socket.AF_UNIX")
	assertFileContains(t, "../examples/computer/watch-driver.py", "/wefty/control/driver.json", "os.replace", `type(value["version"]) is not int`, `type(value["human_driving"]) is not bool`)
	assertFileContains(t, "../examples/computer/oracle.html", `data-wefty-input-oracle="v1"`, "events=0 bytes=0 hash=00000000")
	assertFileContains(t, "../examples/computer/pointer-oracle.py", "XQueryPointer", "input-oracle.json", "os.replace")
	assertFileContains(t, "../scripts/test-computer-image-runtime.sh", "x11vnc-view.*-viewonly", "--mode pointer-event", "wrong_version_type", "wrong_human_driving_type", "rootfs_read_only", "restarted_edge_recovered")
	assertFileContains(t, "../docs/guides/computer-images.md", "Bring-your-own desktop is the product", "not a required base image", "CPU rendering", "--no-sandbox", "ticket #182", "ticket #207")
	assertFileContains(t, "../docs/guides/computer-images.md", "docker buildx create", "tonistiigi/binfmt@sha256:", "scripts/test-computer-image-runtime.sh", "scripts/probe-rfb-websocket.py", "operator-owned")
	assertFileContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", "RunComputerServiceRealtiming", "computer_reference_publication_loss_recovery=%t", "computer_reference_helper_stop_start_profile_sign_in_rootfs=%t")
	assertFileContains(t, "../docs/runbooks/oci-node.md", "wefty node load-image", "acceptance-image-index-digest.txt")
	assertFileContains(t, "../docs/acceptance/m3-lima-transport.md", "acceptance-image-index-digest.txt", "computer-image-index-digest.txt", "wefty-computer-reference.oci.tar", "atomically within 60 seconds")
}

func TestReferenceComputerReceiptFailsEveryNegativeMutation(t *testing.T) {
	_, imageBytes := readWorkflow(t, "../.github/workflows/acceptance-image.yml")
	match := regexp.MustCompile(`receipt_assertion='([^']+)'`).FindSubmatch(imageBytes)
	if len(match) != 2 {
		t.Fatal("publisher receipt_assertion was not found")
	}
	assertion := string(match[1]) + " and .elf_e_machine == 62"
	conformant := map[string]any{
		"digest": "sha256:good", "executed": true, "rfb_websocket_v1": true, "transport_assertions": true,
		"negative_rows":          map[string]any{"driver_fail_closed": true},
		"endpoints":              map[string]any{"view": "loopback", "control": "loopback"},
		"roles":                  map[string]any{"view_process_view_only": true, "control_process_interactive": true, "view_pointer_discarded": true},
		"driver_signal_consumed": true, "profile_persistent": true, "sign_in_persistent": true,
		"rootfs_read_only": true, "attempt_tmpfs_discarded": true,
		"shm":               map[string]any{"private": true, "conformant": true},
		"readiness_seconds": 1, "restarted_edge_recovered": true, "elf_e_machine": 62,
	}
	valid := func(value map[string]any) bool {
		payload, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command("jq", "-e", "--arg", "digest", "sha256:good", assertion)
		command.Stdin = bytes.NewReader(payload)
		return command.Run() == nil
	}
	if !valid(conformant) {
		t.Fatal("real publisher jq assertion rejected a conformant receipt")
	}
	mutations := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"wrong-digest", func(v map[string]any) { v["digest"] = "sha256:bad" }},
		{"not-executed", func(v map[string]any) { v["executed"] = false }},
		{"wrong-transport", func(v map[string]any) { v["rfb_websocket_v1"] = false }},
		{"transport-negative", func(v map[string]any) { v["transport_assertions"] = false }},
		{"driver-not-fail-closed", func(v map[string]any) { v["negative_rows"].(map[string]any)["driver_fail_closed"] = false }},
		{"view-wildcard", func(v map[string]any) { v["endpoints"].(map[string]any)["view"] = "wildcard" }},
		{"control-wildcard", func(v map[string]any) { v["endpoints"].(map[string]any)["control"] = "wildcard" }},
		{"view-process-interactive", func(v map[string]any) { v["roles"].(map[string]any)["view_process_view_only"] = false }},
		{"control-process-view-only", func(v map[string]any) { v["roles"].(map[string]any)["control_process_interactive"] = false }},
		{"view-pointer-accepted", func(v map[string]any) { v["roles"].(map[string]any)["view_pointer_discarded"] = false }},
		{"driver-unconsumed", func(v map[string]any) { v["driver_signal_consumed"] = false }},
		{"profile-lost", func(v map[string]any) { v["profile_persistent"] = false }},
		{"sign-in-lost", func(v map[string]any) { v["sign_in_persistent"] = false }},
		{"writable-rootfs", func(v map[string]any) { v["rootfs_read_only"] = false }},
		{"attempt-state-retained", func(v map[string]any) { v["attempt_tmpfs_discarded"] = false }},
		{"shared-shm", func(v map[string]any) { v["shm"].(map[string]any)["private"] = false }},
		{"bad-shm", func(v map[string]any) { v["shm"].(map[string]any)["conformant"] = false }},
		{"readiness-timeout", func(v map[string]any) { v["readiness_seconds"] = 60 }},
		{"edge-not-recovered", func(v map[string]any) { v["restarted_edge_recovered"] = false }},
		{"wrong-elf", func(v map[string]any) { v["elf_e_machine"] = 183 }},
	}
	if len(mutations) != 20 {
		t.Fatalf("negative mutation rows = %d, want 20", len(mutations))
	}
	failed := 0
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			payload, err := json.Marshal(conformant)
			if err != nil {
				t.Fatal(err)
			}
			var candidate map[string]any
			if err := json.Unmarshal(payload, &candidate); err != nil {
				t.Fatal(err)
			}
			mutation.mutate(candidate)
			if valid(candidate) {
				t.Fatalf("negative mutation %q passed", mutation.name)
			}
			failed++
		})
	}
	if failed != 20 {
		t.Fatalf("negative mutation coverage = %d/20", failed)
	}
}

func readWorkflow(t *testing.T, path string) (workflowContract, []byte) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var workflow workflowContract
	if err := yaml.Unmarshal(payload, &workflow); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return workflow, payload
}

func marshalJob(t *testing.T, job workflowJob) string {
	t.Helper()
	payload, err := yaml.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}

func stringSlice(t *testing.T, value any) []string {
	t.Helper()
	values, ok := value.([]any)
	if !ok {
		t.Fatalf("workflow needs has type %T", value)
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		text, ok := value.(string)
		if !ok {
			t.Fatalf("workflow need has type %T", value)
		}
		result = append(result, text)
	}
	return result
}

func assertFileContains(t *testing.T, path string, values ...string) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if !strings.Contains(string(payload), value) {
			t.Fatalf("%s is missing %q", path, value)
		}
	}
}

func assertFileMatches(t *testing.T, path string, patterns ...string) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, pattern := range patterns {
		if !regexp.MustCompile(pattern).Match(payload) {
			t.Fatalf("%s does not match %q", path, pattern)
		}
	}
}
