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
	"time"

	"github.com/Derek-X-Wang/wefty/internal/computerconformance"

	"gopkg.in/yaml.v3"
)

type workflowContract struct {
	On          map[string]any         `yaml:"on"`
	Permissions map[string]string      `yaml:"permissions"`
	Jobs        map[string]workflowJob `yaml:"jobs"`
}

type workflowJob struct {
	If             string            `yaml:"if"`
	Needs          any               `yaml:"needs"`
	Permissions    map[string]string `yaml:"permissions"`
	Uses           string            `yaml:"uses"`
	TimeoutMinutes int               `yaml:"timeout-minutes"`
	Steps          []workflowStep    `yaml:"steps"`
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
	waylandPublish := image.Jobs["publish-wayland-reference-computer"]
	if waylandPublish.Permissions["packages"] != "write" || !strings.Contains(waylandPublish.If, "github.event_name == 'push'") || !strings.Contains(waylandPublish.If, "refs/heads/main") {
		t.Fatalf("Wayland Computer publisher permissions/guard = %+v if=%q", waylandPublish.Permissions, waylandPublish.If)
	}
	for name, job := range map[string]workflowJob{"publish": publish, "publish-reference-computer": computerPublish, "publish-wayland-reference-computer": waylandPublish} {
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
		if name != "publish" && name != "publish-reference-computer" && name != "publish-wayland-reference-computer" && job.Permissions["packages"] != "" {
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
	for _, required := range []string{"reference-computer-platform-build", "examples/computer/Dockerfile", "examples/computer-wayland/Dockerfile", "scripts/test-computer-image-runtime.sh", "scripts/test-computer-wayland-furniture.sh", "scripts/measure-computer-image.sh", "wefty-computer-reference", "wefty-computer-wayland-reference", "usr/bin/wayvnc"} {
		if !strings.Contains(string(buildBytes), required) {
			t.Fatalf("PR-callable reference Computer build is missing %q", required)
		}
	}

	called := gate.Jobs["acceptance-image"]
	if called.Uses != "./.github/workflows/acceptance-image-build.yml" {
		t.Fatalf("contract-gate image lane uses %q", called.Uses)
	}
	if !bytes.Contains(gateBytes, []byte("GOOS=linux go vet -tags=service_acceptance_realtiming ./...")) {
		t.Fatal("contract-gate does not compile the Linux production-timing acceptance surface")
	}
	if !bytes.Contains(gateBytes, []byte("GOOS=darwin go vet -tags=service_acceptance_realtiming ./...")) {
		t.Fatal("contract-gate does not compile the Darwin production-timing acceptance surface")
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
	for _, required := range []string{"workflow_run.head_sha", "workflow_run.id", "actions/download-artifact@", "run-id:", "acceptance-image-index-digest.txt", "$ECHO_REFERENCE@$ECHO_DIGEST", "wefty-computer-reference-", "WEFTY_OCI_COMPUTER_ARCHIVE", "WEFTY_OCI_COMPUTER_RUNTIME_RECEIPT", "wefty-computer-wayland-reference-", "WEFTY_OCI_WAYLAND_COMPUTER_ARCHIVE"} {
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
	for _, required := range []string{"ref: refs/heads/main", "fetch-depth: 0", "git merge-base --is-ancestor", "git checkout --detach", "typed-skip: no successful acceptance-image publication exists", "acceptance-image-index-digest.txt", "$ECHO_REFERENCE@$ECHO_DIGEST", "wefty-computer-reference-", "WEFTY_OCI_COMPUTER_ARCHIVE", "WEFTY_OCI_COMPUTER_RUNTIME_RECEIPT", "wefty-computer-wayland-reference-", "WEFTY_OCI_WAYLAND_COMPUTER_ARCHIVE"} {
		if !strings.Contains(scheduledText, required) {
			t.Fatalf("scheduled realtiming is missing %q", required)
		}
	}
	if strings.Contains(scheduledText, "ref: ${{ needs.resolve-published-artifact.outputs.candidate-sha }}") || strings.Contains(scheduledText, "ref: ${{ github.event.workflow_run.head_sha }}") {
		t.Fatal("scheduled realtiming must check out the commit selected by the resolved immutable artifact")
	}
	for name, fixture := range map[string]struct {
		workflow workflowContract
		text     string
	}{"workflow-run": {realtiming, realtimeText}, "scheduled": {scheduled, scheduledText}} {
		jobTimeout := fixture.workflow.Jobs["service-acceptance-realtiming"].TimeoutMinutes
		goTimeout := workflowGoTestTimeoutMinutes(t, name, fixture.text)
		if jobTimeout != 100 || jobTimeout < goTimeout+15 {
			t.Fatalf("%s realtiming timeouts: job=%dm go=%dm, want job=100m and at least 15m outer margin", name, jobTimeout, goTimeout)
		}
		assertRegistryFaultAction(t, name, fixture.text, "disable-registry",
			"iptables -I OUTPUT 1 -p tcp --dport 443 -m conntrack --ctstate NEW -m owner --uid-owner 0 -j REJECT")
		assertRegistryFaultAction(t, name, fixture.text, "enable-registry",
			"iptables -D OUTPUT -p tcp --dport 443 -m conntrack --ctstate NEW -m owner --uid-owner 0 -j REJECT")
		assertAllPort443RulesOwnerScoped(t, name, fixture.text)
		for _, required := range []string{
			"sudo iptables -I OUTPUT 1 -p tcp --dport 443 -m conntrack --ctstate NEW -m owner --uid-owner 0 -j REJECT",
			"sudo iptables -D OUTPUT -p tcp --dport 443 -m conntrack --ctstate NEW -m owner --uid-owner 0 -j REJECT",
			`runner_job_uid="$(id -u)"`, "::error::the workflow job is unexpectedly running as root",
			"::error::Runner.Listener process was not found", "::error::Runner.Listener uid could not be read",
			"::error::Runner.Listener owner could not be read", "::error::Runner.Listener is unexpectedly running as root",
			"runner_job_uid=%s\\nrunner_listener_uid=%s\\nrunner_listener_owner=%s\\n",
			"WEFTY_OCI_PROVISION_RECEIPT=%s\\n",
		} {
			if !strings.Contains(fixture.text, required) {
				t.Fatalf("%s realtiming provisioning is missing %q", name, required)
			}
		}
	}
	for name, workflow := range map[string]workflowContract{"workflow-run": realtiming, "scheduled": scheduled} {
		result, ok := workflow.Jobs["realtiming-result"]
		if !ok {
			t.Fatalf("%s realtiming has no fail-closed result job", name)
		}
		needs := stringSlice(t, result.Needs)
		if !slices.Contains(needs, "resolve-published-artifact") || !slices.Contains(needs, "service-acceptance-realtiming") ||
			!strings.Contains(result.If, "always()") {
			t.Fatalf("%s realtiming result dependencies/guard = %#v if=%q", name, needs, result.If)
		}
		resultText := marshalJob(t, result)
		for _, required := range []string{"ARTIFACT_AVAILABLE", "$ARTIFACT_AVAILABLE", "= true",
			"REALTIMING_RESULT", "$REALTIMING_RESULT", "= success", "check-linux-computer-receipt.sh", "linux-computer-matrix.json"} {
			if !strings.Contains(resultText, required) {
				t.Fatalf("%s realtiming result does not fail closed on %q", name, required)
			}
		}
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
	for _, required := range []string{"publish-wayland-reference-computer", "$WAYLAND_COMPUTER_IMAGE_NAME@$index_digest", "wefty-computer-wayland-reference.oci.tar", "wefty-computer-wayland-reference-release.tar", "wayland-computer-image-receipt.json", "furniture_assertion"} {
		if !strings.Contains(string(imageBytes), required) {
			t.Fatalf("Wayland reference Computer publication contract is missing %q", required)
		}
	}
	assertFileContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", "exec /usr/local/bin/wefty-echo-service", "published-echo-service:", "clean-cache wefty node load-image")
	assertFileContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", "CodeImageUnavailable", "ImageFailureNetwork", "WEFTY_OCI_PROVISION_RECEIPT", "registry_disabled_pull_rejected=%t")
	assertFileContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", "WEFTY_OCI_COMPUTER_REFERENCE", "exerciseNativeLinuxReferenceComputer", "ComputerStartupReadinessTimeout", "assertReferenceComputerWireNegatives")
	assertFileContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", "WEFTY_OCI_WAYLAND_COMPUTER_REFERENCE", `exerciseNativeLinuxReferenceComputer(t, ctx, session, adapter, "wayland"`, "wayland_computer_reference_wire_negatives=true")
	assertFileMatches(t, "../examples/oci-echo-service/Dockerfile", `(?m)^# syntax=.*@sha256:[0-9a-f]{64}$`, `(?m)^ARG GO_IMAGE=.*@sha256:[0-9a-f]{64}$`, `(?m)^ARG BUSYBOX_IMAGE=.*@sha256:[0-9a-f]{64}$`)
	assertFileMatches(t, "../examples/computer/Dockerfile", `(?m)^# syntax=.*@sha256:[0-9a-f]{64}$`, `(?m)^ARG DEBIAN_IMAGE=.*@sha256:[0-9a-f]{64}$`)
	assertFileMatches(t, "../examples/computer-wayland/Dockerfile", `(?m)^# syntax=.*@sha256:[0-9a-f]{64}$`, `(?m)^ARG DEBIAN_IMAGE=.*@sha256:[0-9a-f]{64}$`)
	assertFileContains(t, "../examples/computer/Dockerfile", "snapshot.debian.org/archive/debian/20260827T000000Z", "ARG SOURCE_DATE_EPOCH=0", "chromium=", "/var/log/dpkg.log", "/var/log/alternatives.log", "/var/log/apt/*")
	assertFileContains(t, "../examples/computer/entrypoint.sh", "WEFTY_COMPUTER_VIEW_PORT", "WEFTY_COMPUTER_CONTROL_PORT", "/wefty/service")
	assertFileContains(t, "../examples/computer/rfb-websocket.py", `target.path != "/websockify"`, `"binary" not in offered`, "BinaryOnlyWebSocket")
	assertFileContains(t, "../examples/computer/rfb-backend.py", `command.append("-viewonly")`, "socket.AF_UNIX")
	assertFileContains(t, "../examples/computer/watch-driver.py", "/wefty/control/driver.json", "os.replace", `type(value["version"]) is not int`, `type(value["human_driving"]) is not bool`)
	assertFileContains(t, "../examples/computer/oracle.html", `data-wefty-input-oracle="v1"`, "events=0 bytes=0 hash=00000000")
	assertFileContains(t, "../examples/computer/pointer-oracle.py", "XQueryPointer", "input-oracle.json", "os.replace")
	assertFileContains(t, "../examples/computer-wayland/Dockerfile", "snapshot.debian.org/archive/debian/20260827T000000Z", "wayvnc=0.9.1-1", "sway=1.10.1-2", "mise-v2026.8.14", "wefty-verify-licenses", "ldconfig -p", "/usr/local/lib/libneatvnc.so.0")
	assertFileContains(t, "../examples/computer-wayland/Dockerfile", "WLR_BACKENDS=headless", "WLR_RENDERER=pixman", "WLR_HEADLESS_OUTPUTS=1")
	assertFileContains(t, "../examples/computer-wayland/entrypoint.sh", "wayvnc -w", "--disable-input", "WEFTY_COMPUTER_VIEW_PORT", "WEFTY_COMPUTER_CONTROL_PORT")
	assertFileContains(t, "../examples/computer-wayland/watch-driver.py", "/wefty/control/driver.json", "type(value[\"version\"]) is not int", "os.replace")
	assertFileContains(t, "../examples/computer-wayland/patches/neatvnc-rfb-websocket-v1.patch", "GET /websockify", "Sec-WebSocket-Protocol: binary", "WS_OPCODE_TEXT", "WEFTY_WAYVNC_RECORD_INPUT", "native-input-events")
	assertFileContains(t, "../examples/computer-wayland/surface.py", "input-oracle.json", "native-input-events", "agent-state-surface.json", "theme-surface.json", "os.replace")
	assertFileContains(t, "../examples/computer-wayland/LICENSES.md", "Herdr", "Apache-2.0", "no code or assets copied", "no code, assets, installer, name, or branding copied")
	assertFileContains(t, "../scripts/test-computer-wayland-furniture.sh", "test ! -e /dev/dri", "agent-state-surface.json", "theme-surface.json", "crash-briefing.json", "idle_rss_bytes")
	assertFileContains(t, "../scripts/test-computer-image-runtime.sh", "cmd/wefty-computer-conformance", "--input-oracle-path", "--driver-oracle-path")
	assertFileContains(t, "../docs/guides/computer-images.md", "Bring-your-own desktop is the product", "not a required base image", "CPU rendering", "--no-sandbox", "wefty-computer-conformance", "GPU-free Wayland")
	assertFileContains(t, "../docs/guides/computer-images.md", "docker buildx create", "tonistiigi/binfmt@sha256:", "--input-oracle-path", "NOT-RUN", "operator-owned")
	assertFileContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", "RunComputerServiceRealtiming", "computer_reference_publication_loss_recovery=%t", "computer_reference_helper_stop_start_profile_sign_in_rootfs=%t")
	assertFileContains(t, "../docs/runbooks/oci-node.md", "wefty node load-image", "acceptance-image-index-digest.txt")
	assertFileContains(t, "../docs/acceptance/m3-lima-transport.md", "acceptance-image-index-digest.txt", "computer-image-index-digest.txt", "wefty-computer-reference.oci.tar", "atomically within 60 seconds")
}

func assertRegistryFaultAction(t *testing.T, workflowName, workflowText, action, want string) {
	t.Helper()
	body := workflowCaseActionBody(t, workflowName, workflowText, action)
	var portRules []string
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "iptables ") && strings.Contains(line, "--dport 443") {
			portRules = append(portRules, line)
		}
	}
	if !slices.Equal(portRules, []string{want}) {
		t.Fatalf("%s %s port-443 rules = %#v, want %q", workflowName, action, portRules, want)
	}
}

func assertAllPort443RulesOwnerScoped(t *testing.T, workflowName, workflowText string) {
	t.Helper()
	for _, line := range strings.Split(workflowText, "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "iptables ") && strings.Contains(line, "--dport 443") &&
			(!strings.Contains(line, "-m owner") || !strings.Contains(line, "--uid-owner 0")) {
			t.Fatalf("%s realtiming has a port-443 rule outside the root-owner fault scope: %q", workflowName, line)
		}
	}
}

func workflowGoTestTimeoutMinutes(t *testing.T, workflowName, workflowText string) int {
	t.Helper()
	matches := regexp.MustCompile(`(?m)\bgo test [^\n]*-timeout=([0-9]+)m\b`).FindAllStringSubmatch(workflowText, -1)
	if len(matches) != 1 {
		t.Fatalf("%s realtiming go test timeout count = %d, want 1", workflowName, len(matches))
	}
	duration, err := time.ParseDuration(matches[0][1] + "m")
	if err != nil {
		t.Fatalf("%s realtiming go test timeout: %v", workflowName, err)
	}
	return int(duration / time.Minute)
}

func workflowCaseActionBody(t *testing.T, workflowName, workflowText, action string) string {
	t.Helper()
	var bodies []string
	lines := strings.Split(workflowText, "\n")
	for index := 0; index < len(lines); index++ {
		if strings.TrimSpace(lines[index]) != action+")" {
			continue
		}
		start := index + 1
		for index = start; index < len(lines) && strings.TrimSpace(lines[index]) != ";;"; index++ {
		}
		if index == len(lines) {
			t.Fatalf("%s %s case body has no terminator", workflowName, action)
		}
		bodies = append(bodies, strings.Join(lines[start:index], "\n"))
	}
	if len(bodies) != 1 {
		t.Fatalf("%s %s case body count = %d, want 1", workflowName, action, len(bodies))
	}
	return bodies[0]
}

func TestReferenceComputerPublisherConsumesRealCheckerReceipt(t *testing.T) {
	_, imageBytes := readWorkflow(t, "../.github/workflows/acceptance-image.yml")
	match := regexp.MustCompile(`receipt_assertion='([^']+)'`).FindSubmatch(imageBytes)
	if len(match) != 2 {
		t.Fatal("publisher receipt_assertion was not found")
	}
	assertion := string(match[1]) + " and .elf_e_machine == 62"
	recorder := computerconformance.NewRecorder("reference", "docker", "linux/amd64", time.Unix(1, 0))
	for _, definition := range computerconformance.CheckCatalog {
		status, detail := computerconformance.StatusPass, "observed"
		if definition.Scope == computerconformance.ScopeProfile {
			status, detail = computerconformance.StatusNotRun, computerconformance.ContainerdProfileNotRun
		}
		if err := recorder.Record(definition.ID, status, detail); err != nil {
			t.Fatal(err)
		}
	}
	recorder.RecordReadiness(false, time.Second)
	recorder.RecordPersistence(true, true)
	recorder.RecordEdgeRecovery(true)
	payload, err := computerconformance.Marshal(recorder.Finish(time.Unix(2, 0)))
	if err != nil {
		t.Fatal(err)
	}
	var conformant map[string]any
	if err := json.Unmarshal(payload, &conformant); err != nil {
		t.Fatal(err)
	}
	conformant["digest"], conformant["elf_e_machine"] = "sha256:good", 62
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
}

func TestReferenceComputerRunsTwentyBrokenImagesThroughRealChecker(t *testing.T) {
	payload, err := os.ReadFile("../scripts/test-computer-image-runtime.sh")
	if err != nil {
		t.Fatal(err)
	}
	rows := regexp.MustCompile(`(?m)^run_mutation ([^ ]+) ([^ ]+) '([^']+)'$`).FindAllSubmatch(payload, -1)
	if len(rows) != 20 {
		t.Fatalf("real broken-image rows = %d, want 20", len(rows))
	}
	names, cells := map[string]bool{}, map[string]bool{}
	for _, row := range rows {
		name, cell, detail := string(row[1]), string(row[2]), string(row[3])
		if names[name] {
			t.Fatalf("duplicate broken image %q", name)
		}
		if cells[cell] {
			t.Fatalf("duplicate owning cell %q", cell)
		}
		if detail == "" {
			t.Fatalf("broken image %q has no stable reason", name)
		}
		names[name], cells[cell] = true, true
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
