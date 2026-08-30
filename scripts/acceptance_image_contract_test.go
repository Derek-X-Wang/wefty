package scripts

import (
	"bytes"
	"encoding/json"
	"maps"
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
	for name, workflow := range map[string]workflowContract{"required": build, "publisher": image} {
		referenceBuild := workflow.Jobs["reference-computer-platform-build"]
		text := marshalJob(t, referenceBuild)
		if strings.Count(text, "docker buildx build --no-cache") != 2 {
			t.Fatalf("%s reference Computer lane must perform exactly two empty-cache solves", name)
		}
		if strings.Contains(text, "if [[ $VARIANT == xfce") || strings.Contains(text, `crane" push --insecure "$layout" "$registry_reference"`) {
			t.Fatalf("%s reference Computer lane bypasses the second solve for one variant", name)
		}
		assertWaylandFurnitureInvocations(t, name, workflow)
		for _, required := range []string{"::error title=computer image runtime::stage", "stage='runtime-conformance'", "stage='digest-compare'", "stage='rootfs-export'", "stage='elf-validate'", "stage='receipt-finalize'"} {
			if !strings.Contains(text, required) {
				t.Fatalf("%s reference Computer runtime step is missing attributed stage %q", name, required)
			}
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
		for _, required := range []string{
			"StandardError=append:/tmp/wefty-oci-helper-realtiming.stderr",
			"if: ${{ always() && runner.os == 'Linux' }}",
			"journalctl --boot --no-pager --utc --output=short-precise",
			"-u wefty-oci-helper-realtiming.service",
			"-u wefty-test-containerd.service",
			"systemctl status --no-pager --full",
			"wefty-oci-helper.stderr.txt",
			"linux-oci-diagnostics.status.txt",
			"set +e",
			"journalctl_exit=%s\\n",
			"systemctl_status_exit=%s\\n",
			"helper_stderr_test_exit=%s\\n",
			"helper_stderr_capture_exit=%s\\n",
			"PIPESTATUS[0]",
		} {
			if !strings.Contains(fixture.text, required) {
				t.Fatalf("%s realtiming diagnostics are missing %q", name, required)
			}
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
		workflowText := realtimeText
		if name == "scheduled" {
			workflowText = scheduledText
		}
		for _, required := range []string{"{os: ubuntu-latest, image: xfce}", "{os: ubuntu-latest, image: wayland}", "WEFTY_OCI_COMPUTER_VARIANT", "wayland_computer_release/amd64-runtime.json", "ubuntu-latest-xfce-", "ubuntu-latest-wayland-"} {
			if !strings.Contains(workflowText, required) {
				t.Fatalf("%s realtiming image dimension is missing %q", name, required)
			}
		}
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
			"REALTIMING_RESULT", "$REALTIMING_RESULT", "= success", "check-linux-computer-receipt.sh", "linux-computer-matrix.json", "linux-computer-receipt-xfce", "linux-computer-receipt-wayland", "xfce", "wayland"} {
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
	assertFileContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", "CodeImageUnavailable", "ImageFailureNetwork", "WEFTY_OCI_PROVISION_RECEIPT", "pull_from_empty=true\\nregistry_disabled_pull_rejected=%t\\nregistry_disabled_import=true")
	assertFileContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", "Wipe that repopulated cache", `requestRootFault(t, "reset-containerd")`, "wiped-cache binding reconciliation")
	assertFileNotContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", `strings.Replace(evidence, "pull_from_empty=true\\n"`)
	assertFileContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", "WEFTY_OCI_COMPUTER_REFERENCE", "exerciseNativeLinuxReferenceComputer", "ComputerStartupReadinessTimeout", "assertReferenceComputerWireNegatives")
	assertFileContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", "WEFTY_OCI_WAYLAND_COMPUTER_REFERENCE", `exerciseNativeLinuxReferenceComputer(t, ctx, session, adapter, "wayland"`, "wayland_computer_reference_wire_negatives=true")
	assertFileContains(t, "../agent/oci_service_publication_acceptance_test.go", `trap 'exit 143' TERM; while :; do sleep 0.1; done`, "restart_requested=true", `kill "$server" 2>/dev/null || true`)
	assertFileNotContains(t, "../agent/oci_service_publication_acceptance_test.go", `kill "$(cat /tmp/wefty-httpd.pid)"`)
	assertFileMatches(t, "../examples/oci-echo-service/Dockerfile", `(?m)^# syntax=.*@sha256:[0-9a-f]{64}$`, `(?m)^ARG GO_IMAGE=.*@sha256:[0-9a-f]{64}$`, `(?m)^ARG BUSYBOX_IMAGE=.*@sha256:[0-9a-f]{64}$`)
	assertFileMatches(t, "../examples/computer/Dockerfile", `(?m)^# syntax=.*@sha256:[0-9a-f]{64}$`, `(?m)^ARG DEBIAN_IMAGE=.*@sha256:[0-9a-f]{64}$`)
	assertFileMatches(t, "../examples/computer-wayland/Dockerfile", `(?m)^# syntax=.*@sha256:[0-9a-f]{64}$`, `(?m)^ARG DEBIAN_IMAGE=.*@sha256:[0-9a-f]{64}$`)
	assertFileContains(t, "../examples/computer/Dockerfile", "snapshot.debian.org/archive/debian/20260827T000000Z", "ARG SOURCE_DATE_EPOCH=0", "chromium=", "/var/log/dpkg.log", "/var/log/alternatives.log", "/var/log/apt/*")
	assertFileContains(t, "../examples/computer/entrypoint.sh", "WEFTY_COMPUTER_VIEW_PORT", "WEFTY_COMPUTER_CONTROL_PORT", "/wefty/service")
	for _, path := range []string{"../examples/computer/entrypoint.sh", "../examples/computer/fixtures/entrypoint.sh", "../examples/computer-wayland/entrypoint.sh", "../examples/computer-wayland/entrypoint-fixture.sh"} {
		assertFileContains(t, path, "wait || true\n  chmod -R u+rwX,go+rwX /wefty/service")
	}
	assertFileContains(t, "../examples/computer/rfb-websocket.py", `target.path != "/websockify"`, `"binary" not in offered`, "BinaryOnlyWebSocket")
	assertFileContains(t, "../examples/computer/rfb-backend.py", `command.append("-viewonly")`, "socket.AF_UNIX")
	assertFileContains(t, "../examples/computer/watch-driver.py", "/wefty/control/driver.json", "fingerprint", "os.replace", `type(value["version"]) is not int`, `type(value["human_driving"]) is not bool`)
	for _, path := range []string{"../examples/computer/entrypoint.sh", "../examples/computer/watch-driver.py", "../examples/computer/rfb-websocket.py"} {
		assertFileNotContains(t, path, "WEFTY_CONFORMANCE_MUTATION")
	}
	for _, path := range []string{"../examples/computer/fixtures/entrypoint.sh", "../examples/computer/fixtures/watch-driver.py", "../examples/computer/fixtures/rfb-websocket.py"} {
		assertFileContains(t, path, "WEFTY_CONFORMANCE_MUTATION")
	}
	assertFileContains(t, "../examples/computer/oracle.html", `data-wefty-input-oracle="v1"`, "events=0 bytes=0 hash=00000000")
	assertFileContains(t, "../examples/computer/pointer-oracle.py", "XQueryPointer", "input-oracle.json", "os.replace")
	assertFileContains(t, "../examples/computer-wayland/Dockerfile", "snapshot.debian.org/archive/debian/20260827T000000Z", "wayvnc=0.9.1-1", "sway=1.10.1-2", "wev=", "mise-v2026.8.14", "wefty-verify-licenses", "ldconfig -p", "/usr/local/lib/libneatvnc.so.0", "non-dpkg-components.tsv", "mise-MIT.txt", "wefty-Apache-2.0.txt", "rm -f \"/usr/lib/$multiarch/libneatvnc.so\"*")
	assertFileNotContains(t, "../examples/computer-wayland/Dockerfile", "LD_LIBRARY_PATH", "ADD --chmod")
	waylandDockerfile, err := os.ReadFile("../examples/computer-wayland/Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(waylandDockerfile), "/var/cache/ldconfig/aux-cache") != 2 {
		t.Fatal("Wayland image must remove ldconfig's nondeterministic cache after both invocations")
	}
	assertFileContains(t, "../examples/computer-wayland/Dockerfile", "WLR_BACKENDS=headless", "WLR_RENDERER=pixman", "WLR_HEADLESS_OUTPUTS=1")
	assertFileContains(t, "../examples/computer-wayland/entrypoint.sh", "wayvnc -w", "--disable-input", "--class=chromium", "--app=http://127.0.0.1:18888/", "WEFTY_COMPUTER_VIEW_PORT", "WEFTY_COMPUTER_CONTROL_PORT", "surface-ready", "surface-failure", "surface_wait=45", "within 45 seconds", "view-edge-ready", "control-edge-ready")
	assertFileContains(t, "../examples/computer-wayland/entrypoint-fixture.sh", "WEFTY_CONFORMANCE_MUTATION", "--class=chromium", "--app=http://127.0.0.1:18888/", "surface-failure", "oracle_wait=45", "within 45 seconds", "wefty-view-proxy")
	assertFileContains(t, "../examples/computer/fixtures/Dockerfile", "view-proxy-fixture.py", "/usr/local/libexec/wefty-view-proxy", "wefty-xfce-entrypoint-fixture", "wefty-xfce-watch-driver-fixture", "wefty-xfce-rfb-websocket-fixture")
	assertFileNotContains(t, "../examples/computer-wayland/entrypoint.sh", "WEFTY_CONFORMANCE_MUTATION", "WEFTY_WAYVNC_RECORD_INPUT")
	assertFileContains(t, "../examples/computer-wayland/watch-driver.py", "/wefty/control/driver.json", "type(value[\"version\"]) is not int", "fingerprint", "os.replace")
	assertFileContains(t, "../examples/computer-wayland/patches/neatvnc-rfb-websocket-v1.patch", "GET /websockify", "Sec-WebSocket-Protocol: binary", "WS_OPCODE_TEXT", "wefty_mutation_hooks", "view-edge-ready", "control-edge-ready")
	assertFileNotContains(t, "../examples/computer-wayland/patches/neatvnc-rfb-websocket-v1.patch", "WEFTY_WAYVNC_RECORD_INPUT", "native-input-events")
	assertFileContains(t, "../examples/computer-wayland/surface.py", "input-oracle.json", `"ready": False`, `INPUT["ready"] = True`, "def wefty_record_input", "Record wev keys or pointer facts from the focused Chromium client", `"wev"`, "wl_keyboard", "secrets.token_hex(32)", "secrets.compare_digest", "wayland-surface-focus-convergence-failed", "focus_input_observer", `'[app_id="wev"] focus'`, `self.path not in ("/surface-ready", "/input")`, "agent-state-surface.json", "theme-surface.json", "os.replace")
	assertFileNotContains(t, "../examples/computer-wayland/surface.py", "wl_pointer", "POINTER_MOTION")
	assertFileContains(t, "../examples/computer-wayland/oracle.html", "__WEFTY_INPUT_NONCE__", "inputNonce", "pointermove", "pointerdown", "pointerup", "window.outerHeight - window.innerHeight", "'/input'", "'/surface-ready'")
	assertFileNotContains(t, "../examples/computer-wayland/oracle.html", "keydown", "keyup")
	assertFileContains(t, "../examples/computer-wayland/sway-config", `title="^Wefty Wayland Computer$"`, "border none", "fullscreen enable", `app_id="wev"`, "opacity set 0.0", "floating enable", "resize set width 32 px height 32 px", "focus")
	assertFileContains(t, "../examples/computer-wayland/LICENSES.md", "Herdr", "Apache-2.0", "no code or assets copied", "no code, assets, installer, name, or branding copied")
	assertFileContains(t, "../scripts/test-computer-wayland-furniture.sh", "gpu_device_absent=$(docker exec", "agent_states_observed=$(docker exec", "agent_states_observed:$agent_states_observed", "self_reconfiguration_observed=$(docker exec", "crash_briefing_observed=$(docker exec", "mise_stubs_present=$(docker exec", "license_manifest_present=$(docker exec", "test ! -e /dev/dri", "agent-state-surface.json", "theme-surface.json", "crash-briefing.json", "idle_rss_bytes", "wefty-verify-licenses --check", `type == "number"`)
	assertFileContains(t, "../scripts/test-computer-image-runtime.sh", "cmd/wefty-computer-conformance", "--input-oracle-path", "--driver-oracle-path", "executed_rows", "check-computer-image-runtime-evidence.sh", "Dockerfile.wayland-text", "checker_wall_seconds", "docker rmi")
	for _, path := range []string{"../docs/contracts/computer-image.md", "../docs/guides/computer-images.md"} {
		assertFileContains(t, path, "version", "human_driving", "generation", "fingerprint", "classification", "lowercase", "exact bytes", "valid", "malformed", "unknown-version", "missing", "sentinel")
	}
	assertFileContains(t, "../docs/guides/computer-images.md", "Bring-your-own desktop is the product", "not a required base image", "CPU rendering", "--no-sandbox", "wefty-computer-conformance", "GPU-free Wayland")
	assertFileContains(t, "../docs/guides/computer-images.md", "docker buildx create", "tonistiigi/binfmt@sha256:", "--input-oracle-path", "NOT-RUN", "operator-owned")
	assertFileContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", "RunComputerServiceRealtiming", "computer_reference_publication_loss_recovery=%t", "computer_reference_helper_stop_start_profile_sign_in_rootfs=%t")
	assertFileContains(t, "../docs/runbooks/oci-node.md", "wefty node load-image", "acceptance-image-index-digest.txt")
	assertFileContains(t, "../docs/acceptance/m3-lima-transport.md", "acceptance-image-index-digest.txt", "computer-image-index-digest.txt", "wefty-computer-reference.oci.tar", "atomically within 60 seconds")
}

func assertWaylandFurnitureInvocations(t *testing.T, workflowName string, workflow workflowContract) {
	t.Helper()
	const command = "scripts/test-computer-wayland-furniture.sh"
	wantArguments := []string{
		`--image "$pinned_reference"`,
		`--arch "$ARCH"`,
		`--conformance-receipt "$evidence/${ARCH}-runtime.json"`,
		`--output "$evidence/${ARCH}-furniture.json"`,
		`--metrics-output "$evidence/${ARCH}-metrics.json"`,
	}

	var invocations []string
	for _, job := range workflow.Jobs {
		for _, step := range job.Steps {
			lines := strings.Split(step.Run, "\n")
			for index := 0; index < len(lines); index++ {
				line := strings.TrimSpace(lines[index])
				if !strings.HasPrefix(line, command+" ") {
					continue
				}
				invocation := strings.TrimSpace(strings.TrimSuffix(line, `\`))
				for strings.HasSuffix(line, `\`) {
					index++
					if index == len(lines) {
						t.Fatalf("%s has an unterminated %s invocation", workflowName, command)
					}
					line = strings.TrimSpace(lines[index])
					invocation += " " + strings.TrimSpace(strings.TrimSuffix(line, `\`))
				}
				invocations = append(invocations, invocation)
			}
		}
	}
	if len(invocations) != 1 {
		t.Fatalf("%s %s invocation count = %d, want 1", workflowName, command, len(invocations))
	}
	for _, required := range wantArguments {
		if !strings.Contains(invocations[0], required) {
			t.Fatalf("%s %s invocation is missing %q: %s", workflowName, command, required, invocations[0])
		}
	}
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
	matches := regexp.MustCompile(`receipt_assertion='([^']+)'`).FindAllSubmatch(imageBytes, -1)
	if len(matches) != 2 {
		t.Fatalf("publisher receipt_assertions = %d, want XFCE and Wayland", len(matches))
	}
	if string(matches[0][1]) != string(matches[1][1]) {
		t.Fatal("XFCE and Wayland publishers enforce different runtime receipts")
	}
	assertion := string(matches[0][1]) + " and .elf_e_machine == 62"
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
	for name, mutate := range map[string]func(map[string]any){
		"empty catalog": func(value map[string]any) { value["checks"] = []any{} },
		"missing load-bearing cell": func(value map[string]any) {
			checks := value["checks"].([]any)
			filtered := checks[:0]
			for _, raw := range checks {
				if raw.(map[string]any)["id"] != "input.control-accepted" {
					filtered = append(filtered, raw)
				}
			}
			value["checks"] = filtered
		},
		"renamed catalog": func(value map[string]any) {
			for _, raw := range value["checks"].([]any) {
				raw.(map[string]any)["id"] = "renamed"
			}
		},
	} {
		t.Run("reject/"+name, func(t *testing.T) {
			payload, err := json.Marshal(conformant)
			if err != nil {
				t.Fatal(err)
			}
			var subject map[string]any
			if err := json.Unmarshal(payload, &subject); err != nil {
				t.Fatal(err)
			}
			mutate(subject)
			if valid(subject) {
				t.Fatal("real publisher jq assertion accepted invalid receipt")
			}
		})
	}
}

func TestWaylandFurniturePublisherRejectsUnearnedFields(t *testing.T) {
	_, imageBytes := readWorkflow(t, "../.github/workflows/acceptance-image.yml")
	match := regexp.MustCompile(`furniture_assertion='([^']+)'`).FindSubmatch(imageBytes)
	if len(match) != 2 {
		t.Fatal("publisher furniture_assertion was not found")
	}
	assertion := string(match[1])
	conformant := map[string]any{
		"gpu_device_absent": true, "native_wayvnc_websocket": true, "view_disable_input": true,
		"agent_states_observed":         []string{"idle", "working", "blocked", "done"},
		"self_reconfiguration_observed": true, "crash_briefing_observed": true,
		"mise_stubs_present": true, "license_manifest_present": true,
		"idle_rss_bytes": 1.0, "cold_start_seconds": 1.0,
	}
	valid := func(value map[string]any) bool {
		payload, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command("jq", "-e", assertion)
		command.Stdin = bytes.NewReader(payload)
		return command.Run() == nil
	}
	if !valid(conformant) {
		t.Fatal("publisher rejected observed furniture receipt")
	}
	for name, mutate := range map[string]func(map[string]any){
		"null cold start":           func(value map[string]any) { value["cold_start_seconds"] = nil },
		"literal license claim":     func(value map[string]any) { value["license_manifest_present"] = false },
		"missing input enforcement": func(value map[string]any) { delete(value, "view_disable_input") },
	} {
		t.Run(name, func(t *testing.T) {
			value := maps.Clone(conformant)
			mutate(value)
			if valid(value) {
				t.Fatal("publisher accepted unearned furniture evidence")
			}
		})
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
	if bytes.Contains(payload, []byte(`if [[ $arch != amd64 ]]; then exit 0; fi`)) {
		t.Fatal("arm64 skips the broken-image matrix")
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

func assertFileNotContains(t *testing.T, path string, values ...string) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if strings.Contains(string(payload), value) {
			t.Fatalf("%s unexpectedly contains %q", path, value)
		}
	}
}
