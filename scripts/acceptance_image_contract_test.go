package scripts

import (
	"os"
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
	gate, gateBytes := readWorkflow(t, "../.github/workflows/contract-gate.yml")
	realtiming, realtimingBytes := readWorkflow(t, "../.github/workflows/service-acceptance-realtiming.yml")

	if _, ok := image.On["workflow_call"]; !ok {
		t.Fatal("acceptance-image must expose the secretless required workflow_call lane")
	}
	if _, ok := image.On["push"]; !ok {
		t.Fatal("acceptance-image must publish from push to main")
	}
	for _, workflow := range []workflowContract{image, gate, realtiming} {
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
	for name, job := range image.Jobs {
		if name != "publish" && job.Permissions["packages"] != "" {
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
	if strings.Contains(string(imageBytes), "secrets.") {
		t.Fatal("acceptance-image must not use repository secrets")
	}
	if strings.Contains(marshalJob(t, image.Jobs["reproducible-platform-build"]), "github.token") {
		t.Fatal("PR-callable image build must not consume github.token")
	}

	called := gate.Jobs["acceptance-image"]
	if called.Uses != "./.github/workflows/acceptance-image.yml" {
		t.Fatalf("contract-gate image lane uses %q", called.Uses)
	}
	needs := stringSlice(t, gate.Jobs["all-tests-pass"].Needs)
	if !slices.Contains(needs, "acceptance-image") || !strings.Contains(string(gateBytes), "ACCEPTANCE_IMAGE_RESULT") {
		t.Fatal("all-tests-pass does not fail closed on acceptance-image")
	}

	if _, ok := realtiming.On["workflow_run"]; !ok {
		t.Fatal("realtiming must be causally downstream of acceptance-image")
	}
	realtimeText := string(realtimingBytes)
	for _, required := range []string{"workflow_run.head_sha", "workflow_run.id", "actions/download-artifact@", "run-id:", "acceptance-image-index-digest.txt", "$ECHO_REFERENCE@$ECHO_DIGEST"} {
		if !strings.Contains(realtimeText, required) {
			t.Fatalf("realtiming is missing immutable artifact contract %q", required)
		}
	}
	for _, forbidden := range []string{"ref: main", "$IMAGE_NAME:$CANDIDATE_SHA", "crane pull"} {
		if strings.Contains(realtimeText, forbidden) {
			t.Fatalf("realtiming reintroduced mutable consumption %q", forbidden)
		}
	}
	for _, required := range []string{"$IMAGE_NAME@$index_digest", "--probe-reference \"$IMAGE_NAME\"", "bin/wefty", "wefty-acceptance-image-release.tar", "Public GHCR verification failed"} {
		if !strings.Contains(string(imageBytes), required) {
			t.Fatalf("publication contract is missing %q", required)
		}
	}
	assertFileContains(t, "../runner/ocihelper/containerd_engine_realtiming_linux_test.go", "exec /usr/local/bin/wefty-echo-service", "published-echo-service:", "clean-cache wefty node load-image")
	assertFileMatches(t, "../examples/oci-echo-service/Dockerfile", `(?m)^# syntax=.*@sha256:[0-9a-f]{64}$`, `(?m)^ARG GO_IMAGE=.*@sha256:[0-9a-f]{64}$`, `(?m)^ARG BUSYBOX_IMAGE=.*@sha256:[0-9a-f]{64}$`)
	assertFileContains(t, "../docs/runbooks/oci-node.md", "wefty node load-image", "acceptance-image-index-digest.txt")
	assertFileContains(t, "../docs/acceptance/m3-lima-transport.md", "acceptance-image-index-digest.txt")
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
