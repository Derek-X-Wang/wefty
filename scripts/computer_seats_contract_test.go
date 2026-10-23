package scripts

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func validateSeatWorkflow(payload []byte) error {
	var workflow struct {
		On map[string]struct {
			Paths []string `yaml:"paths"`
		} `yaml:"on"`
		Concurrency struct {
			Cancel string `yaml:"cancel-in-progress"`
		} `yaml:"concurrency"`
		Jobs map[string]struct {
			RunsOn   string `yaml:"runs-on"`
			Strategy struct {
				Matrix struct {
					Arch []string `yaml:"arch"`
				} `yaml:"matrix"`
			} `yaml:"strategy"`
			Needs       []string          `yaml:"needs"`
			If          string            `yaml:"if"`
			Permissions map[string]string `yaml:"permissions"`
			Steps       []workflowStep    `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(payload, &workflow); err != nil {
		return err
	}
	if workflow.Concurrency.Cancel != "${{ github.event_name == 'pull_request' }}" {
		return fmt.Errorf("main promotion can be cancelled")
	}
	lane, ok := workflow.Jobs["default-computer-seats"]
	if !ok || !slices.Equal(lane.Strategy.Matrix.Arch, []string{"amd64", "arm64"}) {
		return fmt.Errorf("missing native two-architecture matrix")
	}
	if lane.RunsOn != "${{ matrix.arch == 'arm64' && 'ubuntu-24.04-arm' || 'ubuntu-24.04' }}" {
		return fmt.Errorf("wrong native runners: %s", lane.RunsOn)
	}
	publisher := workflow.Jobs["publish-default-computer"]
	if !slices.Equal(publisher.Needs, []string{"default-computer-seats"}) || publisher.Permissions["packages"] != "write" || !strings.Contains(publisher.If, "github.event_name == 'push'") || !strings.Contains(publisher.If, "refs/heads/main") {
		return fmt.Errorf("publisher is not gated on successful main Seat tests")
	}
	for _, event := range []string{"pull_request", "push", "workflow_dispatch"} {
		if _, ok := workflow.On[event]; !ok {
			return fmt.Errorf("missing event %s", event)
		}
	}
	for _, required := range []string{"examples/computer/**", "internal/computerconformance/**", "cmd/wefty-computer-conformance/**", "runner/ocihelper/**", "contract/**", "go.mod", "go.sum", "scripts/verify-acceptance-image.sh", "scripts/oci-tested-versions.env", ".github/workflows/computer-seats.yml"} {
		if !slices.Contains(workflow.On["pull_request"].Paths, required) {
			return fmt.Errorf("missing PR path filter %s", required)
		}
	}
	text := string(payload)
	for _, required := range []string{"examples/computer/**", "internal/computerconformance/**", "cmd/wefty-computer-conformance/**", "runner/ocihelper/**", "contract/**", "go.mod", "go.sum", "scripts/verify-acceptance-image.sh", "scripts/oci-tested-versions.env", ".github/workflows/computer-seats.yml", "--build-arg BASE_IMAGE=\"$pinned\"", "TestDefaultComputerSeatsAtProductionTimings", "--seat-evidence"} {
		if !strings.Contains(text, required) {
			return fmt.Errorf("missing %s", required)
		}
	}
	repair, upload := -1, -1
	for i, step := range lane.Steps {
		if step.Name == "Make Seat evidence readable after every outcome" && step.If == "always()" && strings.Contains(step.Run, "sudo chown") {
			repair = i
		}
		if strings.Contains(step.Uses, "actions/upload-artifact@") {
			upload = i
		}
	}
	if repair < 0 || upload <= repair {
		return fmt.Errorf("evidence ownership is not repaired before upload on failure")
	}
	for _, step := range publisher.Steps {
		if strings.Contains(step.Run, "docker build") {
			return fmt.Errorf("publisher rebuilds tested bytes")
		}
	}
	if !strings.Contains(text, "crane\" push") || !strings.Contains(text, "crane\" index append") {
		return fmt.Errorf("missing digest-preserving promotion")
	}
	return nil
}

func TestDefaultComputerSeatsRunBothNativeArchitecturesBeforePublication(t *testing.T) {
	payload := mustReadFile(t, "../.github/workflows/computer-seats.yml")
	if err := validateSeatWorkflow(payload); err != nil {
		t.Fatal(err)
	}
	for name, mutation := range map[string][]byte{
		"profile dependency filter": bytes.Replace(payload, []byte("      - contract/**\n"), nil, 1),
		"architecture":              bytes.Replace(payload, []byte("arch: [amd64, arm64]"), []byte("arch: [amd64]"), 1),
		"runner":                    bytes.Replace(payload, []byte("'ubuntu-24.04-arm'"), []byte("'ubuntu-latest'"), 1),
		"main cancellation":         bytes.Replace(payload, []byte("cancel-in-progress: ${{ github.event_name == 'pull_request' }}"), []byte("cancel-in-progress: true"), 1),
		"promotion gate":            bytes.Replace(payload, []byte("needs: [default-computer-seats]"), []byte("needs: []"), 1),
	} {
		if bytes.Equal(payload, mutation) || validateSeatWorkflow(mutation) == nil {
			t.Fatalf("%s drift was accepted", name)
		}
	}
	for _, path := range []string{"acceptance-image-build.yml", "acceptance-image.yml", "contract-gate.yml", "service-acceptance-realtiming.yml"} {
		payload := mustReadFile(t, "../.github/workflows/"+path)
		if strings.Contains(string(payload), "default-computer-seats") || strings.Contains(string(payload), "computer-seats.yml") {
			t.Fatalf("Seat image lane is coupled to %s", path)
		}
	}
}

func TestHelperSeatIM5UsesRootOnlySeatLane(t *testing.T) {
	payload := mustReadFile(t, "../.github/workflows/computer-seats.yml")
	text := string(payload)
	for _, required := range []string{"TestHelperComputerSeatsIM5", "helper-seats.test", "WEFTY_HELPER_SEAT_EVIDENCE"} {
		if !strings.Contains(text, required) {
			t.Fatalf("helper IM-5 lane missing %s", required)
		}
	}
	test := string(mustReadFile(t, "../runner/ocihelper/computer_seats_lane_linux_test.go"))
	if !strings.HasPrefix(test, "//go:build linux && computer_seat_lane\n") {
		t.Fatal("root-only helper test escaped its dedicated tag")
	}
}
