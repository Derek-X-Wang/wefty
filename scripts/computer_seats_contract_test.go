package scripts

import (
	"strings"
	"testing"
)

func TestDefaultComputerSeatsRunBothNativeArchitecturesBeforePublication(t *testing.T) {
	build, _ := readWorkflow(t, "../.github/workflows/acceptance-image-build.yml")
	image, _ := readWorkflow(t, "../.github/workflows/acceptance-image.yml")
	job, ok := build.Jobs["default-computer-seats"]
	if !ok {
		t.Fatal("default image has no real-profile Seat acceptance lane")
	}
	text := marshalJob(t, job)
	for _, required := range []string{"TestDefaultComputerSeatsAtProductionTimings", "BuildRuntimeSpec", "-tags service_acceptance_realtiming", "Dockerfile.missing-seat-passwd", "--seat-evidence", "--expected-platform", "--receipt", "sha256"} {
		if !strings.Contains(text, required) {
			t.Errorf("Seat lane missing %s", required)
		}
	}
	publish := image.Jobs["publish-default-computer"]
	if publish.Permissions["packages"] != "write" || !strings.Contains(publish.If, "refs/heads/main") {
		t.Fatal("default publisher is not main-only")
	}
	text = marshalJob(t, publish)
	if strings.Contains(text, "docker build") || !strings.Contains(text, "crane\" push") {
		t.Fatal("publisher must promote tested archives")
	}
}
