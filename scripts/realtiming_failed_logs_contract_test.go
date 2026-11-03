package scripts

import (
	"strings"
	"testing"
)

func TestRealtimingFailedRowLogArtifactContract(t *testing.T) {
	for _, path := range []string{
		"../.github/workflows/service-acceptance-realtiming.yml",
		"../.github/workflows/service-acceptance-realtiming-scheduled.yml",
	} {
		t.Run(path, func(t *testing.T) {
			workflow, _ := readWorkflow(t, path)
			var tests, uploads int
			const evidence = "${{ runner.temp }}/service-acceptance-realtiming"
			const helper = "${{ runner.os == 'Linux' && '/tmp/wefty-oci-helper-realtiming.stderr' || '' }}"
			for _, step := range workflow.Jobs["service-acceptance-realtiming"].Steps {
				if strings.Contains(step.Run, "go test -count=1 -p 1 -timeout=75m -v -tags=service_acceptance_realtiming ./...") {
					tests++
					if step.Env["WEFTY_REALTIME_EVIDENCE_DIR"] != evidence || step.Env["WEFTY_REALTIME_HELPER_LOG"] != helper {
						t.Fatal("test rows must retain failed logs in the uploaded directory, with Linux helper snapshots")
					}
				}
				if step.Name == "Upload service acceptance evidence" {
					uploads++
					if step.If != "${{ always() }}" || step.With["path"] != evidence || !strings.HasPrefix(step.Uses, "actions/upload-artifact@") {
						t.Fatal("failed-row logs must upload even after test failure")
					}
				}
			}
			if tests != 1 || uploads != 1 {
				t.Fatalf("test steps=%d upload steps=%d", tests, uploads)
			}
		})
	}
}
