package scripts

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const craneCheckoutAction = "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"

var (
	craneTempInvocation     = regexp.MustCompile(`(?m)^\s*bash\s+["']?\$(?:RUNNER_TEMP|\{RUNNER_TEMP\})/download-crane-modules\.sh["']?\s*$`)
	craneRelativeInvocation = regexp.MustCompile(`(?:bash\s+|(?m:^\s*))["']?(?:\./)?scripts/download-crane-modules\.sh(?:["'\s;]|$)`)
)

type craneHelperPlan struct {
	directory, stage, invocation string
}

func candidateCraneCheckout(step workflowStep) bool {
	ref, _ := step.With["ref"].(string)
	return strings.HasPrefix(step.Uses, "actions/checkout@") && ref != "" && ref != "${{ github.sha }}" && ref != "${{ github.workflow_sha }}"
}

func validateCandidateCraneDownload(job workflowJob) (craneHelperPlan, error) {
	plan := craneHelperPlan{}
	helper, stage, candidate, invocation := -1, -1, -1, -1
	for i, step := range job.Steps {
		if strings.HasPrefix(step.Uses, "actions/checkout@") && step.With["sparse-checkout"] == "scripts/download-crane-modules.sh" {
			helper = i
			if step.Uses != craneCheckoutAction || step.If != "" {
				return plan, fmt.Errorf("helper checkout must use the existing pin and run unconditionally")
			}
			if step.With["repository"] != "${{ github.repository }}" || step.With["ref"] != "${{ github.workflow_sha }}" {
				return plan, fmt.Errorf("helper must come from the workflow repository and revision")
			}
			if step.With["sparse-checkout-cone-mode"] != false || step.With["persist-credentials"] != false {
				return plan, fmt.Errorf("helper checkout must select one file without persisted credentials")
			}
			plan.directory, _ = step.With["path"].(string)
			if !filepath.IsLocal(plan.directory) || plan.directory == "." {
				return plan, fmt.Errorf("helper checkout needs a dedicated workspace folder")
			}
		} else if candidateCraneCheckout(step) {
			candidate = i
			if path, _ := step.With["path"].(string); path != "" && path != "." {
				return plan, fmt.Errorf("candidate checkout is expected at the workspace root")
			}
		} else if strings.Contains(step.Run, "download-crane-modules.sh") {
			if craneRelativeInvocation.MatchString(step.Run) {
				return plan, fmt.Errorf("candidate job must not execute a candidate-relative crane helper")
			}
			if command := craneTempInvocation.FindString(step.Run); command != "" {
				invocation = i
				plan.invocation = strings.TrimSpace(command)
				if strings.Index(step.Run, command) > strings.Index(step.Run, "go install github.com/google/go-containerregistry/cmd/crane@") {
					return plan, fmt.Errorf("helper must execute before crane installation")
				}
			} else {
				stage, plan.stage = i, step.Run
				if step.If != "" {
					return plan, fmt.Errorf("helper staging must run unconditionally")
				}
			}
		}
	}
	if helper < 0 || stage <= helper || candidate <= stage || invocation <= candidate {
		return plan, fmt.Errorf("require helper checkout, staging, candidate checkout, then temp helper execution")
	}
	for _, step := range job.Steps[candidate+1:] {
		if strings.Contains(step.Run, plan.directory) {
			return plan, fmt.Errorf("candidate builds must not use the helper checkout folder")
		}
	}
	return plan, nil
}

func TestCraneDownloadWorkflowRevisionContract(t *testing.T) {
	paths, err := filepath.Glob("../.github/workflows/*.y*ml")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"acceptance-image-build.yml/reproducible-platform-build":       false,
		"acceptance-image-build.yml/reference-computer-platform-build": false,
		"computer-seats.yml/default-computer-seats":                    false,
	}
	callers := 0
	for _, path := range paths {
		workflow, _ := readWorkflow(t, path)
		for name, job := range workflow.Jobs {
			key := filepath.Base(path) + "/" + name
			if strings.Contains(job.Uses, "/acceptance-image-build.yml") {
				callers++
				if job.Uses != "./.github/workflows/acceptance-image-build.yml" {
					t.Errorf("%s: workflow SHA requires a local reusable-workflow reference", key)
				}
			}
			candidate, usesHelper := false, false
			for _, step := range job.Steps {
				candidate = candidate || candidateCraneCheckout(step)
				usesHelper = usesHelper || strings.Contains(step.Run, "download-crane-modules.sh")
			}
			if !candidate || !usesHelper {
				continue
			}
			want[key] = true
			t.Run(key, func(t *testing.T) {
				plan, err := validateCandidateCraneDownload(job)
				if err != nil {
					t.Fatal(err)
				}
				verifyCraneHelperStaging(t, plan)
			})
		}
	}
	for key, seen := range want {
		if !seen {
			t.Errorf("missing workflow-revision crane download: %s", key)
		}
	}
	if callers != 2 {
		t.Fatalf("expected both local acceptance-image-build callers, got %d", callers)
	}
}

func verifyCraneHelperStaging(t *testing.T, plan craneHelperPlan) {
	t.Helper()
	for _, cleanup := range []string{"git-clean", "recreate-root"} {
		t.Run(cleanup, func(t *testing.T) {
			root := t.TempDir()
			workspace, temp := filepath.Join(root, "workspace"), filepath.Join(root, "runner-temp")
			helper := filepath.Join(workspace, plan.directory, "scripts", "download-crane-modules.sh")
			if err := os.MkdirAll(filepath.Dir(helper), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(temp, 0755); err != nil {
				t.Fatal(err)
			}
			payload := []byte("#!/usr/bin/env bash\nprintf '%s' \"$PWD\" > \"$HELPER_OBSERVED_CWD\"\n")
			if err := os.WriteFile(helper, payload, 0644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(workspace, "stale"), []byte("old workspace file"), 0644); err != nil {
				t.Fatal(err)
			}
			env := append(os.Environ(), "RUNNER_TEMP="+temp, "GITHUB_WORKSPACE="+workspace, "HELPER_OBSERVED_CWD="+filepath.Join(temp, "cwd"))
			run := func(name string, args ...string) {
				t.Helper()
				cmd := exec.Command(name, args...)
				cmd.Dir, cmd.Env = workspace, env
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("%s: %v\n%s", name, err, output)
				}
			}
			run("bash", "-c", plan.stage)
			if _, err := os.Stat(filepath.Join(workspace, plan.directory)); !os.IsNotExist(err) {
				t.Fatal("sparse helper checkout must be removed before candidate checkout")
			}
			if cleanup == "git-clean" {
				run("git", "init", "--quiet")
				run("git", "clean", "-ffdx")
			} else {
				entries, err := os.ReadDir(workspace)
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if err := os.RemoveAll(filepath.Join(workspace, entry.Name())); err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, err := os.Stat(filepath.Join(workspace, "stale")); !os.IsNotExist(err) {
				t.Fatal("candidate checkout cleanup did not remove the stale workspace file")
			}
			staged, err := os.ReadFile(filepath.Join(temp, "download-crane-modules.sh"))
			if err != nil || !bytes.Equal(staged, payload) {
				t.Fatalf("helper must survive root checkout cleanup outside the candidate tree: %v", err)
			}
			run("bash", "-c", plan.invocation)
			cwd, err := os.ReadFile(filepath.Join(temp, "cwd"))
			if err != nil {
				t.Fatal(err)
			}
			physical, err := filepath.EvalSymlinks(workspace)
			if err != nil || string(cwd) != physical {
				t.Fatalf("helper working directory = %q, want candidate %q: %v", cwd, physical, err)
			}
			// A missing source must fail staging, even though removing its folder succeeds.
			if err := validateCraneStagingFailure(plan.stage, workspace, env); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCraneDownloadWorkflowRevisionRejectsCandidateHelper(t *testing.T) {
	_, payload := readWorkflow(t, "../.github/workflows/acceptance-image-build.yml")
	for name, mutate := range map[string]func(*workflowJob){
		"candidate revision":   func(job *workflowJob) { job.Steps[0].With["ref"] = "${{ inputs.candidate_sha }}" },
		"candidate repository": func(job *workflowJob) { job.Steps[0].With["repository"] = "${{ inputs.source_repository }}" },
		"relative execution": func(job *workflowJob) {
			for i := range job.Steps {
				if craneTempInvocation.MatchString(job.Steps[i].Run) {
					job.Steps[i].Run = craneTempInvocation.ReplaceAllString(job.Steps[i].Run, "bash scripts/download-crane-modules.sh")
				}
			}
		},
		"additional relative execution": func(job *workflowJob) {
			job.Steps[len(job.Steps)-1].Run += "\nbash ./scripts/download-crane-modules.sh\n"
		},
		"staging after candidate checkout":         func(job *workflowJob) { job.Steps[1], job.Steps[2] = job.Steps[2], job.Steps[1] },
		"helper checkout after candidate checkout": func(job *workflowJob) { job.Steps[0], job.Steps[2] = job.Steps[2], job.Steps[0] },
		"candidate folder used for staging":        func(job *workflowJob) { job.Steps[0].With["path"] = "." },
		"sparse cone mode":                         func(job *workflowJob) { job.Steps[0].With["sparse-checkout-cone-mode"] = true },
		"persisted credentials":                    func(job *workflowJob) { job.Steps[0].With["persist-credentials"] = true },
	} {
		t.Run(name, func(t *testing.T) {
			var workflow workflowContract
			if err := yaml.Unmarshal(payload, &workflow); err != nil {
				t.Fatal(err)
			}
			job := workflow.Jobs["reproducible-platform-build"]
			mutate(&job)
			if _, err := validateCandidateCraneDownload(job); err == nil {
				t.Fatal("accepted crane helper regression")
			}
		})
	}
}

func validateCraneStagingFailure(stage, workspace string, env []string) error {
	cmd := exec.Command("bash", "-c", stage)
	cmd.Dir, cmd.Env = workspace, env
	if output, err := cmd.CombinedOutput(); err == nil {
		return fmt.Errorf("staging ignored a missing helper: %s", output)
	}
	return nil
}

func TestCraneDownloadWorkflowStagingRejectsIgnoredFailure(t *testing.T) {
	workflow, _ := readWorkflow(t, "../.github/workflows/acceptance-image-build.yml")
	plan, err := validateCandidateCraneDownload(workflow.Jobs["reproducible-platform-build"])
	if err != nil {
		t.Fatal(err)
	}
	// The former curl --fail mutation becomes a staging failure mutation now
	// that actions/checkout owns fetching. A failed copy must stop the job.
	mutated := strings.Replace(plan.stage, "set -euo pipefail", "set -uo pipefail", 1)
	if mutated == plan.stage {
		t.Fatal("staging failure mutation did not change the fixture")
	}
	env := append(os.Environ(), "RUNNER_TEMP="+t.TempDir())
	if validateCraneStagingFailure(mutated, t.TempDir(), env) == nil {
		t.Fatal("accepted staging that ignores a missing helper")
	}
}
