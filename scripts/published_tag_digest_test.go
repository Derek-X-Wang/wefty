package scripts

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const publishedTagHelper = "bash scripts/published-tag-digest.sh"

func TestPublishedTagDigestRetry(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	tag := "ghcr.io/example/image:commit"
	for _, tc := range []struct {
		name, mode string
		attempts   int
		wantError  bool
	}{
		{"404 twice then match", "lag", 3, false},
		{"always 404", "missing", 5, true},
		{"wrong digest fails immediately", "wrong", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			crane := filepath.Join(dir, "crane")
			fake := `#!/usr/bin/env bash
set -euo pipefail
[[ $1 == digest && $2 == "$TEST_TAG" ]]
printf 'read\n' >> "$TEST_CALLS"
attempt=$(wc -l < "$TEST_CALLS")
if [[ $TEST_MODE == wrong ]]; then
  printf 'sha256:%064d\n' 0
elif [[ $TEST_MODE == missing ]] || ((attempt <= 2)); then
  printf '404 MANIFEST_UNKNOWN attempt %d\n' "$attempt" >&2
  exit 1
else
  printf '%s\n' "$TEST_DIGEST"
fi
`
			if err := os.WriteFile(crane, []byte(fake), 0o755); err != nil {
				t.Fatal(err)
			}
			// Record the delay contract without waiting for wall-clock time.
			if err := os.WriteFile(filepath.Join(dir, "sleep"), []byte("#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> \"$TEST_SLEEPS\"\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			calls, sleeps := filepath.Join(dir, "calls"), filepath.Join(dir, "sleeps")
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("TEST_TAG", tag)
			t.Setenv("TEST_DIGEST", digest)
			t.Setenv("TEST_MODE", tc.mode)
			t.Setenv("TEST_CALLS", calls)
			t.Setenv("TEST_SLEEPS", sleeps)
			cmd := exec.Command("bash", "published-tag-digest.sh", "read-tag", crane, tag, digest)
			out, err := cmd.CombinedOutput()
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v, output = %s", err, out)
			}
			if tc.wantError {
				for _, required := range []string{tag, digest} {
					if !strings.Contains(string(out), required) {
						t.Fatalf("failure omits %q: %s", required, out)
					}
				}
				if tc.mode == "missing" && !strings.Contains(string(out), "last error: 404 MANIFEST_UNKNOWN attempt 5") {
					t.Fatalf("failure omits last error: %s", out)
				}
			} else if string(out) != digest+"\n" {
				t.Fatalf("digest output = %q", out)
			}
			readCalls, err := os.ReadFile(calls)
			if err != nil || string(readCalls) != strings.Repeat("read\n", tc.attempts) {
				t.Fatalf("read calls = %q, error = %v; want %d", readCalls, err, tc.attempts)
			}
			sleepCalls, err := os.ReadFile(sleeps)
			if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			if want := strings.Repeat("2\n", tc.attempts-1); string(sleepCalls) != want {
				t.Fatalf("sleep calls = %q, want %q", sleepCalls, want)
			}
		})
	}
}

func TestPublishedTagDigestIndexOutput(t *testing.T) {
	repo := "ghcr.io/example/image"
	digest := "sha256:" + strings.Repeat("a", 64)
	valid := repo + "@" + digest
	for _, output := range []string{
		valid + "\n", "", digest + "\n", "ghcr.io/other/image@" + digest + "\n",
		repo + "@sha256:abc\n", repo + "@sha256:" + strings.Repeat("g", 64) + "\n",
		" " + valid + "\n", valid + " \n", valid + "\n" + valid + "\n",
		"log message\n" + valid + "\n", valid + "\n\n",
	} {
		t.Run(fmt.Sprintf("%q", output), func(t *testing.T) {
			cmd := exec.Command("bash", "published-tag-digest.sh", "index-digest", repo)
			cmd.Stdin = strings.NewReader(output)
			out, err := cmd.CombinedOutput()
			if output == valid+"\n" {
				if err != nil || string(out) != digest+"\n" {
					t.Fatalf("valid output: error = %v, output = %q", err, out)
				}
			} else if err == nil || !strings.Contains(string(out), "invalid crane index append output") {
				t.Fatalf("malformed output accepted: error = %v, output = %q", err, out)
			}
		})
	}
}

func validatePublishedTagReads(job workflowJob, repo, platforms string, variants, resolve bool) error {
	var runs []string
	checkedOut := false
	for _, step := range job.Steps {
		if strings.HasPrefix(step.Uses, "actions/checkout@") && step.If == "" {
			path, _ := step.With["path"].(string)
			checkedOut = path == "" || path == "."
		}
		if strings.Contains(step.Run, publishedTagHelper) && !checkedOut {
			return fmt.Errorf("publish helper requires a prior checkout in the default workspace")
		}
		runs = append(runs, step.Run)
	}
	text := strings.Join(runs, "\n")
	if regexp.MustCompile(`(?:"?\$\{?crane\}?"?|crane)\s+digest\b`).MatchString(text) {
		return fmt.Errorf("publish job contains a direct crane digest read")
	}
	read := publishedTagHelper + ` read-tag "$crane" `
	indexCount, readCount := 1, 2
	if resolve {
		readCount++
	}
	if variants {
		indexCount++
		readCount += 2
	}
	if strings.Count(text, publishedTagHelper+" index-digest ") != indexCount || strings.Count(text, read) != readCount ||
		strings.Count(text, `index_digest="$("$crane" index append \`) != indexCount {
		return fmt.Errorf("publish job must parse each index push and use bounded tag reads")
	}
	tags := []string{`"$` + repo + `:$GITHUB_SHA"`}
	archTags := []string{`"$` + repo + `:${GITHUB_SHA}-${arch}"`}
	files := []string{`"$(cat "$RUNNER_TEMP/` + platforms + `/$arch.digest")"`}
	if variants {
		tags = append(tags, `"$IMAGE_NAME:${GITHUB_SHA}-user-${variant}"`)
		archTags = append(archTags, `"$IMAGE_NAME:${GITHUB_SHA}-user-${variant}-${arch}"`)
		files = append(files, `"$(cat "$RUNNER_TEMP/platforms/${arch}-user-${variant}.digest")"`)
	}
	for i, tag := range tags {
		for _, required := range []string{
			read + archTags[i] + ` ` + files[i] + ` >/dev/null`,
			`-t ` + tag + ` | ` + publishedTagHelper + ` index-digest "$` + repo + `")"` + "\n",
			read + tag + ` "$index_digest" >/dev/null`,
		} {
			if !strings.Contains(text, required) {
				return fmt.Errorf("publish job is missing %s", required)
			}
		}
	}
	if resolve && (!strings.Contains(text, `index_digest="$(`+read+tags[0]+` "$ACTION_DIGEST")"`) ||
		!strings.Contains(text, `test "$index_digest" = "$ACTION_DIGEST"`)) {
		return fmt.Errorf("published tag must still resolve to ACTION_DIGEST")
	}
	return nil
}

func TestAcceptanceImagePublishedTagReads(t *testing.T) {
	workflow, _ := readWorkflow(t, "../.github/workflows/acceptance-image.yml")
	seats, _ := readWorkflow(t, "../.github/workflows/computer-seats.yml")
	for _, tc := range []struct {
		workflow             workflowContract
		job, repo, platforms string
		variants, resolve    bool
	}{
		{workflow, "publish", "IMAGE_NAME", "platforms", true, true},
		{workflow, "publish-reference-computer", "COMPUTER_IMAGE_NAME", "computer-platforms", false, true},
		{workflow, "publish-wayland-reference-computer", "WAYLAND_COMPUTER_IMAGE_NAME", "wayland-computer-platforms", false, true},
		{seats, "publish-default-computer", "DEFAULT_COMPUTER_IMAGE", "default-platforms", false, false},
	} {
		t.Run(tc.job, func(t *testing.T) {
			job := tc.workflow.Jobs[tc.job]
			if err := validatePublishedTagReads(job, tc.repo, tc.platforms, tc.variants, tc.resolve); err != nil {
				t.Fatal(err)
			}
			mutations := []struct{ from, to string }{
				{publishedTagHelper + ` read-tag "$crane"`, `"$crane" digest`},
				{publishedTagHelper + " index-digest ", "cat "},
				{` "$index_digest" >/dev/null`, ` "sha256:wrong" >/dev/null`},
				{`"$(cat "$RUNNER_TEMP/` + tc.platforms + `/$arch.digest")" >/dev/null`, `"sha256:wrong" >/dev/null`},
			}
			if tc.resolve {
				mutations = append(mutations, struct{ from, to string }{` "$ACTION_DIGEST")"`, ` "sha256:wrong")"`},
					struct{ from, to string }{`test "$index_digest" = "$ACTION_DIGEST"`, "true"})
			}
			for _, mutation := range mutations {
				candidate := job
				candidate.Steps = append([]workflowStep(nil), job.Steps...)
				changed := false
				for i := range candidate.Steps {
					old := candidate.Steps[i].Run
					candidate.Steps[i].Run = strings.ReplaceAll(old, mutation.from, mutation.to)
					changed = changed || old != candidate.Steps[i].Run
				}
				if !changed || validatePublishedTagReads(candidate, tc.repo, tc.platforms, tc.variants, tc.resolve) == nil {
					t.Fatalf("contract accepted mutation %q -> %q (applied=%v)", mutation.from, mutation.to, changed)
				}
			}
			candidate := job
			candidate.Steps = append([]workflowStep(nil), job.Steps...)
			for i := range candidate.Steps {
				if strings.HasPrefix(candidate.Steps[i].Uses, "actions/checkout@") {
					candidate.Steps[i].With = map[string]any{"path": "elsewhere"}
				}
			}
			if validatePublishedTagReads(candidate, tc.repo, tc.platforms, tc.variants, tc.resolve) == nil {
				t.Fatal("contract accepted checkout outside the default workspace")
			}
		})
	}
}
