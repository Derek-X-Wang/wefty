package scripts

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The Wayland image build takes a verified, cached mise binary from CI, so a
// release-host outage does not fail it (#1575). The Dockerfile still
// downloads it when no copy is supplied, and checks it against the pinned
// SHA-256 either way.

func TestWaylandDockerfileVerifiesSuppliedOrDownloadedMise(t *testing.T) {
	text := string(mustReadFile(t, "../examples/computer-wayland/Dockerfile"))
	for _, want := range []string{
		"FROM scratch AS mise-release",
		"RUN --mount=type=bind,from=mise-release,target=/tmp/mise-release",
		"if [ -f /tmp/mise-release/mise ]; then",
		"cp /tmp/mise-release/mise /tmp/mise;",
		`"https://github.com/jdx/mise/releases/download/v${MISE_VERSION}/mise-v${MISE_VERSION}-linux-${mise_arch}"`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("Wayland Dockerfile is missing %q", want)
		}
	}
	// The SHA-256 check follows both branches, so neither path skips it.
	check := `fi \` + "\n" + `    && printf '%s  %s\n' "$mise_sha" /tmp/mise | sha256sum --check - \`
	if !strings.Contains(text, check) {
		t.Fatal("Wayland Dockerfile must check mise against the pinned SHA-256 after both the supplied and the downloaded path")
	}
	for _, arg := range []string{"MISE_VERSION", "MISE_SHA256_AMD64", "MISE_SHA256_ARM64"} {
		if got := len(regexp.MustCompile(`(?m)^ARG `+arg+`=\S+$`).FindAllString(text, -1)); got != 1 {
			t.Fatalf("Wayland Dockerfile has %d pinned 'ARG %s=' lines, want 1", got, arg)
		}
		if !regexp.MustCompile(`(?m)^ARG ` + arg + `$`).MatchString(text) {
			t.Fatalf("the neatvnc-build stage must declare ARG %s", arg)
		}
	}
}

func TestWaylandImageWorkflowsSupplyTheCachedMise(t *testing.T) {
	for _, path := range []string{"../.github/workflows/acceptance-image.yml", "../.github/workflows/acceptance-image-build.yml"} {
		workflow, _ := readWorkflow(t, path)
		job, ok := workflow.Jobs["reference-computer-platform-build"]
		if !ok {
			t.Fatalf("%s has no reference-computer-platform-build job", path)
		}
		index := map[string]int{}
		for i, step := range job.Steps {
			index[step.Name] = i
		}
		order := []string{
			"Read the pinned mise release",
			"Restore the verified mise binary",
			"Fetch and verify the pinned mise binary",
			"Save the verified mise binary",
			"Build the reference desktop into its execution shape",
		}
		for i, name := range order {
			at, ok := index[name]
			if !ok {
				t.Fatalf("%s is missing step %q", path, name)
			}
			if i > 0 && at <= index[order[i-1]] {
				t.Fatalf("%s: step %q must follow %q", path, name, order[i-1])
			}
		}
		pin := job.Steps[index[order[0]]]
		restore := job.Steps[index[order[1]]]
		fetch := job.Steps[index[order[2]]]
		save := job.Steps[index[order[3]]]
		build := job.Steps[index[order[4]]]
		for _, step := range []workflowStep{pin, restore, fetch} {
			if step.If != "matrix.variant == 'wayland'" {
				t.Fatalf("%s: step %q runs for %q, want only the Wayland variant", path, step.Name, step.If)
			}
		}
		if !strings.Contains(pin.Run, `scripts/fetch-mise-release.sh pin "$ARCH"`) {
			t.Fatalf("%s: the pin step must read the pin from the Dockerfile through the fetch script", path)
		}
		if !strings.Contains(fetch.Run, `scripts/fetch-mise-release.sh fetch "$ARCH" "$RUNNER_TEMP/mise-release"`) {
			t.Fatalf("%s: the fetch step must verify into $RUNNER_TEMP/mise-release", path)
		}
		// Save only a fresh copy; the default success() condition keeps an unverified file out.
		if save.If != "matrix.variant == 'wayland' && steps.mise-cache.outputs.cache-hit != 'true'" {
			t.Fatalf("%s: the save step condition is %q", path, save.If)
		}
		key := "mise-release-v1-linux-${{ matrix.arch }}-${{ steps.mise-pin.outputs.version }}-${{ steps.mise-pin.outputs.sha256 }}"
		for _, step := range []workflowStep{restore, save} {
			if step.With["key"] != key || step.With["path"] != "${{ runner.temp }}/mise-release" {
				t.Fatalf("%s: step %q must use key %q and path ${{ runner.temp }}/mise-release, got %v", path, step.Name, key, step.With)
			}
		}
		if !strings.Contains(build.Run, `wayland) variant_flags=(--build-context "mise-release=$RUNNER_TEMP/mise-release") ;;`) ||
			strings.Count(build.Run, `"${variant_flags[@]}"`) != 2 {
			t.Fatalf("%s: both Wayland solves must take the verified mise as the mise-release build context", path)
		}
	}
	build, _ := readWorkflow(t, "../.github/workflows/acceptance-image-build.yml")
	if !slices.Contains(strings.Fields(build.Env["IMAGE_TEST_INTERFACE_PATHS"]), "scripts/fetch-mise-release.sh") {
		t.Fatal("a candidate without scripts/fetch-mise-release.sh must fail the interface check with its merge-main message")
	}
}

func TestFetchMiseReleasePinReadsTheDockerfile(t *testing.T) {
	text := string(mustReadFile(t, "../examples/computer-wayland/Dockerfile"))
	arg := func(name string) string {
		return regexp.MustCompile(`(?m)^ARG ` + name + `=(\S+)$`).FindStringSubmatch(text)[1]
	}
	for arch, shaArg := range map[string]string{"amd64": "MISE_SHA256_AMD64", "arm64": "MISE_SHA256_ARM64"} {
		out := filepath.Join(t.TempDir(), "output")
		cmd := exec.Command("bash", "fetch-mise-release.sh", "pin", arch)
		cmd.Env = append(os.Environ(), "GITHUB_OUTPUT="+out)
		if payload, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("pin %s: %v\n%s", arch, err, payload)
		}
		want := "version=" + arg("MISE_VERSION") + "\nsha256=" + arg(shaArg) + "\n"
		if got := string(mustReadFile(t, out)); got != want {
			t.Fatalf("pin %s wrote %q, want %q", arch, got, want)
		}
	}
}

func TestFetchMiseReleaseChecksEveryPath(t *testing.T) {
	if _, err := exec.LookPath("sha256sum"); err != nil {
		t.Skip("sha256sum is not installed")
	}
	good := []byte("pinned mise payload\n")
	sum := sha256.Sum256(good)
	pinned := hex.EncodeToString(sum[:])

	// A copy of the script in a fake tree, so the pin can match a test payload.
	root := t.TempDir()
	for _, dir := range []string{"scripts", "examples/computer-wayland", "bin"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	writeFile := func(path string, payload []byte, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(path, payload, mode); err != nil {
			t.Fatal(err)
		}
	}
	writeFile(filepath.Join(root, "scripts/fetch-mise-release.sh"), mustReadFile(t, "fetch-mise-release.sh"), 0o755)
	writeFile(filepath.Join(root, "examples/computer-wayland/Dockerfile"), []byte(
		"ARG MISE_VERSION=2026.8.14\nARG MISE_SHA256_AMD64="+pinned+"\nARG MISE_SHA256_ARM64="+pinned+"\n"), 0o644)
	body := filepath.Join(root, "curl-body")
	calls := filepath.Join(root, "curl-calls")
	fail := filepath.Join(root, "curl-fails") // when present, the stub writes the body and then fails
	// The stub records each call and writes curl-body to the -o path.
	writeFile(filepath.Join(root, "bin/curl"), []byte(`#!/usr/bin/env bash
echo "$*" >> "`+calls+`"
while [ $# -gt 0 ]; do
  if [ "$1" = -o ]; then
    cp "`+body+`" "$2"
    if [ -e "`+fail+`" ]; then exit 22; fi
    exit 0
  fi
  shift
done
exit 99
`), 0o755)

	run := func(dir string) (string, error) {
		cmd := exec.Command("bash", filepath.Join(root, "scripts/fetch-mise-release.sh"), "fetch", "arm64", dir)
		cmd.Env = append(os.Environ(), "PATH="+filepath.Join(root, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
		payload, err := cmd.CombinedOutput()
		return string(payload), err
	}
	curlCalls := func() int {
		payload, err := os.ReadFile(calls)
		if os.IsNotExist(err) {
			return 0
		}
		if err != nil {
			t.Fatal(err)
		}
		return strings.Count(string(payload), "\n")
	}

	t.Run("miss downloads and keeps a verified copy", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "mise-release")
		writeFile(body, good, 0o644)
		before := curlCalls()
		if out, err := run(dir); err != nil {
			t.Fatalf("fetch: %v\n%s", err, out)
		}
		if curlCalls() != before+1 {
			t.Fatal("a cache miss must download once")
		}
		lines := strings.Split(strings.TrimSpace(string(mustReadFile(t, calls))), "\n")
		if !strings.Contains(lines[len(lines)-1], "https://github.com/jdx/mise/releases/download/v2026.8.14/mise-v2026.8.14-linux-arm64") {
			t.Fatalf("download URL: %s", lines[len(lines)-1])
		}
		if got := mustReadFile(t, filepath.Join(dir, "mise")); string(got) != string(good) {
			t.Fatal("the verified download was not kept")
		}
		// A second run is a cache hit: no download.
		out, err := run(dir)
		if err != nil || curlCalls() != before+1 || !strings.Contains(out, "using the cached mise 2026.8.14 (arm64); no download") {
			t.Fatalf("cache hit: err=%v calls=%d\n%s", err, curlCalls()-before, out)
		}
	})
	t.Run("miss removes a download that fails the check", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "mise-release")
		writeFile(body, []byte("tampered\n"), 0o644)
		if out, err := run(dir); err == nil {
			t.Fatalf("a wrong download passed\n%s", out)
		}
		if _, err := os.Stat(filepath.Join(dir, "mise")); !os.IsNotExist(err) {
			t.Fatalf("a failed download stayed in the cache folder: %v", err)
		}
	})
	t.Run("miss removes partial bytes from a failed transfer", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "mise-release")
		writeFile(body, good[:5], 0o644)
		writeFile(fail, nil, 0o644)
		defer os.Remove(fail)
		if out, err := run(dir); err == nil || !strings.Contains(out, "download failed") {
			t.Fatalf("a failed transfer passed: err=%v\n%s", err, out)
		}
		if _, err := os.Stat(filepath.Join(dir, "mise")); !os.IsNotExist(err) {
			t.Fatalf("partial bytes stayed in the cache folder: %v", err)
		}
	})
	t.Run("hit with a changed byte fails without a download", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "mise-release")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		changed := slices.Clone(good)
		changed[0] ^= 1
		writeFile(filepath.Join(dir, "mise"), changed, 0o755)
		before := curlCalls()
		out, err := run(dir)
		if err == nil || !strings.Contains(out, "checksum mismatch") {
			t.Fatalf("a changed cached copy passed: err=%v\n%s", err, out)
		}
		if curlCalls() != before {
			t.Fatal("a cache hit must not download")
		}
	})
}
