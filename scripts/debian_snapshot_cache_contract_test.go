package scripts

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const snapshotCacheAction = "0057852bfaa89a56745cba8c7296529d2fc39830"

const snapshotXvfbProbe = "pool/main/x/xorg-server/xvfb_21.1.7-3%2bdeb12u12"
const snapshotWayvncProbe = "pool/main/w/wayvnc/wayvnc_0.9.1-1"
const snapshotVariantProbe = "${{ matrix.variant == 'wayland' && '" + snapshotWayvncProbe + "' || '" + snapshotXvfbProbe + "' }}"

type snapshotCacheStep struct {
	workflowStep `yaml:",inline"`
	ID           string `yaml:"id"`
}

type snapshotCacheJob struct {
	Steps    []snapshotCacheStep `yaml:"steps"`
	Strategy struct {
		Matrix struct {
			Include []map[string]string `yaml:"include"`
		} `yaml:"matrix"`
	} `yaml:"strategy"`
}

func snapshotCacheJobs(t *testing.T, path string) map[string]snapshotCacheJob {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]snapshotCacheJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(payload, &workflow); err != nil {
		t.Fatal(err)
	}
	return workflow.Jobs
}

func validateSnapshotCacheJob(name string, job snapshotCacheJob, wantBuilds int) error {
	restore, start, summary, save, firstBuild, lastBuild, runtimeStep, snapshotBuilds := -1, -1, -1, -1, -1, -1, -1, 0
	var restoreKey, saveKey, prefix string
	hostNetwork := false
	fileFlag := regexp.MustCompile(`--file\s+("[^"]+"|[^\s]+)`)
	for i, step := range job.Steps {
		switch step.Uses {
		case "actions/cache/restore@" + snapshotCacheAction:
			restore = i
			restoreKey, _ = step.With["key"].(string)
			prefix, _ = step.With["restore-keys"].(string)
			if step.With["path"] != "${{ runner.temp }}/debian-snapshot-cache" || step.If != "" {
				return fmt.Errorf("restore must run unconditionally into the proxy cache directory")
			}
		case "actions/cache/save@" + snapshotCacheAction:
			save = i
			saveKey, _ = step.With["key"].(string)
			if step.With["path"] != "${{ runner.temp }}/debian-snapshot-cache" || step.If != "always() && steps.debian-cache-summary.outputs.stored != '' && steps.debian-cache-summary.outputs.stored != '0'" {
				return fmt.Errorf("save must retain partial fills after failure, only when stored > 0")
			}
		}
		if strings.HasPrefix(step.Uses, "docker/setup-buildx-action@") {
			opts, _ := step.With["driver-opts"].(string)
			hostNetwork = strings.Contains(opts, "\nnetwork=host\n")
		}
		if strings.TrimSpace(step.Run) == `bash scripts/debian-snapshot-cache.sh start "${PROBE_PACKAGE}_${ARCH}.deb"` {
			start = i
			if step.If != "" || step.Env["ARCH"] != "${{ matrix.arch }}" {
				return fmt.Errorf("proxy startup and real package health check must run for this arch")
			}
			probe := snapshotXvfbProbe
			if name == "reference-computer-platform-build" {
				probe = snapshotVariantProbe
			}
			if step.Env["PROBE_PACKAGE"] != probe {
				return fmt.Errorf("health probe must fetch a pinned package used by this build")
			}
		}
		if strings.TrimSpace(step.Run) == "bash scripts/debian-snapshot-cache.sh stop" {
			summary = i
			if step.ID != "debian-cache-summary" || step.If != "always()" {
				return fmt.Errorf("proxy summary must expose stored after every outcome")
			}
		}
		if strings.Contains(step.Run, "scripts/test-computer-image-runtime.sh ") {
			runtimeStep = i
			if step.Env["WEFTY_DEBIAN_SNAPSHOT_PROXY"] != "${{ matrix.variant == 'wayland' && env.DEBIAN_CACHE_PROXY || '' }}" {
				return fmt.Errorf("Wayland runtime fixtures must receive this leg's snapshot proxy")
			}
		}
		for _, line := range strings.Split(strings.ReplaceAll(step.Run, "\\\n", " "), "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "docker buildx build ") {
				continue
			}
			if firstBuild < 0 {
				firstBuild = i
			}
			lastBuild = i
			file := fileFlag.FindStringSubmatch(line)
			if len(file) != 2 {
				return fmt.Errorf("build must name its Dockerfile: %s", line)
			}
			files := []string{strings.Trim(file[1], `"`)}
			if files[0] == "$DOCKERFILE" {
				if step.Env["DOCKERFILE"] != "${{ matrix.dockerfile }}" || len(job.Strategy.Matrix.Include) != 4 {
					return fmt.Errorf("dynamic Dockerfile must resolve through all four desktop matrix rows")
				}
				files = nil
				seen := map[string]bool{}
				for _, row := range job.Strategy.Matrix.Include {
					key := row["arch"] + "/" + row["variant"]
					expected := map[string]string{"xfce": "examples/computer/Dockerfile", "wayland": "examples/computer-wayland/Dockerfile"}[row["variant"]]
					if (row["arch"] != "amd64" && row["arch"] != "arm64") || expected == "" || row["dockerfile"] != expected || seen[key] {
						return fmt.Errorf("desktop matrix must cover each variant and architecture exactly once")
					}
					seen[key] = true
					files = append(files, row["dockerfile"])
				}
			}
			usesSnapshot := false
			for _, file := range files {
				payload, err := os.ReadFile("../" + file)
				if err != nil {
					return err
				}
				usesSnapshot = usesSnapshot || strings.Contains(string(payload), "snapshot.debian.org")
			}
			if !usesSnapshot {
				continue
			}
			snapshotBuilds++
			for _, arg := range []string{`--build-arg http_proxy="$DEBIAN_CACHE_PROXY"`, `--build-arg HTTP_PROXY="$DEBIAN_CACHE_PROXY"`, "--no-cache", "--provenance=false", "--sbom=false", "--build-arg SOURCE_DATE_EPOCH=0", "rewrite-timestamp=true"} {
				if !strings.Contains(line, arg) {
					return fmt.Errorf("snapshot build lacks %s", arg)
				}
			}
		}
	}
	if wantBuilds == 0 {
		if snapshotBuilds != 0 || restore != -1 || start != -1 || summary != -1 || save != -1 {
			return fmt.Errorf("echo-only builds must not acquire a Debian snapshot dependency")
		}
		return nil
	}
	if !hostNetwork || !(restore >= 0 && restore < start && start < firstBuild && lastBuild < summary && summary < save) {
		return fmt.Errorf("cache lifecycle must bracket all builds with a reachable host-network builder: %d %d %d %d %d %d", restore, start, firstBuild, lastBuild, summary, save)
	}
	if snapshotBuilds != wantBuilds {
		return fmt.Errorf("snapshot build count = %d, want %d", snapshotBuilds, wantBuilds)
	}
	if name == "reference-computer-platform-build" && !(lastBuild < runtimeStep && runtimeStep < summary) {
		return fmt.Errorf("reference proxy must stay running through runtime fixture builds, then stop and save")
	}
	if !strings.HasPrefix(prefix, "debian-snapshot-v1-"+name+"-${{ matrix.arch }}-") || !strings.Contains(prefix, "20260827T000000Z-${{ hashFiles(") || !strings.HasSuffix(prefix, " }}-") || restoreKey != prefix+"${{ github.run_id }}-${{ github.run_attempt }}" || saveKey != restoreKey {
		return fmt.Errorf("cache keys must separate job/arch, snapshot and Dockerfiles, restoring latest and saving a new run entry")
	}
	files := map[string][]string{
		"default-computer-seats":            {"examples/computer/Dockerfile"},
		"reproducible-platform-build":       {"examples/oci-echo-service/Dockerfile", "examples/oci-echo-service/Dockerfile.service-users"},
		"reference-computer-platform-build": {"examples/computer/Dockerfile", "examples/computer-wayland/Dockerfile", "examples/computer/fixtures/Dockerfile.wayland-text"},
	}[name]
	for _, file := range files {
		if !strings.Contains(prefix, "'"+file+"'") {
			return fmt.Errorf("cache key lacks Dockerfile hash input %s", file)
		}
	}
	if name == "reference-computer-platform-build" && !strings.Contains(prefix, "${{ matrix.variant }}-") {
		return fmt.Errorf("desktop caches must separate variants")
	}
	return nil
}

func TestDebianSnapshotCacheWorkflowWiring(t *testing.T) {
	for _, plan := range []struct {
		path, job string
		builds    int
	}{
		{"computer-seats.yml", "default-computer-seats", 2},
		// This job builds only Alpine/BusyBox echo-service images, so there are no snapshot build args.
		{"acceptance-image-build.yml", "reproducible-platform-build", 0},
		{"acceptance-image-build.yml", "reference-computer-platform-build", 2},
		{"acceptance-image.yml", "reproducible-platform-build", 0},
		{"acceptance-image.yml", "reference-computer-platform-build", 2},
	} {
		t.Run(plan.path+"/"+plan.job, func(t *testing.T) {
			job := snapshotCacheJobs(t, "../.github/workflows/"+plan.path)[plan.job]
			if err := validateSnapshotCacheJob(plan.job, job, plan.builds); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDebianSnapshotCacheRejectsWorkflowRegressions(t *testing.T) {
	for name, mutate := range map[string]func(*snapshotCacheJob){
		"missing proxy argument": func(job *snapshotCacheJob) {
			for i := range job.Steps {
				job.Steps[i].Run = strings.Replace(job.Steps[i].Run, `--build-arg http_proxy="$DEBIAN_CACHE_PROXY"`, "", 1)
			}
		},
		"restore after build": func(job *snapshotCacheJob) {
			for i := range job.Steps {
				if strings.HasPrefix(job.Steps[i].Uses, "actions/cache/restore@") {
					job.Steps[i], job.Steps[i+2] = job.Steps[i+2], job.Steps[i]
					break
				}
			}
		},
		"missing start": func(job *snapshotCacheJob) {
			for i := range job.Steps {
				if strings.Contains(job.Steps[i].Run, "scripts/debian-snapshot-cache.sh start ") {
					job.Steps[i].Run = "true"
				}
			}
		},
		"unused health package": func(job *snapshotCacheJob) {
			for i := range job.Steps {
				if strings.Contains(job.Steps[i].Run, "scripts/debian-snapshot-cache.sh start ") {
					job.Steps[i].Env["PROBE_PACKAGE"] = snapshotWayvncProbe
				}
			}
		},
		"save only on success": func(job *snapshotCacheJob) {
			for i := range job.Steps {
				if strings.HasPrefix(job.Steps[i].Uses, "actions/cache/save@") {
					job.Steps[i].If = "success()"
				}
			}
		},
		"save on unfilled misses": func(job *snapshotCacheJob) {
			for i := range job.Steps {
				if strings.HasPrefix(job.Steps[i].Uses, "actions/cache/save@") {
					job.Steps[i].If = strings.ReplaceAll(job.Steps[i].If, "outputs.stored", "outputs.misses")
				}
			}
		},
		"cache shared across arch": func(job *snapshotCacheJob) {
			for i := range job.Steps {
				if strings.HasPrefix(job.Steps[i].Uses, "actions/cache/") {
					for key, value := range job.Steps[i].With {
						if text, ok := value.(string); ok {
							job.Steps[i].With[key] = strings.ReplaceAll(text, "${{ matrix.arch }}-", "")
						}
					}
				}
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			job := snapshotCacheJobs(t, "../.github/workflows/computer-seats.yml")["default-computer-seats"]
			mutate(&job)
			if err := validateSnapshotCacheJob("default-computer-seats", job, 2); err == nil {
				t.Fatal("accepted workflow cache regression")
			}
		})
	}
}

func TestDebianSnapshotCacheFixtureLifecycleRegressions(t *testing.T) {
	for _, workflow := range []string{"acceptance-image.yml", "acceptance-image-build.yml"} {
		for _, mutation := range []string{"missing runtime proxy", "early stop", "missing fixture hash"} {
			t.Run(workflow+"/"+mutation, func(t *testing.T) {
				job := snapshotCacheJobs(t, "../.github/workflows/"+workflow)["reference-computer-platform-build"]
				runtimeStep, stop := -1, -1
				for i := range job.Steps {
					if strings.Contains(job.Steps[i].Run, "scripts/test-computer-image-runtime.sh ") {
						runtimeStep = i
					}
					if job.Steps[i].ID == "debian-cache-summary" {
						stop = i
					}
				}
				if runtimeStep < 0 || stop < 0 {
					t.Fatal("missing runtime or stop step")
				}
				switch mutation {
				case "missing runtime proxy":
					delete(job.Steps[runtimeStep].Env, "WEFTY_DEBIAN_SNAPSHOT_PROXY")
				case "early stop":
					job.Steps[runtimeStep], job.Steps[stop] = job.Steps[stop], job.Steps[runtimeStep]
				case "missing fixture hash":
					for i := range job.Steps {
						for key, value := range job.Steps[i].With {
							if value, ok := value.(string); ok {
								job.Steps[i].With[key] = strings.ReplaceAll(value, ", 'examples/computer/fixtures/Dockerfile.wayland-text'", "")
							}
						}
					}
				}
				if err := validateSnapshotCacheJob("reference-computer-platform-build", job, 2); err == nil {
					t.Fatal("accepted fixture cache regression")
				}
			})
		}
	}
}

func TestDebianSnapshotCacheFixtureBuildArguments(t *testing.T) {
	payload, err := os.ReadFile("test-computer-image-runtime.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(payload)
	start := strings.Index(script, "  local -a fixture_build_args=")
	if start < 0 {
		t.Fatal("fixture build arguments not found")
	}
	end := strings.Index(script[start:], ".; then\n")
	if end < 0 {
		t.Fatal("fixture build command not found")
	}
	block := strings.Replace(script[start:start+end+1], "if ! docker build", "docker build", 1)
	for _, proxy := range []string{"", "http://172.17.0.1:43123"} {
		t.Run(proxy, func(t *testing.T) {
			capture := t.TempDir() + "/argv"
			command := exec.Command("bash", "-c", `set -euo pipefail
docker() { printf '%s\n' "$@" > "$CAPTURE"; }
exercise() {
  arch=arm64
  fixture_dockerfile=examples/computer/fixtures/Dockerfile.wayland-text
  image=reference@sha256:example
  mutation=text-frames-accepted
  tag=wefty-computer-broken-text-frames-accepted:local
`+block+"\n}\nexercise\n")
			// Execute the real argument assembly with a shell-function Docker stub.
			for _, value := range os.Environ() {
				if !strings.HasPrefix(value, "WEFTY_DEBIAN_SNAPSHOT_PROXY=") {
					command.Env = append(command.Env, value)
				}
			}
			command.Env = append(command.Env, "CAPTURE="+capture)
			if proxy != "" {
				command.Env = append(command.Env, "WEFTY_DEBIAN_SNAPSHOT_PROXY="+proxy)
			}
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("fixture argument assembly: %v\n%s", err, output)
			}
			want := []string{"build", "--platform", "linux/arm64", "--file", "examples/computer/fixtures/Dockerfile.wayland-text"}
			if proxy != "" {
				want = append(want, "--build-arg", "http_proxy="+proxy, "--build-arg", "HTTP_PROXY="+proxy)
			}
			want = append(want, "--build-arg", "BASE_IMAGE=reference@sha256:example", "--build-arg", "MUTATION=text-frames-accepted", "--tag", "wefty-computer-broken-text-frames-accepted:local", ".")
			got, err := os.ReadFile(capture)
			if err != nil || string(got) != strings.Join(want, "\n")+"\n" {
				t.Fatalf("fixture args = %q, want %q: %v", got, want, err)
			}
		})
	}
}

func validateSnapshotAptIntegrity(payload string) error {
	insecure := regexp.MustCompile(`(?i)trusted\s*=\s*(yes|true|1)|allow-?insecure-?repositories|allow-?unauthenticated|allow-insecure\s*=|allow-downgrade-to-insecure\s*=`)
	if insecure.MatchString(payload) {
		return fmt.Errorf("snapshot cache must preserve apt signature authentication")
	}
	snapshotURLs := regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://snapshot\.debian\.org[^\s'"\\]*`).FindAllString(payload, -1)
	if len(snapshotURLs) == 0 {
		return fmt.Errorf("snapshot sources missing")
	}
	for _, url := range snapshotURLs {
		if url != "http://snapshot.debian.org/archive/debian/20260827T000000Z" && !strings.HasPrefix(url, "http://snapshot.debian.org/archive/debian/20260827T000000Z/") {
			return fmt.Errorf("snapshot fetch must use the pinned HTTP URL: %s", url)
		}
	}
	return nil
}

func TestDebianSnapshotCacheAptIntegrity(t *testing.T) {
	for _, file := range []string{"examples/computer/Dockerfile", "examples/computer-wayland/Dockerfile", "examples/computer/fixtures/Dockerfile.wayland-text"} {
		payload, err := os.ReadFile("../" + file)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateSnapshotAptIntegrity(string(payload)); err != nil {
			t.Errorf("%s: %v", file, err)
		}
		if strings.Contains(string(payload), "https://snapshot.debian.org/") {
			t.Errorf("%s has a snapshot fetch that bypasses the HTTP cache", file)
		}
	}
	for _, bypass := range []string{"deb [trusted=yes] http://snapshot.debian.org", "-o Acquire::AllowInsecureRepositories=true", "-o APT::Get::AllowUnauthenticated=true", "--allow-unauthenticated", "deb [allow-insecure=yes] http://snapshot.debian.org"} {
		if err := validateSnapshotAptIntegrity(bypass); err == nil {
			t.Errorf("accepted apt authentication bypass %q", bypass)
		}
	}
	// The probe packages must be actual pinned installs, not extra cache-only downloads.
	assertFileContains(t, "../examples/computer/Dockerfile", "xvfb=2:21.1.7-3+deb12u12")
	assertFileContains(t, "../examples/computer-wayland/Dockerfile", "wayvnc=0.9.1-1")
	assertFileContains(t, "../examples/computer-wayland/Dockerfile",
		"      '46d689818d88e0d65387bf9182f1bd1015c4b28e0347696f0bec7c7d9c829194' \\\n      /tmp/neatvnc.tar.xz | sha256sum --check -")
	assertFileContains(t, "../examples/computer/fixtures/Dockerfile.wayland-text",
		"    && printf '%s  %s\\n' 46d689818d88e0d65387bf9182f1bd1015c4b28e0347696f0bec7c7d9c829194 /tmp/neatvnc.tar.xz | sha256sum --check -")
	for _, url := range []string{"https://snapshot.debian.org/archive/debian/20260827T000000Z/pool/file.deb", "ftp://snapshot.debian.org/archive/debian/20260827T000000Z/pool/file.deb", "http://snapshot.debian.org/archive/debian/latest/pool/file.deb"} {
		if err := validateSnapshotAptIntegrity(url); err == nil {
			t.Errorf("accepted snapshot URL %s", url)
		}
	}
}

func TestDebianSnapshotCacheProxy(t *testing.T) {
	command := exec.Command("python3", "test-debian-snapshot-cache-proxy.py")
	command.Env = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Debian snapshot proxy: %v\n%s", err, output)
	}
}
