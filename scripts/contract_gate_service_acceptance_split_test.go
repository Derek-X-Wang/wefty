package scripts

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const serviceAcceptanceModule = "github.com/Derek-X-Wang/wefty"

// The service acceptance suite decides when the contract gate ends (#1035),
// so each OS divides it into legs by package. Hosted macOS runs "cmd"
// (cmd/wefty) and "rest"; ubuntu-latest runs "cmd", "l1" and "rest". A "rest"
// leg lists every package that the same tags list and skips the packages that
// have their own leg on that OS, so a new package lands in "rest" by default.
// The test runs each leg's real step script with a stub go command against the
// real package list, and requires that each OS runs every package exactly once.
func TestContractGateServiceAcceptanceLegsRunEveryPackageOnce(t *testing.T) {
	_, payload := readWorkflow(t, "../.github/workflows/contract-gate.yml")
	list := exec.Command("go", "list", "-tags=service_acceptance", "./...")
	list.Dir = ".."
	listed, err := list.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	packages := strings.Fields(string(listed))
	if !slices.Contains(packages, serviceAcceptanceModule+"/cmd/wefty") || !slices.Contains(packages, serviceAcceptanceModule+"/l1") {
		t.Fatalf("go list does not name cmd/wefty and l1: %q", packages)
	}
	listFile := t.TempDir() + "/packages"
	if err := os.WriteFile(listFile, listed, 0o600); err != nil {
		t.Fatal(err)
	}
	// runLeg runs the step script as GitHub Actions does (bash -e) with a
	// stub go: "go list -tags=service_acceptance ./..." prints the real list
	// (and then fails, when listFails is set), any other list fails, and "go test"
	// prints its arguments one per line.
	runLeg := func(script, shard, skip string, listFails bool) ([]string, error) {
		stub := `go() {
  if [ "$1" = list ]; then
    if [ "$*" != "list -tags=service_acceptance ./..." ]; then return 1; fi
    cat "$PACKAGE_LIST"
    if [ -n "$LIST_FAILS" ]; then return 1; fi
    return
  fi
  printf '%s\n' "$@"
}
`
		cmd := exec.Command("bash", "-e", "-c", stub+script)
		cmd.Env = append(os.Environ(), "SHARD="+shard, "SKIP="+skip, "PACKAGE_LIST="+listFile)
		if listFails {
			cmd.Env = append(cmd.Env, "LIST_FAILS=1")
		}
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("%w: %s", err, stderr.String())
		}
		return strings.Fields(string(out)), nil
	}
	validate := func(payload []byte) error {
		var workflow struct {
			Jobs map[string]struct {
				Name     string   `yaml:"name"`
				Needs    []string `yaml:"needs"`
				Strategy struct {
					FailFast *bool `yaml:"fail-fast"`
					Matrix   struct {
						Include []map[string]string `yaml:"include"`
					} `yaml:"matrix"`
				} `yaml:"strategy"`
				Steps []workflowStep `yaml:"steps"`
			} `yaml:"jobs"`
		}
		if err := yaml.Unmarshal(payload, &workflow); err != nil {
			return err
		}
		job, ok := workflow.Jobs["service-acceptance"]
		if !ok {
			return errors.New("contract gate has no service-acceptance job")
		}
		if job.Strategy.FailFast == nil || *job.Strategy.FailFast {
			return errors.New("service-acceptance must set fail-fast: false so one red leg does not cancel the others")
		}
		var run *workflowStep
		for i := range job.Steps {
			if job.Steps[i].Name == "Run service acceptance suite" {
				run = &job.Steps[i]
			}
		}
		if run == nil || fmt.Sprint(run.Env["SHARD"]) != "${{ matrix.shard }}" || fmt.Sprint(run.Env["SKIP"]) != "${{ matrix.skip }}" {
			return errors.New("service acceptance step does not select its leg by matrix.shard and matrix.skip")
		}
		shards := map[string][]string{}
		runs := map[string]map[string]int{}
		var names []string
		for _, leg := range job.Strategy.Matrix.Include {
			goos := leg["os"]
			names = append(names, strings.NewReplacer("${{ matrix.os }}", goos, "${{ matrix.shard }}", leg["shard"]).Replace(job.Name))
			shards[goos] = append(shards[goos], leg["shard"])
			args, err := runLeg(run.Run, leg["shard"], leg["skip"], false)
			if err != nil {
				return fmt.Errorf("%s / %s: %v", goos, leg["shard"], err)
			}
			flags := []string{"test", "-p", "1", "-timeout=45m", "-tags=service_acceptance"}
			if len(args) <= len(flags) || !slices.Equal(args[:len(flags)], flags) {
				return fmt.Errorf("%s / %s runs go %q, want go %q and packages", goos, leg["shard"], args, flags)
			}
			if runs[goos] == nil {
				runs[goos] = map[string]int{}
			}
			for _, pkg := range args[len(flags):] {
				if path, ok := strings.CutPrefix(pkg, "./"); ok {
					pkg = serviceAcceptanceModule + "/" + path
				}
				runs[goos][pkg]++
			}
			if _, err := runLeg(run.Run, leg["shard"], leg["skip"], true); leg["shard"] == "rest" && err == nil {
				return fmt.Errorf("%s / %s passes when go list fails", goos, leg["shard"])
			}
		}
		slices.Sort(shards["macos-latest"])
		slices.Sort(shards["ubuntu-latest"])
		if !slices.Equal(shards["macos-latest"], []string{"cmd", "rest"}) || !slices.Equal(shards["ubuntu-latest"], []string{"cmd", "l1", "rest"}) || len(shards) != 2 {
			return fmt.Errorf("service-acceptance legs = %q, want macOS cmd and rest, and ubuntu-latest cmd, l1 and rest", shards)
		}
		// Check names are what branch rules and people see; the macOS names
		// predate the Ubuntu split and stay as they were.
		slices.Sort(names)
		if want := []string{
			"service-acceptance / macos-latest / cmd",
			"service-acceptance / macos-latest / rest",
			"service-acceptance / ubuntu-latest / cmd",
			"service-acceptance / ubuntu-latest / l1",
			"service-acceptance / ubuntu-latest / rest",
		}; !slices.Equal(names, want) {
			return fmt.Errorf("service-acceptance check names = %q, want %q", names, want)
		}
		for goos, counts := range runs {
			for _, pkg := range packages {
				if counts[pkg] != 1 {
					return fmt.Errorf("%s runs %s %d times, want once", goos, pkg, counts[pkg])
				}
			}
			if len(counts) != len(packages) {
				return fmt.Errorf("%s runs %d packages, go list names %d", goos, len(counts), len(packages))
			}
		}
		if _, err := runLeg(run.Run, "unknown", "", false); err == nil {
			return errors.New("an unknown shard passes")
		}
		aggregate, ok := workflow.Jobs["all-tests-pass"]
		if !ok || !slices.Contains(aggregate.Needs, "service-acceptance") {
			return errors.New("all-tests-pass does not wait for service-acceptance")
		}
		// The matrix result covers every leg only if the aggregate requires
		// it to be success.
		for _, step := range aggregate.Steps {
			if fmt.Sprint(step.Env["SERVICE_ACCEPTANCE_RESULT"]) == "${{ needs['service-acceptance'].result }}" &&
				strings.Count(step.Run, `test "$SERVICE_ACCEPTANCE_RESULT" = success`) == 1 {
				return nil
			}
		}
		return errors.New("all-tests-pass does not require service-acceptance to succeed")
	}
	if err := validate(payload); err != nil {
		t.Fatal(err)
	}
	for name, mutation := range map[string][]byte{
		"ubuntu l1 leg removed":        bytes.Replace(payload, []byte("          - os: ubuntu-latest\n            shard: l1\n"), nil, 1),
		"macOS rest leg removed":       bytes.Replace(payload, []byte("          - os: macos-latest\n            shard: rest\n            skip: cmd/wefty\n"), nil, 1),
		"ubuntu rest runs l1 again":    bytes.Replace(payload, []byte("skip: cmd/wefty l1\n"), []byte("skip: cmd/wefty\n"), 1),
		"macOS rest runs cmd again":    bytes.Replace(payload, []byte("skip: cmd/wefty\n    runs-on"), []byte("skip: l1\n    runs-on"), 1),
		"rest lists without tags":      bytes.Replace(payload, []byte(`$(go list -tags=service_acceptance ./...`), []byte(`$(go list ./...`), 1),
		"packages run in parallel":     bytes.Replace(payload, []byte(`go test -p 1 -timeout=45m`), []byte(`go test -timeout=45m`), 1),
		"timeout dropped":              bytes.Replace(payload, []byte(`-timeout=45m -tags=service_acceptance $packages`), []byte(`-tags=service_acceptance $packages`), 1),
		"list failure hidden":          bytes.Replace(payload, []byte("set -o pipefail\n          case \"$SHARD\""), []byte(`case "$SHARD"`), 1),
		"unknown shard passes":         bytes.Replace(payload, []byte(`>&2; exit 1 ;;`), []byte(`>&2; exit 0 ;;`), 1),
		"one red leg cancels the rest": bytes.Replace(payload, []byte("fail-fast: false\n      matrix:\n        # This suite"), []byte("fail-fast: true\n      matrix:\n        # This suite"), 1),
		"legs share one check name":    bytes.Replace(payload, []byte("name: service-acceptance / ${{ matrix.os }} / ${{ matrix.shard }}"), []byte("name: service-acceptance / ${{ matrix.os }}"), 1),
		"aggregate ignores result":     bytes.Replace(payload, []byte(`test "$SERVICE_ACCEPTANCE_RESULT" = success`), []byte(`true`), 1),
	} {
		if bytes.Equal(mutation, payload) {
			t.Fatalf("%s: mutation did not apply", name)
		}
		if validate(mutation) == nil {
			t.Fatalf("%s: drift was accepted", name)
		}
	}
}
