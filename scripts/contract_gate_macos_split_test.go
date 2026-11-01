package scripts

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Hosted macOS runs cmd/wefty in two halves (#1035). Every test must run in
// exactly one of them, so both halves read one pattern: "exec-fleet" runs the
// tests it matches and "rest" skips them and runs everything else, including
// every other package. A new test then always lands in a half.
func TestContractGateMacOSHalvesRunEveryTestOnce(t *testing.T) {
	_, payload := readWorkflow(t, "../.github/workflows/contract-gate.yml")
	validate := func(payload []byte) error {
		var workflow struct {
			Jobs map[string]struct {
				Name     string            `yaml:"name"`
				Needs    []string          `yaml:"needs"`
				Env      map[string]string `yaml:"env"`
				Strategy struct {
					Matrix struct {
						Include []map[string]string `yaml:"include"`
					} `yaml:"matrix"`
				} `yaml:"strategy"`
				Steps []workflowStep `yaml:"steps"`
			} `yaml:"jobs"`
		}
		if err := yaml.Unmarshal(payload, &workflow); err != nil {
			return err
		}
		job, ok := workflow.Jobs["mac-linux-runner"]
		if !ok {
			return errors.New("contract gate has no mac-linux-runner job")
		}
		// A slash would make the pattern match subtests, and both halves would
		// then run the parent test.
		if pattern := job.Env["CMD_WEFTY_SPLIT"]; pattern == "" || strings.Contains(pattern, "/") {
			return fmt.Errorf("mac-linux-runner CMD_WEFTY_SPLIT = %q, want one top-level pattern", pattern)
		}
		var shards []string
		for _, leg := range job.Strategy.Matrix.Include {
			if leg["os"] == "macos-latest" {
				shards = append(shards, leg["shard"])
			}
		}
		slices.Sort(shards)
		if !slices.Equal(shards, []string{"exec-fleet", "rest"}) {
			return fmt.Errorf("hosted macOS legs = %q, want exec-fleet and rest", shards)
		}
		if !strings.Contains(job.Name, "matrix.shard") {
			return errors.New("the two hosted macOS legs need distinct job names")
		}
		var run, upload *workflowStep
		for i := range job.Steps {
			switch job.Steps[i].Name {
			case "Run cross-platform runner and contract tests (per-test times)":
				run = &job.Steps[i]
			case "Upload per-test times":
				upload = &job.Steps[i]
			}
		}
		if run == nil || run.If != "matrix.os == 'macos-latest'" || run.Env["SHARD"] != "${{ matrix.shard }}" {
			return errors.New("hosted macOS test step does not select its half by matrix.shard")
		}
		for _, want := range []string{
			`go_test cmd -run "$CMD_WEFTY_SPLIT" ./cmd/wefty`,
			`go_test cmd -skip "$CMD_WEFTY_SPLIT" ./cmd/wefty &`,
			`others=$(go list ./... | grep -vxF github.com/Derek-X-Wang/wefty/cmd/wefty)`,
			`go_test others $others &`,
			`go test -json -timeout=45m "$@" | tee "$RUNNER_TEMP/go-test-$name.json"`,
			`[ "$pipes" -eq 0 ] || exit 1`,
			`exit "$tests"`,
		} {
			if strings.Count(run.Run, want) != 1 {
				return fmt.Errorf("hosted macOS test step runs %q %d times", want, strings.Count(run.Run, want))
			}
		}
		if upload == nil || fmt.Sprint(upload.With["name"]) != "go-test-json-macos-latest-${{ matrix.shard }}" {
			return errors.New("each hosted macOS half must upload its own per-test times")
		}
		if aggregate, ok := workflow.Jobs["all-tests-pass"]; !ok || !slices.Contains(aggregate.Needs, "mac-linux-runner") {
			return errors.New("all-tests-pass does not wait for mac-linux-runner")
		}
		return nil
	}
	if err := validate(payload); err != nil {
		t.Fatal(err)
	}
	for name, mutation := range map[string][]byte{
		"rest half runs its matches": bytes.Replace(payload, []byte(`go_test cmd -skip "$CMD_WEFTY_SPLIT"`), []byte(`go_test cmd -run "$CMD_WEFTY_SPLIT"`), 1),
		"a half has its own pattern": bytes.Replace(payload, []byte(`go_test cmd -run "$CMD_WEFTY_SPLIT"`), []byte(`go_test cmd -run '^TestExec'`), 1),
		"other packages dropped":     bytes.Replace(payload, []byte(`go_test others $others &`), []byte(`true &`), 1),
		"one macOS half removed":     bytes.Replace(payload, []byte("          - os: macos-latest\n            shard: rest\n"), nil, 1),
		"halves share an artifact":   bytes.Replace(payload, []byte("name: go-test-json-macos-latest-${{ matrix.shard }}"), []byte("name: go-test-json-macos-latest"), 1),
		"pattern matches subtests":   bytes.Replace(payload, []byte(`CMD_WEFTY_SPLIT: '^Test(Exec|Fleet)'`), []byte(`CMD_WEFTY_SPLIT: '^Test(Exec|Fleet)/sub'`), 1),
		"timeout dropped":            bytes.Replace(payload, []byte(`go test -json -timeout=45m "$@" |`), []byte(`go test -json "$@" |`), 1),
		"test failure swallowed":     bytes.Replace(payload, []byte(`exit "$tests"`), []byte(`exit 0`), 1),
		"pipe failure swallowed":     bytes.Replace(payload, []byte(`[ "$pipes" -eq 0 ] || exit 1`), []byte(`true`), 1),
	} {
		if bytes.Equal(mutation, payload) || validate(mutation) == nil {
			t.Fatalf("%s: drift was accepted", name)
		}
	}
}
