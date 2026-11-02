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

// Hosted macOS runs the service acceptance suite in two halves (#1035):
// "cmd" runs cmd/wefty and "rest" runs every other package that the same
// tags list. Every package then runs in exactly one half, and a new package
// lands in "rest" by default.
func TestContractGateServiceAcceptanceHalvesRunEveryPackageOnce(t *testing.T) {
	_, payload := readWorkflow(t, "../.github/workflows/contract-gate.yml")
	validate := func(payload []byte) error {
		var workflow struct {
			Jobs map[string]struct {
				Name     string   `yaml:"name"`
				Needs    []string `yaml:"needs"`
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
		job, ok := workflow.Jobs["service-acceptance"]
		if !ok {
			return errors.New("contract gate has no service-acceptance job")
		}
		legs := map[string][]string{}
		for _, leg := range job.Strategy.Matrix.Include {
			legs[leg["os"]] = append(legs[leg["os"]], leg["shard"])
		}
		slices.Sort(legs["macos-latest"])
		if !slices.Equal(legs["macos-latest"], []string{"cmd", "rest"}) || !slices.Equal(legs["ubuntu-latest"], []string{""}) {
			return fmt.Errorf("service-acceptance legs = %q, want one whole ubuntu-latest leg and macOS cmd and rest", legs)
		}
		if !strings.Contains(job.Name, "matrix.shard") {
			return errors.New("the two hosted macOS legs need distinct job names")
		}
		var run *workflowStep
		for i := range job.Steps {
			if job.Steps[i].Name == "Run service acceptance suite" {
				run = &job.Steps[i]
			}
		}
		if run == nil || fmt.Sprint(run.Env["SHARD"]) != "${{ matrix.shard }}" {
			return errors.New("service acceptance step does not select its half by matrix.shard")
		}
		for _, want := range []string{
			`set -o pipefail`,
			`'') packages=./... ;;`,
			`cmd) packages=./cmd/wefty ;;`,
			`rest) packages=$(go list -tags=service_acceptance ./... | grep -vxF github.com/Derek-X-Wang/wefty/cmd/wefty) ;;`,
			`*) echo "unknown shard: $SHARD" >&2; exit 1 ;;`,
			`go test -p 1 -timeout=45m -tags=service_acceptance $packages`,
		} {
			if strings.Count(run.Run, want) != 1 {
				return fmt.Errorf("service acceptance step has %q %d times", want, strings.Count(run.Run, want))
			}
		}
		if aggregate, ok := workflow.Jobs["all-tests-pass"]; !ok || !slices.Contains(aggregate.Needs, "service-acceptance") {
			return errors.New("all-tests-pass does not wait for service-acceptance")
		}
		return nil
	}
	if err := validate(payload); err != nil {
		t.Fatal(err)
	}
	for name, mutation := range map[string][]byte{
		"rest lists without tags":   bytes.Replace(payload, []byte(`$(go list -tags=service_acceptance ./...`), []byte(`$(go list ./...`), 1),
		"rest runs cmd/wefty again": bytes.Replace(payload, []byte(` | grep -vxF github.com/Derek-X-Wang/wefty/cmd/wefty)`), []byte(`)`), 1),
		"one macOS half removed":    bytes.Replace(payload, []byte("          - os: macos-latest\n            shard: rest\n"), nil, 1),
		"packages run in parallel":  bytes.Replace(payload, []byte(`go test -p 1 -timeout=45m`), []byte(`go test -timeout=45m`), 1),
		"timeout dropped":           bytes.Replace(payload, []byte(`-timeout=45m -tags=service_acceptance $packages`), []byte(`-tags=service_acceptance $packages`), 1),
		"list failure hidden":       bytes.Replace(payload, []byte("set -o pipefail\n"), nil, 1),
		"unknown shard passes":      bytes.Replace(payload, []byte(`>&2; exit 1 ;;`), []byte(`>&2; exit 0 ;;`), 1),
	} {
		if bytes.Equal(mutation, payload) || validate(mutation) == nil {
			t.Fatalf("%s: drift was accepted", name)
		}
	}
}
