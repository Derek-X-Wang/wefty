package scripts

import (
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// A copied pin to a commit that does not exist upstream stays hidden while its
// job is skipped (#1491). Resolving each SHA needs the network, so this test
// checks the offline invariant instead: each action has one commit pin across
// all workflows. Bump an action in every workflow together.
func TestWorkflowActionPinsAgree(t *testing.T) {
	var paths []string
	for _, pattern := range []string{"../.github/workflows/*.yml", "../.github/workflows/*.yaml"} {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, matches...)
	}
	workflows := map[string]workflowContract{}
	for _, path := range paths {
		workflows[filepath.Base(path)], _ = readWorkflow(t, path)
	}
	pins, conflicts := actionPinConflicts(workflows)
	if pins == 0 {
		t.Fatal("no commit-pinned actions found")
	}
	for _, conflict := range conflicts {
		t.Error(conflict)
	}
}

func TestActionPinConflictsReadsDecodedUses(t *testing.T) {
	const a, b = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	for _, tc := range []struct {
		name, workflow string
		conflicts      int
	}{
		{"quoted conflict", "jobs:\n  x:\n    steps:\n      - uses: \"actions/setup-go@" + b + "\"\n", 1},
		{"block scalar conflict", "jobs:\n  x:\n    steps:\n      - uses: >-\n          actions/setup-go@" + b + "\n", 1},
		{"reusable workflow conflict", "jobs:\n  x:\n    uses: actions/setup-go@" + b + "\n", 1},
		{"commented old pin", "jobs:\n  x:\n    steps:\n      # - uses: actions/setup-go@" + b + "\n      - uses: actions/setup-go@" + a + "\n", 0},
		{"pin text inside run", "jobs:\n  x:\n    steps:\n      - run: |\n          echo 'uses: actions/setup-go@" + b + "'\n", 0},
		{"tag reference", "jobs:\n  x:\n    steps:\n      - uses: actions/setup-go@v7\n", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var base, candidate workflowContract
			if err := yaml.Unmarshal([]byte("jobs:\n  x:\n    steps:\n      - uses: actions/setup-go@"+a+"\n"), &base); err != nil {
				t.Fatal(err)
			}
			if err := yaml.Unmarshal([]byte(tc.workflow), &candidate); err != nil {
				t.Fatal(err)
			}
			if _, conflicts := actionPinConflicts(map[string]workflowContract{"base.yml": base, "candidate.yml": candidate}); len(conflicts) != tc.conflicts {
				t.Fatalf("conflicts=%v, want %d", conflicts, tc.conflicts)
			}
		})
	}
}

// actionPinConflicts returns the number of commit-pinned uses and one message
// per action pinned to more than one commit.
func actionPinConflicts(workflows map[string]workflowContract) (int, []string) {
	pinned := regexp.MustCompile(`^([\w./-]+)@([0-9a-f]{40})$`)
	pins := map[string]map[string][]string{}
	count := 0
	add := func(file, uses string) {
		match := pinned.FindStringSubmatch(uses)
		if match == nil {
			return
		}
		count++
		if pins[match[1]] == nil {
			pins[match[1]] = map[string][]string{}
		}
		pins[match[1]][match[2]] = append(pins[match[1]][match[2]], file)
	}
	for file, workflow := range workflows {
		for _, job := range workflow.Jobs {
			add(file, job.Uses)
			for _, step := range job.Steps {
				add(file, step.Uses)
			}
		}
	}
	var conflicts []string
	for action, shas := range pins {
		if len(shas) < 2 {
			continue
		}
		var found []string
		for sha, files := range shas {
			sort.Strings(files)
			found = append(found, sha+" in "+files[0])
		}
		sort.Strings(found)
		conflicts = append(conflicts, action+" has more than one commit pin: "+strings.Join(found, ", "))
	}
	sort.Strings(conflicts)
	return count, conflicts
}
