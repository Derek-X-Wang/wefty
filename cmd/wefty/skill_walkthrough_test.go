package main

import (
	"bytes"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l3"
)

// The skill is a contract with an agent, and an agent will follow it literally.
// These tests hold it to that: the commands it tells an agent to run must be
// commands this binary has, in the order it gives them, and the first one must
// be the one that stops an agent on a machine where nothing is running.

// skillLoopSequence is the order the skill teaches. Reading it from the document
// rather than restating it is the point: if the document drifts, this fails.
var skillLoopSequence = []string{"submit", "wait", "inspect", "results"}

func readSkill(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("../../skills/wefty/SKILL.md")
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestSkillStartsWithStatusAndNamesItsExitCodes pins the shape the ticket asked
// for: an agent reads `status` first and learns what a refusal means before it
// has submitted anything.
func TestSkillStartsWithStatusAndNamesItsExitCodes(t *testing.T) {
	t.Parallel()

	skill := readSkill(t)
	headings := regexp.MustCompile(`(?m)^## (.+)$`).FindAllStringSubmatch(skill, -1)
	if len(headings) == 0 {
		t.Fatal("the skill has no sections")
	}
	if !strings.Contains(strings.ToLower(headings[0][1]), "ready") {
		t.Fatalf("the skill opens with %q rather than the readiness check", headings[0][1])
	}
	statusIndex := strings.Index(skill, "wefty --json status")
	submitIndex := strings.Index(skill, "wefty --json submit")
	if statusIndex < 0 || submitIndex < 0 || statusIndex > submitIndex {
		t.Fatal("the skill does not put `wefty status` before `wefty submit`")
	}
	// The codes an agent branches on, named in the document rather than left
	// to be discovered.
	for _, want := range []string{"| 0 |", "| 12 |", "| 10 |", "| 11 |"} {
		if !strings.Contains(skill, want) {
			t.Fatalf("the skill does not document exit code row %q", want)
		}
	}
	// Bring-up is a human step, and the skill has to say so or an agent will
	// try to do it.
	for _, want := range []string{"human steps", "docs/acceptance/v0.1-dogfood.md"} {
		if !strings.Contains(skill, want) {
			t.Fatalf("the skill does not say %q", want)
		}
	}
	// And it must not overstate what each command proves.
	if !strings.Contains(skill, "the run's **status**, as an exit code. Nothing about what it produced") {
		t.Fatal("the skill does not say what `wait` alone proves")
	}
}

// TestSkillLoopUsesCommandsThisBinaryHas walks the sequence the skill teaches
// and proves every step of it is a real command, in that order. A skill that
// names a command the binary does not have is worse than no skill.
func TestSkillLoopUsesCommandsThisBinaryHas(t *testing.T) {
	t.Parallel()

	skill := readSkill(t)
	loop := skill[strings.Index(skill, "## The loop"):]
	previous := -1
	for _, command := range skillLoopSequence {
		index := strings.Index(loop, "wefty "+command+" ")
		if index < 0 {
			index = strings.Index(loop, "wefty --json "+command+" ")
		}
		if index < 0 {
			t.Fatalf("the skill's loop never runs `wefty %s`", command)
		}
		if index < previous {
			t.Fatalf("the skill's loop runs `wefty %s` out of order", command)
		}
		previous = index
		// The command is dispatched rather than rejected as unknown. It is
		// called with nothing so it fails on its own arguments, which is proof
		// it exists; reaching the cluster is not this test's business.
		var out, errOut bytes.Buffer
		err := execute(t.Context(), nil, false, []string{command}, &out, &errOut)
		if err == nil {
			t.Fatalf("`wefty %s` with no arguments succeeded against no cluster", command)
		}
		if strings.Contains(err.Error(), "unknown command") {
			t.Fatalf("the skill names `wefty %s`, which this binary does not have", command)
		}
	}
	// The helper commands the skill tells a workflow to use, same rule.
	for _, helper := range []string{"run", "workflow"} {
		if !strings.Contains(skill, "wefty "+helper+" ") {
			t.Fatalf("the skill never mentions `wefty %s`", helper)
		}
	}
}

// TestStatusStopsAnAgentWhenNothingIsListening is the second half of the
// ticket's acceptance: on a machine without the stack, an agent following the
// skill stops here, quickly, with a reason.
func TestStatusStopsAnAgentWhenNothingIsListening(t *testing.T) {
	t.Parallel()

	// A real Fabric with nothing registered on either address, which is what a
	// machine that has not had the stack started looks like.
	network := plain.NewNetwork()
	participant := network.NewFabric(fabric.Identity{NodeID: "operator", UserID: "alice"})
	clients, err := newAPIClients(participant, l3.DefaultL1Address, l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(clients.close)

	var out, errOut bytes.Buffer
	started := time.Now()
	waitErr := executeStatus(t.Context(), clients, false, nil, &out, &errOut)
	elapsed := time.Since(started)
	if waitErr == nil {
		t.Fatalf("status reported ready with nothing running:\n%s", out.String())
	}
	if code := commandExitCode(waitErr); code != exitNotReady {
		t.Fatalf("status exited %d (%v), want %d", code, waitErr, exitNotReady)
	}
	if elapsed > statusBudget+2*time.Second {
		t.Fatalf("status took %s to say nothing is running", elapsed)
	}
	// The reason names both services and the endpoints it looked at, which is
	// what the person reading it needs in order to start them.
	rendered := out.String()
	for _, want := range []string{"not ready:", "L1", "L3", l3.DefaultL1Address, l3.DefaultL3Address} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("the refusal does not mention %q:\n%s", want, rendered)
		}
	}
}
