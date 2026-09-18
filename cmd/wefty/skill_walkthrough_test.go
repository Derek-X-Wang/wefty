package main

import (
	"bytes"
	"net"
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

// TestSkillExamplesUseFlagsThisBinaryHas parses every flag in every documented
// example against the real flag set of the command it belongs to. A skill that
// tells an agent to pass a flag that was renamed or removed sends it into a
// usage error it cannot diagnose, and a prose-only check would never catch it.
func TestSkillExamplesUseFlagsThisBinaryHas(t *testing.T) {
	t.Parallel()

	skill := readSkill(t)
	examples := regexp.MustCompile("(?s)```sh\n(.*?)```").FindAllStringSubmatch(skill, -1)
	if len(examples) == 0 {
		t.Fatal("the skill has no shell examples")
	}
	checked := 0
	for _, block := range examples {
		for _, line := range splitContinuedLines(block[1]) {
			command, flags, ok := parseSkillCommand(line)
			if !ok {
				continue
			}
			known := knownFlags(t, command)
			if known == nil {
				continue
			}
			for _, flag := range flags {
				if _, present := known[flag]; !present {
					t.Fatalf("the skill passes --%s to `wefty %s`, which has no such flag",
						flag, strings.Join(command, " "))
				}
			}
			checked++
		}
	}
	if checked < 5 {
		t.Fatalf("only %d documented commands were checked; the parser is not reading the examples", checked)
	}
}

// splitContinuedLines joins shell line continuations so one command is one line.
func splitContinuedLines(block string) []string {
	joined := strings.ReplaceAll(block, "\\\n", " ")
	return strings.Split(joined, "\n")
}

// parseSkillCommand pulls the wefty subcommand and its long flags out of one
// documented line, ignoring shell around it.
func parseSkillCommand(line string) ([]string, []string, bool) {
	line = strings.TrimSpace(line)
	if index := strings.Index(line, "wefty "); index >= 0 {
		line = line[index:]
	} else {
		return nil, nil, false
	}
	line = strings.SplitN(line, "|", 2)[0]
	line = strings.SplitN(line, "#", 2)[0]
	fields := strings.Fields(line)
	var command, flags []string
	for _, field := range fields[1:] {
		switch {
		case field == "--json":
			continue
		case strings.HasPrefix(field, "--"):
			name := strings.TrimPrefix(field, "--")
			name = strings.SplitN(name, "=", 2)[0]
			if name == "" {
				continue
			}
			flags = append(flags, name)
		case strings.HasPrefix(field, "-"), strings.HasPrefix(field, "$"), strings.HasPrefix(field, "<"),
			strings.HasPrefix(field, "\""):
			continue
		default:
			if len(flags) == 0 && isSkillSubcommand(command, field) {
				command = append(command, field)
			}
		}
	}
	if len(command) == 0 {
		return nil, nil, false
	}
	return command, flags, true
}

// isSkillSubcommand keeps run IDs and file paths from being read as verbs.
func isSkillSubcommand(command []string, field string) bool {
	if len(command) == 0 {
		return true
	}
	// Only the two-word commands this skill documents take a second verb.
	switch command[0] {
	case "run", "workflow", "runs":
		return len(command) == 1
	default:
		return false
	}
}

// knownFlags builds the flag set a command really has, by asking the command
// itself for its usage. A command this test cannot introspect returns nil and
// is skipped rather than guessed at.
func knownFlags(t *testing.T, command []string) map[string]struct{} {
	t.Helper()
	var out, errOut bytes.Buffer
	_ = execute(t.Context(), nil, false, append(append([]string{}, command...), "--wefty-unknown-flag"), &out, &errOut)
	usage := out.String() + errOut.String()
	if !strings.Contains(usage, "-") {
		return nil
	}
	flags := map[string]struct{}{}
	for _, match := range regexp.MustCompile(`(?m)^\s+-([A-Za-z0-9][-A-Za-z0-9]*)`).FindAllStringSubmatch(usage, -1) {
		flags[match[1]] = struct{}{}
	}
	if len(flags) == 0 {
		return nil
	}
	return flags
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

// TestStatusStopsAnAgentWhenTheConnectionIsRefused is the other shape of
// "nothing is running": an address that exists and refuses, rather than a name
// that resolves to nothing. A real port is bound and released so the refusal is
// the operating system's rather than the Fabric's.
func TestStatusStopsAnAgentWhenTheConnectionIsRefused(t *testing.T) {
	t.Parallel()

	refused := releasedAddress(t)
	network := plain.NewNetwork()
	participant := network.NewFabric(fabric.Identity{NodeID: "operator", UserID: "alice"})
	clients, err := newAPIClients(participant, refused, releasedAddress(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(clients.close)

	var out, errOut bytes.Buffer
	started := time.Now()
	statusErr := executeStatus(t.Context(), clients, false, nil, &out, &errOut)
	if statusErr == nil {
		t.Fatalf("status reported ready against a refused port:\n%s", out.String())
	}
	if code := commandExitCode(statusErr); code != exitNotReady {
		t.Fatalf("status exited %d (%v), want %d", code, statusErr, exitNotReady)
	}
	if elapsed := time.Since(started); elapsed > statusBudget+2*time.Second {
		t.Fatalf("a refused connection took %s to report", elapsed)
	}
	if !strings.Contains(out.String(), refused) {
		t.Fatalf("the refusal does not name the endpoint it tried:\n%s", out.String())
	}
}

// releasedAddress binds a loopback port and gives it back, so the address is
// real and refuses connections instead of hanging.
func releasedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
