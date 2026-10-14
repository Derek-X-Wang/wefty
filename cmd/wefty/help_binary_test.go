package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

var helpCommands = []string{
	"", "help", "setup", "submit", "status", "whoami", "cancel", "rerun", "logs", "wait", "results", "inspect", "drain",
	"nodes", "nodes list", "nodes inspect", "nodes set-claims", "jobs", "jobs list", "computers", "computers list", "runs", "runs list",
	"admin", "admin bootstrap", "admin policy", "admin policy get", "admin policy add", "admin policy remove",
	"admins", "admins list", "admins get", "admins add", "admins remove",
	"node", "node doctor", "node setup-oci", "node load-image", "node oci", "node oci start", "node oci stop", "node oci removals",
	"services", "services create", "services list", "services status", "services start", "services stop", "services restart", "services logs", "services remove", "services forget",
	"services reimage", "services reset", "services resize", "services abort", "services grants", "services grant", "services revoke",
	"services submission", "services submission enable", "services submission disable", "services submission set-inflight",
	"services takeover", "services takeover view", "services takeover take", "services takeover release", "services takeover sessions", "services takeover sessions list", "services takeover audit", "services takeover audit tail",
	"services backup", "services backup create", "services backup list", "services backup prune", "services backup set-cap", "services restore", "services clone",
	"services custody", "services custody export", "services custody import", "services custody attest",
	"run", "run envelope", "run step", "run gate", "run result", "run params", "workflow", "workflow init",
}

func TestHelpAndUsageErrorsFromRealBinary(t *testing.T) {
	binary := buildWefty(t)

	for _, command := range helpCommands {
		for _, help := range []string{"--help", "-h", "-help", "--h", "--help=true", "--help=false"} {
			for _, jsonMode := range []bool{false, true} {
				args := append(strings.Fields(command), help)
				if jsonMode {
					args = append([]string{"--json"}, args...)
				}
				t.Run(strings.Join(args, " "), func(t *testing.T) {
					code, stdout, stderr := runPolishBinary(t, binary, args...)
					if code != 0 || len(stderr) != 0 || json.Valid(stdout) || !helpOutputMatchesCommand(stdout, command) {
						t.Fatalf("exit=%d stdout=%s stderr=%s; want exit 0, usage on stdout, empty stderr", code, stdout, stderr)
					}
				})
			}
		}
	}
	for _, args := range [][]string{
		{"--fabric=invalid", "help"},
		{"submit", "--help", "--json"},
		{"rerun", "run-example", "--help"},
		{"rerun", "--again", "run-example", "--help"},
		{"services", "restore", "--wait=1s", "computer-example", "--help"},
		{"services", "grant", "--wait", "computer-example", "person-example", "--help"},
		{"services", "grant", "computer-example", "person-example", "--help"},
		{"services", "backup", "create", "computer-example", "--help"},
		{"services", "takeover", "sessions", "list", "computer-example", "--help"},
		{"services", "takeover", "audit", "tail", "computer-example", "--help"},
		{"--node-config=/no-installed-config", "node", "load-image", "archive.tar", "--help"},
		{"--fabric=tsnet", "--l1=", "--l3=", "--json", "services", "grant", "--help"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			code, stdout, stderr := runPolishBinary(t, binary, args...)
			if code != 0 || len(stderr) != 0 || json.Valid(stdout) || !helpOutputMatchesCommand(stdout, helpCommandPath(args)) {
				t.Fatalf("exit=%d stdout=%s stderr=%s; want plain usage on stdout without opening Fabric or node state", code, stdout, stderr)
			}
		})
	}
	// The real process must preserve ordinary usage errors and their stream,
	// including JSON errors and help-looking values or escaped positionals.
	for _, args := range [][]string{
		{"submit", "--no-such-flag"}, {"--no-such-flag"}, {"submit"}, {"submit", "extra"},
		{"status", "--timeout=bad"}, {"status", "--timeout=-1s"},
		{"status", "--timeout", "--help"}, {"submit", "--", "--help"},
		{"whoami", "--unsupported", "--help"},
		{"services", "status", "--unsupported", "--help"},
		{"rerun", "--again", "run-example", "--unsupported", "--help"},
		{"submit", "--no-such-flag", "--help"}, {"services", "no-such-command", "--help"},
	} {
		for _, jsonMode := range []bool{false, true} {
			args := append([]string(nil), args...)
			if jsonMode {
				args = append([]string{"--json"}, args...)
			}
			t.Run("usage error/"+strings.Join(args, " "), func(t *testing.T) {
				code, stdout, stderr := runPolishBinary(t, binary, args...)
				if code != exitUsage || len(stdout) != 0 || len(stderr) == 0 {
					t.Fatalf("exit=%d stdout=%s stderr=%s; want exit 2 and stderr only", code, stdout, stderr)
				}
				if jsonMode {
					var document contract.ErrorResponse
					if err := json.Unmarshal(stderr, &document); err != nil || document.Error.Code != contract.ErrorInvalidRequest {
						t.Fatalf("invalid JSON usage error: %s (%v)", stderr, err)
					}
				} else if strings.Contains(strings.Join(args, " "), "--no-such-flag") && !bytes.Contains(stderr, []byte("flag provided but not defined: -no-such-flag")) {
					t.Fatalf("missing original flag diagnostic: %s", stderr)
				}
			})
		}
	}
}

func helpFirstLine(output []byte) string {
	line, _, _ := strings.Cut(string(output), "\n")
	return line
}

func expectedHelpFirstLine(command string) string {
	if command == "" || command == "help" || commandHelpGroups[command] {
		return helpFirstLine([]byte(rootUsage))
	}
	if _, ok := positionalCommandUsage[command]; ok {
		return "usage: wefty " + command
	}
	switch {
	case command == "run" || strings.HasPrefix(command, "run "):
		return "Usage: wefty run <envelope|step|gate|result|params> [flags]"
	case command == "workflow" || strings.HasPrefix(command, "workflow "):
		return "Usage: wefty workflow init NAME [--lang bash|ts] [--dir DIR]"
	}
	switch command {
	case "admins add":
		command = "admin policy add"
	case "admins remove":
		command = "admin policy remove"
	case "services revoke":
		command = "services grant"
	case "services takeover audit tail":
		command = "services takeover audit"
	}
	return "Usage of " + command + ":"
}

func helpCommandPath(args []string) string {
	// Only matrix paths count; resource IDs and global flags are not commands.
	best := ""
	for _, command := range helpCommands {
		words := strings.Fields(command)
		if len(words) == 0 {
			continue
		}
		for start := range args {
			if start+len(words) > len(args) {
				continue
			}
			if strings.Join(args[start:start+len(words)], " ") == command && len(command) > len(best) {
				best = command
			}
		}
	}
	return best
}

func helpOutputMatchesCommand(output []byte, command string) bool {
	first := helpFirstLine(output)
	expected := expectedHelpFirstLine(command)
	return first == expected || (strings.HasPrefix(expected, "usage: wefty ") && strings.HasPrefix(first, expected+" "))
}

func TestRunScriptFlagHelpFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, command := range [][]string{{"wait", "run_help"}, {"inspect", "run_help"}, {"cancel", "run_help"}, {"runs", "list"}} {
		args := append([]string{"--json", "--fabric=invalid"}, command...)
		args = append(args, "--include-script", "--help")
		t.Run(strings.Join(command, " "), func(t *testing.T) {
			code, stdout, stderr := runPolishBinary(t, binary, args...)
			if code != 0 || len(stderr) != 0 || !strings.Contains(string(stdout), "-include-script") {
				t.Fatalf("exit=%d stdout=%s stderr=%s; want flag help without opening Fabric", code, stdout, stderr)
			}
		})
	}
}
