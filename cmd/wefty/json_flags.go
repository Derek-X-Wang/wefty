package main

import (
	"fmt"
	"strconv"
	"strings"
)

// Value-taking flags protect their next argument from global --json extraction.
// Keep this vocabulary in sync with command registrations (including authoring
// and platform-specific commands); TestJSONValueFlagVocabulary checks it.
const jsonValueFlags = `agent-path allowed-mount-root argv auth-key backup-cap cap
capability claims-enabled class control-url cpu-millicores cursor dir disk-bytes
envelope-schema envelope-schema-file evidence-file fabric fabric-id fabric-name
file follow-for helper-checksum idempotency-key image install-manifest instance-key
intent-file intent-revision interpreter key kind l1 l3 lang limit manifest
manifest-digest max-cost max-inflight max-runtime memory-bytes memory-capacity-bytes
memory-reserve-bytes mode mount name node node-config node-id operator-user origin
out outcome params params-file path payload-file payload-json-file permission
plain-device-id plain-fabric-id plain-identity plain-user-id policy-revision
poll-interval probe-archive probe-digest probe-reference published-port reason
reference restart revision runtime-handler script session-token-file setup-state
state state-dir status step storage-generation storage-id submit-intent-revision
submitter summary tag timeout unit-directory wait wait-timeout workflow-ref
working-directory`

func removeBoolFlag(args []string, name string) ([]string, bool, error) {
	valueFlags := make(map[string]bool)
	for _, valueFlag := range strings.Fields(jsonValueFlags) {
		valueFlags[valueFlag] = true
	}
	filtered := make([]string, 0, len(args))
	enabled := false
	// --wait is boolean on grant/revoke, but takes a duration on storage commands.
	// Track the first two positional command words while protecting flag values.
	var command []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			filtered = append(filtered, args[i:]...)
			break
		}
		if arg == name {
			enabled = true
			continue
		}
		if value, ok := strings.CutPrefix(arg, name+"="); ok {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return nil, true, fmt.Errorf("invalid boolean value %q for %s", value, name)
			}
			enabled = parsed
			continue
		}
		filtered = append(filtered, arg)
		flagName := strings.TrimLeft(arg, "-")
		takesValue := strings.HasPrefix(arg, "-") && valueFlags[flagName]
		if flagName == "wait" && len(command) >= 2 && command[0] == "services" && (command[1] == "grant" || command[1] == "revoke") {
			takesValue = false
		}
		if takesValue && i+1 < len(args) {
			i++
			filtered = append(filtered, args[i])
		} else if !strings.HasPrefix(arg, "-") && len(command) < 2 {
			command = append(command, arg)
		}
	}
	return filtered, enabled, nil
}
