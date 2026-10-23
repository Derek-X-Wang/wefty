package serviceplan

import (
	"reflect"
	"strings"
	"testing"
)

func TestGenerationSelectionLeavesLegacyArgumentsAndReplacesReadyPath(t *testing.T) {
	args := []string{"--profile=/profile", "--ready-file=/old-ready", "--shutdown-budget=75s"}
	before := append([]string{}, args...)
	id := strings.Repeat("a", 32)
	g := NodeGeneration{ID: id, Root: "/data/agent-generations", Executable: "/data/agent-generations/" + id + "/wefty-agent", ReadyFile: "/data/agent-generations/" + id + "/ready.json", LaunchAgents: "/home/Library/LaunchAgents"}
	selected, err := GenerationArguments(args, g)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(args, before) {
		t.Fatal("selection mutated legacy input")
	}
	if strings.Contains(strings.Join(selected, " "), "/old-ready") || !strings.Contains(strings.Join(selected, " "), "--agent-generation-id="+id) {
		t.Fatal("generation paths not selected")
	}
	g.Executable = "/data/.upgrade-retained/wefty-agent"
	if _, err := GenerationArguments(args, g); err == nil {
		t.Fatal("accepted staging executable")
	}
}
