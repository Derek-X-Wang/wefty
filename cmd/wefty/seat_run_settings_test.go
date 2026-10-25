package main

import (
	"flag"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"io"
	"slices"
	"testing"
)

func TestSeatRunSetupDeadlineIsClusterOnlyAndRetained(t *testing.T) {
	flags := flag.NewFlagSet("setup", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var value setupComputerOptions
	bindSetupComputerFlags(flags, &value)
	if err := flags.Parse([]string{"--seat-command-max-runtime=2h"}); err != nil {
		t.Fatal(err)
	}
	if !value.hasClusterSettings() || !slices.Contains(value.arguments(true, false), "--seat-command-max-runtime=2h0m0s") {
		t.Fatalf("L1 setting=%v", value.arguments(true, false))
	}
	if len(value.arguments(false, true)) != 0 {
		t.Fatalf("cluster deadline reached node arguments: %v", value.arguments(false, true))
	}
	paths, err := resolveSetupPaths(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	planned, err := applySetupComputerSettings(paths, connprofile.Profile{}, []launchagent.Agent{{Label: setupL1Label}}, serviceplan.Legacy, value)
	if err != nil || len(planned) != 1 || !slices.Contains(planned[0].Arguments, "--seat-command-max-runtime=2h0m0s") {
		t.Fatalf("installed L1 plan=%+v %v", planned, err)
	}
	var retained setupComputerOptions
	retained.merge(value)
	retained.merge(setupComputerOptions{})
	if err := retained.validate(); err != nil || !slices.Contains(retained.arguments(true, false), "--seat-command-max-runtime=2h0m0s") {
		t.Fatalf("retained=%v %v", retained.arguments(true, false), err)
	}
	for _, raw := range []string{"0", "-1s", "500ms", "none"} {
		flags = flag.NewFlagSet("setup", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		value = setupComputerOptions{}
		bindSetupComputerFlags(flags, &value)
		if err := flags.Parse([]string{"--seat-command-max-runtime=" + raw}); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}
