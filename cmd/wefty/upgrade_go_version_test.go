package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

// The upgrade fixture's go.mod version, and a go env answer that can build it.
const (
	upgradeFixtureGo    = "1.27.3"
	upgradeFixtureGoEnv = `{"GOVERSION":"go1.99.0","GOTOOLCHAIN":"local"}`
)

// fakeUpgradeGo writes a go that answers `go env -json` from FAKE_GOVERSION
// and the GOTOOLCHAIN it receives (empty means the default, auto), and that
// records a build by creating FAKE_BUILT.
func fakeUpgradeGo(t *testing.T) string {
	t.Helper()
	tools := t.TempDir()
	script := `#!/bin/sh
if [ "$1" = env ]; then printf '{"GOVERSION":"%s","GOTOOLCHAIN":"%s"}\n' "$FAKE_GOVERSION" "${GOTOOLCHAIN:-auto}"; exit 0; fi
if [ "$1" = build ]; then : > "$FAKE_BUILT"; exit 0; fi
exit 99
`
	if err := os.WriteFile(filepath.Join(tools, "go"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return tools
}

// addUpgradeFixtureGoMod adds a commit with a go.mod on the fixture's main,
// for tests that run the real build step and its Go version check. The base
// fixture commit stays unchanged, because byte goldens name it.
func addUpgradeFixtureGoMod(t *testing.T, f *upgradeFixture) {
	t.Helper()
	goMod := "module example.invalid/fixture\n\ngo " + upgradeFixtureGo + "\n"
	stream := fmt.Sprintf("commit refs/heads/main\ncommitter Fixture <fixture@example.invalid> 1700000002 +0000\ndata 11\nAdd go.mod\n\nfrom %s\nM 100644 inline go.mod\ndata %d\n%s\n", f.commit, len(goMod), goMod)
	command := exec.Command("git", "--git-dir="+f.operations.remote, "fast-import", "--quiet")
	command.Stdin = strings.NewReader(stream)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("add go.mod: %v %s", err, output)
	}
	output, err := exec.Command("git", "--git-dir="+f.operations.remote, "rev-parse", "refs/heads/main").Output()
	if err != nil {
		t.Fatal(err)
	}
	f.commit = strings.TrimSpace(string(output))
}

func TestUpgradeGoVersionCheckBeforeBuild(t *testing.T) {
	tools := fakeUpgradeGo(t)
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "go.mod"), []byte("module example.invalid/fixture\n\ngo "+upgradeFixtureGo+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	builds := map[string]func(*testing.T) func(context.Context, string, string, string, string) error{
		"plain": func(*testing.T) func(context.Context, string, string, string, string) error {
			return upgradeOperations(setupHost{}).build
		},
		"fleet": func(*testing.T) func(context.Context, string, string, string, string) error {
			return fleetUpgradeOperations(setupHost{}, setupPaths{}, connprofile.Profile{}, serviceplan.Plan{}).build
		},
	}
	for _, test := range []struct {
		name, goVersion, toolchain string
		refuse                     bool
	}{
		{"older go with default policy downloads", "go1.26.0", "", false},
		{"older go with auto downloads", "go1.26.0", "auto", false},
		{"older go with path searches PATH", "go1.26.0", "path", false},
		{"older pinned go with auto downloads", "go1.26.0", "go1.26.0+auto", false},
		{"older go with local refuses", "go1.26.0", "local", true},
		{"older pinned go refuses", "go1.26.0", "go1.26.0", true},
		{"same go with local builds", "go" + upgradeFixtureGo, "local", false},
		{"newer go with local builds", "go1.28.1", "local", false},
		{"development go leaves it to the build", "devel go1.28-abcdef", "local", false},
	} {
		for kind, build := range builds {
			t.Run(kind+"/"+test.name, func(t *testing.T) {
				built := filepath.Join(t.TempDir(), "built")
				t.Setenv("FAKE_GOVERSION", test.goVersion)
				t.Setenv("FAKE_BUILT", built)
				t.Setenv("GOTOOLCHAIN", test.toolchain)
				err := build(t)(t.Context(), source, t.TempDir(), "commit", tools)
				_, statErr := os.Stat(built)
				if test.refuse {
					if err == nil || !strings.Contains(err.Error(), "this commit needs Go "+upgradeFixtureGo) || !strings.Contains(err.Error(), "install Go "+upgradeFixtureGo+" or newer") || !strings.Contains(err.Error(), "GOTOOLCHAIN="+test.toolchain) {
						t.Fatalf("no clear refusal: %v", err)
					}
					if statErr == nil {
						t.Fatal("refused Go still built")
					}
					return
				}
				if err != nil || statErr != nil {
					t.Fatalf("build did not run: %v %v", err, statErr)
				}
			})
		}
	}
}

// A refused Go stops the upgrade before any version check, preflight or
// service restart, and the installed stack stays unchanged.
func TestUpgradeRefusesOldGoBeforeChangingAnything(t *testing.T) {
	f := newUpgradeFixture(t)
	addUpgradeFixtureGoMod(t, f)
	tools := fakeUpgradeGo(t)
	t.Setenv("FAKE_GOVERSION", "go1.26.0")
	t.Setenv("FAKE_BUILT", filepath.Join(t.TempDir(), "built"))
	t.Setenv("GOTOOLCHAIN", "local")
	paths, _ := resolveSetupPaths(f.home)
	plist := strings.ReplaceAll(f.plist(t, setupNodeAgentLabel), f.loginPATH, tools+":"+f.loginPATH)
	if err := os.WriteFile(filepath.Join(paths.launchAgents, setupNodeAgentLabel+".plist"), []byte(plist), 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := executableDigest(filepath.Join(paths.bin, "wefty-agent"))
	if err != nil {
		t.Fatal(err)
	}
	f.operations.build = nil
	_, err = runUpgrade(t.Context(), f.host, true, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "upgrade build failed; installed stack unchanged: this commit needs Go "+upgradeFixtureGo) {
		t.Fatalf("old Go was not refused before the build: %v", err)
	}
	if len(f.events) != 0 {
		t.Fatalf("upgrade went past the refusal: %v", f.events)
	}
	if after, err := executableDigest(filepath.Join(paths.bin, "wefty-agent")); err != nil || after != before {
		t.Fatalf("installed binary changed: %v", err)
	}
}
