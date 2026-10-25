//go:build rolling_upgrade_acceptance

package scripts

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"howett.net/plist"
)

// Compile the actual adapter against the production package and execute only
// its print-plan entry point. No service command or installation is permitted.
func TestB1CompiledPlanUsesOwnedNamespace(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(root, "home")
	if err := os.Mkdir(home, 0700); err != nil {
		t.Fatal(err)
	}
	repo, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	prefix := "dev.wefty-b1-proof." + strings.Repeat("a", 32) + "."
	broker := filepath.Join(root, "never-run-broker")
	// Fail visibly if a future plan hook accidentally invokes the manager.
	marker := filepath.Join(root, "broker-called")
	if err := os.WriteFile(broker, []byte("#!/bin/sh\nprintf called > '"+marker+"'\nexit 99\n"), 0700); err != nil {
		t.Fatal(err)
	}
	resources := map[string]string{"B1_PROOF_ID": strings.Repeat("a", 32), "GOMAXPROCS": "2", "GOMEMLIMIT": "512MiB", "GOFLAGS": "-p=1", "GOENV": "off", "TMPDIR": filepath.Join(root, "tmp"), "GOTMPDIR": filepath.Join(root, "tmp"), "GOCACHE": filepath.Join(root, "cache"), "GOMODCACHE": filepath.Join(root, "mod"), "GOPATH": filepath.Join(root, "go")}
	envJSON, _ := json.Marshal(resources)
	pathJSON, _ := json.Marshal(os.Getenv("PATH"))
	template, err := os.ReadFile("testdata/rolling-upgrade-plan.go.tmpl")
	if err != nil {
		t.Fatal(err)
	}
	generated := strings.ReplaceAll(strings.ReplaceAll(string(template), "@@ENV@@", string(envJSON)), "@@PATH@@", string(pathJSON))
	replacements := map[string]string{"dev.wefty.": prefix, "127.0.0.1:47101": "127.0.0.1:59101", "127.0.0.1:47102": "127.0.0.1:59102", "/bin/launchctl": broker, "https://github.com/Derek-X-Wang/wefty.git": filepath.Join(root, "source")}
	overlays := map[string]string{}
	paths := []string{"cmd/wefty/setup_command.go", "cmd/wefty/upgrade_command.go", "internal/launchagent/launchagent.go"}
	for i, path := range paths {
		payload, err := os.ReadFile(filepath.Join(repo, path))
		if err != nil {
			t.Fatal(err)
		}
		content := string(payload)
		for before, after := range replacements {
			content = strings.ReplaceAll(content, before, after)
		}
		if i == 0 {
			content = strings.ReplaceAll(content, "loginShell: runLoginShell", "loginShell: b1LoginShell")
			content = strings.ReplaceAll(content, "agents = append(agents, service)", "b1Resources(&service)\n agents = append(agents, service)")
		}
		copy := filepath.Join(root, filepath.Base(path))
		if err := os.WriteFile(copy, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		overlays[filepath.Join(repo, path)] = copy
	}
	generatedFile := filepath.Join(root, "b1_proof_plan.go")
	if err := os.WriteFile(generatedFile, []byte(generated), 0600); err != nil {
		t.Fatal(err)
	}
	overlays[filepath.Join(repo, "cmd/wefty/b1_proof_plan.go")] = generatedFile
	overlayJSON, _ := json.Marshal(map[string]any{"Replace": overlays})
	overlayPath := filepath.Join(root, "overlay.json")
	if err := os.WriteFile(overlayPath, overlayJSON, 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "wefty")
	build := exec.Command("go", "build", "-overlay="+overlayPath, "-o", binary, "./cmd/wefty")
	build.Dir = repo
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("compile plan adapter: %v\n%s", err, output)
	}
	command := exec.Command(binary, "--b1-print-plan")
	command.Env = []string{"HOME=" + home, "PATH=" + os.Getenv("PATH"), "USER=proof", "SHELL=/bin/sh"}
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	var plan struct {
		Services [][]byte `json:"services"`
		Ports    []string `json:"ports"`
		Remote   string   `json:"source_remote"`
		Broker   string   `json:"broker"`
	}
	if err := json.Unmarshal(output, &plan); err != nil {
		t.Fatalf("%v: %s", err, output)
	}
	if len(plan.Services) != 3 || strings.Join(plan.Ports, ",") != "127.0.0.1:59101,127.0.0.1:59102" || plan.Remote != filepath.Join(root, "source") || plan.Broker != broker {
		t.Fatalf("wrong effective plan: %s", output)
	}
	for _, payload := range plan.Services {
		var definition struct {
			Label                string
			ProgramArguments     []string
			EnvironmentVariables map[string]string
		}
		if _, err := plist.Unmarshal(payload, &definition); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(definition.Label, prefix) || !strings.HasPrefix(definition.ProgramArguments[0], home+string(os.PathSeparator)) {
			t.Fatalf("escaped service: %s", payload)
		}
		for key, want := range resources {
			if definition.EnvironmentVariables[key] != want {
				t.Fatalf("service %s missing %s=%s", definition.Label, key, want)
			}
		}
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("print-plan invoked its service manager")
	}
}

// Mutation checks are opt-in: each must fail through a behavior assertion.
func TestB1RunnerSafetyMutations(t *testing.T) {
	command := exec.Command("python3", "test-rolling-upgrade-mutations.py")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("mutation sensitivity: %v\n%s", err, output)
	}
}

func TestB1RunnerOwnershipMatrix(t *testing.T) {
	command := exec.Command("python3", "test-rolling-upgrade-round2.py")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("ownership matrix: %v\n%s", err, output)
	}
}

func TestB1RunnerRecoveryIdentityMatrix(t *testing.T) {
	command := exec.Command("python3", "test-rolling-upgrade-round3.py")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("recovery identity matrix: %v\n%s", err, output)
	}
}

func TestB1RunnerGUIDomainAndSourceMatrix(t *testing.T) {
	command := exec.Command("python3", "test-rolling-upgrade-round4.py")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("GUI domain/source matrix: %v\n%s", err, output)
	}
}
