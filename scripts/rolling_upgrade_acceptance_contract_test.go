package scripts

import (
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// A plan must remain useful when no installation, HOME or service tools exist.
// Never exercise the execution branch from this contract test.
func TestRollingUpgradePlanNeedsNoInstallation(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	script, err := filepath.Abs("measure-rolling-upgrade.py")
	if err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(t.TempDir(), "absent-home")
	owner, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	evidence := filepath.Join(owner.HomeDir, ".local/share/wefty-b1-evidence-"+strings.Repeat("a", 32))
	var first string
	for i, args := range [][]string{{"--dry-run"}, {"--dry-run", "--repo", ".."}} {
		command := exec.Command(python, append([]string{script, "--evidence-dir", evidence}, args...)...)
		command.Env = []string{"HOME=" + home, "PATH=/usr/bin:/bin", "WEFTY_L1_ADDR=127.0.0.1:47101", "WEFTY_L3_ADDR=127.0.0.1:47102"}
		output, err := command.CombinedOutput()
		if err != nil {
			// A future source change may correctly refuse the pinned review.
			// The plan must still be complete without any installation.
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 1 || !strings.Contains(string(output), "compilation policy REFUSE") {
				t.Fatalf("plan failed: %v\n%s", err, output)
			}
		}
		if !strings.Contains(string(output), "PLAN ONLY") || !strings.Contains(string(output), "dev.wefty-b1-proof.") {
			t.Fatalf("missing plan/isolation: %s", output)
		}
		if i == 0 {
			first = string(output)
		} else if string(output) != first {
			t.Fatal("normalized default and explicit repository plans differ")
		}
		if _, err := os.Lstat(home); !os.IsNotExist(err) {
			t.Fatalf("plan touched HOME: %v", err)
		}
	}
}

// Safety regressions import the runner and inject all service-manager effects.
// They never invoke the installed trial or a real launchctl process.
func TestRollingUpgradeSafetyRegressions(t *testing.T) {
	command := exec.Command("python3", "test-rolling-upgrade.py")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("B1 runner safety: %v\n%s", err, output)
	}
}

func TestRollingUpgradePlanAdapterSyntax(t *testing.T) {
	payload, err := os.ReadFile("testdata/rolling-upgrade-plan.go.tmpl")
	if err != nil {
		t.Fatal(err)
	}
	source := strings.ReplaceAll(string(payload), "@@ENV@@", `{"GOMAXPROCS":"2"}`)
	source = strings.ReplaceAll(source, "@@PATH@@", `"/usr/bin:/bin"`)
	if _, err := parser.ParseFile(token.NewFileSet(), "b1_proof_plan.go", source, parser.AllErrors); err != nil {
		t.Fatal(err)
	}
}

func TestRollingUpgradeRound2SafetyRegressions(t *testing.T) {
	command := exec.Command("python3", "test-rolling-upgrade-round2.py")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("B1 ownership safety: %v\n%s", err, output)
	}
}

// Fake namespace and session observations; no installed trial or launchctl.
func TestRollingUpgradeRound3SafetyRegressions(t *testing.T) {
	command := exec.Command("python3", "test-rolling-upgrade-round3.py")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("B1 recovery identity: %v\n%s", err, output)
	}
}

// Explicit GUI-domain observations and source checks use fake launchd only.
func TestRollingUpgradeRound4SafetyRegressions(t *testing.T) {
	if runtime.GOOS != "darwin" {
		// These fixtures use macOS process birth identities and session
		// semantics; the runner itself only runs on macOS.
		t.Skip("round-4 runner safety fixtures need macOS process identity")
	}
	command := exec.Command("python3", "test-rolling-upgrade-round4.py")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("B1 GUI/source compatibility: %v\n%s", err, output)
	}
}
