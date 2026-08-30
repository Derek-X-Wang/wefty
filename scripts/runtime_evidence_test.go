package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRuntimeEvidenceDiagnosticsNameEveryTerminalFailure(t *testing.T) {
	temp := t.TempDir()
	write := func(name, payload string) string {
		path := filepath.Join(temp, name)
		if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	zero := write("zero.json", `{"checks":[{"id":"expected","status":"PASS","detail":"ok"}]}`)
	wrong := write("wrong.json", `{"checks":[{"id":"other","status":"FAIL","detail":"wrong"}]}`)
	malformed := write("malformed.json", `{not-json`)
	badSummary := write("summary.json", `{"version":1,"platform":"linux/arm64","executed_rows":19}`)

	for name, fixture := range map[string]struct {
		args []string
		want string
	}{
		"zero fail":         {[]string{"mutation", zero, "row", "expected", "detail", "1", "3"}, "fail-set/row"},
		"wrong fail set":    {[]string{"mutation", wrong, "row", "expected", "detail", "1", "3"}, "fail-set/row"},
		"malformed receipt": {[]string{"mutation", malformed, "row", "expected", "detail", "1", "3"}, "receipt/row"},
		"missing receipt":   {[]string{"mutation", filepath.Join(temp, "missing.json"), "row", "expected", "detail", "1", "3"}, "receipt/row"},
		"row count":         {[]string{"summary", badSummary, "linux/arm64", "20"}, "row-count"},
	} {
		t.Run(name, func(t *testing.T) {
			command := exec.Command("bash", append([]string{"./check-computer-image-runtime-evidence.sh"}, fixture.args...)...)
			output, err := command.CombinedOutput()
			if err == nil {
				t.Fatalf("diagnostic unexpectedly passed: %s", output)
			}
			if !strings.Contains(string(output), "::error title=computer runtime conformance::"+fixture.want+":") {
				t.Fatalf("output %q does not name %q", output, fixture.want)
			}
		})
	}
}

func TestDriverWatcherUsesOneExactByteRead(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skipf("python3 unavailable; watcher exact-byte regression not run: %v", err)
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not resolve runtime_evidence_test.go path")
	}
	repositoryRoot := filepath.Dir(filepath.Dir(source))
	testPath := filepath.Join(repositoryRoot, "examples", "computer", "test_watch_driver.py")
	command := exec.Command(python, testPath)
	command.Dir = repositoryRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("driver watcher regression failed: %v\n%s", err, output)
	}
}
