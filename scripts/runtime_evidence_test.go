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
	teardown := `"teardown":{"retries_used":0,"permission_repair_performed":false,"observations":[],"leftovers":[]}`
	zero := write("zero.json", `{"version":2,"checks":[{"id":"expected","status":"PASS","detail":"ok"}],`+teardown+`}`)
	wrong := write("wrong.json", `{"version":2,"checks":[{"id":"other","status":"FAIL","detail":"wrong"}],`+teardown+`}`)
	positive := write("positive.json", `{"version":2,"status":"PASS","checks":[],`+teardown+`}`)
	oldPositive := write("old-positive.json", `{"version":1,"status":"PASS","checks":[],`+teardown+`}`)
	nonPass := write("non-pass.json", `{"version":2,"status":"FAIL","checks":[],`+teardown+`}`)
	missingTeardown := write("missing-teardown.json", `{"version":2,"status":"PASS","checks":[]}`)
	leftover := write("leftover.json", `{"version":2,"checks":[{"id":"expected","status":"FAIL","detail":"detail"}],"teardown":{"retries_used":0,"permission_repair_performed":false,"observations":[],"leftovers":["temporary-root:/tmp/example"]}}`)
	repair := write("repair.json", `{"version":2,"checks":[],"teardown":{"retries_used":0,"permission_repair_performed":true,"permission_repair_seconds":1.25,"observations":[],"leftovers":[]}}`)
	unmeasuredRepair := write("unmeasured-repair.json", `{"version":2,"checks":[],"teardown":{"retries_used":0,"permission_repair_performed":true,"observations":[],"leftovers":[]}}`)
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
		"old positive":      {[]string{"positive", oldPositive}, "receipt/positive-runtime"},
		"non-pass positive": {[]string{"positive", nonPass}, "receipt/positive-runtime"},
		"missing teardown":  {[]string{"positive", missingTeardown}, "receipt/positive-runtime"},
		"teardown leftover": {[]string{"mutation", leftover, "row", "expected", "detail", "1", "3"}, "receipt/row"},
		"repair wrong exit": {[]string{"teardown-repair", repair, "1"}, "teardown-repair"},
		"repair unmeasured": {[]string{"teardown-repair", unmeasuredRepair, "2"}, "teardown-repair"},
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

	command := exec.Command("bash", "./check-computer-image-runtime-evidence.sh", "positive", positive)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("valid positive receipt failed: %v: %s", err, output)
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
