package scripts

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/computerconformance"
)

func TestReadinessCheckEventPairingsMatchRecorderAndJQ(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not resolve runtime_evidence_test.go path")
	}
	repositoryRoot := filepath.Dir(filepath.Dir(source))
	read := func(relative string) string {
		t.Helper()
		payload, err := os.ReadFile(filepath.Join(repositoryRoot, relative))
		if err != nil {
			t.Fatal(err)
		}
		return string(payload)
	}
	type scriptPairing struct {
		Check  string   `json:"check"`
		Events []string `json:"events"`
	}
	script := read("scripts/check-computer-image-runtime-evidence.sh")
	match := regexp.MustCompile(`(?s)readonly readiness_pairings='([^']+)'`).FindStringSubmatch(script)
	if len(match) != 2 {
		t.Fatal("jq readiness pairing table is missing")
	}
	var scriptPairings []scriptPairing
	if err := json.Unmarshal([]byte(match[1]), &scriptPairings); err != nil {
		t.Fatalf("parse jq readiness pairing table: %v", err)
	}
	canonical := func(values map[string][]string) []string {
		var pairs []string
		for check, events := range values {
			for _, event := range events {
				pairs = append(pairs, check+"="+event)
			}
		}
		sort.Strings(pairs)
		return pairs
	}
	recorderPairings := make(map[string][]string)
	allEvents := make(map[string]bool)
	for check, events := range computerconformance.ReadinessEventPairings() {
		for _, event := range events {
			recorderPairings[check] = append(recorderPairings[check], string(event))
			allEvents[string(event)] = true
		}
	}
	jqPairings := make(map[string][]string)
	for _, pairing := range scriptPairings {
		jqPairings[pairing.Check] = append(jqPairings[pairing.Check], pairing.Events...)
	}
	if recorder, jq := strings.Join(canonical(recorderPairings), "\n"), strings.Join(canonical(jqPairings), "\n"); recorder != jq {
		t.Fatalf("readiness check/event pairings differ:\nrecorder:\n%s\njq:\n%s", recorder, jq)
	}

	temp := t.TempDir()
	for checkID, events := range recorderPairings {
		allowed := make(map[string]bool, len(events))
		for _, event := range events {
			allowed[event] = true
		}
		for event := range allEvents {
			path := filepath.Join(temp, strings.ReplaceAll(checkID+"-"+event, ".", "-")+".json")
			payload := `{"version":2,"checks":[` +
				`{"id":"persistence.edge-recovers","status":"FAIL","detail":"expected","failure_reason":"mutation_detected"},` +
				`{"id":"` + checkID + `","status":"FAIL","detail":"late","failure_reason":"readiness_timeout","readiness_event":"` + event + `","readiness_observation_window_seconds":1.5,"readiness_observation_elapsed_seconds":1.5,"readiness_observed_later":true},` +
				`{"id":"input.control-accepted","status":"PASS","detail":"observed"}],` +
				`"teardown":{"retries_used":0,"permission_repair_performed":false,"observations":[],"leftovers":[]}}`
			if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("bash", filepath.Join(repositoryRoot, "scripts/check-computer-image-runtime-evidence.sh"), "mutation", path, "edge-does-not-recover", "persistence.edge-recovers", "expected", "1", "3")
			_, err := command.CombinedOutput()
			if (err == nil) != allowed[event] {
				t.Fatalf("jq readiness pairing %s -> %s accepted=%t, want %t: %v", checkID, event, err == nil, allowed[event], err)
			}
		}
	}
}

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
	wrong := write("wrong.json", `{"version":2,"checks":[{"id":"other","status":"FAIL","detail":"wrong","failure_reason":"assertion_failed"}],`+teardown+`}`)
	positive := write("positive.json", `{"version":2,"status":"PASS","checks":[],`+teardown+`}`)
	oldPositive := write("old-positive.json", `{"version":1,"status":"PASS","checks":[],`+teardown+`}`)
	nonPass := write("non-pass.json", `{"version":2,"status":"FAIL","checks":[],`+teardown+`}`)
	missingTeardown := write("missing-teardown.json", `{"version":2,"status":"PASS","checks":[]}`)
	leftover := write("leftover.json", `{"version":2,"checks":[{"id":"expected","status":"FAIL","detail":"detail","failure_reason":"mutation_detected"}],"teardown":{"retries_used":0,"permission_repair_performed":false,"observations":[],"leftovers":["temporary-root:/tmp/example"]}}`)
	bogusReadiness := write("bogus-readiness.json", `{"version":2,"checks":[{"id":"expected","status":"FAIL","detail":"detail","failure_reason":"mutation_detected"},{"id":"other","status":"FAIL","detail":"late","failure_reason":"readiness_timeout","readiness_event":"eventually"}],`+teardown+`}`)
	repair := write("repair.json", `{"version":2,"checks":[],"teardown":{"retries_used":0,"permission_repair_performed":true,"permission_repair_seconds":1.25,"observations":[],"leftovers":[]}}`)
	unmeasuredRepair := write("unmeasured-repair.json", `{"version":2,"checks":[],"teardown":{"retries_used":0,"permission_repair_performed":true,"observations":[],"leftovers":[]}}`)
	malformed := write("malformed.json", `{not-json`)
	badSummary := write("summary.json", `{"version":1,"platform":"linux/arm64","executed_rows":19}`)

	for name, fixture := range map[string]struct {
		args []string
		want string
	}{
		"zero fail":               {[]string{"mutation", zero, "row", "expected", "detail", "1", "3"}, "fail-set/row"},
		"wrong fail set":          {[]string{"mutation", wrong, "row", "expected", "detail", "1", "3"}, "fail-set/row"},
		"malformed receipt":       {[]string{"mutation", malformed, "row", "expected", "detail", "1", "3"}, "receipt/row"},
		"missing receipt":         {[]string{"mutation", filepath.Join(temp, "missing.json"), "row", "expected", "detail", "1", "3"}, "receipt/row"},
		"old positive":            {[]string{"positive", oldPositive}, "receipt/positive-runtime"},
		"non-pass positive":       {[]string{"positive", nonPass}, "receipt/positive-runtime"},
		"missing teardown":        {[]string{"positive", missingTeardown}, "receipt/positive-runtime"},
		"teardown leftover":       {[]string{"mutation", leftover, "row", "expected", "detail", "1", "3"}, "receipt/row"},
		"unknown readiness event": {[]string{"mutation", bogusReadiness, "row", "expected", "detail", "1", "3"}, "receipt/row"},
		"repair wrong exit":       {[]string{"teardown-repair", repair, "1"}, "teardown-repair"},
		"repair unmeasured":       {[]string{"teardown-repair", unmeasuredRepair, "2"}, "teardown-repair"},
		"row count":               {[]string{"summary", badSummary, "linux/arm64", "20"}, "row-count"},
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

func TestRuntimeEvidenceReplaysIssue320ReadinessRows(t *testing.T) {
	temp := t.TempDir()
	write := func(name, checks string) string {
		path := filepath.Join(temp, name+".json")
		payload := `{"version":2,"checks":` + checks + `,"teardown":{"retries_used":0,"permission_repair_performed":false,"observations":[],"leftovers":[]}}`
		if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	mutation := func(name, expectedID, expectedDetail, readinessID, readinessEvent string, observedLater bool) {
		t.Helper()
		checks := `[` +
			`{"id":"` + expectedID + `","status":"FAIL","detail":"` + expectedDetail + `","failure_reason":"mutation_detected"}`
		if readinessID != "" {
			recovered := ""
			if observedLater {
				recovered = `,"readiness_observed_later":true`
			}
			checks += `,` +
				`{"id":"` + readinessID + `","status":"FAIL","detail":"readiness event was not observed","failure_reason":"readiness_timeout","readiness_event":"` + readinessEvent + `","readiness_observation_window_seconds":18.588,"readiness_observation_elapsed_seconds":18.625` + recovered + `},` +
				`{"id":"input.control-accepted","status":"PASS","detail":"control input observed"}`
		}
		checks += `]`
		receipt := write(name, checks)
		command := exec.Command("bash", "./check-computer-image-runtime-evidence.sh", "mutation", receipt, name, expectedID, expectedDetail, "1", "29")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("recorded evidence %s rejected: %v\n%s", name, err, output)
		}
		if readinessID != "" && !strings.Contains(string(output), "readiness-timeout/"+name) {
			t.Fatalf("recorded evidence %s did not surface its typed readiness timeout: %s", name, output)
		}
	}

	// These are post-fix projections of the recorded rows: receipt v1 did
	// not carry failure reasons, readiness pairings, observation timing, or
	// later-event recovery evidence, so the literal artifacts remain invalid.
	// The missing-control classifier emits only its owning mutation failure; it
	// cannot emit a second readiness row with the same check ID.
	mutation("profile-state-lost", "persistence.profile-survives", "profile marker under HOME was lost", "input.view-isolated-during-tenure", "key_observer_advanced", true)
	mutation("edge-does-not-recover", "persistence.edge-recovers", "both endpoints did not recover after edge withdrawal", "input.view-isolated-during-tenure", "key_observer_advanced", true)
	mutation("missing-control-endpoint", "transport.control-ready", "control never completed rfb-websocket-v1", "", "", false)

	positive := write("pr-323-positive", `[{"id":"input.view-isolated-during-tenure","status":"FAIL","detail":"key observer did not advance after the control liveness keystroke (key_events=2 observer_lines=0)","failure_reason":"readiness_timeout","readiness_event":"key_observer_advanced"}]`)
	positivePayload, err := os.ReadFile(positive)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(positive, []byte(strings.Replace(string(positivePayload), `"version":2`, `"version":2,"status":"FAIL"`, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("bash", "./check-computer-image-runtime-evidence.sh", "positive", positive)
	if output, err := command.CombinedOutput(); err == nil || !strings.Contains(string(output), "receipt/positive-runtime") {
		t.Fatalf("PR #323 positive-control readiness failure did not fail closed: err=%v output=%s", err, output)
	}

	wrong := write("wrong-fail-set", `[`+
		`{"id":"persistence.profile-survives","status":"FAIL","detail":"profile marker under HOME was lost","failure_reason":"mutation_detected"},`+
		`{"id":"harness.rootfs-read-only","status":"FAIL","detail":"write failed with EACCES, not EROFS","failure_reason":"assertion_failed"}`+
		`]`)
	command = exec.Command("bash", "./check-computer-image-runtime-evidence.sh", "mutation", wrong, "profile-state-lost", "persistence.profile-survives", "profile marker under HOME was lost", "1", "29")
	if output, err := command.CombinedOutput(); err == nil || !strings.Contains(string(output), "fail-set/profile-state-lost") {
		t.Fatalf("synthetic wrong fail-set did not fail closed: err=%v output=%s", err, output)
	}
}

func TestMutationEvidenceClosesEveryReadinessMaskingBranch(t *testing.T) {
	temp := t.TempDir()
	teardown := `"teardown":{"retries_used":0,"permission_repair_performed":false,"observations":[],"leftovers":[]}`
	run := func(name, checks string) error {
		t.Helper()
		path := filepath.Join(temp, name+".json")
		if err := os.WriteFile(path, []byte(`{"version":2,"checks":`+checks+`,`+teardown+`}`), 0o600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command("bash", "./check-computer-image-runtime-evidence.sh", "mutation", path, "edge-does-not-recover", "persistence.edge-recovers", "both endpoints did not recover after edge withdrawal", "1", "29")
		_, err := command.CombinedOutput()
		return err
	}
	expected := `{"id":"persistence.edge-recovers","status":"FAIL","detail":"both endpoints did not recover after edge withdrawal","failure_reason":"mutation_detected"}`
	inputTimeout := `{"id":"input.view-isolated-during-tenure","status":"FAIL","detail":"late","failure_reason":"readiness_timeout","readiness_event":"key_observer_advanced","readiness_observation_window_seconds":18.588,"readiness_observation_elapsed_seconds":18.625}`
	controlPass := `{"id":"input.control-accepted","status":"PASS","detail":"observed"}`

	for name, checks := range map[string]string{
		"unrecovered input timeout":    `[` + expected + `,` + inputTimeout + `,` + controlPass + `]`,
		"control not run":              `[` + expected + `,` + strings.Replace(inputTimeout, `}`, `,"readiness_observed_later":true}`, 1) + `,{"id":"input.control-accepted","status":"NOT-RUN","detail":"unavailable"}]`,
		"adjacent control not run":     `[` + expected + `,{"id":"input.view-isolated","status":"PASS","detail":"observed"},{"id":"input.control-accepted","status":"NOT-RUN","detail":"unavailable"}]`,
		"duplicate expected assertion": `[` + expected + `,{"id":"persistence.edge-recovers","status":"FAIL","detail":"both endpoints did not recover after edge withdrawal","failure_reason":"assertion_failed"}]`,
		"owning reason deleted":        `[{"id":"persistence.edge-recovers","status":"FAIL","detail":"both endpoints did not recover after edge withdrawal","failure_reason":"assertion_failed"}]`,
		"invalid check event pairing":  `[` + expected + `,{"id":"input.view-isolated-during-tenure","status":"FAIL","detail":"late","failure_reason":"readiness_timeout","readiness_event":"first_rfb_frame","readiness_observation_window_seconds":1.5,"readiness_observation_elapsed_seconds":1.5,"readiness_observed_later":true},` + controlPass + `]`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(name, checks); err == nil {
				t.Fatal("malformed mutation evidence passed")
			}
		})
	}

	recovered := strings.Replace(inputTimeout, `}`, `,"readiness_observed_later":true}`, 1)
	if err := run("recovered-input-timeout", `[`+expected+`,`+recovered+`,`+controlPass+`]`); err != nil {
		t.Fatalf("closed recovered readiness evidence was rejected: %v", err)
	}
}

func TestRuntimeEvidenceAcceptsRelabelledReadinessMeasurement(t *testing.T) {
	temp := t.TempDir()
	receipt := filepath.Join(temp, "relabelled-readiness.json")
	payload := `{"version":2,"checks":[` +
		`{"id":"persistence.edge-recovers","status":"FAIL","detail":"both endpoints did not recover after edge withdrawal","failure_reason":"mutation_detected"},` +
		`{"id":"input.view-isolated-during-tenure","status":"FAIL","detail":"late","failure_reason":"assertion_failed","readiness_observation_window_seconds":18.588,"readiness_observation_elapsed_seconds":18.625}],` +
		`"teardown":{"retries_used":0,"permission_repair_performed":false,"observations":[],"leftovers":[]}}`
	if err := os.WriteFile(receipt, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("bash", "./check-computer-image-runtime-evidence.sh", "mutation", receipt, "edge-does-not-recover", "persistence.edge-recovers", "both endpoints did not recover after edge withdrawal", "1", "29")
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("unrecovered readiness assertion unexpectedly passed mutation evidence: %s", output)
	}
	if !strings.Contains(string(output), "fail-set/edge-does-not-recover") || strings.Contains(string(output), "receipt/edge-does-not-recover") {
		t.Fatalf("measured readiness assertion was treated as malformed instead of terminal: %s", output)
	}
}

func TestMutationEvidenceStillRejectsMissingMutationWhenPrimaryCardinalityGuardIsRelaxed(t *testing.T) {
	script, err := os.ReadFile("check-computer-image-runtime-evidence.sh")
	if err != nil {
		t.Fatal(err)
	}
	mutated := strings.Replace(string(script), `($mutation | length) == 1`, `($mutation | length) >= 0`, 1)
	if mutated == string(script) {
		t.Fatal("mutation cardinality guard was not found")
	}
	temp := t.TempDir()
	mutantPath := filepath.Join(temp, "relaxed-checker.sh")
	if err := os.WriteFile(mutantPath, []byte(mutated), 0o700); err != nil {
		t.Fatal(err)
	}
	receipt := filepath.Join(temp, "missing-mutation.json")
	payload := `{"version":2,"checks":[` +
		`{"id":"input.view-isolated-during-tenure","status":"FAIL","detail":"late","failure_reason":"readiness_timeout","readiness_event":"key_observer_advanced","readiness_observation_window_seconds":18.588,"readiness_observation_elapsed_seconds":18.625,"readiness_observed_later":true},` +
		`{"id":"input.control-accepted","status":"PASS","detail":"observed"}],` +
		`"teardown":{"retries_used":0,"permission_repair_performed":false,"observations":[],"leftovers":[]}}`
	if err := os.WriteFile(receipt, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("bash", mutantPath, "mutation", receipt, "edge-does-not-recover", "persistence.edge-recovers", "both endpoints did not recover after edge withdrawal", "1", "29")
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("receipt without mutation passed the relaxed checker: %s", output)
	}
}

func TestMutationEvidenceRejectsDuplicateExpectedIDRegardlessOfReason(t *testing.T) {
	temp := t.TempDir()
	receipt := filepath.Join(temp, "duplicate-expected-id.json")
	payload := `{"version":2,"checks":[` +
		`{"id":"transport.control-ready","status":"FAIL","detail":"control never completed rfb-websocket-v1","failure_reason":"mutation_detected"},` +
		`{"id":"transport.control-ready","status":"FAIL","detail":"late first frame","failure_reason":"readiness_timeout","readiness_event":"first_rfb_frame","readiness_observation_window_seconds":15,"readiness_observation_elapsed_seconds":15}],` +
		`"teardown":{"retries_used":0,"permission_repair_performed":false,"observations":[],"leftovers":[]}}`
	if err := os.WriteFile(receipt, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("bash", "./check-computer-image-runtime-evidence.sh", "mutation", receipt, "missing-control-endpoint", "transport.control-ready", "control never completed rfb-websocket-v1", "1", "29")
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("duplicate expected id passed mutation evidence: %s", output)
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
