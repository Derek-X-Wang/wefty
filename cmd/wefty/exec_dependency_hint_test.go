package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestExecIgnoredDependencyHint(t *testing.T) {
	const nodeHint = "wefty: this checkout's node_modules is Git-ignored, so the run did not get it; install inside the command, for example \"bun install --frozen-lockfile && ...\" or \"pnpm install --frozen-lockfile && ...\".\n"
	const venvHint = "wefty: this checkout's .venv is Git-ignored, so the run did not get it; install inside the command, for example \"uv sync && ...\".\n"
	for _, tc := range []struct {
		name, ignore, document, cancel, want string
		dirs                                 []string
		status                               contract.RunState
		tracked, file, gitError              bool
		code                                 int
	}{
		{name: "ignored node_modules", ignore: "node_modules/\n", dirs: []string{"sub/nested/node_modules"}, want: nodeHint},
		{name: "passing run", ignore: "node_modules/\n", dirs: []string{"node_modules"}, status: contract.RunSucceeded},
		{name: "missing node_modules", ignore: "node_modules/\n"},
		{name: "tracked node_modules", dirs: []string{"node_modules"}, tracked: true},
		{name: "untracked not ignored", dirs: []string{"node_modules"}},
		{name: "ignored file not folder", ignore: "node_modules\n", file: true},
		{name: "ancestor and root only once", ignore: "node_modules/\n", dirs: []string{"node_modules", "sub/node_modules", "sub/nested/node_modules"}, want: nodeHint},
		{name: "ignored venv", ignore: ".venv/\n", dirs: []string{"sub/.venv"}, want: venvHint},
		{name: "both dependencies", ignore: "node_modules/\n.venv/\n", dirs: []string{"node_modules", "sub/.venv"}, want: nodeHint + venvHint},
		{name: "exit 127", ignore: "node_modules/\n", dirs: []string{"node_modules"}, document: `{"exit_code":127}`, want: nodeHint},
		{name: "sibling ignored folder", ignore: "node_modules/\n", dirs: []string{"other/node_modules"}},
		{name: "outside repo ignored folder", ignore: "node_modules/\n"},
		{name: "git error", ignore: "node_modules/\n", dirs: []string{"node_modules"}, gitError: true},
		{name: "cancelled", ignore: "node_modules/\n", dirs: []string{"node_modules"}, cancel: "settled"},
		{name: "infrastructure failure", ignore: "node_modules/\n", dirs: []string{"node_modules"}, document: `{"exit_code":1,"infrastructure_error":"checkout failed"}`, code: exitUnavailable},
	} {
		for _, jsonOutput := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%v", tc.name, jsonOutput), func(t *testing.T) {
				t.Setenv("WEFTY_EXEC_STORAGE_ROOT", t.TempDir())
				repo := execTestRepo(t)
				writeExecFile(t, filepath.Join(repo, ".gitignore"), tc.ignore)
				writeExecFile(t, filepath.Join(repo, "sub/nested/tracked"), "base")
				for _, dir := range tc.dirs {
					writeExecFile(t, filepath.Join(repo, dir, "dependency"), "installed")
				}
				if tc.tracked {
					execTestGit(t, repo, "add", "node_modules")
				}
				if tc.file {
					writeExecFile(t, filepath.Join(repo, "node_modules"), "not a folder")
				}
				if tc.name == "outside repo ignored folder" {
					writeExecFile(t, filepath.Join(filepath.Dir(repo), "node_modules/dependency"), "outside")
				}
				capture, err := captureExec(t.Context(), filepath.Join(repo, "sub/nested"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := cleanupExecStorage(capture.Storage); err != nil {
						t.Error(err)
					}
				})
				// Finish from another folder: hints must use the captured checkout.
				t.Chdir(t.TempDir())
				if tc.gitError {
					t.Setenv("PATH", t.TempDir())
				}
				status, document := tc.status, tc.document
				if status == "" {
					status = contract.RunFailed
				}
				if document == "" {
					document = `{"exit_code":1}`
				}
				record := contract.RunRecord{RunID: "finished", Status: status, FailureReason: "command failed", CancelStatus: tc.cancel}
				clients := newStatusHarness(t).clients()
				clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path != "/v1/runs/finished/result" {
						t.Fatalf("unexpected read: %s", r.URL.Path)
					}
					raw, err := json.Marshal(l3.RunResult{Document: []byte(document)})
					if err != nil {
						t.Fatal(err)
					}
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(raw))}, nil
				})
				var out, stderr bytes.Buffer
				err = reportExecFinished(t.Context(), clients, record, capture, jsonOutput, &out, &stderr)
				wantCode := tc.code
				if wantCode == 0 && status == contract.RunFailed {
					wantCode = exitRunFailed
				}
				if got := execModeCode(err); got != wantCode {
					t.Fatalf("exit=%d want=%d err=%v", got, wantCode, err)
				}
				var hints strings.Builder
				for _, line := range strings.SplitAfter(stderr.String(), "\n") {
					if strings.HasPrefix(line, "wefty: this checkout's") {
						hints.WriteString(line)
					}
				}
				if hints.String() != tc.want {
					t.Errorf("hints=%q want=%q stderr=%q", hints.String(), tc.want, &stderr)
				}
				if tc.name == "exit 127" {
					if strings.Count(stderr.String(), "wefty: exit 127") != 1 || strings.Index(stderr.String(), "wefty: exit 127") > strings.Index(stderr.String(), nodeHint) {
						t.Errorf("expected PATH hint then dependency hint: %s", &stderr)
					}
				}
				if strings.Contains(out.String(), "wefty:") {
					t.Errorf("hint leaked to stdout: %s", &out)
				}
				if jsonOutput {
					events := execModeEvents(t, out.String())
					if len(events) != 1 || events[0]["type"] != "finished" {
						t.Errorf("unexpected JSON events: %s", &out)
					}
				}
			})
		}
	}
}
