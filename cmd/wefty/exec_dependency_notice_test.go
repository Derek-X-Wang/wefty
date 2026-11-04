package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestExecIgnoredDependencyNotice(t *testing.T) {
	for _, tc := range []struct {
		name, ignore, folder               string
		dirs, want                         []string
		tracked, file, gitError, cancelled bool
		keep, empty                        bool
	}{
		{name: "ignored root", ignore: "node_modules/\n", dirs: []string{"node_modules"}, want: []string{"node_modules"}},
		{name: "ignored node contents", ignore: "node_modules/*\n", dirs: []string{"node_modules"}, want: []string{"node_modules"}},
		{name: "ignored venv contents", ignore: ".venv/*\n", dirs: []string{".venv"}, want: []string{".venv"}},
		{name: "negation keeps captured file", ignore: "node_modules/*\n!node_modules/keep.txt\n", dirs: []string{"node_modules"}, keep: true},
		{name: "overlapping candidates", folder: "node_modules/workspace", ignore: "/node_modules/*\n!/node_modules/workspace/\n/node_modules/workspace/node_modules/\n", dirs: []string{"node_modules", "node_modules/workspace", "node_modules/workspace/node_modules"}, want: []string{"node_modules/workspace/node_modules"}},
		{name: "empty unignored folder", dirs: []string{"node_modules"}, empty: true},
		{name: "absent", ignore: "node_modules/\n"},
		{name: "not ignored", dirs: []string{"node_modules"}},
		{name: "tracked despite ignore rule", ignore: "node_modules/\n", dirs: []string{"node_modules"}, tracked: true},
		{name: "file", ignore: "node_modules\n", file: true},
		{name: "root and command folder", folder: "sub/nested", ignore: "node_modules/\n.venv/\n", dirs: []string{"node_modules", ".venv", "sub/nested/node_modules", "sub/nested/.venv"}, want: []string{"node_modules", ".venv", "sub/nested/node_modules", "sub/nested/.venv"}},
		{name: "no ancestor or sibling scan", folder: "sub/nested", ignore: "node_modules/\n", dirs: []string{"sub/node_modules", "other/node_modules"}},
		{name: "git failure", ignore: "node_modules/\n", dirs: []string{"node_modules"}, gitError: true},
		{name: "cancelled", ignore: "node_modules/\n", dirs: []string{"node_modules"}, cancelled: true},
		{name: "local excludes", dirs: []string{"node_modules"}, want: []string{"node_modules"}},
		{name: "global excludes", dirs: []string{"node_modules"}, want: []string{"node_modules"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			execTestStorageRoot(t)
			repo := execTestRepo(t)
			writeExecFile(t, filepath.Join(repo, "sub/nested/tracked"), "base")
			for _, dir := range tc.dirs {
				writeExecFile(t, filepath.Join(repo, dir, "dependency"), "installed")
				if tc.empty {
					if err := os.Remove(filepath.Join(repo, dir, "dependency")); err != nil {
						t.Fatal(err)
					}
				}
			}
			if tc.keep {
				writeExecFile(t, filepath.Join(repo, "node_modules/keep.txt"), "captured")
			}
			if tc.tracked {
				execTestGit(t, repo, "add", "node_modules")
			}
			writeExecFile(t, filepath.Join(repo, ".gitignore"), tc.ignore)
			if tc.file {
				writeExecFile(t, filepath.Join(repo, "node_modules"), "not a directory")
			}
			if tc.name == "local excludes" {
				writeExecFile(t, filepath.Join(repo, ".git/info/exclude"), "node_modules/\n")
			}
			if tc.name == "global excludes" {
				exclude := filepath.Join(t.TempDir(), "ignore")
				writeExecFile(t, exclude, "node_modules/\n")
				execTestGit(t, repo, "config", "core.excludesFile", exclude)
			}
			folder := tc.folder
			if folder == "" {
				folder = "."
			}
			capture, err := captureExec(t.Context(), filepath.Join(repo, folder))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cleanupExecStorage(capture.Storage) })
			if tc.gitError {
				t.Setenv("PATH", t.TempDir())
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			// Ambient Git overrides must not change the capture's Git view.
			t.Setenv("GIT_DIR", filepath.Join(t.TempDir(), "not-the-checkout"))
			var stderr bytes.Buffer
			shown := writeExecIgnoredDependencyNotice(ctx, &stderr, &capture)
			if shown != (len(tc.want) > 0) {
				t.Fatalf("notice shown=%v want=%v", shown, len(tc.want) > 0)
			}
			if len(tc.want) == 0 {
				if stderr.Len() != 0 {
					t.Fatalf("unexpected notice: %s", &stderr)
				}
				return
			}
			var paths []string
			for _, path := range tc.want {
				paths = append(paths, fmt.Sprintf("%q", path))
			}
			want := "wefty: ignored dependency folders " + strings.Join(paths, ", ") + " will not be present in the run; install dependencies inside the command, for example: wefty exec -- sh -c 'npm ci && npm test' (Node.js) or wefty exec -- sh -c 'uv sync && uv run pytest' (Python).\n"
			if stderr.String() != want {
				t.Fatalf("notice=%q want=%q", stderr.String(), want)
			}
			// Failure reporting must not repeat an advisory already shown.
			writeExecDependencyHints(ctx, &stderr, execFinished{Outcome: "fail"}, capture)
			if stderr.String() != want {
				t.Fatalf("failure repeated the notice: %q", stderr.String())
			}
		})
	}
}

func TestExecIgnoredDependencyNoticeBeforeSubmission(t *testing.T) {
	for _, jsonOutput := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%v", jsonOutput), func(t *testing.T) {
			execTestStorageRoot(t)
			repo := execTestRepo(t)
			writeExecFile(t, filepath.Join(repo, ".gitignore"), "node_modules/\n")
			t.Chdir(repo)
			h := newStatusHarness(t)
			node := readyNode("notice-test")
			node.AuthoritativeTags = []string{"wefty:node:notice-test"}
			h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
			clients := h.clients()
			clients.nodeConfigPath = filepath.Join(t.TempDir(), "node.json")
			writeExecFile(t, clients.nodeConfigPath, `{"version":1,"node_id":"notice-test","control_socket":"/tmp/unused.sock"}`)
			base := clients.l3.client.Transport
			var out, stderr bytes.Buffer
			present, writeFails := false, false
			submissions := 0
			clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/runs" {
					return base.RoundTrip(r)
				}
				submissions++
				if got, want := strings.Contains(stderr.String(), "ignored dependency folders"), present && !writeFails; got != want {
					t.Fatalf("notice before submit=%v want=%v stderr=%q", got, want, stderr.String())
				}
				var request l3.CreateRunRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Fatal(err)
				}
				var params execParams
				if err := json.Unmarshal(request.Params, &params); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = cleanupExecStorage(params.Storage) })
				files, err := execGit(t.Context(), repo, filepath.Join(params.Storage, "snapshot.git"), nil, "ls-tree", "-r", "--name-only", params.Tree)
				if err != nil || strings.Contains(files, "node_modules") {
					t.Fatalf("ignored dependency captured: files=%q err=%v", files, err)
				}
				return &http.Response{StatusCode: http.StatusCreated, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"run_id":"notice-run"}`))}, nil
			})
			var baseline string
			for _, tc := range []struct{ present, writeFails bool }{{}, {present: true}, {present: true, writeFails: true}} {
				present, writeFails = tc.present, tc.writeFails
				if present {
					writeExecFile(t, filepath.Join(repo, "node_modules/dependency"), "installed")
				}
				out.Reset()
				stderr.Reset()
				var noticeOutput io.Writer = &stderr
				if writeFails {
					// A failed notice write must not prevent submission either.
					noticeOutput = &failingExecNoticeWriter{}
				}
				err := executeExec(t.Context(), clients, jsonOutput, []string{"--detach", "--", "true"}, &out, noticeOutput)
				if code := execModeCode(err); code != 0 {
					t.Fatalf("exit=%d err=%v stderr=%s", code, err, &stderr)
				}
				if !present {
					baseline = out.String()
					if stderr.Len() != 0 {
						t.Fatalf("unexpected baseline stderr: %s", &stderr)
					}
				} else if out.String() != baseline || (!writeFails && strings.Count(stderr.String(), "ignored dependency folders") != 1) || (writeFails && stderr.Len() != 0) {
					t.Fatalf("stdout changed: got=%q baseline=%q stderr=%q", out.String(), baseline, stderr.String())
				}
			}
			if submissions != 3 {
				t.Fatalf("submissions=%d want=3", submissions)
			}
		})
	}
}

func TestExecIgnoredDependencyNoticeKeepsOtherFailureHints(t *testing.T) {
	execTestStorageRoot(t)
	repo := execTestRepo(t)
	writeExecFile(t, filepath.Join(repo, ".gitignore"), "node_modules/\n.venv/\n")
	writeExecFile(t, filepath.Join(repo, "node_modules/dependency"), "installed")
	writeExecFile(t, filepath.Join(repo, "sub/.venv/dependency"), "installed")
	writeExecFile(t, filepath.Join(repo, "sub/nested/tracked"), "base")
	capture, err := captureExec(t.Context(), filepath.Join(repo, "sub/nested"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanupExecStorage(capture.Storage) })
	var notice, hints bytes.Buffer
	if !writeExecIgnoredDependencyNotice(t.Context(), &notice, &capture) {
		t.Fatal("missing pre-submit notice")
	}
	writeExecDependencyHints(t.Context(), &hints, execFinished{Outcome: "fail"}, capture)
	if strings.Contains(hints.String(), "node_modules") || !strings.Contains(hints.String(), "this checkout's .venv is Git-ignored") || strings.Count(hints.String(), "\n") != 1 {
		t.Fatalf("expected only the unnoticed .venv hint: %q", hints.String())
	}
	// A rejected write must record no kinds and must leave failure hints enabled.
	capture.noticedDependencies = nil
	rejected := &failingExecNoticeWriter{}
	if writeExecIgnoredDependencyNotice(t.Context(), rejected, &capture) || rejected.calls != 1 || len(capture.noticedDependencies) != 0 {
		t.Fatalf("failed write counted as shown: calls=%d kinds=%v", rejected.calls, capture.noticedDependencies)
	}
	hints.Reset()
	writeExecDependencyHints(t.Context(), &hints, execFinished{Outcome: "fail"}, capture)
	if !strings.Contains(hints.String(), "this checkout's node_modules is Git-ignored") {
		t.Fatalf("failed notice suppressed the node_modules hint: %q", hints.String())
	}
}

func TestExecIgnoredDependencyNoticeNoFoldersRunsNoGit(t *testing.T) {
	repo := execTestRepo(t)
	marker := filepath.Join(t.TempDir(), "git-called")
	bin := t.TempDir()
	git := filepath.Join(bin, "git")
	writeExecFile(t, git, "#!/bin/sh\nprintf called > \"$WEFTY_NOTICE_GIT_MARKER\"\nexit 1\n")
	if err := os.Chmod(git, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Setenv("WEFTY_NOTICE_GIT_MARKER", marker)
	var stderr bytes.Buffer
	if writeExecIgnoredDependencyNotice(t.Context(), &stderr, &execCapture{localRoot: repo, Folder: "sub"}) || stderr.Len() != 0 {
		t.Fatalf("unexpected notice: %q", stderr.String())
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("Git ran without any candidate folders: %v", err)
	}
}

type failingExecNoticeWriter struct{ calls int }

func (w *failingExecNoticeWriter) Write([]byte) (int, error) {
	w.calls++
	return 0, os.ErrPermission
}
