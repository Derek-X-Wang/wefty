package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise the capture, upload and shipped checkout wrapper with both Git
// object formats. Bundle SHA-256 is independent of the repository's format.
func TestExecCaptureBundleGitObjectFormats(t *testing.T) {
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			execTestStorageRoot(t)
			repo := t.TempDir()
			execTestGit(t, repo, "init", "-q", "--object-format="+format)
			writeExecFile(t, filepath.Join(repo, "tracked"), "base")
			execTestGit(t, repo, "add", ".")
			execTestGit(t, repo, "commit", "-qm", "base")
			base := strings.TrimSpace(execTestGit(t, repo, "rev-parse", "HEAD"))
			writeExecFile(t, filepath.Join(repo, "tracked"), "changed")
			writeExecFile(t, filepath.Join(repo, "new"), "new-file")
			capture, err := captureExec(t.Context(), repo)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := cleanupExecStorage(capture.Storage); err != nil {
					t.Error(err)
				}
			})
			length := 40
			if format == "sha256" {
				length = 64
			}
			if len(capture.Base) != length || len(capture.Tree) != length {
				t.Fatalf("capture identities: %+v", capture)
			}
			mailbox := t.TempDir()
			clients := &apiClients{l3: &apiClient{client: &http.Client{Transport: handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				bundle, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(bundle)
				hash := hex.EncodeToString(digest[:])
				if r.Method != http.MethodPost || r.URL.Path != "/v1/exec-captures/"+hash || r.ContentLength != int64(len(bundle)) {
					t.Fatalf("upload request: %+v", r)
				}
				if err := os.WriteFile(filepath.Join(mailbox, "capture.bundle"), bundle, 0600); err != nil {
					t.Fatal(err)
				}
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(map[string]any{"sha256": hash, "size_bytes": len(bundle)})
			})}}}}
			capture, err = uploadExecBundle(t.Context(), clients, capture)
			if err != nil {
				t.Fatal(err)
			}
			wrapper, err := buildExecBundleWrapper(capture, []string{"/bin/sh", "-c", `printf '%s\n' "$(git rev-parse --show-object-format)" "$(git rev-parse HEAD)"; cat tracked new; git status --porcelain`})
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), "/bin/bash", "-c", wrapper)
			cmd.Env = append(os.Environ(), "WEFTY_RUN_DIR="+mailbox)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("checkout wrapper: %v: %s", err, out)
			}
			for _, want := range []string{format + "\n" + base + "\n", "changednew-file", " M tracked", "?? new"} {
				if !strings.Contains(string(out), want) {
					t.Errorf("output lacks %q: %s", want, out)
				}
			}
			waitExecDeleteSteps(t, mailbox)
		})
	}
}
