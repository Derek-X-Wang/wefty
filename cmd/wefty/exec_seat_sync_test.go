package main

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecSeatSyncBeforeRunAndRefusal(t *testing.T) {
	binary := buildWefty(t)
	for _, mode := range []string{"inside", "refused", "outside", "bare"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			checkout := mode == "inside" || mode == "refused"
			if checkout {
				directory = execTestRepo(t)
				writeExecFile(t, filepath.Join(directory, "pkg/main.txt"), "snapshot")
				if err := os.MkdirAll(filepath.Join(directory, "node_modules"), 0700); err != nil {
					t.Fatal(err)
				}
				writeExecFile(t, filepath.Join(directory, ".gitignore"), "node_modules\n")
				writeExecFile(t, filepath.Join(directory, "node_modules/ignored"), "not sent")
				directory = filepath.Join(directory, "pkg")
			}
			if mode == "bare" {
				repo := execTestRepo(t)
				directory = filepath.Join(t.TempDir(), "bare.git")
				if _, err := execGit(t.Context(), repo, "", nil, "clone", "--bare", "--", repo, directory); err != nil {
					t.Fatal(err)
				}
			}
			t.Chdir(directory)
			t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
			t.Setenv("WEFTY_EXEC_STORAGE_ROOT", t.TempDir())
			if _, err := storeSeatCredential(execSeatTestID, "secret"); err != nil {
				t.Fatal(err)
			}
			syncs, posts := 0, 0
			var address string
			address = startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/copy-target"):
					json.NewEncoder(w).Encode(l1.SeatCopyTarget{Endpoint: "ws://" + address + "/rfb"})
				case strings.HasSuffix(r.URL.Path, "/copy"):
					syncs++
					if r.URL.Query().Get("direction") != "sync" || r.URL.Query().Get("path") != "work" || r.Header.Get(contract.SeatCredentialHeader) != "secret" {
						t.Error("sync route or credential")
					}
					tr := tar.NewReader(r.Body)
					found := false
					for {
						h, err := tr.Next()
						if err == io.EOF {
							break
						}
						if err != nil {
							t.Error(err)
							break
						}
						if strings.Contains(h.Name, ".git/") || strings.Contains(h.Name, "node_modules") {
							t.Errorf("ignored/history sent: %s", h.Name)
						}
						if h.Name == "pkg/main.txt" {
							data, _ := io.ReadAll(tr)
							found = string(data) == "snapshot"
						}
					}
					if !found {
						t.Error("captured untracked file missing")
					}
					if mode == "refused" {
						json.NewEncoder(w).Encode(l1.SeatCopyStreamEvent{Error: &contract.APIError{Code: "seat_copy_failed", Message: "sync refused"}})
						return
					}
					size := int64(8)
					json.NewEncoder(w).Encode(l1.SeatCopyStreamEvent{CopyID: "sync-1", SizeBytes: &size})
				case r.URL.Path == "/v1/runs":
					posts++
					if checkout && syncs != 1 {
						t.Error("Run submitted before sync")
					}
					var raw map[string]any
					json.NewDecoder(r.Body).Decode(&raw)
					source := raw["seat_run"].(map[string]any)
					want := "work/pkg"
					if !checkout {
						want = ""
					}
					got, _ := source["working_directory"].(string)
					if got != want {
						t.Errorf("folder=%q want=%q", got, want)
					}
					w.WriteHeader(201)
					fmt.Fprint(w, `{"run_id":"run-seat"}`)
				case strings.HasPrefix(r.URL.Path, "/v1/seat-requests/"):
					json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: execSeatTestID, State: l1.SeatLive}})
				default:
					t.Errorf("unexpected %s", r.URL.Path)
					w.WriteHeader(500)
				}
			})
			code, out, stderr := runPolishBinary(t, binary, "--fabric=plain", "--l1="+address, "--l3="+address, "--json", "exec", "--seat", execSeatTestID, "--detach", "--", "pwd")
			if mode == "refused" {
				var refusal contract.ErrorResponse
				_ = json.Unmarshal(stderr, &refusal)
				if code != exitFailure || refusal.Error.Code != "seat_copy_failed" || posts != 0 || len(out) != 0 {
					t.Fatalf("refusal code=%d posts=%d out=%s err=%s", code, posts, out, stderr)
				}
				return
			}
			if code != 0 || posts != 1 || !bytes.Contains(out, []byte(`"run_id":"run-seat"`)) {
				t.Fatalf("code=%d posts=%d out=%s err=%s", code, posts, out, stderr)
			}
			if !checkout && syncs != 0 {
				t.Fatal("outside checkout synced")
			}
		})
	}
}

func TestExecSeatSyncCapturePreservesAttributesModesAndLinks(t *testing.T) {
	directory := execTestRepo(t)
	t.Chdir(directory)
	t.Setenv("WEFTY_EXEC_STORAGE_ROOT", t.TempDir())
	writeExecFile(t, filepath.Join(directory, ".gitattributes"), "secret export-ignore\nsubstitute export-subst\n")
	writeExecFile(t, filepath.Join(directory, "secret"), "included")
	writeExecFile(t, filepath.Join(directory, "substitute"), "$Format:%H$")
	writeExecFile(t, filepath.Join(directory, "executable"), "#!/bin/sh\n")
	if err := os.Chmod(filepath.Join(directory, "executable"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../outside", filepath.Join(directory, "link")); err != nil {
		t.Fatal(err)
	}
	capture, err := captureExec(t.Context(), directory)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(capture.Storage)
	// Change the live checkout after capture. Packing must use only frozen blobs.
	writeExecFile(t, filepath.Join(directory, "secret"), "later")
	var output bytes.Buffer
	if err := packSeatCapture(t.Context(), &output, capture); err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(&output)
	seen := map[string]bool{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(tr)
		seen[h.Name] = true
		switch h.Name {
		case "secret":
			if string(data) != "included" {
				t.Fatalf("capture bytes=%q", data)
			}
		case "substitute":
			if string(data) != "$Format:%H$" {
				t.Fatalf("attribute rewrote bytes: %q", data)
			}
		case "link":
			if h.Typeflag != tar.TypeSymlink || h.Linkname != "../outside" {
				t.Fatalf("symlink=%+v", h)
			}
		case "executable":
			if h.Mode != 0755 {
				t.Fatalf("mode=%o", h.Mode)
			}
		}
	}
	for _, name := range []string{"secret", "substitute", "link", "executable"} {
		if !seen[name] {
			t.Fatalf("missing capture path %s", name)
		}
	}
}
