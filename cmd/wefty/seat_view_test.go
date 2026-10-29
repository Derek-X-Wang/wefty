package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestSeatViewRealBinary(t *testing.T) {
	binary := buildWefty(t)
	id := "seat_" + strings.Repeat("a", 32)
	target := "http://seat-node:80/v1/seats/" + id + "/person/viewer"
	for _, mode := range []string{"no-open", "json", "open", "term", "startup-ended"} {
		t.Run(mode, func(t *testing.T) {
			ended := atomic.Bool{}
			ended.Store(mode == "startup-ended")
			address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/v1/seats/"+id+"/viewer-target" || r.Header.Get("X-Wefty-Seat-Credential") != "" {
					t.Errorf("unexpected discovery %s %s", r.Method, r.URL.Path)
				}
				if ended.Load() {
					w.WriteHeader(410)
					ioBody := `{"error":{"code":"seat_ended","message":"This Seat has ended"}}`
					w.Write([]byte(ioBody))
					return
				}
				json.NewEncoder(w).Encode(map[string]string{"url": target})
			})
			directory := t.TempDir()
			record := filepath.Join(directory, "opened")
			opener := "open"
			if runtime.GOOS == "linux" {
				opener = "xdg-open"
			}
			if err := os.WriteFile(filepath.Join(directory, opener), []byte("#!/bin/sh\nprintf '%s' \"$1\" > \"$VIEWER_OPEN_RECORD\"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			args := []string{"--plain-user-id", "owner", "--plain-device-id", "owner-device", "--l1", address, "seat", "view", id}
			if mode == "no-open" || mode == "term" {
				args = append(args, "--no-open")
			}
			if mode == "json" || mode == "startup-ended" {
				args = append(args, "--json", "--no-open")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, binary, args...)
			command.Env = append(os.Environ(), "PATH="+directory+string(os.PathListSeparator)+os.Getenv("PATH"), "VIEWER_OPEN_RECORD="+record)
			stdout, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			command.Stderr = &stderr
			if err = command.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { cancel(); _ = command.Wait() })
			reader := bufio.NewReader(stdout)
			if mode == "startup-ended" {
				output, _ := io.ReadAll(reader)
				err = command.Wait()
				var exit *exec.ExitError
				var result struct {
					Error struct {
						Code string `json:"code"`
					}
				}
				if !errors.As(err, &exit) || exit.ExitCode() != 5 || len(output) != 0 || json.Unmarshal(stderr.Bytes(), &result) != nil || result.Error.Code != "seat_ended" {
					t.Fatal("startup lost the typed Seat end")
				}
				return
			}

			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal("relay link was not printed")
			}
			printed := strings.TrimSpace(line)
			if mode == "json" || mode == "startup-ended" {
				var result map[string]string
				if json.Unmarshal([]byte(line), &result) != nil {
					t.Fatal("invalid JSON link")
				}
				printed = result["url"]
			}
			link, err := url.Parse(printed)
			if err != nil || link.Scheme != "http" || link.Hostname() != "127.0.0.1" || link.Port() == "" || len(link.Query().Get("secret")) < 26 || len(strings.Trim(link.Path, "/")) < 26 || strings.Trim(link.Path, "/") == link.Query().Get("secret") {
				t.Fatal("missing local secret link")
			}
			// A cookie-less request reaches the live relay and is refused locally.
			local := &http.Client{Timeout: 10 * time.Second}
			response, err := local.Get(link.Scheme + "://" + link.Host + strings.TrimSuffix(link.Path, "/") + "/v1/seats/" + id + "/person/viewer")
			if err != nil {
				t.Fatal("foreground relay is not serving")
			}
			response.Body.Close()
			if response.StatusCode != 403 {
				t.Fatal("relay accepted an unauthenticated browser")
			}
			if mode != "open" {
				if _, err = os.Stat(record); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("--no-open opened a browser")
				}
			}
			wantReason := "Seat has ended"
			if mode == "term" {
				wantReason = "Seat viewer stopped"
				if err = command.Process.Signal(syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
			} else {
				ended.Store(true)
			}
			remaining, _ := io.ReadAll(reader)
			if len(remaining) != 0 {
				t.Fatal("command printed more than one link line")
			}
			err = command.Wait()
			var exit *exec.ExitError
			wantExit := 5
			if mode == "term" {
				wantExit = 1
			}
			if !errors.As(err, &exit) || exit.ExitCode() != wantExit || !strings.Contains(stderr.String(), wantReason) {
				t.Fatalf("missing ended reason: %v %s", err, stderr.String())
			}
			if ctx.Err() != nil {
				t.Fatal("viewer did not exit when the Seat ended")
			}
			if mode == "json" {
				var result struct {
					Error struct {
						Code string `json:"code"`
					}
				}
				if json.Unmarshal(stderr.Bytes(), &result) != nil || result.Error.Code != "seat_ended" {
					t.Fatal("viewing lost the JSON Seat end code")
				}
			}
			if mode == "open" {
				data, err := os.ReadFile(record)
				if err != nil || string(data) != printed {
					t.Fatal("browser opener did not get the prefixed secret relay URL")
				}
			}
			if _, err = local.Get(link.Scheme + "://" + link.Host + "/"); err == nil {
				t.Fatal("relay survived the command")
			}
		})
	}
	if !requiresPersonCommand([]string{"seat", "view", id}) {
		t.Fatal("viewer must select the owner's person identity")
	}
}
