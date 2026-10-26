package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestSeatViewRealBinary(t *testing.T) {
	binary := buildWefty(t)
	id := "seat_" + strings.Repeat("a", 32)
	want := "http://seat-node:80/v1/seats/" + id + "/person/viewer"
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/v1/seats/"+id+"/viewer-target" || r.Header.Get("X-Wefty-Seat-Credential") != "" {
			t.Errorf("unexpected discovery %s %s", r.Method, r.URL)
		}
		json.NewEncoder(w).Encode(map[string]string{"url": want})
	})
	for _, args := range [][]string{{"--no-open"}, {"--json"}} {
		code, out := runWefty(t, binary, 30*time.Second, append([]string{"--plain-user-id", "owner", "--plain-device-id", "owner-device", "--l1", address, "seat", "view", id}, args...)...)
		if code != 0 {
			t.Fatalf("exit=%d %s", code, out)
		}
		if args[0] == "--json" {
			var result map[string]string
			if json.Unmarshal([]byte(out), &result) != nil || result["url"] != want {
				t.Fatalf("JSON=%s", out)
			}
		} else if strings.TrimSpace(out) != want {
			t.Fatalf("URL=%s", out)
		}
	}
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		return
	}
	directory := t.TempDir()
	record := filepath.Join(directory, "opened")
	opener := "open"
	if runtime.GOOS == "linux" {
		opener = "xdg-open"
	}
	t.Setenv("VIEWER_OPEN_RECORD", record)
	if err := os.WriteFile(filepath.Join(directory, opener), []byte("#!/bin/sh\nprintf '%s' \"$1\" > \"$VIEWER_OPEN_RECORD\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	code, out := runWefty(t, binary, 30*time.Second, "--plain-user-id", "owner", "--plain-device-id", "owner-device", "--l1", address, "seat", "view", id)
	if code != 0 || strings.TrimSpace(out) != want {
		t.Fatalf("open exit=%d %s", code, out)
	}
	// The system browser launcher is asynchronous. This wait observes a real process.
	var data []byte
	var err error
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		data, err = os.ReadFile(record)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || string(data) != want {
		t.Fatalf("opened=%q %v", data, err)
	}
	if !requiresPersonCommand([]string{"seat", "view", id}) {
		t.Fatal("viewer must select the owner's person identity")
	}
}
