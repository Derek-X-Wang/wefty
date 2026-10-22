package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// This capture runs the real binary against fixed plain-protocol responses.
// The same commands generated the fixture on base 12657df6.
func TestPlainAdminWhoamiCLIBytes(t *testing.T) {
	binary := buildWefty(t)
	requests := make(chan string, 20)
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		request := r.Method + " " + r.URL.Path
		if len(body) > 0 {
			request += " " + string(body)
		}
		requests <- request + "\n"
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/whoami" {
			io.WriteString(w, `{"fabric_id":"plain-fixture","user_id":"owner","device_id":"owner-device","seen_at":"2026-10-21T12:00:00Z"}`)
			return
		}
		if r.URL.Path == "/v1/admin-bootstrap" {
			w.WriteHeader(201)
		}
		io.WriteString(w, `{"revision":2,"admins":[{"fabric_id":"plain-fixture","user_id":"owner","added_revision":1,"added_at":"2026-10-21T12:00:00Z"}]}`)
	})
	var capture bytes.Buffer
	for _, command := range [][]string{
		{"whoami"}, {"admin", "policy", "get"}, {"admins", "list"},
		{"admin", "bootstrap", "fixture-nonce"},
		{"admin", "policy", "add", "second", "--policy-revision", "1"},
		{"admin", "policy", "remove", "second", "--policy-revision", "2"},
	} {
		for _, jsonMode := range []bool{false, true} {
			args := []string{"--plain-user-id", "owner", "--plain-device-id", "owner-device", "--l1", address}
			if jsonMode {
				args = append(args, "--json")
			}
			args = append(args, command...)
			code, out := runWefty(t, binary, 30*time.Second, args...)
			if code != 0 {
				t.Fatalf("plain CLI %v exit=%d output=%s", command, code, out)
			}
			request := <-requests
			fmt.Fprintf(&capture, "%s json=%t\n%s%s", strings.Join(command, " "), jsonMode, request, out)
		}
	}
	if path := os.Getenv("WEFTY_T1090_CAPTURE"); path != "" {
		if err := os.WriteFile(path, capture.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	expected, err := os.ReadFile("testdata/plain-person-cli-base-12657df6.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(capture.Bytes(), expected) {
		t.Fatalf("plain CLI bytes changed:\n%s", capture.Bytes())
	}
}
