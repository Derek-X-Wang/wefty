package main

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExecStalledUploadDoesNotBlockDefaultExecFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	fleet := newExecFleet(t, 0)
	t.Chdir(execTestRepo(t))
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	release := make(chan struct{})
	defer close(release)
	go func() { writer.Write([]byte("x")); <-release; writer.Close() }()
	request, _ := http.NewRequest(http.MethodPost, "http://wefty.invalid/v1/exec-captures/"+strings.Repeat("a", 64), reader)
	go func() {
		res, e := fleet.clients.l3.client.Do(request)
		if e == nil {
			res.Body.Close()
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		entries, _ := os.ReadDir(filepath.Join(fleet.ledgerRoot, "exec-captures"))
		staged := false
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".upload-") {
				staged = true
			}
		}
		if staged {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("upload did not start")
		}
		time.Sleep(time.Millisecond)
	}
	// The complete default exec must still dispatch, run and project its result.
	code, out, stderr := runPolishBinary(t, binary, "--l1="+fleet.l1Address, "--l3="+fleet.l3Address, "--node-config="+fleet.clients.nodeConfigPath, "exec", "--", "printf", "local-still-runs")
	if code != 0 || string(out) != "local-still-runs" {
		t.Fatalf("default exec blocked: exit=%d out=%s err=%s", code, out, stderr)
	}
}
