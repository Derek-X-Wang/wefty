package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestT899LegacyPlainRunCanStillBeCancelled(t *testing.T) {
	stack := newExecStack(t, execRealBinaryIdleLimit)
	t.Setenv("WEFTY_PROFILE", "")
	t.Setenv("WEFTY_DEV_PLAIN_USER_ID", "")
	t.Setenv("WEFTY_DEV_PLAIN_DEVICE_ID", "")
	home, _ := os.UserHomeDir()
	profilePath, _ := connprofile.DefaultPath(home)
	if err := connprofile.Write(profilePath, connprofile.Profile{Version: 1, Fabric: "plain", L1Address: stack.l1Address, L3Address: stack.l3Address, Identities: connprofile.Identities{CLI: "machine-client", PersonUserID: "owner-person", PersonDeviceID: "owner-device"}}); err != nil {
		t.Fatal(err)
	}
	legacy := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "machine-client", UserID: "owner-person", DeviceID: "owner-device", Tags: []string{"tag:wefty-client"}})
	clients, err := newAPIClients(legacy, stack.l1Address, stack.l3Address)
	if err != nil {
		t.Fatal(err)
	}
	defer clients.close()
	script := filepath.Join(t.TempDir(), "work.sh")
	writeExecFile(t, script, "#!/bin/sh\nsleep 30\n")
	var stdout, stderr bytes.Buffer
	err = execute(t.Context(), clients, true, []string{"submit", "--script", script, "--params", "{}", "--tag", "wefty:node:node-cli", "--idempotency-key", "legacy-owner"}, &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	var accepted l3.RunAccepted
	if err := json.Unmarshal(stdout.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	record, err := stack.clients.getRun(t.Context(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Trigger.Principal != "owner-person" {
		t.Fatalf("legacy principal=%q", record.Trigger.Principal)
	}
	code, out, errout := runPolishBinary(t, buildWefty(t), "--node-config="+stack.clients.nodeConfigPath, "cancel", accepted.RunID)
	if code != 0 {
		t.Fatalf("legacy cancel with unchanged profile: exit=%d stdout=%s stderr=%s", code, out, errout)
	}
	// New calls from the unchanged legacy profile retain the same submitter.
	invoke := func(args ...string) []byte {
		t.Helper()
		code, out, errout := runPolishBinary(t, buildWefty(t), append([]string{"--node-config=" + stack.clients.nodeConfigPath}, args...)...)
		if code != 0 {
			t.Fatalf("legacy %v exit=%d stdout=%s stderr=%s", args, code, out, errout)
		}
		return out
	}
	if err := json.Unmarshal(invoke("--json", "submit", "--script", script, "--params", "{}", "--tag", "wefty:node:node-cli", "--idempotency-key", "legacy-new-submission"), &accepted); err != nil {
		t.Fatal(err)
	}
	assertLegacySubmitter := func(id string) {
		t.Helper()
		record, err := stack.clients.getRun(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		if record.Trigger.Principal != "owner-person" {
			t.Fatalf("legacy submitter=%q", record.Trigger.Principal)
		}
	}
	assertLegacySubmitter(accepted.RunID)
	invoke("cancel", accepted.RunID)
	t.Chdir(execTestRepo(t))
	events := execModeEvents(t, string(invoke("--json", "exec", "--", "/bin/echo", "legacy-proof")))
	assertLegacySubmitter(events[len(events)-1]["run_id"].(string))
	var status struct {
		Identity map[string]any `json:"identity"`
	}
	if err := json.Unmarshal(invoke("--json", "status"), &status); err != nil {
		t.Fatal(err)
	}
	if _, ok := status.Identity["node_id"]; ok {
		t.Fatal("legacy status gained participant identity fields")
	}
	if _, ok := status.Identity["person_sign_in"]; ok {
		t.Fatal("legacy status gained participant sign-in fields")
	}
	text := string(invoke("status"))
	if strings.Contains(text, "local person sign-in") || strings.Contains(text, "identity: client ") {
		t.Fatalf("legacy identity text changed: %s", text)
	}

}
