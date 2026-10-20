package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestPlainBinaryUsesPersonForWhoamiAndClientForSubmitAndExec(t *testing.T) {
	stack := newExecStack(t, execRealBinaryIdleLimit)
	for _, name := range []string{"WEFTY_PROFILE", "WEFTY_DEV_PLAIN_USER_ID", "WEFTY_DEV_PLAIN_DEVICE_ID"} {
		t.Setenv(name, "")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	profilePath, err := connprofile.DefaultPath(home)
	if err != nil {
		t.Fatal(err)
	}
	// Participant records select the strict split, also on the plain test Fabric.
	if err := connprofile.Write(profilePath, connprofile.Profile{
		Version: 1, Fabric: "plain", L1Address: stack.l1Address, L3Address: stack.l3Address,
		Identities: connprofile.Identities{CLI: "legacy-client", PersonUserID: "legacy-owner", PersonDeviceID: "legacy-device"},
		Participants: map[string]connprofile.Participant{
			"client": {Name: "wefty://client/machine-1", StateDirectory: savedChoiceState(t, "client"), NodeID: "machine-client"},
			"person": {Name: "wefty://person/machine-1", StateDirectory: savedChoiceState(t, "person"), NodeID: "owner-device", UserID: "owner-person"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	binary := buildWefty(t)
	socket := clientServiceSocketForTest(t)
	t.Setenv("WEFTY_CLIENT_SOCKET", socket)
	startClientServiceForTest(t, binary, profilePath, socket)
	invoke := func(args ...string) []byte {
		t.Helper()
		code, stdout, stderr := runPolishBinary(t, binary, append([]string{"--node-config=" + stack.clients.nodeConfigPath}, args...)...)
		if code != 0 {
			t.Fatalf("%v: exit=%d stdout=%s stderr=%s", args, code, stdout, stderr)
		}
		return stdout
	}
	var person l1.AuthenticatedPerson
	if err := json.Unmarshal(invoke("--json", "whoami"), &person); err != nil {
		t.Fatal(err)
	}
	if person.UserID != "owner-person" || person.DeviceID != "owner-device" {
		t.Fatalf("whoami = %+v", person)
	}

	script := filepath.Join(t.TempDir(), "work.sh")
	writeExecFile(t, script, "#!/bin/sh\necho identity-proof\n")
	var accepted l3.RunAccepted
	if err := json.Unmarshal(invoke("--json", "submit", "--script", script, "--params", "{}", "--tag", "wefty:node:node-cli", "--idempotency-key", "identity-submit"), &accepted); err != nil {
		t.Fatal(err)
	}
	assertSubmitter := func(runID string) {
		t.Helper()
		run, err := stack.clients.getRun(t.Context(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Trigger.Principal != "machine-client" {
			t.Fatalf("run %s submitter = %q, want client machine-client", runID, run.Trigger.Principal)
		}
	}
	assertSubmitter(accepted.RunID)
	t.Chdir(execTestRepo(t))
	events := execModeEvents(t, string(invoke("--json", "exec", "--", "/bin/echo", "identity-proof")))
	assertSubmitter(events[len(events)-1]["run_id"].(string))
	var status struct {
		Identity struct {
			Kind         string `json:"kind"`
			NodeID       string `json:"node_id"`
			PersonSignIn string `json:"person_sign_in"`
		} `json:"identity"`
	}
	if err := json.Unmarshal(invoke("--json", "status"), &status); err != nil {
		t.Fatal(err)
	}
	if status.Identity.Kind != "machine" || status.Identity.NodeID != "machine-client" || status.Identity.PersonSignIn != "signed_in" {
		t.Fatalf("status identity = %+v", status.Identity)
	}
	profile, err := connprofile.Read(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	delete(profile.Participants, "person")
	if err := connprofile.Write(profilePath, profile); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(invoke("--json", "status"), &status); err != nil {
		t.Fatal(err)
	}
	if status.Identity.PersonSignIn != "not_signed_in" || status.Identity.Kind != "machine" {
		t.Fatalf("status without person = %+v", status.Identity)
	}
}
