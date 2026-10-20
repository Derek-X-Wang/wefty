package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

// These are the person-only command families, including every current verb.
// Ordinary Computer operations stay client operations under usesPersonProtocol.
func TestCommandParticipantChoice(t *testing.T) {
	personCommands := []string{
		"whoami", "admin bootstrap nonce", "admins list", "admins add owner", "admins remove owner",
		"admin policy get", "admin policy add owner", "admin policy remove owner",
		"services grant computer", "services grants computer", "services revoke computer",
		"services takeover computer", "services submission enable computer",
		"services submission disable computer", "services submission set-inflight computer",
	}
	clientCommands := []string{
		"status", "submit", "exec", "cancel job-1", "nodes list", "drain node-1", "rerun run-1",
		"services list", "services create", "services start computer", "services stop computer",
		"services remove computer", "services view computer", "services control computer", "services connect computer",
		"services submission-status computer", "runs list", "logs run-1", "inspect run-1", "wait run-1",
	}
	for _, shape := range []string{"legacy", "participants"} {
		for _, mode := range []string{"plain", "tsnet"} {
			for _, person := range []bool{false, true} {
				commands := clientCommands
				if person {
					commands = personCommands
				}
				for _, command := range commands {
					t.Run(shape+"/"+mode+"/"+command, func(t *testing.T) {
						options, _, err := parseGlobalOptions([]string{"--fabric=" + mode, "status"}, io.Discard)
						if err != nil {
							t.Fatal(err)
						}
						options.plainIdentity, options.plainUserID, options.plainDeviceID = "legacy-client", "legacy-owner", "legacy-device"
						clientState, personState := savedChoiceState(t, "client"), savedChoiceState(t, "person")
						if shape == "participants" {
							options.participants = map[string]connprofile.Participant{
								"client": {Name: "wefty://client/machine-1", StateDirectory: clientState, NodeID: "client-opaque"},
								"person": {Name: "wefty://person/machine-1", StateDirectory: personState, NodeID: "person-opaque", UserID: "owner"},
							}
						}
						config, err := commandFabricConfig(options, strings.Fields(command))
						if err != nil {
							t.Fatal(err)
						}
						want := fabric.Identity{NodeID: "legacy-client", UserID: "legacy-owner", DeviceID: "legacy-device", Tags: []string{"tag:wefty-client"}}
						if person {
							want.Tags = nil
						}
						if shape == "participants" {
							want = fabric.Identity{NodeID: "client-opaque", Tags: []string{"tag:wefty-client"}}
							role, state := "client", clientState
							if person {
								want = fabric.Identity{NodeID: "person-opaque", UserID: "owner", DeviceID: "person-opaque"}
								role, state = "person", personState
							}
							if mode == "tsnet" && (config.Name != "wefty://"+role+"/machine-1" || config.StateDirectory != state || config.AuthKey != "") {
								t.Fatalf("participant config=%+v", config)
							}
						}
						if shape == "participants" && (config.ExpectedIdentity == nil || !reflect.DeepEqual(*config.ExpectedIdentity, want)) {
							t.Fatalf("missing authenticated participant expectation: %+v", config)
						}
						if shape == "legacy" && config.ExpectedIdentity != nil {
							t.Fatal("legacy profile gained a participant guard")
						}
						if !reflect.DeepEqual(config.Identity, want) {
							t.Fatalf("identity=%+v, want %+v", config.Identity, want)
						}
					})
				}
			}
		}
	}
}

func TestMissingPersonSignInIsTypedFromRealBinary(t *testing.T) {
	home := t.TempDir()
	binary := buildWefty(t)
	for _, mode := range []string{"plain", "tsnet"} {
		profilePath := filepath.Join(home, "profile.json")
		if err := connprofile.Write(profilePath, connprofile.Profile{Version: 1, Fabric: mode, Participants: map[string]connprofile.Participant{
			"client": {Name: "wefty://client/machine-1", StateDirectory: savedChoiceState(t, "client"), NodeID: "client"},
		}}); err != nil {
			t.Fatal(err)
		}
		code, output := runWeftyWithEnv(t, binary, 30*time.Second, []string{
			"HOME=" + home, "WEFTY_PROFILE=", "WEFTY_DEV_PLAIN_USER_ID=", "WEFTY_DEV_PLAIN_DEVICE_ID=", "TS_AUTHKEY=",
		}, "--fabric="+mode, "--profile="+profilePath, "--json", "whoami")
		var response contract.ErrorResponse
		if json.Unmarshal([]byte(output), &response) != nil || code != 3 || response.Error.Code != "person_identity_required" ||
			!strings.Contains(response.Error.Message, "fleet setup") || strings.Contains(response.Error.Message, "--plain-user-id") || strings.Contains(response.Error.Message, "run wefty setup") {
			t.Fatalf("%s missing sign-in: exit=%d output=%s", mode, code, output)
		}
	}
}

func TestPersonParticipantNeverFallsBackToClient(t *testing.T) {
	options := globalOptions{fabricMode: "tsnet", participants: map[string]connprofile.Participant{
		"client": {Name: "wefty://client/machine-1", NodeID: "client", StateDirectory: savedChoiceState(t, "client")},
	}}
	_, err := commandFabricConfig(options, []string{"whoami"})
	var missing *personSignInRequiredError
	if !errors.As(err, &missing) {
		t.Fatalf("missing sign-in = %v", err)
	}
	if _, err := commandFabricConfig(options, []string{"status"}); err != nil {
		t.Fatalf("status required a person: %v", err)
	}
}

func savedChoiceState(t *testing.T, role string) string {
	t.Helper()
	folder := filepath.Join(t.TempDir(), role)
	if err := os.MkdirAll(folder, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "saved-state"), []byte("opaque"), 0o600); err != nil {
		t.Fatal(err)
	}
	return folder
}

func TestEnrolledParticipantCannotBeRedirectedOrReenrolledByFlags(t *testing.T) {
	for _, flag := range []string{"--fabric-name=wefty://person/machine-1", "--state-dir=/other/state", "--auth-key=single-use-key", "--ephemeral"} {
		options, _, err := parseGlobalOptions([]string{"--fabric=tsnet", flag, "status"}, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		options.participants = map[string]connprofile.Participant{"client": {Name: "wefty://client/machine-1", StateDirectory: savedChoiceState(t, "client"), NodeID: "client-opaque"}}
		_, err = commandFabricConfig(options, []string{"status"})
		if err == nil || commandExitCode(err) != 2 {
			t.Fatalf("%s = %v, want a usage refusal", strings.Split(flag, "=")[0], err)
		}
	}
}

func TestPersonSignInRequiresSavedStateAndHonoursExpiry(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	state := filepath.Join(t.TempDir(), "person")
	options := globalOptions{fabricMode: "tsnet", participants: map[string]connprofile.Participant{
		"person": {Name: "wefty://person/machine-1", NodeID: "person-opaque", UserID: "owner", StateDirectory: state},
	}}
	if got := localPersonSignIn(options, now); got != "not_signed_in" {
		t.Fatalf("missing state = %s", got)
	}
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	if got := localPersonSignIn(options, now); got != "not_signed_in" {
		t.Fatalf("empty state = %s", got)
	}
	if err := os.WriteFile(filepath.Join(state, "saved-state"), []byte("opaque"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := localPersonSignIn(options, now); got != "signed_in" {
		t.Fatalf("saved state = %s", got)
	}
	person := options.participants["person"]
	person.KeyExpiry = now
	options.participants["person"] = person
	if got := localPersonSignIn(options, now); got != "expired" {
		t.Fatalf("expired state = %s", got)
	}
}

func TestLegacyEnvironmentJoinKeyIsCapturedBeforeOpening(t *testing.T) {
	t.Setenv("TS_AUTHKEY", "legacy-single-use-key")
	options, _, err := parseGlobalOptions([]string{"--fabric=tsnet", "status"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	resolveFabricEnvironment(&options)
	config, err := commandFabricConfig(options, []string{"status"})
	if err != nil {
		t.Fatal(err)
	}
	if config.AuthKey != "legacy-single-use-key" {
		t.Fatal("legacy environment join key was not captured explicitly")
	}
}
