package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

func TestParticipantRecoveryAdviceAndLegacyName(t *testing.T) {
	options, _, err := parseGlobalOptions([]string{"status"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if options.fabricName != "wefty-cli" {
		t.Errorf("legacy name=%q", options.fabricName)
	}
	options.participants = map[string]connprofile.Participant{"client": {Name: "wefty://client/one", NodeID: "saved", StateDirectory: t.TempDir()}}
	for _, command := range []string{"whoami", "status"} {
		_, err := commandFabricConfig(options, []string{command})
		if err == nil {
			t.Fatal("missing saved identity accepted")
		}
		if strings.Contains(err.Error(), "--plain-user-id") || strings.Contains(err.Error(), "run wefty setup") || !strings.Contains(err.Error(), "fleet setup") {
			t.Fatalf("misleading recovery: %v", err)
		}
	}
}

func TestParticipantTextStatusKeepsUnreachableIdentityDetail(t *testing.T) {
	var output bytes.Buffer
	err := writeStatus(&output, clusterStatus{Identity: fabricStatus{Kind: identityUnknown, NodeID: "saved-client", PersonSignIn: "not_signed_in", Detail: "unknown: control plane unreachable"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "identity: client saved-client; unknown: control plane unreachable; local person sign-in: not_signed_in") {
		t.Fatalf("lost identity detail: %s", output.String())
	}
}

func TestPurgeKeepsParticipantRoleSymlinkAndItsTarget(t *testing.T) {
	f := newSetupFixture(t)
	f.setup(t, setupRequest{})
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	marker := filepath.Join(target, "saved-state")
	if err := os.WriteFile(marker, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	folder := filepath.Join(paths.data, "fabric")
	if err := os.MkdirAll(folder, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(folder, "person")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := runSetupRemove(t.Context(), f.host, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(link); err != nil {
		t.Fatal("removed role symlink:", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("removed symlink target:", err)
	}
}

func TestMissingClientStateIsTypedFromRealBinary(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(home, "profile.json")
	if err := connprofile.Write(path, connprofile.Profile{Version: 1, Fabric: "tsnet", Participants: map[string]connprofile.Participant{"client": {Name: "wefty://client/one", NodeID: "saved", StateDirectory: filepath.Join(home, "missing-client")}}}); err != nil {
		t.Fatal(err)
	}
	code, output := runWeftyWithEnv(t, buildWefty(t), 30*time.Second, []string{"HOME=" + home, "WEFTY_PROFILE="}, "--profile="+path, "--json", "status")
	var response contract.ErrorResponse
	if json.Unmarshal([]byte(output), &response) != nil || code != 3 || response.Error.Code != contract.ErrorPersonIdentityRequired || !strings.Contains(response.Error.Message, "restore") {
		t.Fatalf("missing client state: exit=%d output=%s", code, output)
	}
}

func TestSavedBackendIdentityRefusalUsesTypedUnauthorizedExit(t *testing.T) {
	err := errors.Join(errors.New("opening participant"), &fabric.ParticipantIdentityError{Reason: "does not match the recorded node ID"})
	report := decideCommandReport(err)
	response, ok := report.errorDocument.(contract.ErrorResponse)
	if report.exitCode != 3 || !ok || response.Error.Code != contract.ErrorPersonIdentityRequired || response.Error.Retryable {
		t.Fatalf("report=%+v", report)
	}
}
