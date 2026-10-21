package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
)

func TestSetupJoinCodeCannotChooseCoordinator(t *testing.T) {
	const custom = "https://attacker.example.test"
	for _, tc := range []struct {
		name, codeURL, flagURL, envURL string
		refuse                         bool
	}{
		{name: "code alone refuses", codeURL: custom, refuse: true},
		{name: "matching owner flag", codeURL: custom, flagURL: custom},
		{name: "matching owner environment", codeURL: custom, envURL: custom},
		{name: "mismatched owner flag", codeURL: custom, flagURL: "https://owner.example.test", refuse: true},
		{name: "default code mismatches custom flag", flagURL: custom, refuse: true},
		{name: "default code mismatches custom environment", envURL: custom, refuse: true},
		{name: "default code and default owner choice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TS_CONTROL_URL", tc.envURL)
			f, e, prompts := newJoinFixture(t)
			input := strings.NewReader("secret-node\nsecret-client\n")
			f.host.enrolmentInput = input
			_, err := runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: joinCodeForTest(t, "opaque-l1", "opaque-l3", tc.codeURL), coordinatorURL: tc.flagURL, nodeID: "studio-0123abcd"})
			if tc.refuse {
				if err == nil || len(e.roles) != 0 || input.Len() != len("secret-node\nsecret-client\n") || prompts.Len() != 0 {
					t.Fatalf("code selected coordinator or read keys: err=%v roles=%v output=%s", err, e.roles, prompts)
				}
			} else if err != nil || len(e.roles) != 2 {
				t.Fatalf("explicit coordinator refused: %v", err)
			}
		})
	}
}

func TestSetupPrintsCoordinatorBeforeFirstKey(t *testing.T) {
	for _, role := range []string{"join", "control-plane"} {
		for _, url := range []string{"", "https://owner.example.test/enrol"} {
			t.Run(role+url, func(t *testing.T) {
				f, _, prompts := newFleetFixture(t)
				if role == "join" {
					f, _, prompts = newJoinFixture(t)
				}
				request := setupRequest{role: role, coordinatorURL: url, nodeID: "studio-0123abcd"}
				if role == "join" {
					request.joinCode = joinCodeForTest(t, "opaque-l1", "opaque-l3", url)
				}
				_ = f.setup(t, request)
				want := "Join keys will go to coordinator: default"
				if url != "" {
					want = "Join keys will go to coordinator: owner.example.test"
				}
				index := strings.Index(prompts.String(), want)
				prompt := strings.Index(prompts.String(), "Single-use join key")
				if index < 0 || index >= prompt {
					t.Fatalf("coordinator is not visible before key prompt: %s", prompts)
				}
			})
		}
	}
}

func TestSetupJoinCanDropUnsavedPersonBeforePublication(t *testing.T) {
	for _, boundary := range []string{"stage-record", "enrolled:client", "person failure"} {
		t.Run(boundary, func(t *testing.T) {
			f, e, prompts := newJoinFixture(t)
			browserCalls := 0
			f.host.ownerLogin = func(context.Context, string) error { browserCalls++; return errors.New("browser sign-in refused") }
			code := joinCodeForTest(t, "opaque-l1", "opaque-l3", "")
			_, err := runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: code, withPerson: true, nodeID: "studio-0123abcd", afterFleetStep: func(step string) error {
				if step == boundary {
					return errors.New("interrupted")
				}
				return nil
			}})
			if err == nil {
				t.Fatal("missing sign-in failure or interruption")
			}
			paths, _ := resolveSetupPaths(f.home)
			if _, err := os.Stat(filepath.Join(paths.config, setupManifestName)); !os.IsNotExist(err) {
				t.Fatal("published before optional-person recovery")
			}
			calls := browserCalls
			machineCount := savedMachineCount(e.roles)
			prompts.Reset()
			f.host.enrolmentInput = strings.NewReader(strings.Repeat("remaining-key\n", 2-machineCount))
			report, err := runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: code})
			if err != nil {
				t.Fatalf("optional person still blocks machine without the flag: %v", err)
			}
			if browserCalls != calls || strings.Count(prompts.String(), "Single-use join key") != 2-machineCount {
				t.Fatal("resumed an unsaved person or requested saved-machine keys")
			}
			p := f.profile(t)
			if len(p.Participants) != 2 || len(report.Services) != 2 {
				t.Fatalf("machine is not usable without person: %+v", report)
			}
			record, err := readFleetSetupRecord(paths)
			if err != nil || record.WithPerson || !record.Committed {
				t.Fatalf("person choice not dropped: %+v %v", record, err)
			}
			for _, check := range report.Readiness {
				if check.Status != "VERIFIED" {
					t.Fatalf("machine readiness: %+v", check)
				}
			}
			assertJoinArtifactsPrivate(t, f.home, prompts.String(), report, "secret-node", "secret-client", "remaining-key", "https://login.invalid/one-use")
		})
	}
}

func TestSetupJoinLocalParticipantCodeRecordsRecoverableFailure(t *testing.T) {
	for _, peer := range []string{"l1", "l3"} {
		for _, localRole := range []string{"node-agent", "client", "person"} {
			t.Run(peer+"/"+localRole, func(t *testing.T) {
				f, e, prompts := newJoinFixture(t)
				f.host.ownerLogin = func(context.Context, string) error { return nil }
				l1ID, l3ID := "opaque-l1", "opaque-l3"
				if peer == "l1" {
					l1ID = "opaque-" + localRole
				} else {
					l3ID = "opaque-" + localRole
				}
				f.host.peerLookup = func(_ context.Context, c fabricconfig.Config) (fabric.PeerLookup, fabricconfig.CloseFunc, error) {
					if c.ExpectedIdentity == nil || c.ExpectedIdentity.NodeID != "opaque-node-agent" {
						t.Fatal("wrong verifier identity")
					}
					return setupPeerLookup(func(_ context.Context, id string) (fabric.Identity, error) {
						tag := "tag:wefty-client"
						if id == l1ID || id == "opaque-l1" {
							tag = "tag:wefty-control"
						}
						return fabric.Identity{NodeID: id, Tags: []string{tag}}, nil
					}), func() error { return nil }, nil
				}
				_, err := runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: joinCodeForTest(t, l1ID, l3ID, ""), withPerson: localRole == "person", nodeID: "studio-0123abcd"})
				if err == nil {
					t.Fatal("published a local participant as a remote endpoint")
				}
				paths, _ := resolveSetupPaths(f.home)
				record, readErr := readFleetSetupRecord(paths)
				if readErr != nil || record.PeerVerificationFailure != peer+":local_participant" {
					t.Errorf("local participant failure not recorded: %+v %v", record, readErr)
				}
				staged, readErr := connprofile.Read(filepath.Join(paths.config, fleetSetupProfileName))
				if readErr != nil || len(staged.Participants) < 2 {
					t.Fatal("lost enrolled machine participants")
				}
				if _, err := os.Stat(paths.profile); !os.IsNotExist(err) {
					t.Fatal("published before local-participant refusal")
				}
				count := len(e.roles)
				prompts.Reset()
				f.host.enrolmentInput = strings.NewReader("")
				report, err := runSetup(t.Context(), f.host, setupRequest{role: "join", joinCode: joinCodeForTest(t, "opaque-l1", "opaque-l3", "")})
				if err != nil {
					t.Fatalf("corrected code is stuck after local participant refusal: %v", err)
				}
				if len(e.roles) != count || prompts.Len() != 0 {
					t.Fatal("corrected local-participant code consumed another key or person sign-in")
				}
				record, err = readFleetSetupRecord(paths)
				if err != nil || record.PeerVerificationFailure != "" || !record.Committed {
					t.Fatalf("correction not committed: %+v %v", record, err)
				}
				assertJoinArtifactsPrivate(t, f.home, prompts.String(), report, "secret-node", "secret-client", "https://login.invalid/one-use")
			})
		}
	}
}

func TestSetupJoinCoordinatorSelectionFromRealBinary(t *testing.T) {
	binary := buildFleetFixtureBinary(t)
	const custom = "https://owner.example.test/enrol"
	for _, tc := range []struct {
		name, codeURL, flagURL, envURL string
		exit                           int
	}{
		{name: "code cannot choose", codeURL: custom, exit: 1},
		{name: "explicit matching flag", codeURL: custom, flagURL: custom},
		{name: "explicit matching environment", codeURL: custom, envURL: custom},
		{name: "mismatched flag", codeURL: custom, flagURL: "https://another.example.test", exit: 1},
		{name: "default code custom flag", flagURL: custom, exit: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := newFleetTestHome(t)
			keys := "single-use-node\nsingle-use-client\n"
			args := []string{"--json", "setup", "--role", "join", "--node-id", "studio-0123abcd", "--join-code", joinCodeForTest(t, "opaque-l1", "opaque-l3", tc.codeURL)}
			if tc.flagURL != "" {
				args = append(args, "--coordinator-url", tc.flagURL)
			}
			command := exec.CommandContext(t.Context(), binary, args...)
			command.Env = []string{"HOME=" + home, "PATH=/usr/bin:/bin", "TS_CONTROL_URL=" + tc.envURL}
			command.Stdin = strings.NewReader(keys)
			var out, diagnostics bytes.Buffer
			command.Stdout, command.Stderr = &out, &diagnostics
			err := command.Run()
			exit := 0
			var failed *exec.ExitError
			if errors.As(err, &failed) {
				exit = failed.ExitCode()
			} else if err != nil {
				t.Fatal(err)
			}
			if exit != tc.exit {
				t.Fatalf("exit=%d want %d: %s", exit, tc.exit, &diagnostics)
			}
			if tc.exit != 0 {
				if out.Len() != 0 || strings.Contains(diagnostics.String(), "Single-use join key") {
					t.Fatalf("refusal read or printed key prompt: %s", &diagnostics)
				}
			} else {
				target := strings.Index(diagnostics.String(), "Join keys will go to coordinator: owner.example.test")
				if target < 0 || target >= strings.Index(diagnostics.String(), "Single-use join key") {
					t.Fatalf("destination hidden before key prompt: %s", &diagnostics)
				}
				var report setupReport
				if json.Unmarshal(out.Bytes(), &report) != nil || report.Role != "join" {
					t.Fatal("missing joining report")
				}
				assertJoinArtifactsPrivate(t, home, diagnostics.String(), report, "single-use-node", "single-use-client")
			}
		})
	}
}
