//go:build darwin

package lima

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

func inventoryLegacyFake(t *testing.T, run commandRunner) func(*exec.Cmd) error {
	t.Helper()
	return func(cmd *exec.Cmd) error {
		output, err := run(t.Context(), cmd.Args[0], cmd.Args[1:]...)
		_, writeErr := cmd.Stdout.Write(output)
		return errors.Join(err, writeErr)
	}
}

func TestGuestHelperRemovalFullInventoryStreams(t *testing.T) {
	failure := errors.New("inventory exit failure")
	for _, tc := range []struct {
		name, stdout, stderr string
		exit                 error
		present, wantError   bool
	}{
		{name: "empty_with_warning", stderr: "No instance found.\n"},
		{name: "other_with_warning", stdout: `{"name":"other","status":"running"}`, stderr: "validation warning\n"},
		{name: "present_with_warning", stdout: `{"name":"wefty-oci","status":"running"}`, stderr: "validation warning\n", present: true},
		{name: "malformed", stdout: "not-json", stderr: "warning", wantError: true},
		{name: "truncated_after_other", stdout: `{"name":"other","status":"running"}` + "\n{", wantError: true},
		{name: "nonzero_empty", exit: failure, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var commands [][]string
			installer := guestHelperInstaller{
				inventoryExecute: func(cmd *exec.Cmd) error {
					commands = append(commands, append([]string(nil), cmd.Args...))
					if _, err := io.WriteString(cmd.Stdout, tc.stdout); err != nil {
						t.Fatal(err)
					}
					if _, err := io.WriteString(cmd.Stderr, tc.stderr); err != nil {
						t.Fatal(err)
					}
					return tc.exit
				},
				run: func(_ context.Context, name string, args ...string) ([]byte, error) {
					commands = append(commands, append([]string{name}, args...))
					return nil, nil
				},
			}
			evidence, err := installer.remove(t.Context(), GuestHelperRemovalConfig{Instance: DefaultInstanceName, Limactl: "limactl"})
			if !reflect.DeepEqual(commands[0], []string{"limactl", "list", "--json"}) {
				t.Errorf("inventory command = %v; must enumerate without a target", commands[0])
			}
			if tc.wantError {
				if err == nil || evidence != (GuestHelperRemovalEvidence{}) {
					t.Errorf("unconfirmed evidence=%+v err=%v", evidence, err)
				}
				if tc.exit != nil && !errors.Is(err, tc.exit) {
					t.Errorf("lost exit identity: %v", err)
				}
			} else if err != nil || evidence != (GuestHelperRemovalEvidence{SocketStopped: true, ServiceStopped: true, FilesAbsent: true}) {
				t.Errorf("confirmed inventory evidence=%+v err=%v", evidence, err)
			}
			wantCommands := 1
			if tc.present {
				wantCommands = 6
			}
			if len(commands) != wantCommands {
				t.Errorf("commands=%v want count %d", commands, wantCommands)
			}
		})
	}
}

func TestGuestInventoryNonzeroTargetedMissingIsNotAbsence(t *testing.T) {
	failure := errors.New("unmatched instances")
	payload, err := runGuestHelperInventory(t.Context(), "limactl", func(cmd *exec.Cmd) error {
		if !reflect.DeepEqual(cmd.Args, []string{"limactl", "list", "--json", DefaultInstanceName}) {
			t.Fatal(cmd.Args)
		}
		_, _ = io.WriteString(cmd.Stderr, "No instance matching target found")
		return failure
	}, "list", "--json", DefaultInstanceName)
	if !errors.Is(err, failure) || payload != nil || !strings.Contains(err.Error(), "No instance matching target found") {
		t.Fatalf("nonzero result payload=%q err=%v", payload, err)
	}
}

func TestGuestInventoryRetainsBoundedStderrAndExitIdentity(t *testing.T) {
	failure := errors.New("exit identity")
	payload, err := runGuestHelperInventory(t.Context(), "limactl", func(cmd *exec.Cmd) error {
		if _, writeErr := io.WriteString(cmd.Stderr, strings.Repeat("x", 8192)); writeErr != nil {
			t.Fatal(writeErr)
		}
		if _, writeErr := io.WriteString(cmd.Stderr, "discarded-tail"); writeErr != nil {
			t.Fatal(writeErr)
		}
		_, _ = io.WriteString(cmd.Stdout, "stdout-not-error-diagnostic")
		return failure
	}, "list", "--json")
	if payload != nil || !errors.Is(err, failure) {
		t.Fatalf("payload=%q err=%v", payload, err)
	}
	want := "limactl: exit identity: " + strings.Repeat("x", 4096)
	if err.Error() != want {
		t.Fatalf("diagnostic length=%d want=%d", len(err.Error()), len(want))
	}
}
