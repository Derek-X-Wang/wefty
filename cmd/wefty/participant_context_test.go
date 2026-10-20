package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/tsnet"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
)

func participantContextProfile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "profile.json")
	if err := connprofile.Write(path, connprofile.Profile{Version: 1, Fabric: "tsnet", Participants: map[string]connprofile.Participant{"client": {Name: "wefty://client/test", NodeID: "saved", StateDirectory: savedChoiceState(t, "client")}}}); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParticipantStatusBoundsStartupAndReportsFailure(t *testing.T) {
	for _, jsonOutput := range []bool{false, true} {
		for _, timeout := range []string{"", "--timeout=40ms"} {
			t.Run(timeout+map[bool]string{false: "/text", true: "/json"}[jsonOutput], func(t *testing.T) {
				path := participantContextProfile(t)
				var stdout, stderr bytes.Buffer
				checked := false
				opener := func(ctx context.Context, config fabricconfig.Config) (fabric.Fabric, fabricconfig.CloseFunc, error) {
					checked = true
					deadline, ok := ctx.Deadline()
					limit := statusProbeBudget
					if timeout != "" {
						limit = 40 * time.Millisecond
					}
					if !ok || time.Until(deadline) > limit {
						t.Errorf("participant startup has no %s budget", limit)
					}
					return nil, nil, &fabric.ParticipantIdentityError{Reason: "needs machine authorization"}
				}
				args := []string{"--profile=" + path, "status"}
				if timeout != "" {
					args = append(args, timeout)
				}
				if jsonOutput {
					args = append(args, "--json")
				}
				err := runWithFabricOpener(t.Context(), args, &stdout, &stderr, opener)
				if !checked || commandExitCode(err) != 12 {
					t.Fatalf("checked=%v err=%v output=%s", checked, err, stdout.String())
				}
				if jsonOutput {
					var status clusterStatus
					if err := json.Unmarshal(stdout.Bytes(), &status); err != nil || status.Ready || status.Identity.NodeID != "saved" || !strings.Contains(status.Identity.Detail, "machine authorization") || len(status.Reasons) == 0 {
						t.Fatalf("status=%s decode=%v", stdout.String(), err)
					}
				} else if !strings.Contains(stdout.String(), "identity: client saved") || !strings.Contains(stdout.String(), "machine authorization") {
					t.Fatalf("status=%s", stdout.String())
				}
				if stderr.Len() != 0 {
					t.Fatalf("stderr=%s", stderr.String())
				}
			})
		}
	}
}

func TestParticipantCommandPassesCancellationBeforeRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var stdout, stderr bytes.Buffer
	opener := func(probe context.Context, _ fabricconfig.Config) (fabric.Fabric, fabricconfig.CloseFunc, error) {
		cancel()
		<-probe.Done()
		return nil, nil, probe.Err()
	}
	err := runWithFabricOpener(ctx, []string{"--profile=" + participantContextProfile(t), "nodes", "list"}, &stdout, &stderr, opener)
	if !errors.Is(err, context.Canceled) || commandExitCode(err) != 1 || stdout.Len() != 0 {
		t.Fatalf("err=%v stdout=%s", err, stdout.String())
	}
	var output bytes.Buffer
	writeCommandError(&output, err, true)
	var response contract.ErrorResponse
	if json.Unmarshal(output.Bytes(), &response) != nil || response.Error.Code != "internal" || response.Error.Retryable {
		t.Fatalf("cancellation=%s", output.String())
	}
}

func TestParticipantStatusUnreachableIsReportedWithinTimeout(t *testing.T) {
	path := participantContextProfile(t)
	var stdout, stderr bytes.Buffer
	opener := func(ctx context.Context, _ fabricconfig.Config) (fabric.Fabric, fabricconfig.CloseFunc, error) {
		<-ctx.Done()
		return nil, nil, &fabric.ParticipantUnreachableError{Cause: ctx.Err()}
	}
	started := time.Now()
	err := runWithFabricOpener(t.Context(), []string{"--profile=" + path, "status", "--timeout=40ms", "--json"}, &stdout, &stderr, opener)
	var status clusterStatus
	if json.Unmarshal(stdout.Bytes(), &status) != nil || commandExitCode(err) != 12 || status.Ready || len(status.Reasons) != 1 || status.Reasons[0].Code != "participant_check_failed" || !strings.Contains(status.Identity.Detail, "context deadline exceeded") {
		t.Fatalf("err=%v output=%s", err, stdout.String())
	}
	if time.Since(started) > time.Second {
		t.Fatalf("status exceeded its short budget: %s", time.Since(started))
	}
	for _, service := range status.Services {
		if service.Reachable || !strings.Contains(service.Detail, "not probed") {
			t.Fatalf("unverified service=%+v", service)
		}
	}
}

func TestParticipantUnreachableCommandReport(t *testing.T) {
	err := &fabric.ParticipantUnreachableError{Cause: context.DeadlineExceeded}
	report := decideCommandReport(err)
	response, ok := report.errorDocument.(contract.ErrorResponse)
	if report.exitCode != 13 || !ok || response.Error.Code != "fabric_unreachable" || !response.Error.Retryable {
		t.Fatalf("report=%+v", report)
	}
}

func TestParticipantStatusReportsBeforeBackendCleanup(t *testing.T) {
	var stdout, stderr bytes.Buffer
	closed := false
	opener := func(context.Context, fabricconfig.Config) (fabric.Fabric, fabricconfig.CloseFunc, error) {
		return nil, func() error {
			if stdout.Len() == 0 {
				t.Error("backend cleanup ran before the status verdict")
			}
			closed = true
			return nil
		}, &fabric.ParticipantUnreachableError{Cause: context.DeadlineExceeded}
	}
	err := runWithFabricOpener(t.Context(), []string{"--profile=" + participantContextProfile(t), "status"}, &stdout, &stderr, opener)
	if !closed || commandExitCode(err) != 12 {
		t.Fatalf("closed=%v err=%v", closed, err)
	}
}

// Adapter tests inject the failing local status source. Here a pre-expired
// context obtains the actual adapter's public error without starting a backend,
// then exercises both production status renderers with that seam result.
func TestParticipantStatusFailureKeepsBackendNamesPrivate(t *testing.T) {
	for _, jsonOutput := range []bool{false, true} {
		t.Run(map[bool]string{false: "text", true: "json"}[jsonOutput], func(t *testing.T) {
			path := participantContextProfile(t)
			var stdout, stderr bytes.Buffer
			opener := func(ctx context.Context, config fabricconfig.Config) (fabric.Fabric, fabricconfig.CloseFunc, error) {
				participant, err := tsnet.New(tsnet.Config{Name: config.Name, StateDir: config.StateDirectory, ExpectedIdentity: config.ExpectedIdentity})
				if err != nil {
					t.Fatal(err)
				}
				expired, cancel := context.WithDeadline(ctx, time.Unix(0, 0))
				defer cancel()
				err = participant.VerifyParticipant(expired)
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("lost deadline: %v", err)
				}
				return nil, participant.Close, err
			}
			args := []string{"--profile=" + path, "status"}
			if jsonOutput {
				args = append(args, "--json")
			}
			err := runWithFabricOpener(t.Context(), args, &stdout, &stderr, opener)
			if commandExitCode(err) != 12 || !strings.Contains(stdout.String(), "context deadline exceeded") {
				t.Fatalf("err=%v output=%s", err, stdout.String())
			}
			if jsonOutput {
				var status clusterStatus
				if err := json.Unmarshal(stdout.Bytes(), &status); err != nil || status.Identity.Detail == "" || len(status.Reasons) != 1 {
					t.Fatalf("status=%s decode=%v", stdout.String(), err)
				}
			}
			for _, surface := range []string{err.Error(), stdout.String(), stderr.String()} {
				for _, fragment := range []string{"tailscale", "tailscaled", "localapi", ".sock"} {
					if strings.Contains(strings.ToLower(surface), fragment) {
						t.Errorf("public output exposes %q: %s", fragment, surface)
					}
				}
			}
		})
	}
}
