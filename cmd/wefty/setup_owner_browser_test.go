package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSetupOwnerBrowserSelectsPlatformWithoutLeakingURL(t *testing.T) {
	for _, platform := range []string{"darwin", "linux"} {
		t.Run(platform, func(t *testing.T) {
			const login = "https://login.invalid/one-use-private"
			files, _ := linuxTestFiles(t)
			var command string
			var values []string
			open := setupOwnerLoginBrowser(platform, func(_ context.Context, name string, args ...string) error {
				command = name
				values = args
				return errors.New("browser failed with " + login)
			}, files)
			err := open(t.Context(), login)
			want := "/usr/bin/open"
			if platform == "linux" {
				want = "/usr/bin/xdg-open"
			}
			if command != want || len(values) != 1 || values[0] != login {
				t.Fatalf("wrong browser command: %s", command)
			}
			if err == nil || strings.Contains(err.Error(), login) {
				t.Fatal("browser URL was exposed", err)
			}
		})
	}
}
func TestSetupOwnerBrowserRejectsInvalidAddressBeforeCommand(t *testing.T) {
	for _, value := range []string{"http://login.invalid/one-use", "https://user:password@login.invalid/one-use", "https://login.invalid/one-use\n", "not-a-url"} {
		files, _ := linuxTestFiles(t)
		called := false
		open := setupOwnerLoginBrowser("linux", func(context.Context, string, ...string) error { called = true; return nil }, files)
		if err := open(t.Context(), value); err == nil || called || strings.Contains(err.Error(), value) {
			t.Fatal("invalid address reached browser or report")
		}
	}
}

func TestSetupLinuxDefaultPersonBrowserFailureCanFinishMachineSetup(t *testing.T) {
	f, _, prompts, _ := newLinuxFleetFixture(t, "join")
	f.host.ownerLogin = nil
	calls := 0
	f.host.browserCommand = func(_ context.Context, name string, args ...string) error {
		calls++
		if name != "/usr/bin/xdg-open" || len(args) != 1 {
			t.Fatal("Linux default selected the wrong browser")
		}
		return errors.New("injected browser error: " + args[0])
	}
	r := linuxSetupRequest(t, "join")
	r.withPerson = true
	report, err := runSetup(t.Context(), f.host, r)
	if err == nil || calls != 1 || strings.Contains(err.Error(), "https://login") || !strings.Contains(err.Error(), "without --with-person") {
		t.Fatal("browser failure lost safe recovery advice", err)
	}
	assertJoinArtifactsPrivate(t, f.home, prompts.String(), report, "secret-node", "secret-client", "https://login.invalid/one-use")
	prompts.Reset()
	f.host.enrolmentInput = strings.NewReader("")
	r.withPerson = false
	report, err = runSetup(t.Context(), f.host, r)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || strings.Contains(prompts.String(), "Single-use join key") {
		t.Fatal("resume reopened browser or consumed more keys")
	}
	for _, check := range report.Readiness {
		if check.Status != "VERIFIED" {
			t.Fatal(report.Readiness)
		}
	}
}

func TestSetupOwnerBrowserForegroundTimeoutAllowsSignIn(t *testing.T) {
	f, e, prompts, _ := newLinuxFleetFixture(t, "join")
	f.host.ownerLogin = nil
	calls := 0
	f.host.browserCommand = func(ctx context.Context, name string, args ...string) error { calls++; <-ctx.Done(); return ctx.Err() }
	r := linuxSetupRequest(t, "join")
	r.withPerson = true
	report, err := runSetup(t.Context(), f.host, r)
	if err != nil {
		t.Fatalf("foreground browser prevented sign-in: %v", err)
	}
	if calls != 1 || len(e.roles) != 3 || e.roles[2] != "person" {
		t.Fatal("person enrollment was not confirmed", calls, e.roles)
	}
	for _, check := range report.Readiness {
		if check.Status != "VERIFIED" {
			t.Fatal(check)
		}
	}
	assertJoinArtifactsPrivate(t, f.home, prompts.String(), report, "secret-node", "secret-client", "https://login.invalid/one-use")
}
