package tsnet

import (
	"fmt"
	"strings"
	"testing"
)

// Synthetic samples only -- none of these are real credentials or a real
// tailnet identifier. Shapes match what the pinned tailscale.com/tsnet
// dependency actually logs (see fabric/tsnet/redact.go's pattern comments).
const (
	sampleAuthKeyLine       = `Authkey is set; but state is NeedsLogin. Ignoring authkey tskey-auth-k9EXAMPLE1CNTRL-0000000000000000000000000000example. Re-run with TSNET_FORCE_LOGIN=1 to force use of authkey.`
	sampleEnrollmentURLLine = `To start this tsnet server, restart with TS_AUTHKEY set, or go to: https://login.example-control.test/a/0123456789abcdefEXAMPLE?authkey=tskey-auth-k9EXAMPLE1CNTRL-0000000000000000000000000000example`
	sampleMagicDNSLine      = `tsnet running state path /var/lib/wefty/state; node-runner-1.example-tailnet.ts.net`
	syntheticAuthKeyValue   = `tskey-auth-k9EXAMPLE1CNTRL-0000000000000000000000000000example`
	syntheticHostname       = `node-runner-1.example-tailnet.ts.net`
)

func TestRedactFabricLogStripsSensitiveContent(t *testing.T) {
	tests := []struct {
		name string
		line string
	}{
		{"auth key", sampleAuthKeyLine},
		{"enrollment URL with key", sampleEnrollmentURLLine},
		{"MagicDNS-style hostname", sampleMagicDNSLine},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := redactFabricLog(tt.line)
			if strings.Contains(got, syntheticAuthKeyValue) {
				t.Fatalf("redactFabricLog(%q) = %q, still contains the auth key", tt.line, got)
			}
			if strings.Contains(got, "authkey=tskey") || strings.Contains(got, "key=tskey") {
				t.Fatalf("redactFabricLog(%q) = %q, still contains an unredacted key= query", tt.line, got)
			}
			if strings.Contains(got, syntheticHostname) {
				t.Fatalf("redactFabricLog(%q) = %q, still contains the tailnet hostname", tt.line, got)
			}
		})
	}
}

func TestWrapUserLogfDefaultsToWeftyEnrollmentLine(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "")

	var got []string
	sink := func(format string, args ...any) {
		got = append(got, fmt.Sprintf(format, args...))
	}
	userLogf := wrapUserLogf(sink)

	userLogf("To start this tsnet server, restart with TS_AUTHKEY set, or go to: %s",
		"https://login.example-control.test/a/0123456789abcdefEXAMPLE?authkey="+syntheticAuthKeyValue)

	if len(got) != 1 {
		t.Fatalf("sink called %d times, want 1: %v", len(got), got)
	}
	if got[0] != enrollmentRequiredMessage {
		t.Fatalf("enrollment line = %q, want the wefty-owned message %q", got[0], enrollmentRequiredMessage)
	}
	if strings.Contains(got[0], syntheticAuthKeyValue) || strings.Contains(got[0], "login.example-control.test") {
		t.Fatalf("enrollment line %q leaked the raw URL or key", got[0])
	}
}

func TestWrapUserLogfPrintsURLOnlyWhenOptedIn(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "1")

	var got []string
	sink := func(format string, args ...any) {
		got = append(got, fmt.Sprintf(format, args...))
	}
	userLogf := wrapUserLogf(sink)

	userLogf("To start this tsnet server, restart with TS_AUTHKEY set, or go to: %s",
		"https://login.example-control.test/a/0123456789abcdefEXAMPLE?authkey="+syntheticAuthKeyValue)

	if len(got) != 1 {
		t.Fatalf("sink called %d times, want 1: %v", len(got), got)
	}
	if strings.Contains(got[0], syntheticAuthKeyValue) {
		t.Fatalf("opted-in enrollment line %q still leaked the auth key", got[0])
	}
	if !strings.Contains(got[0], "login.") || !strings.Contains(got[0], "/a/") {
		t.Fatalf("opted-in enrollment line %q dropped the login URL an operator needs", got[0])
	}
}

func TestWrapUserLogfRedactsNonEnrollmentLines(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "")

	var got []string
	sink := func(format string, args ...any) {
		got = append(got, fmt.Sprintf(format, args...))
	}
	userLogf := wrapUserLogf(sink)

	userLogf("%s", sampleMagicDNSLine)

	if len(got) != 1 {
		t.Fatalf("sink called %d times, want 1: %v", len(got), got)
	}
	if strings.Contains(got[0], syntheticHostname) {
		t.Fatalf("non-enrollment line %q still leaked the tailnet hostname", got[0])
	}
}

func TestWrapBackendLogfSilentByDefault(t *testing.T) {
	t.Setenv(debugLogEnv, "")

	called := false
	sink := func(format string, args ...any) { called = true }
	backendLogf := wrapBackendLogf(sink)

	backendLogf("magicsock: some verbose backend detail")

	if called {
		t.Fatal("backend log forwarded a line to sink with WEFTY_FABRIC_DEBUG_LOG unset")
	}
}

func TestWrapBackendLogfForwardsWhenDebugEnabled(t *testing.T) {
	t.Setenv(debugLogEnv, "1")

	var got []string
	sink := func(format string, args ...any) {
		got = append(got, fmt.Sprintf(format, args...))
	}
	backendLogf := wrapBackendLogf(sink)

	backendLogf("magicsock: %s", sampleMagicDNSLine)

	if len(got) != 1 {
		t.Fatalf("sink called %d times, want 1: %v", len(got), got)
	}
	if strings.Contains(got[0], syntheticHostname) {
		t.Fatalf("debug backend line %q still leaked the tailnet hostname", got[0])
	}
}

func TestNewSetsWeftyOwnedLoggersByDefault(t *testing.T) {
	f, err := New(Config{Name: "wefty://node/runner-1"})
	if err != nil {
		t.Fatal(err)
	}
	if f.server.Logf == nil {
		t.Fatal("New() with a zero Config.Logf left server.Logf nil")
	}
	if f.server.UserLogf == nil {
		t.Fatal("New() with a zero Config.Logf left server.UserLogf nil")
	}
}

func TestNewSetsWeftyOwnedLoggersWhenCallerSuppliesOne(t *testing.T) {
	calls := 0
	f, err := New(Config{
		Name: "wefty://node/runner-1",
		Logf: func(string, ...any) { calls++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.server.Logf == nil {
		t.Fatal("New() with a caller Config.Logf left server.Logf nil")
	}
	if f.server.UserLogf == nil {
		t.Fatal("New() with a caller Config.Logf left server.UserLogf nil")
	}
}
