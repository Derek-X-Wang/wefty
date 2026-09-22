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
	sampleAuthKeyLine         = `Authkey is set; but state is NeedsLogin. Ignoring authkey tskey-auth-k9EXAMPLE1CNTRL-0000000000000000000000000000example. Re-run with TSNET_FORCE_LOGIN=1 to force use of authkey.`
	sampleEnrollmentURLLine   = `To start this tsnet server, restart with TS_AUTHKEY set, or go to: https://login.example-control.test/a/0123456789abcdefEXAMPLE?authkey=tskey-auth-k9EXAMPLE1CNTRL-0000000000000000000000000000example`
	sampleRegisterURLLine     = `AuthURL is https://control.example-headscale.test/register/0123456789abcdef0123456789abcdef`
	sampleMagicDNSLine        = `tsnet running state path /var/lib/wefty/state; node-runner-1.example-tailnet.ts.net`
	sampleMagicDNSUpperLine   = `tsnet running state path /var/lib/wefty/state; NODE-RUNNER-1.EXAMPLE-TAILNET.TS.NET`
	sampleNodeKeyLine         = `tka: node key nodekey:aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899 rejected by tailnet lock`
	sampleDiscoKeyLongLine    = `magicsock: peer disco key discokey:aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899 endpoint change`
	sampleDiscoKeyShortLine   = `magicsock: node [ABCD123] key d:aabbccdd11223344 now using endpoint 203.0.113.5:41641`
	sampleTailnetIPv4Line     = `peerapi: serving http://100.101.102.103:5000/ for peer`
	sampleTailnetIPv6Line     = `netmap: peer addr fd7a:115c:a1e0:1234:5678:9abc:def0:1111 added`
	sampleUnrecognizedURLLine = `dial tcp: lookup control.example-control.test: no such host, see https://tailscale.com/s/dns-fail for help`
	syntheticAuthKeyValue     = `tskey-auth-k9EXAMPLE1CNTRL-0000000000000000000000000000example`
	syntheticHostname         = `node-runner-1.example-tailnet.ts.net`
	syntheticHostnameUpper    = `NODE-RUNNER-1.EXAMPLE-TAILNET.TS.NET`
	syntheticNodeKey          = `nodekey:aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899`
	syntheticDiscoKeyLong     = `discokey:aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899`
	syntheticDiscoKeyShort    = `d:aabbccdd11223344`
	syntheticTailnetIPv4      = `100.101.102.103`
	syntheticTailnetIPv6      = `fd7a:115c:a1e0:1234:5678:9abc:def0:1111`
	syntheticEnrollmentURL    = `https://login.example-control.test/a/0123456789abcdefEXAMPLE`
	syntheticRegisterURL      = `https://control.example-headscale.test/register/0123456789abcdef0123456789abcdef`
)

func TestRedactFabricLogStripsSensitiveContent(t *testing.T) {
	tests := []struct {
		name           string
		line           string
		mustNotContain []string
	}{
		{"auth key", sampleAuthKeyLine, []string{syntheticAuthKeyValue}},
		{"enrollment URL query key", sampleEnrollmentURLLine, []string{syntheticAuthKeyValue, "authkey=tskey", "key=tskey"}},
		{"MagicDNS-style hostname", sampleMagicDNSLine, []string{syntheticHostname}},
		{"MagicDNS-style hostname, uppercase", sampleMagicDNSUpperLine, []string{syntheticHostnameUpper}},
		{"nodekey: long form", sampleNodeKeyLine, []string{syntheticNodeKey}},
		{"discokey: long form", sampleDiscoKeyLongLine, []string{syntheticDiscoKeyLong}},
		{"d: abbreviated disco key", sampleDiscoKeyShortLine, []string{syntheticDiscoKeyShort}},
		{"tailnet IPv4 (100.64.0.0/10)", sampleTailnetIPv4Line, []string{syntheticTailnetIPv4}},
		{"tailnet IPv6 (fd7a:115c:a1e0::/48)", sampleTailnetIPv6Line, []string{syntheticTailnetIPv6}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := redactFabricLog(tt.line)
			for _, secret := range tt.mustNotContain {
				if strings.Contains(got, secret) {
					t.Fatalf("redactFabricLog(%q) = %q, still contains %q", tt.line, got, secret)
				}
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

func TestWrapUserLogfPrintsRealURLIntactWhenOptedIn(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "1")

	var got []string
	sink := func(format string, args ...any) {
		got = append(got, fmt.Sprintf(format, args...))
	}
	userLogf := wrapUserLogf(sink)

	userLogf("To start this tsnet server, restart with TS_AUTHKEY set, or go to: %s",
		syntheticEnrollmentURL+"?authkey="+syntheticAuthKeyValue)

	if len(got) != 1 {
		t.Fatalf("sink called %d times, want 1: %v", len(got), got)
	}
	// The host and /a/<token> path must survive byte-for-byte: an operator
	// has to be able to paste this URL into a browser (wefty #498 finding
	// 1). Only the authkey= query value is expected to be stripped.
	if !strings.Contains(got[0], syntheticEnrollmentURL) {
		t.Fatalf("opted-in enrollment line %q does not contain the real URL %q intact", got[0], syntheticEnrollmentURL)
	}
	if strings.Contains(got[0], syntheticAuthKeyValue) {
		t.Fatalf("opted-in enrollment line %q still leaked the auth key", got[0])
	}
}

func TestWrapUserLogfDetectsAlternateControlRegisterShape(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "")

	var got []string
	sink := func(format string, args ...any) {
		got = append(got, fmt.Sprintf(format, args...))
	}
	userLogf := wrapUserLogf(sink)

	userLogf("%s", sampleRegisterURLLine)

	if len(got) != 1 {
		t.Fatalf("sink called %d times, want 1: %v", len(got), got)
	}
	if got[0] != enrollmentRequiredMessage {
		t.Fatalf("register/<key> line = %q, want the wefty-owned enrollment message %q", got[0], enrollmentRequiredMessage)
	}

	t.Run("opted in keeps the real URL", func(t *testing.T) {
		t.Setenv(printEnrollmentURLEnv, "1")
		var got2 []string
		sink2 := func(format string, args ...any) { got2 = append(got2, fmt.Sprintf(format, args...)) }
		userLogf2 := wrapUserLogf(sink2)
		userLogf2("%s", sampleRegisterURLLine)
		if len(got2) != 1 || !strings.Contains(got2[0], syntheticRegisterURL) {
			t.Fatalf("opted-in register/<key> line = %v, want it to contain %q", got2, syntheticRegisterURL)
		}
	})
}

func TestWrapUserLogfKeepsUnrecognizedURLLineButRedactsTheURL(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "")

	var got []string
	sink := func(format string, args ...any) {
		got = append(got, fmt.Sprintf(format, args...))
	}
	userLogf := wrapUserLogf(sink)

	userLogf("%s", sampleUnrecognizedURLLine)

	if len(got) != 1 {
		t.Fatalf("sink called %d times, want 1: %v", len(got), got)
	}
	if got[0] == enrollmentRequiredMessage {
		t.Fatalf("an ordinary error URL was swallowed into the enrollment notice: %q", got[0])
	}
	if !strings.Contains(got[0], "dial tcp: lookup control.example-control.test: no such host") {
		t.Fatalf("non-enrollment line %q lost its error context", got[0])
	}
	if strings.Contains(got[0], "https://tailscale.com/s/dns-fail") {
		t.Fatalf("non-enrollment line %q still contains the raw URL, want it blanked", got[0])
	}
	if !strings.Contains(got[0], "[REDACTED-URL]") {
		t.Fatalf("non-enrollment line %q missing the [REDACTED-URL] placeholder", got[0])
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

func TestWrapBackendLogfDiscardsWithNoCallerSuppliedLogf(t *testing.T) {
	backendLogf := wrapBackendLogf(nil)

	backendLogf("magicsock: some verbose backend detail")

	// wrapBackendLogf(nil) must be safe to call and must not panic; there
	// is no sink to observe, so the assertion is just that it returns.
	_ = backendLogf
}

func TestWrapBackendLogfForwardsRedactedLinesToCallerSuppliedLogf(t *testing.T) {
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
		t.Fatalf("backend line %q still leaked the tailnet hostname", got[0])
	}
}

func TestWrapBackendLogfAppliesTheSameEnrollmentPolicyAsUserLogf(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "")

	var got []string
	sink := func(format string, args ...any) {
		got = append(got, fmt.Sprintf(format, args...))
	}
	backendLogf := wrapBackendLogf(sink)

	// This is the exact shape finding 2 identified: control/controlclient's
	// AuthURL log and ipn/ipnlocal's "Received auth URL" log both go
	// through the backend hook, not the user-facing one. With the opt-in
	// off, it must never leak the raw URL either.
	backendLogf("AuthURL is %s", syntheticEnrollmentURL+"?authkey="+syntheticAuthKeyValue)

	if len(got) != 1 {
		t.Fatalf("sink called %d times, want 1: %v", len(got), got)
	}
	if got[0] != enrollmentRequiredMessage {
		t.Fatalf("backend enrollment line = %q, want the wefty-owned message %q", got[0], enrollmentRequiredMessage)
	}

	t.Run("opted in keeps the real URL", func(t *testing.T) {
		t.Setenv(printEnrollmentURLEnv, "1")
		var got2 []string
		sink2 := func(format string, args ...any) { got2 = append(got2, fmt.Sprintf(format, args...)) }
		backendLogf2 := wrapBackendLogf(sink2)
		backendLogf2("AuthURL is %s", syntheticEnrollmentURL+"?authkey="+syntheticAuthKeyValue)
		if len(got2) != 1 || !strings.Contains(got2[0], syntheticEnrollmentURL) {
			t.Fatalf("opted-in backend enrollment line = %v, want it to contain %q", got2, syntheticEnrollmentURL)
		}
		if strings.Contains(got2[0], syntheticAuthKeyValue) {
			t.Fatalf("opted-in backend enrollment line %q still leaked the auth key", got2[0])
		}
	})
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
