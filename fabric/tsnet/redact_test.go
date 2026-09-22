package tsnet

import (
	"bytes"
	"fmt"
	"log"
	"os"
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
	sampleMachineKeyLine      = `error decoding RegisterResponse with server key mkey:00112233445566778899aabbccddeeff and machine key mkey:ffeeddccbbaa99887766554433221100: unexpected EOF`
	sampleLoginIdentityLine   = `netmap: self: [nUcW1] auth=MachineAuthorized u=operator@example-tailnet.test [100.101.102.103/32]`
	sampleDiscoKeyLongLine    = `magicsock: peer disco key discokey:aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899 endpoint change`
	sampleDiscoKeyShortLine   = `magicsock: node [ABCD123] key d:aabbccdd11223344 now using endpoint 203.0.113.5:41641`
	sampleTailnetIPv4Line     = `peerapi: serving http://100.101.102.103:5000/ for peer`
	sampleTailnetIPv6Line     = `netmap: peer addr fd7a:115c:a1e0:1234:5678:9abc:def0:1111 added`
	sampleUnrecognizedURLLine = `dial tcp: lookup control.example-control.test: no such host, see https://tailscale.com/s/dns-fail for help`
	syntheticAuthKeyValue     = `tskey-auth-k9EXAMPLE1CNTRL-0000000000000000000000000000example`
	syntheticHostname         = `node-runner-1.example-tailnet.ts.net`
	syntheticHostnameUpper    = `NODE-RUNNER-1.EXAMPLE-TAILNET.TS.NET`
	syntheticNodeKey          = `nodekey:aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899`
	syntheticMachineKey       = `mkey:ffeeddccbbaa99887766554433221100`
	syntheticLoginIdentity    = `operator@example-tailnet.test`
	syntheticDiscoKeyLong     = `discokey:aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899`
	syntheticDiscoKeyShort    = `d:aabbccdd11223344`
	syntheticTailnetIPv4      = `100.101.102.103`
	syntheticTailnetIPv6      = `fd7a:115c:a1e0:1234:5678:9abc:def0:1111`
	syntheticEnrollmentURL    = `https://login.example-control.test/a/0123456789abcdefEXAMPLE`
	syntheticRegisterURL      = `https://control.example-headscale.test/register/0123456789abcdef0123456789abcdef`
	// Round-2 review samples: an enrollment URL whose host happens to be a
	// tailnet address or MagicDNS name, and a control URL carrying
	// basic-auth userinfo.
	syntheticTailnetHostRegisterURL  = `http://100.101.102.103/register/0123456789abcdef`
	syntheticMagicDNSHostAuthURL     = `https://login.example-tailnet.ts.net/a/0123456789abcdefEXAMPLE`
	sampleControlURLWithUserinfoLine = `control server key from https://operator:example-password@control.example.test`
	syntheticControlURLUserinfo      = `operator:example-password@`
	// Round-4 review samples: a URL scheme is case-insensitive and nothing
	// in the pinned dependency lowercases the operator-supplied control URL
	// before logging it, so every rule has to survive a mixed-case scheme.
	sampleUpperSchemeUserinfoLine   = `control server key from HTTPS://operator:example-password@control.example.test`
	sampleUpperSchemeEnrollmentLine = `AuthURL is HTTPS://LOGIN.EXAMPLE-CONTROL.TEST/a/0123456789abcdefEXAMPLE`
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
		{"mkey: machine key long form", sampleMachineKeyLine, []string{syntheticMachineKey}},
		{"tailnet login identity", sampleLoginIdentityLine, []string{syntheticLoginIdentity}},
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

// TestRedactFabricLogLeavesAbbreviatedNodeKeys documents a deliberate
// coverage limit rather than an oversight: NodePublic.ShortString renders as
// five base64 characters in brackets, which no pattern can tell apart from
// the dependency's many other bracketed short strings. The limit is stated
// next to the key patterns in redact.go; this test pins it so a later change
// that silently starts blanking bracketed text is visible.
func TestRedactFabricLogLeavesAbbreviatedNodeKeys(t *testing.T) {
	const line = `magicsock: derp route for [nUcW1] set to derp-1 (nyc)`
	if got := redactFabricLog(line); got != line {
		t.Fatalf("redactFabricLog(%q) = %q, want it unchanged: abbreviated keys are out of scope", line, got)
	}
}

func TestWrapUserLogfDefaultsToWeftyEnrollmentLine(t *testing.T) {
	tests := []struct {
		name string
		line string
		want string
	}{
		{
			"lowercase scheme",
			sampleEnrollmentURLLine,
			`To start this tsnet server, restart with TS_AUTHKEY set, or go to: ` + enrollmentRequiredMessage,
		},
		{
			// Round-4 review finding 1: a byte-exact "https://" search let
			// this line through untouched, printing a live enrollment URL
			// with the opt-in off.
			"mixed-case scheme and host",
			sampleUpperSchemeEnrollmentLine,
			`AuthURL is ` + enrollmentRequiredMessage,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(printEnrollmentURLEnv, "")

			var got []string
			sink := func(format string, args ...any) {
				got = append(got, fmt.Sprintf(format, args...))
			}
			wrapUserLogf(sink)("%s", tt.line)

			if len(got) != 1 {
				t.Fatalf("sink called %d times, want 1: %v", len(got), got)
			}
			if got[0] != tt.want {
				t.Fatalf("enrollment line = %q, want %q", got[0], tt.want)
			}
			if strings.Contains(got[0], syntheticAuthKeyValue) || strings.Contains(got[0], "0123456789abcdefEXAMPLE") {
				t.Fatalf("enrollment line %q leaked the raw URL or key", got[0])
			}
		})
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

	// The host and /a/<token> path must survive byte-for-byte: an operator
	// has to be able to paste this URL into a browser (wefty #498 finding
	// 1). Only the authkey= query value is expected to be stripped, so the
	// assertion is the whole line, not a substring.
	const want = `To start this tsnet server, restart with TS_AUTHKEY set, or go to: ` +
		syntheticEnrollmentURL + `?authkey=[REDACTED]`
	if len(got) != 1 || got[0] != want {
		t.Fatalf("opted-in enrollment line = %q, want %q", got, want)
	}
}

// TestWrapUserLogfPreservesEnrollmentURLHostUnderIdentifierRedaction covers
// wefty #498 finding 1, round 2: when opted in, the enrollment URL's host
// must survive intact even when that host itself looks like a tailnet
// address or a MagicDNS name -- the identifier-redaction patterns must not
// run over the enrollment URL.
func TestWrapUserLogfPreservesEnrollmentURLHostUnderIdentifierRedaction(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "1")

	tests := []struct {
		name string
		line string
		want string
	}{
		{"CGNAT-host register URL", "AuthURL is " + syntheticTailnetHostRegisterURL, syntheticTailnetHostRegisterURL},
		{"MagicDNS-host login/a URL", "AuthURL is " + syntheticMagicDNSHostAuthURL, syntheticMagicDNSHostAuthURL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			sink := func(format string, args ...any) { got = append(got, fmt.Sprintf(format, args...)) }
			userLogf := wrapUserLogf(sink)

			userLogf("%s", tt.line)

			if len(got) != 1 {
				t.Fatalf("sink called %d times, want 1: %v", len(got), got)
			}
			if got[0] != "AuthURL is "+tt.want {
				t.Fatalf("opted-in enrollment line = %q, want the URL exactly intact as %q", got[0], "AuthURL is "+tt.want)
			}
		})
	}
}

func TestWrapUserLogfBoundsEnrollmentURLBeforeAdjacentPeerURL(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "1")

	const line = `AuthURL is "https://login.example.test/a/token",peer="http://100.101.102.103"`
	const want = `AuthURL is "https://login.example.test/a/token",peer="http://[REDACTED-TAILNET-ADDR]"`
	var got []string
	userLogf := wrapUserLogf(func(format string, args ...any) {
		got = append(got, fmt.Sprintf(format, args...))
	})

	userLogf("%s", line)

	if len(got) != 1 || got[0] != want {
		t.Fatalf("opted-in adjacent URLs = %q, want %q", got, want)
	}
}

// TestWrapUserLogfBlanksTheWholeRunWhenNoURLPrefixParses covers the round-4
// review's finding 2. Every character used to bound a URL token is legal
// inside a URL, so the bound is a guess about log formatting. When the guess
// cuts a real URL in half the remainder must not fall through to the plain
// text path: a basic-auth password containing a comma or a parenthesis used
// to have its tail printed, in both opt-in states, even though userinfo has
// no opt-in at all.
func TestWrapUserLogfBlanksTheWholeRunWhenNoURLPrefixParses(t *testing.T) {
	tests := []struct {
		name string
		line string
		want string
	}{
		{
			"password carrying a comma",
			`control server key from https://operator:s3cr,et@control.example.test: ts2021=[nUcW1]`,
			`control server key from [REDACTED-URL]: ts2021=[nUcW1]`,
		},
		{
			"password carrying a closing parenthesis",
			`control server key from https://operator:s3cr)et@control.example.test: ts2021=[nUcW1]`,
			`control server key from [REDACTED-URL]: ts2021=[nUcW1]`,
		},
	}
	for _, optedIn := range []string{"", "1"} {
		for _, tt := range tests {
			t.Run(tt.name+"/opted-in="+optedIn, func(t *testing.T) {
				t.Setenv(printEnrollmentURLEnv, optedIn)

				var got []string
				wrapUserLogf(func(format string, args ...any) {
					got = append(got, fmt.Sprintf(format, args...))
				})("%s", tt.line)

				if len(got) != 1 || got[0] != tt.want {
					t.Fatalf("unparseable URL run = %q, want %q", got, tt.want)
				}
				if strings.Contains(got[0], "et@control.example.test") {
					t.Fatalf("line %q kept the tail of a basic-auth password", got[0])
				}
			})
		}
	}
}

// TestWrapBackendLogfHandlesBracketedIPv6Host covers the other half of the
// round-4 finding 2. ipn/ipnlocal logs "peerapi: serving on
// http://[<tailnet ULA>]:<port>" on every IPv6-capable start, so the ']' that
// closes a literal host is not a field delimiter: treating it as one made
// url.Parse fail and printed the port, path and query as ordinary text with
// the opt-in off.
func TestWrapBackendLogfHandlesBracketedIPv6Host(t *testing.T) {
	const line = `peerapi: got request http://[fd7a:115c:a1e0:ab12::1]:35201/v0/put/report.xlsx?authkey=` + syntheticAuthKeyValue
	tests := []struct {
		optedIn string
		want    string
	}{
		{"", `peerapi: got request [REDACTED-URL]`},
		{"1", `peerapi: got request http://[[REDACTED-TAILNET-ADDR]]:35201/v0/put/report.xlsx?authkey=[REDACTED]`},
	}
	for _, tt := range tests {
		t.Run("opted-in="+tt.optedIn, func(t *testing.T) {
			t.Setenv(printEnrollmentURLEnv, tt.optedIn)

			var got []string
			wrapBackendLogf(func(format string, args ...any) {
				got = append(got, fmt.Sprintf(format, args...))
			})("%s", line)

			if len(got) != 1 || got[0] != tt.want {
				t.Fatalf("bracketed IPv6 peerapi line = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestWrapUserLogfKeepsTheTextAroundTheEnrollmentNotice covers the round-4
// finding 7: replacing the whole line with the notice threw away the reason a
// line was logged. A control-server failure that happens to quote an
// enrollment-shaped URL still has to report its status code.
func TestWrapUserLogfKeepsTheTextAroundTheEnrollmentNotice(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "")

	const line = `POST https://ctrl.example.test/register/xyz: 500 Internal Server Error`
	want := `POST ` + enrollmentRequiredMessage + `: 500 Internal Server Error`

	var got []string
	wrapUserLogf(func(format string, args ...any) {
		got = append(got, fmt.Sprintf(format, args...))
	})("%s", line)

	if len(got) != 1 || got[0] != want {
		t.Fatalf("enrollment-shaped error line = %q, want %q", got, want)
	}
}

func TestWrapUserLogfDoesNotTreatQueryAtSignAsUserinfo(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "1")

	// The '@' belongs to the query, not to an authority: the host must stay
	// where it is. The address is redacted as a login identity, which is a
	// separate rule and is what makes the host's survival visible here.
	const line = `https://control.example.test?contact=ops@example.test`
	const want = `https://control.example.test?contact=[REDACTED-LOGIN]`
	var got []string
	userLogf := wrapUserLogf(func(format string, args ...any) {
		got = append(got, fmt.Sprintf(format, args...))
	})

	userLogf("%s", line)

	if len(got) != 1 || got[0] != want {
		t.Fatalf("URL with query @ = %q, want the host preserved as %q", got, want)
	}
}

func TestWrapUserLogfRedactsMalformedURLToken(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "1")

	const line = `request failed for https://%zz.`
	const want = `request failed for [REDACTED-URL].`
	var got []string
	userLogf := wrapUserLogf(func(format string, args ...any) {
		got = append(got, fmt.Sprintf(format, args...))
	})

	userLogf("%s", line)

	if len(got) != 1 || got[0] != want {
		t.Fatalf("malformed URL line = %q, want %q", got, want)
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

	want := `AuthURL is ` + enrollmentRequiredMessage
	if len(got) != 1 || got[0] != want {
		t.Fatalf("register/<key> line = %q, want %q", got, want)
	}

	t.Run("opted in keeps the real URL", func(t *testing.T) {
		t.Setenv(printEnrollmentURLEnv, "1")
		var got2 []string
		sink2 := func(format string, args ...any) { got2 = append(got2, fmt.Sprintf(format, args...)) }
		userLogf2 := wrapUserLogf(sink2)
		userLogf2("%s", sampleRegisterURLLine)
		want2 := `AuthURL is ` + syntheticRegisterURL
		if len(got2) != 1 || got2[0] != want2 {
			t.Fatalf("opted-in register/<key> line = %q, want %q", got2, want2)
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

	want := `dial tcp: lookup control.example-control.test: no such host, see [REDACTED-URL] for help`
	if len(got) != 1 || got[0] != want {
		t.Fatalf("non-enrollment URL line = %q, want %q", got, want)
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

// TestWrapBackendLogfDiscardsWithNoCallerSuppliedLogf pins the one place the
// two hooks deliberately differ: wrapUserLogf falls back to log.Printf for a
// nil sink, because that is what the pinned dependency does for an unset
// UserLogf, while wrapBackendLogf discards, because that is what the
// dependency does for an unset Logf. Capturing the standard logger makes the
// discard observable instead of assumed.
func TestWrapBackendLogfDiscardsWithNoCallerSuppliedLogf(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "")

	var captured bytes.Buffer
	previousFlags := log.Flags()
	log.SetOutput(&captured)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(os.Stderr)
		log.SetFlags(previousFlags)
	})

	wrapBackendLogf(nil)("magicsock: %s", sampleMagicDNSLine)

	if captured.Len() != 0 {
		t.Fatalf("wrapBackendLogf(nil) emitted %q, want the backend line discarded", captured.String())
	}

	// Positive control: the same nil handling in wrapUserLogf does print, so
	// an empty buffer above is a real discard rather than a broken capture.
	wrapUserLogf(nil)("a user-facing line")
	if captured.Len() == 0 {
		t.Fatal("wrapUserLogf(nil) emitted nothing, so the log capture is not working and the discard above proves nothing")
	}
}

func TestWrapBackendLogfForwardsRedactedLinesToCallerSuppliedLogf(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "")

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

	want := `AuthURL is ` + enrollmentRequiredMessage
	if len(got) != 1 || got[0] != want {
		t.Fatalf("backend enrollment line = %q, want %q", got, want)
	}

	t.Run("opted in keeps the real URL", func(t *testing.T) {
		t.Setenv(printEnrollmentURLEnv, "1")
		var got2 []string
		sink2 := func(format string, args ...any) { got2 = append(got2, fmt.Sprintf(format, args...)) }
		backendLogf2 := wrapBackendLogf(sink2)
		backendLogf2("AuthURL is %s", syntheticEnrollmentURL+"?authkey="+syntheticAuthKeyValue)
		want2 := `AuthURL is ` + syntheticEnrollmentURL + `?authkey=[REDACTED]`
		if len(got2) != 1 || got2[0] != want2 {
			t.Fatalf("opted-in backend enrollment line = %q, want %q", got2, want2)
		}
	})
}

// TestURLUserinfoIsAlwaysRedacted covers wefty #498 finding 2, round 2: the
// pinned dependency logs the configured control URL verbatim
// (control/controlclient/direct.go:714, "control server key from
// <serverURL>"), and that URL can carry basic-auth userinfo. Unlike the
// enrollment URL, there is no opt-in for a login/password: it must be
// stripped through both hooks, in both opt-in states. The mixed-case rows are
// the round-4 finding 1 -- the dependency stores and logs the operator's
// --control-url exactly as typed, and nothing normalises its scheme.
func TestURLUserinfoIsAlwaysRedacted(t *testing.T) {
	tests := []struct {
		name         string
		line         string
		wantOptedOut string
		wantOptedIn  string
	}{
		{
			name:         "lowercase scheme",
			line:         sampleControlURLWithUserinfoLine,
			wantOptedOut: `control server key from [REDACTED-URL]`,
			wantOptedIn:  `control server key from https://control.example.test`,
		},
		{
			name:         "mixed-case scheme",
			line:         sampleUpperSchemeUserinfoLine,
			wantOptedOut: `control server key from [REDACTED-URL]`,
			wantOptedIn:  `control server key from https://control.example.test`,
		},
	}
	for _, tt := range tests {
		for _, optedIn := range []string{"", "1"} {
			want := tt.wantOptedOut
			state := "opted out"
			if optedIn == "1" {
				want = tt.wantOptedIn
				state = "opted in"
			}
			hooks := map[string]func(func(string, ...any)) func(string, ...any){
				"UserLogf":    wrapUserLogf,
				"BackendLogf": wrapBackendLogf,
			}
			for hookName, hook := range hooks {
				t.Run(hookName+"/"+tt.name+"/"+state, func(t *testing.T) {
					t.Setenv(printEnrollmentURLEnv, optedIn)
					var got []string
					logf := hook(func(format string, args ...any) {
						got = append(got, fmt.Sprintf(format, args...))
					})

					logf("%s", tt.line)

					if len(got) != 1 {
						t.Fatalf("sink called %d times, want 1: %v", len(got), got)
					}
					if got[0] != want || strings.Contains(got[0], syntheticControlURLUserinfo) {
						t.Fatalf("%s (%s) = %q, want %q with no URL userinfo", hookName, state, got[0], want)
					}
				})
			}
		}
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
