package tsnet

import (
	"fmt"
	"log"
	"os"
	"regexp"
)

// enrollmentRequiredMessage is the wefty-owned line printed in place of the
// pinned tailscale.com/tsnet dependency's own interactive enrollment
// message, which otherwise includes the raw one-time login URL. It never
// names Tailscale: the fabric seam ends at this package. The embedded node
// is its own identity -- separate from any Tailscale client the host
// machine happens to run -- so there is no ambient client to point an
// operator at; --fabric-print-enrollment-url is the only way to see it.
const enrollmentRequiredMessage = "Fabric enrollment required; rerun with --fabric-print-enrollment-url (or WEFTY_FABRIC_PRINT_ENROLLMENT_URL=1) to print the login URL"

// printEnrollmentURLEnv opts an operator into seeing the raw enrollment URL
// the pinned tsnet dependency would otherwise have printed unredacted. Off
// by default, so nothing prints it unless an operator asks: most wefty
// invocations (including every scripted one) have no human present to read
// stderr, and the URL carries a one-time interactive login token.
const printEnrollmentURLEnv = "WEFTY_FABRIC_PRINT_ENROLLMENT_URL"

var (
	// tskey-... is the pinned tailscale.com dependency's auth key shape
	// (e.g. tskey-auth-<stable id>-<secret>). It never belongs in a log.
	authKeyPattern = regexp.MustCompile(`tskey-[A-Za-z0-9-]+`)
	// authkey=... / key=... are the query parameters an enrollment or
	// coordination URL carries the same secret in.
	authKeyQueryPattern = regexp.MustCompile(`(?i)\b(authkey|key)=[^&\s"'<>]+`)
	// nodekey:<hex> is a node's long-form public key (types/key/node.go's
	// NodePublic.String). It is a public identifier, not a secret, but it
	// still names a specific tailnet member and stays behind the seam.
	nodeKeyPattern = regexp.MustCompile(`(?i)\bnodekey:[0-9a-f]+\b`)
	// discokey:<hex> is a disco (path-discovery) public key in its long
	// form (types/key/disco.go's DiscoPublic.String); d:<hex> is its
	// abbreviated 8-byte debug form (DiscoPublic.ShortString), as emitted
	// by magicsock's own logging.
	discoKeyPattern      = regexp.MustCompile(`(?i)\bdiscokey:[0-9a-f]+\b`)
	discoKeyShortPattern = regexp.MustCompile(`(?i)\bd:[0-9a-f]{8,}\b`)
	// Tailnet addresses: the CGNAT IPv4 range 100.64.0.0/10 tailscale
	// assigns node addresses from, and its IPv6 ULA range
	// fd7a:115c:a1e0::/48. Both identify a specific tailnet member.
	tailnetIPv4Pattern = regexp.MustCompile(`\b100\.(?:6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\.[0-9]{1,3}\.[0-9]{1,3}\b`)
	tailnetIPv6Pattern = regexp.MustCompile(`(?i)\bfd7a:115c:a1e0:[0-9a-f:]*\b`)
	// MagicDNS-style tailnet hostnames identify a specific tailnet and are
	// kept behind the fabric seam like every other tailscale-specific name.
	// Matching is case-insensitive: tsnet's own lines are consistently
	// lowercase, but nothing guarantees a forwarded caller message is. A
	// tailnet running a custom MagicDNS suffix (an enterprise "alt domain")
	// is not covered here -- there is no generic pattern for a suffix an
	// operator chose, and this package does not attempt one.
	magicDNSPattern = regexp.MustCompile(`(?i)\b[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*\.ts\.net\b`)
	// enrollmentURLPattern recognizes the two enrollment URL shapes wefty
	// knows how to gate: the tailscale.com SaaS controller's
	// login.<host>/a/<token> path (control/controlclient/direct.go's
	// "AuthURL is %v" log and ipn/ipnlocal/local.go's "Received auth URL"
	// log both forward resp.AuthURL verbatim) and an alternate control
	// server's <control-host>/register/<key> path. Both carry a one-time
	// login token in the URL itself, which is why they get the
	// notice/opt-in treatment below instead of ordinary conservative URL
	// redaction.
	enrollmentURLPattern = regexp.MustCompile(`(?i)\bhttps?://(?:login\.[a-zA-Z0-9.-]+/a/[A-Za-z0-9_-]+|[a-zA-Z0-9.-]+/register/[A-Za-z0-9_-]+)\S*`)
	// Any other http(s) URL a forwarded line carries is unrecognized: it
	// might be an ordinary error message (a dial failure, a bad control
	// URL) rather than an enrollment prompt. processFabricLine keeps the
	// line but blanks the URL with this pattern unless the print opt-in is
	// on, rather than swallowing the whole line.
	urlPattern = regexp.MustCompile(`https?://\S+`)
)

// redactFabricLog strips anything that looks like a tailscale auth key or
// key= query parameter, a node/disco public key, a tailnet address, or a
// MagicDNS-style tailnet hostname out of a line the pinned tsnet dependency
// produced. It is applied to every line this package forwards, whether or
// not that line was recognised as an enrollment or other URL, as defense in
// depth against a future tsnet release folding a credential into some other
// status line. It deliberately leaves a recognised enrollment URL's host and
// path alone -- see enrollmentURLPattern and processFabricLine for that
// policy, which is what makes the opt-in actually usable.
func redactFabricLog(line string) string {
	line = authKeyQueryPattern.ReplaceAllString(line, "$1=[REDACTED]")
	line = authKeyPattern.ReplaceAllString(line, "[REDACTED]")
	line = nodeKeyPattern.ReplaceAllString(line, "nodekey:[REDACTED]")
	line = discoKeyPattern.ReplaceAllString(line, "discokey:[REDACTED]")
	line = discoKeyShortPattern.ReplaceAllString(line, "d:[REDACTED]")
	line = tailnetIPv4Pattern.ReplaceAllString(line, "[REDACTED-TAILNET-ADDR]")
	line = tailnetIPv6Pattern.ReplaceAllString(line, "[REDACTED-TAILNET-ADDR]")
	line = magicDNSPattern.ReplaceAllString(line, "[REDACTED]")
	return line
}

func fabricPrintEnrollmentURLEnabled() bool {
	return os.Getenv(printEnrollmentURLEnv) == "1"
}

// processFabricLine applies wefty's enrollment disclosure policy to a line
// forwarded by either tsnet hook, then redacts whatever remains. It is the
// single place that policy is decided, so the user-facing and backend logs
// can never disagree about what an operator is allowed to see:
//
//   - A recognised enrollment URL (enrollmentURLPattern) becomes the
//     wefty-owned notice, unless the operator opted in, in which case the
//     real URL is kept intact -- only its credential-bearing query
//     parameters are stripped by redactFabricLog.
//   - Any other http(s) URL is conservatively blanked unless that same
//     opt-in is set, so an ordinary error message keeps its text instead of
//     being swallowed.
//   - Every other line is just redacted for stray identifiers.
func processFabricLine(line string) string {
	switch {
	case enrollmentURLPattern.MatchString(line):
		if !fabricPrintEnrollmentURLEnabled() {
			return enrollmentRequiredMessage
		}
		return redactFabricLog(line)
	case urlPattern.MatchString(line) && !fabricPrintEnrollmentURLEnabled():
		return redactFabricLog(urlPattern.ReplaceAllString(line, "[REDACTED-URL]"))
	default:
		return redactFabricLog(line)
	}
}

// wrapUserLogf builds the wefty-owned function tsnet.Server.UserLogf is
// always set to. sink receives the final line after redaction; a nil sink
// falls back to log.Printf, matching the pinned dependency's own default
// for an unset UserLogf, so the field is still useful when a caller passes
// a zero Config.
func wrapUserLogf(sink func(string, ...any)) func(string, ...any) {
	if sink == nil {
		sink = log.Printf
	}
	return func(format string, args ...any) {
		sink("%s", processFabricLine(fmt.Sprintf(format, args...)))
	}
}

// wrapBackendLogf builds the wefty-owned function tsnet.Server.Logf is
// always set to. The pinned dependency documents this field as its verbose
// backend log (LocalBackend, control/login, TKA, DNS, netmap, MagicSock,
// netcheck, and friends). When the caller supplied a Config.Logf, backend
// lines are forwarded to it under the same enrollment policy as the
// user-facing log (processFabricLine), so an enrollment URL can never leak
// through the backend channel while the opt-in is off -- this is what makes
// the pre-existing wefty-l1/wefty-l3/wefty-agent callers' loggers safe to
// keep receiving backend output from (wefty #498). With no caller-supplied
// Logf, the backend log is discarded entirely, matching the pinned
// dependency's own default for an unset Logf (not stderr).
func wrapBackendLogf(sink func(string, ...any)) func(string, ...any) {
	if sink == nil {
		return func(string, ...any) {}
	}
	return func(format string, args ...any) {
		sink("%s", processFabricLine(fmt.Sprintf(format, args...)))
	}
}
