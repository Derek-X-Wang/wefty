package tsnet

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"strings"
	"unicode"
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
)

// redactFabricLog strips anything that looks like a tailscale auth key or
// key= query parameter, a node/disco public key, a tailnet address, or a
// MagicDNS-style tailnet hostname out of text the pinned tsnet dependency
// produced. URL userinfo is handled structurally by processFabricLine before
// this pure text redactor runs.
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

// nextHTTPURLStart finds the next candidate URL without deciding where it
// ends. URL boundaries are determined by scanHTTPURLToken, not a regexp, so
// punctuation-delimited structured fields cannot join one URL to the next.
func nextHTTPURLStart(line string) int {
	httpStart := strings.Index(line, "http://")
	httpsStart := strings.Index(line, "https://")
	if httpStart < 0 {
		return httpsStart
	}
	if httpsStart < 0 || httpStart < httpsStart {
		return httpStart
	}
	return httpsStart
}

// scanHTTPURLToken returns the length of the candidate URL at the start of
// line. Whitespace and punctuation used to delimit log fields end the token;
// a sentence-ending period is left outside it.
func scanHTTPURLToken(line string) int {
	end := len(line)
	for i, r := range line {
		if unicode.IsSpace(r) || strings.ContainsRune("\"'<>,;)]", r) {
			end = i
			break
		}
	}
	for end > 0 && line[end-1] == '.' {
		end--
	}
	return end
}

func isEnrollmentURL(u *url.URL) bool {
	host := strings.ToLower(u.Hostname())
	return strings.HasPrefix(host, "login.") && strings.HasPrefix(u.Path, "/a/") ||
		strings.HasPrefix(u.Path, "/register/")
}

func redactEnrollmentURL(rawURL string) string {
	rawURL = authKeyQueryPattern.ReplaceAllString(rawURL, "$1=[REDACTED]")
	return authKeyPattern.ReplaceAllString(rawURL, "[REDACTED]")
}

// processFabricLine applies wefty's enrollment disclosure policy to a line
// forwarded by either tsnet hook, then redacts whatever remains. It is the
// single place that policy is decided, so the user-facing and backend logs
// can never disagree about what an operator is allowed to see:
//
//   - Candidate URLs end at whitespace or log-field punctuation, are parsed,
//     and have userinfo stripped unconditionally.
//   - A recognised enrollment URL becomes the
//     wefty-owned notice, unless the operator opted in, in which case the
//     real URL is kept intact -- only its credential-bearing query
//     parameters are stripped, not the identifier patterns that would
//     otherwise mangle a CGNAT/MagicDNS-hosted control server's URL.
//   - Any other http(s) URL is conservatively blanked unless that same
//     opt-in is set, so an ordinary error message keeps its text instead of
//     being swallowed.
//   - Text outside an opted-in enrollment URL is redacted normally.
func processFabricLine(line string) string {
	optedIn := fabricPrintEnrollmentURLEnabled()
	var out strings.Builder
	for {
		start := nextHTTPURLStart(line)
		if start < 0 {
			out.WriteString(redactFabricLog(line))
			return out.String()
		}

		out.WriteString(redactFabricLog(line[:start]))
		tokenLen := scanHTTPURLToken(line[start:])
		rawURL := line[start : start+tokenLen]
		u, err := url.Parse(rawURL)
		if err != nil {
			out.WriteString("[REDACTED-URL]")
			line = line[start+tokenLen:]
			continue
		}

		// A control URL can carry basic-auth credentials. There is no opt-in
		// for these, so remove parsed userinfo before classifying or emitting
		// any URL.
		u.User = nil
		safeURL := u.String()
		if isEnrollmentURL(u) {
			if !optedIn {
				return enrollmentRequiredMessage
			}
			out.WriteString(redactEnrollmentURL(safeURL))
		} else if optedIn {
			out.WriteString(redactFabricLog(safeURL))
		} else {
			out.WriteString("[REDACTED-URL]")
		}
		line = line[start+tokenLen:]
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
