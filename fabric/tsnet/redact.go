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

// enrollmentRequiredMessage is the wefty-owned text printed in place of the
// raw one-time login URL in the pinned tailscale.com/tsnet dependency's own
// interactive enrollment message. It never names Tailscale: the fabric seam
// ends at this package. The embedded node is its own identity -- separate
// from any Tailscale client the host machine happens to run -- so there is
// no ambient client to point an operator at; --fabric-print-enrollment-url
// is the only way to see it.
const enrollmentRequiredMessage = "Fabric enrollment required; rerun with --fabric-print-enrollment-url (or WEFTY_FABRIC_PRINT_ENROLLMENT_URL=1) to print the login URL"

// printEnrollmentURLEnv opts an operator into seeing the raw enrollment URL
// the pinned tsnet dependency would otherwise have printed unredacted. Off
// by default, so nothing prints it unless an operator asks: most wefty
// invocations (including every scripted one) have no human present to read
// stderr, and the URL carries a one-time interactive login token.
const printEnrollmentURLEnv = "WEFTY_FABRIC_PRINT_ENROLLMENT_URL"

// urlTailDelimiters are the characters a log line leaves on the end of a URL
// when it writes one into a structured field or an English sentence. They are
// the only characters a failed parse is allowed to hand back to the plain
// text stream, which is why the set contains punctuation and nothing else.
const urlTailDelimiters = `.:"'<>,;)]`

// urlFieldDelimiters end a candidate URL token. Every one of them is legal
// inside a URL, so they are a heuristic about log formatting, never a
// guarantee -- see parseHTTPURLToken for what happens when the heuristic
// cuts a real URL in half.
const urlFieldDelimiters = `"'<>,;)]`

var (
	// httpSchemePattern finds the next candidate URL. Matching is
	// case-insensitive: a URL scheme is case-insensitive (RFC 3986 3.1) and
	// nothing in the pinned dependency lowercases one before logging it --
	// tsnet passes Server.ControlURL through untouched and
	// control/controlclient stores it verbatim and logs it verbatim -- so a
	// byte-exact "https://" search would let an operator-typed
	// HTTPS://user:password@control walk past every rule below (wefty #498).
	httpSchemePattern = regexp.MustCompile(`(?i)https?://`)
	// tskey-... is the pinned tailscale.com dependency's auth key shape
	// (e.g. tskey-auth-<stable id>-<secret>). It never belongs in a log.
	authKeyPattern = regexp.MustCompile(`tskey-[A-Za-z0-9-]+`)
	// authkey=... / key=... are the query parameters an enrollment or
	// coordination URL carries the same secret in.
	authKeyQueryPattern = regexp.MustCompile(`(?i)\b(authkey|key)=[^&\s"'<>]+`)
	// A login name is the tailnet identity behind a node. The pinned
	// dependency prints one in its netmap header ("u=%s" in
	// types/netmap's printConciseHeader), which reaches the backend hook
	// every time ipn/ipnlocal logs a netmap diff. Matching is deliberately
	// broad: wefty routes no address of its own through these hooks, so an
	// over-eager match costs a diagnostic nothing.
	loginIdentityPattern = regexp.MustCompile(`(?i)\b[a-z0-9._%+-]+@[a-z0-9-]+(\.[a-z0-9-]+)*\.[a-z]{2,}\b`)
	// nodekey:<hex> is a node's long-form public key (types/key/node.go's
	// NodePublic.String); mkey:<hex> is a machine's (types/key/machine.go's
	// MachinePublic.String, logged by control/controlclient on a register
	// decode failure). Both are public identifiers, not secrets, but each
	// names a specific tailnet member and stays behind the seam.
	//
	// Coverage limit: the abbreviated forms these keys take in most backend
	// logging -- NodePublic.ShortString's "[" + 5 base64 characters + "]"
	// (types/key/util.go's debug32), as used throughout magicsock -- are
	// deliberately NOT redacted. No pattern for five unanchored base64
	// characters in brackets can tell a node key from the many other
	// bracketed short strings the dependency prints, and a redactor that
	// blanks arbitrary bracketed text destroys the diagnostics this channel
	// exists for. The long forms below are the ones worth catching.
	nodeKeyPattern    = regexp.MustCompile(`(?i)\bnodekey:[0-9a-f]+\b`)
	machineKeyPattern = regexp.MustCompile(`(?i)\bmkey:[0-9a-f]+\b`)
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
// key= query parameter, a tailnet login identity, a node/machine/disco
// public key, a tailnet address, or a MagicDNS-style tailnet hostname out of
// text the pinned tsnet dependency produced. URL userinfo is handled
// structurally by processFabricLine before this pure text redactor runs.
func redactFabricLog(line string) string {
	line = authKeyQueryPattern.ReplaceAllString(line, "$1=[REDACTED]")
	line = authKeyPattern.ReplaceAllString(line, "[REDACTED]")
	line = loginIdentityPattern.ReplaceAllString(line, "[REDACTED-LOGIN]")
	line = nodeKeyPattern.ReplaceAllString(line, "nodekey:[REDACTED]")
	line = machineKeyPattern.ReplaceAllString(line, "mkey:[REDACTED]")
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
// ends. URL boundaries are decided by scanHTTPURLToken and parseHTTPURLToken,
// not by a regexp, so punctuation-delimited structured fields cannot join one
// URL to the next.
func nextHTTPURLStart(line string) int {
	loc := httpSchemePattern.FindStringIndex(line)
	if loc == nil {
		return -1
	}
	return loc[0]
}

// httpURLRunLength returns the length of the whitespace-bounded run at the
// start of line. The run, not the shorter punctuation-bounded token, is the
// unit a failed parse blanks: anything narrower would leave the tail of an
// unparseable URL -- a bracketed IPv6 authority, a password carrying a comma
// -- to be printed as ordinary text (wefty #498).
func httpURLRunLength(line string) int {
	if i := strings.IndexFunc(line, unicode.IsSpace); i >= 0 {
		return i
	}
	return len(line)
}

// scanHTTPURLToken returns the length of the candidate URL at the start of
// run. Punctuation used to delimit log fields ends the token, except a ']'
// closing a bracketed IPv6 literal host, which belongs to the URL's own
// authority. A sentence-ending period or a field-separating colon is left
// outside the token.
func scanHTTPURLToken(run string) int {
	end := len(run)
	inHostBrackets := false
loop:
	for i, r := range run {
		switch {
		case inHostBrackets:
			if r == ']' {
				inHostBrackets = false
			}
		case r == '[' && i >= 3 && run[i-3:i] == "://":
			inHostBrackets = true
		case strings.ContainsRune(urlFieldDelimiters, r):
			end = i
			break loop
		}
	}
	return len(trimURLTail(run[:end]))
}

// trimURLTail drops the sentence-ending periods and field-separating colons
// a log line leaves on the end of a URL.
func trimURLTail(token string) string {
	for token != "" {
		switch token[len(token)-1] {
		case '.', ':':
			token = token[:len(token)-1]
		default:
			return token
		}
	}
	return token
}

// parseHTTPURLToken bounds and parses the candidate URL at the start of run.
// When the punctuation heuristic cut a real URL in half -- a comma inside a
// basic-auth password, say -- the parse fails; one trailing delimiter is then
// dropped at a time and the parse retried, and if nothing resolves the caller
// blanks the whole whitespace-bounded run rather than printing the remainder.
// Only delimiter characters are ever handed back to the plain text stream.
func parseHTTPURLToken(run string) (string, *url.URL, bool) {
	token := run[:scanHTTPURLToken(run)]
	for token != "" {
		if u, err := url.Parse(token); err == nil {
			return token, u, true
		}
		if !strings.ContainsRune(urlTailDelimiters, rune(token[len(token)-1])) {
			return "", nil, false
		}
		token = token[:len(token)-1]
	}
	return "", nil, false
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
//   - Candidate URLs are found case-insensitively, bounded by whitespace,
//     narrowed by log-field punctuation, parsed, and have userinfo stripped
//     unconditionally. A candidate that will not parse at all blanks its
//     whole whitespace-bounded run.
//   - A recognised enrollment URL is replaced in place by the wefty-owned
//     notice, keeping the surrounding text so a failure that happens to
//     carry an enrollment URL still reports its cause. With the operator
//     opted in the real URL is kept instead -- only its credential-bearing
//     query parameters are stripped, not the identifier patterns that would
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

		rest := line[start:]
		run := rest[:httpURLRunLength(rest)]
		token, u, ok := parseHTTPURLToken(run)
		if !ok {
			out.WriteString("[REDACTED-URL]")
			line = rest[len(trimURLTail(run)):]
			continue
		}

		// A control URL can carry basic-auth credentials. There is no
		// opt-in for those, so parsed userinfo goes before anything is
		// classified or emitted. The original slice is emitted when there
		// was none, so an opted-in enrollment URL reaches the operator
		// byte-for-byte as the dependency wrote it rather than re-encoded
		// by net/url.
		safeURL := token
		if u.User != nil {
			u.User = nil
			safeURL = u.String()
		}
		switch {
		case isEnrollmentURL(u) && !optedIn:
			out.WriteString(enrollmentRequiredMessage)
		case isEnrollmentURL(u):
			out.WriteString(redactEnrollmentURL(safeURL))
		case optedIn:
			out.WriteString(redactFabricLog(safeURL))
		default:
			out.WriteString("[REDACTED-URL]")
		}
		line = rest[len(token):]
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
