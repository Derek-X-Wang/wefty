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
// names Tailscale: the fabric seam ends at this package.
const enrollmentRequiredMessage = "Fabric enrollment required; open the URL printed by the device's Tailscale client"

// printEnrollmentURLEnv opts an operator into seeing the raw enrollment URL
// the pinned tsnet dependency would otherwise have printed unredacted. Off
// by default, so nothing prints it unless an operator asks: most wefty
// invocations (including every scripted one) have no human present to read
// stderr, and the URL carries a one-time interactive login token.
const printEnrollmentURLEnv = "WEFTY_FABRIC_PRINT_ENROLLMENT_URL"

// debugLogEnv enables the pinned tsnet dependency's verbose backend log
// (LocalBackend, MagicSock, and friends). Off by default: it exists to
// debug the embedded node itself, not for day-to-day operation, and wefty
// has no `--verbose`/`--debug` flag of its own yet for this to key off of.
const debugLogEnv = "WEFTY_FABRIC_DEBUG_LOG"

var (
	// tskey-... is the pinned tailscale.com dependency's auth key shape
	// (e.g. tskey-auth-<stable id>-<secret>). It never belongs in a log.
	authKeyPattern = regexp.MustCompile(`tskey-[A-Za-z0-9-]+`)
	// authkey=... / key=... are the query parameters an enrollment or
	// coordination URL carries the same secret in.
	authKeyQueryPattern = regexp.MustCompile(`(?i)\b(authkey|key)=[^&\s"'<>]+`)
	// The interactive enrollment path (login.<control-host>/a/<token>)
	// carries a one-time login token in its own right.
	enrollmentPathPattern = regexp.MustCompile(`(?i)\blogin\.[a-zA-Z0-9.-]+/a/[A-Za-z0-9_-]+`)
	// MagicDNS-style tailnet hostnames identify a specific tailnet and are
	// kept behind the fabric seam like every other tailscale-specific name.
	magicDNSPattern = regexp.MustCompile(`\b[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)*\.ts\.net\b`)
	// Any http(s) URL in a tsnet status line is, in practice, the
	// interactive enrollment prompt -- nothing else it logs carries one.
	urlPattern = regexp.MustCompile(`https?://\S+`)
)

// redactFabricLog strips anything that looks like a tailscale auth key, an
// enrollment URL's query parameters or login-path token, or a MagicDNS-style
// tailnet hostname out of a line the pinned tsnet dependency produced. It is
// applied to every line this package forwards, whether or not that line is
// recognised as the enrollment prompt, as defense in depth against a future
// tsnet release folding a credential into some other status line.
func redactFabricLog(line string) string {
	line = authKeyQueryPattern.ReplaceAllString(line, "$1=[REDACTED]")
	line = enrollmentPathPattern.ReplaceAllString(line, "login.[REDACTED]/a/[REDACTED]")
	line = authKeyPattern.ReplaceAllString(line, "[REDACTED]")
	line = magicDNSPattern.ReplaceAllString(line, "[REDACTED]")
	return line
}

func fabricPrintEnrollmentURLEnabled() bool {
	return os.Getenv(printEnrollmentURLEnv) == "1"
}

func fabricDebugLogEnabled() bool {
	return os.Getenv(debugLogEnv) == "1"
}

// wrapUserLogf builds the wefty-owned function tsnet.Server.UserLogf is
// always set to. sink receives the final line after redaction; a nil sink
// falls back to log.Printf so the field is still useful when a caller
// passes a zero Config.
func wrapUserLogf(sink func(string, ...any)) func(string, ...any) {
	if sink == nil {
		sink = log.Printf
	}
	return func(format string, args ...any) {
		line := fmt.Sprintf(format, args...)
		if urlPattern.MatchString(line) {
			if fabricPrintEnrollmentURLEnabled() {
				sink("%s", redactFabricLog(line))
				return
			}
			sink("%s", enrollmentRequiredMessage)
			return
		}
		sink("%s", redactFabricLog(line))
	}
}

// wrapBackendLogf builds the wefty-owned function tsnet.Server.Logf is
// always set to. The pinned dependency documents this field as verbose and
// intended for debugging; it stays silent unless WEFTY_FABRIC_DEBUG_LOG=1 is
// set, and even then is redacted the same way the user-facing log is.
func wrapBackendLogf(sink func(string, ...any)) func(string, ...any) {
	if sink == nil {
		sink = log.Printf
	}
	return func(format string, args ...any) {
		if !fabricDebugLogEnabled() {
			return
		}
		sink("%s", redactFabricLog(fmt.Sprintf(format, args...)))
	}
}
