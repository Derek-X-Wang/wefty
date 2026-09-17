# --------------------------------------------------------------------------
# Inline run-mailbox writer (fallback)
#
# A parser-compatible convenience for an image that does not ship the wefty
# binary. It is NOT hardened and NOT durable: it writes through pathnames, so
# a hostile co-tenant that can plant a symlink in this directory can redirect
# it, it inherits the ambient umask, and it does not flush, so a crash can lose
# an event whose name already exists. `wefty run ...` does all three properly.
# Use it wherever the binary exists; this is the fallback for where it does not.
#
# Today that case is not reachable in production: an OCI attempt receives no
# WEFTY_RUN_DIR at all, because the handoff volume is helper-owned and the
# helper protocol exposes no read path. Until that lands, this is the process
# one-shot's fallback and the proof that the protocol, not the binary, is the
# contract.
#
# Protocol: docs/contracts/run-execution-context.md, "Run mailbox".
# --------------------------------------------------------------------------

# Nanoseconds since the epoch, as a 19-digit number. A sweep publishes in
# lexical order, so the file name is the only ordering signal a producer has;
# without a sub-second stamp, two events written back to back would be
# published in alphabetical order of their kind. The width must match what
# `wefty run` writes, or a workflow that uses both would sort every fallback
# event ahead of every helper event.
#
# bash 5 carries microseconds in EPOCHREALTIME, so the last three digits are
# always zero; a shell without it (bash 4, dash) degrades to seconds and relies
# on the per-process sequence below for ordering.
wefty_nanos() {
	if [ -n "${EPOCHREALTIME:-}" ]; then
		printf '%019d' "$(printf '%s%.6s000' "${EPOCHREALTIME%%[.,]*}" "${EPOCHREALTIME#*[.,]}000000")"
	else
		printf '%019d' "$(date -u +%s)000000000"
	fi
}

# wefty_header_value VALUE LIMIT -- one header line's worth of VALUE: fold what
# a header cannot carry, drop the rest, and truncate to LIMIT bytes.
#
# Truncating rather than refusing is the fallback's choice, and it is the safe
# direction: an over-long summary that pushed the "--" separator past the 64 KiB
# event bound would be truncated by the agent mid-header and then refused
# outright, costing the whole event. `wefty run` refuses instead, because it can
# tell the author which flag was too long.
wefty_header_value() {
	printf '%s' "${1:-}" | LC_ALL=C tr '\n\r\t' '   ' | LC_ALL=C tr -d '\000-\037\177' |
		LC_ALL=C cut -b "1-${2:-255}" |
		sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

# wefty_event KIND NAME STEP STATUS OUTCOME SUMMARY PAYLOAD_FILE KEY
# Every argument after KIND may be empty. The rename into events/ is the only
# "done writing" signal the protocol has, so nothing partial is ever visible.
#
# The payload is always declared `text`. This shell has no JSON parser, and the
# agent refuses an event whose payload claims `json` and then fails to decode
# -- so a structural guess here would trade a readable text payload for a lost
# event. `wefty run` declares `json` because it can actually validate one.
wefty_event() {
	[ -n "${WEFTY_RUN_DIR:-}" ] || {
		printf 'no WEFTY_RUN_DIR: this job has no run mailbox to report through\n' >&2
		return 1
	}
	mkdir -p "$WEFTY_RUN_DIR/tmp" "$WEFTY_RUN_DIR/events" || return 1
	WEFTY_EVENT_SEQ=$(((${WEFTY_EVENT_SEQ:-0} + 1) % 10000))
	wefty_slug=$(printf '%s' "${2:-${3:-$1}}" | LC_ALL=C tr -c 'A-Za-z0-9._-' '-')
	wefty_file=$(printf '%s-%04d-%s-%.32s-%08x' \
		"$(wefty_nanos)" "$WEFTY_EVENT_SEQ" "$1" "${wefty_slug:-event}" "$$")
	{
		printf 'wefty-protocol: 1\nkind: %s\n' "$1"
		for wefty_header in name step status outcome summary key; do
			case $wefty_header in
			# 255 is the v1 schema's identifier bound and 2048 the agent's
			# summary bound; past either the agent rewrites the value, and a
			# rewritten key is a different idempotency identity.
			name) wefty_raw=${2:-} wefty_limit=255 ;;
			step) wefty_raw=${3:-} wefty_limit=255 ;;
			status) wefty_raw=${4:-} wefty_limit=32 ;;
			outcome) wefty_raw=${5:-} wefty_limit=32 ;;
			summary) wefty_raw=${6:-} wefty_limit=2048 ;;
			key) wefty_raw=${8:-} wefty_limit=255 ;;
			esac
			wefty_value=$(wefty_header_value "$wefty_raw" "$wefty_limit")
			[ -z "$wefty_value" ] || printf '%s: %s\n' "$wefty_header" "$wefty_value"
		done
		printf 'payload: text\ncreated-at: %s\n--\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
		[ -z "${7:-}" ] || cat "$7"
	} >"$WEFTY_RUN_DIR/tmp/$wefty_file" || return 1
	mv "$WEFTY_RUN_DIR/tmp/$wefty_file" "$WEFTY_RUN_DIR/events/$wefty_file"
}

# Read one run parameter without jq. Deliberately limited, and the limits are
# the reason to prefer `wefty run params NAME` whenever the binary is present:
# this reads TOP-LEVEL STRING params only, it does not decode JSON escapes (a
# value containing \" or \\ reads as empty rather than as a truncated value,
# so a workflow can detect it), and a key that also appears in a nested object
# is not disambiguated. Anything richer needs the helper or jq.
wefty_param() {
	[ -f "${WEFTY_RUN_DIR:-}/params.json" ] || return 0
	tr -d '\n' <"$WEFTY_RUN_DIR/params.json" |
		sed -n 's/.*"'"$1"'"[[:space:]]*:[[:space:]]*"\([^"\\]*\)".*/\1/p'
}

# JSON-escape one string value for a document this script assembles by hand.
# It escapes the two characters that can break out of a JSON string and drops
# the control bytes JSON cannot carry raw.
wefty_json_escape() {
	printf '%s' "${1:-}" | LC_ALL=C tr -d '\000-\010\013-\037\177' | LC_ALL=C tr '\011\012' '  ' |
		sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}
