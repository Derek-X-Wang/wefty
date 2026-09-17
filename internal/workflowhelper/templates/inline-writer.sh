# --------------------------------------------------------------------------
# Inline run-mailbox writer (fallback)
#
# Used only when the `wefty` binary is not on PATH. It writes byte-identical
# event files to the ones `wefty run ...` writes; wefty's own
# TestInlineBashWriterProducesByteIdenticalEvents holds that.
#
# It exists for an image that does not ship the wefty binary. Today that case
# is not reachable: an OCI attempt receives no WEFTY_RUN_DIR at all, because
# the handoff volume is helper-owned and the helper protocol exposes no read
# path (#476 slice C). Until then this is the process one-shot's fallback and
# the proof that the protocol, not the binary, is the contract.
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

# A header value is one line: fold what a header cannot carry, drop the rest.
wefty_header_value() {
	printf '%s' "${1:-}" | LC_ALL=C tr '\n\r\t' '   ' | LC_ALL=C tr -d '\000-\037\177' |
		sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

# wefty_event KIND NAME STEP STATUS OUTCOME SUMMARY FORMAT PAYLOAD_FILE KEY
# Every argument after KIND may be empty. The rename into events/ is the only
# "done writing" signal the protocol has, so nothing partial is ever visible.
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
		for wefty_pair in "name:${2:-}" "step:${3:-}" "status:${4:-}" \
			"outcome:${5:-}" "summary:${6:-}" "key:${9:-}"; do
			wefty_value=$(wefty_header_value "${wefty_pair#*:}")
			[ -z "$wefty_value" ] || printf '%s: %s\n' "${wefty_pair%%:*}" "$wefty_value"
		done
		printf 'payload: %s\ncreated-at: %s\n--\n' "${7:-text}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
		[ -z "${8:-}" ] || cat "$8"
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
