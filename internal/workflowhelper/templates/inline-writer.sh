# --------------------------------------------------------------------------
# Inline run-mailbox writer (fallback)
#
# Used only when the `wefty` binary is not on PATH -- an OCI image that does
# not ship it, for example. It writes byte-identical event files to the ones
# `wefty run ...` writes; wefty's own
# TestInlineBashWriterProducesByteIdenticalEvents holds that.
#
# Protocol: docs/contracts/run-execution-context.md, "Run mailbox".
# --------------------------------------------------------------------------

# Milliseconds since the epoch. A sweep publishes in lexical order, so the file
# name is the only ordering signal a producer has; without a sub-second stamp,
# two events written back to back would be published in alphabetical order of
# their kind. A shell without EPOCHREALTIME degrades to second resolution.
wefty_millis() {
	if [ -n "${EPOCHREALTIME:-}" ]; then
		printf '%s%.3s' "${EPOCHREALTIME%%[.,]*}" "${EPOCHREALTIME#*[.,]}000"
	else
		printf '%s000' "$(date -u +%s)"
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
	wefty_file=$(printf '%013d-%04d-%s-%.32s-%08x' \
		"$(wefty_millis)" "$WEFTY_EVENT_SEQ" "$1" "${wefty_slug:-event}" "$$")
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

# Read one run parameter without jq: params.json is delivered by the agent, and
# a flat string field is what a shell workflow should ask for.
wefty_param() {
	[ -f "${WEFTY_RUN_DIR:-}/params.json" ] || return 0
	tr -d '\n' <"$WEFTY_RUN_DIR/params.json" |
		sed -n 's/.*"'"$1"'"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p'
}
