#!/usr/bin/env bash
#
# issue-to-pr: read a GitHub issue, let a coding agent implement it, gate the
# result, and hand back a draft pull request.
#
# Contract: docs/contracts/run-execution-context.md. The workflow reads
# WEFTY_RUN_ID, WEFTY_RUN_DIR and WEFTY_HANDOFF_DIR, clones the repository into
# a scratch directory, works on a job-owned branch, runs the repository's own
# gates, pushes, opens a draft PR, and writes pr.json and summary.md into the
# handoff directory.
#
# It reports through the run mailbox, not over HTTP. `wefty run step|envelope|
# gate|result` writes event files into WEFTY_RUN_DIR and the node agent
# publishes them with the credential it already holds, so this job holds none of
# its own. Where `wefty` is not on PATH the embedded inline writer below
# produces the same events.
#
# Input arrives as run params, read from the mailbox with `wefty run params`:
#
#   issue            required   the issue number to implement
#   repo             optional   owner/name (default: Derek-X-Wang/wefty)
#   agent            optional   claude | codex (default: claude)
#   continue_from    optional   a pushed branch from an earlier run; the work
#                               resumes on it at the first phase whose marker
#                               commit is absent
#   budget_minutes   optional   wall clock for the agent phases (default: 60)
#   max_turns        optional   the agent's own turn cap (default: 40)
#
# ISSUE_TO_PR_ISSUE / _REPO / _AGENT / _CONTINUE_FROM / _BUDGET_MINUTES /
# _MAX_TURNS override the params for a local dry run.
#
# Phases and resumption
#
# The work is six phases -- read-issue, plan, implement, gates, push, open-pr --
# and each one ends by recording an empty marker commit and pushing it:
#
#   issue-to-pr: phase <name> complete
#
# The marker is pushed, not merely committed, because a resumed run is a cold
# `wefty rerun`: it starts from an empty scratch directory and clones the
# branch, so the only phases it can know about are the ones the remote can tell
# it. With `continue_from` set, every phase whose marker is already on that
# branch is skipped and the run resumes at the first one that is not. This is
# the whole of the resume design: no state file, no lock, nothing the run has to
# be trusted to have written correctly -- just commits on a branch that either
# are or are not there.
#
# Trust boundary: this job is a `kind=process` one-shot on a node the user owns,
# and it runs as that user. It deliberately has no isolation from them -- it
# uses their `gh` login and their agent CLI login, because those are the point.
# It holds no wefty credential: reporting goes through the mailbox, so this
# shell cannot write another run or submit a child job. What it can do is
# whatever that user's `gh` and agent logins can do, which is why this workflow
# belongs on a personal node and nowhere else.
#
# Secrets: nothing from the environment is written into the handoff directory.
# The documents this workflow publishes are built from the issue, the plan the
# agent wrote, and the gate results. `gh` reads its own credential from the
# user's keychain or environment and this script never reads, prints or copies
# it.
#
# Exit codes carry the verdict. A finished run's handoff directory is retained
# on every outcome, so exiting non-zero is the verdict itself and not a way to
# keep evidence.

# The embedded inline writer below is a verbatim copy, so its functions cannot
# carry their own directives, and this workflow reaches them indirectly --
# through `report`, which takes the producer as arguments. SC2329 (and SC2317,
# which shellcheck 0.9.0 in CI raises for the same reason) is therefore
# silenced for the file rather than for a block a copy is not allowed to have.
# shellcheck disable=SC2317,SC2329
set -u

WORKFLOW=issue-to-pr
DEFAULT_REPO=Derek-X-Wang/wefty
DEFAULT_AGENT=claude
DEFAULT_BUDGET_MINUTES=60
DEFAULT_MAX_TURNS=40
KNOWN_AGENTS="claude codex"
GATE_NAMES="gofmt vet test"
FAILURE_BYTE_LIMIT=524288

# Fields the failure path needs before they are known.
RESULT_FILE=
ISSUE=
REPO=
AGENT=
CONTINUE_FROM=
BUDGET_MINUTES=
MAX_TURNS=
BRANCH=
HEAD_SHA=
PR_URL=
STARTED_AT=
SKIPPED_PHASES=
RAN_PHASES=

log() { printf '%s: %s\n' "$WORKFLOW" "$*"; }

# abort is only for failures before the execution context is known, where there
# is no handoff directory to write into and no run to report against.
abort() {
	printf '%s: %s\n' "$WORKFLOW" "$*" >&2
	exit 1
}

# --------------------------------------------------------------------------
# Execution context
# --------------------------------------------------------------------------

require_context() {
	eval "value=\${$1:-}"
	[ -n "$value" ] || abort "$1 is required (see docs/contracts/run-execution-context.md)"
	printf '%s' "$value"
}

RUN_ID=$(require_context WEFTY_RUN_ID) || exit 1
require_context WEFTY_RUN_DIR >/dev/null || exit 1
HANDOFF_DIR=$(require_context WEFTY_HANDOFF_DIR) || exit 1
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# The boundary this workflow claims, stated in its own log on every run. A
# reporting run holds no credential, so this list is empty; if it ever is not,
# the run was submitted with dispatch authority it does not need.
visible_credentials=
for name in WEFTY_RUN_TOKEN WEFTY_ATTEMPT_TOKEN WEFTY_COMPUTER_TOKEN; do
	eval "value=\${$name:-}"
	[ -z "$value" ] || visible_credentials="$visible_credentials $name"
done
log "credentials in the workflow environment: [${visible_credentials# }]"

mkdir -p "$HANDOFF_DIR" || abort "cannot create handoff directory $HANDOFF_DIR"

# One private scratch directory holds the clone, the worktree, the prompts and
# every log. Short and not under $TMPDIR for the same reason branch-gates gives:
# a subject's tests create unix sockets under the TMPDIR handed to them, and a
# socket path is capped at 104 bytes on macOS.
WORK_ROOT=${ISSUE_TO_PR_WORK_ROOT:-/tmp}
WORK_ROOT=$(cd "$WORK_ROOT" 2>/dev/null && pwd -P) ||
	abort "work root ${ISSUE_TO_PR_WORK_ROOT:-/tmp} is not a usable directory"
WORK_DIR=$WORK_ROOT/wefty-i2p-$(printf '%.12s' "${RUN_ID##*_}")
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR" || abort "cannot create $WORK_DIR"
chmod 0700 "$WORK_DIR" 2>/dev/null || true
CLONE_DIR=$WORK_DIR/clone
TREE_DIR=$WORK_DIR/tree
RESULT_FILE=$WORK_DIR/result.json
ISSUE_FILE=$WORK_DIR/issue.md

# FINALIZED is set once a verdict -- success or workflow error -- has been
# written. Anything that leaves this script without setting it is an exit nobody
# planned for, and the finalizer below turns it into the same documents every
# other outcome produces.
FINALIZED=

# shellcheck disable=SC2317,SC2329 # invoked indirectly via the EXIT trap below.
finalize_unplanned_exit() {
	exit_status=$1
	[ -z "$FINALIZED" ] || return 0
	[ "$exit_status" -ne 0 ] || return 0
	FINALIZED=1
	message="the workflow exited $exit_status without recording a verdict"
	printf '%s: %s\n' "$WORKFLOW" "$message" >&2
	# Best effort, and it says so: this runs on the way out of the shell, so it
	# cannot promise anything after a SIGKILL, a power loss, or a node that
	# stopped. What it does cover is the ordinary unplanned exit -- an unset
	# variable, a tool that vanished, a bug in this script.
	write_result_document false unknown "$message" 2>/dev/null || true
	cp "$RESULT_FILE" "$HANDOFF_DIR/result.json" 2>/dev/null || true
	chmod 0600 "$HANDOFF_DIR/result.json" 2>/dev/null || true
	{
		printf '===== workflow-error: unknown =====\n'
		printf '%s\n' "$message"
	} >>"$HANDOFF_DIR/failures.txt" 2>/dev/null || true
	chmod 0600 "$HANDOFF_DIR/failures.txt" 2>/dev/null || true
	publish_result failed "$message" 2>/dev/null || true
}

# shellcheck disable=SC2317,SC2329 # invoked indirectly via the EXIT trap below.
cleanup() {
	status=$?
	# The verdict is written before the scratch directory is swept, because the
	# result document is assembled in it.
	finalize_unplanned_exit "$status"
	if [ -d "$WORK_DIR" ]; then
		chmod -R u+rwX "$WORK_DIR" 2>/dev/null || true
		rm -rf "$WORK_DIR" || true
		if [ -d "$WORK_DIR" ]; then
			printf '%s: WARNING: scratch directory %s survived cleanup\n' "$WORKFLOW" "$WORK_DIR"
		fi
	fi
	return "$status"
}
trap cleanup EXIT

# --------------------------------------------------------------------------
# JSON by hand, for the documents this workflow owns
# --------------------------------------------------------------------------

json_escape() {
	printf '%s' "$1" |
		LC_ALL=C tr -d '\000-\010\013-\037\177' |
		LC_ALL=C tr '\011' ' ' |
		sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' |
		awk 'BEGIN { ORS = "" } NR > 1 { printf "\\n" } { print }'
}

timestamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }


# --------------------------------------------------------------------------
# Reporting
#
# Every protocol document is written into the run mailbox for the node agent to
# publish. Two producers write the same file protocol: `wefty run`, preferred
# wherever the binary exists, and the inline writer below for an image that does
# not ship it. The inline writer is a verbatim copy of the one
# `wefty workflow init` scaffolds; a test asserts the two stay byte-identical.
#
# These return non-zero instead of exiting, so the caller decides whether a
# failed write is fatal (normal path) or best effort (failure path).
# --------------------------------------------------------------------------

# >>> BEGIN embedded inline run-mailbox writer >>>
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
# The OCI case is exactly where it earns its keep: an OCI attempt's mailbox lives
# in its helper-owned handoff volume, and the image that runs there is usually
# not one that ships the wefty binary. The caveats above still apply, and they
# apply more there, because that directory is inside a container.
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
# <<< END embedded inline run-mailbox writer <<<
if command -v wefty >/dev/null 2>&1; then
	REPORTER=wefty
else
	REPORTER=inline
	log "wefty is not on PATH; reporting with the inline mailbox writer"
fi

REPORT_ERROR=

# report runs one reporting action and captures its diagnosis. The producer's
# own stderr is what explains a refusal, so it is kept rather than discarded.
report() {
	report_log=$WORK_DIR/report.log
	if "$@" >"$report_log" 2>&1; then
		return 0
	fi
	REPORT_ERROR="reporting $2 failed: $(tr '\n' ' ' <"$report_log")"
	return 1
}

# Its locals are prefixed because this is POSIX sh: a function assigns globals,
# and a caller may be mid-loop with its own `name`, `status` or `summary`.
append_step() {
	step_name=$1
	step_state=$2
	step_summary=$3
	if [ "$REPORTER" = wefty ]; then
		if [ "$step_state" = ended ]; then
			report wefty run step --name "$step_name" --end --summary "$step_summary"
			return
		fi
		report wefty run step --name "$step_name" --summary "$step_summary"
		return
	fi
	report wefty_event step "$step_name" "" "$step_state" "" "$step_summary" ""
}

append_envelope() {
	env_step=$1
	env_status=$2
	env_summary=$3
	env_payload=$4
	if [ "$REPORTER" = wefty ]; then
		if [ -n "$env_payload" ]; then
			report wefty run envelope --step "$env_step" --status "$env_status" \
				--summary "$env_summary" --payload-json-file "$env_payload"
			return
		fi
		report wefty run envelope --step "$env_step" --status "$env_status" --summary "$env_summary"
		return
	fi
	# The inline writer declares every payload `text`: it has no JSON parser,
	# and an event whose payload claims `json` and fails to decode is refused.
	report wefty_event envelope "" "$env_step" "$env_status" "" "$env_summary" "$env_payload"
}

append_gate() {
	gate_name=$1
	gate_outcome=$2
	gate_evidence=$3
	if [ "$REPORTER" = wefty ]; then
		if [ -n "$gate_evidence" ]; then
			report wefty run gate --name "$gate_name" --outcome "$gate_outcome" --evidence-file "$gate_evidence"
			return
		fi
		report wefty run gate --name "$gate_name" --outcome "$gate_outcome"
		return
	fi
	report wefty_event gate "$gate_name" "" "" "$gate_outcome" "" "$gate_evidence"
}

# publish_result records the run's final document both ways: into the ledger as
# the run's result event, and into the handoff directory as result.json. The
# handoff copy is written here rather than left to `wefty run result`, which
# also writes it, so an operator reading the node does not depend on reporting
# having succeeded -- the failure path reports last and may not report at all.
publish_result() {
	result_status=$1
	result_summary=$2
	if ! cp "$RESULT_FILE" "$HANDOFF_DIR/result.json" 2>/dev/null; then
		log "WARNING: could not write $HANDOFF_DIR/result.json"
	fi
	chmod 0600 "$HANDOFF_DIR/result.json" 2>/dev/null || true
	if [ "$REPORTER" = wefty ]; then
		report wefty run result --file "$RESULT_FILE" --status "$result_status" --summary "$result_summary"
		return
	fi
	report wefty_event result result result "$result_status" "" "$result_summary" "$RESULT_FILE"
}

# write_result_document assembles this workflow's own schema. It is the one
# document a reader gets with `wefty results RUN_ID`.
write_result_document() {
	doc_passed=$1
	doc_error_step=$2
	doc_error_message=$3
	error_json=null
	if [ -n "$doc_error_step" ]; then
		error_json=$(printf '{"step":"%s","message":"%s"}' \
			"$(json_escape "$doc_error_step")" "$(json_escape "$doc_error_message")")
	fi
	printf '{"schema_version":1,"workflow":"%s","run_id":"%s","repo":"%s","issue":"%s","agent":"%s","branch":"%s","head_sha":"%s","pr_url":"%s","passed":%s,"phases_ran":"%s","phases_skipped":"%s","workflow_error":%s,"started_at":"%s","finished_at":"%s"}\n' \
		"$WORKFLOW" \
		"$(json_escape "$RUN_ID")" \
		"$(json_escape "$REPO")" \
		"$(json_escape "$ISSUE")" \
		"$(json_escape "$AGENT")" \
		"$(json_escape "$BRANCH")" \
		"$(json_escape "$HEAD_SHA")" \
		"$(json_escape "$PR_URL")" \
		"$doc_passed" \
		"$(json_escape "${RAN_PHASES# }")" \
		"$(json_escape "${SKIPPED_PHASES# }")" \
		"$error_json" \
		"$STARTED_AT" \
		"$(timestamp)" >"$RESULT_FILE" 2>/dev/null ||
		log "WARNING: could not write $RESULT_FILE"
}

# --------------------------------------------------------------------------
# The single failure path
#
# Everything that goes wrong after the execution context is known leaves the
# same documents, then publishes a failed envelope and an `error` gate on a
# best-effort basis before exiting non-zero. An `error` gate means the workflow
# could not do its job; a `fail` gate means the work is not acceptable.
# --------------------------------------------------------------------------

fail_workflow() {
	fail_step=$1
	fail_message=$2
	fail_detail=${3:-}
	FINALIZED=1
	log "WORKFLOW ERROR at $fail_step: $fail_message"

	write_result_document false "$fail_step" "$fail_message"
	{
		printf '===== workflow-error: %s =====\n' "$fail_step"
		printf '%s\n' "$fail_message"
		[ -z "$fail_detail" ] || printf '%s\n' "$fail_detail"
	} >"$HANDOFF_DIR/failures.txt" 2>/dev/null ||
		log "WARNING: could not write $HANDOFF_DIR/failures.txt"
	chmod 0600 "$HANDOFF_DIR/failures.txt" 2>/dev/null || true

	append_envelope "$fail_step" failed "$fail_message" "" || log "WARNING: $REPORT_ERROR"
	publish_result failed "$fail_message" || log "WARNING: $REPORT_ERROR"
	evidence_file=$WORK_DIR/error-evidence.txt
	printf 'step: %s\nerror: %s\n' "$fail_step" "$fail_message" >"$evidence_file" 2>/dev/null || evidence_file=
	append_gate "$WORKFLOW" error "$evidence_file" || log "WARNING: $REPORT_ERROR"
	log "done: workflow-error at $fail_step"
	exit 1
}

for tool in git gh; do
	command -v "$tool" >/dev/null 2>&1 ||
		fail_workflow environment "$tool is required in the job rootfs"
done
# The wall clock is not optional. An agent phase without one can run until the
# job's own max-runtime kills it, which is a much blunter instrument and leaves
# no verdict. macOS installs coreutils under a g- prefix, and gtimeout is the
# same program, so either satisfies this.
TIMEOUT_CMD=
for candidate in timeout gtimeout; do
	if command -v "$candidate" >/dev/null 2>&1; then
		TIMEOUT_CMD=$candidate
		break
	fi
done
[ -n "$TIMEOUT_CMD" ] ||
	fail_workflow environment "timeout (GNU coreutils; gtimeout on macOS) is required to bound the agent"

# --------------------------------------------------------------------------
# Input
# --------------------------------------------------------------------------

param() {
	if [ "$REPORTER" = wefty ]; then
		wefty run params "$1" 2>/dev/null || true
		return
	fi
	wefty_param "$1" 2>/dev/null || true
}

ISSUE=${ISSUE_TO_PR_ISSUE:-$(param issue)}
REPO=${ISSUE_TO_PR_REPO:-$(param repo)}
AGENT=${ISSUE_TO_PR_AGENT:-$(param agent)}
CONTINUE_FROM=${ISSUE_TO_PR_CONTINUE_FROM:-$(param continue_from)}
BUDGET_MINUTES=${ISSUE_TO_PR_BUDGET_MINUTES:-$(param budget_minutes)}
MAX_TURNS=${ISSUE_TO_PR_MAX_TURNS:-$(param max_turns)}
# Whether the submitter asked for a cap, as opposed to taking the default. The
# two are different: a default this workflow cannot apply is not an error, a
# request it cannot honour is.
MAX_TURNS_REQUESTED=$MAX_TURNS

[ -n "$REPO" ] || REPO=$DEFAULT_REPO
[ -n "$AGENT" ] || AGENT=$DEFAULT_AGENT
[ -n "$BUDGET_MINUTES" ] || BUDGET_MINUTES=$DEFAULT_BUDGET_MINUTES
[ -n "$MAX_TURNS" ] || MAX_TURNS=$DEFAULT_MAX_TURNS

# The numbers are read as bounded decimal integers. A leading zero is refused
# rather than normalised: "07" and "7" would be the same issue but different
# branch names, and a branch name is what a resumed run matches on.
case $ISSUE in
'') fail_workflow input "params.issue is required: submit with --param issue=<number>" ;;
*[!0-9]*) fail_workflow input "params.issue must be a decimal number, got \"$ISSUE\"" ;;
0*) fail_workflow input "params.issue must not carry a leading zero, got \"$ISSUE\"" ;;
esac
[ "${#ISSUE}" -le 9 ] || fail_workflow input "params.issue is implausibly long: \"$ISSUE\""
case $REPO in
*/*) : ;;
*) fail_workflow input "params.repo must be owner/name, got \"$REPO\"" ;;
esac
case " $KNOWN_AGENTS " in
*" $AGENT "*) : ;;
*) fail_workflow input "params.agent must be one of $KNOWN_AGENTS, got \"$AGENT\"" ;;
esac
case $BUDGET_MINUTES in
'' | *[!0-9]*) fail_workflow input "params.budget_minutes must be a decimal number, got \"$BUDGET_MINUTES\"" ;;
0*) fail_workflow input "params.budget_minutes must not carry a leading zero, got \"$BUDGET_MINUTES\"" ;;
esac
[ "${#BUDGET_MINUTES}" -le 4 ] ||
	fail_workflow input "params.budget_minutes is implausibly large: \"$BUDGET_MINUTES\""
[ "$BUDGET_MINUTES" -gt 0 ] 2>/dev/null ||
	fail_workflow input "params.budget_minutes must be positive, got \"$BUDGET_MINUTES\""
case $MAX_TURNS in
'' | *[!0-9]*) fail_workflow input "params.max_turns must be a decimal number, got \"$MAX_TURNS\"" ;;
0*) fail_workflow input "params.max_turns must not carry a leading zero, got \"$MAX_TURNS\"" ;;
esac
[ "${#MAX_TURNS}" -le 4 ] || fail_workflow input "params.max_turns is implausibly large: \"$MAX_TURNS\""
[ "$MAX_TURNS" -gt 0 ] 2>/dev/null ||
	fail_workflow input "params.max_turns must be positive, got \"$MAX_TURNS\""
# A turn cap this workflow cannot actually apply is refused rather than
# silently ignored: `codex exec` has no equivalent of claude's --max-turns, so
# accepting the parameter would promise a bound that does not exist.
if [ -n "$MAX_TURNS_REQUESTED" ] && [ "$AGENT" = codex ]; then
	fail_workflow input "params.max_turns cannot be honoured with agent=codex: codex exec has no turn cap; use the budget_minutes wall clock instead"
fi
# A branch name reaches git and gh as an argument, so it is bounded to what a
# branch may safely be. It is then bounded much further: only a branch this
# workflow itself created, for this issue. Resuming onto an arbitrary branch
# would let a run push its commits and open a pull request against whatever the
# submitter named -- the repository's default branch included -- which is not
# resumption, it is a different operation wearing its name.
case $CONTINUE_FROM in
'') : ;;
*[!A-Za-z0-9._/-]* | -* | */ | *..*)
	fail_workflow input "params.continue_from is not a usable branch name: \"$CONTINUE_FROM\""
	;;
esac
if [ -n "$CONTINUE_FROM" ]; then
	case $CONTINUE_FROM in
	"issue-to-pr/$ISSUE-"?*) : ;;
	*)
		fail_workflow input "params.continue_from must be a branch this workflow created for issue $ISSUE (issue-to-pr/$ISSUE-<suffix>), got \"$CONTINUE_FROM\""
		;;
	esac
	case ${CONTINUE_FROM#"issue-to-pr/$ISSUE-"} in
	*[!A-Za-z0-9._-]*)
		fail_workflow input "params.continue_from has a suffix this workflow does not create: \"$CONTINUE_FROM\""
		;;
	esac
fi

BUDGET_SECONDS=$((BUDGET_MINUTES * 60))
DEADLINE=$(($(date -u +%s) + BUDGET_SECONDS))

# --------------------------------------------------------------------------
# Phases
#
# Each phase is a step bracket in the ledger and a marker commit on the branch.
# The bracket is what `wefty runs list` and `wefty inspect` read; the commit is
# what a resumed run reads, because a resumed run is a cold rerun that has only
# the remote to learn from.
# --------------------------------------------------------------------------

marker_subject() { printf '%s: phase %s complete' "$WORKFLOW" "$1"; }

# SKIPPABLE_PHASES is deliberately short. Reading the issue, planning and
# implementing are expensive, produce a durable artefact on the branch, and are
# the only phases worth not paying for twice. The gates, the push and the pull
# request are cheap and are the run's own verification, so they run on every
# start: a resumed run that skipped its gates would be trusting whatever is on
# the branch to have been gated by something else.
SKIPPABLE_PHASES="read-issue plan implement"

# phase_done reports whether this phase's marker is on the branch as origin had
# it when this run cloned.
#
# Three things make that answer worth trusting, and none of them is a security
# boundary -- the agent runs on this branch and can write commits -- so they are
# stacked deliberately:
#
#   - The snapshot is the commit origin had at clone time, fixed for the run.
#     Local history evolves as this run works; asking it would let a phase this
#     run just performed answer a question about what an earlier run did.
#   - A marker is a trailer, not a subject line. A subject is prose the agent
#     may write for any reason; a trailer naming this issue and this phase is a
#     structured claim, and it must also carry a run identity and the
#     workflow's own author address.
#   - Only the three phases above can be skipped at all, so forging a marker
#     can at worst re-use work, never bypass the gates.
phase_done() {
	[ -n "$RESUMING" ] || return 1
	case " $SKIPPABLE_PHASES " in
	*" $1 "*) : ;;
	*) return 1 ;;
	esac
	[ -n "$ORIGIN_SNAPSHOT" ] || return 1
	git -C "$TREE_DIR" log \
		--format='%(trailers:key=Issue-To-PR-Marker,valueonly,separator=%x2C)%x09%(trailers:key=Issue-To-PR-Run,valueonly,separator=%x2C)%x09%ae' \
		"$ORIGIN_SNAPSHOT" 2>/dev/null |
		awk -F'\t' -v want="$ISSUE/$1" -v who="$MARKER_EMAIL" \
			'$1 == want && $2 != "" && $3 == who { found = 1 } END { exit found ? 0 : 1 }'
}

# phase_complete records the phase and publishes it. The commit is empty when
# the phase produced no files of its own, which is the point: the marker is a
# fact about the run reaching here, not about what it wrote.
phase_complete() {
	complete_phase=$1
	git -C "$TREE_DIR" add -A >/dev/null 2>&1 || true
	if ! git -C "$TREE_DIR" commit --allow-empty --quiet \
		-m "$(marker_subject "$complete_phase")" \
		-m "$(printf 'Issue-To-PR-Marker: %s/%s\nIssue-To-PR-Run: %s' "$ISSUE" "$complete_phase" "$RUN_ID")" \
		>"$WORK_DIR/commit.log" 2>&1; then
		fail_workflow "$complete_phase" "cannot record the phase marker commit" \
			"$(tail -n 20 "$WORK_DIR/commit.log")"
	fi
	# A plain push, never a force. A branch that has moved under this run is
	# something this run does not understand, and overwriting it would discard
	# whatever moved it -- another run, or a person.
	if ! git -C "$TREE_DIR" push --quiet origin "HEAD:refs/heads/$BRANCH" \
		>"$WORK_DIR/push.log" 2>&1; then
		fail_workflow "$complete_phase" "cannot fast-forward $BRANCH on origin; it moved under this run" \
			"$(tail -n 20 "$WORK_DIR/push.log")"
	fi
	HEAD_SHA=$(git -C "$TREE_DIR" rev-parse HEAD 2>/dev/null) || HEAD_SHA=
	RAN_PHASES="$RAN_PHASES $complete_phase"
	append_step "$complete_phase" ended "phase $complete_phase complete" || log "WARNING: $REPORT_ERROR"
	append_envelope "$complete_phase" succeeded "phase $complete_phase complete on $BRANCH" "" ||
		log "WARNING: $REPORT_ERROR"
}

phase_skipped() {
	SKIPPED_PHASES="$SKIPPED_PHASES $1"
	log "phase $1: already complete on $CONTINUE_FROM, skipping"
	append_envelope "$1" succeeded "phase $1 was already complete on $CONTINUE_FROM" "" ||
		log "WARNING: $REPORT_ERROR"
}

phase_start() {
	log "phase $1: $2"
	append_step "$1" started "$2" || log "WARNING: $REPORT_ERROR"
}

# check_budget fails the run when the wall clock the submitter allowed is spent.
# It is checked between phases and around each agent invocation, because those
# are the only places where stopping is meaningful.
check_budget() {
	now=$(date -u +%s)
	[ "$now" -lt "$DEADLINE" ] ||
		fail_workflow "$1" "the ${BUDGET_MINUTES}-minute budget is spent"
}

# --------------------------------------------------------------------------
# The agent
#
# One bounded, non-interactive invocation per phase that needs one. The command
# is resolved rather than hard-coded so the integration test can substitute a
# fake agent: WEFTY_ISSUE_TO_PR_AGENT_CMD is a TEST SEAM, documented in the
# README, and a real run leaves it unset.
#
# The override is invoked as `CMD <phase> <prompt-file>` with the worktree as
# its working directory, which is the smallest contract a fake can implement.
# --------------------------------------------------------------------------

run_agent() {
	agent_phase=$1
	agent_prompt=$2
	agent_log=$WORK_DIR/agent-$agent_phase.log
	check_budget "$agent_phase"
	remaining=$((DEADLINE - $(date -u +%s)))
	[ "$remaining" -gt 0 ] || fail_workflow "$agent_phase" "the ${BUDGET_MINUTES}-minute budget is spent"

	if [ -n "${WEFTY_ISSUE_TO_PR_AGENT_CMD:-}" ]; then
		# Deliberately unquoted: the seam accepts a command line so a test can
		# pass an interpreter and a script.
		# shellcheck disable=SC2086
		set -- $WEFTY_ISSUE_TO_PR_AGENT_CMD "$agent_phase" "$agent_prompt"
	else
		case $AGENT in
		claude) set -- claude -p "$(cat "$agent_prompt")" --max-turns "$MAX_TURNS" ;;
		codex) set -- codex exec "$(cat "$agent_prompt")" ;;
		*) fail_workflow "$agent_phase" "no invocation is defined for agent \"$AGENT\"" ;;
		esac
	fi

	agent_status=0
	# -k 30s: an agent that ignores the polite signal gets half a minute to
	# finish what it was writing and is then killed. Without the escalation a
	# process that traps SIGTERM defeats the budget entirely.
	(cd "$TREE_DIR" && "$TIMEOUT_CMD" -k 30s "$remaining" "$@") >"$agent_log" 2>&1 || agent_status=$?

	# 124 is the polite timeout; 137 is the escalation actually killing it.
	if [ "$agent_status" -eq 124 ] || [ "$agent_status" -eq 137 ]; then
		fail_workflow "$agent_phase" "the agent exceeded the ${BUDGET_MINUTES}-minute budget" \
			"$(tail -c "$FAILURE_BYTE_LIMIT" "$agent_log" 2>/dev/null)"
	fi
	if [ "$agent_status" -ne 0 ]; then
		fail_workflow "$agent_phase" "the $AGENT agent exited $agent_status" \
			"$(tail -c "$FAILURE_BYTE_LIMIT" "$agent_log" 2>/dev/null)"
	fi
	check_budget "$agent_phase"
}

# --------------------------------------------------------------------------
# Working tree
# --------------------------------------------------------------------------

REMOTE=${ISSUE_TO_PR_REMOTE:-https://github.com/$REPO.git}
BRANCH=${CONTINUE_FROM:-issue-to-pr/$ISSUE-${RUN_ID##*_}}
RESUMING=
# MARKER_EMAIL is the author address this workflow commits its markers under,
# and the one a marker must carry to be believed.
MARKER_EMAIL=issue-to-pr@wefty.invalid
# ORIGIN_SNAPSHOT is what origin had for this branch when this run cloned. Every
# resume decision is made against it rather than against local history, which
# this run is itself changing as it goes.
ORIGIN_SNAPSHOT=

log "run $RUN_ID: issue $ISSUE in $REPO with the $AGENT agent, branch $BRANCH"

clone_log=$WORK_DIR/clone.log
if ! git clone --quiet "$REMOTE" "$CLONE_DIR" >"$clone_log" 2>&1; then
	fail_workflow checkout "cannot clone $REPO" "$(tail -n 20 "$clone_log")"
fi
BASE_BRANCH=$(git -C "$CLONE_DIR" symbolic-ref --quiet --short HEAD 2>/dev/null) || BASE_BRANCH=main
[ -n "$BASE_BRANCH" ] || BASE_BRANCH=main

if [ -n "$CONTINUE_FROM" ]; then
	# The default branch is refused by name as well as by pattern. A repository
	# whose default branch happened to match would otherwise be one push away
	# from this workflow committing to it.
	if [ "$CONTINUE_FROM" = "$BASE_BRANCH" ]; then
		fail_workflow input "params.continue_from is the repository's default branch ($BASE_BRANCH); this workflow never works on it"
	fi
	if ! git -C "$CLONE_DIR" worktree add --quiet "$TREE_DIR" "origin/$CONTINUE_FROM" \
		>"$clone_log" 2>&1; then
		fail_workflow checkout "params.continue_from names a branch origin does not have: $CONTINUE_FROM" \
			"$(tail -n 20 "$clone_log")"
	fi
	ORIGIN_SNAPSHOT=$(git -C "$CLONE_DIR" rev-parse "origin/$CONTINUE_FROM" 2>/dev/null) || ORIGIN_SNAPSHOT=
	[ -n "$ORIGIN_SNAPSHOT" ] ||
		fail_workflow checkout "cannot resolve origin/$CONTINUE_FROM after checking it out"
	RESUMING=1
	log "resuming on $CONTINUE_FROM at $ORIGIN_SNAPSHOT"
else
	if ! git -C "$CLONE_DIR" worktree add --quiet -b "$BRANCH" "$TREE_DIR" "origin/$BASE_BRANCH" \
		>"$clone_log" 2>&1; then
		fail_workflow checkout "cannot create the worktree for $BRANCH" "$(tail -n 20 "$clone_log")"
	fi
fi
# The commits this workflow records are its own, not the user's.
git -C "$TREE_DIR" config user.name "wefty issue-to-pr" >/dev/null 2>&1 || true
git -C "$TREE_DIR" config user.email "issue-to-pr@wefty.invalid" >/dev/null 2>&1 || true
HEAD_SHA=$(git -C "$TREE_DIR" rev-parse HEAD 2>/dev/null) || HEAD_SHA=

# PLAN.md is this run's own scratch, not part of the change: excluding it here
# means every `git add -A` this script runs -- the phase markers and the
# implement-phase safety net alike -- can never sweep it into a commit, so it
# never ships in the pull request and the plan marker commit stays empty, as
# the README promises. The exclude lives in the shared git-common-dir, which
# only this run's own clone uses, so it cannot leak to anything else.
GIT_COMMON_DIR=$(git -C "$TREE_DIR" rev-parse --git-common-dir 2>/dev/null) || GIT_COMMON_DIR=.git
case $GIT_COMMON_DIR in
/*) : ;;
*) GIT_COMMON_DIR=$TREE_DIR/$GIT_COMMON_DIR ;;
esac
mkdir -p "$GIT_COMMON_DIR/info" 2>/dev/null || true
printf '/PLAN.md\n' >>"$GIT_COMMON_DIR/info/exclude" 2>/dev/null ||
	log "WARNING: could not exclude PLAN.md from $BRANCH"

# --------------------------------------------------------------------------
# read-issue
# --------------------------------------------------------------------------

PLAN_FILE=$TREE_DIR/PLAN.md

if phase_done read-issue; then
	phase_skipped read-issue
else
	phase_start read-issue "reading issue $ISSUE from $REPO"
	check_budget read-issue
	if ! gh issue view "$ISSUE" --repo "$REPO" \
		--json number,title,body,labels,url >"$WORK_DIR/issue.json" 2>"$WORK_DIR/gh.log"; then
		fail_workflow read-issue "cannot read issue $ISSUE from $REPO" "$(tail -n 20 "$WORK_DIR/gh.log")"
	fi
	# The issue is written as markdown for the agent to read. It stays in the
	# scratch directory: it is GitHub's copy of someone else's words, not
	# something this run should commit into a branch.
	if ! gh issue view "$ISSUE" --repo "$REPO" >"$ISSUE_FILE" 2>"$WORK_DIR/gh.log"; then
		fail_workflow read-issue "cannot render issue $ISSUE" "$(tail -n 20 "$WORK_DIR/gh.log")"
	fi
	[ -s "$ISSUE_FILE" ] || fail_workflow read-issue "issue $ISSUE rendered empty"
	log "read issue $ISSUE ($(wc -c <"$ISSUE_FILE" | tr -d ' ') bytes)"
	phase_complete read-issue
fi

# A resumed run skipped the phase but still needs the text, because the text
# lives in scratch and scratch is new every run. A failure here is a failure:
# the later phases build the agent's prompt and the pull request body out of it,
# and doing that from an empty file would produce a confidently empty result.
if [ ! -s "$ISSUE_FILE" ]; then
	if ! gh issue view "$ISSUE" --repo "$REPO" >"$ISSUE_FILE" 2>"$WORK_DIR/gh.log"; then
		fail_workflow read-issue "cannot re-read issue $ISSUE while resuming" \
			"$(tail -n 20 "$WORK_DIR/gh.log")"
	fi
	[ -s "$ISSUE_FILE" ] || fail_workflow read-issue "issue $ISSUE rendered empty while resuming"
fi

# --------------------------------------------------------------------------
# plan
# --------------------------------------------------------------------------

if phase_done plan; then
	phase_skipped plan
else
	phase_start plan "asking $AGENT for a plan"
	{
		printf 'You are implementing one GitHub issue in the repository at %s.\n\n' "$TREE_DIR"
		printf 'Write a short implementation plan to PLAN.md in the repository root.\n'
		printf 'Do not change any other file in this phase. Do not commit.\n\n'
		printf '===== issue %s =====\n' "$ISSUE"
		cat "$ISSUE_FILE" 2>/dev/null || true
	} >"$WORK_DIR/prompt-plan.txt"
	run_agent plan "$WORK_DIR/prompt-plan.txt"
	[ -s "$PLAN_FILE" ] ||
		fail_workflow plan "the $AGENT agent wrote no PLAN.md" "$(tail -c "$FAILURE_BYTE_LIMIT" "$WORK_DIR/agent-plan.log" 2>/dev/null)"
	log "plan written ($(wc -c <"$PLAN_FILE" | tr -d ' ') bytes)"
	phase_complete plan
fi

# --------------------------------------------------------------------------
# implement
# --------------------------------------------------------------------------

if phase_done implement; then
	phase_skipped implement
else
	phase_start implement "asking $AGENT to implement the plan"
	BEFORE_SHA=$(git -C "$TREE_DIR" rev-parse HEAD 2>/dev/null) || BEFORE_SHA=
	{
		printf 'You are implementing one GitHub issue in the repository at %s.\n\n' "$TREE_DIR"
		printf 'Follow PLAN.md in the repository root. Make the change and commit it.\n'
		printf 'Keep the change as small as the issue allows. Do not push.\n\n'
		printf '===== plan =====\n'
		cat "$PLAN_FILE" 2>/dev/null || true
		printf '\n===== issue %s =====\n' "$ISSUE"
		cat "$ISSUE_FILE" 2>/dev/null || true
	} >"$WORK_DIR/prompt-implement.txt"
	run_agent implement "$WORK_DIR/prompt-implement.txt"
	AFTER_SHA=$(git -C "$TREE_DIR" rev-parse HEAD 2>/dev/null) || AFTER_SHA=
	# A run that changed nothing is a failed run, not a successful empty one:
	# the marker commit this phase is about to record would otherwise make an
	# agent that did nothing look like an agent that was finished. The test is
	# both halves -- HEAD unchanged AND the tree clean -- because an agent that
	# edited files and forgot to commit did the work, and those edits are the
	# work; this phase commits them rather than throwing them away.
	if [ "$AFTER_SHA" = "$BEFORE_SHA" ] && [ -z "$(git -C "$TREE_DIR" status --porcelain 2>/dev/null)" ]; then
		fail_workflow implement "the $AGENT agent produced no commit and left no changes" \
			"$(tail -c "$FAILURE_BYTE_LIMIT" "$WORK_DIR/agent-implement.log" 2>/dev/null)"
	fi
	if [ -n "$(git -C "$TREE_DIR" status --porcelain 2>/dev/null)" ]; then
		log "the agent left uncommitted changes; committing them as the implementation"
		git -C "$TREE_DIR" add -A >/dev/null 2>&1 || true
		if ! git -C "$TREE_DIR" commit --quiet -m "issue-to-pr: implement issue $ISSUE" \
			>"$WORK_DIR/commit.log" 2>&1; then
			fail_workflow implement "cannot commit the changes the agent left" \
				"$(tail -n 20 "$WORK_DIR/commit.log")"
		fi
	fi
	phase_complete implement
fi

# --------------------------------------------------------------------------
# gates
#
# The repository's own gates, run against the branch. The list is deliberately
# duplicated from branch-gates rather than sourced: that workflow owns its own
# argument handling and failure path, and importing it would import those too.
# --------------------------------------------------------------------------

gate_command() {
	case $1 in
	gofmt) printf 'gofmt -l .' ;;
	vet) printf 'go vet ./...' ;;
	test) printf 'go test ./...' ;;
	esac
}

# The gates are not skippable. A resumed run that trusted an earlier run's
# verdict would be trusting a branch to have been gated by something it cannot
# check, so they run every time, on whatever is actually there now.
phase_start gates "running the repository gates"
{
	FAILED_GATES=0
	: >"$HANDOFF_DIR/failures.txt" 2>/dev/null || true
	for gate in $GATE_NAMES; do
		check_budget gates
		command_line=$(gate_command "$gate")
		gate_log=$WORK_DIR/gate-$gate.log
		log "gate $gate: $command_line"
		append_step "$gate" started "gate $gate" || log "WARNING: $REPORT_ERROR"
		# The repository gates must not inherit this run's execution context:
		# the repo's own tests assert WEFTY_L1_ENDPOINT is unset, and the
		# attempt bridge exports it into every one-shot job environment.
		(cd "$TREE_DIR" && unset WEFTY_RUN_ID WEFTY_RUN_DIR WEFTY_HANDOFF_DIR \
			WEFTY_L1_ENDPOINT WEFTY_L3_ENDPOINT WEFTY_ATTEMPT_TOKEN WEFTY_RUN_TOKEN &&
			sh -c "$command_line") >"$gate_log" 2>&1
		exit_code=$?
		# gofmt reports unformatted files on stdout and still exits 0.
		if [ "$gate" = gofmt ] && [ "$exit_code" -eq 0 ] && [ -s "$gate_log" ]; then
			exit_code=1
		fi
		if [ "$exit_code" -eq 0 ]; then
			outcome=pass
			summary="gate $gate passed"
		else
			outcome=fail
			summary="gate $gate failed with exit code $exit_code"
			FAILED_GATES=$((FAILED_GATES + 1))
			{
				printf '===== %s (exit %s) =====\n' "$gate" "$exit_code"
				head -c "$FAILURE_BYTE_LIMIT" "$gate_log"
				printf '\n'
			} >>"$HANDOFF_DIR/failures.txt" 2>/dev/null || true
		fi
		log "$summary"
		append_step "$gate" ended "$summary" || log "WARNING: $REPORT_ERROR"
		append_gate "$gate" "$outcome" "$gate_log" || log "WARNING: $REPORT_ERROR"
	done
	chmod 0600 "$HANDOFF_DIR/failures.txt" 2>/dev/null || true
	if [ "$FAILED_GATES" -gt 0 ]; then
		fail_workflow gates "$FAILED_GATES of the repository gates failed on $BRANCH" \
			"$(head -c "$FAILURE_BYTE_LIMIT" "$HANDOFF_DIR/failures.txt" 2>/dev/null)"
	fi
}
phase_complete gates

# --------------------------------------------------------------------------
# push
#
# Every phase pushes its own marker, so by here the branch is already on the
# remote. This phase exists because "the branch is published" is its own fact
# worth recording and resuming past: it forces the remote to match local HEAD,
# which is the state open-pr depends on.
# --------------------------------------------------------------------------

# Publishing is not skippable either: it is what open-pr depends on, and a
# fast-forward push of a branch that is already published costs nothing.
phase_start push "publishing $BRANCH"
if ! git -C "$TREE_DIR" push --quiet origin "HEAD:refs/heads/$BRANCH" \
	>"$WORK_DIR/push.log" 2>&1; then
	fail_workflow push "cannot fast-forward $BRANCH on origin; it moved under this run" \
		"$(tail -n 20 "$WORK_DIR/push.log")"
fi
phase_complete push

# --------------------------------------------------------------------------
# open-pr
# --------------------------------------------------------------------------

SUMMARY_FILE=$HANDOFF_DIR/summary.md
PR_FILE=$HANDOFF_DIR/pr.json

# open-pr always runs, including on a resume. It reconciles first: a branch this
# workflow already pushed may already have a pull request, and opening a second
# one is neither possible nor wanted. The artifacts are regenerated either way,
# because they belong to *this* run's handoff directory and an earlier run's
# copy is somewhere this run's reader cannot see.
phase_start open-pr "opening a draft pull request"
{
	{
		printf 'Implements #%s.\n\n' "$ISSUE"
		# shellcheck disable=SC2016 # the backticks are markdown, not a shell expansion.
		printf 'Written by the wefty `issue-to-pr` workflow with the %s agent, run `%s`.\n' "$AGENT" "$RUN_ID"
		printf 'The repository gates (%s) passed on this branch.\n\n' "$(printf '%s' "$GATE_NAMES" | tr ' ' ',')"
		printf '## Plan\n\n'
		cat "$PLAN_FILE" 2>/dev/null || printf '_no plan was recorded_\n'
		printf '\n---\n\nThis pull request is a draft and has not been reviewed by a person.\n'
	} >"$WORK_DIR/pr-body.md"
	cp "$WORK_DIR/pr-body.md" "$SUMMARY_FILE" 2>/dev/null || log "WARNING: could not write $SUMMARY_FILE"
	chmod 0600 "$SUMMARY_FILE" 2>/dev/null || true

	pr_title="issue-to-pr: #$ISSUE"
	PR_URL=
	# Reconcile before creating. `gh pr list` is read-only, so asking costs
	# nothing and answers the one question that decides what to do next.
	if gh pr list --repo "$REPO" --head "$BRANCH" --base "$BASE_BRANCH" \
		--json url,headRefOid >"$WORK_DIR/pr-list.json" 2>"$WORK_DIR/pr-list.log"; then
		PR_URL=$(grep -Eo 'https://[^"[:space:]]+' "$WORK_DIR/pr-list.json" | head -n 1)
	else
		log "WARNING: could not list existing pull requests for $BRANCH; assuming there is none"
	fi
	if [ -n "$PR_URL" ]; then
		log "reusing the existing pull request $PR_URL"
	else
		if ! gh pr create --repo "$REPO" --draft --base "$BASE_BRANCH" --head "$BRANCH" \
			--title "$pr_title" --body-file "$WORK_DIR/pr-body.md" >"$WORK_DIR/pr.log" 2>&1; then
			fail_workflow open-pr "cannot open a draft pull request for $BRANCH" \
				"$(tail -n 20 "$WORK_DIR/pr.log")"
		fi
		PR_URL=$(grep -Eo 'https://[^[:space:]]+' "$WORK_DIR/pr.log" | tail -n 1)
		[ -n "$PR_URL" ] ||
			fail_workflow open-pr "gh printed no pull request URL" "$(tail -n 20 "$WORK_DIR/pr.log")"
	fi
	log "pull request $PR_URL"
}
phase_complete open-pr

# pr.json is written only now, after this phase's own marker commit has been
# committed and pushed: that push is the branch's actual final state, and
# GitHub resolves the pull request's head against the branch, not against
# whatever HEAD was before this phase's marker. Writing it earlier recorded a
# commit that was already one behind the pull request by the time anyone read
# it, and disagreed with result.json, which is written after every phase.
HEAD_SHA=$(git -C "$TREE_DIR" rev-parse HEAD 2>/dev/null) || HEAD_SHA=
printf '{"url":"%s","head_sha":"%s","branch":"%s","issue":"%s","repo":"%s"}\n' \
	"$(json_escape "$PR_URL")" "$(json_escape "$HEAD_SHA")" "$(json_escape "$BRANCH")" \
	"$(json_escape "$ISSUE")" "$(json_escape "$REPO")" >"$PR_FILE" 2>/dev/null ||
	log "WARNING: could not write $PR_FILE"
chmod 0600 "$PR_FILE" 2>/dev/null || true
[ -s "$PR_FILE" ] || fail_workflow open-pr "cannot write $PR_FILE"

# --------------------------------------------------------------------------
# Verdict
# --------------------------------------------------------------------------

# The artifacts are prerequisites for success, not a best-effort flourish. A run
# that opened a pull request and then could not tell anyone about it has not
# succeeded: `wefty results` would answer nothing and the handoff directory
# would be empty, which reads exactly like a run that never got this far.
for required in "$PR_FILE" "$SUMMARY_FILE"; do
	[ -s "$required" ] ||
		fail_workflow open-pr "the run could not write $required, so its outcome is unreadable"
done
write_result_document true "" ""
if ! publish_result succeeded "issue $ISSUE: draft pull request $PR_URL"; then
	fail_workflow open-pr "the run could not publish its result" "$REPORT_ERROR"
fi
[ -s "$HANDOFF_DIR/result.json" ] ||
	fail_workflow open-pr "the run could not write $HANDOFF_DIR/result.json"
FINALIZED=1
append_gate "$WORKFLOW" pass "$PR_FILE" || log "WARNING: $REPORT_ERROR"
log "ran:[${RAN_PHASES# }] skipped:[${SKIPPED_PHASES# }]"
log "done: draft pull request for issue $ISSUE"
