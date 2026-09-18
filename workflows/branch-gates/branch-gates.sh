#!/usr/bin/env bash
#
# branch-gates: run the wefty gates against one ref and hand the verdict back.
#
# Contract: docs/contracts/run-execution-context.md. The workflow reads
# WEFTY_RUN_ID, WEFTY_RUN_DIR and WEFTY_HANDOFF_DIR, clones the repository at
# the requested ref into a scratch directory, runs the gates, writes
# result.json and failures.txt into the handoff directory, reports one envelope
# per gate, and reports one final gate result.
#
# It reports through the run mailbox, not over HTTP. `wefty run envelope|gate|
# result` writes event files into WEFTY_RUN_DIR and the node agent publishes
# them to the run ledger with the credential it already holds, so this job holds
# none of its own. Where `wefty` is not on PATH -- a stock upstream image, for
# one -- the embedded inline writer below produces the same events.
#
# Input arrives as run params, read from the mailbox with `wefty run params`
# (the agent writes them there; a job never receives them in its environment):
#
#   ref        required   branch, tag or commit to test
#   repo_url   optional   clone source (default: the public wefty repository)
#   gates      optional   comma-separated subset of gofmt,fabric-boundary,vet,test;
#                         an empty string means the same as omitting it
#   copy_to    optional   directory to copy the two result files into, for an
#                         OCI run whose handoff volume is not reachable from the
#                         host (pair it with an operator --mount)
#
# BRANCH_GATES_REF / BRANCH_GATES_REPO_URL / BRANCH_GATES_GATES /
# BRANCH_GATES_COPY_TO override the params for a local dry run.
#
# Trust boundary: the gates run as the run's own OS user, as descendants of
# this shell. Reporting goes through the mailbox, so this shell holds neither
# the run token nor the attempt credential, and a hostile branch therefore gains
# no authority over the cluster from this run -- it cannot write another run or
# submit a child job. It can still forge this run's own evidence by writing into
# the mailbox. Everything else is unchanged: `env -i` with a small allowlist,
# fresh per-run HOME/XDG/Go caches and disabled global Git configuration are
# hygiene, not an OS boundary, the subject inherits this node's module and TLS
# configuration (a proxy URL can carry credentials of its own), and a branch
# running under the same UID still shares this user's access to the machine.
# Only test branches you trust.
#
# Exit codes carry the verdict, deliberately, and they no longer decide whether
# the files survive: a finished run's handoff directory is retained on every
# outcome (docs/contracts/run-execution-context.md, "Results and their
# retention"). Exiting non-zero on a failing verdict is the verdict itself, not
# a way to keep evidence. A failing one-shot is terminal in L1; it is not
# re-executed. The run is failed either way, because L3 fails a run whose gate
# result is `fail`.
#
# Every exit after the execution context is known goes through one path and
# leaves the same two files: a verdict writes the gate results, a workflow error
# writes a result.json carrying `workflow_error` plus a diagnostic failures.txt.
# The node uploads result.json to the ledger when the run completes, so the
# document is read with `wefty results RUN_ID` from anywhere, with no node
# involved. It is no longer echoed into the run log: the log carried it only
# because nothing else could reach it, and a result document duplicated into a
# log stream is noise once it has a home of its own.

# The embedded inline writer below is a verbatim copy, so its functions cannot
# carry their own directives, and this workflow reaches them indirectly --
# through `report`, which takes the producer as arguments. SC2329 (and SC2317,
# which shellcheck 0.9.0 in CI raises for the same reason) is therefore
# silenced for the file rather than for a block a copy is not allowed to have.
# shellcheck disable=SC2317,SC2329
set -u

WORKFLOW=branch-gates
DEFAULT_REPO_URL=https://github.com/Derek-X-Wang/wefty.git
DEFAULT_GATES=gofmt,fabric-boundary,vet,test
KNOWN_GATES="gofmt fabric-boundary vet test"
FAILURE_BYTE_LIMIT=524288

# Fields the failure path needs before they are known.
RESULT_FILE=
REF=
REPO_URL=
GATES=
COPY_TO=
COMMIT=
STARTED_AT=

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
# The value is not kept: every `wefty run` subcommand reads WEFTY_RUN_DIR from
# the environment itself. What matters here is failing early, with the contract
# named, when this job was dispatched without a mailbox to report through.
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

# Everything this run creates lives under one private scratch directory: the
# curl auth config, the clone, the gate logs and every directory the subject is
# allowed to write. The Go module cache makes its directories read-only, so the
# sweep restores write permission before removing the tree.
# Deliberately short and deliberately not under $TMPDIR: a subject's own tests
# create unix sockets under the TMPDIR handed to them, and a socket path is
# capped at 104 bytes on macOS. The per-user $TMPDIR there is already ~49 bytes,
# so a work directory named after the full run ID pushes the subject's sockets
# over the limit -- wefty's own suite fails that way. BRANCH_GATES_WORK_ROOT
# overrides the root for a node whose /tmp is unsuitable.
# The root is resolved to its physical path because /tmp is a symlink on macOS
# and code under test refuses symlinked components (wefty's own agent does).
WORK_ROOT=${BRANCH_GATES_WORK_ROOT:-/tmp}
WORK_ROOT=$(cd "$WORK_ROOT" 2>/dev/null && pwd -P) ||
	abort "work root ${BRANCH_GATES_WORK_ROOT:-/tmp} is not a usable directory"
WORK_DIR=$WORK_ROOT/wefty-bg-$(printf '%.12s' "${RUN_ID##*_}")
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR" || abort "cannot create $WORK_DIR"
chmod 0700 "$WORK_DIR" 2>/dev/null || true
# The result document is assembled here and published from here: `wefty run
# result` is what places the handoff copy, so writing that copy by hand as well
# would mean the helper reading the file it is writing.
RESULT_FILE=$WORK_DIR/result.json
# shellcheck disable=SC2317,SC2329 # invoked indirectly via the EXIT trap below.
cleanup() {
	status=$?
	if [ -d "$WORK_DIR" ]; then
		# The Go module cache -- and a downloaded toolchain in particular --
		# leaves read-only directories that u+w alone cannot make traversable.
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
#
# The protocol bodies are the helper's job now. What is still assembled here is
# result.json, which is this workflow's own schema rather than the contract's.
# json_escape drops control characters that JSON cannot carry raw, escapes the
# two structural characters, and folds newlines.
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
# publish. Two producers write the same file protocol: `wefty run`, which is
# preferred wherever the binary exists, and the inline writer below for an image
# that does not ship it -- the upstream `golang` image the OCI examples use, for
# one. The inline writer is a verbatim copy of the one `wefty workflow init`
# scaffolds; a test asserts the two stay byte-identical.
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

append_envelope() {
	step=$1
	status=$2
	summary=$3
	payload_file=$4
	if [ "$REPORTER" = wefty ]; then
		if [ -n "$payload_file" ]; then
			report wefty run envelope --step "$step" --status "$status" \
				--summary "$summary" --payload-json-file "$payload_file"
			return
		fi
		report wefty run envelope --step "$step" --status "$status" --summary "$summary"
		return
	fi
	# The inline writer declares every payload `text`: it has no JSON parser, and
	# an event whose payload claims `json` and fails to decode is refused
	# outright. The document is the same, carried as the envelope's detail.
	report wefty_event envelope "" "$step" "$status" "" "$summary" "$payload_file"
}

append_gate() {
	name=$1
	outcome=$2
	evidence_file=$3
	if [ "$REPORTER" = wefty ]; then
		if [ -n "$evidence_file" ]; then
			report wefty run gate --name "$name" --outcome "$outcome" --evidence-file "$evidence_file"
			return
		fi
		report wefty run gate --name "$name" --outcome "$outcome"
		return
	fi
	report wefty_event gate "$name" "" "" "$outcome" "" "$evidence_file"
}

# publish_result records the run's final document two ways: into the ledger as
# the run's result event, and into the handoff directory as result.json.
#
# The handoff copy is written here rather than left to `wefty run result`,
# which also writes it. An operator reads the verdict off the node, and that
# must not depend on reporting having succeeded -- the failure path in
# particular reports last and may not report at all.
publish_result() {
	status=$1
	summary=$2
	if ! cp "$RESULT_FILE" "$HANDOFF_DIR/result.json" 2>/dev/null; then
		log "WARNING: could not write $HANDOFF_DIR/result.json"
	fi
	chmod 0600 "$HANDOFF_DIR/result.json" 2>/dev/null || true
	if [ "$REPORTER" = wefty ]; then
		report wefty run result --file "$RESULT_FILE" --status "$status" --summary "$summary"
		return
	fi
	report wefty_event result result result "$status" "" "$summary" "$RESULT_FILE"
}

# --------------------------------------------------------------------------
# The single failure path
#
# Everything that goes wrong after the execution context is known — bad input,
# a missing tool, a clone failure, a refused protocol write — leaves the same
# two files in the handoff directory, echoes them, then publishes a failed
# envelope and an `error` gate on a best-effort basis before exiting non-zero.
# An `error` gate means the workflow could not do its job; a `fail` gate means
# the branch is bad.
# --------------------------------------------------------------------------

fail_workflow() {
	step=$1
	message=$2
	detail=${3:-}
	log "WORKFLOW ERROR at $step: $message"

	printf '{"schema_version":1,"workflow":"%s","run_id":"%s","repo_url":"%s","ref":"%s","commit":"%s","passed":false,"failed_gates":0,"workflow_error":{"step":"%s","message":"%s"},"started_at":"%s","finished_at":"%s","gates":[]}\n' \
		"$WORKFLOW" \
		"$(json_escape "$RUN_ID")" \
		"$(json_escape "$REPO_URL")" \
		"$(json_escape "$REF")" \
		"$(json_escape "$COMMIT")" \
		"$(json_escape "$step")" \
		"$(json_escape "$message")" \
		"$STARTED_AT" \
		"$(timestamp)" >"$RESULT_FILE" 2>/dev/null ||
		log "WARNING: could not write $RESULT_FILE"
	{
		printf '===== workflow-error: %s =====\n' "$step"
		printf '%s\n' "$message"
		[ -z "$detail" ] || printf '%s\n' "$detail"
	} >"$HANDOFF_DIR/failures.txt" 2>/dev/null ||
		log "WARNING: could not write $HANDOFF_DIR/failures.txt"
	chmod 0600 "$HANDOFF_DIR/failures.txt" 2>/dev/null || true

	append_envelope "$step" failed "$message" "" || log "WARNING: $REPORT_ERROR"
	publish_result failed "$message" || log "WARNING: $REPORT_ERROR"
	evidence_file=$WORK_DIR/error-evidence.txt
	printf 'step: %s\nerror: %s\n' "$step" "$message" >"$evidence_file" 2>/dev/null || evidence_file=
	append_gate "$WORKFLOW" error "$evidence_file" || log "WARNING: $REPORT_ERROR"
	copy_results_out
	# Last line on this path. An operator (and the CI exercise) can wait for
	# "done:" instead of guessing whether the log has settled.
	log "done: workflow-error at $step"
	exit 1
}

# failures.txt is not uploaded anywhere -- only result.json is -- so an OCI run
# can still copy both files out through an operator mount when one is configured.
copy_results_out() {
	[ -n "$COPY_TO" ] || return 0
	if mkdir -p "$COPY_TO/$RUN_ID" 2>/dev/null &&
		cp "$HANDOFF_DIR/result.json" "$HANDOFF_DIR/failures.txt" "$COPY_TO/$RUN_ID/" 2>/dev/null; then
		log "copied result.json and failures.txt to $COPY_TO/$RUN_ID"
	else
		log "WARNING: could not copy results to $COPY_TO/$RUN_ID"
	fi
}

for tool in git go; do
	command -v "$tool" >/dev/null 2>&1 ||
		fail_workflow environment "$tool is required in the job rootfs"
done

# --------------------------------------------------------------------------
# Input
# --------------------------------------------------------------------------

# The agent writes the submitted params into the mailbox, and the helper reads
# one named value out of them. No cluster call, no credential, and no
# hand-rolled JSON extraction.
param() {
	if [ "$REPORTER" = wefty ]; then
		wefty run params "$1" 2>/dev/null || true
		return
	fi
	wefty_param "$1" 2>/dev/null || true
}

REF=${BRANCH_GATES_REF:-$(param ref)}
REPO_URL=${BRANCH_GATES_REPO_URL:-$(param repo_url)}
GATES=${BRANCH_GATES_GATES:-$(param gates)}
COPY_TO=${BRANCH_GATES_COPY_TO:-$(param copy_to)}
[ -n "$REPO_URL" ] || REPO_URL=$DEFAULT_REPO_URL
# An absent and a present-but-empty gates param are deliberately the same
# thing: the default set. Anything else that would silently reduce the set --
# an empty element, whitespace, an unknown name, a repeat -- is rejected below.
[ -n "$GATES" ] || GATES=$DEFAULT_GATES

if [ -z "$REF" ]; then
	fail_workflow input "params.ref is required: submit with --params '{\"ref\":\"<branch-or-commit>\"}'"
fi

# Parse the gate list without word splitting or globbing, and reject anything
# that would silently run the wrong set — an empty element, whitespace, an
# unknown name or a duplicate.
case "$GATES" in
,* | *, | *,,*)
	fail_workflow input "params.gates has an empty element: \"$GATES\"" ;;
esac
SELECTED_GATES=()
IFS=',' read -r -a SELECTED_GATES <<<"$GATES"
if [ "${#SELECTED_GATES[@]}" -eq 0 ]; then
	fail_workflow input "params.gates selected no gate; it accepts $DEFAULT_GATES"
fi
seen_gates=" "
for candidate in "${SELECTED_GATES[@]}"; do
	case "$candidate" in
	"") fail_workflow input "params.gates has an empty element: \"$GATES\"" ;;
	*[[:space:]]*) fail_workflow input "params.gates element \"$candidate\" contains whitespace" ;;
	esac
	case " $KNOWN_GATES " in
	*" $candidate "*) ;;
	*) fail_workflow input "unknown gate \"$candidate\"; params.gates accepts $DEFAULT_GATES" ;;
	esac
	case "$seen_gates" in
	*" $candidate "*) fail_workflow input "params.gates repeats \"$candidate\"" ;;
	esac
	seen_gates="$seen_gates$candidate "
done

log "run $RUN_ID: gates [$GATES] on $REF from $REPO_URL"

# --------------------------------------------------------------------------
# Checkout
# --------------------------------------------------------------------------

CLONE_DIR=$WORK_DIR/repo

# Every directory the subject may write is fresh and private to this run, so it
# can neither read the operator's dotfiles (.netrc, .gitconfig, credential
# helpers, SSH config, the Go env file) nor poison a shared cache for the next
# run. The cost is a cold Go cache on every run; warm-cache seeding is a later
# question, not a correctness one.
SUBJECT_HOME=$WORK_DIR/subject-home
SUBJECT_TMP=$WORK_DIR/subject-tmp
SUBJECT_GOCACHE=$WORK_DIR/go-build
SUBJECT_GOMODCACHE=$WORK_DIR/go-mod
SUBJECT_GOPATH=$WORK_DIR/go-path
mkdir -p "$SUBJECT_HOME/.config" "$SUBJECT_HOME/.cache" "$SUBJECT_TMP" \
	"$SUBJECT_GOCACHE" "$SUBJECT_GOMODCACHE" "$SUBJECT_GOPATH" ||
	fail_workflow environment "cannot create the subject directories under $WORK_DIR"

# A fixed PATH built from the tools this workflow verified, plus the standard
# system directories -- not the ambient PATH.
SUBJECT_PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
for tool in go gofmt git bash sh; do
	resolved=$(command -v "$tool" 2>/dev/null) || continue
	directory=$(dirname "$resolved")
	case ":$SUBJECT_PATH:" in
	*":$directory:"*) ;;
	*) SUBJECT_PATH="$directory:$SUBJECT_PATH" ;;
	esac
done

# SUBJECT_ENV is the complete environment handed to the code under test: `env -i`
# plus exactly these names. No WEFTY_* value crosses it, global and system Git
# configuration are disabled so a hostile checkout cannot activate a configured
# hook or filter, and the Go caches point into this run's scratch directory.
SUBJECT_ENV=(
	env -i
	"PATH=$SUBJECT_PATH"
	"HOME=$SUBJECT_HOME"
	"TMPDIR=$SUBJECT_TMP"
	"XDG_CONFIG_HOME=$SUBJECT_HOME/.config"
	"XDG_CACHE_HOME=$SUBJECT_HOME/.cache"
	"GOCACHE=$SUBJECT_GOCACHE"
	"GOMODCACHE=$SUBJECT_GOMODCACHE"
	"GOPATH=$SUBJECT_GOPATH"
	"GOTOOLCHAIN=${GOTOOLCHAIN:-auto}"
	GIT_TERMINAL_PROMPT=0
	GIT_CONFIG_GLOBAL=/dev/null
	GIT_CONFIG_NOSYSTEM=1
)
# Configuration, not secrets: a private or offline node needs these to resolve
# modules and trust its CA at all, so they are passed through when set.
for name in LANG LC_ALL GOFLAGS GOPROXY GOSUMDB GONOSUMDB GOPRIVATE GOINSECURE \
	SSL_CERT_FILE SSL_CERT_DIR HTTP_PROXY HTTPS_PROXY NO_PROXY \
	http_proxy https_proxy no_proxy; do
	eval "value=\${$name:-}"
	[ -z "$value" ] || SUBJECT_ENV+=("$name=$value")
done

clone_log=$WORK_DIR/clone.log
if ! "${SUBJECT_ENV[@]}" git clone --quiet --filter=blob:none "$REPO_URL" "$CLONE_DIR" >"$clone_log" 2>&1; then
	fail_workflow checkout "clone $REPO_URL failed" "$(tail -n 20 "$clone_log")"
fi
if ! "${SUBJECT_ENV[@]}" git -C "$CLONE_DIR" checkout --quiet --detach "$REF" >"$clone_log" 2>&1 &&
	! "${SUBJECT_ENV[@]}" git -C "$CLONE_DIR" checkout --quiet --detach "origin/$REF" >>"$clone_log" 2>&1; then
	fail_workflow checkout "ref \"$REF\" is not a branch, tag or commit in $REPO_URL" "$(tail -n 20 "$clone_log")"
fi
COMMIT=$("${SUBJECT_ENV[@]}" git -C "$CLONE_DIR" rev-parse HEAD) ||
	fail_workflow checkout "cannot resolve HEAD after checking out \"$REF\""
log "checked out $REF at $COMMIT"

# --------------------------------------------------------------------------
# Gates
# --------------------------------------------------------------------------

gate_command() {
	case "$1" in
	gofmt) printf '%s' 'gofmt -l .' ;;
	fabric-boundary) printf '%s' 'bash scripts/check-fabric-boundary.sh' ;;
	vet) printf '%s' 'go vet ./...' ;;
	test) printf '%s' 'go test ./...' ;;
	esac
}

FAILURES_FILE=$WORK_DIR/failures.txt
: >"$FAILURES_FILE"
GATES_JSON=
GATE_EVIDENCE=
FAILED_GATES=0
RAN_GATES=0

for gate in "${SELECTED_GATES[@]}"; do
	command_line=$(gate_command "$gate")
	gate_log=$WORK_DIR/gate-$gate.log
	started=$(date -u +%s)
	log "gate $gate: $command_line"
	(cd "$CLONE_DIR" && "${SUBJECT_ENV[@]}" sh -c "$command_line") >"$gate_log" 2>&1
	exit_code=$?
	# gofmt reports unformatted files on stdout and still exits 0.
	if [ "$gate" = gofmt ] && [ "$exit_code" -eq 0 ] && [ -s "$gate_log" ]; then
		exit_code=1
	fi
	duration=$(($(date -u +%s) - started))
	RAN_GATES=$((RAN_GATES + 1))

	if [ "$exit_code" -eq 0 ]; then
		outcome=pass
		status=succeeded
		summary="gate $gate passed on $REF ($COMMIT) in ${duration}s"
	else
		outcome=fail
		status=failed
		FAILED_GATES=$((FAILED_GATES + 1))
		summary="gate $gate failed on $REF ($COMMIT) with exit code $exit_code after ${duration}s"
		{
			printf '===== %s (exit %s) =====\n' "$gate" "$exit_code"
			head -c "$FAILURE_BYTE_LIMIT" "$gate_log"
			if [ "$(wc -c <"$gate_log")" -gt "$FAILURE_BYTE_LIMIT" ]; then
				printf '\n... truncated at %s bytes; the full output is in the run log ...\n' "$FAILURE_BYTE_LIMIT"
			fi
			printf '\n'
		} >>"$FAILURES_FILE"
	fi
	log "$summary"

	[ -z "$GATES_JSON" ] || GATES_JSON="$GATES_JSON,"
	GATES_JSON="$GATES_JSON{\"name\":\"$(json_escape "$gate")\",\"outcome\":\"$outcome\",\"exit_code\":$exit_code,\"duration_seconds\":$duration,\"command\":\"$(json_escape "$command_line")\"}"
	[ -z "$GATE_EVIDENCE" ] || GATE_EVIDENCE="$GATE_EVIDENCE,"
	GATE_EVIDENCE="$GATE_EVIDENCE\"$(json_escape "$gate")\":\"$outcome\""

	envelope_payload=$WORK_DIR/envelope-$gate.json
	printf '{"ref":"%s","commit":"%s","gate":"%s","outcome":"%s","exit_code":%s,"duration_seconds":%s}\n' \
		"$(json_escape "$REF")" "$(json_escape "$COMMIT")" "$(json_escape "$gate")" \
		"$outcome" "$exit_code" "$duration" >"$envelope_payload" ||
		fail_workflow publish "cannot write the $gate envelope payload"
	append_envelope "$gate" "$status" "$summary" "$envelope_payload" ||
		fail_workflow publish "$REPORT_ERROR"
done

if [ "$RAN_GATES" -eq 0 ]; then
	fail_workflow input "no gate ran for \"$GATES\"; params.gates accepts $DEFAULT_GATES"
fi

FINISHED_AT=$(timestamp)
if [ "$FAILED_GATES" -eq 0 ]; then
	PASSED=true
	VERDICT=pass
else
	PASSED=false
	VERDICT=fail
fi

# --------------------------------------------------------------------------
# Results
# --------------------------------------------------------------------------

printf '{"schema_version":1,"workflow":"%s","run_id":"%s","repo_url":"%s","ref":"%s","commit":"%s","passed":%s,"failed_gates":%s,"started_at":"%s","finished_at":"%s","gates":[%s]}\n' \
	"$WORKFLOW" \
	"$(json_escape "$RUN_ID")" \
	"$(json_escape "$REPO_URL")" \
	"$(json_escape "$REF")" \
	"$(json_escape "$COMMIT")" \
	"$PASSED" \
	"$FAILED_GATES" \
	"$STARTED_AT" \
	"$FINISHED_AT" \
	"$GATES_JSON" >"$RESULT_FILE" ||
	fail_workflow results "cannot write $RESULT_FILE"
cp "$FAILURES_FILE" "$HANDOFF_DIR/failures.txt" ||
	fail_workflow results "cannot write $HANDOFF_DIR/failures.txt"
chmod 0600 "$HANDOFF_DIR/failures.txt" 2>/dev/null || true

# One call records the verdict document both ways: into the ledger as the run's
# result event, and into the handoff directory as result.json.
if [ "$FAILED_GATES" -eq 0 ]; then
	RESULT_STATUS=succeeded
else
	RESULT_STATUS=failed
fi
publish_result "$RESULT_STATUS" "verdict $VERDICT for $REF ($COMMIT): $FAILED_GATES of $RAN_GATES gates failed" ||
	fail_workflow publish "$REPORT_ERROR"
copy_results_out

log "result: wefty results $RUN_ID"
if [ "$FAILED_GATES" -gt 0 ]; then
	log "failures.txt (first 200 lines):"
	head -n 200 "$HANDOFF_DIR/failures.txt"
	printf '\n'
fi

# The final gate carries one evidence document rather than the protocol's
# per-entry array: the mailbox gives a gate exactly one evidence value, so the
# structure moves inside it.
VERDICT_EVIDENCE=$WORK_DIR/verdict-evidence.json
printf '{"ref":"%s","commit":"%s","gates_run":%s,"gates_failed":%s,"gates":{%s}}\n' \
	"$(json_escape "$REF")" "$(json_escape "$COMMIT")" \
	"$RAN_GATES" "$FAILED_GATES" "$GATE_EVIDENCE" >"$VERDICT_EVIDENCE" ||
	fail_workflow publish "cannot write the verdict evidence"
append_gate "$WORKFLOW" "$VERDICT" "$VERDICT_EVIDENCE" ||
	fail_workflow publish "$REPORT_ERROR"

log "verdict $VERDICT for $REF ($COMMIT): $FAILED_GATES of $RAN_GATES gates failed"
if [ "$FAILED_GATES" -gt 0 ]; then
	log "handoff directory $HANDOFF_DIR is retained because this attempt fails"
	log "done: $VERDICT"
	exit 1
fi
log "done: $VERDICT"
exit 0
