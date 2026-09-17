#!/usr/bin/env bash
#
# branch-gates: run the wefty gates against one ref and hand the verdict back.
#
# Contract: docs/contracts/run-execution-context.md. The workflow reads
# WEFTY_RUN_ID, WEFTY_L3_ENDPOINT, WEFTY_RUN_TOKEN and WEFTY_HANDOFF_DIR, clones
# the repository at the requested ref into a scratch directory, runs the gates,
# writes result.json and failures.txt into the handoff directory, appends one
# envelope per gate and appends one final gate result.
#
# Input arrives as run params (read back from GET /v1/runs/{run_id} with the run
# token, because a job never receives its own params):
#
#   ref        required   branch, tag or commit to test
#   repo_url   optional   clone source (default: the public wefty repository)
#   gates      optional   comma-separated subset of gofmt,fabric-boundary,vet,test
#   copy_to    optional   directory to copy the two result files into, for an
#                         OCI run whose handoff volume is not reachable from the
#                         host (pair it with an operator --mount)
#
# BRANCH_GATES_REF / BRANCH_GATES_REPO_URL / BRANCH_GATES_GATES /
# BRANCH_GATES_COPY_TO override the params for a local dry run.
#
# The repository under test is untrusted code. Every process that touches it —
# the clone, the checkout and each gate — runs under `env -i` with a small
# allowlist, so no run token, attempt credential or other WEFTY_* value is ever
# visible to it. That matters twice: a gate's raw output is copied verbatim into
# failures.txt before the agent's log redaction ever sees it, and a credential
# reaching the subject would let it write its own run or submit L1 children.
# All L3 reporting happens in this shell, outside every gate process.
#
# Exit codes carry the verdict, deliberately. The node agent removes a handoff
# directory as soon as its attempt succeeds (docs/contracts/run-execution-context.md,
# "Node-local handoff lifecycle"), so a workflow that exits 0 leaves no files
# behind. Exiting non-zero on a failing verdict is what keeps result.json and
# failures.txt on the node for the 24-hour retention window — which is the case
# an operator actually wants to read. A failing one-shot is terminal in L1; it
# is not re-executed. The run is failed either way, because L3 fails a run whose
# gate result is `fail`.
#
# Every exit after the execution context is known goes through one path and
# leaves the same two files: a verdict writes the gate results, a workflow error
# writes a result.json carrying `workflow_error` plus a diagnostic failures.txt.
# Both paths also echo result.json into the run log, which is the only result
# surface reachable from the host for an OCI run.

set -u

WORKFLOW=branch-gates
DEFAULT_REPO_URL=https://github.com/Derek-X-Wang/wefty.git
DEFAULT_GATES=gofmt,fabric-boundary,vet,test
KNOWN_GATES="gofmt fabric-boundary vet test"
FAILURE_BYTE_LIMIT=524288

# Fields the failure path needs before they are known.
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
L3_ENDPOINT=$(require_context WEFTY_L3_ENDPOINT) || exit 1
RUN_TOKEN=$(require_context WEFTY_RUN_TOKEN) || exit 1
HANDOFF_DIR=$(require_context WEFTY_HANDOFF_DIR) || exit 1
L3_ENDPOINT=${L3_ENDPOINT%/}
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

mkdir -p "$HANDOFF_DIR" || abort "cannot create handoff directory $HANDOFF_DIR"

# --------------------------------------------------------------------------
# JSON without a helper
#
# There is no envelope/gate helper yet, so every protocol body below is
# assembled by hand. json_escape drops control characters that JSON cannot
# carry raw, escapes the two structural characters, and folds newlines.
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
# L3 calls
#
# These return non-zero instead of exiting, so the caller decides whether a
# failed protocol write is fatal (normal path) or best effort (failure path).
# There is no bounded retry here; adding one is part of #476.
# --------------------------------------------------------------------------

# l3_call sets L3_STATUS and leaves the response body in L3_RESPONSE_FILE. It
# must not be called inside a command substitution: the status has to land in
# this shell, not in a subshell.
L3_STATUS=0
L3_RESPONSE_FILE=
L3_ERROR=

l3_call() {
	method=$1
	path=$2
	body_file=${3:-}
	[ -z "$L3_RESPONSE_FILE" ] || rm -f "$L3_RESPONSE_FILE"
	L3_RESPONSE_FILE=$(mktemp) || return 1
	if [ -n "$body_file" ]; then
		L3_STATUS=$(curl --silent --show-error --max-time 60 \
			--output "$L3_RESPONSE_FILE" --write-out '%{http_code}' \
			--request "$method" \
			--header "Authorization: Bearer $RUN_TOKEN" \
			--header 'Content-Type: application/json' \
			--data-binary "@$body_file" \
			"$L3_ENDPOINT$path") || L3_STATUS=000
	else
		L3_STATUS=$(curl --silent --show-error --max-time 60 \
			--output "$L3_RESPONSE_FILE" --write-out '%{http_code}' \
			--request "$method" \
			--header "Authorization: Bearer $RUN_TOKEN" \
			"$L3_ENDPOINT$path") || L3_STATUS=000
	fi
}

append_envelope() {
	step=$1
	status=$2
	summary=$3
	extensions=$4
	body=$(mktemp) || {
		L3_ERROR="mktemp failed while building the $step envelope"
		return 1
	}
	printf '{"schema_version":1,"envelope_id":"%s","idempotency_key":"%s","run_id":"%s","step_id":"%s","status":"%s","summary":"%s","extensions":{"dev.wefty.branch-gates":%s},"created_at":"%s"}\n' \
		"$(json_escape "$RUN_ID-envelope-$step")" \
		"$(json_escape "$RUN_ID-envelope-$step")" \
		"$(json_escape "$RUN_ID")" \
		"$(json_escape "$step")" \
		"$status" \
		"$(json_escape "$summary")" \
		"$extensions" \
		"$(timestamp)" >"$body"
	l3_call POST "/v1/runs/$RUN_ID/envelopes" "$body"
	rm -f "$body"
	case "$L3_STATUS" in
	200 | 201) return 0 ;;
	esac
	L3_ERROR="append $step envelope returned HTTP $L3_STATUS: $(cat "$L3_RESPONSE_FILE")"
	return 1
}

append_gate() {
	name=$1
	outcome=$2
	evidence=$3
	body=$(mktemp) || {
		L3_ERROR="mktemp failed while building the $name gate"
		return 1
	}
	printf '{"schema_version":1,"gate_id":"%s","idempotency_key":"%s","run_id":"%s","step_id":"%s","name":"%s","outcome":"%s","evidence":%s,"evaluated_at":"%s"}\n' \
		"$(json_escape "$RUN_ID-gate-$name")" \
		"$(json_escape "$RUN_ID-gate-$name")" \
		"$(json_escape "$RUN_ID")" \
		"$(json_escape "$name")" \
		"$(json_escape "$name")" \
		"$outcome" \
		"$evidence" \
		"$(timestamp)" >"$body"
	l3_call POST "/v1/runs/$RUN_ID/gates" "$body"
	rm -f "$body"
	case "$L3_STATUS" in
	200 | 201) return 0 ;;
	esac
	L3_ERROR="append $name gate returned HTTP $L3_STATUS: $(cat "$L3_RESPONSE_FILE")"
	return 1
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
		"$(timestamp)" >"$HANDOFF_DIR/result.json" 2>/dev/null ||
		log "WARNING: could not write $HANDOFF_DIR/result.json"
	{
		printf '===== workflow-error: %s =====\n' "$step"
		printf '%s\n' "$message"
		[ -z "$detail" ] || printf '%s\n' "$detail"
	} >"$HANDOFF_DIR/failures.txt" 2>/dev/null ||
		log "WARNING: could not write $HANDOFF_DIR/failures.txt"
	chmod 0600 "$HANDOFF_DIR/result.json" "$HANDOFF_DIR/failures.txt" 2>/dev/null || true

	log "result.json:"
	cat "$HANDOFF_DIR/result.json" 2>/dev/null || true
	copy_results_out

	append_envelope "$step" failed "$message" '{}' || log "WARNING: $L3_ERROR"
	append_gate "$WORKFLOW" error \
		"[{\"kind\":\"step\",\"value\":\"$(json_escape "$step")\"},{\"kind\":\"error\",\"value\":\"$(json_escape "$message")\"}]" ||
		log "WARNING: $L3_ERROR"
	exit 1
}

# There is no command that reads a handoff file out of an OCI handoff volume,
# so an OCI run can also copy the two files through an operator mount.
copy_results_out() {
	[ -n "$COPY_TO" ] || return 0
	if mkdir -p "$COPY_TO/$RUN_ID" 2>/dev/null &&
		cp "$HANDOFF_DIR/result.json" "$HANDOFF_DIR/failures.txt" "$COPY_TO/$RUN_ID/" 2>/dev/null; then
		log "copied result.json and failures.txt to $COPY_TO/$RUN_ID"
	else
		log "WARNING: could not copy results to $COPY_TO/$RUN_ID"
	fi
}

for tool in curl git go; do
	command -v "$tool" >/dev/null 2>&1 ||
		fail_workflow environment "$tool is required in the job rootfs"
done

# --------------------------------------------------------------------------
# Input
# --------------------------------------------------------------------------

l3_call GET "/v1/runs/$RUN_ID" || fail_workflow input "cannot read the run record"
case "$L3_STATUS" in
200) ;;
*) fail_workflow input "read run params returned HTTP $L3_STATUS" "$(cat "$L3_RESPONSE_FILE")" ;;
esac
RUN_RECORD=$(cat "$L3_RESPONSE_FILE")

# There is no helper for reading params either. jq is not in the reference
# rootfs, so a flat string field is extracted textually; every branch-gates
# param is deliberately a flat string for exactly this reason.
param() {
	if command -v jq >/dev/null 2>&1; then
		printf '%s' "$RUN_RECORD" | jq -r --arg name "$1" '.params[$name] // empty'
		return
	fi
	printf '%s' "$RUN_RECORD" |
		tr -d '\n' |
		sed -n 's/.*"'"$1"'"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p'
}

REF=${BRANCH_GATES_REF:-$(param ref)}
REPO_URL=${BRANCH_GATES_REPO_URL:-$(param repo_url)}
GATES=${BRANCH_GATES_GATES:-$(param gates)}
COPY_TO=${BRANCH_GATES_COPY_TO:-$(param copy_to)}
[ -n "$REPO_URL" ] || REPO_URL=$DEFAULT_REPO_URL
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

WORK_DIR=${TMPDIR:-/tmp}/wefty-branch-gates-$RUN_ID
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR" || fail_workflow environment "cannot create $WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT
CLONE_DIR=$WORK_DIR/repo
# The gates shell out to the Go toolchain, which needs a cache directory. A
# container rootfs may carry no HOME at all.
[ -n "${HOME:-}" ] || { HOME=$WORK_DIR/home && export HOME && mkdir -p "$HOME"; }
export GIT_TERMINAL_PROMPT=0
export GOTOOLCHAIN=${GOTOOLCHAIN:-auto}

# SUBJECT_ENV is the complete environment handed to untrusted code: `env -i`
# plus the few names the clone and the gates genuinely need. Nothing else
# crosses, so WEFTY_RUN_TOKEN and WEFTY_ATTEMPT_TOKEN cannot reach the subject
# repository or its output.
SUBJECT_ENV=(env -i)
for name in PATH HOME LANG LC_ALL TMPDIR GIT_TERMINAL_PROMPT \
	GOFLAGS GOTOOLCHAIN GOCACHE GOMODCACHE GOPATH; do
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
	GATE_EVIDENCE="$GATE_EVIDENCE{\"kind\":\"gate:$(json_escape "$gate")\",\"value\":\"$outcome\"}"

	append_envelope "$gate" "$status" "$summary" \
		"{\"ref\":\"$(json_escape "$REF")\",\"commit\":\"$(json_escape "$COMMIT")\",\"gate\":\"$(json_escape "$gate")\",\"outcome\":\"$outcome\",\"exit_code\":$exit_code,\"duration_seconds\":$duration}" ||
		fail_workflow publish "$L3_ERROR"
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

RESULT_FILE=$HANDOFF_DIR/result.json
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
chmod 0600 "$RESULT_FILE" "$HANDOFF_DIR/failures.txt" 2>/dev/null || true
copy_results_out

log "result.json:"
cat "$RESULT_FILE"
if [ "$FAILED_GATES" -gt 0 ]; then
	log "failures.txt (first 200 lines):"
	head -n 200 "$HANDOFF_DIR/failures.txt"
	printf '\n'
fi

append_gate "$WORKFLOW" "$VERDICT" \
	"[{\"kind\":\"ref\",\"value\":\"$(json_escape "$REF")\"},{\"kind\":\"commit\",\"value\":\"$(json_escape "$COMMIT")\"},{\"kind\":\"gates-run\",\"value\":\"$RAN_GATES\"},{\"kind\":\"gates-failed\",\"value\":\"$FAILED_GATES\"},$GATE_EVIDENCE]" ||
	fail_workflow publish "$L3_ERROR"

log "verdict $VERDICT for $REF ($COMMIT): $FAILED_GATES of $RAN_GATES gates failed"
if [ "$FAILED_GATES" -gt 0 ]; then
	log "handoff directory $HANDOFF_DIR is retained because this attempt fails"
	exit 1
fi
exit 0
