#!/usr/bin/env bash
#
# go-test-agent: run `go test ./agent/` for one commit of a local wefty
# checkout, as a kind=process run on this Mac, and report through the run
# mailbox (no credential needed).
#
# Params:  repo    path of the local checkout to clone from
#          commit  commit to test
#
# The tests run in a fresh clone, so edits to the working tree during the run
# do not change what is tested. TMPDIR is a short, symlink-free directory:
# macOS caps unix socket paths at 104 bytes, and wefty's agent refuses a root
# reached through a symlink (see workflows/branch-gates/README.md). The
# job's own WEFTY_* variables are removed from the test process: the agent
# tests start child processes and assert which WEFTY_* variables they see, so
# the outer run's context would leak into those assertions.
set -u

WEFTY=$PROTO/bin/wefty
GO=/opt/homebrew/bin/go
GIT=/opt/homebrew/bin/git
JQ=/opt/homebrew/bin/jq
export PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

log() { printf 'go-test-agent: %s\n' "$*"; }

: "${WEFTY_RUN_ID:?WEFTY_RUN_ID is required}"
: "${WEFTY_RUN_DIR:?WEFTY_RUN_DIR is required}"

REPO=$("$WEFTY" run params repo) || { log "cannot read param repo"; exit 1; }
COMMIT=$("$WEFTY" run params commit) || { log "cannot read param commit"; exit 1; }
COMMAND="go test -count=1 ./agent/"

WORK=$(cd /tmp && pwd -P)/wefty-gta-$(printf '%.12s' "${WEFTY_RUN_ID##*_}")
rm -rf "$WORK"
mkdir -p "$WORK/tmp" || { log "cannot create $WORK"; exit 1; }
chmod 0700 "$WORK"
trap 'chmod -R u+rwX "$WORK" 2>/dev/null; rm -rf "$WORK"' EXIT

# finish PASSED EXIT_CODE STARTED FINISHED [WORKFLOW_ERROR]
finish() {
	local passed=$1 rc=$2 started=$3 finished=$4 werr=${5:-}
	local status=succeeded outcome=pass
	[ "$passed" = true ] || { status=failed; outcome=fail; }
	[ -z "$werr" ] || outcome=error
	tail -n 200 "$WORK/test.log" >"$WORK/evidence.txt" 2>/dev/null || : >"$WORK/evidence.txt"
	"$JQ" -n \
		--argjson passed "$passed" --argjson exit_code "$rc" \
		--arg command "$COMMAND" --arg commit "$COMMIT" \
		--arg started_at "$(date -u -r "$started" +%Y-%m-%dT%H:%M:%SZ)" \
		--arg finished_at "$(date -u -r "$finished" +%Y-%m-%dT%H:%M:%SZ)" \
		--argjson duration_seconds "$((finished - started))" \
		--arg workflow_error "$werr" \
		--rawfile output_tail "$WORK/evidence.txt" \
		'{passed: $passed, command: $command, commit: $commit, exit_code: $exit_code,
		  started_at: $started_at, finished_at: $finished_at,
		  duration_seconds: $duration_seconds, output_tail: $output_tail}
		 + (if $workflow_error == "" then {} else {workflow_error: $workflow_error} end)' \
		>"$WORK/result.json"
	"$WEFTY" run envelope --step test --status "$status" \
		--summary "$COMMAND: exit $rc in $((finished - started))s" \
		--payload-file "$WORK/evidence.txt"
	"$WEFTY" run gate --name go-test-agent --outcome "$outcome" \
		--evidence-file "$WORK/evidence.txt" --summary "$COMMAND exit $rc"
	"$WEFTY" run result --file "$WORK/result.json" --status "$status" \
		--summary "passed=$passed duration=$((finished - started))s"
}

"$WEFTY" run step --name checkout --summary "clone $COMMIT"
started=$(date +%s)
if ! { "$GIT" clone --quiet --no-checkout "$REPO" "$WORK/src" &&
	"$GIT" -C "$WORK/src" checkout --quiet --detach "$COMMIT"; } >"$WORK/test.log" 2>&1; then
	cat "$WORK/test.log"
	"$WEFTY" run step --name checkout --end
	finish false 1 "$started" "$(date +%s)" "checkout of $COMMIT from $REPO failed"
	exit 1
fi
"$WEFTY" run step --name checkout --end
log "testing $COMMIT: $COMMAND"

"$WEFTY" run step --name test --summary "$COMMAND"
started=$(date +%s)
# go test prints nothing until the package finishes; a heartbeat keeps the
# one-shot from being stopped as idle after 10 silent minutes.
(while sleep 60; do log "still running ($(($(date +%s) - started))s)"; done) &
heartbeat=$!
unset_args=()
for name in $(compgen -e); do
	case $name in WEFTY_*) unset_args+=(-u "$name") ;; esac
done
log "removed from the test environment: ${unset_args[*]+${unset_args[*]}}"
(cd "$WORK/src" && env ${unset_args[@]+"${unset_args[@]}"} TMPDIR="$WORK/tmp" "$GO" test -count=1 ./agent/) 2>&1 | tee "$WORK/test.log"
rc=${PIPESTATUS[0]}
finished=$(date +%s)
kill "$heartbeat" 2>/dev/null
wait "$heartbeat" 2>/dev/null
"$WEFTY" run step --name test --end
log "exit $rc after $((finished - started))s"

if [ "$rc" -eq 0 ]; then
	finish true 0 "$started" "$finished"
	exit 0
fi
finish false "$rc" "$started" "$finished"
exit 1
