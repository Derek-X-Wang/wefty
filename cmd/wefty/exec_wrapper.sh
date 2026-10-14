#!/bin/bash
set -u
# Values below are shell-quoted by the CLI, never evaluated as shell source.
__WEFTY_EXEC_INPUT__
work="$storage/work"
heartbeat=''
preparing=''
child=''
cleanup() {
  code=$?
  trap - EXIT INT TERM HUP
  if [ -n "$preparing" ]; then kill "$preparing" 2>/dev/null || :; wait "$preparing" 2>/dev/null || :; fi
  if [ -n "$heartbeat" ]; then kill "$heartbeat" 2>/dev/null || :; wait "$heartbeat" 2>/dev/null || :; fi
  if [ -n "$child" ]; then kill "$child" 2>/dev/null || :; wait "$child" 2>/dev/null || :; fi
  cd /tmp || :
  # Bound removal even if a filesystem stalls. The CLI also removes storage
  # after observing a terminal run, including a cancelled queued run.
  (
    operation=''
    trap 'if [ -n "$operation" ]; then kill "$operation" 2>/dev/null || :; wait "$operation" 2>/dev/null || :; fi; exit 1' TERM INT HUP
    /bin/chmod -R u+w "$storage" & operation=$!
    wait "$operation" || exit 1
    /bin/rm -rf -- "$storage" & operation=$!
    wait "$operation"
  ) &
  remover=$!
  (
    sleeper=''
    trap 'if [ -n "$sleeper" ]; then kill "$sleeper" 2>/dev/null || :; wait "$sleeper" 2>/dev/null || :; fi; exit' TERM INT HUP
    sleep 10 & sleeper=$!
    wait "$sleeper" || exit
    sleeper=''
    kill "$remover" 2>/dev/null || :
  ) &
  watchdog=$!
  wait "$remover" || {
    [ ! -e "$storage" ] || printf 'wefty: warning: snapshot cleanup failed at %s\n' "$storage" >&2
  }
  kill "$watchdog" 2>/dev/null || :
  wait "$watchdog" 2>/dev/null || :
  exit "$code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
snapshot_checkout_failed() {
  printf 'wefty: snapshot checkout failed\n' >&2
  if [ -n "${WEFTY_HANDOFF_DIR:-}" ]; then
    printf '{"infrastructure_error":"snapshot checkout failed"}\n' > "$WEFTY_HANDOFF_DIR/result.json"
  fi
  exit 1
}
# A large snapshot can take a while to clone. Report liveness from the start,
# so the no-output limit measures the command, not the checkout.
(
  sleeper=''
  trap 'if [ -n "$sleeper" ]; then kill "$sleeper" 2>/dev/null || :; wait "$sleeper" 2>/dev/null || :; fi; exit' TERM INT HUP
  minutes=0
  while :; do
    sleep "$heartbeat_seconds" & sleeper=$!
    wait "$sleeper" || exit
    sleeper=''
    minutes=$((minutes+1))
    printf 'still preparing the snapshot (%sm)\n' "$minutes" >&2
  done
) &
preparing=$!
# Acceptance can precede the CLI's marker write, or its reply can be lost.
# Record ownership before cloning changes the capture directory's age.
if [ -n "${WEFTY_RUN_ID:-}" ]; then
  (umask 077; printf '%s\n' "$WEFTY_RUN_ID" > "$storage/run-id") || snapshot_checkout_failed
fi
# --no-local prevents hard links or alternates back to the capture.
(
  for name in ${!GIT_@}; do unset "$name"; done
  export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
  git -c core.hooksPath=/dev/null -c core.autocrlf=false clone --template= --quiet --no-local --no-checkout -- "$storage/snapshot.git" "$work" || exit 1
  # Override checkout transforms so ident, EOL and encoding attributes cannot
  # change the exact working bytes frozen by --no-filters during capture.
  mkdir -p "$work/.git/info" || exit 1
  printf '* -filter -ident -text -eol -working-tree-encoding\n' > "$work/.git/info/attributes" || exit 1
  git -C "$work" -c core.hooksPath=/dev/null -c core.autocrlf=false checkout --quiet --force HEAD
) || snapshot_checkout_failed
cd "$work/$folder" || snapshot_checkout_failed
kill "$preparing" 2>/dev/null || :
wait "$preparing" 2>/dev/null || :
preparing=''
(
  for name in ${!WEFTY_@}; do unset "$name"; done
  exec -- "${command[@]}"
) &
child=$!
(
  # Reap each sleep too: stopping the heartbeat must not leave an output pipe
  # open for a minute after the command has already finished.
  sleeper=''
  trap 'if [ -n "$sleeper" ]; then kill "$sleeper" 2>/dev/null || :; wait "$sleeper" 2>/dev/null || :; fi; exit' TERM INT HUP
  minutes=0
  while :; do
    sleep "$heartbeat_seconds" & sleeper=$!
    wait "$sleeper" || exit
    sleeper=''
    # The child can end before the wrapper is scheduled to reap this timer.
    kill -0 "$child" 2>/dev/null || exit
    minutes=$((minutes+1))
    printf 'still running (%sm)\n' "$minutes" >&2
  done
) &
heartbeat=$!
wait "$child"
code=$?
child=''
# Stop liveness output before result publication or filesystem cleanup.
kill "$heartbeat" 2>/dev/null || :
wait "$heartbeat" 2>/dev/null || :
heartbeat=''
# The wrapper keeps mailbox context; only the command loses WEFTY_*.
if [ -n "${WEFTY_HANDOFF_DIR:-}" ]; then
  printf '{"exit_code":%s}\n' "$code" > "$WEFTY_HANDOFF_DIR/result.json"
fi
exit "$code"
