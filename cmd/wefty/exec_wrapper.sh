#!/bin/bash
set -u
# Values below are shell-quoted by the CLI, never evaluated as shell source.
__WEFTY_EXEC_INPUT__
work="$storage/work"
# The liveness loop stops on these markers, not on a signal from this shell.
# A signal that reaches a new subshell while it still carries this shell's
# traps is caught and then lost, so the loop could run on for a whole period.
started="$storage/started"
finished="$storage/finished"
heartbeat=''
child=''
removal_polls=0
# The command can make storage unwritable. SIGKILL cannot be caught, so it
# still stops the liveness loop when the marker cannot be written.
stop_heartbeat() {
  { : > "$finished"; } 2>/dev/null || kill -KILL "$heartbeat" 2>/dev/null || :
  wait "$heartbeat" 2>/dev/null || :
  heartbeat=''
}
# Run one removal step within the shared ten-second budget, so a stalled
# filesystem cannot keep the run open. The step itself is tracked and killed.
bounded_removal() {
  "$@" </dev/null >/dev/null 2>&1 &
  operation=$!
  while kill -0 "$operation" 2>/dev/null && [ "$removal_polls" -lt 100 ]; do
    sleep 0.1
    removal_polls=$((removal_polls+1))
  done
  kill -KILL "$operation" 2>/dev/null || :
  wait "$operation"
}
cleanup() {
  code=$?
  trap - EXIT INT TERM HUP
  if [ -n "$child" ]; then kill "$child" 2>/dev/null || :; wait "$child" 2>/dev/null || :; fi
  if [ -n "$heartbeat" ]; then stop_heartbeat; fi
  cd /tmp || :
  # Bound removal even if a filesystem stalls. The CLI also removes storage
  # after observing a terminal run, including a cancelled queued run.
  bounded_removal /bin/chmod -R u+w "$storage" && bounded_removal /bin/rm -rf -- "$storage"
  [ ! -e "$storage" ] || printf 'wefty: warning: snapshot cleanup failed at %s\n' "$storage" >&2
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
# Report liveness from the start, so the no-output limit measures the
# command, not the checkout of a large snapshot. The loop ends by itself
# within one tick of the finished marker, or when storage is gone.
(
  phase=preparing
  ticks=0
  minutes=0
  while :; do
    sleep "$heartbeat_tick" </dev/null >/dev/null 2>&1 || exit 0
    if [ -e "$finished" ] || [ ! -d "$storage" ]; then exit 0; fi
    if [ "$phase" = preparing ] && [ -e "$started" ]; then
      phase=running
      ticks=0
      minutes=0
    fi
    ticks=$((ticks+1))
    [ "$ticks" -ge "$heartbeat_ticks" ] || continue
    ticks=0
    minutes=$((minutes+1))
    if [ "$phase" = preparing ]; then
      printf 'still preparing the snapshot (%sm)\n' "$minutes" >&2
    else
      printf 'still running (%sm)\n' "$minutes" >&2
    fi
  done
) &
heartbeat=$!
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
: > "$started" || snapshot_checkout_failed
(
  for name in ${!WEFTY_@}; do unset "$name"; done
  exec -- "${command[@]}"
) &
child=$!
wait "$child"
code=$?
child=''
# Stop liveness output before result publication or filesystem cleanup.
stop_heartbeat
# The wrapper keeps mailbox context; only the command loses WEFTY_*.
if [ -n "${WEFTY_HANDOFF_DIR:-}" ]; then
  printf '{"exit_code":%s}\n' "$code" > "$WEFTY_HANDOFF_DIR/result.json"
fi
exit "$code"
