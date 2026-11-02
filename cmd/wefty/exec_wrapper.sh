#!/bin/bash
set -u
# Values below are shell-quoted by the CLI, never evaluated as shell source.
__WEFTY_EXEC_INPUT__
__WEFTY_EXEC_CLEANUP__
work="$storage/work"
# The liveness loop stops on these markers, not on a signal from this shell.
# A signal that reaches a new subshell while it still carries this shell's
# traps is caught and then lost, so the loop could run on for a whole period.
started="$storage/started"
finished="$storage/finished"
heartbeat=''
child=''
exec_stopped_processes=''
# The command can make storage unwritable. SIGKILL cannot be caught, so it
# still stops the liveness loop when the marker cannot be written.
stop_heartbeat() {
  { : > "$finished"; } 2>/dev/null || kill -KILL "$heartbeat" 2>/dev/null || :
  wait "$heartbeat" 2>/dev/null || :
  heartbeat=''
}
cleanup() {
  code=$?
  trap - EXIT INT TERM HUP
  if [ -n "$child" ]; then kill "$child" 2>/dev/null || :; wait "$child" 2>/dev/null || :; fi
  if [ -n "$heartbeat" ]; then stop_heartbeat; fi
  cd /tmp || :
  result_directory=''
  if [ "${command_started:-}" = 1 ]; then result_directory=${WEFTY_HANDOFF_DIR:-}; fi
  retire_exec_storage "$storage" capture "$result_directory" "$code"
  case $? in
    0) ;;
    3) printf 'wefty: warning: could not remove the temp link of %s\n' "$storage" >&2 ;;
    *) printf 'wefty: warning: snapshot cleanup failed at %s\n' "$storage" >&2 ;;
  esac
  exit "$code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
snapshot_checkout_failed() {
  reason=${1:-'snapshot checkout failed'}
  if [ ! -d "$storage/snapshot.git" ]; then
    reason='the exec snapshot was deleted when the run ended; run `wefty exec` again'
  fi
  printf 'wefty: %s\n' "$reason" >&2
  if [ -n "${WEFTY_HANDOFF_DIR:-}" ]; then
    printf '{"infrastructure_error":"%s"}\n' "$reason" > "$WEFTY_HANDOFF_DIR/result.json"
  fi
  exit 1
}
[ -d "$storage/snapshot.git" ] || snapshot_checkout_failed
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
  (umask 077; printf '%s\n' "$WEFTY_RUN_ID" > "$storage/run-id") 2>/dev/null || snapshot_checkout_failed
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
  git -C "$work" -c core.hooksPath=/dev/null -c core.autocrlf=false checkout --quiet --force HEAD || exit 1
  # Restore the caller's base and index, keeping the captured working files.
  git -C "$work" -c core.hooksPath=/dev/null -c core.autocrlf=false reset --quiet --mixed HEAD^
) || snapshot_checkout_failed
cd "$work/$folder" || snapshot_checkout_failed
# /var/tmp survives macOS temp purges; runs have no default time limit.
# Resolve roots once so neither home nor storage can contain the allocation.
physical_home=$(cd -- "$HOME" && pwd -P) || snapshot_checkout_failed "could not create the run's temp folder"
physical_storage=$(cd -- "$storage/.." && pwd -P) || snapshot_checkout_failed "could not create the run's temp folder"
tmp_root=$(cd -- /var/tmp 2>/dev/null && pwd -P) || tmp_root=''
case "$tmp_root/" in
  "${physical_home%/}/"*|"${physical_storage%/}/"*) tmp_root='' ;;
esac
if [ -z "$tmp_root" ] || [ ! -w "$tmp_root" ]; then
  tmp_root=$(cd -- /tmp && pwd -P) || snapshot_checkout_failed "could not create the run's temp folder"
fi
case "$tmp_root/" in
  "${physical_home%/}/"*|"${physical_storage%/}/"*) snapshot_checkout_failed "could not create the run's temp folder" ;;
esac
tmp_dir=''
for ((tries=0; tries<10; tries++)); do
  candidate=$(mktemp -u "$tmp_root/wefty-run-data.XXXXXXXX") || break
  (umask 077; printf '%s\n' "$candidate" > "$storage/tmp-dir") || break
  if mkdir -m 700 -- "$candidate"; then
    tmp_dir=$candidate
    break
  fi
done
[ -n "$tmp_dir" ] || snapshot_checkout_failed "could not create the run's temp folder"
(umask 077; printf '%s\n' "$storage" > "$tmp_dir/owner.tmp" && mv -f -- "$tmp_dir/owner.tmp" "$tmp_dir/owner") || snapshot_checkout_failed "could not create the run's temp folder"
mkdir -m 700 -- "$tmp_dir/tmp" || snapshot_checkout_failed "could not create the run's temp folder"
# Record each candidate before linking so retirement also covers setup failure.
tmp_link=''
for ((tries=0; tries<10; tries++)); do
  candidate=$(mktemp -u /tmp/wefty-run.XXXXXXXX) || break
  (umask 077; printf '%s\n' "$candidate" > "$storage/tmp-link") || break
  [ ! -e "$candidate" ] && [ ! -L "$candidate" ] || continue
  # If a folder appeared at the name, ln would link inside it; check the result.
  if ln -s -- "$tmp_dir/tmp" "$candidate" && [ -L "$candidate" ] && [ "$(readlink "$candidate")" = "$tmp_dir/tmp" ]; then
    tmp_link=$candidate
    break
  fi
done
[ -n "$tmp_link" ] || snapshot_checkout_failed "could not create the run's temp folder"
: > "$started" || snapshot_checkout_failed
(
  export TMPDIR="$tmp_link"
  for name in ${!WEFTY_@}; do unset "$name"; done
  exec -- "${command[@]}"
) &
child=$!
command_started=1
wait "$child"
code=$?
child=''
# Stop liveness output before result publication or filesystem cleanup.
stop_heartbeat
# Publish the command verdict before cleanup; cleanup appends sweep evidence.
if [ -n "${WEFTY_HANDOFF_DIR:-}" ]; then
  printf '{"exit_code":%s}\n' "$code" > "$WEFTY_HANDOFF_DIR/result.json"
fi
exit "$code"
