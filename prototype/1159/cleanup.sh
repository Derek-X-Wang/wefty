#!/bin/bash
# Only this experiment's labels and executables. Safe after a partial run.
set -u
here=$(cd "$(dirname "$0")" && pwd -P)
bin="$here/bin/fixture"
domain="gui/$(id -u)"
temp=${1:-}
if [[ -z "$temp" && -f "$here/bin/temp-dir" ]]; then IFS= read -r temp < "$here/bin/temp-dir"; fi
if [[ ! -x "$bin" ]]; then
  echo "Build prototype/1159/bin/fixture first; cleanup needs its bounded-command mode." >&2
  exit 1
fi
status=0
listing=$("$bin" bounded --timeout 4s -- /bin/launchctl list 2>&1)
rc=$?
if [[ $rc -ne 0 ]]; then
  echo "cleanup: launchctl list failed ($rc): $listing" >&2
  status=1
else
  labels=$(printf '%s\n' "$listing" | awk '$3 ~ /^exp[.]wefty1159[.][A-Za-z0-9_.-]+$/ {print $3}')
  # Concurrent bounded bootouts prevent an unresponsive label from stretching
  # cleanup beyond the suite's wall-clock bound. Never glob installed plists.
  jobs=()
  while IFS= read -r label; do
    [[ -n "$label" ]] || continue
    "$bin" bounded --timeout 4s -- /bin/launchctl bootout "$domain/$label" &
    jobs+=("$!")
  done <<< "$labels"
  if [[ ${#jobs[@]} -gt 0 ]]; then
    for job in "${jobs[@]}"; do
      if ! wait "$job"; then echo 'cleanup: bootout failed or timed out; checking residue' >&2; status=1; fi
    done
  fi
fi
# Every experiment process uses this exact absolute executable path. Matching
# the start of command (not a substring in another process's arguments) avoids
# killing the driving shell, editor, or any installed wefty binary.
pids=$(ps -axo pid=,command= | awk -v prefix="$here/bin/" '{pid=$1; sub(/^[[:space:]]*[0-9]+[[:space:]]+/, ""); if (index($0,prefix)==1) print pid}')
while IFS= read -r pid; do
  [[ "$pid" =~ ^[0-9]+$ ]] || continue
  command=$(ps -p "$pid" -o command= 2>/dev/null || true)
  if [[ "$command" == "$here/bin/"* ]]; then kill -KILL "$pid" 2>/dev/null || true; fi
done <<< "$pids"
# Catch a KeepAlive restart racing with bootout before declaring cleanup done.
sleep 1
remaining=$("$bin" bounded --timeout 4s -- /bin/launchctl list 2>&1)
rc=$?
if [[ $rc -ne 0 ]]; then
  echo "cleanup: could not verify label removal: $remaining" >&2; status=1
elif printf '%s\n' "$remaining" | awk '$3 ~ /^exp[.]wefty1159[.]/ {found=1} END {exit !found}'; then
  echo 'cleanup: experiment labels remain loaded; rerun cleanup.sh' >&2; status=1
fi
remaining_pids=$(ps -axo pid=,command= | awk -v prefix="$here/bin/" '{pid=$1; sub(/^[[:space:]]*[0-9]+[[:space:]]+/, ""); if (index($0,prefix)==1) print pid}')
if [[ -n "$remaining_pids" ]]; then echo "cleanup: fixture processes remain: $remaining_pids" >&2; status=1; fi
if [[ -n "$temp" && -d "$temp" ]]; then
  physical=$(cd "$temp" && pwd -P)
  owner=''
  if [[ -f "$physical/.wefty1159-owner" ]]; then IFS= read -r owner < "$physical/.wefty1159-owner"; fi
  tmp_root=$(cd "${TMPDIR:-/tmp}" && pwd -P)
  case "$physical" in
    /private/tmp/wefty1159.*|/tmp/wefty1159.*|"$tmp_root"/wefty1159.*)
      if [[ "$owner" == "$here" ]]; then rm -rf -- "$physical"; else echo 'cleanup: refused unmarked temp directory' >&2; status=1; fi ;;
    *) echo "cleanup: refused unexpected temp path: $physical" >&2; status=1 ;;
  esac
fi
if [[ -f "$here/bin/temp-dir" ]]; then
  saved=''; IFS= read -r saved < "$here/bin/temp-dir"
  if [[ "$saved" == "$temp" && ! -d "$temp" ]]; then rm -f -- "$here/bin/temp-dir"; fi
fi
if [[ ${2:-} != --keep-lock ]]; then rmdir "$here/bin/run.lock" 2>/dev/null || true; fi
exit "$status"
