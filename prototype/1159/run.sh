#!/bin/bash
# macOS bash 3.2 compatible. Never installs a plist in LaunchAgents.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd -P)
repo=$(cd "$here/../.." && pwd -P)
bin="$here/bin/fixture"
domain="gui/$(id -u)"

launch() { "$bin" bounded --timeout 8s -- /bin/launchctl "$@"; }
sample() {
  printf '\n--- sample utc=%s elapsed=%ss ---\n' "$(date -u +%FT%TZ)" "$SECONDS"
  local pids='' gen controller payload pgid
  for gen in old new; do
    if [[ -f "$caseout/$gen/ready" ]]; then
      read -r controller payload pgid < "$caseout/$gen/ready"
      pids="${pids:+$pids,}$controller,$payload"
      printf '%s original_controller=%s payload=%s payload_pgid=%s output_lines=' "$gen" "$controller" "$payload" "$pgid"
      wc -l < "$caseout/$gen/output.log"
      tail -n 1 "$caseout/$gen/output.log" || true
    fi
  done
  if [[ -n "$pids" ]]; then ps -p "$pids" -o pid,pgid,ppid,stat,command || true; fi
  # Also show KeepAlive restarts, including their distinct controller PIDs.
  ps -axo pid=,pgid=,ppid=,stat=,command= | awk -v binary="$bin" -v path="$caseout/" '{command=$0; for(i=0;i<4;i++) sub(/^[[:space:]]*[^[:space:]]+[[:space:]]+/, "", command); if(index(command,binary " ")==1 && index(command,path)>0) print}'
}
ready() {
  local gen=$1 start=$SECONDS
  until [[ -s "$caseout/$gen/ready" && -s "$caseout/$gen/output.log" ]]; do
    if (( SECONDS - start >= 6 )); then echo "ERROR: $gen did not become ready"; return 1; fi
    sleep 0.2
  done
}
makegen() {
  local gen=$1 duration=$2
  mkdir -p "$caseout/$gen"
  local label="exp.wefty1159.$casename.$gen"
  "$bin" plist --label "$label" --dir "$caseout/$gen" --term "$term" --group "$group" --output "$output_mode" --seconds "$duration" --abandon="$abandon" > "$casetmp/$gen.plist"
  /usr/bin/plutil -lint "$casetmp/$gen.plist"
  # Preserve the exact plist even though cleanup removes the temporary one.
  cp "$casetmp/$gen.plist" "$caseout/$gen.plist"
  launch bootstrap "$domain" "$casetmp/$gen.plist"
  ready "$gen"
}
reportgen() {
  local gen=$1 controller payload pgid f orphan_ppids=''
  [[ -f "$caseout/$gen/ready" ]] || return 0
  read -r controller payload pgid < "$caseout/$gen/ready"
  printf '\n=== %s outcome: controller=%s payload=%s pgid=%s ===\n' "$gen" "$controller" "$payload" "$pgid"
  if grep -q '"event":"payload_wait"' "$caseout/$gen/controller-$controller.jsonl"; then
    echo 'parent_wait=RECORDED (exact exit code or signal follows)'
    grep '"event":"payload_wait"' "$caseout/$gen/controller-$controller.jsonl"
  elif ! ps -p "$controller" -o pid= >/dev/null 2>&1; then
    echo 'parent_wait=UNAVAILABLE: original controller died; payload lost its wait owner.'
    echo 'payload_final_exit_status=UNKNOWN (exit_intent is not a wait result).'
  else
    echo 'parent_wait=NOT-YET-RECORDED: controller still exists; do not infer payload completion.'
  fi
  awk 'BEGIN {expected=1; gaps=0} /^counter=/ {split($1,a,"="); if(a[2]!=expected) gaps++; expected=a[2]+1; n++} END {printf "controller_output_ticks=%d counter_sequence_gaps=%d\n",n,gaps}' "$caseout/$gen/output.log"
  echo 'Observed independent witness PPIDs:'
  if [[ -f "$caseout/$gen/witness.jsonl" ]]; then
    sed -n 's/.*"ppid":\([0-9]*\).*/\1/p' "$caseout/$gen/witness.jsonl" | sort -nu
    orphan_ppids=$(sed -n 's/.*"ppid":\([0-9]*\).*/\1/p' "$caseout/$gen/witness.jsonl" | awk -v parent="$controller" '$1 != parent' | sort -nu)
  fi
  if [[ -n "$orphan_ppids" ]]; then
    printf 'orphaned=OBSERVED: payload witness recorded a new PPID: %s\n' "$orphan_ppids"
  else
    echo 'orphaned=NOT-OBSERVED in witness; inspect ps samples (rapid death may hide the interval).'
  fi
  for f in "$caseout/$gen"/*; do
    [[ -f "$f" ]] || continue
    printf '\n--- artifact %s ---\n' "$(basename "$f")"
    cat "$f"
  done
}

if [[ ${1:-} == --case ]]; then
  # Parent runs this entire case under a 65s deadline. Cleanup runs after the
  # bounded helper exits so cleanup cannot kill the helper supervising itself.
  casename=$2; caseout=$3; casetmp=$4; scenario=$5; abandon=$6; group=$7
  term=forward
  output_mode=pipe
  if [[ "$group" == shared ]]; then output_mode='file'; fi
  if [[ "$scenario" == bootout-leave ]]; then term=leave; fi
  SECONDS=0
  printf 'case=%s scenario=%s AbandonProcessGroup=%s payload_group=%s\n' "$casename" "$scenario" "$abandon" "$group"
  echo 'Current node keys: RunAtLoad=true KeepAlive.SuccessfulExit=false ThrottleInterval=10 ExitTimeOut=95 ProcessType=Interactive'
  echo 'Payload duration=24s chosen_exit=17; TERM forwarding drains for 15s then sends group TERM (5s KILL grace).'
  printf 'Output mode=%s; shared+file is a diagnostic control, not runner/process behavior.\n' "$output_mode"
  echo 'Witness is independent diagnostic evidence, never a recovered parent wait status.'
  makegen old 24
  observe_until=$((SECONDS + 30))
  sample
  sleep 3
  sample
  read -r controller payload pgid < "$caseout/old/ready"
  printf '\nACTION utc=%s elapsed=%ss\n' "$(date -u +%FT%TZ)" "$SECONDS"
  case "$scenario" in
    bootout-forward|bootout-leave)
      echo "launchctl bootout $domain/exp.wefty1159.$casename.old"
      if launch bootout "$domain/exp.wefty1159.$casename.old"; then echo 'bootout_return=0'; else echo "bootout_return=$? (bounded; removal may still be in progress)"; fi ;;
    retire)
      echo 'Dedicated retirement: SIGUSR1 marks old retiring without a shutdown timer; bootstrap second label.'
      kill -USR1 "$controller"
      makegen new 12 ;;
    sigkill)
      echo "SIGKILL original controller $controller; leave label loaded to observe current KeepAlive keys."
      kill -KILL "$controller" ;;
    *) echo 'unknown scenario'; exit 2 ;;
  esac
  sample
  # Fixed observation interval includes payload completion and a throttled
  # controller restart. All ps and output reads refer to the original payload.
  while (( SECONDS < observe_until )); do sleep 1; sample; done
  printf '\n=== final launchd state (before cleanup) ===\n'
  for gen in old new; do
    if [[ -f "$caseout/$gen/ready" ]]; then
      "$bin" bounded --timeout 3s -- /bin/launchctl print "$domain/exp.wefty1159.$casename.$gen" || true
    fi
  done
  reportgen old
  reportgen new
  echo 'Observation complete. Any later signals belong to cleanup, outside this results file.'
  exit 0
fi

if [[ $# -ne 1 ]]; then echo 'Usage: bash prototype/1159/run.sh <outdir>' >&2; exit 2; fi
if [[ $(uname -s) != Darwin ]]; then echo 'This experiment requires macOS and a GUI login domain.' >&2; exit 2; fi
if [[ ! -x "$bin" ]]; then echo 'Build fixture first; see README.md.' >&2; exit 2; fi
# Require an existing parent so we can resolve symlinks before making any write.
# The output can live only in this checkout or the OS temporary directory.
if [[ -e "$1" ]]; then out=$(cd "$1" && pwd -P); else out="$(cd "$(dirname "$1")" && pwd -P)/$(basename "$1")"; fi
tmp_root=$(cd "${TMPDIR:-/tmp}" && pwd -P)
case "$out" in
  "$repo"/*|/private/tmp/*|/tmp/*|"$tmp_root"/*) ;;
  *) echo 'Output directory must be inside this checkout or the OS temp directory.' >&2; exit 2 ;;
esac
mkdir -p "$out"
if [[ -n $(ls -A "$out") ]]; then echo 'Use an empty output directory to keep experiment evidence separate.' >&2; exit 2; fi
# A lock prevents this namespace-wide cleanup from disrupting another copy of
# this fixture. Standalone cleanup can clear a stale lock after interruption.
if ! mkdir "$here/bin/run.lock" 2>/dev/null; then echo 'Fixture lock exists; run cleanup.sh after confirming no other fixture run is active.' >&2; exit 2; fi
temp=''
# shellcheck disable=SC2329 # Invoked by the EXIT trap below.
finish() {
  local rc=$?
  trap - EXIT INT TERM
  if ! bash "$here/cleanup.sh" "$temp" >> "$out/cleanup.log" 2>&1; then rc=1; fi
  rmdir "$here/bin/run.lock" 2>/dev/null || true
  exit "$rc"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
temp=$(mktemp -d "${TMPDIR:-/tmp}/wefty1159.XXXXXX")
printf '%s\n' "$here" > "$temp/.wefty1159-owner"
printf '%s\n' "$temp" > "$here/bin/temp-dir"
{
  date -u +%FT%TZ
  sw_vers
  uname -a
  printf 'domain=%s\nfixture=%s\noutput=%s\ntemp=%s\n' "$domain" "$here" "$out" "$temp"
  echo 'No installed wefty label or plist is used.'
} > "$out/environment.txt"
SECONDS=0
status=0
for group in separate shared; do
  for scenario in bootout-forward bootout-leave retire sigkill; do
    # Shared-group + direct-file output removes broken-pipe death as a cause,
    # so AbandonProcessGroup's effect is visible in this diagnostic control.
    if [[ "$group" == shared && "$scenario" != sigkill ]]; then continue; fi
    for abandon in false true; do
      if (( SECONDS >= 480 )); then echo 'Suite deadline reached; remaining cases NOT-RUN.' >&2; exit 1; fi
      policy=default; if [[ "$abandon" == true ]]; then policy=abandon; fi
      casename="$scenario-$group-$policy"
      caseout="$out/$casename"
      casetmp=$(mktemp -d "$temp/wefty1159.XXXXXX")
      printf '%s\n' "$here" > "$casetmp/.wefty1159-owner"
      mkdir -p "$caseout"
      echo "Running $casename (about 30 seconds)"
      if "$bin" bounded --timeout 65s -- /bin/bash "$here/run.sh" --case "$casename" "$caseout" "$casetmp" "$scenario" "$abandon" "$group" > "$out/$casename.results.txt" 2>&1; then
        printf '\ncase_driver_exit=0\n' >> "$out/$casename.results.txt"
      else
        rc=$?; printf '\ncase_driver_exit=%s CASE INCOMPLETE; see evidence above.\n' "$rc" >> "$out/$casename.results.txt"; status=1
      fi
      if ! bash "$here/cleanup.sh" "$casetmp" --keep-lock >> "$out/cleanup.log" 2>&1; then
        echo 'Cleanup could not confirm isolation; stopping suite.' >&2; exit 1
      fi
    done
  done
done
printf 'Results: %s (elapsed=%ss)\n' "$out" "$SECONDS"
exit "$status"
