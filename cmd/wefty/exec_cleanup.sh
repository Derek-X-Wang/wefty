# Inventory is NUL-delimited so command names and working folders remain data.
exec_storage_process_inventory() {
  cd / || return 1
  local uid=$1 entry key value rest pid owner name cwd
  case "$(uname -s)" in
    Darwin) exec lsof -nP -a -u "$uid" -d cwd -Fpcn0 ;;
    Linux)
      [ -d /proc/self ] || return 1
      for entry in /proc/[0-9]*; do
        [ -r "$entry/status" ] || continue
        owner='' name=''
        while IFS=$'\t' read -r key value rest; do
          case "$key" in
            Uid:) owner=$value ;;
            Name:) name=$value ;;
          esac
        done < "$entry/status" 2>/dev/null || continue
        [ "$owner" = "$uid" ] || continue
        cwd=$(readlink -- "$entry/cwd" 2>/dev/null) || continue
        pid=${entry##*/}
        printf 'p%s\0c%s\0n%s\0' "$pid" "$name" "$cwd"
      done
      ;;
    *) return 1 ;;
  esac
}

# Set exec_process_json to a JSON string, including uncommon control bytes.
exec_storage_json_string() {
  local value=$1 escaped='' char code i
  local LC_ALL=C
  for ((i=0; i<${#value}; i++)); do
    char=${value:i:1}
    case "$char" in
      '"') escaped+='\"' ;;
      \\) escaped+="\\\\" ;;
      *) printf -v code '%d' "'$char"
         if [ "$code" -ge 0 ] && [ "$code" -lt 32 ]; then
           printf -v char '\\u%04x' "$code"
         fi
         escaped+=$char ;;
    esac
  done
  exec_process_json="\"$escaped\""
}

# This runs before retirement, with stderr still connected to the run log.
# One inventory, bounded to two seconds; no discovery retry. Helpers use /
# as cwd so the sweep cannot select its own inventory or deadline worker.
sweep_exec_storage_processes() {
  exec_stopped_processes=''
  local source=$1 inventory status field pid='' name='' cwd=''
  local scan watchdog uid protected i index warning=0
  local pids=() names=() folders=() stopped=()
  [ -d "$source" ] && [ ! -L "$source" ] || return 0
  source=${source%/}
  source=$(cd -P -- "${source%/*}" && printf '%s/%s' "$PWD" "${source##*/}") || {
    printf 'wefty: warning: could not scan processes in run storage\n' >&2
    return 0
  }
  inventory=$(mktemp -d /tmp/wefty-sweep.XXXXXXXX) || {
    printf 'wefty: warning: could not scan processes in run storage\n' >&2
    return 0
  }
  uid=$(id -u)
  exec_storage_process_inventory "$uid" > "$inventory/processes" 2>/dev/null &
  scan=$!
  (
    cd / || exit 1
    sleep 2
    : > "$inventory/timeout"
    kill -KILL "$scan" 2>/dev/null || :
  ) </dev/null >/dev/null 2>&1 &
  watchdog=$!
  wait "$scan" 2>/dev/null
  status=$?
  kill "$watchdog" 2>/dev/null || :
  wait "$watchdog" 2>/dev/null || :
  if [ "$status" -ne 0 ] || [ -e "$inventory/timeout" ]; then
    printf 'wefty: warning: could not scan processes in run storage\n' >&2
    /bin/rm -rf -- "$inventory"
    return 0
  fi
  index=-1
  while IFS= read -r -d '' field; do
    # lsof terminates each process/file record with a newline after its NUL.
    field=${field#$'\n'}
    case "$field" in
      p*) index=$((index+1)); pids[index]=${field#p}; names[index]=''; folders[index]='' ;;
      c*) names[index]=${field#c} ;;
      n*) folders[index]=${field#n} ;;
    esac
  done < "$inventory/processes"
  /bin/rm -rf -- "$inventory"
  protected=" $$ $PPID "
  for ((i=0; i<${#pids[@]}; i++)); do
    pid=${pids[i]} name=${names[i]} cwd=${folders[i]}
    case "$pid" in ''|*[!0-9]*|0|1) continue ;; esac
    case "$protected" in *" $pid "*) continue ;; esac
    [ "$name" != wefty-agent ] || continue
    # Both discovery methods return physical cwd paths. pwd -P above resolves
    # the storage alias, including /var, /tmp and the run's short TMPDIR link.
    case "$cwd" in "$source"|"$source"/*) ;; *) continue ;; esac
    if kill -TERM "$pid" 2>/dev/null; then stopped[${#stopped[@]}]=$i
    elif kill -0 "$pid" 2>/dev/null; then warning=1
    fi
  done
  if [ "${#stopped[@]}" -gt 0 ]; then
    sleep 0.2
    for index in "${stopped[@]}"; do
      pid=${pids[index]} name=${names[index]} cwd=${folders[index]}
      if kill -0 "$pid" 2>/dev/null && ! kill -KILL "$pid" 2>/dev/null; then
        if kill -0 "$pid" 2>/dev/null; then warning=1; continue; fi
      fi
      # %q keeps the log to one line even for an unusual process name.
      printf 'wefty: stopped process %s (%q) left in run storage\n' "$pid" "$name" >&2
      [ -z "$exec_stopped_processes" ] || exec_stopped_processes+=','
      exec_storage_json_string "$name"
      exec_stopped_processes+="{\"pid\":$pid,\"name\":$exec_process_json,"
      exec_storage_json_string "$cwd"
      exec_stopped_processes+="\"cwd\":$exec_process_json}"
    done
    # Give a killed descendant time to release its cwd before storage moves.
    sleep 0.1
  fi
  if [ "$warning" -ne 0 ]; then
    printf 'wefty: warning: could not stop a process in run storage\n' >&2
  fi
  return 0
}

# Shared by the wrapper and observing/sweeping CLI. The caller only waits
# for atomic retirement; the helper owns deletion for as long as it takes.
retire_exec_storage() {
  local source=$1 kind=$2 trash operation polls=0
  sweep_exec_storage_processes "$source"
  # The wrapper passes its result folder and verdict. Publish before a slow
  # rename can outlast the agent's TERM grace; CLI stale sweeps pass neither.
  if [ -n "${3:-}" ] && [ -n "$exec_stopped_processes" ]; then
    printf '{"exit_code":%s,"stopped_processes":[%s]}\n' "$4" "$exec_stopped_processes" > "$3/result.json"
  fi
  if [ "$kind" = trash ]; then
    trash=$source
  else
    trash="${source%/*}/.wefty-trash-${source##*/}-$$"
  fi
  # For restartable exec, source is beneath an Attempt-specific mailbox. A
  # delayed helper must never receive a successor's storage path.
  # The process runner reaps the wrapper's process group at attempt end.
  # Job control gives this helper a separate group, also on macOS Bash 3.2
  # where setsid is unavailable. No inherited output pipe may keep a run open.
  # When the kernel refuses the new group, Bash reports it from the child
  # before the redirections below apply. Keep that line out of the run's
  # stderr (#1204); fd 9 holds the caller's stderr until the fork is done.
  exec 9>&2 2>/dev/null
  set -m
  (
    trap - EXIT INT TERM
    trap '' HUP
    link_failure=0
    # Keep every filesystem operation off the foreground path: even a stat
    # can stall. Never traverse a symlink, including discovered trash.
    [ -e "$source" ] || [ -L "$source" ] || exit 0
    [ -d "$source" ] && [ ! -L "$source" ] || exit 1
    if [ "$kind" != trash ]; then
      [ ! -e "$trash" ] && [ ! -L "$trash" ] || exit 1
      # macOS 15 refuses to rename a directory its owner cannot write, and a
      # command may lock its own storage. Only the top directory, so this
      # stays instant; deletion unlocks the tree.
      /bin/chmod u+w "$source" 2>/dev/null || :
      # Only unlink our short alias. Never follow it or remove another run's
      # link, even when the command has replaced the record or the path.
      # A link that is already gone is fine: the wrapper and the CLI can
      # retire the same capture at once. Only a link that stays is status 3.
      if [ -f "$source/tmp-link" ] && [ ! -L "$source/tmp-link" ]; then
        tmp_link=''
        # A record without a final newline still sets the value.
        IFS= read -r tmp_link < "$source/tmp-link" || :
        case "$tmp_link" in
          /tmp/wefty-run.?*)
            suffix=${tmp_link#/tmp/wefty-run.}
            case "$suffix" in
              *[!a-zA-Z0-9._-]*) ;;
              *)
                if [ -L "$tmp_link" ] && [ "$(readlink "$tmp_link")" = "$source/tmp" ]; then
                  /bin/rm -f -- "$tmp_link" || [ ! -L "$tmp_link" ] || link_failure=3
                fi
                ;;
            esac
            ;;
        esac
      fi
      /bin/mv -- "$source" "$trash" || exit 1
    fi
    # This child keeps the helper's independent lifetime. Exiting the
    # retirement helper acknowledges the rename, not recursive deletion.
    set -m
    /bin/bash -c '
      trash=$1
      root=${trash%/*}
      name=".wefty-delete-${trash##*/}"
      marker="$root/$name"
      # Publish an already populated directory, so a killed initializer
      # cannot leave an ownerless lock. Nonempty directories cannot replace
      # one another in a racing rename. All probes stay in this helper.
      if [ -e "$marker" ] || [ -L "$marker" ]; then
        [ -d "$marker" ] && [ ! -L "$marker" ] || exit 0
        for owner in "$marker"/*; do
          [ -e "$owner" ] || [ -L "$owner" ] || continue
          pid=${owner##*/}
          case "$pid" in ""|*[!0-9]*) exit 0 ;; esac
          [ ! -L "$owner" ] || exit 0
          # The helper leads its own process group. Keep the guard while
          # either it or a surviving chmod/rm child is still alive.
          kill -0 -- "-$pid" 2>/dev/null && exit 0
          /bin/rmdir -- "$owner" || exit 0
        done
        /bin/rmdir -- "$marker" || exit 0
      fi
      pending=$(mktemp -d "$root/.wefty-delete-pending-XXXXXX") || exit 1
      /bin/mkdir -- "$pending/$name" "$pending/$name/$$" || exit 1
      /bin/mv -n -- "$pending/$name" "$root/"
      if [ -d "$pending/$name" ]; then
        /bin/rmdir -- "$pending/$name/$$" "$pending/$name" "$pending"
        exit 0
      fi
      /bin/rmdir -- "$pending"
      [ ! -L "$marker" ] && [ -d "$marker/$$" ] || exit 1
      # Do not release from a signal trap: a child may still be deleting.
      # A command can leave a folder its owner cannot read or enter. Open
      # each folder before it is read, and delete even when chmod reports
      # an error, so one odd entry cannot keep the whole copy (#1040).
      # X adds search to folders but not to plain files: a hard link shares
      # its mode with a file outside the run, such as a package store entry.
      if [ -d "$trash" ] && [ ! -L "$trash" ]; then
        /bin/chmod -R u+rwX "$trash"
        /bin/rm -rf -- "$trash"
      fi
      /bin/rmdir -- "$marker/$$" && /bin/rmdir -- "$marker"
    ' wefty-exec-delete "$trash" &
    exit "$link_failure"
  ) </dev/null >/dev/null 2>&1 9>&- &
  operation=$!
  set +m
  exec 2>&9 9>&-
  [ "$kind" != trash ] || return 0
  # A stalled rename must not keep a run open. Do not kill the helper when
  # this budget expires: it may still retire and delete the capture later.
  while kill -0 "$operation" 2>/dev/null; do
    [ "$polls" -lt 100 ] || return 1
    sleep 0.1
    polls=$((polls+1))
  done
  wait "$operation"
}
