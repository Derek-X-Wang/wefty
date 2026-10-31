# Newline-delimited inventory; cwd containing a newline or tab is not supported.
exec_storage_process_inventory() {
  cd / || return 1
  case "$1" in
    Darwin) exec lsof -nP -a -u "$2" -d cwd -Fpcn ;;
    Linux)
      # One find process, without a shell read/status/readlink loop per PID.
      # Symlink ownership is the process uid; %l reads the physical cwd.
      exec find /proc/[0-9]*/cwd -maxdepth 0 -user "$2" \
        ! -lname $'*\n*' ! -lname $'*\t*' -printf 'p%h\nn%l\n'
      ;;
    *) return 1 ;;
  esac
}

# Only the scanner owns the captured pipe. The watchdog has no inherited pipe,
# so cancelling its sleep cannot hold command substitution open. Both helpers
# use / as cwd; there is no scratch directory to delete before retirement.
exec_storage_bounded_inventory() {
  cd / || return 1
  local scan watchdog status deadline=${3:-1}
  exec_storage_process_inventory "$1" "$2" 2>/dev/null &
  scan=$!
  ( sleep "$deadline"; kill -KILL "$scan" 2>/dev/null || : ) </dev/null >/dev/null 2>&1 &
  watchdog=$!
  wait "$scan" 2>/dev/null
  status=$?
  kill "$watchdog" 2>/dev/null || :
  wait "$watchdog" 2>/dev/null || :
  return "$status"
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

# Populate the caller's pids/names/folders and mark storage matches. Retain
# nonmatching pids too: a changed cwd or name does not prove the process stopped.
exec_storage_parse_inventory() {
  local inventory=$1 platform=$2 source=$3 protected=" $$ $PPID "
  local field pid name cwd i index
  pids=() names=() folders=() matches=()
  index=-1
  while IFS= read -r field; do
    case "$field" in
      p*) pid=${field#p}; pid=${pid#/proc/}
          case "$pid" in ''|*[!0-9]*) index=-1; continue ;; esac
          index=${#pids[@]}; pids[index]=$pid; names[index]=''; folders[index]=''; matches[index]=0 ;;
      c*) [ "$index" -lt 0 ] || names[index]=${field#c} ;;
      n*) [ "$index" -lt 0 ] || folders[index]=${field#n} ;;
    esac
  done <<< "$inventory"
  for ((i=0; i<${#pids[@]}; i++)); do
    pid=${pids[i]} name=${names[i]} cwd=${folders[i]}
    case "$pid" in ''|*[!0-9]*|0|1) continue ;; esac
    case "$protected" in *" $pid "*) continue ;; esac
    # Both discovery methods return physical cwd paths. pwd -P above resolves
    # the storage alias, including /var, /tmp and the run's short TMPDIR link.
    case "$cwd" in *$'\t'*|*$'\n'*) continue ;; esac
    case "$cwd" in "$source"|"$source"/*) ;; *) continue ;; esac
    if [ "$platform" = Linux ]; then
      # Read a command name only for a matching cwd, not for every process.
      IFS= read -r name < "/proc/$pid/comm" 2>/dev/null || continue
      names[i]=$name
    fi
    [ "$name" != wefty-agent ] || continue
    matches[i]=1
  done
}

# This runs before retirement, with stderr still connected to the run log.
# Discovery has a one-second bound; confirmation before KILL has 600 ms.
sweep_exec_storage_processes() {
  exec_stopped_processes=''
  local source=$1 inventory status platform pid name cwd
  local uid i j index warning=0
  local pids=() names=() folders=() matches=()
  local termed_pids=() termed_names=() termed_folders=()
  [ -d "$source" ] && [ ! -L "$source" ] || return 0
  source=${source%/}
  source=$(cd -P -- "${source%/*}" && printf '%s/%s' "$PWD" "${source##*/}") || {
    printf 'wefty: warning: could not scan processes in run storage\n' >&2
    return 0
  }
  platform=$(uname -s)
  uid=$(id -u)
  inventory=$(exec_storage_bounded_inventory "$platform" "$uid" 1)
  status=$?
  # A proc entry can vanish between the glob and find's stat/readlink. Keep
  # complete usable records from that scan; a wholly failed scan still warns.
  if [ "$platform" = Linux ] && [ "$status" -eq 1 ] && [ -n "$inventory" ]; then status=0; fi
  if [ "$status" -ne 0 ]; then
    printf 'wefty: warning: could not scan processes in run storage\n' >&2
    return 0
  fi
  exec_storage_parse_inventory "$inventory" "$platform" "$source"
  for ((i=0; i<${#pids[@]}; i++)); do
    [ "${matches[i]}" -eq 1 ] || continue
    pid=${pids[i]} name=${names[i]} cwd=${folders[i]}
    if kill -TERM "$pid" 2>/dev/null; then
      index=${#termed_pids[@]}
      termed_pids[index]=$pid termed_names[index]=$name termed_folders[index]=$cwd
    elif kill -0 "$pid" 2>/dev/null; then warning=1
    fi
  done
  if [ "${#termed_pids[@]}" -gt 0 ]; then
    sleep 0.2
    inventory=$(exec_storage_bounded_inventory "$platform" "$uid" 0.6)
    status=$?
    # Processes exit after TERM, so proc entries vanish mid-scan; a vanished
    # pid is absent from the records, which is the stopped case.
    if [ "$platform" = Linux ] && [ "$status" -eq 1 ] && [ -n "$inventory" ]; then status=0; fi
    if [ "$status" -eq 0 ]; then
      exec_storage_parse_inventory "$inventory" "$platform" "$source"
    else
      warning=1
    fi
    for ((i=0; i<${#termed_pids[@]}; i++)); do
      pid=${termed_pids[i]} name=${termed_names[i]} cwd=${termed_folders[i]}
      if [ "$status" -ne 0 ]; then
        # No KILL after failed confirmation; only a vanished pid is confirmed.
        if kill -0 "$pid" 2>/dev/null; then continue; fi
      else
        index=-1
        for ((j=0; j<${#pids[@]}; j++)); do
          if [ "${pids[j]}" = "$pid" ]; then index=$j; break; fi
        done
        if [ "$index" -ge 0 ]; then
          [ "${matches[index]}" -eq 1 ] && [ "${names[index]}" = "$name" ] || continue
          if kill -0 "$pid" 2>/dev/null && ! kill -KILL "$pid" 2>/dev/null; then
            if kill -0 "$pid" 2>/dev/null; then warning=1; continue; fi
          fi
        fi
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
  exec_stopped_processes=''
  if [ "$kind" = capture ]; then sweep_exec_storage_processes "$source"; fi
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
