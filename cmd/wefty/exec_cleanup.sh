# Shared by the wrapper and observing/sweeping CLI. The caller only waits
# for atomic retirement; the helper owns deletion for as long as it takes.
retire_exec_storage() {
  local source=$1 kind=$2 trash operation polls=0
  if [ "$kind" = trash ]; then
    trash=$source
  else
    trash="${source%/*}/.wefty-trash-${source##*/}-$$"
  fi
  # The process runner reaps the wrapper's process group at attempt end.
  # Job control gives this helper a separate group, also on macOS Bash 3.2
  # where setsid is unavailable. No inherited output pipe may keep a run open.
  set -m
  (
    trap - EXIT INT TERM
    trap '' HUP
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
      if [ -d "$trash" ] && [ ! -L "$trash" ]; then
        /bin/chmod -R u+w "$trash" && /bin/rm -rf -- "$trash"
      fi
      /bin/rmdir -- "$marker/$$" && /bin/rmdir -- "$marker"
    ' wefty-exec-delete "$trash" &
    exit 0
  ) </dev/null >/dev/null 2>&1 &
  operation=$!
  set +m
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
