#!/bin/sh
set -eu

action="${1:-}"
case "$action" in
  inject-live-log:wefty-log-segments-*)
    name="${action#inject-live-log:}"
    if ! printf '%s\n' "$name" | grep -Eq '^wefty-log-segments-[0-9a-f]{32}$'; then
      printf 'invalid helper log identity %s\n' "$name" >&2
      exit 64
    fi
    path="/var/lib/wefty/oci/logs/$name"
    install --directory --owner=root --group=root --mode=0700 "$path"
    : > "$path/stdout.frames"
    : > "$path/stderr.frames"
    python3 - "$path" <<'PY' &
import hashlib
import pathlib
import struct
import sys
import time

root = pathlib.Path(sys.argv[1])
time.sleep(1)
seal = struct.pack(">4sQI", b"WLS1", 0, 0) + hashlib.sha256(b"").digest()
for stream in ("stdout.frames", "stderr.frames"):
    with (root / stream).open("ab") as target:
        target.write(seal)
        target.flush()
PY
    ;;
  populate-cgroup:wefty-cgroup-*)
    name="${action#populate-cgroup:}"
    if ! printf '%s\n' "$name" | grep -Eq '^wefty-cgroup-[0-9a-f]{32}$'; then
      printf 'invalid helper cgroup identity %s\n' "$name" >&2
      exit 64
    fi
    path="/sys/fs/cgroup/$name"
    mkdir "$path"
    sleep 300 &
    pid="$!"
    printf '%s\n' "$pid" > "$path/cgroup.procs"
    ;;
  save-attempt-record:wefty-container-*)
    container="${action#save-attempt-record:}"
    if ! printf '%s\n' "$container" | grep -Eq '^wefty-container-[0-9a-f]{32}$'; then
      printf 'invalid helper container identity %s\n' "$container" >&2
      exit 64
    fi
    cp "/var/lib/wefty/oci/attempt-ownership/$container.json" "/tmp/wefty-oci-faults/$container.record"
    ;;
  restore-never-seal-log:wefty-container-*:wefty-log-segments-*)
    identities="${action#restore-never-seal-log:}"
    container="${identities%%:*}"
    log_segment="${identities#*:}"
    if ! printf '%s\n' "$container" | grep -Eq '^wefty-container-[0-9a-f]{32}$' ||
       ! printf '%s\n' "$log_segment" | grep -Eq '^wefty-log-segments-[0-9a-f]{32}$'; then
      printf 'invalid helper lost-log identities %s %s\n' "$container" "$log_segment" >&2
      exit 64
    fi
    install --directory --owner=root --group=root --mode=0700 /var/lib/wefty/oci/attempt-ownership "/var/lib/wefty/oci/logs/$log_segment"
    install --owner=root --group=root --mode=0600 "/tmp/wefty-oci-faults/$container.record" "/var/lib/wefty/oci/attempt-ownership/$container.json"
    : > "/var/lib/wefty/oci/logs/$log_segment/stdout.frames"
    : > "/var/lib/wefty/oci/logs/$log_segment/stderr.frames"
    ;;
  restore-populated-cgroup:wefty-container-*:wefty-cgroup-*)
    identities="${action#restore-populated-cgroup:}"
    container="${identities%%:*}"
    cgroup="${identities#*:}"
    if ! printf '%s\n' "$container" | grep -Eq '^wefty-container-[0-9a-f]{32}$' ||
       ! printf '%s\n' "$cgroup" | grep -Eq '^wefty-cgroup-[0-9a-f]{32}$'; then
      printf 'invalid helper lost-cgroup identities %s %s\n' "$container" "$cgroup" >&2
      exit 64
    fi
    install --directory --owner=root --group=root --mode=0700 /var/lib/wefty/oci/attempt-ownership
    install --owner=root --group=root --mode=0600 "/tmp/wefty-oci-faults/$container.record" "/var/lib/wefty/oci/attempt-ownership/$container.json"
    mkdir "/sys/fs/cgroup/$cgroup"
    sleep 300 &
    pid="$!"
    printf '%s\n' "$pid" > "/sys/fs/cgroup/$cgroup/cgroup.procs"
    ;;
  remove-lost-log:wefty-container-*:wefty-log-segments-*)
    identities="${action#remove-lost-log:}"
    container="${identities%%:*}"
    log_segment="${identities#*:}"
    if ! printf '%s\n' "$container" | grep -Eq '^wefty-container-[0-9a-f]{32}$' ||
       ! printf '%s\n' "$log_segment" | grep -Eq '^wefty-log-segments-[0-9a-f]{32}$'; then
      exit 64
    fi
    rm -f "/var/lib/wefty/oci/attempt-ownership/$container.json"
    rm -rf "/var/lib/wefty/oci/logs/$log_segment"
    ;;
  *)
    exit 64
    ;;
esac
