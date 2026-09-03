#!/bin/sh
set -eu

action="${1:-}"
case "$action" in
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
