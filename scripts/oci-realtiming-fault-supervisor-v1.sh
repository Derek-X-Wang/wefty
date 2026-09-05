#!/bin/sh
set -eu

# Versioned authority shared by the required and scheduled realtiming lanes.
readonly WEFTY_OCI_FAULT_HARNESS_VERSION=1

record_action_failure() {
  temporary="/tmp/wefty-oci-faults/$action.failed.tmp.$$"
  printf '%s\n' "$1" > "$temporary"
  mv "$temporary" "/tmp/wefty-oci-faults/$action.failed"
}

while true; do
  if IFS= read -r action < /tmp/wefty-oci-faults/control; then
    printf '%s\n' "$action" >> /tmp/wefty-oci-faults/actions.log
    case "$action" in
      kill-shim)
        printf '%s\n' 'kill-shim requires an exact workload Job binding' > "/tmp/wefty-oci-faults/$action.failed"
        continue
        ;;
      kill-payload:*|kill-shim:*)
        job_id="${action#*:}"
        containers=""
        for candidate in $(/usr/local/bin/ctr --address /run/wefty-containerd/containerd.sock --namespace wefty tasks list --quiet); do
          candidate_job_id="$(/usr/local/bin/ctr --address /run/wefty-containerd/containerd.sock --namespace wefty containers info "$candidate" | jq -r '.Labels["io.wefty/job_id"] // empty')"
          if [ "$candidate_job_id" = "$job_id" ]; then
            containers="$containers $candidate"
          fi
        done
        set -- $containers
        if [ "$#" -ne 1 ]; then
          printf 'exact Job binding matched %s workloads\n' "$#" > "/tmp/wefty-oci-faults/$action.failed"
          continue
        fi
        container="$1"
        if [ "${action%%:*}" = kill-payload ]; then
          /usr/local/bin/ctr --address /run/wefty-containerd/containerd.sock --namespace wefty tasks kill --signal KILL "$container"
        else
          shim_pids=""
          for command_line in /proc/[0-9]*/cmdline; do
            [ -r "$command_line" ] || continue
            if tr '\0' '\n' < "$command_line" | awk -v container="$container" 'previous == "-id" && $0 == container { found=1 } { previous=$0 } END { exit !found }'; then
              shim_pid="${command_line#/proc/}"
              shim_pid="${shim_pid%/cmdline}"
              shim_pids="$shim_pids $shim_pid"
            fi
          done
          set -- $shim_pids
          if [ "$#" -ne 1 ]; then
            printf 'exact workload shim binding matched %s processes\n' "$#" > "/tmp/wefty-oci-faults/$action.failed"
            continue
          fi
          kill -KILL "$1"
        fi
        ;;
      stop-containerd)
        systemctl stop wefty-test-containerd.service
        ;;
      start-containerd)
        systemctl start wefty-test-containerd.service
        for _ in $(seq 1 60); do
          if /usr/local/bin/ctr --address /run/wefty-containerd/containerd.sock version >/dev/null 2>&1; then break; fi
          sleep 1
        done
        /usr/local/bin/ctr --address /run/wefty-containerd/containerd.sock version >/dev/null
        chmod 0600 /run/wefty-containerd/containerd.sock
        ;;
      kill-helper:native-lost-attempt-sweep|kill-helper:native-computer-helper-death|kill-helper:service-restart-survival|kill-helper:service-reconfiguration-reset|kill-helper:service-l1-fresh-attempt)
        systemctl kill --kill-who=all --signal=KILL wefty-oci-helper-realtiming.service
        for _ in $(seq 1 300); do
          if systemctl is-active --quiet wefty-oci-helper-realtiming.socket && systemctl is-active --quiet wefty-oci-helper-realtiming.service; then break; fi
          sleep .1
        done
        systemctl is-active --quiet wefty-oci-helper-realtiming.socket
        systemctl is-active --quiet wefty-oci-helper-realtiming.service
        ;;
      reproduce-helper-start-burst:[1-9]*)
        failures="${action##*:}"
        case "$failures" in *[!0-9]*) exit 64 ;; esac
        printf '%s\n' "$failures" > /tmp/wefty-oci-faults/helper-startup-failures
        systemctl kill --kill-who=all --signal=KILL wefty-oci-helper-realtiming.service
        systemctl is-active --quiet wefty-oci-helper-realtiming.socket
        ;;
      manufacture-computer-allocation-mismatch:wefty-computer-disk-*)
        name="${action#*:}"
        disk_root="/var/lib/wefty/oci/computer-disks/$name"
        test ! -e "$disk_root"
        install --directory --owner=root --group=root --mode=0700 "$disk_root"
        fallocate --length 16777216 "$disk_root/disk.ext4"
        mkfs.ext4 -F "$disk_root/disk.ext4" >/dev/null
        cat > "$disk_root/attachment.json" <<MANIFEST
{"version":1,"storage":{"computer_id":"round2-crash","storage_id":"round2-storage","storage_generation":1,"intent_revision":1,"disk_bytes":16777216},"disk_image":"disk.ext4","mount_directory":"$name","prepared":true}
MANIFEST
        sync "$disk_root/attachment.json" "$disk_root/disk.ext4"
        fallocate --length 33554432 "$disk_root/disk.ext4"
        sync "$disk_root/disk.ext4"
        systemctl kill --kill-who=all --signal=KILL wefty-oci-helper-realtiming.service
        ;;
      assert-helper-units-active)
        systemctl is-active --quiet wefty-oci-helper-realtiming.socket
        systemctl is-active --quiet wefty-oci-helper-realtiming.service
        ;;
      stop-helper-topology)
        systemctl stop wefty-oci-helper-realtiming.service wefty-oci-helper-realtiming.socket
        if systemctl is-active --quiet wefty-oci-helper-realtiming.service; then
          record_action_failure 'helper service remained active after topology stop'
          continue
        fi
        if systemctl is-active --quiet wefty-oci-helper-realtiming.socket || test -S /run/wefty-oci-helper/helper.sock; then
          record_action_failure 'helper socket remained active after topology stop'
          continue
        fi
        ;;
      start-helper-topology)
        systemctl start wefty-oci-helper-realtiming.socket
        systemctl start wefty-oci-helper-realtiming.service
        systemctl is-active --quiet wefty-oci-helper-realtiming.socket
        systemctl is-active --quiet wefty-oci-helper-realtiming.service
        test -S /run/wefty-oci-helper/helper.sock
        ;;
      reset-containerd)
        systemctl stop wefty-test-containerd.service
        rm -rf /var/lib/wefty-containerd/root /run/wefty-containerd/state
        systemctl start wefty-test-containerd.service
        for _ in $(seq 1 60); do
          if /usr/local/bin/ctr --address /run/wefty-containerd/containerd.sock version >/dev/null 2>&1; then break; fi
          sleep 1
        done
        /usr/local/bin/ctr --address /run/wefty-containerd/containerd.sock version >/dev/null
        chmod 0600 /run/wefty-containerd/containerd.sock
        ;;
      disable-registry)
        iptables -I OUTPUT 1 -p tcp --dport 443 -m conntrack --ctstate NEW -m owner --uid-owner 0 -j REJECT
        ;;
      enable-registry)
        iptables -D OUTPUT -p tcp --dport 443 -m conntrack --ctstate NEW -m owner --uid-owner 0 -j REJECT
        ;;
      save-attempt-record:wefty-container-*|restore-never-seal-log:wefty-container-*:wefty-log-segments-*|restore-populated-cgroup:wefty-container-*:wefty-cgroup-*|remove-lost-log:wefty-container-*:wefty-log-segments-*)
        # stderr goes to a scratch file first: the test treats the mere existence of
        # .failed as a verdict, so it must only appear once the fault has failed.
        if /usr/local/libexec/wefty-oci-lost-attempt-fault "$action" 2> "/tmp/wefty-oci-faults/$action.stderr"; then
          rm -f "/tmp/wefty-oci-faults/$action.stderr"
        else
          fault_status="$?"
          printf 'fault %s exited %s\n' "$action" "$fault_status" >> "/tmp/wefty-oci-faults/$action.stderr"
          mv "/tmp/wefty-oci-faults/$action.stderr" "/tmp/wefty-oci-faults/$action.failed"
          continue
        fi
        ;;
      assert-computer-clean:wefty-log-segments-*)
        name="${action#assert-computer-clean:}"
        path="/var/lib/wefty/oci/logs/$name"
        if grep -F " $path/control " /proc/mounts >/dev/null || test -e "$path"; then
          printf 'control mount or log directory remains at %s\n' "$path" > "/tmp/wefty-oci-faults/$action.failed"
          continue
        fi
        ;;
      *) exit 64 ;;
    esac
    touch "/tmp/wefty-oci-faults/$action.done"
  fi
done
