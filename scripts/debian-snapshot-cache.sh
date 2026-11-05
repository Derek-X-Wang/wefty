#!/usr/bin/env bash
# Runner-only cache for apt and pinned source archives; never an image layer or BuildKit cache.
set -euo pipefail

state="$RUNNER_TEMP/debian-snapshot-proxy"
cache="$RUNNER_TEMP/debian-snapshot-cache"
case "${1:?start or stop}" in
  start)
    probe="${2:?pass a pool package path installed by this build}"
    mkdir -p "$state" "$cache"
    gateway="$(docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}')"
    test -n "$gateway"
    python3 scripts/debian-snapshot-cache-proxy.py --cache-dir "$cache" \
      --summary "$state/summary.json" --ready-file "$state/ready" --port 0 > "$state/proxy.log" 2>&1 &
    echo "$!" > "$state/pid"
    for _ in $(seq 1 30); do
      kill -0 "$(cat "$state/pid")" || { cat "$state/proxy.log"; exit 1; }
      if test -f "$state/ready"; then break; fi
      sleep 1
    done
    test -f "$state/ready" || { cat "$state/proxy.log"; exit 1; }
    proxy="http://$gateway:$(cat "$state/ready")"
    # A real pool package proves the proxy can serve apt downloads, not just indexes.
    curl --fail --silent --show-error --max-time 110 --noproxy '' --proxy "$proxy" \
      "http://snapshot.debian.org/archive/debian/20260827T000000Z/$probe" \
      --output /dev/null
    printf 'DEBIAN_CACHE_PROXY=%s\n' "$proxy" >> "$GITHUB_ENV"
    ;;
  stop)
    if test -f "$state/pid"; then
      kill -TERM "$(cat "$state/pid")" 2>/dev/null || true
      for _ in $(seq 1 120); do
        if ! test -f "$state/ready"; then break; fi
        sleep 1
      done
    fi
    # Counters are persisted on each request, including a failed build or forced stop.
    if test -f "$state/summary.json"; then
      cat "$state/proxy.log"
      cat "$state/summary.json" >> "$GITHUB_STEP_SUMMARY"
      python3 - "$state/summary.json" >> "$GITHUB_OUTPUT" <<'PY'
import json, sys
print("misses=" + str(json.load(open(sys.argv[1]))["misses"]))
PY
      du -sh "$cache"
    fi
    test ! -f "$state/ready"
    ;;
  *) exit 2 ;;
esac
