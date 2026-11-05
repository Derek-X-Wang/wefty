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
    # Probe a real pool URL without fetching upstream. A cold-cache 502 is ready.
    status="$(curl --silent --show-error --max-time 15 --noproxy '' --proxy "$proxy" \
      --header 'X-Debian-Cache-Probe: 1' --dump-header "$state/probe.headers" \
      "http://snapshot.debian.org/archive/debian/20260827T000000Z/$probe" \
      --output /dev/null --write-out '%{http_code}')"
    result="$(awk 'tolower($1) == "x-debian-cache:" { gsub("\r", "", $2); print $2 }' "$state/probe.headers")"
    [[ $status =~ ^[1-5][0-9][0-9]$ && $result =~ ^(HIT|MISS|REFUSED)$ ]] || { cat "$state/proxy.log"; exit 1; }
    printf 'Debian snapshot proxy probe: HTTP %s cache=%s\n' "$status" "$result"
    printf 'DEBIAN_CACHE_PROXY=%s\n' "$proxy" >> "$GITHUB_ENV"
    ;;
  stop)
    stop_status=0
    if test -f "$state/pid"; then
      pid="$(cat "$state/pid")"
      kill -TERM "$pid" 2>/dev/null || true
      for _ in $(seq 1 10); do
        if ! test -f "$state/ready" || ! kill -0 "$pid" 2>/dev/null; then break; fi
        sleep 1
      done
      if test -f "$state/ready" && kill -0 "$pid" 2>/dev/null; then
        printf 'Debian snapshot proxy did not stop after 10 s; sending SIGKILL\n' >&2
        if ! kill -KILL "$pid" 2>/dev/null; then
          if kill -0 "$pid" 2>/dev/null; then stop_status=1; fi
        fi
      fi
    fi
    # SIGKILL may leave readiness and unfinished downloads behind. Completed
    # bodies and their completion metadata remain available to actions/cache.
    rm -f "$state/ready" "$state/pid"
    if test -d "$cache"; then find "$cache" -type f -name '*.tmp' -delete; fi
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
    exit "$stop_status"
    ;;
  *) exit 2 ;;
esac
