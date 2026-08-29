#!/usr/bin/env bash
set -euo pipefail

image=
arch=
conformance_receipt=
output=
metrics_output=
while (($# > 0)); do
  case "$1" in
    --image) image="${2:-}"; shift ;;
    --arch) arch="${2:-}"; shift ;;
    --conformance-receipt) conformance_receipt="${2:-}"; shift ;;
    --output) output="${2:-}"; shift ;;
    --metrics-output) metrics_output="${2:-}"; shift ;;
    *) printf '%s\n' 'usage: scripts/test-computer-wayland-furniture.sh --image REF@sha256:DIGEST --arch amd64|arm64 --conformance-receipt FILE --output FILE --metrics-output FILE' >&2; exit 64 ;;
  esac
  shift
done
[[ $image == *@sha256:* && $arch =~ ^(amd64|arm64)$ && -f $conformance_receipt && -n $output && -n $metrics_output ]] || exit 64

root=$(mktemp -d)
container=
cleanup() {
  if [[ -n $container ]]; then
    # Match the public checker's cleanup: the running tenant relaxes only its
    # own bind content before the container is removed. No root cleanup image
    # receives the host path.
    docker exec "$container" sh -c 'chmod -R u+rwX,go+rwX /wefty/service 2>/dev/null || true' >/dev/null 2>&1 || true
    docker rm --force "$container" >/dev/null 2>&1 || true
  fi
  if ! rm -rf "$root"; then printf 'warning: could not remove furniture directory %s\n' "$root" >&2; fi
}
trap cleanup EXIT
mkdir -p "$root/service" "$root/control" "$root/handoff"
chmod 0777 "$root/service" "$root/control" "$root/handoff"
printf '%s\n' '{"version":1,"human_driving":false}' > "$root/control/driver.json"
chmod 0444 "$root/control/driver.json"
read -r view_port control_port < <(python3 - <<'PY'
import socket
ports = []
for _ in range(2):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    ports.append(sock.getsockname()[1])
    sock.close()
print(*ports)
PY
)
container=$(docker run --detach --platform "linux/$arch" --network host --read-only \
  --security-opt no-new-privileges:true --cap-drop ALL \
  --tmpfs /tmp:rw,nosuid,nodev,size=536870912,mode=1777 \
  --tmpfs /var/tmp:rw,nosuid,nodev,size=67108864,mode=1777 \
  --tmpfs /run:rw,nosuid,nodev,size=67108864,mode=0755 \
  --tmpfs /dev/shm:rw,nosuid,nodev,noexec,size=1073741824,mode=1777 \
  --mount "type=bind,src=$root/service,dst=/wefty/service" \
  --mount "type=bind,src=$root/control,dst=/wefty/control,readonly" \
  --mount "type=bind,src=$root/handoff,dst=/wefty/handoff" \
  --env WEFTY_SERVICE_DIR=/wefty/service \
  --env WEFTY_COMPUTER_VIEW_PORT="$view_port" \
  --env WEFTY_COMPUTER_CONTROL_PORT="$control_port" \
  "$image")

ready=false
for _ in $(seq 1 360); do
  if python3 - "$view_port" "$control_port" >/dev/null 2>&1 <<'PY'
import socket, sys
for value in sys.argv[1:]:
    connection = socket.create_connection(("127.0.0.1", int(value)), timeout=.2)
    connection.settimeout(.5)
    connection.sendall((
        "GET /websockify HTTP/1.1\r\nHost: 127.0.0.1\r\n"
        "Upgrade: websocket\r\nConnection: Upgrade\r\n"
        "Sec-WebSocket-Key: d2VmdHktY29uZm9ybWFuY2U=\r\n"
        "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: binary\r\n\r\n"
    ).encode("ascii"))
    response = b""
    while b"\r\n\r\n" not in response:
        response += connection.recv(4096)
    assert response.startswith(b"HTTP/1.1 101 ")
    assert b"sec-websocket-protocol: binary\r\n" in response.lower()
    connection.close()
PY
  then
    if docker exec "$container" sh -c '
      test -s /tmp/wefty-computer/input-oracle.json &&
      { test -S "$XDG_RUNTIME_DIR/wayland-1" || test -S "$XDG_RUNTIME_DIR/wayland-0"; } &&
      jq -e '\''.state == "idle" and .observed == ["idle"]'\'' /tmp/wefty-computer/agent-state-surface.json
    ' >/dev/null 2>&1; then
      ready=true
      break
    fi
  fi
  sleep 0.25
done
if [[ $ready != true ]]; then
  echo 'Wayland furniture did not become ready before the deadline' >&2
  docker logs "$container" >&2
  exit 1
fi
gpu_device_absent=$(docker exec "$container" sh -c '
  test ! -e /dev/dri &&
  tr "\000" " " < /proc/1/cmdline | grep -q wefty-computer-wayland-entrypoint &&
  printf true
')
docker exec "$container" sh -c 'for p in /proc/[0-9]*; do tr "\000" " " < "$p/cmdline" 2>/dev/null; echo; done' > "$root/processes.txt"
grep -F 'sway --config /opt/wefty-computer-wayland/sway-config' "$root/processes.txt"
view_disable_input=$(grep -E 'wayvnc -w .*--disable-input|wayvnc -w --disable-input' "$root/processes.txt" >/dev/null && printf true)
grep -E "wayvnc -w .*127\.0\.0\.1 $control_port" "$root/processes.txt"
if grep -q websockify "$root/processes.txt"; then echo 'websockify process is forbidden' >&2; exit 1; fi
native_wayvnc_websocket=$(grep -E "wayvnc -w .*127\.0\.0\.1 ($view_port|$control_port)" "$root/processes.txt" >/dev/null && printf true)

docker exec --env WEFTY_AGENT_DEMO_PAUSE=0.6 "$container" wefty-agent-session-demo
for _ in $(seq 1 80); do
  if docker exec "$container" jq -e '.state == "done" and .observed == ["idle","working","blocked","done"]' /tmp/wefty-computer/agent-state-surface.json >/dev/null 2>&1; then break; fi
  sleep 0.125
done
agent_states_observed=$(docker exec "$container" jq -ce '
  select(.state == "done" and .observed == ["idle","working","blocked","done"]) | .observed
' /tmp/wefty-computer/agent-state-surface.json)

docker exec "$container" wefty-theme amber
for _ in $(seq 1 80); do
  if docker exec "$container" jq -e '.theme == "amber"' /tmp/wefty-computer/theme-surface.json >/dev/null 2>&1; then break; fi
  sleep 0.125
done
self_reconfiguration_observed=$(docker exec "$container" jq -er '.theme == "amber"' /tmp/wefty-computer/theme-surface.json)

set +e
docker exec "$container" wefty-crash-briefing sh -c 'printf crash-proof >&2; exit 23'
crash_status=$?
set -e
test "$crash_status" -eq 23
crash_briefing_observed=$(docker exec "$container" jq -er \
  '.kind == "crash-briefing" and .exit_code == 23 and (.log_tail | contains("crash-proof"))' \
  /home/wefty/.local/state/wefty/crash-briefing.json)
mise_stubs_present=$(docker exec "$container" sh -c '
  test -x /usr/local/bin/mise &&
  test -L /usr/local/bin/claude &&
  test -L /usr/local/bin/codex &&
  printf true
') || {
  echo 'mise or its agent command stubs are unavailable in the running reference image' >&2
  exit 1
}
license_manifest_present=$(docker exec "$container" sh -c '
  /usr/local/libexec/wefty-verify-licenses --check \
    /usr/share/wefty/licenses/debian-packages.tsv \
    /usr/share/wefty/licenses/non-dpkg-components.tsv >/dev/null &&
  printf true
')

idle_rss_kib=$(docker exec "$container" sh -c '
  total=0
  for status in /proc/[0-9]*/status; do
    while read -r key value _; do
      if [ "$key" = "VmRSS:" ]; then total=$((total + value)); break; fi
    done < "$status" 2>/dev/null || true
  done
  printf "%s\n" "$total"
')
if ! test "$idle_rss_kib" -gt 0; then
  printf 'whole-container idle RSS must be positive, got %s KiB\n' "$idle_rss_kib" >&2
  exit 1
fi
cold_start=$(jq -r '.first_boot_readiness_seconds' "$conformance_receipt")
jq -e '.first_boot_readiness_seconds | type == "number" and . < 60' "$conformance_receipt" >/dev/null
jq -n --arg platform "linux/$arch" --argjson idle_rss_bytes "$((idle_rss_kib * 1024))" \
  --argjson cold_start_seconds "$cold_start" \
  --argjson gpu_device_absent "$gpu_device_absent" \
  --argjson native_wayvnc_websocket "$native_wayvnc_websocket" \
  --argjson view_disable_input "$view_disable_input" \
  --argjson agent_states_observed "$agent_states_observed" \
  --argjson self_reconfiguration_observed "$self_reconfiguration_observed" \
  --argjson crash_briefing_observed "$crash_briefing_observed" \
  --argjson mise_stubs_present "$mise_stubs_present" \
  --argjson license_manifest_present "$license_manifest_present" \
  '{version:1,platform:$platform,gpu_device_absent:$gpu_device_absent,native_wayvnc_websocket:$native_wayvnc_websocket,view_disable_input:$view_disable_input,agent_states_observed:$agent_states_observed,self_reconfiguration_observed:$self_reconfiguration_observed,crash_briefing_observed:$crash_briefing_observed,mise_stubs_present:$mise_stubs_present,license_manifest_present:$license_manifest_present,idle_rss_bytes:$idle_rss_bytes,cold_start_seconds:$cold_start_seconds}' \
  > "$output"
jq -n --arg platform "linux/$arch" --argjson idle_rss_bytes "$((idle_rss_kib * 1024))" \
  --argjson cold_start_seconds "$cold_start" \
  '{version:1,platform:$platform,idle_rss_bytes:$idle_rss_bytes,cold_start_seconds:$cold_start_seconds}' > "$metrics_output"
