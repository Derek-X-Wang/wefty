#!/usr/bin/env bash
set -euo pipefail

image=
arch=
conformance_receipt=
output=
while (($# > 0)); do
  case "$1" in
    --image) image="${2:-}"; shift ;;
    --arch) arch="${2:-}"; shift ;;
    --conformance-receipt) conformance_receipt="${2:-}"; shift ;;
    --output) output="${2:-}"; shift ;;
    *) printf '%s\n' 'usage: scripts/measure-computer-image.sh --image REF@sha256:DIGEST --arch amd64|arm64 --conformance-receipt FILE --output FILE' >&2; exit 64 ;;
  esac
  shift
done
[[ $image == *@sha256:* && $arch =~ ^(amd64|arm64)$ && -f $conformance_receipt && -n $output ]] || exit 64

root=$(mktemp -d)
container=
cleanup() {
  if [[ -n $container ]]; then
    docker exec "$container" sh -c 'chmod -R u+rwX,go+rwX /wefty/service 2>/dev/null || true' >/dev/null 2>&1 || true
    docker rm --force "$container" >/dev/null 2>&1 || true
  fi
  if ! rm -rf "$root"; then printf 'warning: could not remove measurement directory %s\n' "$root" >&2; fi
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
for _ in $(seq 1 240); do
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
  then ready=true; break; fi
  sleep 0.25
done
if [[ $ready != true ]]; then
  echo 'computer image did not expose both edge ports before the measurement deadline' >&2
  docker logs "$container" >&2
  exit 1
fi
sleep 2
idle_rss_kib=$(docker exec "$container" sh -c '
  total=0
  for status in /proc/[0-9]*/status; do
    while read -r key value _; do
      if [ "$key" = "VmRSS:" ]; then total=$((total + value)); break; fi
    done < "$status" 2>/dev/null || true
  done
  printf "%s\n" "$total"
')
test "$idle_rss_kib" -gt 0
cold_start=$(jq -r '.first_boot_readiness_seconds' "$conformance_receipt")
jq -n --arg platform "linux/$arch" --argjson idle_rss_bytes "$((idle_rss_kib * 1024))" \
  --argjson cold_start_seconds "$cold_start" \
  '{version:1,platform:$platform,idle_rss_bytes:$idle_rss_bytes,cold_start_seconds:$cold_start_seconds}' > "$output"
