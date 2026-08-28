#!/usr/bin/env bash
set -euo pipefail

image=
arch=
evidence=
while (($# > 0)); do
  case "$1" in
    --image) image="${2:-}"; shift ;;
    --arch) arch="${2:-}"; shift ;;
    --evidence) evidence="${2:-}"; shift ;;
    *) printf '%s\n' 'usage: scripts/test-computer-image-runtime.sh --image REF@sha256:DIGEST --arch amd64|arm64 --evidence DIR' >&2; exit 64 ;;
  esac
  shift
done
[[ $image == *@sha256:* && $arch =~ ^(amd64|arm64)$ && -n $evidence ]] || exit 64
mkdir -p "$evidence"

runtime_root="$(mktemp -d)"
container_id=
cleanup() {
  if [[ -n $container_id ]]; then docker stop --time 15 "$container_id" >/dev/null 2>&1 || true; fi
  rm -rf -- "$runtime_root"
}
trap cleanup EXIT
install -d -m 0777 "$runtime_root/service"
install -d -m 0755 "$runtime_root/control"
printf '%s' '{"version":1,"human_driving":false}' > "$runtime_root/control/driver.json"
chmod 0444 "$runtime_root/control/driver.json"

run_attempt() {
  docker run --detach --rm --platform "linux/$arch" --network host --read-only \
    --tmpfs /tmp:rw,nosuid,nodev,size=536870912,mode=1777 \
    --tmpfs /var/tmp:rw,nosuid,nodev,size=67108864,mode=1777 \
    --tmpfs /run:rw,nosuid,nodev,size=67108864,mode=0755 \
    --shm-size 1g \
    --mount "type=bind,src=$runtime_root/service,dst=/wefty/service" \
    --mount "type=bind,src=$runtime_root/control,dst=/wefty/control,readonly" \
    --env WEFTY_SERVICE_DIR=/wefty/service \
    --env WEFTY_COMPUTER_VIEW_PORT=18181 \
    --env WEFTY_COMPUTER_CONTROL_PORT=18182 \
    "$image"
}

started_at="$(date +%s)"
container_id="$(run_attempt)"
for _ in $(seq 1 60); do
  if scripts/probe-rfb-websocket.py --port 18181 --mode ready >/dev/null 2>&1 \
      && scripts/probe-rfb-websocket.py --port 18182 --mode ready >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
scripts/probe-rfb-websocket.py --port 18181 --mode ready
scripts/probe-rfb-websocket.py --port 18182 --mode ready
ready_elapsed="$(( $(date +%s) - started_at ))"
test "$ready_elapsed" -lt 60

for port in 18181 18182; do
  scripts/probe-rfb-websocket.py --port "$port" --mode wrong-path
  scripts/probe-rfb-websocket.py --port "$port" --mode missing-protocol
  scripts/probe-rfb-websocket.py --port "$port" --mode wrong-protocol
  scripts/probe-rfb-websocket.py --port "$port" --mode text-frame
done

docker exec "$container_id" sh -c '
  test -L /home/wefty
  test "$(readlink /home/wefty)" = /wefty/service/home/wefty
  test "$(stat -c %a /dev/shm)" = 1777
  awk '\''$2 == "/dev/shm" && $3 == "tmpfs" && index("," $4 ",", ",nosuid,") && index("," $4 ",", ",nodev,") && index("," $4 ",", ",noexec,") && index($4, "size=1048576k") { found=1 } END { exit !found }'\'' /proc/mounts
  test "$(cat /tmp/wefty-computer/driver-state.json)" = '\''{"version":1,"human_driving":false}'\''
  awk '\''$2 == "0100007F:4705" { view=1 } $2 == "0100007F:4706" { control=1 } END { exit !(view && control) }'\'' /proc/net/tcp
  printf persistent-profile > "$HOME/.config/chromium/wefty-profile-marker"
  printf persistent-sign-in > "$HOME/.config/chromium/wefty-sign-in-marker"
  printf attempt-local > /tmp/wefty-rootfs-marker
'

chmod 0644 "$runtime_root/control/driver.json"
printf '%s' '{"version":2,"human_driving":true}' > "$runtime_root/control/driver.json.new"
chmod 0444 "$runtime_root/control/driver.json.new"
mv "$runtime_root/control/driver.json.new" "$runtime_root/control/driver.json"
sleep 1
docker exec "$container_id" sh -c 'test "$(cat /tmp/wefty-computer/driver-state.json)" = '\''{"version":1,"human_driving":false}'\'''

printf '%s' '{"version":1,"human_driving":true}' > "$runtime_root/control/driver.json.new"
chmod 0444 "$runtime_root/control/driver.json.new"
mv "$runtime_root/control/driver.json.new" "$runtime_root/control/driver.json"
for _ in $(seq 1 20); do
  if docker exec "$container_id" sh -c 'test "$(cat /tmp/wefty-computer/driver-state.json)" = '\''{"version":1,"human_driving":true}'\''' >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec "$container_id" sh -c 'test "$(cat /tmp/wefty-computer/driver-state.json)" = '\''{"version":1,"human_driving":true}'\'''
mv "$runtime_root/control/driver.json" "$runtime_root/control/driver.json.missing"
for _ in $(seq 1 20); do
  if docker exec "$container_id" sh -c 'test "$(cat /tmp/wefty-computer/driver-state.json)" = '\''{"version":1,"human_driving":false}'\''' >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec "$container_id" sh -c 'test "$(cat /tmp/wefty-computer/driver-state.json)" = '\''{"version":1,"human_driving":false}'\'''

docker stop --time 15 "$container_id"
container_id=
printf '%s' '{"version":1,"human_driving":false}' > "$runtime_root/control/driver.json.new"
chmod 0444 "$runtime_root/control/driver.json.new"
mv "$runtime_root/control/driver.json.new" "$runtime_root/control/driver.json"
container_id="$(run_attempt)"
for _ in $(seq 1 60); do
  if scripts/probe-rfb-websocket.py --port 18181 --mode ready >/dev/null 2>&1 \
      && scripts/probe-rfb-websocket.py --port 18182 --mode ready >/dev/null 2>&1; then break; fi
  sleep 1
done
scripts/probe-rfb-websocket.py --port 18181 --mode ready
scripts/probe-rfb-websocket.py --port 18182 --mode ready
docker exec "$container_id" sh -c '
  test "$(cat "$HOME/.config/chromium/wefty-profile-marker")" = persistent-profile
  test "$(cat "$HOME/.config/chromium/wefty-sign-in-marker")" = persistent-sign-in
  test ! -e /tmp/wefty-rootfs-marker
'

jq -n --arg platform "linux/$arch" --arg image "$image" --argjson readiness_seconds "$ready_elapsed" '
  {platform: $platform, image: $image, executed: true, rfb_websocket_v1: true,
   negative_rows: {wrong_path: true, missing_protocol: true, wrong_protocol: true, text_frame: true,
                   unknown_driver_version: true, missing_driver: true},
   endpoints: {view: "loopback", control: "loopback"}, driver_signal_consumed: true,
   profile_persistent: true, sign_in_persistent: true, rootfs_discarded: true,
   shm: {private: true, bytes: 1073741824, mode: "1777", nosuid: true, nodev: true, noexec: true},
   readiness_seconds: $readiness_seconds}' > "$evidence/${arch}-runtime.json"
