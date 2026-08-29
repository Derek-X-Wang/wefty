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
  # The image deliberately runs as uid 12000, so persisted browser directories
  # are not writable by the hosted runner. Restore traversal through the exact
  # bind mount before asking the runner to remove its mktemp directory.
  docker run --rm --platform "linux/$arch" --user 0:0 --entrypoint /bin/chmod \
    --mount "type=bind,src=$runtime_root,dst=/cleanup" "$image" -R a+rwX /cleanup \
    >/dev/null 2>&1 || true
  rm -rf -- "$runtime_root" || true
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
scripts/probe-rfb-websocket.py --port 18181 --mode query-ready
scripts/probe-rfb-websocket.py --port 18182 --mode query-ready
scripts/probe-rfb-websocket.py --port 18181 --mode fragment-ready
scripts/probe-rfb-websocket.py --port 18182 --mode fragment-ready
ready_elapsed="$(( $(date +%s) - started_at ))"
test "$ready_elapsed" -lt 60

scripts/probe-rfb-websocket.py --port 18181 --mode hold --hold-seconds 10 &
view_probe_pid=$!
scripts/probe-rfb-websocket.py --port 18182 --mode hold --hold-seconds 10 &
control_probe_pid=$!
for _ in $(seq 1 20); do
  if docker exec "$container_id" pgrep -af x11vnc > "$evidence/${arch}-rfb-processes.txt" \
      && grep -q 'x11vnc-view.*-viewonly' "$evidence/${arch}-rfb-processes.txt" \
      && grep -q 'x11vnc-control' "$evidence/${arch}-rfb-processes.txt"; then break; fi
  sleep 0.25
done
grep -q 'x11vnc-view.*-viewonly' "$evidence/${arch}-rfb-processes.txt"
grep -q 'x11vnc-control' "$evidence/${arch}-rfb-processes.txt"
if grep 'x11vnc-control' "$evidence/${arch}-rfb-processes.txt" | grep -q -- '-viewonly'; then
  echo 'control RFB process unexpectedly has -viewonly' >&2
  exit 1
fi
wait "$view_probe_pid" "$control_probe_pid"

for port in 18181 18182; do
  scripts/probe-rfb-websocket.py --port "$port" --mode wrong-path
  scripts/probe-rfb-websocket.py --port "$port" --mode missing-protocol
  scripts/probe-rfb-websocket.py --port "$port" --mode wrong-protocol
  scripts/probe-rfb-websocket.py --port "$port" --mode text-frame
done

oracle_receipt() {
  docker exec "$container_id" cat /tmp/wefty-computer/input-oracle.json 2>/dev/null || true
}
for _ in $(seq 1 240); do
  before_pointer="$(oracle_receipt)"
  if jq -e '.version == 1 and .events >= 0' <<<"$before_pointer" >/dev/null 2>&1; then break; fi
  sleep 0.25
done
if ! jq -e '.version == 1 and .events >= 0' <<<"$before_pointer" >/dev/null 2>&1; then
  echo "input oracle did not expose its initial receipt: ${before_pointer:-missing}" >&2
  exit 1
fi
scripts/probe-rfb-websocket.py --port 18182 --mode pointer-event --x 320 --y 180
for _ in $(seq 1 120); do
  after_control="$(oracle_receipt)"
  if [[ -n $after_control && $after_control != "$before_pointer" ]]; then break; fi
  sleep 0.25
done
if [[ -z $after_control || $after_control == "$before_pointer" ]]; then
  echo "control pointer did not change the oracle: ${after_control:-missing}" >&2
  exit 1
fi
scripts/probe-rfb-websocket.py --port 18181 --mode pointer-event --x 960 --y 540
for _ in $(seq 1 120); do
  after_view="$(oracle_receipt)"
  if [[ -n $after_view ]]; then break; fi
  sleep 0.25
done
if [[ $after_view != "$after_control" ]]; then
  echo "view pointer changed the oracle: before=$after_control after=${after_view:-missing}" >&2
  exit 1
fi

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
  if printf forbidden > /wefty-rootfs-write-probe 2>/dev/null; then
    echo rootfs-write-unexpectedly-succeeded >&2
    exit 1
  fi
  test ! -e /wefty-rootfs-write-probe
'

set_driver() {
  chmod 0644 "$runtime_root/control/driver.json" 2>/dev/null || true
  printf '%s' "$1" > "$runtime_root/control/driver.json.new"
  chmod 0444 "$runtime_root/control/driver.json.new"
  mv "$runtime_root/control/driver.json.new" "$runtime_root/control/driver.json"
}
expect_driver() {
  expected=$1
  for _ in $(seq 1 40); do
    if docker exec "$container_id" sh -c "test \"\$(cat /tmp/wefty-computer/driver-state.json)\" = '$expected'" >/dev/null 2>&1; then break; fi
    sleep 0.25
  done
  docker exec "$container_id" sh -c "test \"\$(cat /tmp/wefty-computer/driver-state.json)\" = '$expected'"
}

set_driver '{"version":1,"human_driving":true}'
expect_driver '{"version":1,"human_driving":true}'
for malformed in \
  '{"version":2,"human_driving":true}' \
  '{"version":true,"human_driving":true}' \
  '{"version":1,"human_driving":1}'; do
  set_driver "$malformed"
  expect_driver '{"version":1,"human_driving":false}'
  sleep 0.5
  expect_driver '{"version":1,"human_driving":false}'
done
mv "$runtime_root/control/driver.json" "$runtime_root/control/driver.json.missing"
expect_driver '{"version":1,"human_driving":false}'
sleep 0.5
expect_driver '{"version":1,"human_driving":false}'

docker exec "$container_id" pkill -f 'wefty-rfb-websocket --port 18181' || true
for _ in $(seq 1 40); do
  if scripts/probe-rfb-websocket.py --port 18181 --mode ready >/dev/null 2>&1; then break; fi
  sleep 0.25
done
scripts/probe-rfb-websocket.py --port 18181 --mode ready

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
   transport_assertions: true,
   negative_rows: {driver_fail_closed: true, wrong_path: true, missing_protocol: true, wrong_protocol: true,
                   text_frame: true, unknown_driver_version: true, wrong_version_type: true,
                   wrong_human_driving_type: true, missing_driver: true},
   endpoints: {view: "loopback", control: "loopback"},
   roles: {view_process_view_only: true, control_process_interactive: true, view_pointer_discarded: true},
   driver_signal_consumed: true, profile_persistent: true, sign_in_persistent: true,
   rootfs_read_only: true, attempt_tmpfs_discarded: true, restarted_edge_recovered: true,
   shm: {private: true, conformant: true, bytes: 1073741824, mode: "1777", nosuid: true, nodev: true, noexec: true},
   readiness_seconds: $readiness_seconds}' > "$evidence/${arch}-runtime.json"
