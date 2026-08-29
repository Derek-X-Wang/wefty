#!/bin/sh
set -eu

case "${WEFTY_COMPUTER_VIEW_PORT:-}" in ''|*[!0-9]*) echo 'WEFTY_COMPUTER_VIEW_PORT must be a decimal port' >&2; exit 64 ;; esac
case "${WEFTY_COMPUTER_CONTROL_PORT:-}" in ''|*[!0-9]*) echo 'WEFTY_COMPUTER_CONTROL_PORT must be a decimal port' >&2; exit 64 ;; esac
if [ "$WEFTY_COMPUTER_VIEW_PORT" = "$WEFTY_COMPUTER_CONTROL_PORT" ]; then
  echo 'view and control ports must be distinct' >&2
  exit 64
fi
if [ "${WEFTY_SERVICE_DIR:-}" != /wefty/service ]; then
  echo 'WEFTY_SERVICE_DIR must be /wefty/service' >&2
  exit 64
fi

mkdir -p /wefty/service/home/wefty "$HOME/.config/chromium" "$HOME/.config/wefty" \
  "$HOME/.local/state/wefty" "$XDG_RUNTIME_DIR" /tmp/wefty-computer
chmod 0700 "$HOME" "$XDG_RUNTIME_DIR"
if [ ! -f "$HOME/.config/wefty/theme.json" ]; then
  printf '%s\n' '{"version":1,"theme":"graphite"}' > "$HOME/.config/wefty/theme.json"
fi

pids=
start() { "$@" & pids="$pids $!"; }
cleanup() {
  trap - TERM INT EXIT
  for pid in $pids; do kill "$pid" 2>/dev/null || true; done
  wait || true
}
trap cleanup TERM INT EXIT

start sway --config /opt/wefty-computer-wayland/sway-config
remaining=15
while [ "$remaining" -gt 0 ]; do
  for candidate in "$XDG_RUNTIME_DIR"/wayland-*; do
    if [ -S "$candidate" ]; then WAYLAND_DISPLAY=${candidate##*/}; export WAYLAND_DISPLAY; break; fi
  done
  for candidate in "$XDG_RUNTIME_DIR"/sway-ipc.*.sock; do
    if [ -S "$candidate" ]; then SWAYSOCK=$candidate; export SWAYSOCK; break; fi
  done
  if [ -n "${WAYLAND_DISPLAY:-}" ]; then break; fi
  sleep 1
  remaining=$((remaining - 1))
done
if [ -z "${WAYLAND_DISPLAY:-}" ]; then
  echo 'headless Wayland socket did not become ready within 15 seconds' >&2
  exit 1
fi

output_wait=10
while [ "$output_wait" -gt 0 ]; do
  for candidate in "$XDG_RUNTIME_DIR"/sway-ipc.*.sock; do
    if [ -S "$candidate" ]; then SWAYSOCK=$candidate; export SWAYSOCK; break; fi
  done
  if [ -n "${SWAYSOCK:-}" ] && swaymsg --type get_outputs --raw 2>/dev/null | \
    jq -e 'any(.[]; .name == "HEADLESS-1" and .active and .rect.width == 1280 and .rect.height == 720)' >/dev/null; then
    break
  fi
  sleep 1
  output_wait=$((output_wait - 1))
done
if [ "$output_wait" -eq 0 ]; then
  echo 'HEADLESS-1 did not become active within 10 seconds' >&2
  exit 1
fi

DBUS_SESSION_BUS_ADDRESS=$(dbus-daemon --session --fork --print-address)
export DBUS_SESSION_BUS_ADDRESS
start /usr/local/libexec/wefty-watch-driver
start /usr/local/libexec/wefty-wayland-surface
start chromium --no-sandbox --disable-gpu --disable-software-rasterizer=false \
  --ozone-platform=wayland --enable-features=UseOzonePlatform \
  --no-first-run --no-default-browser-check --class=chromium \
  --user-data-dir="$HOME/.config/chromium" --app=http://127.0.0.1:18888/ 2>/dev/null

surface_wait=25
while [ "$surface_wait" -gt 0 ] && { [ ! -s /tmp/wefty-computer/surface-ready ] || [ ! -s /tmp/wefty-computer/driver-state.json ]; }; do
  sleep 1
  surface_wait=$((surface_wait - 1))
done
if [ "$surface_wait" -eq 0 ]; then
  echo 'Wayland input surface or driver observer did not become ready within 25 seconds' >&2
  exit 1
fi

supervise_edge() (
  role=$1
  port=$2
  shift 2
  while :; do
    socket="$XDG_RUNTIME_DIR/wayvnc-$role.ctl"
    rm -f "$socket" "/tmp/wefty-computer/$role-edge-ready"
    WEFTY_WAYVNC_ROLE="$role" wayvnc -w "$@" --output HEADLESS-1 -L info -R -S "$socket" 127.0.0.1 "$port" &
    edge_pid=$!
    edge_status=0
    wait "$edge_pid" || edge_status=$?
    printf 'wayvnc %s edge exited with status %s\n' "$role" "$edge_status" >&2
    sleep 1
  done
)

start supervise_edge view "$WEFTY_COMPUTER_VIEW_PORT" --disable-input
start supervise_edge control "$WEFTY_COMPUTER_CONTROL_PORT"
edge_wait=5
while [ "$edge_wait" -gt 0 ] && { [ ! -s /tmp/wefty-computer/view-edge-ready ] || [ ! -s /tmp/wefty-computer/control-edge-ready ]; }; do
  sleep 1
  edge_wait=$((edge_wait - 1))
done
if [ "$edge_wait" -eq 0 ]; then
  echo 'native view and control listeners did not become ready within 5 seconds' >&2
  exit 1
fi

start foot --server
start mako
wait
