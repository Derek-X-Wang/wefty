#!/bin/sh
set -eu

mutation=${WEFTY_CONFORMANCE_MUTATION:-}
case "${WEFTY_COMPUTER_VIEW_PORT:-}" in ''|*[!0-9]*) echo 'WEFTY_COMPUTER_VIEW_PORT must be a decimal port' >&2; exit 64 ;; esac
case "${WEFTY_COMPUTER_CONTROL_PORT:-}" in ''|*[!0-9]*) echo 'WEFTY_COMPUTER_CONTROL_PORT must be a decimal port' >&2; exit 64 ;; esac
if [ "$WEFTY_COMPUTER_VIEW_PORT" = "$WEFTY_COMPUTER_CONTROL_PORT" ] && [ "$mutation" != duplicate-endpoint ]; then
  echo 'view and control ports must be distinct' >&2
  exit 64
fi
if [ "${WEFTY_SERVICE_DIR:-}" != /wefty/service ]; then
  echo 'WEFTY_SERVICE_DIR must be /wefty/service' >&2
  exit 64
fi

view_port=$WEFTY_COMPUTER_VIEW_PORT
control_port=$WEFTY_COMPUTER_CONTROL_PORT
if [ "$mutation" = reserved-env-shadowed ]; then view_port=$WEFTY_CONFORMANCE_REAL_VIEW_PORT; fi

mkdir -p /wefty/service/home/wefty "$HOME/.config/chromium" "$HOME/.config/wefty" \
  "$HOME/.local/state/wefty" "$XDG_RUNTIME_DIR" /tmp/wefty-computer
chmod 0700 "$HOME" "$XDG_RUNTIME_DIR"
if [ ! -f "$HOME/.config/wefty/theme.json" ]; then
  printf '%s\n' '{"version":1,"theme":"graphite"}' > "$HOME/.config/wefty/theme.json"
fi
if [ "$mutation" = profile-state-lost ]; then rm -f "$HOME/.config/wefty-conformance/profile"; fi
if [ "$mutation" = sign-in-state-lost ]; then rm -f "$HOME/.local/share/wefty-conformance/sign-in"; fi

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
  # Stay inside the shell while polling. Forking find, swaymsg, and jq on every
  # pass consumes most of the 60-second contract budget under arm64 QEMU.
  for candidate in "$XDG_RUNTIME_DIR"/wayland-*; do
    if [ -S "$candidate" ]; then
      WAYLAND_DISPLAY=${candidate##*/}
      export WAYLAND_DISPLAY
      break
    fi
  done
  for candidate in "$XDG_RUNTIME_DIR"/sway-ipc.*.sock; do
    if [ -S "$candidate" ]; then
      SWAYSOCK=$candidate
      export SWAYSOCK
      break
    fi
  done
  # wayvnc is the output-readiness authority: it retries below until Sway's
  # configured headless output exists and then owns the contract edge itself.
  if [ -n "${WAYLAND_DISPLAY:-}" ]; then break; fi
  sleep 1
  remaining=$((remaining - 1))
done
if [ -z "${WAYLAND_DISPLAY:-}" ]; then
  echo 'headless Wayland socket did not become ready within 15 seconds' >&2
  exit 1
fi

output_wait=10
output_ready=false
while [ "$output_wait" -gt 0 ]; do
  for candidate in "$XDG_RUNTIME_DIR"/sway-ipc.*.sock; do
    if [ -S "$candidate" ]; then
      SWAYSOCK=$candidate
      export SWAYSOCK
      break
    fi
  done
  if [ -n "${SWAYSOCK:-}" ] && swaymsg --type get_outputs --raw 2>/dev/null | jq -e 'any(.[]; .name == "HEADLESS-1" and .active and .rect.width == 1280 and .rect.height == 720)' >/dev/null; then
    output_ready=true
    break
  fi
  sleep 1
  output_wait=$((output_wait - 1))
done
if [ "$output_ready" != true ]; then
  echo 'HEADLESS-1 did not become active before edge startup' >&2
  exit 1
fi

if [ "$mutation" = readiness-over-60s ]; then sleep 61; fi

supervise_edge() (
  role=$1
  port=$2
  input_flag=${3:-}
  while :; do
    address=127.0.0.1
    if [ "$mutation" = "$role-wildcard-bind" ]; then address=0.0.0.0; fi
    socket="$XDG_RUNTIME_DIR/wayvnc-$role.ctl"
    rm -f "$socket"
    if [ -n "$input_flag" ]; then
      WEFTY_WAYVNC_ROLE="$role" wayvnc -w "$input_flag" --output HEADLESS-1 -L info -R -S "$socket" "$address" "$port" &
    else
      WEFTY_WAYVNC_ROLE="$role" wayvnc -w --output HEADLESS-1 -L info -R -S "$socket" "$address" "$port" &
    fi
    edge_pid=$!
    edge_status=0
    wait "$edge_pid" || edge_status=$?
    printf 'wayvnc %s edge exited with status %s\n' "$role" "$edge_status" >&2
    if [ "$mutation" = edge-does-not-recover ] && [ "$role" = view ]; then exit 0; fi
    sleep 1
  done
)

# Prepare assertion-derived input and driver state before exposing the edges.
# The checker can begin its oracle probes immediately after RFB readiness, so
# publishing either edge before these files exist would create a startup race.
DBUS_SESSION_BUS_ADDRESS=$(dbus-daemon --session --fork --print-address)
export DBUS_SESSION_BUS_ADDRESS
start /usr/local/libexec/wefty-watch-driver
start /usr/local/libexec/wefty-wayland-surface
start chromium --no-sandbox --disable-gpu --disable-software-rasterizer=false \
  --ozone-platform=wayland --enable-features=UseOzonePlatform \
  --no-first-run --no-default-browser-check --class=chromium \
  --user-data-dir="$HOME/.config/chromium" --app=http://127.0.0.1:18888/ 2>/dev/null
oracle_wait=25
while [ "$oracle_wait" -gt 0 ] && { [ ! -s /tmp/wefty-computer/surface-ready ] || [ ! -s /tmp/wefty-computer/driver-state.json ]; }; do
  sleep 1
  oracle_wait=$((oracle_wait - 1))
done
if [ "$oracle_wait" -eq 0 ]; then
  echo 'Wayland input surface or driver observer did not become ready within 25 seconds' >&2
  exit 1
fi

view_flag=--disable-input
if [ "$mutation" = view-accepts-input ]; then view_flag=; fi
wait_native_edge() {
  role=$1
  edge_wait=5
  while [ "$edge_wait" -gt 0 ] && { \
    [ ! -S "$XDG_RUNTIME_DIR/wayvnc-$role.ctl" ] || \
    [ ! -f "/tmp/wefty-computer/$role-edge-ready" ]; \
  }; do
    sleep 1
    edge_wait=$((edge_wait - 1))
  done
}

# Fixture-only mutations replace the production entrypoint in derivative
# images. Listener markers sequence startup; external handshakes remain the
# conformance authority.
if [ -z "$mutation" ]; then
  start supervise_edge view "$view_port" "$view_flag"
  start supervise_edge control "$control_port"
  wait_native_edge view
  wait_native_edge control
elif [ "$mutation" != missing-view-endpoint ]; then
  start supervise_edge view "$view_port" "$view_flag"
fi
if [ "$mutation" = plain-tcp-control ]; then
  start python3 -m http.server "$control_port" --bind 127.0.0.1
elif [ -n "$mutation" ] && [ "$mutation" != missing-control-endpoint ] && [ "$mutation" != duplicate-endpoint ]; then
  start supervise_edge control "$control_port"
fi

start foot --server
start mako

wait
