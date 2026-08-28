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

mkdir -p /wefty/service/home/wefty
mkdir -p "$HOME/.config/chromium" "$XDG_RUNTIME_DIR" /tmp/wefty-computer
chmod 0700 "$HOME" "$XDG_RUNTIME_DIR"

pids=
start() {
  "$@" &
  pids="$pids $!"
}
cleanup() {
  trap - TERM INT EXIT
  for pid in $pids; do kill "$pid" 2>/dev/null || true; done
  wait || true
}
trap cleanup TERM INT EXIT

start Xvfb "$DISPLAY" -screen 0 1280x720x24 -nolisten tcp -noreset
startup_remaining=60
while [ "$startup_remaining" -gt 0 ]; do
  if xdpyinfo -display "$DISPLAY" >/dev/null 2>&1; then break; fi
  sleep 1
  startup_remaining=$((startup_remaining - 1))
done
if ! xdpyinfo -display "$DISPLAY" >/dev/null 2>&1; then
  echo 'display server did not become ready within the 60 second startup budget' >&2
  exit 1
fi

# shellcheck disable=SC2016 # HOME expands in the dbus-run-session child shell.
start dbus-run-session -- sh -c '
  xfce4-session &
  exec chromium --no-sandbox --disable-gpu \
    --no-first-run --no-default-browser-check --start-fullscreen \
    --user-data-dir="$HOME/.config/chromium" file:///opt/wefty-computer/oracle.html
'
start /usr/local/libexec/wefty-watch-driver
start /usr/local/libexec/wefty-pointer-oracle

view_socket=/tmp/wefty-computer/view-rfb.sock
control_socket=/tmp/wefty-computer/control-rfb.sock

supervise_edge() (
  role=$1
  port=$2
  socket_path=$3
  view_flag=${4:-}
  backend_pid=
  websocket_pid=
  # shellcheck disable=SC2329 # invoked by the trap below.
  stop_edge() {
    trap - TERM INT EXIT
    [ -z "$websocket_pid" ] || kill "$websocket_pid" 2>/dev/null || true
    [ -z "$backend_pid" ] || kill "$backend_pid" 2>/dev/null || true
    wait || true
    rm -f "$socket_path"
  }
  trap stop_edge TERM INT EXIT
  while :; do
    rm -f "$socket_path"
    if [ -n "$view_flag" ]; then
      /usr/local/libexec/wefty-rfb-backend --socket "$socket_path" --view-only &
    else
      /usr/local/libexec/wefty-rfb-backend --socket "$socket_path" &
    fi
    backend_pid=$!
    edge_remaining=60
    while [ ! -S "$socket_path" ] && kill -0 "$backend_pid" 2>/dev/null && [ "$edge_remaining" -gt 0 ]; do
      sleep 1
      edge_remaining=$((edge_remaining - 1))
    done
    if [ ! -S "$socket_path" ]; then
      echo "$role RFB backend did not become ready" >&2
      kill "$backend_pid" 2>/dev/null || true
      wait "$backend_pid" 2>/dev/null || true
      sleep 1
      continue
    fi
    /usr/local/libexec/wefty-rfb-websocket --port "$port" --target "$socket_path" &
    websocket_pid=$!
    while kill -0 "$backend_pid" 2>/dev/null && kill -0 "$websocket_pid" 2>/dev/null; do sleep 1; done
    kill "$websocket_pid" "$backend_pid" 2>/dev/null || true
    wait "$websocket_pid" 2>/dev/null || true
    wait "$backend_pid" 2>/dev/null || true
    websocket_pid=
    backend_pid=
    sleep 1
  done
)

start supervise_edge view "$WEFTY_COMPUTER_VIEW_PORT" "$view_socket" --view-only
start supervise_edge control "$WEFTY_COMPUTER_CONTROL_PORT" "$control_socket"
while [ "$startup_remaining" -gt 0 ]; do
  if [ -S "$view_socket" ] && [ -S "$control_socket" ]; then break; fi
  sleep 1
  startup_remaining=$((startup_remaining - 1))
done
if [ ! -S "$view_socket" ] || [ ! -S "$control_socket" ]; then
  echo 'RFB backends did not become ready within the 60 second startup budget' >&2
  exit 1
fi

# Edge supervisors restart failed local transports. The helper independently
# decides whether the pair is eligible for publication.
wait
