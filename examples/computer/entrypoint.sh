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
for _ in $(seq 1 60); do
  if xdpyinfo -display "$DISPLAY" >/dev/null 2>&1; then break; fi
  sleep 1
done
xdpyinfo -display "$DISPLAY" >/dev/null

# shellcheck disable=SC2016 # HOME expands in the dbus-run-session child shell.
start dbus-run-session -- sh -c '
  xfce4-session &
  exec chromium --no-sandbox --disable-gpu \
    --no-first-run --no-default-browser-check --start-fullscreen \
    --user-data-dir="$HOME/.config/chromium" file:///opt/wefty-computer/oracle.html
'
start /usr/local/libexec/wefty-watch-driver

view_socket=/tmp/wefty-computer/view-rfb.sock
control_socket=/tmp/wefty-computer/control-rfb.sock
start /usr/local/libexec/wefty-rfb-backend --socket "$view_socket" --view-only
start /usr/local/libexec/wefty-rfb-backend --socket "$control_socket"
for _ in $(seq 1 60); do
  if [ -S "$view_socket" ] && [ -S "$control_socket" ]; then break; fi
  sleep 1
done
test -S "$view_socket"
test -S "$control_socket"

start /usr/local/libexec/wefty-rfb-websocket --port "$WEFTY_COMPUTER_VIEW_PORT" --target "$view_socket"
start /usr/local/libexec/wefty-rfb-websocket --port "$WEFTY_COMPUTER_CONTROL_PORT" --target "$control_socket"

# The helper probes both edges continuously and atomically withdraws publication
# if either one disappears. Keep the remaining image processes available so a
# recovered edge can become eligible for republication.
wait
