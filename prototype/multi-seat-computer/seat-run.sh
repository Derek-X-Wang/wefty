#!/bin/sh
# PROTOTYPE. Runs as the Seat user (privileges dropped by the supervisor).
# env: SEAT_ID SEAT_DIR SEAT_URL VIEW_PORT CONTROL_PORT
set -eu
umask 077
export HOME="$SEAT_DIR/home" XDG_RUNTIME_DIR="$SEAT_DIR/run" TMPDIR="$SEAT_DIR/tmp"
export XAUTHORITY="$SEAT_DIR/.Xauthority" DISPLAY=":$SEAT_ID"
mkdir -p "$HOME/.config/chromium" "$XDG_RUNTIME_DIR" "$TMPDIR"
cookie=$(mcookie)
rm -f "$XAUTHORITY"
xauth -q -f "$XAUTHORITY" add "$DISPLAY" . "$cookie"
Xvfb "$DISPLAY" -screen 0 1280x720x24 -nolisten tcp -noreset -auth "$XAUTHORITY" &
n=0
until xdpyinfo -display "$DISPLAY" >/dev/null 2>&1; do
  n=$((n+1)); [ $n -gt 600 ] && { echo "Xvfb not ready" >&2; exit 1; }
  sleep 0.1
done
# shellcheck disable=SC2016
dbus-run-session -- sh -c '
  xfce4-session &
  exec chromium --no-sandbox --disable-gpu \
    --no-first-run --no-default-browser-check --start-fullscreen \
    --user-data-dir="$HOME/.config/chromium" "$SEAT_URL"
' &
x11vnc -display "$DISPLAY" -auth "$XAUTHORITY" -localhost -nopw -forever -shared -quiet \
  -noxdamage -rfbport $((VIEW_PORT+10000)) -viewonly &
x11vnc -display "$DISPLAY" -auth "$XAUTHORITY" -localhost -nopw -forever -shared -quiet \
  -noxdamage -rfbport $((CONTROL_PORT+10000)) &
websockify 127.0.0.1:"$VIEW_PORT" 127.0.0.1:$((VIEW_PORT+10000)) &
websockify 127.0.0.1:"$CONTROL_PORT" 127.0.0.1:$((CONTROL_PORT+10000)) &
wait
