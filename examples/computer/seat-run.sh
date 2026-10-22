#!/bin/sh
# Runs as the Seat user. All installs, browser profiles and temp data live on disk.
set -eu
umask 077
export HOME="$SEAT_DIR/home" XDG_RUNTIME_DIR="$SEAT_DIR/run" TMPDIR="$SEAT_DIR/tmp"
export XAUTHORITY="$SEAT_DIR/.Xauthority" DISPLAY=":$SEAT_ID"
export XDG_CACHE_HOME="$HOME/.cache" XDG_CONFIG_HOME="$HOME/.config"
export PLAYWRIGHT_BROWSERS_PATH="$HOME/.cache/ms-playwright"
export npm_config_cache="$HOME/.cache/npm" PIP_CACHE_DIR="$HOME/.cache/pip"
export WEFTY_DRIVER_SOURCE="/wefty/control/seats/$SEAT_ID/driver.json"
export WEFTY_DRIVER_TARGET="$XDG_RUNTIME_DIR/driver-state.json"
mkdir -p "$HOME" "$XDG_RUNTIME_DIR" "$TMPDIR"
cookie=$(mcookie)
printf 'add %s . %s\n' "$DISPLAY" "$cookie" | xauth -q -f "$XAUTHORITY"
Xvfb "$DISPLAY" -screen 0 1280x800x24 -nolisten tcp -noreset -auth "$XAUTHORITY" &
display_pid=$!
# The root supervisor owns the 60 s deadline, including all this setup.
until xdpyinfo -display "$DISPLAY" >/dev/null 2>&1; do
  kill -0 "$display_pid"
  sleep 0.1
done
# shellcheck disable=SC2016
# Chromium's sandbox cannot run within wefty-v1's noNewPrivileges profile.
# Disk-backed temp avoids charging browser temp files against shared tmpfs.
dbus-run-session -- sh -c '
  xfce4-session &
  exec chromium --no-sandbox --disable-gpu --disable-dev-shm-usage \
    --no-first-run --no-default-browser-check --start-fullscreen \
    --user-data-dir="$HOME/.config/chromium" \
    "file:///opt/wefty-computer/seat.html?place=$SEAT_ID"
' &
browser_pid=$!
/usr/local/libexec/wefty-watch-driver &
driver_pid=$!
/usr/local/libexec/wefty-seat-endpoints &
endpoints_pid=$!
# A TCP or RFB greeting alone did not prove a browser in the prototype.
until xprop -root _NET_CLIENT_LIST 2>/dev/null | tr ',' '\n' | \
  sed -n 's/.*\(0x[0-9a-fA-F]*\).*/\1/p' | while read -r window; do
    xprop -id "$window" _NET_WM_NAME 2>/dev/null
  done | grep -q "wefty-seat-$SEAT_ID-ready"; do
  kill -0 "$browser_pid" "$display_pid" "$endpoints_pid" "$driver_pid"
  sleep 0.1
done
: > "$XDG_RUNTIME_DIR/desktop-ready"
while :; do
  kill -0 "$browser_pid" "$display_pid" "$endpoints_pid" "$driver_pid"
  sleep 0.2
done
