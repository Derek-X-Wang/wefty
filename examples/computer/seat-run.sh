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
# The display which answered must still be this Seat's own Xvfb.
kill -0 "$display_pid"
printf '%s\n' "$display_pid" > "$XDG_RUNTIME_DIR/display.pid"
# The session bus and desktop stay up across browser and edge restarts.
exec dbus-run-session -- /usr/local/libexec/wefty-seat-session --display-pid "$display_pid"
