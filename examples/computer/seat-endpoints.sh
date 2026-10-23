#!/bin/sh
# P7: both WebSocket edges and RFB backends are private Seat Unix sockets.
set -eu
umask 077
/usr/local/libexec/wefty-rfb-backend --socket "$XDG_RUNTIME_DIR/view.sock" --view-only &
view_backend=$!
/usr/local/libexec/wefty-rfb-backend --socket "$XDG_RUNTIME_DIR/control.sock" &
control_backend=$!
until [ -S "$XDG_RUNTIME_DIR/view.sock" ] && [ -S "$XDG_RUNTIME_DIR/control.sock" ]; do
  kill -0 "$view_backend" "$control_backend"
  sleep 0.1
done
/usr/local/libexec/wefty-rfb-websocket --socket "$XDG_RUNTIME_DIR/view.ws" --target "$XDG_RUNTIME_DIR/view.sock" &
view_edge=$!
/usr/local/libexec/wefty-rfb-websocket --socket "$XDG_RUNTIME_DIR/control.ws" --target "$XDG_RUNTIME_DIR/control.sock" &
control_edge=$!
while :; do
  kill -0 "$view_backend" "$control_backend" "$view_edge" "$control_edge"
  sleep 0.2
done
