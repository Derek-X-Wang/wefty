#!/bin/sh
# The P7 replacement seam: only this file binds per-Seat loopback endpoints.
# Cross-Seat port confinement is owned by #1138, and is not claimed here.
set -eu
/usr/local/libexec/wefty-rfb-backend --socket "$XDG_RUNTIME_DIR/view.sock" --view-only &
view_backend=$!
/usr/local/libexec/wefty-rfb-backend --socket "$XDG_RUNTIME_DIR/control.sock" &
control_backend=$!
until [ -S "$XDG_RUNTIME_DIR/view.sock" ] && [ -S "$XDG_RUNTIME_DIR/control.sock" ]; do
  kill -0 "$view_backend" "$control_backend"
  sleep 0.1
done
/usr/local/libexec/wefty-rfb-websocket --port "$VIEW_PORT" --target "$XDG_RUNTIME_DIR/view.sock" &
view_edge=$!
/usr/local/libexec/wefty-rfb-websocket --port "$CONTROL_PORT" --target "$XDG_RUNTIME_DIR/control.sock" &
control_edge=$!
while :; do
  kill -0 "$view_backend" "$control_backend" "$view_edge" "$control_edge"
  sleep 0.2
done
