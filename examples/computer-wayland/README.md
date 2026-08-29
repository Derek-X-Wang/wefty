# Optional GPU-free Wayland reference Computer image

This second Debian/Sway/Chromium image is an acceptance and image-author
example. It is not a required base image, runtime layer, compatibility target,
or security profile, and it does not replace the XFCE reference.

Sway runs on wlroots' headless backend with the pixman renderer and no
`/dev/dri`. Two native `wayvnc -w` servers expose the same output: view uses
`--disable-input`, while control accepts input. A small ISC-licensed neatvnc
source patch makes its native WebSocket edge implement Wefty's exact
`rfb-websocket-v1` path, subprotocol, and binary-frame behavior; there is no
websockify process.

The focused, transparent, output-sized `wev` client publishes the deterministic
input oracle only after Sway delivers keyboard or pointer events to its native
`wl_keyboard` or `wl_pointer` listener.
Chromium also shows `idle`, `working`, `blocked`, and `done` agent states. The included
self-reconfiguration skill changes a persistent theme setting. Agent CLI stubs
delegate lazy installation to pinned mise, and crash briefings preserve a
bounded local explanation for the next agent loop.

See [`docs/guides/computer-images.md`](../../docs/guides/computer-images.md)
and [`LICENSES.md`](LICENSES.md).
