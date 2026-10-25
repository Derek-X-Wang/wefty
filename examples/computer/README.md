# Default Computer and optional reference image

The final `seats` target is the default multi-Seat Computer image. It starts
a root supervisor and waits for `/wefty/control/seats.json`. It requires the
profile-owned, bounded `/wefty/seat-status` tmpfs beside the read-only control
bind. It provides four named Seat users, 1280×800 desktops, test tools and
disk-backed Seat temp folders. See the [Seat contract](../../docs/contracts/computer-image.md#default-image-and-seats-computer-request-p5-p6-p8-p9)
for directive fields, answers, deadlines and current P7 limits.

Each screen role starts one warm x11vnc on first use and supervises it across
client connections. Clients relay through private Unix sockets; x11vnc opens
no TCP listener. See the [screen backend contract](../../docs/contracts/computer-image.md#warm-screen-backends).

## Single-desktop reference target

Build the earlier single-desktop reference with `--target single-seat`.
That Debian/XFCE/Chromium image is an acceptance and image-author example. It
is not a required base image, runtime layer, compatibility target, or security
profile. The generic OCI examples and `wefty-echo-service` remain separate.

The image owns its desktop stack and implements both reserved
`rfb-websocket-v1` roles. The `view` role starts `x11vnc` with server-side
input disabled; `control` accepts input. Both are exposed only at the injected
loopback ports, require `/websockify` and the `binary` subprotocol, and reject
text frames. The focused Chromium page is the deterministic oracle seam that
`wefty-computer-conformance` exercises with real, byte-identical view and
control input.

The checker simulates tenure through the raw control endpoint and its local
`driver.json`; this example is not a #223/#225 admission or tenure integration
test. Its oracle records actual X key events and pointer history so a control
sentinel can prove ordering without a fixed sleep.

`/home/wefty` is an image-owned symlink to
`/wefty/service/home/wefty`, so browser profile and sign-in markers follow the
Computer disk across attempts and stop/start. Runtime scratch state stays in
attempt-local `/run`, `/tmp`, and `/dev/shm`.

See [`docs/guides/computer-images.md`](../../docs/guides/computer-images.md)
for the complete build and security boundary.
