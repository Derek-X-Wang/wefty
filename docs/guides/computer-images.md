# Building Computer images

A compatible Computer image brings its own Linux distribution, init, desktop,
display server, two RFB-over-WebSocket servers, and any tenant agent. Wefty
does not install, launch, or repair desktop components inside an image. Build
against [`docs/contracts/computer-image.md`](../contracts/computer-image.md),
which is the authoritative boot, endpoint, persistence, and profile contract.

Wefty ships the public, digest-pinned `examples/computer/` image as an optional
acceptance and image-author example. It is not a required base image, runtime
layer, compatibility target, or security profile. Bring-your-own desktop is the product:
inheriting from the reference image is neither required nor a claim of
compatibility, and generic OCI Jobs and services do not acquire any Computer
behavior from it.

## Build and select immutable bytes

Build both supported platforms from the repository root:

```sh
# Once per Docker host: install emulation and select a container-backed builder.
docker run --privileged --rm \
  tonistiigi/binfmt@sha256:400a4873b838d1b89194d982c45e5fb3cda4593fbfd7e08a02e76b03b21166f0 \
  --install amd64,arm64
docker buildx create --name wefty-computer --driver docker-container --use
docker buildx inspect --bootstrap

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --file examples/computer/Dockerfile \
  --provenance=false --sbom=false \
  --output type=oci,dest=wefty-computer.oci.tar,rewrite-timestamp=true .
```

CI builds and executes each architecture without credentials on pull requests.
After merge, the `acceptance-image` workflow is the only GHCR writer. It
publishes `ghcr.io/derek-x-wang/wefty-computer-reference:<commit>`, resolves
one multi-platform index digest, proves its `linux/amd64` and `linux/arm64`
children, proves anonymous pull, and packages the exact digest-selected OCI
tarball. Treat the commit tag as discovery only; submit or copy the image by
the recorded `sha256:` index digest. Linux realtiming and attended Mac/Lima
acceptance consume that same digest and tar identity.

The reference build uses a dated Debian snapshot, pins every directly selected
package version, passes `SOURCE_DATE_EPOCH`, and removes apt/dpkg logs in the
install layer. CI performs two empty-cache solves per architecture and requires
their platform digests to match. That is a reproducibility check for these
inputs, not a promise that tags or future toolchains reproduce the bytes; the
recorded digest is the only image identity.

## Build your own image

An operator-owned image need not inherit this example. Its Dockerfile should
install a display server, desktop, browser, an RFB server, and a binary-only
WebSocket edge, create an unprivileged user, and make that user's home resolve
inside `/wefty/service`. Its entrypoint should follow this shape:

```Dockerfile
FROM your-linux-base@sha256:<digest>
RUN install-your-pinned-display-desktop-rfb-and-websocket-packages
RUN useradd --uid 12000 --create-home desktop \
 && rm -rf /home/desktop \
 && ln -s /wefty/service/home/desktop /home/desktop
COPY entrypoint.sh /usr/local/bin/computer-entrypoint
USER 12000:12000
ENTRYPOINT ["/usr/local/bin/computer-entrypoint"]
```

```sh
#!/bin/sh
set -eu
# Validate both injected ports and WEFTY_SERVICE_DIR=/wefty/service.
# Start one display, then loopback-only view and control RFB/WebSocket edges.
start_display_and_desktop
start_view_edge --bind 127.0.0.1:"$WEFTY_COMPUTER_VIEW_PORT" --view-only
start_control_edge --bind 127.0.0.1:"$WEFTY_COMPUTER_CONTROL_PORT"
# Reopen /wefty/control/driver.json after every atomic replacement and default
# false unless version is the integer 1 and human_driving is a boolean.
exec watch_driver_and_children
```

Build and push it to an operator-owned registry, resolve its digest, then build
and run the checker against those immutable bytes:

```sh
go build -o ./wefty-computer-conformance ./cmd/wefty-computer-conformance
./wefty-computer-conformance \
  --image registry.example/operator/computer@sha256:<platform-digest> \
  --platform linux/amd64 \
  --input-oracle-path /path/inside/image/to/input-receipt \
  --driver-oracle-path /path/inside/image/to/observed-driver-state \
  --receipt ./computer-conformance.json
```

Use `--runtime nerdctl` for a containerd installation. The checker applies the
GPU-free Computer profile, injects fresh loopback ports, drives the exact
WebSocket/RFB transport (including byte-identical key and pointer input over
view and control), atomically changes `driver.json`, and restarts the image to
test the persistence boundary. It prints a human summary and writes a
machine-readable receipt with stable check IDs. Every cell starts `NOT-RUN`;
omitting either explicit oracle never becomes `PASS`, and the process exits 2
when no check failed but at least one remains `NOT-RUN`.

An input oracle is image-owned observation, not a new Wefty wire protocol. It
must expose a deterministic file whose bytes change after accepted input and
stay byte-identical when input is discarded. A driver oracle similarly exposes
the tenant agent's already-internal observation of `driver.json`. The reference
image uses `/tmp/wefty-computer/input-oracle.json` and
`/tmp/wefty-computer/driver-state.json`; other images choose their own paths.

For focused transport debugging while the image is running on host port 18181:

```sh
scripts/probe-rfb-websocket.py --port 18181 --mode ready
scripts/probe-rfb-websocket.py --port 18181 --mode query-ready
scripts/probe-rfb-websocket.py --port 18181 --mode fragment-ready
scripts/probe-rfb-websocket.py --port 18181 --mode text-frame
```

The standalone probe remains useful for focused transport debugging. The CI
compatibility wrapper `scripts/test-computer-image-runtime.sh` delegates all
contract assertions to `wefty-computer-conformance`, so the reference lane and
image authors consume one checker rather than two drifting harnesses.

## Image responsibilities

The reference image demonstrates the minimum contract shape:

- `WEFTY_COMPUTER_VIEW_PORT` and `WEFTY_COMPUTER_CONTROL_PORT` are distinct,
  helper-allocated IPv4-loopback listeners named `view` and `control`.
- Both implement `rfb-websocket-v1`: exact `/websockify` path (query components
  are ignored), negotiated
  `binary` subprotocol, binary frames, and an RFB version greeting.
- View input is discarded in the RFB server; control input is accepted. Neither
  backend performs viewer authentication.
- `/wefty/control/driver.json` is reopened and treated as false when absent,
  malformed, or unknown. The image never writes or persists it.
- `/home/wefty` is an image-owned symlink into `/wefty/service`; profile and
  sign-in state persist there. Attempt-local rootfs and tmpfs writes do not.

The image runs Xvfb/XFCE and Chromium with CPU rendering. Chromium is launched
with `--no-sandbox`; that is a disclosed limitation of this example, not an
expansion of the OCI security profile. Wefty adds no GPU, device, capability,
ptrace, privilege, browser-sandbox exception, font, locale, or D-Bus policy.
The helper retains the ordinary `wefty-v1` walls and supplies the private 1 GiB
`/dev/shm` defined by the Computer image contract.

The Omarchy-inspired Wayland/wayvnc variant belongs to ticket #207. The
reference image leaves its deterministic focused input and driver oracle
surfaces for `wefty-computer-conformance`; neither surface is a guest-facing
Wefty protocol.
