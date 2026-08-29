# Building Computer images

A compatible Computer image brings its own Linux distribution, init, desktop,
display server, two RFB-over-WebSocket servers, and any tenant agent. Wefty
does not install, launch, or repair desktop components inside an image. Build
against [`docs/contracts/computer-image.md`](../contracts/computer-image.md),
which is the authoritative boot, endpoint, persistence, and profile contract.

Wefty ships two public, digest-pinned examples: the XFCE image under
`examples/computer/` and the GPU-free Wayland image under
`examples/computer-wayland/`. They are optional acceptance and image-author
examples. Each is not a required base image, runtime layer, compatibility
target, or security profile. Bring-your-own desktop is the product:
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

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --file examples/computer-wayland/Dockerfile \
  --provenance=false --sbom=false \
  --output type=oci,dest=wefty-computer-wayland.oci.tar,rewrite-timestamp=true .
```

CI builds and executes each architecture without credentials on pull requests.
After merge, the `acceptance-image` workflow is the only GHCR writer. It
publishes `ghcr.io/derek-x-wang/wefty-computer-reference:<commit>`, resolves
one multi-platform index digest, proves its `linux/amd64` and `linux/arm64`
children, proves anonymous pull, and packages the exact digest-selected OCI
tarball. Treat the commit tag as discovery only; submit or copy the image by
the recorded `sha256:` index digest. Linux realtiming and attended Mac/Lima
acceptance consume that same digest and tar identity.

Both reference builds use a dated Debian snapshot, pin every directly selected
package version, pass `SOURCE_DATE_EPOCH`, and remove apt/dpkg logs in the
install layer. XFCE retains its two-empty-cache-solve reproducibility check.
The Wayland lane builds one OCI tar, promotes that exact layout into the
ephemeral execution registry, and requires its digest to remain unchanged.
This proves that the bytes executed and later published are the same; it does
not promise that future toolchains reproduce them. The recorded digest is the
only image identity.

The second publication is
`ghcr.io/derek-x-wang/wefty-computer-wayland-reference:<commit>`. It follows
the same per-architecture execute-and-ELF-check, promote-not-rebuild, immutable
index, anonymous-pull, and digest-selected archive rules. Its platform jobs
run the same public conformance checker and the same 20 broken-image rows; a
row is useful only when the conformant image passes and that derivative fails
its one owning cell.

### Reference measurements

The required image lane records cold-start time from the conformance receipt
and whole-container idle RSS from a separate steady-state boot. The values are
evidence for one runner and digest, not sizing promises. The 2026-08-29 required
[contract-gate snapshot](https://github.com/Derek-X-Wang/wefty/actions/runs/33241736102)
recorded:

| Reference | Platform | Idle RSS | Cold start | Executed digest |
| --- | --- | ---: | ---: | --- |
| XFCE/Xvfb | `linux/amd64` | 1,353.3 MiB | 2.662 s | `sha256:e810a2d6a072b73211cab8f408603f320769336ece313dd02c7bca7dcc19c647` |
| XFCE/Xvfb | `linux/arm64` | 1,308.2 MiB | 7.523 s | `sha256:fc0c8c0f68568c1864fa437013e4babf01f1622fd78861645d8681ef6257a37c` |
| GPU-free Wayland | `linux/amd64` | 931.2 MiB | 3.395 s | `sha256:5e078930cca3a40343a990d670ecfe2f476393249666108ca7bac9dfbb701adc` |
| GPU-free Wayland | `linux/arm64` | 882.5 MiB | 3.527 s | `sha256:36d643df24e1c485364e5b37e1cd25eef247ce639151a3aa83525046622f43bc` |

The exact-byte `amd64-metrics.json` and `arm64-metrics.json` receipts are in
`wefty-computer-reference-platform-*` for XFCE and
`wefty-computer-wayland-reference-platform-*` for Wayland. They retain
`idle_rss_bytes` and `cold_start_seconds`; publication carries the same files
forward without rebuilding.

## Second example: GPU-free Wayland

The Wayland example runs Sway with `WLR_BACKENDS=headless`,
`WLR_RENDERER=pixman`, one 1280x720 headless output, and no `/dev/dri`. Two
native `wayvnc -w` processes serve that output directly. The view process uses
`--disable-input`; the control process accepts input. There is no websockify
process or client-side input filter.

Debian's pinned `wayvnc` already owns RFB-over-WebSocket, but its pinned
`neatvnc` accepts a different subprotocol, does not constrain the request path,
and ignores text frames. The image builds an ISC-licensed patch over the pinned
source so the native server accepts only `/websockify` (with ignored query or
fragment), negotiates exactly `binary`, and closes on text frames. The
repository mutation hook makes the text rejection load-bearing in the same
20-row negative lane.

The image derives its input receipt at neatvnc's native RFB parser. The control
role records parsed pointer and key events; the view role records none only
when it actually starts with `--disable-input`. This keeps the isolation proof
load-bearing without making the browser or another client-side layer filter
input.

The image also demonstrates optional, image-owned agent furniture outside the
Computer wire contract:

- a focused surface exposing `idle`, `working`, `blocked`, and `done`, with a
  scripted session proving all four observations;
- a bounded crash briefing for the next agent loop;
- a self-reconfiguration skill that atomically changes a persistent theme;
- pinned mise with lazy `claude` and `codex` stubs; and
- keyboard-first Sway bindings for Foot, Chromium, Neovim, lazygit, Fuzzel,
  and notifications.

These are tenant-image choices, not cluster judgment or new Wefty protocols.
The surface and `driver.json` consumers reopen atomically replaced files and
fail closed. `/home/wefty` remains a symlink into `/wefty/service`.

The repo and image carry
`examples/computer-wayland/LICENSES.md`; the build also rejects Debian
`contrib`/`non-free`, requires package copyright files, and records the
installed-package manifest. Herdr is credited for Apache-2.0 design
inspiration only. The other researched desktop is inspiration only: no code,
assets, installer, trademark, or branding is copied. Chromium still runs with
`--no-sandbox`, which is an image limitation rather than an OCI-profile change.

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
  --edge-process-pattern 'your-websocket-edge --port' \
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

The checker is the sole transport and runtime harness. Its Docker/nerdctl
profile cells are labelled `harness.*` and reported separately from image
conformance. Capability-set, seccomp, namespace, device, and cgroup read-backs
are explicit `NOT-RUN` cells because this harness is not the containerd
`wefty-v1` profile; the native tagged acceptance lane owns those assertions.

The input test simulates Controller tenure inside the harness by opening the
raw control port and atomically replacing the local `driver.json`. It proves
the image role and consumer contracts, including view isolation both before
and during simulated tenure, but it is not an integration test of the #223
grant or #225 sealed-control-tenure front door. A control-pointer sentinel is
the consumption barrier, and the oracle must expose observed key events as
well as pointer coordinates.

## Image responsibilities

Both reference images demonstrate the minimum contract shape:

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

The first image runs Xvfb/XFCE and Chromium with CPU rendering. Chromium is launched
with `--no-sandbox`; that is a disclosed limitation of this example, not an
expansion of the OCI security profile. Wefty adds no GPU, device, capability,
ptrace, privilege, browser-sandbox exception, font, locale, or D-Bus policy.
The helper retains the ordinary `wefty-v1` walls and supplies the private 1 GiB
`/dev/shm` defined by the Computer image contract.

The second image runs headless Sway/pixman and native wayvnc. Both images leave
deterministic focused input and driver oracle surfaces for
`wefty-computer-conformance`; neither surface is a guest-facing Wefty protocol.
