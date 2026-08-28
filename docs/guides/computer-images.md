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

## Image responsibilities

The reference image demonstrates the minimum contract shape:

- `WEFTY_COMPUTER_VIEW_PORT` and `WEFTY_COMPUTER_CONTROL_PORT` are distinct,
  helper-allocated IPv4-loopback listeners named `view` and `control`.
- Both implement `rfb-websocket-v1`: exact `/websockify` path, negotiated
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
`wefty-computer-conformance` checker belongs to ticket #182; this image leaves
its deterministic focused input-oracle surface without implementing that CLI.
