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
  --file examples/computer/Dockerfile --target single-seat \
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
install layer. Both references retain the two-empty-cache-solve
reproducibility check: one solve emits the OCI tar and the second emits the
ephemeral registry image, whose platform digests must be equal.
This proves that the bytes executed and later published are the same; it does
not promise that future toolchains reproduce them. The recorded digest is the
only image identity.

The `computer-seats`, main-push `acceptance-image`, and reusable
`acceptance-image-build` desktop jobs restore immutable Debian files for snapshot
`20260827T000000Z` from GitHub Actions cache. A runner
HTTP proxy serves apt files (indexes, packages and source packages) and the
checksum-pinned neatvnc source archive; it is not an image layer cache.
Before the build, a cache-only request checks that the proxy is serving HTTP,
using the pinned `xvfb` pool URL for XFCE/default Seats or `wayvnc` for Wayland.
The probe never contacts upstream. It logs its HTTP status and cache result:
a validated warm entry returns `200/HIT`, and a cold entry returns `502/CACHE-ONLY-MISS`.
Both prove readiness, including in offline mode; upstream health cannot block
startup. Other responses or failure to reach the proxy fail the step.
Misses fetch directly from snapshot.debian.org with bounded retries;
partial fills are saved even when a build fails. `DEBIAN_CACHE_OFFLINE=1` refuses
misses for cache-only proof runs. The Wayland builder and its text-frame test
fixture fetch neatvnc over HTTP so they share the cache; both retain the same
SHA-256 check. Other HTTPS downloads still use their original upstream URLs.
The reference jobs keep the proxy running through runtime checks and pass
`WEFTY_DEBIAN_SNAPSHOT_PROXY` to Wayland's fixture builds. Without that variable,
the runtime script keeps its original build arguments. These fixtures have only
local tags, are removed after testing, and are never published.

The jobs pass only predefined `http_proxy`/`HTTP_PROXY` build arguments. Docker
[excludes these from image history and build cache by default](https://docs.docker.com/build/building/variables/#proxy-arguments);
the Dockerfiles do not declare them. The runner's Docker bridge gateway reaches
the proxy without changing a build's network mode. The fixture build's default
bridge can reach the runner at that same gateway address; the proxy listens on
all runner interfaces. Dockerfile edits are limited to the approved Wayland
builder-stage URL schemes and comments. Pins, checksum checks and both
empty-cache image digest checks stay unchanged; CI must also compare final
Wayland digests with the prior images. Cache keys separate jobs,
architectures and desktop variants, and include the snapshot date and Dockerfile
hashes, including the text-frame fixture in reference-job keys. GitHub scopes PR
caches to the PR merge ref, so a PR fill cannot seed
main. Main must fill its own cache. GitHub's default repository cache budget is
10 GB; eviction can make a later build cold again. The proxy reports request
counts and cache disk usage after each build.

Only a complete upstream HTTP 200 is published. Downloads use a 30-second read
inactivity timeout and a 15-minute total budget across retries, so large packages
can make steady progress without an unbounded trickle. Each file has an atomic
completion record containing its length and SHA-256. Every hit checks that record
against the bytes; truncated, altered or legacy entries without metadata become
misses. Online builds refetch them; offline builds refuse them. Shutdown sends
SIGTERM, waits up to 10 seconds, then uses SIGKILL if needed. The last atomic
summary is still read, so files stored before shutdown can still be saved.

A save creates a new full entry only when the proxy reports stored > 0 (newly
published files); it can retain a partial fill after failure. Misses and upstream
failures remain in the summary and logs, but a run that stores nothing creates
no entry. Restore selects the newest entry matching
the job/architecture/variant/date/Dockerfile prefix. Old entries are not updated
in place. GitHub evicts entries unused for more than 7 days and, above the
default 10 GB budget, evicts entries in order of oldest last access first
([cache scope, matching and eviction](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching)).
Unmeasured planning estimates per architecture are 0.7–1.2 GiB for default Seats
and 0.4–0.8 GiB for each reference variant: at most about 5.6 GiB for one entry
per prefix across both architectures. Main and reusable reference jobs share
prefixes, but concurrent fills can save separate copies; one extra reference
generation raises that estimate to 8.8 GiB. Historical entries and PR-scoped
copies can exhaust the budget and evict a warm cache.

The proxy stores and returns response bodies byte for byte; it does not rewrite
signed metadata or decompress indexes. Apt still authenticates `InRelease` or
`Release` plus `Release.gpg` and checks package hashes. The existing
`check-valid-until=no` setting disables expiry checks for the historical snapshot,
not signatures. No source uses `trusted=yes`, and no apt flag allows insecure
repositories or unauthenticated packages. CI's apt output should include
`Get:1 http://snapshot.debian.org/archive/debian/20260827T000000Z bookworm InRelease`
(or `trixie InRelease`). This line shows metadata retrieval; successful
`apt-get update` with the unchanged authentication defaults establishes signature
acceptance. See Debian's [apt-secure documentation](https://manpages.debian.org/bookworm/apt/apt-secure.8.en.html).

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
evidence for one runner and digest, not sizing promises, so this guide does not
copy values that would go stale when the example changes. Each required run's
exact-byte `amd64-metrics.json` and `arm64-metrics.json` receipts are in
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
published library contains no mutation hook; the `text-frames-accepted`
derivative recompiles it with `-Dwefty_mutation_hooks=true` before proving the
owning cell fails.

The image derives its input receipt from two guest-side Wayland clients after
compositor delivery. The fullscreen Chromium surface records pointer
coordinates through a per-boot nonce-bound callback, while a transparent
native `wev` client records `wl_keyboard` events only when it owns keyboard
focus. These are client observations, not compositor-listener evidence. The
checker first
proves view input leaves that guest receipt unchanged and then proves control
input changes it at exact coordinates. Thus parser acceptance alone cannot
satisfy `input.control-accepted`, and regressing `--disable-input` fails both
view-isolation cells.

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
`examples/computer-wayland/LICENSES.md`; the build requires every installed
Debian package copyright file and records a separate closed manifest for the
patched neatvnc library, mise, and Wefty-authored image files. Their ISC, MIT,
and repository Apache-2.0 notices are shipped in the image. Herdr is credited
for Apache-2.0 design inspiration only. The other researched desktop is
inspiration only: no code, assets, installer, trademark, or branding is copied. Chromium still runs with
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
  --repair-image registry.example/operator/computer@sha256:<platform-digest> \
  --platform linux/amd64 \
  --input-oracle-path /path/inside/image/to/input-receipt \
  --driver-oracle-path /path/inside/image/to/observed-driver-state \
  --edge-process-pattern 'your-websocket-edge --port' \
  --receipt ./computer-conformance.json
```

`--reference-surface-probe` explicitly opts in to the reference images'
Chromium profile and display conventions for
`screen.input-capture-current-frame`. Leave it off for other images; the check
then stays `NOT-RUN`, and this optional check alone does not hold the image
result at `NOT-RUN`. Missing input evidence or unavailable preparation records
`NOT-RUN` without a readiness wait.

The check proves the Seat screen CLI's capture after a settle timeout (#1484).
A page turns green 400 ms after a key, so the CLI's 250 ms wait times out.
Fifteen trials run in the order `C,B,B,C,T` three times, each with its own
colour change from a confirmed red page:

- C, the proof: on one new control connection, the only connection to the
  image, the probe makes the CLI's calls (baseline, key, Sync, settle wait),
  holds the outstanding request until 650 ms after the key, then captures with
  the CLI's drain. Ground truth then reads full frames on the same connection
  every 50 ms until one is green, so a reply left over from the capture's
  requests cannot hide a stale capture. The 1 s limit on these reads is soft:
  it is checked between reads, and one read can wait until the call's 15 s
  deadline. The measured capture itself is never reread. The capture is current
  (green), stale (red while the ground truth turns green), or inconclusive (the
  wait did not time out, or the page never turned green). Red followed by green
  counts as stale. This is the conservative direction: it assumes the page has
  redrawn by the capture at 650 ms (the redraw is due at 400 ms), so a page that
  redraws late can only add false stale results, never hide one.
- B, a positive control: a fresh view connection reads its first full frame
  250 ms after control saw green. Stale B frames are expected data on x11vnc;
  they show the probe can detect staleness and never fail the check.
- T: the same as C, but the capture starts as soon as the wait times out, and
  there is no ground truth: T's result comes from its capture alone. Red and
  green are both correct, because a frame taken after the input but before the
  page redraws is not stale. A T trial whose wait did not time out is
  inconclusive. A capture error or hang fails the check. An error after a T
  capture, while waiting for the page before the next trial, keeps the trial's
  result and is reported as the probe's error.

Every trial is one JSON line in the checker's diagnostics and in the detail,
followed by `input-capture trials: C current=…/6 stale=…/6 inconclusive=…/6;
B (fresh view) stale=…/6; T returned=…/3 inconclusive=…/3 red=… green=…
max_ms=…`. The check fails if any C capture is stale or a measurement fails,
and is `NOT-RUN` when setup stays unavailable after bounded retries, fewer than
four C trials are conclusive, or fewer than two T trials timed out and
returned. Measurements are never retried. Reference-image CI enables the
flag, accepts `PASS` or `NOT-RUN`, prints the detail, and fails on `FAIL`.
The Seat lane runs the same probe on the default image through the node front
door and fails on any stale C capture, a measurement error, fewer than four
conclusive C trials, or fewer than two T trials that timed out and returned.

Use `--runtime nerdctl` for a containerd installation. The checker applies the
GPU-free Computer profile, injects fresh loopback ports, drives the exact
WebSocket/RFB transport (including byte-identical key and pointer input over
view and control), atomically changes `driver.json`, and restarts the image to
test the persistence boundary. It prints a human summary and writes a
machine-readable receipt with stable check IDs. Every cell starts `NOT-RUN`;
omitting either explicit oracle never becomes `PASS`, and the process exits 2
when no check failed but at least one remains `NOT-RUN`.

`--repair-image` is the immutable, known-good image whose `/bin/sh` the checker
may use after container removal to repair permissions under only its detached
temporary root. Keep it pinned to trusted bytes rather than replacing it with
an intentionally broken derivative under test.

An input oracle is image-owned observation, not a new Wefty wire protocol. It
must expose a deterministic file whose bytes change after accepted input and
stay byte-identical when input is discarded. A driver oracle similarly exposes
the tenant agent's already-internal observation of `driver.json`. The reference
image uses `/tmp/wefty-computer/input-oracle.json` and
`/tmp/wefty-computer/driver-state.json`; other images choose their own paths.
The driver oracle publishes one atomic JSON object with exactly
`version`, `human_driving`, `generation`, `fingerprint`, and `classification`:

```json
{"version":1,"human_driving":false,"generation":7,"fingerprint":"2f05d4b689d270cafb02285f35f44866c422400c3c83397801d7d8f31c3d5f1b","classification":"valid"}
```

`version` is integer `1`; `human_driving` is fail-closed; `generation` rises
monotonically for the observer process; and `fingerprint` is lowercase
hexadecimal SHA-256 of the exact bytes classified by that observation.
`classification` is exactly `valid`, `malformed`, `unknown-version`, or
`missing`; missing input uses the literal fingerprint sentinel `missing`.
Read and fingerprint once, publish by atomic rename, and do not reset the
generation while the checker is attached.

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

The first image runs Xvfb/XFCE and Chromium with CPU rendering. Its X display
number is derived from the helper-reserved view port, avoiding accidental
contention for a fixed `:99`. The Computer profile supplies a private network
namespace, so the matching Linux abstract X socket is neither enumerable nor
connectable from a co-located Computer. The helper reaches the image only
through its authority-bound named view/control streams.
Chromium is launched with `--no-sandbox`; that is a disclosed limitation of
this example, not an expansion of the OCI security profile. Wefty adds no GPU,
device, capability,
ptrace, privilege, browser-sandbox exception, font, locale, or D-Bus policy.
The helper retains the ordinary `wefty-v1` walls and supplies the private scaled
`/dev/shm` (256 MiB at a 1 GiB Computer cap, at most 1 GiB) defined by the Computer image contract.

The second image runs headless Sway/pixman and native wayvnc. Both images leave
deterministic focused input and driver oracle surfaces for
`wefty-computer-conformance`; neither surface is a guest-facing Wefty protocol.
