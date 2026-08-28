# Computer image and boot contract

This contract defines the image-owned half of a `computer`-trait OCI service
and the agent's all-or-nothing screen-door readiness verdict. The ratified
authority is the agent-computer spec section 7; this document fixes the seam
that reference images and conformance tooling consume.

## Bring your own desktop

A compatible image brings its own Linux distribution, init, desktop, display
server, two RFB-over-WebSocket servers, and any tenant agent. Wefty does not
install, launch, or repair desktop components inside the image.

Image `USER`, `ENTRYPOINT`, and `CMD` retain OCI semantics. `ENTRYPOINT` plus
`CMD` is the effective argv unless the Job explicitly replaces argv, and an
explicit working-directory replacement is the only override of the image
working directory. Wefty adds no capability, device, GPU, ptrace, privilege,
browser sandbox exception, font, locale, or D-Bus policy. Image labels,
`EXPOSE`, and declared volumes are not allocation or publication inputs.

## Reserved environment and targets

Image and operator values for every reserved OCI environment name are removed
before the authoritative attempt-local layer is applied. Other `WEFTY_*`
names remain ordinary tenant environment.

| Name | Computer value | Visibility |
| --- | --- | --- |
| `WEFTY_SERVICE_DIR` | `/wefty/service` | public; preserved persistent-data contract |
| `WEFTY_COMPUTER_VIEW_PORT` | helper-allocated decimal loopback port | public |
| `WEFTY_COMPUTER_CONTROL_PORT` | distinct helper-allocated decimal loopback port | public |
| `WEFTY_SERVICE_PORT` | omitted | reserved but never injected for a Computer |
| `WEFTY_COMPUTER_TOKEN` | L3-minted live-attempt Computer pass when submission is enabled | sensitive; agent-memory to closed helper field only |

The other reserved OCI names remain governed by the run-execution-context
contract. In particular, a Computer never receives `WEFTY_RUN_TOKEN` merely
because it is a Computer.

Submission is disabled by default. When an administrator enables the
revisioned submission intent, the agent must obtain an L3-verified pass bound
to the exact Computer, attempt, Storage generation, submit-intent revision,
host Node, and L3 authority generation before helper preflight. Mint failure
is exact `pass_unavailable`; no token reaches the runtime. The attempt bridge
and guest-visible `WEFTY_L3_ENDPOINT` remain the transport contract owned by
#184 and do not change this credential provenance.

The environment value is start-time delivery only. The helper also publishes
the current pass at `/wefty/control/computer-token` so an administrator can
enable submission during a live attempt. That file is atomically replaced,
mode `0400`, and owned by the resolved image tenant uid/gid. Disable,
revocation, or authority loss removes it; re-enable and policy changes publish
a freshly minted pass. A tenant must reopen or watch the path and treat a
missing file as submission disabled. The file remains attempt-local tmpfs and
never enters `/wefty/service`, a JobSpec, logs, inspection, or removal evidence.

`/wefty/service`, `/wefty/control`, and `/wefty/handoff` are non-shadowable.
Operator mount targets equal to, above, or below any of them are rejected after
normalization. Image filesystem content at a reserved target is hidden by the
helper-owned mount and cannot replace it.

## Named display endpoints

Each attempt receives exactly the endpoint-name set `{view, control}`. The
helper allocates two distinct ports from its private range and keeps their
authority bound to the exact attempt. Two concurrent Computers therefore
receive four distinct ports. Allocation never reads a Job `published_port`,
image label, image `EXPOSE`, image volume declaration, or operator label.

Both image servers must bind their injected ports on IPv4 loopback.
`0.0.0.0`, a bind owned by another cgroup, a fixed numeric port, or any
unreturned endpoint name fails closed. Only the helper can dial a returned
name for current attempt authority; raw port selection never crosses the
agent/runtime seam.

The roles are fixed:

- `view` renders the same display and discards RFB client input server-side.
- `control` renders that display and accepts RFB client input.
- Neither backend authenticates a viewer. Fabric identity and admission live
  at the agent front door, which always begins on `view`.

## `rfb-websocket-v1`

Both named endpoints implement the same exact transport contract:

- WebSocket request path `/websockify`.
- Exactly negotiated `binary` WebSocket subprotocol.
- RFB bytes in binary frames only; a text frame is rejected and closes the
  connection.
- The first 12 payload bytes are an RFB version greeting of the form
  `RFB ddd.ddd\n`.

A TCP accept, an HTTP response without a WebSocket upgrade, a different path,
a missing or different subprotocol, a text greeting, or an invalid/missing RFB
banner is not readiness. Tenant-agent, framebuffer-content, browser, and
desktop-health checks are deliberately outside this screen-door contract.

## Atomic readiness

The privileged helper's successful container task `Start` edge fixes `t0`;
image pull and preparation precede it and do not consume the budget. The
helper returns that timestamp in `Run.started_at`, and the agent carries it
unchanged across the L1 `OCIStarted` acknowledgement and other post-start
round trips before polling both helper tunnels.

```text
Started at t0
    |
    +-- view ready? ----- no --+
    |                          |
    +-- control ready? -- no --+--> publish nothing
    |                          |    at t0 + 60 s:
    +-- both ready ------------+    startup_readiness_timeout
             |
             +--> publish one Fabric front door + display_endpoint
                          |
             either backend is lost
                          |
             close forwarding/sessions, withdraw the one publication
                          |
             both exact contracts recover
                          |
             eligible for one atomic republication
```

The deadline is exactly 60 seconds after `t0`. A probe that finishes at or
after that deadline cannot publish even when both handshakes succeeded; the
agent rechecks its injected clock immediately before publication. Expiry produces the existing
typed `startup_readiness_timeout`, which service restart policy classifies as
restartable; no partial endpoint or placeholder display URL is published.
After the first successful publication, losing either backend withdraws both
without killing the tenant payload. Recovery requires fresh success from both
endpoints before the ordered publication controller may republish.

## OCI profile

A Computer keeps the ordinary `wefty-v1` isolation walls: resolved image user,
12 fixed capabilities, explicit empty inheritable/ambient capability sets,
`noNewPrivileges`, containerd's generated seccomp profile, private
PID/IPC/UTS/mount/cgroup namespaces, shared networking, and deny-all devices
plus the six pseudo-devices. The root filesystem is read-only; the Computer
disk at `/wefty/service` is the persistent writable path.

The Computer-specific profile adds a private `/dev/shm` tmpfs with a 1 GiB
size ceiling, mode `1777`, and `nosuid,nodev,noexec`. It is created in the
attempt's private mount and IPC namespaces, and its pages count against the
attempt's cgroup memory limit. The existing bounded `/tmp` (512 MiB),
`/var/tmp` (64 MiB), `/dev`, and `/run` mounts remain unchanged. The three
Computer-specific ceilings total 1600 MiB. They are caps rather than
reservations: a smaller memory limit remains admissible and the cgroup limit
is enforcement. The assertion-derived profile receipt and node doctor expose
the exact ceiling/limit comparison as typed warnings; the production sizing
OWNER-CALL remains open.

## Tenant `driver.json` consumer contract

The image's tenant agent consumes read-only
`/wefty/control/driver.json`. Every fresh attempt begins with exactly
`{"version":1,"human_driving":false}`. While a human holds Controller tenure
the complete document is exactly `{"version":1,"human_driving":true}`; it
returns to the false document after tenure is released.

The helper owns the attempt-local file outside `/wefty/service`, mode 0444,
and replaces it with a same-directory atomic rename. The consumer therefore
sees only complete version-1 documents, never a partially written update. It
must reopen or watch the path rather than retain one inode, treat any missing,
malformed, or unknown-version document as `human_driving=false`, and must not
attempt to write, rename, or persist it. The signal carries no driver identity,
history, or authority and never asks the tenant process to pause.

## Conformance seam and evidence

The transport-neutral Go contract exports the endpoint names, path,
subprotocol, RFB-banner length/validator, 60-second deadline, and 1 GiB shm
size. Runtime endpoint aliases and the helper consume those values. The
complete serialized Computer profile is pinned at
`runner/ocihelper/testdata/containerd-v2.3.4/wefty-v1-computer-linux-amd64.json`.

Portable tests prove reserved-value stripping, exact endpoint admission,
wire-negative cases, atomic loss/recovery, injected-clock deadline behavior,
and the serialized profile. The Linux `service_acceptance_realtiming` lane
asserts `/dev/shm` mode, flags, size, and a rising cgroup `memory.current` after
a guest write. The optional
reference image and `wefty-computer-conformance` CLI remain separate tickets;
this contract does not implement either artifact.
