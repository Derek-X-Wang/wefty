# Computer image and boot contract

Computer Storage recovery is fail-closed per generation. A valid interrupted
grow or copy record that cannot yet resume remains in place as
`resume_deferred` with an attempt count, first-deferred time, and closed reason.
Only boot-barrier startup sweeps increment the attempt count; in-session reap
sweeps do not. Recovery terminates as `resume_abandoned` only after both
twenty-four failed agent boot-barrier sweeps and 24 elapsed hours, so a helper
restart storm cannot consume the bound in minutes. Structural image/record
mismatch and invalid authority quarantine immediately. Quarantine retains its
payload for 24 hours and its typed tombstone thereafter, so N is never
admissible again; the supported recovery path prepares and admits reset
generation N+1 and clears N only through authorized removal.
For a never-attached Custody import generation, the agent persists an exact
helper `resume_deferred` or quarantine result in L1's import ledger. The CLI
observes that immutable import identity and revision directly; completion
recorded before polling begins is still visible, while a retained helper
outcome is returned immediately as typed evidence rather than a generic wait
deadline. A later successful preparation receipt clears the provisional
outcome and remains the only route to publication.

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
| `WEFTY_L3_ENDPOINT` | attempt-local Computer bridge URL only when submission is enabled at start | public; omitted by default |

The other reserved OCI names remain governed by the run-execution-context
contract. In particular, a Computer never receives `WEFTY_RUN_TOKEN` merely
because it is a Computer.

Submission is disabled by default. When an administrator enables the
revisioned submission intent, the agent must obtain an L3-verified pass bound
to the exact Computer, attempt, Storage generation, submit-intent revision,
host Node, and L3 authority generation before helper preflight. Mint failure
is exact `pass_unavailable`; no token reaches the runtime. Default-off means
there is no bridge and no `WEFTY_L3_ENDPOINT`. Enable-at-start creates the
transport before helper start and injects the endpoint with the pass.

The environment value is start-time delivery only. The helper also publishes
the current pass at `/wefty/control/computer-token` and the matching transport
URL at `/wefty/control/l3-endpoint`, each by same-directory atomic replacement,
mode `0400`, and owned by the resolved image tenant uid/gid. Mid-attempt enable
starts the bridge and publishes both files; disable, revocation, or authority
loss closes the bridge and removes both. A tenant must reopen or watch both
paths and treat either missing file as submission disabled. An in-flight
request canceled by bridge closure has an indeterminate outcome and, when the
bridge can still produce a response, receives typed retryable
`pass_unavailable`; only L3 may return an authorization verdict. The tenant
must reopen both files and retry with the same idempotency key to resolve
whether L3 committed the request before revocation. The files remain
attempt-local tmpfs and never enter `/wefty/service`, a JobSpec, logs,
inspection, or removal evidence.

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

Both image-side named endpoints implement the same request, subprotocol,
binary-payload, and greeting contract:

- WebSocket request path `/websockify`; query and fragment components are
  accepted and ignored, so viewer-added values such as `?token=` do not change
  routing.
- Exactly negotiated `binary` WebSocket subprotocol.
- RFB bytes in binary frames only. An image-side text-frame rejection is
  conformant only when the read observes RFC 6455 `unsupported data` or
  immediate EOF before the probe deadline; any other read failure is not
  rejection evidence. A fresh connection must still return a valid RFB
  greeting, proving that rejection did not crash the display bridge.
- The first 12 payload bytes are an RFB version greeting of the form
  `RFB ddd.ddd\n`.

A TCP accept, an HTTP response without a WebSocket upgrade, a different path,
a missing or different subprotocol, a text greeting, or an invalid/missing RFB
banner is not readiness. Tenant-agent, framebuffer-content, browser, and
desktop-health checks are deliberately outside this screen-door contract.

The agent Fabric front door is a distinct admission surface over the same RFB
stream. It closes client text frames with RFC 6455 `unsupported data`; EOF is
not the portable front-door assertion. Before upgrade, authorization failures
are typed JSON. When the CLI presents L1 policy revision `N` and the hosting
agent has installed an older revision, the front door returns HTTP 403 with
retryable `stale_policy_revision`; an installed-current permanent denial is
non-retryable `control_not_authorized`.

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
disk at `/wefty/service` is the persistent writable path. After attaching the
disk and before retaining profile sources, the helper creates `etc/machine-id`
when absent or repairs it when invalid, and records the repair in helper logs.
The same file remains writable through `/wefty/service/etc/machine-id` but is
mounted read-only at its canonical `/etc/machine-id` path. Root ownership is
verified after identity repair and before the task starts. Restore preserves
the same machine identity, while clone and import initialize a legacy missing
identity and then rekey the copied identity before the destination can attach. A copied
`Prepared` manifest is tenant-owned data, not fresh-root authority; its first
attach verifies the existing disk-root owner and does not recursively re-own
the copied bytes.

Computer disk publication is crash-resumable across its image/manifest pair.
Grow writes exact durable operation intent before resizing in place; copy
retains its staged identity and phase before the staged image can replace the
published image. Helper startup completes or rolls back only a matching record.
An allocation or image/manifest anomaly without that authority quarantines the
exact disk generation through `computer-disk-quarantine` and
`ComputerQuarantines`. The quarantined Computer cannot attach, while its typed
quarantine remains operator-visible and does not withdraw OCI service from
unaffected Computers on the Node. Namespace absence remains verified for all
non-quarantined generations.

Computers share the Node network namespace even though PID, IPC, UTS, mount,
and cgroup namespaces remain private. Image-side local or abstract socket names
should therefore be attempt-unique. The XFCE reference derives its X display
number from the helper-reserved view port instead of claiming a fixed `:99`;
this prevents accidental display-name collisions. It does not isolate X11:
co-located processes in the shared Node network namespace can still reach the
node-wide abstract X socket, which remains a separate isolation risk.

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

The helper sets the Computer's whole-cgroup OOM policy and verifies the live
cgroup before `Started`: `memory.max` equals the declared cap,
`memory.oom.group=1`, and `memory.swap.max=0`. The successful profile receipt
contains those read-back values. The separate atomic admission receipt records
setup-configured capacity, declared memory/disk arithmetic, plus timestamped
`MemTotal`, `MemAvailable`, and disk-root filesystem-free observations;
`MemAvailable` and the 1600 MiB tmpfs ceiling remain warnings/facts and never
reject admission. Disk-root free bytes participate only in the serialized
preallocation decision and never prove post-`Started` exhaustion.

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

JSON types are exact: `version` is an integer (not a boolean) equal to `1`, and
`human_driving` is a boolean (not `0` or `1`). A type mismatch is malformed and
therefore fails closed to `human_driving=false`.

When an image exposes the optional driver conformance oracle, every published
observation has exactly these fields: `version`, `human_driving`, `generation`,
`fingerprint`, and `classification`. Oracle `version` is integer `1`;
`human_driving` is the fail-closed state; `generation` is a monotonically
increasing integer for the lifetime of the observer process; `fingerprint` is
the lowercase hexadecimal SHA-256 of the exact bytes read from `driver.json`;
and `classification` is exactly one of `valid`, `malformed`,
`unknown-version`, or `missing`. A missing source uses the literal fingerprint
sentinel `missing`. The observer must fingerprint and classify the same single
read, publish atomically, and never reset `generation` while an assertion is in
flight. This oracle is diagnostic image-owned evidence, not runtime authority.

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
a guest write. The optional `examples/computer/` XFCE image and
`examples/computer-wayland/` GPU-free Wayland image independently implement
this minimum contract as image-author and acceptance examples. Each exact
platform digest runs the same checker and 20-row broken-image matrix before a
main-only publisher promotes those executed archives into its own immutable
multi-platform index; neither image is a required base or compatibility target.

`wefty-computer-conformance --image` runs these image-owned assertions through
Docker or containerd's `nerdctl`, emits one versioned JSON receipt with stable
check IDs, and prints the same cells as a human summary. Every cell begins as
`NOT-RUN`; an omitted or unavailable input/driver oracle therefore cannot be
folded into an aggregate `PASS`. The reference image supplies both explicit
oracle paths and the secretless required image lane proves every image and
Docker-harness cell `PASS`. Capability-set, seccomp, namespace, device, and
cgroup/OOM/swap read-backs remain explicit `NOT-RUN` cells with the stable
reason `harness profile is not the containerd wefty-v1 profile`; native
containerd acceptance owns those assertions.

Checker teardown asks Docker to stop the container and then removes it so every
bind mount is detached before the checker-owned temporary root is removed. The
pre-stop in-container chmod can race a desktop process creating a later path;
if the detached root is still restrictive, the checker uses an explicitly
supplied, known-good reference digest in a networkless, read-only-root repair
container running as uid 0 with only `DAC_OVERRIDE` and `FOWNER`. Every image
platform job exercises that repair under its actual runtime platform, records
its duration, and enforces the 15-second deadline measured against 118-206 ms
repairs across the four builds in run 33695618869. `EBUSY` and
`ENOTEMPTY` removals are retried every 250 milliseconds within a two-second
scheduled-sleep budget.

The versioned receipt records retry count, permission-repair timing, non-fatal
stop observations, and exact remaining objects. A failed stop is diagnostic
when forced removal succeeds and the root is gone; fail-closed typed errors are
keyed only to a container or temporary root that remains. When container
removal cannot prove bind detachment, the checker deliberately retains the
temporary root under `/tmp/wefty-computer-conformance-*` and names both objects
in the typed error for later runner cleanup. Teardown never changes the row's
assertion result into a non-fatal pass.
