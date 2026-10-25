# Computer image and boot contract

Computer Storage recovery is fail-closed per generation. A valid interrupted
grow or copy record that cannot yet resume remains in place as
`resume_deferred` with an attempt count, first-deferred time, and closed reason.
Operational recovery deferral belongs to the Storage generation rather than
one recovery operation: a later failure updates the live operation while
preserving the generation's first-deferred time and accumulated attempt count.
The helper mirrors that record beside the generation and prefers whichever
primary or mirror copy remains readable, so a one-sided read failure does not
erase Storage identity, attempts, or `first_deferred_at`. When neither copy is
readable, the generation remains countable as `deferral_record_unreadable`
across helper replacement. A primary record bound to a foreign disk is instead
a structural `deferral_record_identity_mismatch` even when the name-bound
mirror is valid. When recovery inspects that record, including the
missing-manifest path, the generation quarantines without Storage authority
and the helper emits the typed mismatch. A valid manifest can instead reach an
operation-specific clear first; clear still refuses the foreign record, while
that path reports the existing `recovery_deferral_invalid` anomaly. A missing
attachment manifest never authorizes deleting an existing image; it
quarantines the bytes unless a valid matching pre-publication copy record
proves that its staged destination was never published and can be rolled back.
Only boot-barrier startup sweeps increment the attempt count; in-session reap
sweeps do not. Recovery terminates as `resume_abandoned` only after both
twenty-four failed agent boot-barrier sweeps and 24 elapsed hours, so a helper
restart storm cannot consume the bound in minutes. Structural image/record
mismatch and invalid authority quarantine immediately. Quarantine retains its
payload for 24 hours; a quarantine whose payload GC has never failed is
collected by the first sweep after that deadline, however late. Its typed
tombstone remains thereafter without an autonomous age bound, so N is never
admissible again; three authority-valid payload-GC
failures escalate and stop automatic retries. The helper tries both the mirror
and primary receipt for each failure update. If neither is writable, typed
in-memory evidence enforces the three-failure bound, and only an observed
memory-only failure may stop at the retention deadline plus 24 hours. A helper
that has no failure evidence attempts GC rather than treating age as evidence.
Inventory and doctor surface the GC count, timestamps, last failure, evidence
location, and escalation count. The supported recovery path
prepares and admits reset generation N+1 and clears N only through authorized
removal. A legacy `-reset-N` directory is only a retained
`legacy_reset_quarantine` tombstone when it lacks a validated historical reset
manifest; its name and modification time never authorize deletion. When that
manifest proves the exact disk generation, the helper normalizes the tombstone
and begins the same receipt-backed retention and payload-drop path from the
normalization time.
For a never-attached Custody import generation, the agent persists an exact
helper-generation `computer_storage_preparation_interrupted`,
`computer_storage_resume_deferred`, or `computer_storage_quarantined` result in
L1's import ledger. The CLI observes that immutable import identity
and revision directly; completion recorded before polling begins is still
visible, while the first durable preparation outcome ends `--wait` immediately
with distinct interrupted/failed, deferred, or quarantined wording and exit
status instead of a generic wait deadline. Preparation acknowledgements carry
their idempotency key and hash. L1 accepts an identical replay, rejects an older
helper generation or timestamp, and keeps the copy directive retryable. A
later successful or terminal copy receipt clears the provisional evidence and
success remains the only route to publication. The terminal import operation
row is retained as immutable idempotency and provenance authority; deleting it
would allow the same source operation to reserve a second destination.

This contract defines the image-owned half of a `computer`-trait OCI service
and the agent's all-or-nothing screen-door readiness verdict. The ratified
authority is the agent-computer spec section 7; this document fixes the seam
that reference images and conformance tooling consume.

Computer client observations follow [the Read model](read-model.md): authority,
current Job, actions, grant policy revision, Storage/Backup/Custody evidence and
take-over views each come from one Read snapshot. `current_job.status` uses the
job view's open vocabulary (`restart-pending`, `unschedulable`, and the other
computed statuses); the CLI Computer table displays it. When removal leaves no
current service Job projection, `services status`, `computers list`, and the
`services remove` result display `N/A` for the service-only columns READY and
LAST FAILURE; the Job status, Job ID and removal outcome still render. JSON
output retains the Job tombstone and Computer removal outcome unchanged.
Intents of a deleted Computer return `not_found`. Post-change Computer views
reload after the commit.
Backup listings accept `cursor` and `limit` (100 by default, clamped to 250),
returning `next_cursor` under the adaptive page cutoff. Storage provenance rows
use the same limits and cutoff, with a Computer-bound creation/ID cursor and
insertion watermark. Custody taint covers imports beyond the current page.
The custody graph, forks and exports remain bounded at 1000: overflow returns
409 `conflict`, `retryable=false`, `reason=storage_custody_limit`, without
truncating custody facts. The CLI walks provenance pages; if refused, Backup
listings and mutation wait outputs keep their operation results and note
`provenance_unavailable`. Applied submission changes retain the revocation
receipt or notice even if their post-change read fails, by falling back to the
committed Computer and its submission policy revision. Submission responses label `inflight_count` with
`inflight_observation="run-ledger"`; the L3 call starts after L1 closes its snapshot.

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

## Computer isolation boundary

Every Computer is isolated from every other Computer on the same Node,
regardless of owner. A Computer may reach its orchestrator channel -- the
helper-reserved `view` and `control` endpoints and the agent directive channel
-- its own Storage, its own screen, and outbound networks through the Node.
Outbound stays open under the #109 profile ruling; the Fabric front door
remains the only supported inbound path. A Computer may not reach any neighbour's
screen, sockets, processes, or files. A real attempt to cross that boundary
must be refused; distinct names or the absence of an accidental collision are
not proof of isolation.

The boundary is ratified by
[ADR-0005](../adr/0005-computer-isolation-boundary.md). Enforcement closes the
known shared-Node gaps mechanism by mechanism. This contract states only the
mechanisms currently proved and does not convert an untested surface into an
isolation claim.

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

Submission is disabled by default and cannot be enabled for `network=none`:
L1 returns HTTP 409 `conflict`, and neither the node agent nor the helper opens
a submission bridge or delivers a pass for that network. When an administrator enables the
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
whether L3 committed the request before revocation. Every request must carry
the pass as `Authorization: Bearer`; the bridge refuses one without it, or one
whose `Connection` header would drop it in transit, with typed `403 forbidden`
before L3 sees it (`run-execution-context.md`). The files remain
attempt-local tmpfs and never enter `/wefty/service`, a JobSpec, logs,
inspection, or removal evidence.

Committing a submission change (enable, disable, or an inflight resize) clears
the Computer's published readiness and `display_endpoint` in the same L1
transaction, so readiness is earned again under the new authority. The running
attempt keeps running. Once the agent has installed the change (a pass minted
and verified at the new revision with both files published, or the transport
closed and both files removed), it republishes that same attempt's current
readiness without waiting for a backend transition or a fresh attempt. The
screen is therefore absent from the commit until the agent's next policy
delivery, one mint, and one publication round trip; if both backends are not
ready at that point, the ordinary atomic readiness rules below decide when it
returns. A re-mint that fails ends the attempt, and the replacement attempt
publishes as usual.

Every ready Computer publication carries `submit_intent_revision`, the
revision whose authority the agent had fully installed when it sent it (the
claim's revision until a change is installed). L1 checks it against the
Computer's current revision inside the publication transaction and refuses a
mismatch with HTTP 409 `stale_policy_revision`, leaving the publication as the
change left it. Readiness earned under revision N therefore cannot restore the
screen after N+1 has committed, however the two race. The agent treats that
refusal as "a newer authority is on its way": it neither retries nor fails
the attempt, and publishes again once it installs the newer revision or its
readiness changes. The field is optional on the wire. An agent that omits it
publishes unfenced as before, and L1 rejects it on ordinary services and on
withdrawals, which are never fenced. Upgrade L1 before or together with the
agents: an L1 that predates the field rejects it as an unknown request field,
so a newer agent's Computer cannot publish its screen there.

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
PID/IPC/UTS/mount/cgroup namespaces, a private network namespace, and deny-all
devices plus the six pseudo-devices. The helper brings up only that namespace's
loopback interface before start. The root filesystem is read-only; the Computer
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

The Computer's fixed network setting selects `internet` or `none`. An omitted
helper setting preserves `internet` for existing callers. Both settings create
a fresh private network namespace. With `network=none`, that namespace has
loopback only: the helper brings `lo` up and verifies that no outward interface,
default route or outward route exists, for either IP family. Kernel-created
fallback tunnels may remain only under their known fallback name and kind
(`tunl0`/`ipip`, `sit0`/`sit`, `ip6tnl0`/`ip6tnl`, `gre0`/`gre`, and the
corresponding GRE tap/ERSPAN, IPv6 and IPsec `ip_vti0`/`vti`, `ip6_vti0`/`vti6`
fallback devices), down and with no IPv4
or IPv6 address. Any other link, active fallback, or addressed fallback is a
typed `oci_spec_rejected` refusal. Kernel-local
loopback routes remain. It creates no veth, NAT, egress proof listener or DNS
proxy. The helper mounts fixed read-only files from its managed
`network/none/` directory: `resolv.conf` names only `127.0.0.1`, with one
one-second attempt, and `hosts` contains only IPv4/IPv6 localhost entries.
No Node hosts snapshot, upstream resolver or search domain enters this profile.
No guest proxy or DNS/IP filter implements this setting; the missing outward
interface and routes are the boundary. The image receives no network
administration capability with either setting.

The helper reaches each Seat's Unix screen sockets through the exact live
attempt's root from outside its network namespace. `ExecComputerSeat` uses
containerd task exec as the Seat's user. Neither transport needs internet.
The existing command stream can also carry command-mediated copies: bytes in
command arguments enter a Seat file, and binary stdout returns a file's bytes.
There is no dependency on a guest TCP file server or DNS. This helper-level
proof does not claim the later `wefty cp` command or end-to-end AP-1 proof.
Seat readiness and `rfb-websocket-v1` greetings check the private screen
endpoints only. They do not probe an upstream network. Liveness remains the
task's running state. The helper records `computer_network=none` and, after
checking the actual namespace, `computer_loopback_only=true`; doctor accepts
that proof without requiring an outward address or DNS/firewall attachment.
A simultaneous `internet` Computer still requires its firewall evidence.
The `computer-seats` CI jobs `default-computer-seats / linux/amd64` and
`default-computer-seats / linux/arm64` prove AP-1 (helper screen, command and
binary copy), AP-2 (web and DNS fail while loopback works), and AP-3 (fresh
readiness probes and task liveness survive those refusals). The root-only test
uses `computer_seat_lane`, never the general non-root realtiming lane.

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

Screen isolation is enforced at the network-namespace boundary. The XFCE
reference still derives its X display number from the helper-reserved view port,
but its abstract `@/tmp/.X11-unix/X<n>` socket exists only in that Computer's
private network namespace: a neighbour can neither enumerate nor connect to it.
The Wayland compositor socket remains inside the Computer's private mount
namespace, and both variants bind their RFB servers only to private loopback.
The filesystem X socket under the attempt-private `/tmp/.X11-unix` is private
by mount namespace; the abstract socket is private by network namespace.
For `view`, `control`, and the agent's Computer submission bridge, the helper
enters only the exact live Computer's network namespace and exposes the existing
attempt-authority-bound stream; the Computer receives no new capability or
privilege.

For `network=internet`, the helper attaches one point-to-point `/30` veth to the private namespace,
installs a default route, retains the helper-managed resolver and hosts mounts,
disables IPv6 through both the namespace `all` and `default` sysctls, and
masquerades the exact Computer IPv4 address through the Node or Lima VM. The
address is allocated from RFC 2544 benchmarking space `198.18.0.0/15`; port
`p` uses the `/30` at byte offset `(p - AttemptPortMin) * 4`. The attempt port
range is capped at 32,768 disjoint `/30`s and helper startup refuses a
conflicting non-Wefty Node route. If the mounted
resolver snapshot names a Node-loopback stub such as `127.0.0.53`, the helper
binds that address only inside the Computer namespace and proxies DNS from the
Node namespace to systemd-resolved's advertised non-loopback uplink after a
bounded lookup proves it reachable, falling back only to a Node stub that
answers the same probe. If neither candidate answers, `Run` fails as
`egress_dns_unavailable`; a routable resolver continues over the
veth. The proxy admits only minimally valid DNS queries and responses, rate
limits each attempt, and bounds concurrent TCP connections. Firewall policy
rejects veth-to-veth forwarding, unsolicited traffic toward a Computer veth,
and every Computer-originated Node-local listener except the exact helper-owned
egress proof endpoint. That exception also requires the owning transparent helper
socket, so closing the helper listener before removing the rule cannot expose a
foreign binder. Mirrored `ip6tables` chains enforce the same refusal if a future
image re-enables IPv6. The endpoint is bound only to the attempt's
gateway/interface tuple and returns no tenant or Node data. The helper verifies
and repairs both chain sets at every Computer start and on its periodic sweep
cadence. Each INPUT, FORWARD, and POSTROUTING jump must be the first rule in
its base chain; a merely present later jump does not prove isolation. Every
helper-owned chain body must also exactly match its canonical ordered rules;
presence without order does not prove the crossover rejection precedes egress
acceptance. Missing kernel `ip6table_nat` support is recorded as
`unavailable_ipv6_disabled` because IPv6 is disabled inside the Computer, but
IPv6 filter failures remain fatal. Startup
sweep removes unowned `wftch*`/`wftcg*` links and IPv4/IPv6 attempt rules, while
orderly helper close tears down live network state and helper-owned Node-wide
firewall/forwarding state. If the helper process crashes after enabling
`net.ipv4.ip_forward`, startup sweep cannot prove its prior Node-wide value and
therefore leaves it enabled; an orderly close restores only a value this helper
observed as disabled. The authority-bound
view, control, and directive paths remain private loopback bridges. Ordinary OCI
jobs keep shared networking unchanged.

Computer submission is available only with `network=internet`. L1 refuses an
admin's enable request for `network=none` with HTTP 409 `conflict`, without
changing submission intent, policy revision, or audit. Disabling remains
available. The node agent does not mint or synchronize a pass, create a
submission bridge, or request a helper bridge for `none`, even if a stale
claim says submission is enabled. The helper independently refuses bridge
preparation and nonempty submission authority for `none`, including later
`SetComputerToken` calls. It exposes no bridge capability or endpoint. Screen,
command, and copy transports continue to work from outside the Computer.
There is no submission exception to the loopback-only rule.

For `internet`, Computer submission uses the helper reverse bridge as its only
transport on every platform. Exactly four bridge pumps may be active per Computer attempt;
additional guest connections wait behind that fixed concurrency bound.

This screen mechanism does not by itself prove the complete isolation boundary.
Other neighbour socket surfaces, process interactions, and file paths retain
their existing namespace, cgroup, read-only-root, Storage, and mount controls,
but complete crossover-refusal coverage for those categories remains a known
gap. Shared operator-selected mounts in particular are not reclassified as
isolated by this change.

The Computer profile creates private, bounded tmpfs mounts in its mount and
IPC namespaces. Their pages count against its cgroup memory limit. At the
1 GiB default the ceilings are `/dev/shm` 256 MiB, `/tmp` 128 MiB, `/var/tmp`
16 MiB, `/run` 32 MiB, `/dev` 32 MiB and `/wefty/seat-status` 1 MiB: 465 MiB
in total. This applies to single-desktop Computers as well as Seat images.
The Docker approximation tests reference images at this same 1 GiB cap and
matches the scaled `/tmp`, `/var/tmp`, `/run`, `/dev/shm` and status sizes.
It leaves Docker's `/dev`, `/dev/pts` and `/dev/mqueue` setup intact and does
not assert `/dev` size; the six-mount total above is real-profile evidence.
The XFCE reference image puts `TMPDIR`, Playwright browser installs and
temporary browser profiles on its disk home; Chromium uses
`--disable-dev-shm-usage` to avoid putting large temp files in the 256 MiB shm.
The 256 MiB shared-memory ceiling leaves Chromium more room than a common
64 MiB mount while reserving at least half the cap for program memory.

All six ceilings are page-aligned and their sum stays at or below half of the
Computer's declared memory cap. `/dev/shm`, `/tmp`, `/var/tmp`, `/run` and
`/dev` scale at 1/4, 1/8, 1/64, 1/32 and 1/32 of memory, with maxima of
1 GiB, 512 MiB, 64 MiB, 64 MiB and 64 MiB. Normal floors are respectively
64, 16, 4, 4 and 4 MiB. For tiny caps the floors yield proportionally to the
half-cap budget; each mount keeps at least one 4 KiB page. Status is at most
1 MiB and shrinks for tiny caps. A cap below 48 KiB cannot hold these six
bounded mounts and is rejected. These are ceilings, not reservations.
The profile receipt reports their actual sum and largest mount.

The status tmpfs is helper-created, root-owned mode `0700`, with
`nosuid,nodev,noexec`. It is writable by the image's root supervisor and is
fresh on each attempt. It has no Node path and is never an operator bind.
Operator mounts remain read-only on Computers, and an operator cannot shadow
the status target or a descendant. The image reports
`attempt_status_mount_required` if this bounded writable tmpfs is absent,
instead of attempting writes through the read-only root filesystem.

The helper sets the Computer's whole-cgroup OOM policy and verifies the live
cgroup before `Started`: `memory.max` equals the declared cap,
`memory.oom.group=1`, and `memory.swap.max=0`. The successful profile receipt
contains those read-back values. The separate atomic admission receipt records
setup-configured capacity, declared memory/disk arithmetic, plus timestamped
`MemTotal`, `MemAvailable`, and disk-root filesystem-free observations;
`MemAvailable` and temp ceilings remain advisory facts and never reject
admission. Both the admission receipt's `computer_tmpfs_ceiling_bytes` and
per-attempt profile receipt report the sum of all six scaled ceilings for the
declared memory limit, including `/dev`, `/run` and status. Disk-root free bytes participate only in
the serialized preallocation decision and never prove post-`Started` exhaustion.

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
subprotocol, RFB-banner length/validator, 60-second deadline, and maximum 1 GiB shm
size. Runtime endpoint aliases and the helper consume those values. The
complete serialized Computer profile is pinned at
`runner/ocihelper/testdata/containerd-v2.3.7/wefty-v1-computer-linux-amd64.json`.

Portable tests prove reserved-value stripping, exact endpoint admission,
wire-negative cases, atomic loss/recovery, injected-clock deadline behavior,
and the serialized profile. The Linux `service_acceptance_realtiming` lane
asserts `/dev/shm` mode, flags, size, and a rising cgroup `memory.current` after
a guest write. The optional `examples/computer/` **`single-seat` target** XFCE image and
`examples/computer-wayland/` GPU-free Wayland image independently implement
this minimum contract as image-author and acceptance examples. Each exact
platform digest runs the same checker and 21-row broken-image matrix before a
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

Every `FAIL` cell also carries a machine-readable `failure_reason`:
`mutation_detected`, `readiness_timeout`, or `assertion_failed`. A readiness
timeout names the exact missing `readiness_event`, such as endpoint readiness,
the first RFB frame, or key-observer advancement, and records both the applied
observation window and elapsed observation. Check and event are validated as a
closed pairing rather than as independent vocabularies. The broken-image matrix
may retain unrelated `readiness_timeout` cells only after the expected check ID
appears exactly once in the receipt and that row carries `mutation_detected`
with the expected detail. An input timeout additionally requires later success
for the same event and an executed `input.control-accepted` cell; an input
timeout not recovered by the end of input evaluation is re-labelled
`assertion_failed` while retaining its diagnostic detail and numeric observation
window and elapsed evidence. An absent or duplicate expected cell, a missing
input-adjacent cell, or any extra `assertion_failed` cell remains a fail-set
failure; the positive reference-image run never ignores a readiness timeout.

The emulated arm64 input-event observation window is the greater of the legacy
1.5-second window and twice that container's observed first-boot endpoint
readiness. amd64 retains the same 1.5-second, 12-poll behavior through the
shared polling loop. A successful WebSocket upgrade waiting for the first RFB
frame is an incomplete readiness event, not proof that plain TCP was accepted.
A separate terminal classification is sampled only after the readiness
observation window is exhausted. It preserves raw-RFB, silent-accept,
accept-then-close, non-upgrade HTTP, and RFB protocol-error detection without
making one transient answer sticky or ending readiness polling early. A
proxy-fronted or still-initializing endpoint may therefore recover from an
intermediate `502` or protocol error, but only a later exact WebSocket and RFB
handshake satisfies readiness; a persistent violation remains terminal at the
unchanged deadline.

This late-only classification intentionally adds approximately 60 seconds to
the `plain-tcp-control` mutation row in each arm64 reference-image lane: its
`python3 -m http.server` listener returns a persistent non-101 HTTP response,
so the row now consumes the existing 60-second readiness deadline before
emitting the unchanged expected detail.

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

### Current hosts snapshot exposure

The helper mounts a read-only snapshot of the Node's `/etc/hosts` into each
Computer. This currently exposes all host-to-IP mappings in that file, including
private Node names and addresses, to every Computer. The snapshot is not filtered
per Computer. The helper refreshes its managed network files before starting an
attempt; existing mounts of that shared snapshot can observe a later refresh.
Knowledge of these
names does not grant network access: the Computer network boundary still refuses
Node listeners and cross-Computer traffic. This describes the current exposure;
it does not extend the Computer's access authority.

The initial post-Start network namespace inode check remains fail closed. Endpoint
readiness alone does not establish X binding: custom images may bind X later, and
Wayland images may never use X. After an owned view/control endpoint is reachable,
the helper reobserves both namespace inodes and probes the exact X token inside
the target namespace without waiting. X absence in the helper namespace is verified
only if the target token is live. Missing target X evidence remains `NOT-RUN`,
including for images without X; this does not infer that an image uses Wayland.
Contradictory namespace, host socket or firewall observations remain failures.
The shipped XFCE image currently starts X before its endpoints, but this is not a
global image ordering requirement.

The DNS proxy discards upstream responses whose questions do not match the query,
including QDCOUNT-zero error replies. Such unauthenticated error shapes are not
forwarded; a client may time out rather than receive the upstream error.

## Default image and Seats (Computer request P5, P6, P7, P8, P9)

The final `seats` target in `examples/computer/Dockerfile` is the default
Computer image. It holds Debian, XFCE, Xvfb, Chromium, `git`, `curl`, Python 3,
Node.js **24.22.1 LTS** and its bundled npm. Both Node.js archive checksums
are pinned from the official `v24.22.1/SHASUMS256.txt`; Debian's Node 18 is
not installed. Its browser system libraries cover Playwright 1.56.1's Debian
12 Chromium, Firefox and WebKit dependency lists on amd64 and arm64. AI agents
install browser binaries in their Seat folder. Generic OCI images need not
derive from it. The separate `single-seat` target retains the earlier optional
XFCE reference image and acceptance contract.

Four passwd/group identities, `seat1`–`seat4` (uid/gid 2001–2004), set this
image's highest Seat limit. Their homes are
`/wefty/service/seats/<place>/home`. A numeric uid without a passwd entry is
refused as `passwd_entry_missing` before desktop startup. User creation stays
in the read-only image; password age uses fixed non-zero day 20000.

### Runtime required by every multi-Seat image

Every multi-Seat image must provide executable `/usr/bin/python3`
(Python 3.8 or newer), with the standard library modules `array`, `base64`,
`errno`, `fcntl`, `json`, `os`, `selectors`, `signal`, `subprocess`, `sys`,
`termios`, and `time`, including `os.set_blocking`, `os.setsid`, and
`signal.pthread_sigmask`. It must run as each Seat uid with
`-I -S`. This applies to custom multi-Seat images as well as the default image;
it does not apply to generic OCI images or images without Seats.

The helper uses an embedded, isolated Python wrapper for Seat commands. It
runs inside the existing task, creates the command's process group, bounds
output buffering, and keeps detached programs from holding a finished command's
output open. Using the existing interpreter avoids mounting and managing a
second helper-owned executable. No Seat-installed Python module is imported.
See [the helper operation](oci-helper-protocol.md#commands-inside-a-ready-seat)
for environment, stream and cancel semantics. A normal command end also
preserves plain background writers that retain
stdout/stderr. A bounded Seat-uid discard reader keeps those pipes readable
without extending the Run or holding helper connection slots. The image's
uid-wide cleanup ends both writer and reader. Idle readers use blocking
select. Their count is not separately capped: closing a reader at a count limit
would break background logging, and the Seat uid can already fork processes.
Readers stay in the Computer's existing memory limit; aggregate per-Seat reader
memory is not guaranteed. See the helper contract for the five-second output
drain and deletion bounds. Process-group scope on cancel
and runtime limit stays unchanged pending the owner decision documented at
that helper seam.

`CheckSeatCommandRuntime` in `internal/computerconformance` probes the exact
absolute interpreter path and required imports with `-I -S` as a Seat user.
A missing or incompatible interpreter is refused with the clear requirement
message, before Seat exercises. The resulting `seat_command_python` observation
is mandatory for a passing `seats.command-runtime` check: false is `FAIL`
with the executable requirement in its detail, and missing evidence is
`NOT-RUN`. The native two-architecture Seat lane records this probe.

### Directive, readiness and positive cleanup proof

P5 binds the **directory** `/wefty/control` read-only. The helper atomically
replaces `seats.json`; the supervisor reopens it on every poll:

```json
{"version":1,"generation":1,"seats":[{"place":1,"user":"seat1","view_port":7102,"control_port":7103,"view_socket":"/wefty/service/seats/1/run/view.ws","control_socket":"/wefty/service/seats/1/run/control.ws"}]}
```

`place` is 1–4. A public Seat ID remains `seat_<hex>` and is never the place.
The positive attempt-local generation increases on each directive. Duplicate
places or ports, incorrect users or socket paths, unknown fields, malformed
documents, missing directives and replayed generations fail closed. A refused document logs
once per replacement, not once per poll. Only a read error resets that
observation. An unchanged starting Seat keeps its original 60-second deadline
when another Seat is added. Failed Seats do not retry on an unrelated change;
remove the place before reuse.

P6 uses the profile's private 1 MiB `/wefty/seat-status` tmpfs, not a writable
operator mount. The supervisor atomically replaces `<place>.json` with mode
`0600`, using descriptor-based, no-follow operations. Before reading, the
helper verifies the attempt's `/wefty/seat-status` mount in its mountinfo:
exactly one private writable tmpfs, `nosuid,nodev,noexec`, bounded to at most
1 MiB, with no shadowing mounts beneath it. It anchors reads at the live
attempt's kernel root descriptor, then uses
`openat2(RESOLVE_IN_ROOT|RESOLVE_NO_SYMLINKS)` or `O_NOFOLLOW` on every path
component. Before reading answers, it compares the opened status directory's
`fstat` `st_dev` major/minor to the validated mountinfo `major:minor`; a
mismatch closes the descriptor and refuses the read. This binds the no-follow
opened directory to the mount that was checked. Each answer opens with
`O_NONBLOCK`, must be a regular file, and
is limited to 8192 bytes (including a bounded read if it grows after `stat`).
Links, FIFOs, devices and oversized files never count as cleanup evidence.
The native Seat lane uses the same reader rule. Answers include:

```json
{"version":1,"generation":1,"place":1,"state":"ready","reason":"","cleanup_complete":false}
{"version":1,"generation":2,"place":1,"state":"removed","reason":"","cleanup_complete":true}
{"version":1,"generation":2,"place":1,"state":"failed","reason":"directive_refused","cleanup_complete":true}
{"version":1,"generation":2,"place":1,"state":"failed","reason":"cleanup_failed","cleanup_complete":false}
```

Readiness requires the browser, a 1280×800 desktop and both endpoints' binary
`rfb-websocket-v1` greetings before that Seat's own deadline. A readiness probe
which crosses the deadline cannot publish ready. The helper independently
checks the endpoints and its own deadline, bound to the exact attempt,
generation and place. A ready file alone never publishes a Seat.

Removal keeps a generation-bound `removed` answer instead of deleting proof.
If the image stops a Seat itself, it writes `failed` and its reason, with
`cleanup_complete:true` only after cleanup succeeds. If cleanup fails, it
writes `failed/cleanup_failed`, keeps that place blocked and retries cleanup
after 30 seconds, doubling each failed retry up to 300 seconds. Scratch-walk
exhaustion (`cleanup_scratch_budget`) and persistent foreign-folder changes
(`cleanup_path_replaced`) use this same failure and retry path. An
unexpected cleanup exception has the same per-place result, including during
startup; it does not end other Seats. No new desktop starts in a blocked
place. **Room returns only after positive cleanup evidence** for that
attempt, directive generation and place; a missing file, a failed cleanup or
an elapsed deadline is not such evidence (spec 2.8.3).

After a valid directive is fully applied without blocked cleanup, the image
also atomically writes `applied.json`:

```json
{"version":1,"generation":2,"wanted_places":[],"cleanup_complete":true}
```

This proves absence for places omitted by that generation, even if an earlier
add directive was replaced before the image observed it. It does not say that
wanted desktops are ready. A stale applied generation never releases Room.
Changing the config of an already wanted place in place is refused as
`in_place_change_requires_remove`. Like other refused directives this stops
**all currently wanted Seats**, including unchanged places, with
`failed/directive_refused`, never a `removed` answer for the still-wanted
config. For helper #1137: **never send an in-place config change**. Send a
new generation omitting the place and retaining the other unchanged entries;
observe that generation's positive cleanup proof, then add the new config in
a later generation. Even after refusal cleanup succeeds, a valid directive
omitting the refused place is required before another add.
Helper/L1 binding and Room updates remain with their wave owners.

### Desktop recovery, folders and root path safety

Each Seat runs under its own uid/gid with no supplementary groups. Its private
X cookie and display `:<place>` use X TCP disabled. Its mode-`0700` disk folder
contains `home`, `run`, and `tmp`; browser profiles, package caches and
`PLAYWRIGHT_BROWSERS_PATH` stay there too. All installs belong there. System
software changes only through a new image.

The launcher proves that its own Xvfb is alive after the display wait. A browser,
driver, desktop session or endpoint exit restarts that component and its
process group without ending the Seat. Three restarts per component within
60 seconds are allowed; the next failure ends the launcher. A dead display
ends it immediately. The root supervisor then cleans the Seat and reports
`restart_limit` or `display_exited` from the launcher; an unreported launcher
exit is `desktop_exited`. The session controller runs under the Seat uid.
A Seat's own `killall python3` or `pkill -u` can kill that controller and ends
the Seat; the root supervisor cleans all its remaining processes. Browser
readiness is polled only until the first ready after a browser start. Only a
browser restart resets that polling; edge and driver restarts after the user
navigates elsewhere must not restart page-title polling.

Before browser relaunch, the launcher marks the existing Default profile's
exit type `Normal` and `exited_cleanly:true`, and passes
`--hide-crash-restore-bubble`. Cookies and unrelated preferences stay intact.
The old browser process group has stopped before these changes and relaunch.
Restart policy uses an injected clock in unit tests; the Linux lane kills a
browser and the edge process and checks that the same Seat recovers. Its wait
failure names the component and retains the last probe output, including
observed colours; each started attempt's Computer log is copied into CI evidence
before temporary cleanup, on both success and failure.

The lane's screen probe samples several rows across the band width, allowing
small vertical shifts and narrow overlays. At least three rows must contain
80 percent expected-colour pixels, and at least 80 percent of clear coloured
rows must agree. View-input still observes the full two-second window and
fails on any observed green band row, even for one frame; the control-input
probe waits for green. Both input probes first observe the page and click a
matching pixel near the centre of its visible colour band, below browser
chrome and clear of narrow overlays. Control input must change the colour;
an already-green baseline cannot prove input delivery. The lane selects the
Seat's control Unix socket for control input and its view Unix socket for
view-input and screen observations. Probe output records the endpoint, click
position,
and colours before and after input on success or failure. Failures retain the
last completed samples when a frame has been observed.

P8 reads the Seat's own read-only
`/wefty/control/seats/<place>/driver.json`. Its body remains exactly
`{"version":1,"human_driving":true}` or `false`. Missing or invalid data
produces local false state in that Seat's `run/driver-state.json`.

### Per-Seat endpoint transport (P7, IM-4)

**Spike decision (#1138): use filesystem Unix sockets.** Linux can pin a socket
with `O_PATH|O_NOFOLLOW` and connect through `/proc/self/fd/<fd>` from the helper,
without entering or trusting an image pathname during connect. The existing
websockify 0.10 `LibProxyServer` needs a small Unix-listener adapter and must
ignore its TCP-only `TCP_NODELAY` option for Unix streams. A real binary
WebSocket/RFB handshake covers that adapter. No per-Seat password is needed.

The directive names exactly `/wefty/service/seats/<place>/run/view.ws` and
`control.ws`, never caller-selected paths. The supervisor rejects any other
spelling, including traversal or another place. The earlier `view_port` and
`control_port` fields remain unique directive metadata for the helper API;
**the Seat image binds no TCP listener at either number**. They cannot be
used to reach a Seat. Older single-desktop images retain their port contract.

`seat-endpoints.sh` binds both `rfb-websocket-v1` edges as mode-0600 filesystem
Unix sockets in the Seat's mode-0700 runtime folder. Only that Seat's uid and
root can open them. The underlying `view.sock` and `control.sock` RFB sockets
are equally private. `/websockify` and the `binary` subprotocol stay unchanged.
View still drops pointer and key input server-side; control accepts it.
The root supervisor opens endpoint parents without following links, requires
Seat-owned private socket files, and compares parent and socket device/inode
identities before connection and after the greeting. Linux peer uid checks
also reject a link-and-restore to another Seat during connect. Links or
replacements cannot earn image readiness; the helper still makes its own
independent descriptor-anchored probes.
X TCP stays disabled and each display requires its own private X cookie.

The helper returns opaque view/control endpoint names in each ready Seat
observation, and accepts them only with that exact Computer attempt, current
directive generation and place. See [the helper contract](oci-helper-protocol.md#per-seat-endpoint-streams-p7).
It descends without following image links, pins socket identities and checks
the listening peer's Seat uid. A replacement never redirects an old name;
a later successful observation publishes fresh names after desktop recovery.

Both `default-computer-seats / linux/amd64` and `/ linux/arm64` in
`computer-seats.yml` attempt real connections from each Seat uid to the other
Seat's view/control edges and RFB backends, and try the other display with the
wrong cookie. Both live Computers also have Seat 1 with identical uid and
socket paths but different frame colours. Probes from each Seat user must
show only that Computer's colour. The helper lane checks those colours through
its named streams too, bracketing cross-Computer name refusals. A distinct live
abstract X token additionally tests network-namespace separation. Missing
crossover observations keep `IM-4.endpoints`
`NOT-RUN`; an observed successful crossover fails conformance.

At startup the root supervisor refuses a non-root-owned or group/other-writable
`/wefty/service`. It `lstat`-checks `seats` and `shared`, refuses links or wrong
owners, and secures directories through held file descriptors. Every path
component is opened with `O_NOFOLLOW`; mode and owner changes use `fchmod` and
`fchown`. Deletion walks iteratively through verified directory descriptors,
uses a bounded number of descriptors at any depth, unlinks links, and never
follows their targets or crosses filesystems.

If an older image left the Storage root mode `0777`, stop the Computer.
A Node owner with maintenance access to the offline Storage filesystem must
verify the attached Storage root, then restore that root's numeric ownership
to `0:0` and mode to `0755` (or stricter) before starting the Computer again.
On Linux this is `chown 0:0 <storage-root>` and `chmod 0755 <storage-root>`.
Fix the root only; do not recursively change Seat or shared-file ownership.
The root supervisor refuses unsafe ownership or write permission rather than
silently trusting a tenant-writable parent.

The removal lane checks all four `.ws`/`.sock` paths with `lstat` and scans
that Computer's `/proc/net/unix` for the Seat prefix and both display tokens.
A dangling endpoint path or an unlinked-but-live socket fails absence; closed
legacy TCP ports do not prove Seat cleanup.

Cleanup kills every process carrying the Seat uid, including detached and
double-forked children. It removes that uid's SysV shared memory, semaphore
sets and message queues, and POSIX `/dev/mqueue` entries **as the Seat uid**;
root intentionally has no `CAP_IPC_OWNER`. The cleaner runs as
`/usr/bin/python3 -I -S`, with cwd `/` and a clean environment, so Seat Python
user-site hooks cannot run during cleanup. It then proves process absence
again, removes uid-owned scratch (including X sockets/locks and POSIX shared
memory), and deletes the Seat folder. The scratch sweep walks `/tmp`,
`/var/tmp`, `/dev/shm`, `/run` and `/tmp/.X11-unix` at every depth, bottom-up,
without following symlinks or crossing nested mounts (`st_dev`). It descends
through folders owned by other uids, removes only entries owned by the ending
Seat uid, and retains other uids' files and folders. An owned folder that still
contains another uid's entries also stays, but the supervisor transfers it
to `0:0` through its no-follow directory descriptor before that place's uid
can be reused. After `fchown`, it uses `fchmod` to restore ordinary permission
bits and sticky, with `S_ISUID|S_ISGID` masked out. The sweep tries `rmdir`
once; `ENOTEMPTY` or `EEXIST` means retain, without rescanning the folder.
Owned symlinks are unlinked without touching their targets. In uid mode,
every disappearance of an entry whose owner is known triggers a new full pass:
directory open, unlink, ascent stat, opening `..`, or `rmdir`. An entry that
vanishes before its owner can be read is skipped: other Seats' short-lived
files vanish there constantly, and retrying on them would hold this Seat's
room while another Seat stays busy. No pass that triggers a retry publishes
positive cleanup evidence; other cleanup errors produce `cleanup_failed` without positive `cleanup_complete` evidence. One sweep has a
**30-second monotonic deadline**
and a **200,000-entry enumeration budget**, shared across all scratch roots
and retries. Bounded enumeration also limits wide and deep foreign trees;
exhaustion raises `cleanup_scratch_budget` and follows that same
`failed/cleanup_failed` path without positive evidence. Checks surround
bounded filesystem operations; they cannot preempt a blocked kernel call.

If a folder owned by another uid changes type or identity during traversal,
the supervisor drops that top-level subtree for the pass, finishes other
entries and retries from all scratch roots, for at most **three passes**.
Persistent changes or disappearances raise `cleanup_path_replaced`. Observed
identity or type replacements of uid-owned paths still fail immediately with
that token. Storage deletion (`uid=None`) keeps
its original fail-closed handling of vanished entries during unlink, directory
open and ascent. A live foreign Seat can still move a file before its source
subtree is enumerated into an already-visited folder. Without an observed
disappearance or identity change, the sweep can miss that file. This
live-writer race cannot be eliminated by a filesystem walk.

`/wefty/service/shared` is mode `1777` and survives. A new attempt has a fresh
status tmpfs and control directory;
startup clears stale Seat folders and starts no Seat without a fresh directive.

### Temp space, install measurement and acceptance

Seat `TMPDIR`, runtime data, npm/pip caches and browser caches use its disk
folder, reserving zero additional memory-backed temp bytes. Chromium's
`--disable-dev-shm-usage` also moves large browser temp files there. The shared
profile ceilings above total **465 MiB at a 1 GiB cap**, with **256 MiB shm**.
The receipt judges temp ceilings against at most a **1 GiB Computer**, even
if input evidence claims a larger cap; more than half of that cap cannot PASS.

The Linux lane prefetches `playwright@1.56.1` and Chromium into a read-only
fixture because the test Computer has loopback networking only. Its Seat
copies the kit into its own home and launches it there. `install_bytes` records
`du -sb home/test-kit` (package, lockfile and browsers), excluding npm download
cache and the installed desktop. A numeric install result remains `NOT-RUN`
until CI produces it. Reclaimed memory requires at least **64 MiB**, sustained
for three samples; the receipt keeps before/after byte counts and rejects
ordinary sample noise.

The independent **`computer-seats`** workflow runs
`default-computer-seats / linux/amd64` and `/ linux/arm64` natively. Relevant
pull requests trigger it once; main pushes and manual dispatch also run it.
It is absent from `acceptance-image-build`, `all-tests-pass` and the existing
realtiming lane's dependencies. On main only, its publisher promotes the
exact tested archives after rechecking both platform digests and observations.
Failures repair root-owned evidence permissions before artifact upload.

`TestDefaultComputerSeatsAtProductionTimings` calls the real
`ocihelper.BuildRuntimeSpec`, uses its returned bytes unchanged, and compares
them with containerd's stored spec. The **1 GiB** live cgroup must report the
same `memory.max`, `memory.swap.max=0` and `memory.oom.group=1`. The lane owns
IM-1, IM-2 and IM-6, plus secondary IM-4 display/file and IM-5 cleanup/memory
proof. The helper IM-5 fixture splits 128 MiB of shared-memory charge between
a top-level file and a nested file inside a root-owned sticky scratch folder.
Both files must disappear, the other uid's folder and file must survive, and
the charge must return below the existing recovery tolerance. It also checks
IPC removal, X socket/lock absence, component recovery,
and the missing-passwd negative image under that same profile.

This is image evidence, not helper/L1/CLI integration. Storage is a directory
instead of a loop-mounted disk; control is an explicit read-only bind and
networking is loopback only. Lease, renewal, ownership, NAT/DNS and the host
bridge remain with their primary owners. The view-input observation lasts
two seconds and is followed by control input using the same bytes.

```sh
wefty-computer-conformance --seat-evidence amd64-seats-observed.json --receipt seats.json
```

This mode evaluates supplied runtime observations; it does not launch the
image. Missing proof stays `NOT-RUN`, false proof fails, and P7 always stays
`NOT-RUN`. The main-only publisher emits
`ghcr.io/derek-x-wang/wefty-computer-default` and the artifact
`wefty-computer-default-<commit>` with the digest-pinned index reference,
verified multi-platform OCI archive and both Seat reports. Anonymous manifest
retrieval must succeed. Local checks do not imply image or publication PASS.

### Per-Seat driver publication (#1147, P8)

The helper initializes `/wefty/control/seats/<place>/driver.json` to exactly
`{"version":1,"human_driving":false}` before publishing a new Seat directive.
`SetSeatDriver` binds the full Attempt authority, current helper generation
and place. It writes the same version-1 true/false body by atomic rename with
mode 0444 in the image's read-only control mount. One Seat's tenure never
changes another Seat's driver file. The node sets true before dialing a human
control endpoint, and clears it after closing that tenure's input leg. A
successful person-to-person override keeps true throughout. The old image's
single `/wefty/control/driver.json` contract remains unchanged.
Removing a Seat also clears its own driver file before the removal directive
is published. This covers release after the Seat no longer has live L1
authority, and preserves every other Seat's signal.
