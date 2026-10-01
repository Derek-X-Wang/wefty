# Lima current-state research (ticket #103)

Date: 2026-08-20. Researcher: wayfinder agent (issue #103, part of #101).

**Version pin:** all "verified-current" claims below are against **Lima v2.2.0**
(latest stable, released **2026-07-21**; preceded by v2.1.4 on 2026-07-03) and the
docs/templates at the `v2.2.0` git tag.
Receipt: <https://github.com/lima-vm/lima/releases> — release list shows "v2.2.0 —
July 21, 2026" as latest.

Legend: **[current]** = verified against v2.2.0 docs/tag or 2026 activity;
**[historical]** = older source, believed still true unless noted.

---

## 1. Mount model

**Mounts are VM-level config, not job-time binds.** [current]
The mount list, mount type, and writable flag live in the instance YAML and are
applied at `limactl start`. There is no API to attach a new host directory to a
running VM; changing mounts means `limactl edit` + restart.
Receipts:
- Mount doc shows mount type selected at start time: "`limactl start --mount-type=reverse-sshfs`" and the YAML `mountType` key. <https://lima-vm.io/docs/config/mount/>
- `templates/default.yaml@v2.2.0` line 58: `mounts: []` with "🟢 Builtin default: [] (Mount nothing)". <https://github.com/lima-vm/lima/blob/v2.2.0/templates/default.yaml>
- FAQ "Filesystem is not writable": "Run `limactl edit <INSTANCE>` to open the YAML editor for an existing instance." (edit + restart is the documented change path). <https://lima-vm.io/docs/faq/>

**virtiofs is the default on vz.** [current]
Mount doc default table: "| >= 1.0 | 9p for QEMU (on non-Windows), virtiofs for VZ |";
virtiofs requirement row: "Lima >= 0.14, macOS >= 13.0", and "For macOS, the
'virtiofs' mount type is supported only on macOS 13 or above with `vmType: vz` config."
vz is itself the default vmType: default.yaml: "🟢 Builtin default: `vz` (on macOS
13.5 and later), `qemu` (on others)".
Receipts: <https://lima-vm.io/docs/config/mount/>, <https://github.com/lima-vm/lima/blob/v2.2.0/templates/default.yaml>

**Host↔guest path translation: identity by default.** [current]
Each mount has `location` (host path) and `mountPoint` (guest path);
default.yaml@v2.2.0: "`mountPoint`: … 🟢 Builtin default: value of location".
So `/Users/foo/src` on the host appears at `/Users/foo/src` in the guest unless
overridden — a host path can be passed into the guest verbatim.
`mountPoint` supports template vars: "`\"mountPoint\"` can use these template
variables: {{.Home}}, {{.Name}}, {{.Hostname}}, {{.UID}}, {{.User}}, and {{.Param.Key}}."
Receipt: <https://github.com/lima-vm/lima/blob/v2.2.0/templates/default.yaml> (lines 54–62).

**Writable is opt-in.** [current]
default.yaml: "`writable` … 🟢 Builtin default: false"; FAQ: "The home directory is
mounted as read-only by default. To enable writing, specify `writable: true` in the
YAML." Also: "Setting `writable` to true is discouraged when mountType is set to
\"reverse-sshfs\"" (no such warning for virtiofs).
Receipts: <https://github.com/lima-vm/lima/blob/v2.2.0/templates/default.yaml>, <https://lima-vm.io/docs/faq/>

**Performance for git-worktree workloads.** [mixed]
- FAQ "Filesystem is slow": "Try virtiofs." — virtiofs is the fastest documented
  option and already the vz default. [current] <https://lima-vm.io/docs/faq/>
- inotify across the mount is experimental and off by default: default.yaml:
  "Enable inotify support for mounted directories (EXPERIMENTAL) … 🟢 Builtin
  default: Disabled by default", and the mount doc notes inotify "will be enabled
  only for writable mounts". [current]
- Known open defect for writable virtiofs on vz: lima-vm/lima#2437 "Unreliable
  permissions for lima with vz and writable virtiofs home directory mount"
  (opened 2024-06-22, **still open**): intermittent "Operation not permitted" on
  chmod during heavy writes ("failed to set permissions for file … Caused by:
  Operation not permitted"). Relevant to `git worktree add` / checkout bursts on a
  writable shared mount. [current: open as of 2026-08-20]
  <https://github.com/lima-vm/lima/issues/2437>
- No primary-source benchmark exists for git-metadata workloads on virtiofs; the
  docs only benchmark network throughput, not filesystem ops. Treat "virtiofs is
  fast enough for worktree churn" as **unverified** — needs local measurement.

## 2. Forwarding the guest containerd socket to the host

**Mechanism: `portForwards` with `guestSocket`/`hostSocket`.** [current]
default.yaml@v2.2.0 documents unix-socket forwarding:

```yaml
# - guestSocket: "/run/user/{{.UID}}/my.sock"
#   hostSocket: mysocket
```

with rules: "Forwarding requires the lima user to have rw access to the
\"guestsocket\", and the local user rwx access to the directory of the
\"hostsocket\"."; "Sockets can also be forwarded to ports and vice versa, but not
to/from a range of ports."; "Put sockets into \"{{.Dir}}/sock\" to avoid collision
with Lima internal sockets!"
Receipt: <https://github.com/lima-vm/lima/blob/v2.2.0/templates/default.yaml> (lines 543–551).

The shipped `templates/docker.yaml@v2.2.0` uses exactly this pattern in production:

```yaml
portForwards:
- guestSocket: "/run/user/{{.UID}}/docker.sock"
  hostSocket: "{{.Dir}}/sock/docker.sock"
```

Receipt: <https://github.com/lima-vm/lima/blob/v2.2.0/templates/docker.yaml> (lines 70–72).

**Maintainer position: forwarding the containerd socket works the same way — for
rootful containerd.** [historical, 2023; no contrary changes since]
Discussion #1275 (answered 2023-01): "There is nothing magical about it; you can
just forward it from the VM to the host, the same way the docker example does it."
Caveat in the same answer: "not all operations can be performed over the socket,
but might need direct access to the storage layer (e.g. `nerdctl build`)."
Receipt: <https://github.com/lima-vm/lima/discussions/1275>

**The mount-namespace caveat (#1417) — status today: unchanged, answered,
no activity since 2023-03-29.** [current check on a historical source]
For **rootless** containerd the socket lives inside rootlesskit's mount namespace.
AkihiroSuda: "The socket path is
`/proc/$(cat $XDG_RUNTIME_DIR/containerd-rootless/child_pid)/root/run/containerd/containerd.sock`
in the guest, but it is not exposed to the host, as most containerd operations
needs the daemon and the client to share the same filesystem." A symlink workaround
was rejected because "a client program has to join the daemon's mountNS anyway",
and the `child_pid` changes every boot. The discussion has zero comments from
2024–2026; nothing in v2.x release notes supersedes it.
Receipt: <https://github.com/lima-vm/lima/discussions/1417>
Consequence: **host-side socket forwarding is only viable against rootful
(`containerd.system`) containerd at `/run/containerd/containerd.sock`**; the
rootless socket path is unstable across boots and namespace-bound.

**Reliability / restart behavior.** [current, partially inferred]
- Socket forwards are executed by the hostagent (SSH forwarder spawns an SSH
  master child; gRPC tunnels ride the existing host↔guest gRPC channel). Default
  forwarder since v1.1 is gRPC: port doc table "| v1.1.0 | GRPC |" and "the
  stability issues were resolved". v2.1.2 fixed a "gRPC tunnel connection leak".
  Receipts: <https://lima-vm.io/docs/config/port/>, <https://github.com/lima-vm/lima/releases>
- Inference (marked): when the hostagent dies, the host-side socket in
  `{{.Dir}}/sock/` stops being serviced — clients see connection refused/stale
  socket; a `limactl start` recreates forwards. Not explicitly documented.
- Guest-side restart: if containerd restarts in the guest the guest socket path is
  recreated (systemd), and the forward re-targets it on next connection; for
  rootless the `/proc/<pid>/...` path breaks every VM boot (#1417, #1275 — the
  confirmed fix in #1275 was "deleting stale symlinks on VM restart"). [historical]

## 3. Rootful vs rootless containerd templates

**Defaults.** [current] default.yaml@v2.2.0:

```yaml
containerd:
  # Enable system-wide (aka rootful) containerd … # 🟢 Builtin default: false
  system: null
  # Enable user-scoped (aka rootless) containerd … # 🟢 Builtin default: true for Linux x86_64 and aarch64 guests, false otherwise
  user: null
```

So the stock template gives **rootless containerd by default; rootful is opt-in**
(`limactl start --containerd=system` or `containerd.system: true`).
Also: "Note that `nerdctl.lima` only works in rootless mode; you have to use
`lima sudo nerdctl ...` to use rootful containerd with nerdctl."
Receipts: <https://github.com/lima-vm/lima/blob/v2.2.0/templates/default.yaml> (lines 186–194), <https://lima-vm.io/docs/examples/containers/containerd/>

**Consequences.**
- Socket access from host: rootful socket is at a stable path forwardable via
  `guestSocket` (needs lima user rw access — i.e. group membership or a
  provisioning chmod); rootless socket is namespace-bound and effectively
  host-inaccessible (#1417). [current+historical, see §2]
- Port binding: with rootless containerd, in-guest published ports are bound by an
  unprivileged user process, so guest ports <1024 are not bindable without extra
  setup (standard rootless limitation; Lima docs don't add an exception). Lima's
  own forwarding then re-publishes guest-loopback ports onto the host loopback
  regardless of which mode bound them (§4). [inference from standard rootless
  semantics + Lima port docs; Lima primary sources do not spell this out]
- Mounts: rootful containers can use the virtiofs mount contents directly via
  bind-mounts as root; rootless containers access them as the unprivileged guest
  user — combined with the writable-virtiofs permission flake (#2437) this is the
  riskier path for write-heavy workloads. [inference, flagged]

## 4. Autostart (`limactl autostart`)

**Status: new and current in v2.2.0; replaces `start-at-login`.** [current]
Usage doc (v2.2.0 tag, "⚡ Requirement | Lima >= 2.2"): "Two conditions are
supported: `login` (start when the user logs in) and `boot` (start at system boot,
before any user session). This replaces the older `limactl start-at-login`
command, which is deprecated as of Lima v2.2."
Deprecated-features page confirms: "limactl start-at-login command: deprecated in
Lima v2.2.0 (Use limactl autostart instead…)".
Receipts: <https://github.com/lima-vm/lima/blob/v2.2.0/website/content/en/docs/usage/autostart.md>, <https://lima-vm.io/docs/releases/deprecated/>, <https://lima-vm.io/docs/reference/limactl_autostart_enable/>

**`--condition=boot` mechanics.** [current]
- "This installs a system LaunchDaemon that starts the instance at boot, before
  any user logs in." … "Register (prompts for sudo once)" … "The plist is
  installed to `/Library/LaunchDaemons/io.lima-vm.daemon.<instance>.plist`."
  `--user` selects the macOS user the instance runs as (default `$USER`); macOS only.
- The v2.2.0 daemon plist template (`pkg/autostart/launchd/io.lima-vm.daemon.INSTANCE.plist`)
  runs `limactl start <instance> --foreground` with `RunAtLoad=true`,
  `ProcessType=Background`, logs to `launchd.stderr.log`/`launchd.stdout.log` in
  the instance dir — and **no `KeepAlive` key**.
Receipts: <https://github.com/lima-vm/lima/blob/v2.2.0/website/content/en/docs/usage/autostart.md>, <https://github.com/lima-vm/lima/blob/v2.2.0/pkg/autostart/launchd/io.lima-vm.daemon.INSTANCE.plist>

**Recovery semantics.** [current, partly inferred]
- After host reboot: launchd starts the instance at boot (`RunAtLoad`). Verified
  by doc + plist.
- After crash of the `limactl start --foreground` process: with no `KeepAlive`,
  launchd does **not** restart it — the VM stays down until reboot or manual
  `limactl start`. [inference from the shipped plist contents]
- After `limactl stop`: same — nothing relaunches it until next boot. [inference]
- **Open bug to watch:** lima-vm/lima#5087 (opened 2026-06-06, **open**),
  "bug(vz/launchdaemon): VZ driver leaves VM orphaned on shutdown; limactl start
  fails to recover broken state": on system shutdown the VZ driver can't honor
  SIGTERM ("vz: CanRequestStop is not supported"), the driver process is orphaned,
  and after reboot the instance is `Broken` with "errors inspecting instance:
  [vz driver is running but host agent is not]" — start attempts fail until a
  manual `limactl stop --force`. (Reported against 2.1.1 + a dev build using
  KeepAlive; the failure-to-recover-Broken part applies generally.)
  Receipt: <https://github.com/lima-vm/lima/issues/5087>
  Consequence: **boot-autostart on vz is not yet a hands-off recovery story;
  a supervisor should detect Broken and issue `limactl stop --force` + `start`.**

## 5. Host↔guest networking

**Default user-mode network.** [current]
- "By default Lima only enables the user-mode networking aka 'slirp'. The subnet
  is hard-coded to `192.168.5.0/24`." Guest IP: "The guest IP address is set to
  `192.168.5.15`. … This IP address is not accessible from the host by design."
- Guest→host: "The loopback addresses of the host is `192.168.5.2` and is
  accessible from the guest as `host.lima.internal`." **This is the answer to the
  L3-bridge question: a guest container reaches a host-loopback listener by
  dialing `host.lima.internal:<port>`** (the docker template even aliases
  `host.docker.internal: host.lima.internal`).
Receipts: <https://lima-vm.io/docs/config/network/user/>, <https://github.com/lima-vm/lima/blob/v2.2.0/templates/docker.yaml> (lines 68–69).

**Host→guest TCP (container endpoint).** [current]
Two options:
1. **Guest-loopback port forwarding (default):** "Lima supports automatic
   port-forwarding of localhost ports from guest to host." Matching defaults in
   default.yaml: "default: guestIP: \"127.0.0.1\" (also matches bind addresses
   \"0.0.0.0\", \"::\", and \"::1\")", forwarded to host `127.0.0.1`, same port,
   dynamic (guest agent watches for new listeners). So a container published with
   `-p 127.0.0.1:8080:80` in the guest becomes `127.0.0.1:8080` on the host with
   no per-port config. Forwarder default since v1.1 is gRPC (TCP+UDP, no child
   process); SSH forwarder optional via `LIMA_SSH_PORT_FORWARDER=true`, faster
   when AF_VSOCK is available (vz + systemd ≥256, Lima ≥2.0).
   In plain mode "dynamic port forwarding is disabled and only rules with
   `static: true` are forwarded."
   Receipts: <https://lima-vm.io/docs/config/port/>, <https://github.com/lima-vm/lima/blob/v2.2.0/templates/default.yaml> (lines 519–559).
2. **vzNAT (routable guest IP):** "To access a guest's ports by its IP address,
   connect the guest to the `vzNAT` or the `lima:shared` network. The `vzNAT`
   network is extremely faster and easier to use, however, `vzNAT` is only
   available for VZ guests." VMNet doc: "VMNet assigns a 'real' IP address that is
   reachable from the host", "The range of the IP address is not specifiable",
   needs no `socket_vmnet` binary/sudoers. Benchmarks (docs, Lima 2.0.0-alpha.2 on
   M4 Max): loopback-forwarded TCP ~5.4 Gbit/s (gRPC) vs vzNAT ~59 Gbit/s.
   Receipts: <https://lima-vm.io/docs/config/port/>, <https://lima-vm.io/docs/config/network/vmnet/>

**Port-forwarding scope notes.** [current]
- Only guest-loopback (and 0.0.0.0-bound) listeners are auto-forwarded; forwards
  land on host `127.0.0.1` unless a rule sets `hostIP: "0.0.0.0"`.
- `ignore: true` rules can exclude ranges; sockets and ports are interconvertible
  ("Sockets can also be forwarded to ports and vice versa, but not to/from a
  range of ports").
Receipt: <https://github.com/lima-vm/lima/blob/v2.2.0/templates/default.yaml>.

## 6. Failure modes — what the host observes

**Anatomy.** [current]
Instance dir (`${LIMA_HOME}/<INSTANCE>/`) exposes the observable surface:
"`ha.pid`: hostagent PID", "`ha.sock`: hostagent REST API", "`ha.stdout.log`:
hostagent stdout (JSON lines, see `pkg/hostagent/events.Event`)", "`ha.stderr.log`:
hostagent stderr (human-readable messages)", "`ga.sock`: Forwarded to
`/run/lima-guestagent.sock` in the guest, via SSH", "`serial.log`: default serial
log (QEMU only)" (vz page: with vz, "`serial.log` will not contain kernel boot logs").
Guest agent channel: "QEMU: uses virtio-port `io.lima-vm.guest_agent.0`; VZ: uses
vsock port 2222"; "The fallback is to use port forward over ssh port."
Receipts: <https://lima-vm.io/docs/dev/internals/>, <https://lima-vm.io/docs/config/vmtype/vz/>

**Guest agent death.** [current doc + flagged inference]
The guest agent is what reports new/removed listeners for dynamic forwarding; with
it gone (e.g. plain mode: "guest agent will not be running"), dynamic
port-forwarding events stop — existing SSH sessions and static forwards survive.
Host-side signal: hostagent log lines in `ha.stderr.log`; no status change in
`limactl list` for guest-agent-only failure. [the log-line specifics are
inference; docs only establish the roles]

**VM wedge / unclean stop.** [current]
The documented worst case is instance status `Broken` with inspection errors, e.g.
#5087: "errors inspecting instance: [vz driver is running but host agent is not]",
where `limactl start` refuses to run and repeated starts fail until
`limactl stop --force`. Host observables: `limactl list` status, `ha.pid`
liveness vs driver process, `ha.stderr.log`.
Receipt: <https://github.com/lima-vm/lima/issues/5087>

**Socket gone.** [inference, flagged]
Forwarded host sockets live under `{{.Dir}}/sock/`; when the hostagent exits they
stop being serviced (connect refused / ENOENT after cleanup). A health check
should probe the forwarded socket end-to-end (e.g. containerd version call), not
just stat the file. Not explicitly documented in primary sources.

---

## Decision-relevant summary

1. Mounts are start-time VM config with identity path mapping and read-only
   default — a worktree-per-job design must pre-mount a writable parent directory
   and cannot bind-mount per job. [current]
2. Rootful (`containerd.system: true`, non-default) is the only containerd mode
   whose socket can be forwarded to the host; rootless (the default) is
   namespace-bound per #1417, unchanged since 2023. [current]
3. `limactl autostart enable --condition=boot` exists in v2.2.0 (LaunchDaemon at
   `/Library/LaunchDaemons/io.lima-vm.daemon.<instance>.plist`, sudo once), but
   the shipped plist has no KeepAlive, and open bug #5087 means vz instances can
   land in `Broken` after unclean shutdown, requiring `limactl stop --force` —
   external supervision still required. [current]
4. Host→container TCP: automatic guest-loopback→host-loopback forwarding (gRPC
   forwarder, dynamic) covers it with zero config; vzNAT gives a routable guest
   IP and ~10x throughput if needed. Guest→host-loopback: `host.lima.internal`
   (192.168.5.2). [current]
5. Writable virtiofs on vz has an open intermittent-permission bug (#2437) —
   heavy git/write workloads on the shared mount need either tolerance/retries or
   guest-local disk for hot paths; no primary-source benchmark for git-metadata
   workloads exists. [current]
