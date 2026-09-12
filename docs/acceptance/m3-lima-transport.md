# M3 Lima transport and service publication attended acceptance

This is the owner-hardware lane for Tickets #145, #147, #148, #149, #150, #181, and #207 and the Mac rows of the M3 OCI
spec §9. It is deliberately absent from `service-acceptance-realtiming`: hosted
macOS runners do not prove nested Lima `vz`. A run is PASS only when every row
below has a captured command, exit code, and redacted receipt from the same
attended session. `WEFTY_RUN_TOKEN`, helper session capabilities, credentials,
and raw environment dumps must never enter the artifact.

## Preconditions

- macOS owner hardware, Lima 2.2 or newer with `vz`, and FileVault already
  unlocked;
- a Linux/arm64 `wefty-agent` from the candidate commit installed inside the
  `wefty-oci` guest, with the helper socket `0660 root:wefty-oci` and the Lima
  user in that group;
- rootful containerd 2.0 or newer, runc 1.x, and working `overlayfs` in the
  guest;
- the `wefty-echo-service-<candidate-commit>` artifact from the successful
  main `acceptance-image` workflow, with all registry access using the
  repository name plus `acceptance-image-index-digest.txt` (the commit tag is
  discovery-only); its OCI archive contains the same
  `/bin/sh`, BusyBox utilities, and `cmd/wefty-echo-service` program used by
  Linux realtiming, including the distinct one-shot stdout/stderr markers;
- from the same artifact, the two image-user variants of that echo image that
  `service_data_guest_native` needs: `wefty-echo-service-user-numeric.oci.tar`
  (image user `13001:13002`) and `wefty-echo-service-user-named.oci.tar`
  (image user `wefty:wefty`, resolving to `12001:12002`). They differ from the
  root echo image only in that user, they are published as
  `<commit>-user-numeric` and `<commit>-user-named` tags of the same public
  `wefty-echo-service` package, and their index and per-platform digests are
  recorded in `acceptance-image-receipt.json` under `service_user_variants`.
  Import each by its `acceptance-image-user-<variant>-index-digest.txt`. Each
  archive carries its own published tag as its reference, so `wefty node
  load-image` keys the two variants and the root echo image by three distinct
  names; an artifact from before #418, which annotates only a digest, imports
  under that digest or under an explicit `--reference
  ghcr.io/derek-x-wang/wefty-echo-service:<commit>-user-<variant>` inside the
  same repository;
- the separate `wefty-computer-reference-<candidate-commit>` artifact from
  that exact workflow run. Extract `wefty-computer-reference-release.tar`,
  require its commit to match the echo artifact, and use the repository name
  plus `computer-image-index-digest.txt` and
  `wefty-computer-reference.oci.tar` unchanged. The index and archive receipt
  must name the same amd64/arm64 child digests executed by secretless CI;
- the separate `wefty-computer-wayland-reference-<candidate-commit>` artifact
  from that exact workflow run. Extract
  `wefty-computer-wayland-reference-release.tar`, require its commit to match
  both earlier artifacts, and use `wayland-computer-image-index-digest.txt`
  plus `wefty-computer-wayland-reference.oci.tar` unchanged. Its platform
  receipts must show conformance, furniture, and ELF execution before this
  attended lane imports the arm64 child;
- one temporary host operator-mount root dedicated to this run.

Ticket #152 adds the minimum installed boot topology consumed by this lane.
Build the candidate macOS agent, extract `wefty-acceptance-image-release.tar`,
and use its matching runnable Linux/arm64 release tree before running the private bootstrap mode. Use its
`share/wefty/oci/manifest.json` reference, digest, and adjacent
`wefty-echo-service.oci.tar` unchanged; manifest inspection must show the same
arm64 platform digest recorded in `acceptance-image-index.json`. Pass
secret-free agent arguments only; tsnet must use persisted state rather than
an auth key in the plist or process arguments.

The private `__wefty_mac_bootstrap` mode is an interim acceptance/setup seam,
not the general `wefty node setup-oci` or doctor UI owned by later tickets. It
requires explicit operator/home/Lima/work/log paths, the helper checksum,
guest user and UID, probe reference/digest/archive, node ID, mount root, and
repeatable `--agent-arg` values. `--intent-file` is optional and defaults under
`LIMA_HOME`; bootstrap creates its initial enabled marker only when absent. It
starts the existing configured instance, verifies the requested host mount
root against that instance's configured mounts and refuses the helper install
with typed reason `host_mount_root_not_mounted` before staging any unit,
installs and verifies the helper and probe, then installs and starts the system
LaunchDaemon. Record the complete command with credential values omitted from
the artifact.

## Template and permissions

Resolve the setup-time defaults and retain the explicit values in the receipt:
`--vm-memory` is 25% of host RAM capped at 4 GiB, `--vm-cpus` is 4 capped at
half the logical cores, and `--vm-disk` is 32 GiB. A changed value is
restart-required and must not be applied without the future `--apply-restart`
convergence path.

Run the tagged contract lane first:

```sh
go test -tags=service_acceptance -v ./runner/lima ./runner/oci ./runner/ocihelper
```

Capture the stored `lima.yaml` and assert all of the following without editing
it in place:

- `vmType: vz`, explicit memory/CPU/disk, rootful-only containerd, and a healthy
  `io.containerd.snapshotter.v1 overlayfs` plugin;
- exactly one writable host allowed root mapped to `/mnt/wefty-host`;
- only `/run/wefty/oci-helper.sock` forwarded into the instance `sock/`
  directory; `limactl` creates that host-side directory itself when it wires
  the socket forward (wefty issues no `chmod`/`chown` for it), and its
  shipped mode is operator-owned `0750`, not `0700` — the forwarded socket
  file inside it stays `0600` owner-only regardless, so the directory's
  group-readability does not expose the socket; the guest socket is
  `0660 root:wefty-oci`;
- the raw containerd socket has no host forward;
- `limactl template copy --fill <stored-template> -` shows the first matching
  TCP/UDP rule as `guestIP: 0.0.0.0`, `guestIPMustBeZero: false`,
  `guestPortRange: [1,65535]`, `proto: any`, `ignore: true`.

For the `dynamic_forwarding_disabled` receipt, bind two distinct marker HTTP
listeners inside the guest, first on `127.0.0.1:<port>` and then on
`0.0.0.0:<port>`. From macOS, record `nc -vz 127.0.0.1 <port>` and a request to
the host's non-loopback addresses failing for each listener while the same
marker succeeds from `limactl shell`; stop each listener before continuing.
Record both addresses as `false` in the row's `dynamic_listeners` map.

## Installed boot topology

Before bootstrap, run `limactl autostart disable wefty-oci` if Lima autostart
was ever enabled. Bootstrap fails closed while any
`/Library/LaunchDaemons/io.lima-vm.daemon.*.plist`, system/user LaunchAgent, or
loaded `io.lima-vm.autostart.*` user/gui unit remains; it never installs a
second VM supervisor. After bootstrap, capture all of these facts:

- `dev.wefty.agent` is loaded in the system launchd domain with `UserName` set
  to the operator, absolute program/log/working paths, `RunAtLoad`, throttled
  `KeepAlive`, and explicit `HOME`, `LIMA_HOME`, `USER`, `LOGNAME`, and `PATH`;
- no `io.lima-vm.daemon.*` or `io.lima-vm.autostart.*` unit exists in system,
  user, or gui domains, and the agent is the only process that invokes
  lifecycle-changing `limactl start` or `stop --force` commands;
- the installed Linux helper checksum equals the candidate receipt, its
  handshake reports the candidate version and protocol major, the guest socket
  is exactly `0660 root:wefty-oci`, and raw containerd remains unforwarded;
- first-time guest group installation performs one ordinary VM stop/start so
  the Lima guest agent picks up `wefty-oci`; an already-member rerun does not.
  This clause has never been exercised on an owner host whose operator has been
  in `wefty-oci` since a previous run — proving it needs a fresh guest — so the
  row's evidence must say which clause ran;
- the probe archive imports to the recorded top-level digest through the helper
  API and the functional create/start/wait/delete probe succeeds.

Capture the atomic minimal-facts JSON named by
`--oci-minimal-doctor-facts`. It contains only schema version, observation
time, unit, Lima/helper/probe state, capability revision, the supervisor's
repair counter and bounded lifecycle transition trail, and a stable
sanitized reason code. It must contain no raw error, helper session capability,
credential, or environment dump. The unit state is `launched_by_unit`, state
values use the closed contract vocabulary, and unchanged content is not
rewritten more frequently than the 20-second observation floor.

Exercise supervision twice from a clean process-only baseline. With an enabled
intent-file revision, stop Lima and require `stopped -> running`; inject or
observe `Broken` and require `broken -> stopped -> running` through one bounded
`stop --force`/capped-backoff repair. Read both recoveries from the facts file's
`lima.transitions` trail and `lima.repair_count`, not from the momentary
`lima.state`: the 20-second observation floor is far coarser than a repair, so
`lima.state` shows `running` on both sides of one and proves nothing. Each
recovery must raise `lima.repair_count` by one and must leave a trail whose tail
is the states the supervisor itself observed and performed —
`stopped -> running` for the stopped row, and for the broken row either
`stopped -> running` or `broken -> stopped -> running`, whichever the supervisor
read.

`broken_enabled_recovery` proves the repair, not the name Lima gave the fault.
Record, with timestamps, both views of the injection: the host's own
`limactl list` states in `host_observed_states` with the time of the reading in
`host_observed_at`, and the agent's `lima.transitions` tail in `lima_states`.
The row PASSes when the intent was enabled, `repair_count` rose by exactly one,
the agent trail carries the fault's `stopped -> running` pair, capability is
re-earned — and either the agent trail itself contains `broken` or the host
observation does. It FAILs when that repair evidence is missing. It is NOT-RUN
only when the fault never landed at all, meaning the host never saw the instance
leave `Running`.

Do not require `broken` in the agent trail. Lima's status for a killed-hostagent
fault is not a state the supervisor can be asked to read: the Broken reading
lasts one to two seconds at an offset that moved between +1 s and +5 s after the
kill across attended injections, and in run 4 `limactl list` reported `Broken`
at 02:46:32Z while the supervisor's own inspection at 02:46:32.390652Z — the
same second — reported `Stopped`. Landing inside the window is not even
sufficient to read it. The supervisor still re-inspects once when a fault's
first reading is `stopped`, which is how a durably Broken instance reaches the
Broken branch; for this fault profile it changes nothing, and widening that
window cannot (#435). Persist a higher disabled intent-file
revision, stop Lima, and require it to remain stopped with no recovery mutation;
if the attended harness cannot safely write that fixture, emit structured
NOT-RUN for `stopped_disabled_no_recovery`. During each
outage, prove the same agent process remains alive, process work remains
available, OCI capability is withdrawn with a higher revision, and OCI returns
only after the helper handshake, boot sweep, and real probe.

## Runtime matrix

Use the normal helper client and boot barrier, not `ctr` from the host. Record
the helper instance/session generation before each row.

1. Barrier and probe: acquire, sweep the whole `wefty` namespace, independently
   verify absence, then run the pinned `/bin/true` functional probe. Prove no
   OCI claim begins before the fresh capability revision is acknowledged. This
   is the daemon's own boot-time acquire, exercised already by the Installed
   boot topology section above; it produces the `probe` row and needs no
   further procedure.

### Exclusive helper session for items 2-4 and the fallback half of item 6

Items 2-4 below (`task_logs_delete`, `mount_validation`, `host_to_guest`) and
the bridge half of item 6 (`guest_to_host_fallback`) call
`Run`/`Mount`/`DialAttemptPort`/`DialHostBridge` directly against the helper
rather than through `wefty submit`, and the helper accepts only one session at
a time. `dev.wefty.agent` has held that session continuously since it started,
so take it the same way [m3.5-mac-computer.md](m3.5-mac-computer.md)'s
`mac.removal` row takes it from the daemon for its independent removal
inventory:

```sh
sudo launchctl bootout system/dev.wefty.agent
pgrep -fl wefty-agent   # must print nothing
```

With the daemon offline, drive all four rows from one direct helper-client
session with `TestAttendedHelperTransportRows` (#410). Once, before the first
attended run on this host, create the device-node mount negative, which macOS
will not let a non-root user make:

```sh
mkdir -p "$WEFTY_ATTENDED_MOUNT_ROOT/negatives"
sudo mknod "$WEFTY_ATTENDED_MOUNT_ROOT/negatives/device" c 1 3
```

Then, inside the window:

```sh
WEFTY_OCI_HELPER_SOCKET=/path/to/forwarded/oci-helper.sock \
WEFTY_OCI_HELPER_CHECKSUM=<installed helper sha256> \
WEFTY_OCI_PROBE_REFERENCE=<pinned probe reference> \
WEFTY_OCI_PROBE_DIGEST=<pinned probe top-level digest> \
WEFTY_OCI_PROBE_ARCHIVE=/path/to/acceptance-image.tar \
WEFTY_ATTENDED_SESSION_ID=<the artifact's shared session ID> \
WEFTY_ATTENDED_MOUNT_ROOT=<the configured operator host mount root> \
WEFTY_ATTENDED_ROWS_OUT=/abs/path/transport-rows.json \
  go test -tags=service_acceptance -run TestAttendedHelperTransportRows \
  -count=1 -v ./serviceacceptance
```

It refuses to start unless `pgrep -fl wefty-agent` is empty, performs each
row's operations in order against the guest socket, and writes
`task_logs_delete`, `mount_validation`, `host_to_guest` and
`guest_to_host_fallback` to `WEFTY_ATTENDED_ROWS_OUT` in the receipt's row
shape, each carrying `session_id`, the exact `command`, `exit_code`, and a
`reason` recording the typed refusal code every negative returned. The
"independent absent `Verify`" each attempt-bearing row ends with is a
read-only namespace `Verify` projected onto that attempt's deterministic
resource names, not an attempt-scoped one: the helper authorizes an
attempt-scoped `Verify` only against a live attempt, and a positive `Delete`
is exactly what ends that, so the independence the row claims has to come from
the namespace side. Each row's `reason` says so. The gate
(`runner/lima/service_acceptance_test.go`) requires no other typed field for
these rows beyond that shared shape. Fold the fragment into the receipt as the
Receipt section describes. The mount row proves the host-to-guest translation
from both sides: the payload's write appears on the host under the operator
mount root and, read back with `limactl shell`, in the guest under
`/mnt/wefty-host`. The one clause the entrypoint does not exercise is
item 6's "discovery failure must fail start and must not select fallback",
which is agent-side and outside a direct helper-client session; the row's
`reason` says so verbatim, so judge the row with that in view.

Re-install and restart the daemon immediately afterward, before item 5 and the
rest of the runtime matrix — service publication, service data, and the
removal manifest all assume `dev.wefty.agent` holds the session again:

```sh
sudo launchctl bootstrap system /Library/LaunchDaemons/dev.wefty.agent.plist
sudo launchctl kickstart -k system/dev.wefty.agent
```

Confirm the reload the same way the Installed boot topology section already
does: `dev.wefty.agent` loaded in the system domain with the operator
`UserName`, no competing `io.lima-vm.daemon.*`/`io.lima-vm.autostart.*` unit,
and the guest socket back to `0660 root:wefty-oci`.

2. Task/log/delete: run the test image through `Run`; require authoritative
   `Started`, ordered distinct stdout/stderr frames, terminal exit 0, positive
   `Delete`, and an independent absent `Verify`.
3. Mount validation: a strict descendant of the configured host root translates
   to `/mnt/wefty-host/...` and can be read/written as requested. Reject the
   root itself, an outside path, a symlink component, socket, device, FIFO, and
   every reserved-target overlap. After deletion, prove the host bind source is
   byte-identical and still exists.
4. Host to guest: request one helper-allocated port, bind the payload only on
   guest loopback, and exchange distinct request/response markers through
   `DialAttemptPort` for the returned `service` endpoint name. A different name
   and a different attempt tuple must return typed authorization failures.

### Runtime matrix, continued

5. Guest to host primary: resolve `host.lima.internal` from inside the current
   guest, record the discovered address, and prove it is VM-private before
   anything binds it. Prove no `0.0.0.0` listener and no fixed gateway string
   exists in config, argv, or source. The proof of VM-privateness follows how
   Lima attached this instance, read from Lima itself: on a `vmType: vz`
   instance the user-mode network lives inside Virtualization.framework and the
   host owns no interface for the gateway, so the proof is that the discovered
   address is the instance's own user-network gateway as Lima configured it; on
   a vmnet/socket_vmnet instance the proof is that the route to the address
   leaves through a virtual machine interface rather than a physical one.

   On vz both halves of that proof — the resolution of `host.lima.internal`
   and the instance's user-network gateway — are read from inside the guest,
   so a compromised instance could name one of the host's own addresses twice
   and satisfy the equality. A host-side floor that asks the guest nothing is
   what prevents that: the address must not be assigned to any host interface,
   because a vz user-network gateway never is, and the host being
   unenumerable refuses the gateway rather than binding it. Without that
   floor a lying guest would obtain a plaintext run bridge on the LAN, since
   macOS can bind an address it actually holds.

   What is bound afterwards follows the same arrangement:

   - vmnet/socket_vmnet: bind only the discovered address on macOS and
     complete one authenticated run-bridge request.
   - vz: macOS cannot assign the vz user-network gateway to a socket at all —
     measured on owner hardware (2026-09-11, Lima 2.2): the bind returns
     `EADDRNOTAVAIL` and no host interface carries the address while the
     instance runs. A primary bind is therefore impossible on vz and the
     capability-gated host-loopback bridge is the transport, not a degraded
     form of one. Require the bridge on host loopback with the helper fallback
     selected, and prove the guest reaches it by fetching the advertised port
     through `host.lima.internal` from inside the guest and matching the
     marker response.

   The receipt records the `vm_type` the binding was judged against and which
   transport was proven.
6. Guest to host fallback: inject a bind failure for that discovered address.
   Require a host-loopback bridge, helper-issued per-attempt bridge capability,
   and successful request through `DialHostBridge`; wrong capability and wrong
   attempt must fail. Discovery failure itself must fail start and must not
   select fallback. On a vz instance this exercises the same transport row 5
   proves, under an injected primary failure rather than the structural one:
   what keeps the rows distinct there is that this one proves the helper-issued
   capability and its wrong-capability and wrong-attempt refusals, and that a
   discovery failure never selects fallback. On a vmnet/socket_vmnet instance
   it remains a different path from row 5.
7. Service publication: export the attended helper socket/checksum, pinned
   probe image reference/digest, and probe archive path as
   `WEFTY_OCI_HELPER_SOCKET`, `WEFTY_OCI_HELPER_CHECKSUM`,
   `WEFTY_OCI_PROBE_REFERENCE`, `WEFTY_OCI_PROBE_DIGEST`, and
   `WEFTY_OCI_PROBE_ARCHIVE`, then run:

   ```sh
   go test -tags=service_acceptance -run TestOCIServicePublicationThroughHelperTunnel -v ./agent
   ```

   Require health and request-body echo through the Fabric front door and the
   helper's `DialAttemptPort` stream, an unpublished startup timeout, immediate
   withdrawal followed by hysteresis-bounded republication, distinct backend
   ports for concurrent attempts, and a portless payload that reports
   authoritative `Started` without allocating an endpoint. Record
   `WEFTY_SERVICE_DIR=/wefty/service` and the absence of guest or host backing
   paths in the payload environment.
8. Service data: run root, numeric `13001:13002`, and named `wefty:wefty`
   (`12001:12002`) image-user variants, the latter two from the
   `wefty-echo-service-user-numeric.oci.tar` and
   `wefty-echo-service-user-named.oci.tar` archives named in the
   preconditions. For each, require `/wefty/service` to
   begin with exactly that UID:GID and accept a payload write. For one stable
   service job, record attempt counters `0,1,2` across crash restart and
   stop→start while a marker outside `/wefty/service` is absent at the start of
   every fresh attempt. Attribute the crash-restart transition (counter `0` to
   `1`: a fresh attempt, container, task, and backend port on the same
   digest-pinned binding) to the `service_restart_fresh_attempt` row, and the
   stop→start transition (stop releases the slot but retains the binding,
   digest pin, and service data; start reacquires service capacity through
   `queued` before the counter advances `1` to `2`) to the
   `service_stop_start_capacity` row. Separately, prove the
   `service_failed_quiescence` row described below. Start
   a second service job on the same pinned digest;
   require its service data to be empty while the original job remains
   digest-pinned and retains its own counter. The helper-owned backing path must resolve inside the
   Linux guest's native filesystem, remain absent from the Lima host mounts,
   and never traverse virtiofs. Record these facts in the
   `service_data_guest_native` row.
9. Reference Computer images: import each arm64 child from its separate
   digest-selected Computer OCI tar through the helper, then boot each without
   argv or working-directory replacement. Require both returned names to reach
   `rfb-websocket-v1` readiness atomically within 60 seconds of authoritative
   `Started`, with view input discarded server-side and control input accepted.
   For XFCE record the CPU-rendered Xvfb/XFCE/Chromium session. For the Wayland
   image record Sway's headless backend, pixman renderer, two native `wayvnc -w`
   processes, `--disable-input` on view, and the absence of `/dev/dri` and
   websockify. For both record the 1 GiB private `/dev/shm`, profile/sign-in
   markers across a fresh attempt and stop→start, missing or malformed
   `driver.json` failing closed, and attempt-local scratch absence. All three
   image receipts must retain distinct repositories, digests, and tar names
   while sharing the candidate commit.
10. Removal manifest: while the agent is offline, request removal of a bound OCI
   service and observe L1 at exactly `removal_pending`. Return the same node
   through the ordinary boot sweep barrier, capturing the node-local removal
   read across the return. Once the agent is back, the whole proof — quiescence
   receipt, service-data deletion, absence attestation, and the L1
   acknowledgement that releases the record — completes inside one sub-second
   pass (the proof alone measured 24 ms on owner hardware, 2026-09-12,
   candidate `46915f2`), so a `while sleep 0.2` loop of
   `wefty --json node oci removals` observes nothing; the capture is an
   in-process poller over the operator control socket, sampling fast enough to
   see that pass end to end and stopping once it holds one record at
   `phase=complete`. The record is durable through its phases and leaves the
   read surface only at the L1 acknowledgement, so the completed record the
   capture holds is the row's evidence, and it must show: the immutable
   job/removal-generation identity (`job_id`, `removal_generation`,
   `cleanup_fence`, `root_instance_id`); `resource_manifests` with every
   attempt's lease, task, container, snapshot, shim, cgroup, framed-log
   directory, service-data volume, and its owner record; `runtime_quiescence`
   with `runtime_quiesced=true` and the positive prior-boot sweep evidence
   (`evidence`, `boot_session_id`, `sweep_epoch`, `helper_generation`); the
   `prepared -> quarantined -> complete` phase history as the `prepared_at`,
   `quiesced_at`, `attested_at`, and `completed_at` timestamps; and an
   `absence_attestation` carrying one `absent=true` assertion for every
   manifested resource class. Require the proof-gated completion path to delete
   the guest-native service-data bytes and owner record and only then reach
   `removed_verified`. Record the guest-native
   inventories and phase facts in `service_removal_manifest_offline`, taking
   `resource_manifests` and `removal_assertions` verbatim from that record.

### Denied quiescence proof (`service_failed_quiescence`)

The row's intent is unchanged: when the runtime cannot prove that a stop was
clean, L1 must say so instead of reporting `stopped`. What changed is the
fault. Killing the attempt's containerd shim does **not** deny that proof — it
was tried twice on owner hardware (2026-09-12, candidate `1e182ff`), once with
a three-second gap before the stop and once concurrently with it, and both
times the task terminalized on the KILL, the helper verified the attempt's
absence, and the Job reached `stopped`.

The proof the runtime actually needs is the helper's `Delete` receipt, and the
helper refuses that receipt while any resource named in the attempt's frozen
manifest still exists. Denying exactly one of those resources therefore denies
the proof without touching the helper session or the namespace. Pin the
attempt's framed-log directory inside the guest, then request the ordinary
service stop.

The directory must be the target job's own. Step 8 runs a second service job
on the same digest, so newest-first guessing can pin a live bystander and latch
the wrong Job. Every one of the attempt's resource names shares one suffix, and
the attempt's containerd lease carries the job it belongs to, so derive the
suffix from that lease rather than from mtime, and require exactly one match:

```sh
limactl shell wefty-oci sudo sh -c '
  set -eu
  job=JOB_ID
  matches=$(ctr --namespace wefty leases list | grep -F "io.wefty/job_id=$job" | wc -l)
  test "$matches" -eq 1
  suffix=$(ctr --namespace wefty leases list | grep -F "io.wefty/job_id=$job" \
    | sed -n "s/^wefty-lease-\([0-9a-f][0-9a-f]*\).*/\1/p")
  test -n "$suffix"
  dir="/var/lib/wefty/oci/logs/wefty-log-segments-$suffix"
  test -d "$dir"
  mkdir -p "$dir/wefty-quiescence-pin"
  mount -t tmpfs -o size=1m none "$dir/wefty-quiescence-pin"
  printf "pinned=%s\n" "$dir"'
wefty services stop JOB_ID
wefty services status JOB_ID
```

Record the printed `pinned=` path in the row and confirm its suffix matches the
lease, container, snapshot and cgroup names the target job's attempt is using.

The helper retries deletion for its whole bounded budget, cannot remove the
pinned directory, and returns a deadline-scoped engine failure. That failure is
attempt-scoped by construction, so it is never promoted to helper or namespace
loss: the stop completes with no runtime-quiescence evidence. Require the Job
to reach `failed` and not `stopped`, desired state to remain `stopped`, the
recorded failure to name the unverified reap, and the binding, digest pin, and
service data to be retained. `oci_runtime_quiescence_failed` belongs to the
node control surface — it is what `wefty node oci stop` returns when the whole
runtime cannot be quiesced — and must not be expected as the per-job latch.

Release the fault and remove the residue the denied deletion left behind, using
the same `pinned=` path:

```sh
limactl shell wefty-oci sudo sh -c '
  set -eu
  dir=PINNED_PATH
  umount "$dir/wefty-quiescence-pin"
  rm -rf "$dir"'
```

The pin is not attempt-scoped: while it is held it also denies the helper's
whole-namespace startup sweep, so any helper restart in this window fails its
boot barrier — not just the attempt's own delete.

Unmounting does not undo the denial. The helper runs its verified-attempt
release only on a `Delete` that succeeded, so the attempt's helper-side entry,
its image pin, its capacity reservation, and its durable ownership record all
stay held after the Job has latched `failed`, and nothing retries the reap
because the Job is terminal. Expect one service slot and one image pin to
remain held for the rest of the session. Run this row immediately before
`helper_loss`, whose helper restart and namespace sweep is what clears the
leftover attempt; if the session order puts it elsewhere, record the retained
slot and pin in the row's `inventories` so a later capacity or cache
observation is not read as a defect.

Record the injected fault, the stop command, its exit code, and the observed
Job state in the `service_failed_quiescence` row.

If the injected fault is nonetheless survived and the runtime proves a clean
stop, the row records that honestly — the fault was real, the observation was
real — rather than being retried until it produces the wanted answer. A row
whose fault was never injected is a failure, never a PASS.

## Ordinary L3 OCI one-shot

Run the normal L1, L3, and agent processes, then submit the candidate echo
artifact through the ordinary `wefty submit --image ... --argv
wefty-echo-service --argv=--once` surface. Do not call the helper or OCI
adapter directly for these rows. The L3 snapshot must be the only source of
the `kind=oci`, `class=one-shot` Job.

Record four rows:

1. `oci_oneshot_run`: require the helper-owned `/wefty/handoff` mount, one
   authenticated run-scoped bridge request, distinct ordered stdout/stderr
   markers, exit zero, accepted top-level and Lima-platform digests, one
   attempt ID, and exactly one payload execution. Capture the exact
   `wefty echo one-shot handoff\n` marker bytes before finalization, then prove
   the helper-owned volume is absent only after L1 accepts success.
2. `oci_oneshot_prestarted_loss`: stop the VM or helper after image evidence
   but before authoritative `Started`. Require the old attempt to terminalize,
   the job to requeue with its original absolute deadline and digest, a fresh
   attempt/fence after recovery, and exactly one payload execution across the
   two attempts.
3. `oci_oneshot_poststarted_loss`: stop the VM or helper after `Started`.
   Require one terminal `runtime_failure`, no automatic requeue, one attempt,
   and exactly one payload execution.
4. `oci_oneshot_rerun_identity`: explicitly rerun the completed first row.
   Require a fresh run/job/attempt, the identical top-level and platform
   digests, a second payload execution, and no tag resolution. The two
   executions must have distinct attempt IDs. Record the same opaque handoff
   owner identity, exact marker bytes, and accepted-completion deletion proof.

For every row, record the ordinary L3 run and L1 job projections, redacted
reserved-name presence (never values), helper generation, attempts, digest
arrays, payload-execution count, logs, exact handoff marker bytes,
`handoff_absent_after_completion`, and final residue inventory. A Mac/Lima
row is `NOT-RUN` unless this attended owner-hardware procedure actually
executes it; hosted macOS does not satisfy the row.

## Loss and recovery order

`helper_loss`, `vm_loss`, and `sweep_before_recovery` need the same exclusive
helper session as `task_logs_delete`, `mount_validation`, and `host_to_guest`
above, for the whole fault-and-recovery sequence: the live marker workload,
the fault, and the recovery all have to be observed from one direct client
session, and `dev.wefty.agent` cannot be left holding a competing session
across an injected helper/VM loss without contaminating the recovery it is
already separately proven for in the Installed boot topology section. Take it
the same way:

```sh
sudo launchctl bootout system/dev.wefty.agent
pgrep -fl wefty-agent   # must print nothing
```

For helper loss and then full VM stop, leave a live marker workload before the
fault, driven from that one direct helper-client session. Each row must show,
in order:

1. the old helper control stream fails and local OCI capability becomes
   restrictive;
2. publication/traffic is unavailable and no new OCI claim is admitted;
3. after the helper/VM is manually returned for this attended ticket, the host
   reconnects and a fresh `AcquireSession` returns a new helper instance/session
   generation (socket inode identity is not authority);
4. the new helper generation sweeps all old `wefty` resources and independently
   verifies absence;
5. only then does the real probe pass and a higher capability revision reopen
   OCI claims.

Reuse the textual agent boot ID in one repetition. Any adopted survivor,
pre-sweep probe, pre-sweep positive publication, reachable old tunnel, or raw
containerd host access is FAIL.

For each of `helper_loss`, `vm_loss`, and `sweep_before_recovery`, record
`session_id`, `command`, and `exit_code: 0` as usual, plus the fields the gate
checks explicitly for these three: `helper_generations` with at least the
pre-fault and the post-recovery generation, `capability_revisions` with at
least the withdrawn and the reopened revision, and `inventories` with at least
the pre-sweep and the post-sweep independent verification — the gate requires
two or more entries in each of those three lists.

`TestAttendedHelperLossRows` (#410) drives that whole sequence from one direct
helper-client session, inside the same daemon-booted-out window:

```sh
WEFTY_OCI_HELPER_SOCKET=... WEFTY_OCI_HELPER_CHECKSUM=... \
WEFTY_OCI_PROBE_REFERENCE=... WEFTY_OCI_PROBE_DIGEST=... \
WEFTY_OCI_PROBE_ARCHIVE=/path/to/acceptance-image.tar \
WEFTY_ATTENDED_SESSION_ID=<the artifact's shared session ID> \
WEFTY_LIMA_INSTANCE=wefty-oci \
WEFTY_ATTENDED_ROWS_OUT=/abs/path/loss-rows.json \
  go test -tags=service_acceptance -run TestAttendedHelperLossRows \
  -count=1 -v ./serviceacceptance
```

Two fault executions produce the three rows. The helper-loss execution
produces both `helper_loss` (the ordered recovery above) and
`sweep_before_recovery` (the assertion that the verified sweep precedes the
functional probe); injecting the same fault twice would prove nothing extra,
so each row carries its own `session_id`, `command` and `exit_code`, and each
row's `reason` names the fault execution it came from. The VM-loss execution
produces `vm_loss` under a fresh textual boot session ID, so the reuse the
runbook asks for is exercised exactly once, by the helper repetition.

By default the entrypoint takes the faults itself and records the exact
commands in each row's `reason`:

| Fault | Injected | Returned |
| --- | --- | --- |
| helper | `limactl shell --workdir=/ <instance> sudo systemctl stop dev.wefty.oci-helper.socket dev.wefty.oci-helper.service` | `... systemctl start dev.wefty.oci-helper.socket` |
| VM | `limactl stop <instance>` | `limactl start <instance>` |

Set `WEFTY_ATTENDED_MANUAL_FAULTS=1` to take them by hand instead: the
entrypoint prints the exact command and waits until you `touch` the
acknowledgement file it names (`WEFTY_ATTENDED_FAULT_ACK`, default
`/tmp/wefty-attended-fault-ack`).

`sweep_before_recovery` is a structural claim rather than a timing race, and
the row says so: `Ensure` acquires the session and completes the verified
sweep as one step, so between the invalidation and the re-acquire there is no
session to probe with at all. The row asserts that the pre-sweep probe was
refused by the unprepared barrier specifically — an unrelated failure does not
satisfy it — and that the probe then succeeded only against a generation whose
sweep had completed.

The post-fault "old tunnel unreachable" and "no new claim admitted" entries
are restrictive observations from a lost session, not typed helper refusals:
once the control stream is gone the helper answers nothing. The driver
excludes its own context deadline and cancellation so they cannot masquerade
as the runtime refusing, and each row's `reason` repeats the qualification.

The `capability_revisions` these rows emit are the driver's own local OCI
capability observations, carrying the barrier's typed reason code. No L1
revision exists inside either window, because `dev.wefty.agent` is booted out
for the whole of it; L1 revision publication is proven separately by the
Installed boot topology rows. Every row says this in its `reason`.

The barrier records a typed capability reason as the outcome of an `Ensure`
and of nothing else, so a loss seen through transport failures on a session
that was already acquired carries no reason at all. The driver therefore takes
a bounded re-`Ensure` inside the fault window — which is how the agent's own
readiness timer obtains one — and the withdrawal carries that refusal's
classification. A re-`Ensure` the runtime accepts means the runtime was not
restricted, and fails the row. Each row's `reason` records the refusal
verbatim alongside the code it produced.

Once all three rows are recorded, re-install and restart the daemon before
continuing to the Receipt section's fold-in commands:

```sh
sudo launchctl bootstrap system /Library/LaunchDaemons/dev.wefty.agent.plist
sudo launchctl kickstart -k system/dev.wefty.agent
```

## Receipt

Store the attended artifact outside Git with: candidate commit SHA; one
session ID repeated by every row; host/Lima/containerd/runc versions; explicit
VM sizing; redacted template checksum; helper generations; capability
revisions; per-row PASS/FAIL/NOT-RUN; exact commands and exit codes; and residue inventories before fault,
after sweep, and after delete. NOT-RUN is not destination success and must name
the missing owner-hardware prerequisite.

Every non-PASS row also carries **`blocked_by`**: the GitHub issue number the
owner judges to own that row's FAIL or NOT-RUN, recorded at capture time. This
is the matrix's only attribution authority. A row that omits it is attributed to
the acceptance-matrix ticket #157 -- honest ("nobody recorded who owns this")
but useless for triage, so record it. Attribution that once lived in Go source
went stale twice in three runs (#394, then #408), which is why the receipt row
now owns it.

The redacted artifact is strict JSON with this shape:

```json
{
  "session_id": "attended-2026-08-23T120000Z",
  "commit": "candidate SHA from git rev-parse HEAD",
  "versions": {"limactl": "2.2.0", "containerd": "2.x", "runc": "1.x"},
  "rows": {
    "template_permissions": {
      "status": "PASS", "session_id": "attended-2026-08-23T120000Z",
      "command": ["limactl", "template", "copy", "--fill", "lima.yaml", "-"],
      "exit_code": 0, "helper_generations": [],
      "capability_revisions": [], "inventories": [], "round_trip": false,
      "dynamic_listeners": {}, "attempt_ids": [],
      "top_level_digests": [], "platform_digests": [],
      "payload_executions": 0, "stdout_markers": [],
      "stderr_markers": []
    }
  }
}
```

The two exclusive-window entrypoints write their rows to
`WEFTY_ATTENDED_ROWS_OUT` in exactly this row shape, so folding them into the
receipt is a merge rather than a transcription:

```sh
jq -s '.[0] as $receipt | $receipt + {rows: ($receipt.rows + .[1].rows + .[2].rows)}' \
  receipt.json transport-rows.json loss-rows.json > receipt-merged.json
```

It must contain PASS evidence for `template_permissions`, `probe`,
`task_logs_delete`, `mount_validation`, `host_to_guest`,
`guest_to_host_primary`, `guest_to_host_fallback`, `helper_loss`, `vm_loss`,
`sweep_before_recovery`, `dynamic_forwarding_disabled`, and
`raw_containerd_denied`, plus `oci_oneshot_run`,
`oci_oneshot_prestarted_loss`, `oci_oneshot_poststarted_loss`, and
`oci_oneshot_rerun_identity`. Ticket #147 additionally requires
`service_health_echo`, `service_startup_timeout`,
`service_withdrawal_republication`, `service_port_collision`, and
`service_portless_started` from the same attended session.

Ticket #148 additionally requires `service_restart_fresh_attempt`,
`service_stop_start_capacity`, and `service_failed_quiescence` from the same
attended session. The gate requires no additional typed fields for the first
two beyond the standard row shape (`session_id`, non-empty `command`, and
`exit_code=0`); put their restart-identity and capacity-reacquisition facts in
the row's free-form `reason` and `inventories` fields.

`service_failed_quiescence` additionally requires
`quiescence_fault_injected=true` — a row whose fault was never injected is not
a PASS — plus `service_job_state`, which the gate holds to the outcome the row
claims: `failed` when `quiescence_latched=true`, and `stopped` with a non-empty
`reason` naming the injected fault and what was observed instead when
`quiescence_latched=false`. The second shape records an honest negative
observation of a real fault; it never records a skipped or synthesized one.

Ticket #149 additionally requires `service_data_guest_native` with
`service_owners` containing `0:0`, `13001:13002`, and `12001:12002`,
`service_attempt_counts` exactly `[0,1,2]`, `guest_native_data=true`,
`virtiofs_data=false`, and `rootfs_discarded=true` from the same attended
session. A hosted macOS runner or a host-shared backing path is `NOT-RUN`, not
PASS evidence.

Ticket #150 additionally requires `service_removal_manifest_offline` with
`removal_phase=complete`, `removal_pending_observed=true`,
`removal_completed=true`, `runtime_quiesced=true`, and non-empty
`resource_manifests` naming the service
data directory and its owner record independently. `wefty node oci removals`
is the surface those fields come from: `removal_phase` is the record's `phase`,
`runtime_quiesced` is `.runtime_quiescence.runtime_quiesced`, and
`resource_manifests` is the record's `resource_manifests` array copied
verbatim. Hosted macOS runners are
`NOT-RUN`; they do not satisfy this owner-hardware row.

Ticket #151 additionally requires `post_delete_attestation=true`,
`service_data_bytes_absent=true`, `service_data_owner_record_absent=true`, and
one `absent=true` assertion for every class/identity in `resource_manifests`.
The same read carries them: a record whose `absence_attestation` is present
proves the post-delete attestation, and its `assertions` array is the row's
`removal_assertions`.
The attended receipt must set `delete_attest_restart_observed=true` only after
observing a real agent process restart at the helper-delete/attestation boundary
without an early L1 acknowledgement. Injected callback errors may be recorded
separately but do not prove a restart; hosted lanes record the restart row as
`NOT-RUN`. The same receipt also carries bind-source byte/digest equality and
the retained image-cache observation. A row that was skipped or could not be
inventoried is a failure, never a synthesized PASS.

Ticket #152 additionally requires PASS rows for `launch_daemon`,
`no_lima_autostart`, `helper_install_permissions`,
`stopped_enabled_recovery`, `stopped_disabled_no_recovery`,
`broken_enabled_recovery`, `process_only_degradation`, and `minimal_doctor`.
The recovery rows include `oci_enabled` and exact `lima_states`, and
`broken_enabled_recovery` additionally includes `repair_count_delta` and its
host observation (`host_observed_states`, `host_observed_at`); the permission
row includes `socket_mode`, `socket_owner`, and `socket_group`; the launch rows
include `launch_units`; and the doctor row embeds the redacted minimal snapshot.

After capture, run the same private bootstrap with `--remove`, instance,
`limactl`, facts, and intent paths. Preserve its JSON evidence and require the
host unit, guest helper binary/socket/service, facts, and intent marker to be
absent. A second removal must report the same absence without failing. The
first `--remove` can intermittently report `unloaded:false` while launchd is
still draining the unit; a second call then succeeds.

Fold the complete artifact into the tagged lane with:

```sh
WEFTY_LIMA_ACCEPTANCE_ARTIFACT=/absolute/path/to/redacted-receipt.json \
  go test -tags=service_acceptance -run AttendedLimaArtifact ./runner/lima
```

## The Mac half of the OCI acceptance matrix (#157)

The attended artifact proves 34 owner-hardware rows. Spec section 9's matrix is
coarser: nine class rows under each platform prefix, one capability row, and the
Mac-only list. `TestServiceAcceptanceAttendedLimaMatrixFragment` maps one onto
the other and writes the `mac.*` fragment that
`scripts/assemble-oci-acceptance-matrix.sh` merges with the Linux realtiming
receipts:

```sh
WEFTY_LIMA_ACCEPTANCE_ARTIFACT=/absolute/path/to/redacted-receipt.json \
WEFTY_LIMA_MATRIX_OUT=/absolute/path/to/mac-oci-matrix-rows.json \
  go test -tags=service_acceptance -run AttendedLimaMatrixFragment ./runner/lima

scripts/assemble-oci-acceptance-matrix.sh /absolute/path/to/oci-acceptance-matrix.json \
  "$(git rev-parse HEAD)" published-artifact /path/to/linux-evidence \
  /absolute/path/to/mac-oci-matrix-rows.json owner-hardware
scripts/check-oci-acceptance-matrix.sh /absolute/path/to/oci-acceptance-matrix.json \
  "$(git rev-parse HEAD)" published-artifact owner-hardware
```

Unlike the destination-success gate above, the fragment producer does not
require PASS rows: a red attended session must still produce an honest, typed
fragment. A failed attended row becomes a matrix `FAIL` carrying the attended
reason verbatim; a blocked row becomes a typed `NOT-RUN`. Either way the owning
ticket comes from that attended row's own `blocked_by` field, falling back to
#157 when the owner recorded none.

The gate then treats those two outcomes differently, by design. A typed
`NOT-RUN` is an honest "not proven yet" and passes. A `FAIL` is a proof that ran
and came back red, so **`check-oci-acceptance-matrix.sh` exits non-zero and names
the failing rows.** Against the 2026-09-11 session it exits 1 on four rows. Those
rows were blocked by #394, which has since been fixed (#407), then by #408,
fixed in turn (#411). Naming the run's dominant blocker in source went stale
both times, so the fragment producer no longer does: record the blocking ticket
per row in the receipt's `blocked_by` field and it appears in the matrix
verbatim. Either way the non-zero exit is the command working, not the command
broken: the matrix cannot be green while a Mac cell is red. The rows that once
had no procedure (`task_logs_delete`, `mount_validation`, `host_to_guest`,
`guest_to_host_fallback`, `helper_loss`, `vm_loss`, `sweep_before_recovery`) now
have one — #403 gave them the window and #410 gave them the client — so a
non-PASS on any of them falls through like any other row, to that row's own
`blocked_by` and reason. Like the artifact itself, the fragment and the
assembled matrix stay outside Git.

The attended agent-computer lane in
[m3.5-mac-computer.md](m3.5-mac-computer.md) sits on top of this one and feeds a
separate matrix: same fold-a-Mac-half-into-a-versioned-artifact shape, spec §10
rows rather than §9 cells. It assumes the transport rows here are green on the
same hardware.

`realtiming-result` assembles the same matrix with `none` for the Mac fragment,
so every `mac.*` row is a typed `NOT-RUN` and the gate prints
`matrix incomplete: 18 Mac rows not run`. A GitHub-hosted runner that claims
attended Mac evidence fails the gate, as does a fragment bound to another
candidate commit. A green CI therefore never means a finished matrix.
