# M3 Lima transport and service publication attended acceptance

This is the owner-hardware lane for Tickets #145, #147, #149, and #150 and the Mac rows of the M3 OCI
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
  main `acceptance-image` workflow, with the public commit tag pinned to
  `acceptance-image-index-digest.txt`; its OCI archive contains the same
  `/bin/sh`, BusyBox utilities, and `cmd/wefty-echo-service` program used by
  Linux realtiming, including the distinct one-shot stdout/stderr markers;
- one temporary host operator-mount root dedicated to this run.

Ticket #152 adds the minimum installed boot topology consumed by this lane.
Build the candidate macOS agent and use the matching Linux/arm64 release tree
from that artifact before running the private bootstrap mode. Use its
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
starts the existing configured instance,
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
  directory; that directory is operator-owned `0700` and the guest socket is
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
  the Lima guest agent picks up `wefty-oci`; an already-member rerun does not;
- the probe archive imports to the recorded top-level digest through the helper
  API and the functional create/start/wait/delete probe succeeds.

Capture the atomic minimal-facts JSON named by
`--oci-minimal-doctor-facts`. It contains only schema version, observation
time, unit, Lima/helper/probe state, capability revision, and a stable
sanitized reason code. It must contain no raw error, helper session capability,
credential, or environment dump. The unit state is `launched_by_unit`, state
values use the closed contract vocabulary, and unchanged content is not
rewritten more frequently than the 20-second observation floor.

Exercise supervision twice from a clean process-only baseline. With an enabled
intent-file revision, stop Lima and require `stopped -> running`; inject or
observe `Broken` and require `broken -> stopped -> running` through one bounded
`stop --force`/capped-backoff repair. Persist a higher disabled intent-file
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
   OCI claim begins before the fresh capability revision is acknowledged.
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
5. Guest to host primary: resolve `host.lima.internal` from inside the current
   guest, record the discovered address, bind only that address on macOS, and
   complete one authenticated run-bridge request. Prove no `0.0.0.0` listener
   and no fixed gateway string exists in config, argv, or source.
6. Guest to host fallback: inject a bind failure for that discovered address.
   Require a host-loopback bridge, helper-issued per-attempt bridge capability,
   and successful request through `DialHostBridge`; wrong capability and wrong
   attempt must fail. Discovery failure itself must fail start and must not
   select fallback.
7. Service publication: export the attended helper socket/checksum and pinned
   probe image reference/digest as `WEFTY_OCI_HELPER_SOCKET`,
   `WEFTY_OCI_HELPER_CHECKSUM`, `WEFTY_OCI_PROBE_REFERENCE`, and
   `WEFTY_OCI_PROBE_DIGEST`, then run:

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
   (`12001:12002`) image-user variants. For each, require `/wefty/service` to
   begin with exactly that UID:GID and accept a payload write. For one stable
   service job, record attempt counters `0,1,2` across crash restart and
   stop→start while a marker outside `/wefty/service` is absent at the start of
   every fresh attempt. Start a second service job on the same pinned digest;
   require its service data to be empty while the original job remains
   digest-pinned and retains its own counter. The helper-owned backing path must resolve inside the
   Linux guest's native filesystem, remain absent from the Lima host mounts,
   and never traverse virtiofs. Record these facts in the
   `service_data_guest_native` row.
9. Removal manifest: while the agent is offline, request removal of a bound OCI
   service and observe L1 at exactly `removal_pending`. Return the same
   node through the ordinary boot sweep barrier, then capture the immutable
   job/removal-generation manifest with every attempt lease, task, container,
   snapshot, shim, cgroup, framed-log directory, service-data volume, and its
   owner record. Require the persisted positive prior-boot sweep receipt and
   `prepared -> quarantined -> complete` phase history, then require the
   proof-gated completion path to delete the guest-native service-data bytes
   and owner record, persist a helper-generation assertion for every manifest
   row, and only then reach `removed_verified`. Record the guest-native
   inventories and phase facts in `service_removal_manifest_offline`.

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

For helper loss and then full VM stop, leave a live marker workload before the
fault. Each row must show, in order:

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

## Receipt

Store the attended artifact outside Git with: candidate commit SHA; one
session ID repeated by every row; host/Lima/containerd/runc versions; explicit
VM sizing; redacted template checksum; helper generations; capability
revisions; per-row PASS/FAIL/NOT-RUN; exact commands and exit codes; and residue inventories before fault,
after sweep, and after delete. NOT-RUN is not destination success and must name
the missing owner-hardware prerequisite.

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
data directory and its owner record independently. Hosted macOS runners are
`NOT-RUN`; they do not satisfy this owner-hardware row.

Ticket #151 additionally requires `post_delete_attestation=true`,
`service_data_bytes_absent=true`, `service_data_owner_record_absent=true`, and
one `absent=true` assertion for every class/identity in `resource_manifests`.
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
The recovery rows include `oci_enabled` and exact `lima_states`; the permission
row includes `socket_mode`, `socket_owner`, and `socket_group`; the launch rows
include `launch_units`; and the doctor row embeds the redacted minimal snapshot.

After capture, run the same private bootstrap with `--remove`, instance,
`limactl`, facts, and intent paths. Preserve its JSON evidence and require the
host unit, guest helper binary/socket/service, facts, and intent marker to be
absent. A second removal must report the same absence without failing.

Fold the complete artifact into the tagged lane with:

```sh
WEFTY_LIMA_ACCEPTANCE_ARTIFACT=/absolute/path/to/redacted-receipt.json \
  go test -tags=service_acceptance -run AttendedLimaArtifact ./runner/lima
```
